"""Stage B: per-installment collection probability + expected amount (MVP)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class StageBModel:
    """Fitted Stage B artifacts."""
    clf: "lgb.Booster"  # type: ignore[name-defined]  # noqa: F821
    reg: "lgb.Booster"  # type: ignore[name-defined]  # noqa: F821
    features: list[str]
    train_rows: int
    holdout_rows: int


def fit_stage_b(
    seq_base: pd.DataFrame,
    features: list[str],
    collected_col: str = "collected_flag_k",
    amount_col: str = "collected_amount_k",
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
    clf_params: Optional[dict] = None,
    reg_params: Optional[dict] = None,
    num_boost_round: int = 400,
    early_stopping_rounds: int = 30,
    random_state: int = 42,
) -> StageBModel:
    """Fit LightGBM binary classifier for P(collected) and regressor for E[$|collected]."""
    import lightgbm as lgb

    eligible = seq_base[seq_base[eligible_col] == True].copy()  # noqa: E712
    X = eligible[features].copy()
    for c in features:
        if not pd.api.types.is_numeric_dtype(X[c]):
            raise ValueError(f"Stage B feature '{c}' is non-numeric; encode it before fitting.")

    train_mask = ~eligible[holdout_col].astype(bool).values
    hold_mask = eligible[holdout_col].astype(bool).values

    # Classifier: P(collected_flag_k)
    y_clf = eligible[collected_col].astype(int).values
    default_clf_params = {
        "objective": "binary",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_data_in_leaf": 50,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 5,
        "metric": "binary_logloss",
        "verbose": -1,
        "seed": random_state,
    }
    clf_p = {**default_clf_params, **(clf_params or {})}

    dtrain_clf = lgb.Dataset(X.loc[train_mask], label=y_clf[train_mask])
    dvalid_clf = lgb.Dataset(X.loc[hold_mask], label=y_clf[hold_mask], reference=dtrain_clf)
    clf_booster = lgb.train(
        clf_p,
        dtrain_clf,
        num_boost_round=num_boost_round,
        valid_sets=[dtrain_clf, dvalid_clf],
        valid_names=["train", "holdout"],
        callbacks=[lgb.early_stopping(early_stopping_rounds), lgb.log_evaluation(0)],
    )

    # Regressor: E[amount | collected_flag_k = 1]
    collected_mask = eligible[collected_col] == 1
    reg_eligible = eligible.loc[collected_mask].copy()
    X_reg = reg_eligible[features]
    y_reg = reg_eligible[amount_col].astype(float).values
    train_mask_r = ~reg_eligible[holdout_col].astype(bool).values
    hold_mask_r = reg_eligible[holdout_col].astype(bool).values

    default_reg_params = {
        "objective": "regression",
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_data_in_leaf": 50,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 5,
        "metric": "mae",
        "verbose": -1,
        "seed": random_state,
    }
    reg_p = {**default_reg_params, **(reg_params or {})}

    dtrain_reg = lgb.Dataset(X_reg.loc[train_mask_r], label=y_reg[train_mask_r])
    dvalid_reg = lgb.Dataset(X_reg.loc[hold_mask_r], label=y_reg[hold_mask_r], reference=dtrain_reg)
    reg_booster = lgb.train(
        reg_p,
        dtrain_reg,
        num_boost_round=num_boost_round,
        valid_sets=[dtrain_reg, dvalid_reg],
        valid_names=["train", "holdout"],
        callbacks=[lgb.early_stopping(early_stopping_rounds), lgb.log_evaluation(0)],
    )

    return StageBModel(
        clf=clf_booster,
        reg=reg_booster,
        features=list(features),
        train_rows=int(train_mask.sum()),
        holdout_rows=int(hold_mask.sum()),
    )


def predict_expected_amount(model: StageBModel, df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with columns p_collected, e_amount_if_collected, e_amount."""
    X = df[model.features]
    p_col = model.clf.predict(X, num_iteration=model.clf.best_iteration)
    e_amt_cond = model.reg.predict(X, num_iteration=model.reg.best_iteration)
    e_amt_cond = np.clip(e_amt_cond, a_min=0.0, a_max=None)
    out = pd.DataFrame({
        "p_collected": p_col,
        "e_amount_if_collected": e_amt_cond,
        "e_amount": p_col * e_amt_cond,
    }, index=df.index)
    return out


def evaluate_stage_b(
    model: StageBModel,
    seq_base: pd.DataFrame,
    collected_col: str = "collected_flag_k",
    amount_col: str = "collected_amount_k",
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
) -> dict:
    """Report classifier + regressor metrics on the holdout slice, plus per-installment calibration."""
    from sklearn.metrics import log_loss, mean_absolute_error, r2_score

    eligible = seq_base[seq_base[eligible_col] == True].copy()  # noqa: E712
    hold = eligible[eligible[holdout_col].astype(bool)]

    preds = predict_expected_amount(model, hold)

    # Classifier metrics
    y_clf = hold[collected_col].astype(int).values
    clf_ll = log_loss(y_clf, preds["p_collected"].clip(1e-6, 1 - 1e-6))
    clf_acc = float((preds["p_collected"] >= 0.5).astype(int).eq(pd.Series(y_clf, index=preds.index)).mean())

    # Regressor metrics (on collected only)
    coll_mask = hold[collected_col] == 1
    y_amt = hold.loc[coll_mask, amount_col].astype(float).values
    p_amt = preds.loc[coll_mask, "e_amount_if_collected"].values
    reg_mae = float(mean_absolute_error(y_amt, p_amt)) if len(y_amt) else float("nan")
    reg_r2 = float(r2_score(y_amt, p_amt)) if len(y_amt) > 1 else float("nan")

    # Per-installment calibration: predicted mean e_amount vs observed mean collected_amount_k.
    joined = hold[[collected_col, amount_col, "InstallmentNumber"]].join(preds[["p_collected", "e_amount"]])
    calib = (
        joined.groupby("InstallmentNumber")
        .agg(
            n=(collected_col, "size"),
            obs_collect_rate=(collected_col, "mean"),
            pred_collect_rate=("p_collected", "mean"),
            obs_amount_mean=(amount_col, "mean"),
            pred_amount_mean=("e_amount", "mean"),
        )
        .reset_index()
    )

    return {
        "holdout_rows": int(len(hold)),
        "classifier_log_loss": float(clf_ll),
        "classifier_accuracy_at_0p5": clf_acc,
        "regressor_mae": reg_mae,
        "regressor_r2": reg_r2,
        "per_installment_calibration": calib,
    }
