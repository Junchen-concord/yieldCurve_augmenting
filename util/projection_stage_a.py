"""Stage A: origination-time multinomial classifier for payoff_type."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from .projection_labels import (
    PAYOFF_TYPE_COLLAPSED_ORDER,
    encode_payoff_type_int,
)


@dataclass
class StageAModel:
    """Lightweight bundle of fitted Stage A artifacts."""
    booster: "xgb.Booster"  # type: ignore[name-defined]  # noqa: F821
    features: list[str]
    class_order: list[str]
    train_rows: int
    holdout_rows: int


def fit_stage_a(
    loan_base: pd.DataFrame,
    features: list[str],
    target_col: str = "payoff_type_collapsed",
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
    xgb_params: Optional[dict] = None,
    num_boost_round: int = 300,
    early_stopping_rounds: int = 20,
    random_state: int = 42,
) -> StageAModel:
    """Fit an XGBoost multinomial classifier on loan_base.

    Parameters
    ----------
    loan_base : DataFrame
        Must include the feature columns, target, `is_training_eligible`, and `is_holdout`.
    features : list[str]
        Stage A feature columns. Numeric / encoded only (caller is responsible for encoding).
    target_col : str
        Column holding the 5-class collapsed payoff_type as strings.
    """
    import xgboost as xgb  # lazy import so the module is importable without xgboost installed

    eligible = loan_base[loan_base[eligible_col] == True].copy()  # noqa: E712
    if target_col not in eligible.columns:
        raise ValueError(f"loan_base must have column '{target_col}' (from collapse_payoff_type).")

    y = encode_payoff_type_int(eligible[target_col])
    if y.lt(0).any():
        bad = eligible.loc[y.lt(0), target_col].value_counts(dropna=False).to_dict()
        raise ValueError(f"Unmapped target labels in {target_col}: {bad}")

    X = eligible[features].copy()
    for c in features:
        if not pd.api.types.is_numeric_dtype(X[c]):
            raise ValueError(f"Stage A feature '{c}' is non-numeric; encode it before fitting.")
    X = X.fillna(np.nan)

    train_mask = ~eligible[holdout_col].astype(bool).values
    hold_mask = eligible[holdout_col].astype(bool).values

    default_params = {
        "objective": "multi:softprob",
        "num_class": len(PAYOFF_TYPE_COLLAPSED_ORDER),
        "eta": 0.08,
        "max_depth": 5,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "tree_method": "hist",
        "eval_metric": "mlogloss",
        "seed": random_state,
    }
    params = {**default_params, **(xgb_params or {})}

    dtrain = xgb.DMatrix(X.loc[train_mask], label=y.loc[train_mask].values)
    dholdout = xgb.DMatrix(X.loc[hold_mask], label=y.loc[hold_mask].values)

    booster = xgb.train(
        params,
        dtrain,
        num_boost_round=num_boost_round,
        evals=[(dtrain, "train"), (dholdout, "holdout")],
        early_stopping_rounds=early_stopping_rounds,
        verbose_eval=False,
    )

    return StageAModel(
        booster=booster,
        features=list(features),
        class_order=list(PAYOFF_TYPE_COLLAPSED_ORDER),
        train_rows=int(train_mask.sum()),
        holdout_rows=int(hold_mask.sum()),
    )


def predict_proba(model: StageAModel, df: pd.DataFrame) -> pd.DataFrame:
    """Predict P(class) for each row of df. Returns a DataFrame with one column per class."""
    import xgboost as xgb

    X = df[model.features].copy()
    for c in model.features:
        if not pd.api.types.is_numeric_dtype(X[c]):
            raise ValueError(f"Stage A feature '{c}' is non-numeric at predict time.")
    dmat = xgb.DMatrix(X)
    probs = model.booster.predict(dmat)
    out = pd.DataFrame(probs, index=df.index, columns=model.class_order)
    out.columns = [f"P_{c}" for c in model.class_order]
    return out


def evaluate_stage_a(
    model: StageAModel,
    loan_base: pd.DataFrame,
    target_col: str = "payoff_type_collapsed",
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
) -> dict:
    """Compute multiclass log-loss + Brier on the holdout slice."""
    from sklearn.metrics import log_loss

    eligible = loan_base[loan_base[eligible_col] == True].copy()  # noqa: E712
    hold = eligible[eligible[holdout_col].astype(bool)]

    probs = predict_proba(model, hold).values
    y_true = encode_payoff_type_int(hold[target_col]).values

    ll = log_loss(y_true, probs, labels=list(range(len(model.class_order))))

    # Multiclass Brier: sum over classes of (p - onehot)^2, averaged over samples.
    onehot = np.zeros_like(probs)
    onehot[np.arange(len(y_true)), y_true] = 1.0
    brier = float(np.mean(np.sum((probs - onehot) ** 2, axis=1)))

    # Per-class calibration summary: predicted mean vs observed rate.
    calib_rows = []
    for j, cls in enumerate(model.class_order):
        calib_rows.append({
            "class": cls,
            "n_true": int(onehot[:, j].sum()),
            "pred_mean": float(probs[:, j].mean()),
            "obs_rate": float(onehot[:, j].mean()),
        })
    calib = pd.DataFrame(calib_rows)

    return {
        "holdout_rows": int(len(hold)),
        "log_loss": float(ll),
        "brier": brier,
        "per_class_calibration": calib,
    }
