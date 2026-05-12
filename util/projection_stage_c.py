"""Stage C: empirical recovery-fraction curves for defaulted loans (MVP)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import numpy as np
import pandas as pd

from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER

# xPD classes carry a post-default recovery term; the others are pinned to 0.0 recovery
# so `apply_stage_c_recovery` is a no-op on them. 4PD/5PD are already collapsed into LatePD
# upstream (see projection_labels.PAYOFF_TYPE_COLLAPSED_ORDER), so only 4 xPD classes here.
_XPD_CLASSES = ("FPD", "SPD", "TPD", "LatePD")


@dataclass
class RecoveryCurve:
    """Empirical recovery curve: recovery_fraction as a function of days_since_default bucket."""
    curve: pd.DataFrame  # columns: days_bucket_mid, mean_recovery_fraction, n_loans
    max_bucket_days: float


@dataclass
class StageCRecoveryModel:
    """Fitted two-part Stage C recovery model."""
    clf: "lgb.Booster"  # type: ignore[name-defined]  # noqa: F821
    reg: "lgb.Booster"  # type: ignore[name-defined]  # noqa: F821
    features: list[str]
    fallback_by_class: Dict[str, float]
    train_rows: int
    holdout_rows: int
    train_positive_rows: int
    min_days_since_default: int


STAGE_C_RECOVERY_FEATURES = [
    "PortFolioID",
    "OriginatedAmount",
    "log_originated_amount",
    "outstanding_at_default",
    "log_outstanding_at_default",
    "paid_by_default",
    "paid_by_default_ratio",
    "default_inst",
    "days_since_default",
    "CustType_enc",
    "Frequency_norm_enc",
    "LoanStatus_enc",
    "payoff_type_collapsed_enc",
    "DM_Band_enc",
    "CM_Band_enc",
    "arr_attempt_count",
    "arr_success_count",
    "arr_fail_count",
    "arr_success_rate",
    "arr_attempted_amount",
    "arr_success_amount",
    "arr_fail_amount",
    "arr_avg_attempt_amount",
    "arr_days_default_to_first_attempt",
    "arr_days_default_to_first_success",
    "arr_days_default_to_last_attempt",
    "arr_days_default_to_last_success",
    "arr_fail_streak",
    "arr_recent_fail_count_last_3",
    "arr_last_attempt_was_success",
    "arr_has_attempt",
    "tp_attempt_count",
    "tp_success_count",
    "tp_fail_count",
    "tp_success_rate",
    "tp_attempted_amount",
    "tp_success_amount",
    "tp_fail_amount",
    "tp_avg_attempt_amount",
    "tp_days_default_to_first_attempt",
    "tp_days_default_to_first_success",
    "tp_days_default_to_last_attempt",
    "tp_days_default_to_last_success",
    "tp_fail_streak",
    "tp_recent_fail_count_last_3",
    "tp_last_attempt_was_success",
    "tp_has_attempt",
    "rec_attempt_count",
    "rec_success_count",
    "rec_fail_count",
    "rec_success_rate",
    "rec_attempted_amount",
    "rec_success_amount",
    "rec_fail_amount",
    "rec_avg_attempt_amount",
    "arr_attempt_share",
    "arr_success_amount_share",
    "tp_attempt_share",
    "tp_success_amount_share",
]


def build_recovery_curve(
    stage_c_base: pd.DataFrame,
    days_col: str = "days_since_default",
    outstanding_col: str = "outstanding_at_default",
    recovery_col: str = "recovery_realized",
    bucket_days: int = 30,
    max_bucket_days: int = 540,
    min_loans_per_bucket: int = 20,
) -> RecoveryCurve:
    """Bucket defaulted loans by time since default and compute mean recovery fraction.

    For MVP we use a single global curve (no segmentation). Refinement later.
    """
    df = stage_c_base.copy()
    df = df[df[outstanding_col] > 0]  # recovery fraction is undefined with zero outstanding

    df["recovery_fraction"] = (df[recovery_col] / df[outstanding_col]).clip(0.0, 1.0)
    df["days_bucket"] = (df[days_col] // bucket_days).clip(lower=0, upper=max_bucket_days // bucket_days) * bucket_days

    agg = (
        df.groupby("days_bucket")
        .agg(
            n_loans=(recovery_col, "size"),
            mean_recovery_fraction=("recovery_fraction", "mean"),
            median_recovery_fraction=("recovery_fraction", "median"),
        )
        .reset_index()
    )
    agg["days_bucket_mid"] = agg["days_bucket"] + bucket_days / 2.0
    agg = agg[agg["n_loans"] >= min_loans_per_bucket].copy()

    if agg.empty:
        # Degenerate fallback: no bucket has enough data. Return a flat zero-recovery curve.
        agg = pd.DataFrame({
            "days_bucket": [0],
            "n_loans": [0],
            "mean_recovery_fraction": [0.0],
            "median_recovery_fraction": [0.0],
            "days_bucket_mid": [bucket_days / 2.0],
        })

    return RecoveryCurve(curve=agg, max_bucket_days=float(max_bucket_days))


def predict_recovery_fraction(
    curve_obj: RecoveryCurve,
    days_since_default: float | np.ndarray,
    method: str = "step",
) -> np.ndarray:
    """Look up recovery fraction for one or more days-since-default values.

    method='step' uses piecewise-constant lookup (safest for small N per bucket).
    method='linear' interpolates between bucket centers.
    """
    days = np.atleast_1d(np.asarray(days_since_default, dtype=float))
    xp = curve_obj.curve["days_bucket_mid"].to_numpy()
    fp = curve_obj.curve["mean_recovery_fraction"].to_numpy()

    if method == "linear":
        out = np.interp(days, xp, fp, left=fp[0], right=fp[-1])
    elif method == "step":
        # For each day value, pick the bucket with the closest lower-bucket-mid.
        idx = np.searchsorted(xp, days, side="right") - 1
        idx = np.clip(idx, 0, len(xp) - 1)
        out = fp[idx]
    else:
        raise ValueError(f"Unknown method '{method}'")

    return out


def summarize_recovery_curve(curve_obj: RecoveryCurve) -> pd.DataFrame:
    """Pretty-print-friendly summary of the recovery curve."""
    return curve_obj.curve[["days_bucket", "n_loans", "mean_recovery_fraction", "median_recovery_fraction"]]


# ---------------------------------------------------------------------------
# Tier 3 (Option 1) -- per-class terminal recovery fraction for the simulator.
#
# The curve above answers "as a loan ages past default, what fraction of
# outstanding has been recovered so far?" The simulator needs the *terminal*
# answer: "for a loan that defaults in class c, what fraction of outstanding
# will eventually be recovered?" We estimate that empirically from matured
# defaulted training loans, segmented by class. Option 2 later replaces the
# per-class constant with a per-loan model prediction; the downstream matrix
# contract (see `broadcast_class_recovery_to_matrix`) is identical either way.
# ---------------------------------------------------------------------------


def compute_terminal_recovery_by_class(
    stage_c_base: pd.DataFrame,
    loan_base: pd.DataFrame,
    class_col: str = "payoff_type_collapsed",
    min_days_since_default: int = 180,
    train_only: bool = True,
    min_loans_per_class: int = 30,
    verbose: bool = True,
) -> Dict[str, float]:
    """Per-class empirical terminal recovery fraction for defaulted loans.

    For each xPD class, returns mean(recovery_realized / outstanding_at_default)
    over matured (>= min_days_since_default) defaulted loans. Classes with fewer
    than `min_loans_per_class` matured loans fall back to the global mature
    recovery fraction (with a printed warning). Non-xPD classes are pinned to 0.

    Parameters
    ----------
    stage_c_base : pd.DataFrame
        Per-loan recovery frame built upstream (must carry LoanID,
        days_since_default, outstanding_at_default, recovery_realized).
    loan_base : pd.DataFrame
        Loan-level frame carrying the collapsed payoff class and the
        training / holdout flags.
    class_col : str
        Column in loan_base holding the 5-class modeled label.
    min_days_since_default : int
        Maturity threshold. Loans defaulted within this window are excluded
        because their recovery stream hasn't had time to play out.
    train_only : bool
        If True, exclude holdout loans from the estimate (keeps the holdout
        projection honest).
    min_loans_per_class : int
        Below this count, fall back to the global mature recovery fraction.

    Returns
    -------
    Dict[str, float]
        Keys cover every class in PAYOFF_TYPE_COLLAPSED_ORDER. xPD classes
        carry the empirical mean; the rest are 0.0.
    """
    meta_cols = ["LoanID", class_col, "is_training_eligible", "is_holdout"]
    missing = [c for c in meta_cols if c not in loan_base.columns]
    if missing:
        raise KeyError(f"loan_base is missing required columns: {missing}")

    # stage_c_base may already carry some of these (e.g. it's built with `is_holdout`
    # upstream). Drop overlaps on the left so loan_base stays the single source of
    # truth and we avoid pandas' `_x` / `_y` suffix trap on the merge.
    overlap = [c for c in meta_cols if c != "LoanID" and c in stage_c_base.columns]
    left = stage_c_base.drop(columns=overlap) if overlap else stage_c_base
    df = left.merge(loan_base[meta_cols], on="LoanID", how="inner")
    df = df[(df["outstanding_at_default"] > 0) & (df["days_since_default"] >= min_days_since_default)]
    if train_only:
        df = df[df["is_training_eligible"] & (~df["is_holdout"].astype(bool))]

    df = df.assign(
        recovery_fraction=(df["recovery_realized"] / df["outstanding_at_default"]).clip(0.0, 1.0),
    )

    global_frac = float(df["recovery_fraction"].mean()) if len(df) else 0.0
    global_n = int(len(df))
    if verbose:
        print(
            f"[stage_c] terminal recovery pool: {global_n:,} matured defaulted loans "
            f"(>= {min_days_since_default} days). Global mean recovery fraction = {global_frac:.3f}."
        )

    recovery_by_class: Dict[str, float] = {cls: 0.0 for cls in PAYOFF_TYPE_COLLAPSED_ORDER}
    for cls in _XPD_CLASSES:
        sub = df.loc[df[class_col] == cls, "recovery_fraction"]
        n = int(len(sub))
        if n >= min_loans_per_class:
            recovery_by_class[cls] = float(sub.mean())
        else:
            recovery_by_class[cls] = global_frac
            if verbose:
                print(
                    f"[stage_c] class {cls!r}: only {n} matured defaulted loans "
                    f"(< {min_loans_per_class}); falling back to global mean {global_frac:.3f}."
                )

    return recovery_by_class


def broadcast_class_recovery_to_matrix(
    recovery_by_class: Dict[str, float],
    like: pd.DataFrame,
) -> pd.DataFrame:
    """Broadcast a class-level recovery dict into a (loans x classes) matrix.

    The returned DataFrame shares index and columns with `like` (typically the
    Stage B payin_matrix), which is the contract the simulator-side apply
    helper expects. Option 2 will produce the same shape directly from a
    per-loan recovery model; the apply step downstream is unchanged.
    """
    missing = [c for c in like.columns if c not in recovery_by_class]
    if missing:
        raise KeyError(f"recovery_by_class missing entries for columns: {missing}")
    values = np.array([[recovery_by_class[c] for c in like.columns]] * len(like.index), dtype=float)
    return pd.DataFrame(values, index=like.index, columns=like.columns)


# ---------------------------------------------------------------------------
# Tier 3 (Option 2) -- per-loan Stage C recovery model.
# ---------------------------------------------------------------------------


def _consecutive_fail_streak(is_fail: pd.Series) -> pd.Series:
    streak = []
    run = 0
    for flag in is_fail.astype(int):
        run = run + 1 if flag == 1 else 0
        streak.append(run)
    return pd.Series(streak, index=is_fail.index, dtype="float64")


def _encode_series(s: pd.Series) -> pd.Series:
    return s.astype("string").fillna("UNKNOWN").astype("category").cat.codes.astype(float)


def _summarize_recovery_attempts(
    payment_df: pd.DataFrame,
    stage_c_base: pd.DataFrame,
    prefix: str,
) -> pd.DataFrame:
    """Summarize arrangement or third-party attempts at loan grain."""
    feature_cols = [
        f"{prefix}_attempt_count",
        f"{prefix}_success_count",
        f"{prefix}_fail_count",
        f"{prefix}_success_rate",
        f"{prefix}_attempted_amount",
        f"{prefix}_success_amount",
        f"{prefix}_fail_amount",
        f"{prefix}_avg_attempt_amount",
        f"{prefix}_days_default_to_first_attempt",
        f"{prefix}_days_default_to_first_success",
        f"{prefix}_days_default_to_last_attempt",
        f"{prefix}_days_default_to_last_success",
        f"{prefix}_fail_streak",
        f"{prefix}_recent_fail_count_last_3",
        f"{prefix}_last_attempt_was_success",
        f"{prefix}_has_attempt",
    ]
    empty = pd.DataFrame(columns=["LoanID", *feature_cols])
    if payment_df.empty:
        return empty

    required = {
        "LoanID",
        "InstallmentNumber",
        "InstallmentDueDate",
        "PaymentID",
        "AttemptNo",
        "PaymentDate",
        "TransactionDate",
        "PaymentAmount",
        "IsSuccess",
        "IsFail",
    }
    missing = sorted(required - set(payment_df.columns))
    if missing:
        raise ValueError(f"payment_df missing Stage C recovery columns: {missing}")

    default_dates = stage_c_base[["LoanID", "default_due_date"]].dropna(subset=["default_due_date"])
    p = payment_df.merge(default_dates, on="LoanID", how="inner")
    p["InstallmentDueDate"] = pd.to_datetime(p["InstallmentDueDate"], errors="coerce")
    p["PaymentDate"] = pd.to_datetime(p["PaymentDate"], errors="coerce")
    p["TransactionDate"] = pd.to_datetime(p["TransactionDate"], errors="coerce")
    p["default_due_date"] = pd.to_datetime(p["default_due_date"], errors="coerce")
    p = p[p["InstallmentDueDate"] >= p["default_due_date"]].copy()
    if p.empty:
        return empty

    for c in ["PaymentID", "AttemptNo", "IsSuccess", "IsFail"]:
        p[c] = pd.to_numeric(p[c], errors="coerce").fillna(0).astype(int)
    p["PaymentAmount"] = pd.to_numeric(p["PaymentAmount"], errors="coerce").fillna(0.0)
    p["success_amount"] = np.where(p["IsSuccess"].eq(1), p["PaymentAmount"], 0.0)
    p["fail_amount"] = np.where(p["IsFail"].eq(1), p["PaymentAmount"], 0.0)

    p = p.sort_values(["LoanID", "InstallmentNumber", "PaymentDate", "TransactionDate", "AttemptNo", "PaymentID"])
    g = p.groupby("LoanID", sort=False)
    p["_fail_streak"] = g["IsFail"].transform(_consecutive_fail_streak)
    p["_recent_fail_count_last_3"] = g["IsFail"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    p["_success_date"] = p["PaymentDate"].where(p["IsSuccess"].eq(1))

    agg = (
        p.groupby("LoanID", as_index=False)
        .agg(
            attempt_count=("PaymentID", "size"),
            success_count=("IsSuccess", "sum"),
            fail_count=("IsFail", "sum"),
            attempted_amount=("PaymentAmount", "sum"),
            success_amount=("success_amount", "sum"),
            fail_amount=("fail_amount", "sum"),
            first_attempt_date=("PaymentDate", "min"),
            first_success_date=("_success_date", "min"),
            last_attempt_date=("PaymentDate", "max"),
            last_success_date=("_success_date", "max"),
            fail_streak=("_fail_streak", "last"),
            recent_fail_count_last_3=("_recent_fail_count_last_3", "last"),
            last_attempt_was_success=("IsSuccess", "last"),
        )
    )
    agg["success_rate"] = agg["success_count"] / agg["attempt_count"].clip(lower=1)
    agg["avg_attempt_amount"] = agg["attempted_amount"] / agg["attempt_count"].clip(lower=1)
    agg = agg.merge(default_dates, on="LoanID", how="left")

    for date_col, out_col in [
        ("first_attempt_date", "days_default_to_first_attempt"),
        ("first_success_date", "days_default_to_first_success"),
        ("last_attempt_date", "days_default_to_last_attempt"),
        ("last_success_date", "days_default_to_last_success"),
    ]:
        agg[out_col] = (agg[date_col] - agg["default_due_date"]).dt.days

    for c in [
        "days_default_to_first_attempt",
        "days_default_to_first_success",
        "days_default_to_last_attempt",
        "days_default_to_last_success",
    ]:
        agg[c] = agg[c].fillna(9999).clip(lower=0)

    agg["has_attempt"] = (agg["attempt_count"] > 0).astype(float)
    rename = {c: f"{prefix}_{c}" for c in agg.columns if c != "LoanID"}
    out = agg.rename(columns=rename)
    return out[["LoanID", *feature_cols]]


def build_stage_c_recovery_model_base(
    stage_c_base: pd.DataFrame,
    loan_base: pd.DataFrame,
    payment_arr_df: pd.DataFrame,
    payment_3p_df: pd.DataFrame,
    class_col: str = "payoff_type_collapsed",
) -> pd.DataFrame:
    """Build a loan-grain Stage C modeling base with p5b/p5c behavior features."""
    meta_cols = [
        "LoanID",
        class_col,
        "is_training_eligible",
        "is_holdout",
        "DM_Band_enc",
        "CM_Band_enc",
    ]
    available_meta = [c for c in meta_cols if c in loan_base.columns]
    overlap = [c for c in available_meta if c != "LoanID" and c in stage_c_base.columns]
    base = stage_c_base.drop(columns=overlap) if overlap else stage_c_base.copy()
    base = base.merge(loan_base[available_meta], on="LoanID", how="left")

    arr = _summarize_recovery_attempts(payment_arr_df, base, "arr")
    tp = _summarize_recovery_attempts(payment_3p_df, base, "tp")
    out = base.merge(arr, on="LoanID", how="left").merge(tp, on="LoanID", how="left")

    for c in [c for c in out.columns if c.startswith(("arr_", "tp_"))]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    out["recovery_fraction"] = (
        out["recovery_realized"] / out["outstanding_at_default"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 1.0)
    out["any_recovery"] = (out["recovery_realized"] > 0).astype(int)

    out["log_originated_amount"] = np.log1p(pd.to_numeric(out["OriginatedAmount"], errors="coerce").fillna(0.0).clip(lower=0.0))
    out["log_outstanding_at_default"] = np.log1p(pd.to_numeric(out["outstanding_at_default"], errors="coerce").fillna(0.0).clip(lower=0.0))
    out["paid_by_default_ratio"] = (
        out["paid_by_default"] / out["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(0.0, 10.0)

    out["CustType_enc"] = _encode_series(out.get("CustType", pd.Series(index=out.index, dtype="object")))
    out["Frequency_norm_enc"] = _encode_series(out.get("Frequency_norm", pd.Series(index=out.index, dtype="object")))
    out["LoanStatus_enc"] = _encode_series(out.get("LoanStatus", pd.Series(index=out.index, dtype="object")))
    out["payoff_type_collapsed_enc"] = _encode_series(out.get(class_col, pd.Series(index=out.index, dtype="object")))

    for c in ["PortFolioID", "OriginatedAmount", "outstanding_at_default", "paid_by_default", "default_inst", "days_since_default"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)

    out["rec_attempt_count"] = out["arr_attempt_count"] + out["tp_attempt_count"]
    out["rec_success_count"] = out["arr_success_count"] + out["tp_success_count"]
    out["rec_fail_count"] = out["arr_fail_count"] + out["tp_fail_count"]
    out["rec_attempted_amount"] = out["arr_attempted_amount"] + out["tp_attempted_amount"]
    out["rec_success_amount"] = out["arr_success_amount"] + out["tp_success_amount"]
    out["rec_fail_amount"] = out["arr_fail_amount"] + out["tp_fail_amount"]
    out["rec_success_rate"] = out["rec_success_count"] / out["rec_attempt_count"].replace(0, np.nan)
    out["rec_avg_attempt_amount"] = out["rec_attempted_amount"] / out["rec_attempt_count"].replace(0, np.nan)
    out["arr_attempt_share"] = out["arr_attempt_count"] / out["rec_attempt_count"].replace(0, np.nan)
    out["tp_attempt_share"] = out["tp_attempt_count"] / out["rec_attempt_count"].replace(0, np.nan)
    out["arr_success_amount_share"] = out["arr_success_amount"] / out["rec_success_amount"].replace(0, np.nan)
    out["tp_success_amount_share"] = out["tp_success_amount"] / out["rec_success_amount"].replace(0, np.nan)

    for c in STAGE_C_RECOVERY_FEATURES:
        if c not in out.columns:
            out[c] = 0.0
        out[c] = pd.to_numeric(out[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return out


def fit_stage_c_recovery_model(
    model_base: pd.DataFrame,
    recovery_by_class: Dict[str, float],
    features: Optional[list[str]] = None,
    class_col: str = "payoff_type_collapsed",
    min_days_since_default: int = 180,
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
    clf_params: Optional[dict] = None,
    reg_params: Optional[dict] = None,
    num_boost_round: int = 300,
    early_stopping_rounds: int = 30,
    random_state: int = 42,
) -> StageCRecoveryModel:
    """Fit a two-part model for P(any recovery) and E[recovery fraction | recovery]."""
    import lightgbm as lgb

    features = list(features or STAGE_C_RECOVERY_FEATURES)
    eligible = model_base[
        (model_base[eligible_col] == True)  # noqa: E712
        & (model_base["outstanding_at_default"] > 0)
        & (model_base["days_since_default"] >= min_days_since_default)
        & (model_base[class_col].isin(_XPD_CLASSES))
    ].copy()
    if eligible.empty:
        raise ValueError("No eligible Stage C recovery rows available for training.")

    X = eligible[features].copy()
    for c in features:
        if not pd.api.types.is_numeric_dtype(X[c]):
            raise ValueError(f"Stage C feature '{c}' is non-numeric; encode it before fitting.")

    train_mask = ~eligible[holdout_col].astype(bool).values
    hold_mask = eligible[holdout_col].astype(bool).values
    if train_mask.sum() == 0:
        raise ValueError("No non-holdout Stage C rows available for training.")

    y_clf = eligible["any_recovery"].astype(int).values
    default_clf_params = {
        "objective": "binary",
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_data_in_leaf": 25,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 5,
        "metric": "binary_logloss",
        "verbose": -1,
        "seed": random_state,
    }
    clf_p = {**default_clf_params, **(clf_params or {})}
    valid_sets = [lgb.Dataset(X.loc[train_mask], label=y_clf[train_mask])]
    valid_names = ["train"]
    if hold_mask.sum() > 0 and len(np.unique(y_clf[hold_mask])) > 1:
        valid_sets.append(lgb.Dataset(X.loc[hold_mask], label=y_clf[hold_mask], reference=valid_sets[0]))
        valid_names.append("holdout")
    clf_booster = lgb.train(
        clf_p,
        valid_sets[0],
        num_boost_round=num_boost_round,
        valid_sets=valid_sets,
        valid_names=valid_names,
        callbacks=[lgb.early_stopping(early_stopping_rounds), lgb.log_evaluation(0)],
    )

    reg_eligible = eligible[eligible["any_recovery"].eq(1)].copy()
    train_mask_r = ~reg_eligible[holdout_col].astype(bool).values
    hold_mask_r = reg_eligible[holdout_col].astype(bool).values
    if train_mask_r.sum() == 0:
        raise ValueError("No positive non-holdout Stage C recovery rows available for regression.")
    X_reg = reg_eligible[features]
    y_reg = reg_eligible["recovery_fraction"].astype(float).values

    default_reg_params = {
        "objective": "regression",
        "learning_rate": 0.05,
        "num_leaves": 15,
        "min_data_in_leaf": 20,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 5,
        "metric": "mae",
        "verbose": -1,
        "seed": random_state,
    }
    reg_p = {**default_reg_params, **(reg_params or {})}
    valid_sets_r = [lgb.Dataset(X_reg.loc[train_mask_r], label=y_reg[train_mask_r])]
    valid_names_r = ["train"]
    if hold_mask_r.sum() > 0:
        valid_sets_r.append(lgb.Dataset(X_reg.loc[hold_mask_r], label=y_reg[hold_mask_r], reference=valid_sets_r[0]))
        valid_names_r.append("holdout")
    reg_booster = lgb.train(
        reg_p,
        valid_sets_r[0],
        num_boost_round=num_boost_round,
        valid_sets=valid_sets_r,
        valid_names=valid_names_r,
        callbacks=[lgb.early_stopping(early_stopping_rounds), lgb.log_evaluation(0)],
    )

    return StageCRecoveryModel(
        clf=clf_booster,
        reg=reg_booster,
        features=features,
        fallback_by_class=dict(recovery_by_class),
        train_rows=int(train_mask.sum()),
        holdout_rows=int(hold_mask.sum()),
        train_positive_rows=int(train_mask_r.sum()),
        min_days_since_default=int(min_days_since_default),
    )


def predict_stage_c_recovery(model: StageCRecoveryModel, df: pd.DataFrame) -> pd.DataFrame:
    """Predict per-loan terminal recovery fraction from a fitted Stage C model."""
    X = df[model.features].copy()
    for c in model.features:
        X[c] = pd.to_numeric(X[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    p_any = model.clf.predict(X, num_iteration=model.clf.best_iteration)
    frac_if_recovered = model.reg.predict(X, num_iteration=model.reg.best_iteration)
    frac_if_recovered = np.clip(frac_if_recovered, 0.0, 1.0)
    out = pd.DataFrame({
        "p_any_recovery": p_any,
        "recovery_fraction_if_recovered": frac_if_recovered,
        "recovery_fraction_pred": np.clip(p_any * frac_if_recovered, 0.0, 1.0),
    }, index=df.index)
    return out


def evaluate_stage_c_recovery_model(
    model: StageCRecoveryModel,
    model_base: pd.DataFrame,
    class_col: str = "payoff_type_collapsed",
    holdout_col: str = "is_holdout",
    eligible_col: str = "is_training_eligible",
) -> dict:
    """Evaluate the Stage C recovery model on the matured holdout slice."""
    from sklearn.metrics import log_loss, mean_absolute_error

    hold = model_base[
        (model_base[eligible_col] == True)  # noqa: E712
        & model_base[holdout_col].astype(bool)
        & (model_base["outstanding_at_default"] > 0)
        & (model_base["days_since_default"] >= model.min_days_since_default)
        & (model_base[class_col].isin(_XPD_CLASSES))
    ].copy()
    if hold.empty:
        return {"holdout_rows": 0, "classifier_log_loss": float("nan"), "recovery_fraction_mae": float("nan")}

    preds = predict_stage_c_recovery(model, hold)
    y_any = hold["any_recovery"].astype(int).values
    clf_ll = float(log_loss(y_any, preds["p_any_recovery"].clip(1e-6, 1 - 1e-6))) if len(np.unique(y_any)) > 1 else float("nan")
    frac_mae = float(mean_absolute_error(hold["recovery_fraction"], preds["recovery_fraction_pred"]))

    joined = hold[["LoanID", class_col, "LoanStatus", "outstanding_at_default", "recovery_realized", "recovery_fraction", "any_recovery"]].join(preds)
    joined["pred_recovery_amount"] = joined["recovery_fraction_pred"] * joined["outstanding_at_default"]
    by_class = (
        joined.groupby(class_col)
        .agg(
            loans=("LoanID", "nunique"),
            obs_any_recovery=("any_recovery", "mean"),
            pred_any_recovery=("p_any_recovery", "mean"),
            obs_recovery_fraction=("recovery_fraction", "mean"),
            pred_recovery_fraction=("recovery_fraction_pred", "mean"),
            obs_recovery_amount=("recovery_realized", "mean"),
            pred_recovery_amount=("pred_recovery_amount", "mean"),
        )
        .reset_index()
    )
    return {
        "holdout_rows": int(len(hold)),
        "classifier_log_loss": clf_ll,
        "recovery_fraction_mae": frac_mae,
        "predictions": joined,
        "by_class": by_class,
    }


def build_stage_c_recovery_fraction_matrix(
    model: StageCRecoveryModel,
    holdout_loans: pd.DataFrame,
    payin_matrix: pd.DataFrame,
    model_base: pd.DataFrame,
    class_col: str = "payoff_type_collapsed",
) -> pd.DataFrame:
    """Return a per-loan/class recovery matrix for `apply_stage_c_recovery`.

    Loans present in the Stage C model base use the model prediction; other
    loans retain the class-level empirical fallback. Clean remains pinned to
    zero recovery.
    """
    matrix = broadcast_class_recovery_to_matrix(model.fallback_by_class, like=payin_matrix)
    xpd_cols = [c for c in _XPD_CLASSES if c in matrix.columns]

    pred_base = model_base[model_base["LoanID"].isin(payin_matrix.index)].copy()
    pred_base = pred_base[pred_base[class_col].isin(_XPD_CLASSES)]
    if not pred_base.empty:
        preds = predict_stage_c_recovery(model, pred_base)
        loan_pred = pd.Series(preds["recovery_fraction_pred"].values, index=pred_base["LoanID"])
        loan_pred = loan_pred[~loan_pred.index.duplicated(keep="first")]
        common = matrix.index.intersection(loan_pred.index)
        matrix.loc[common, xpd_cols] = loan_pred.reindex(common).values[:, None]

    for cls in matrix.columns:
        if cls not in _XPD_CLASSES:
            matrix[cls] = 0.0
    return matrix.clip(0.0, 1.0)
