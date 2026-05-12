"""Payment-attempt history features for Stage B."""
from __future__ import annotations

import numpy as np
import pandas as pd


PAYMENT_HISTORY_FEATURES = [
    "hist_payment_count_lag",
    "hist_success_count_lag",
    "hist_fail_count_lag",
    "hist_success_rate_lag",
    "hist_total_amount_lag",
    "hist_avg_amount_lag",
    "hist_last_payment_amount_lag",
    "log_hist_last_payment_amount_lag",
    "hist_last_attempt_was_success_lag",
    "hist_last_attempt_was_fail_lag",
    "hist_days_since_last_payment_lag",
    "hist_days_since_last_success_lag",
    "hist_fail_streak_lag",
    "hist_recent_fail_count_last_3_lag",
    "due_day_of_week",
    "due_day_of_month",
    "due_is_month_end",
]


_STATE_FEATURES = [
    "hist_payment_count_lag",
    "hist_success_count_lag",
    "hist_fail_count_lag",
    "hist_success_rate_lag",
    "hist_total_amount_lag",
    "hist_avg_amount_lag",
    "hist_last_payment_amount_lag",
    "log_hist_last_payment_amount_lag",
    "hist_last_attempt_was_success_lag",
    "hist_last_attempt_was_fail_lag",
    "hist_fail_streak_lag",
    "hist_recent_fail_count_last_3_lag",
]


def _consecutive_fail_streak(is_fail: pd.Series) -> pd.Series:
    streak = []
    run = 0
    for flag in is_fail.astype(int):
        run = run + 1 if flag == 1 else 0
        streak.append(run)
    return pd.Series(streak, index=is_fail.index, dtype="float64")


def _empty_features(seq_base: pd.DataFrame, due_date_col: str) -> pd.DataFrame:
    out = pd.DataFrame(0.0, index=seq_base.index, columns=PAYMENT_HISTORY_FEATURES)
    due = pd.to_datetime(seq_base[due_date_col], errors="coerce")
    out["due_day_of_week"] = due.dt.dayofweek.fillna(0).astype(int)
    out["due_day_of_month"] = due.dt.day.fillna(0).astype(int)
    out["due_is_month_end"] = due.dt.is_month_end.fillna(False).astype(int)
    return out


def build_payment_history_features(
    seq_base: pd.DataFrame,
    payment_df: pd.DataFrame,
    loan_col: str = "LoanID",
    installment_col: str = "InstallmentNumber",
    due_date_col: str = "InstallmentDueDate",
) -> pd.DataFrame:
    """Return leakage-safe payment-attempt history at the seq_base grain.

    For installment k, the returned features use attempts from prior normal
    installments only (`payment.InstallmentNumber < k`). Consecutive-fail and
    recent-fail features are computed at attempt grain, then carried forward to
    the installment rows.
    """
    required_seq = {loan_col, installment_col, due_date_col}
    required_payment = {
        loan_col,
        installment_col,
        "PaymentID",
        "AttemptNo",
        "PaymentDate",
        "TransactionDate",
        "PaymentAmount",
        "IsSuccess",
        "IsFail",
    }
    missing_seq = sorted(required_seq - set(seq_base.columns))
    if missing_seq:
        raise ValueError(f"seq_base missing payment feature columns: {missing_seq}")
    if payment_df.empty:
        return _empty_features(seq_base, due_date_col)
    missing_payment = sorted(required_payment - set(payment_df.columns))
    if missing_payment:
        raise ValueError(f"payment_df missing expected columns: {missing_payment}")

    p = payment_df.copy()
    p = p.dropna(subset=[loan_col, installment_col])
    p[installment_col] = pd.to_numeric(p[installment_col], errors="coerce")
    p = p.dropna(subset=[installment_col])
    p[installment_col] = p[installment_col].astype(int)

    for c in ["PaymentID", "AttemptNo", "IsSuccess", "IsFail"]:
        p[c] = pd.to_numeric(p[c], errors="coerce").fillna(0).astype(int)
    p["PaymentAmount"] = pd.to_numeric(p["PaymentAmount"], errors="coerce").fillna(0.0)
    p["PaymentDate"] = pd.to_datetime(p["PaymentDate"], errors="coerce")
    p["TransactionDate"] = pd.to_datetime(p["TransactionDate"], errors="coerce")

    order_cols = [loan_col, installment_col, "PaymentDate", "TransactionDate", "AttemptNo", "PaymentID"]
    p = p.sort_values(order_cols).reset_index(drop=True)
    g = p.groupby(loan_col, sort=False)

    p["_attempt_count"] = g.cumcount() + 1
    p["_success_count"] = g["IsSuccess"].cumsum()
    p["_fail_count"] = g["IsFail"].cumsum()
    p["_total_amount"] = g["PaymentAmount"].cumsum()
    p["_last_success_date"] = p["PaymentDate"].where(p["IsSuccess"].eq(1))
    p["_last_success_date"] = p.groupby(loan_col, sort=False)["_last_success_date"].ffill()
    p["_fail_streak"] = g["IsFail"].transform(_consecutive_fail_streak)
    p["_recent_fail_count_last_3"] = g["IsFail"].transform(
        lambda s: s.rolling(3, min_periods=1).sum()
    )

    p["hist_payment_count_lag"] = p["_attempt_count"].astype(float)
    p["hist_success_count_lag"] = p["_success_count"].astype(float)
    p["hist_fail_count_lag"] = p["_fail_count"].astype(float)
    p["hist_success_rate_lag"] = p["_success_count"] / p["_attempt_count"].clip(lower=1)
    p["hist_total_amount_lag"] = p["_total_amount"].astype(float)
    p["hist_avg_amount_lag"] = p["_total_amount"] / p["_attempt_count"].clip(lower=1)
    p["hist_last_payment_amount_lag"] = p["PaymentAmount"].astype(float)
    p["log_hist_last_payment_amount_lag"] = np.log1p(p["hist_last_payment_amount_lag"].clip(lower=0.0))
    p["hist_last_attempt_was_success_lag"] = p["IsSuccess"].astype(float)
    p["hist_last_attempt_was_fail_lag"] = p["IsFail"].astype(float)
    p["hist_fail_streak_lag"] = p["_fail_streak"].astype(float)
    p["hist_recent_fail_count_last_3_lag"] = p["_recent_fail_count_last_3"].astype(float)

    state = (
        p.groupby([loan_col, installment_col], sort=False)
        .tail(1)
        [[loan_col, installment_col, *_STATE_FEATURES, "PaymentDate", "_last_success_date"]]
        .rename(columns={
            "PaymentDate": "_hist_last_payment_date_lag",
            "_last_success_date": "_hist_last_success_date_lag",
        })
    )

    seq = seq_base[[loan_col, installment_col, due_date_col]].copy()
    seq["_row_order"] = np.arange(len(seq))
    seq[due_date_col] = pd.to_datetime(seq[due_date_col], errors="coerce")
    seq[installment_col] = pd.to_numeric(seq[installment_col], errors="coerce").astype(int)

    merged = seq.merge(state, on=[loan_col, installment_col], how="left")
    merged = merged.sort_values([loan_col, installment_col, "_row_order"])
    fill_cols = _STATE_FEATURES + ["_hist_last_payment_date_lag", "_hist_last_success_date_lag"]

    for c in fill_cols:
        merged[c] = merged.groupby(loan_col, sort=False)[c].ffill()
        merged[c] = merged.groupby(loan_col, sort=False)[c].shift(1)

    for c in _STATE_FEATURES:
        merged[c] = merged[c].fillna(0.0)

    merged["hist_days_since_last_payment_lag"] = (
        merged[due_date_col] - merged["_hist_last_payment_date_lag"]
    ).dt.days.fillna(0).clip(lower=0)
    merged["hist_days_since_last_success_lag"] = (
        merged[due_date_col] - merged["_hist_last_success_date_lag"]
    ).dt.days.fillna(0).clip(lower=0)
    merged["due_day_of_week"] = merged[due_date_col].dt.dayofweek.fillna(0).astype(int)
    merged["due_day_of_month"] = merged[due_date_col].dt.day.fillna(0).astype(int)
    merged["due_is_month_end"] = merged[due_date_col].dt.is_month_end.fillna(False).astype(int)

    out = merged.sort_values("_row_order")[PAYMENT_HISTORY_FEATURES].reset_index(drop=True)
    out.index = seq_base.index
    return out.astype(float)


def append_payment_history_features(seq_base: pd.DataFrame, payment_df: pd.DataFrame) -> pd.DataFrame:
    """Return seq_base with payment-attempt history columns appended."""
    out = seq_base.copy()
    features = build_payment_history_features(out, payment_df)
    for c in PAYMENT_HISTORY_FEATURES:
        out[c] = features[c].values
    return out
