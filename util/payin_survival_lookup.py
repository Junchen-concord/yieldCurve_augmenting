"""Survival x payin lookup — the v2 lookup with one new key: status at installment k.

Extends the day-zero lookup (util/payin_lookup.py) with the loan's observed
installment status. For matured loans grouped by (CustType, Frequency_group3),
and for each k = 0..K, every loan is classified into a status:

    default@j   earliest installment default was at j <= k   (FPD/SPD/TPD/...)
    paid_off    loan paid off by installment k
    alive       collected through k, not paid off, not defaulted

The table stores, per (segment, k, status):
    share          state mix — Michael's survival table
    expected_payin $-weighted mean realized final payin for that state
    q05/q50/q95    loan-level quantiles of final payin (the confidence band)

Live application: classify each live loan's status at its OWN settled-installment
count (due date <= as_of, no settlement lag), look up the cell, aggregate to
cohort. The band narrows with k exactly as fast as history says it should —
no simulation, one-sentence explanation:
"loans that looked like yours at this point historically finished here."
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SEGMENT_COLS = ["CustType", "Frequency_group3"]
STATUS_ALIVE = "alive"
STATUS_PAID_OFF = "paid_off"


def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").fillna(0.0)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    total = float(w.sum())
    return float((v * w).sum() / total) if total > 0 else float("nan")


def build_loan_summary(
    install_df: pd.DataFrame,
    as_of_date=None,
    default_col: str = "isInstallDefault",
    payoff_col: str = "LoanPaidOffThisInstall",
) -> pd.DataFrame:
    """One row per loan from normal-stream installment rows (iPaymentMode == 144).

    Returns: LoanID, segment cols, OriginatedAmount, OriginationDate, AppYear,
    AppWeek, final_payin (realized, only meaningful for matured loans),
    earliest_default_inst (NaN if none), payoff_inst (NaN if none),
    n_settled (installments with due date <= as_of), TotalInstallsNumber.
    """
    df = install_df.copy()
    if "iPaymentMode" in df.columns:
        df = df[pd.to_numeric(df["iPaymentMode"], errors="coerce") == 144]
    df["InstallmentNumber"] = pd.to_numeric(df["InstallmentNumber"], errors="coerce")
    df = df.dropna(subset=["LoanID", "InstallmentNumber"])
    df["InstallmentNumber"] = df["InstallmentNumber"].astype(int)
    for c in [default_col, payoff_col]:
        df[c] = pd.to_numeric(df.get(c), errors="coerce").fillna(0).astype(int)
    df["InstallmentDueDate"] = pd.to_datetime(df.get("InstallmentDueDate"), errors="coerce")

    as_of = pd.to_datetime(as_of_date) if as_of_date is not None else None
    if as_of is not None:
        df["_settled"] = df["InstallmentDueDate"].le(as_of).fillna(False)
    else:
        df["_settled"] = True

    first_default = (
        df.loc[df[default_col] == 1]
        .groupby("LoanID")["InstallmentNumber"].min()
        .rename("earliest_default_inst")
    )
    first_payoff = (
        df.loc[df[payoff_col] == 1]
        .groupby("LoanID")["InstallmentNumber"].min()
        .rename("payoff_inst")
    )
    n_settled = (
        df.loc[df["_settled"]]
        .groupby("LoanID")["InstallmentNumber"].max()
        .rename("n_settled")
    )

    loan_cols = {
        "OriginatedAmount": "first",
        "OriginationDate": "first",
        "TotalRealizedPayment": "first",
        "AppYear": "first",
        "AppWeek": "first",
        "TotalInstallsNumber": "max",
        "CustType": "first",
        "Frequency_group3": "first",
    }
    present = {k: v for k, v in loan_cols.items() if k in df.columns}
    out = df.groupby("LoanID", as_index=False).agg(**{k: (k, v) for k, v in present.items()})
    out = (
        out.merge(first_default, on="LoanID", how="left")
        .merge(first_payoff, on="LoanID", how="left")
        .merge(n_settled, on="LoanID", how="left")
    )
    out["n_settled"] = pd.to_numeric(out["n_settled"], errors="coerce").fillna(0).astype(int)
    out["OriginatedAmount"] = pd.to_numeric(out["OriginatedAmount"], errors="coerce")
    out["final_payin"] = (
        pd.to_numeric(out.get("TotalRealizedPayment"), errors="coerce").fillna(0.0)
        / out["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out


def status_at_k(loan_summary: pd.DataFrame, k: int) -> pd.Series:
    """Vectorized status at installment k. Priority: default < paid_off < alive.

    k = 0 returns 'alive' for every loan (day-zero — nothing observed yet).
    """
    d = pd.to_numeric(loan_summary["earliest_default_inst"], errors="coerce")
    p = pd.to_numeric(loan_summary["payoff_inst"], errors="coerce")
    out = pd.Series(STATUS_ALIVE, index=loan_summary.index)
    if k <= 0:
        return out
    defaulted = d.notna() & (d <= k)
    paid = ~defaulted & p.notna() & (p <= k)
    out[paid] = STATUS_PAID_OFF
    out[defaulted] = "default@" + d[defaulted].astype(int).astype(str)
    return out


def build_survival_payin_table(
    matured: pd.DataFrame,
    ks: range = range(0, 6),
    segment_cols: list[str] = None,
    min_n: int = 50,
) -> pd.DataFrame:
    """The survival x payin table from matured loans (final_payin fully realized).

    One row per (segment, k, status): share (state mix at k), n_loans,
    expected_payin ($-weighted), q05/q50/q95, band_width.
    Cells with n < min_n are kept but flagged thin=True (callers decide).
    """
    segment_cols = segment_cols or SEGMENT_COLS
    rows = []
    for seg_key, seg in matured.groupby(segment_cols, dropna=False):
        if not isinstance(seg_key, tuple):
            seg_key = (seg_key,)
        seg_n = len(seg)
        for k in ks:
            st = status_at_k(seg, k)
            for status, sub_idx in st.groupby(st).groups.items():
                sub = seg.loc[sub_idx]
                fp = sub["final_payin"].astype(float)
                rows.append({
                    **dict(zip(segment_cols, seg_key)),
                    "k": int(k),
                    "status": status,
                    "n_loans": int(len(sub)),
                    "share": float(len(sub) / seg_n) if seg_n else np.nan,
                    "expected_payin": _weighted_mean(fp, sub["OriginatedAmount"]),
                    "q05": float(fp.quantile(0.05)),
                    "q50": float(fp.quantile(0.50)),
                    "q95": float(fp.quantile(0.95)),
                    "thin": bool(len(sub) < min_n),
                })
    table = pd.DataFrame(rows)
    table["band_width"] = table["q95"] - table["q05"]
    return table


def apply_survival_lookup(
    loans: pd.DataFrame,
    table: pd.DataFrame,
    segment_cols: list[str] = None,
    k_cap: int = None,
) -> pd.DataFrame:
    """Per-loan lookup at each loan's own observed k (= n_settled, capped).

    Fallback for unmatched / thin cells: same (k, status) pooled across segments,
    then the segment's k=0 'alive' cell (day-zero baseline), then global mean.
    Adds: status_k (the k used), status, expected_payin, q05, q95, lookup_level.
    """
    segment_cols = segment_cols or SEGMENT_COLS
    if k_cap is None:
        k_cap = int(table["k"].max())
    out = loans.copy()
    out["status_k"] = pd.to_numeric(out["n_settled"], errors="coerce").fillna(0).astype(int).clip(0, k_cap)
    # status evaluated at the loan's own k
    statuses = pd.Series(STATUS_ALIVE, index=out.index)
    for k in sorted(out["status_k"].unique()):
        mask = out["status_k"] == k
        statuses[mask] = status_at_k(out[mask], int(k)).values
    out["status"] = statuses

    good = table[~table["thin"]]
    primary = good.set_index([*segment_cols, "k", "status"])[["expected_payin", "q05", "q95"]]
    pooled_rows = {}
    for pk, g in table.groupby(["k", "status"]):
        pooled_rows[pk] = pd.Series({
            "expected_payin": _weighted_mean(g["expected_payin"], g["n_loans"]),
            "q05": float(np.average(g["q05"], weights=g["n_loans"])),
            "q95": float(np.average(g["q95"], weights=g["n_loans"])),
        })
    pooled = pd.DataFrame(pooled_rows).T
    global_row = pooled.loc[(0, STATUS_ALIVE)] if (0, STATUS_ALIVE) in pooled.index else pooled.iloc[0]

    keys = list(zip(*[out[c] for c in segment_cols], out["status_k"], out["status"]))
    exp, q05, q95, level = [], [], [], []
    for key in keys:
        if key in primary.index:
            r = primary.loc[key]
            exp.append(r["expected_payin"]); q05.append(r["q05"]); q95.append(r["q95"]); level.append(0)
            continue
        pk = (key[-2], key[-1])
        if pk in pooled.index:
            r = pooled.loc[pk]
            exp.append(r["expected_payin"]); q05.append(r["q05"]); q95.append(r["q95"]); level.append(1)
            continue
        exp.append(global_row["expected_payin"]); q05.append(global_row["q05"]); q95.append(global_row["q95"]); level.append(2)

    out["expected_payin"] = exp
    out["q05"] = q05
    out["q95"] = q95
    out["lookup_level"] = level
    # A loan's projection can never sit below what it already paid in.
    realized = out["final_payin"].astype(float).clip(lower=0.0)
    out["expected_payin"] = np.maximum(out["expected_payin"], realized)
    out["q95"] = np.maximum(out["q95"], out["expected_payin"])
    out["q05"] = np.minimum(np.maximum(out["q05"], 0.0), out["expected_payin"])
    return out


def score_cohorts(
    scored: pd.DataFrame,
    cohort_cols: list[str],
    weight_col: str = "OriginatedAmount",
) -> pd.DataFrame:
    """$-weighted cohort rollup of the per-loan survival-lookup output."""
    rows = []
    for key, sub in scored.groupby(cohort_cols, dropna=False, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        w = sub[weight_col]
        rows.append({
            **dict(zip(cohort_cols, key)),
            "n_loans": int(len(sub)),
            "orig_total": float(pd.to_numeric(w, errors="coerce").fillna(0).sum()),
            "realized_payin": _weighted_mean(sub["final_payin"], w),
            "projected_payin": _weighted_mean(sub["expected_payin"], w),
            "band_lo": _weighted_mean(sub["q05"], w),
            "band_hi": _weighted_mean(sub["q95"], w),
            "avg_k": _weighted_mean(sub["status_k"], w),
            "pct_alive": float((sub["status"] == STATUS_ALIVE).mean()),
            "pct_default": float(sub["status"].str.startswith("default").mean()),
            "pct_paid_off": float((sub["status"] == STATUS_PAID_OFF).mean()),
        })
    out = pd.DataFrame(rows)
    out["band_width"] = out["band_hi"] - out["band_lo"]
    return out
