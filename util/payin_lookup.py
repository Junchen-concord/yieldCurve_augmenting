"""Historical payin lookup table — day-zero baseline projection.

Builds a tiered $-weighted lookup of `payin_ratio_realized` over groups like
(DM_Band_Name, CM_Band_Name, CustType, PortFolioID, AppMonth, AppWeek,
Frequency_group3), with a fallback hierarchy for thin cells.

Intended use:
  1. Pull #t_lookup (loan-level extract).
  2. Split by OriginationDate cutoff into train / test.
  3. build_lookup_table(train, ...) -> LookupTable
  4. apply_lookup(test, lookup) -> per-loan expected payin + fallback tier
  5. score_cohort(...) -> cohort-level (CustType, AppYear, AppWeek) summary
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Frequency 3-group collapse
# Business 3-group rule: W stays W; B and S collapse to B; M stays M.
# ---------------------------------------------------------------------------
FREQUENCY_GROUP3_MAP = {
    "W": "W",
    "B": "B",
    "S": "B",
    "M": "M",
}


def assign_frequency_group3(freq: pd.Series) -> pd.Series:
    """Collapse raw Frequency (W/B/S/M) into the 3-group bucket (W/B/M).

    Anything else -> 'UNKNOWN'. Upstream QC should report UNKNOWN rate.
    """
    s = freq.astype(str).str.strip().str.upper()
    return s.map(FREQUENCY_GROUP3_MAP).fillna("UNKNOWN")


# ---------------------------------------------------------------------------
# Payin ratio
# ---------------------------------------------------------------------------
def compute_payin_ratio(
    df: pd.DataFrame,
    payment_col: str = "TotalRealizedPayment",
    orig_col: str = "OriginatedAmount",
) -> pd.Series:
    """payin_ratio_realized = TotalRealizedPayment / OriginatedAmount.

    Mirrors projection_feature_builder.py:243 so the lookup numerator/denominator
    matches what the model is being compared against.
    """
    num = pd.to_numeric(df[payment_col], errors="coerce").fillna(0.0)
    den = pd.to_numeric(df[orig_col], errors="coerce").replace(0, np.nan)
    ratio = (num / den).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return ratio


# ---------------------------------------------------------------------------
# Weighted mean helper
# ---------------------------------------------------------------------------
def _weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    v = pd.to_numeric(values, errors="coerce").fillna(0.0)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    total_w = float(w.sum())
    return float((v * w).sum() / total_w) if total_w > 0 else float("nan")


def _weighted_std(values: pd.Series, weights: pd.Series) -> float:
    """$-weighted population std of payin within a cell.

    Used as the dispersion input for the baseline confidence band: a cell's
    standard error of the mean is approximately ``std / sqrt(n)``, and that
    (systematic, shared-across-loans) uncertainty is what keeps the blended
    portfolio CI from collapsing under diversification.
    """
    v = pd.to_numeric(values, errors="coerce").fillna(0.0)
    w = pd.to_numeric(weights, errors="coerce").fillna(0.0).clip(lower=0.0)
    total_w = float(w.sum())
    if total_w <= 0:
        return float("nan")
    mu = float((v * w).sum() / total_w)
    var = float((w * (v - mu) ** 2).sum() / total_w)
    return float(np.sqrt(max(var, 0.0)))


# ---------------------------------------------------------------------------
# Single-tier aggregation
# ---------------------------------------------------------------------------
def _build_single_tier(
    df: pd.DataFrame,
    group_cols: list[str],
    payin_col: str,
    orig_col: str,
    min_n: int,
) -> pd.DataFrame:
    """Aggregate to one row per cell. Cells with n < min_n are dropped."""
    if not group_cols:
        # Global fallback: one row with the overall weighted mean.
        return pd.DataFrame([{
            "expected_payin": _weighted_mean(df[payin_col], df[orig_col]),
            "payin_std": _weighted_std(df[payin_col], df[orig_col]),
            "n_loans": int(len(df)),
            "orig_total": float(pd.to_numeric(df[orig_col], errors="coerce").fillna(0).sum()),
        }])

    rows = []
    for key, sub in df.groupby(group_cols, dropna=False, sort=False):
        if not isinstance(key, tuple):
            key = (key,)
        n = int(len(sub))
        if n < min_n:
            continue
        rows.append({
            **dict(zip(group_cols, key)),
            "expected_payin": _weighted_mean(sub[payin_col], sub[orig_col]),
            "payin_std": _weighted_std(sub[payin_col], sub[orig_col]),
            "n_loans": n,
            "orig_total": float(pd.to_numeric(sub[orig_col], errors="coerce").fillna(0).sum()),
        })
    return pd.DataFrame(rows, columns=[*group_cols, "expected_payin", "payin_std", "n_loans", "orig_total"])


# ---------------------------------------------------------------------------
# Tiered LookupTable
# ---------------------------------------------------------------------------
@dataclass
class LookupTable:
    """Tiered lookup with a progressive-fallback key hierarchy.

    tiers[0]   = most specific (full primary_keys).
    tiers[-1]  = coarsest fallback (or global mean if drop order empties keys).
    tier_keys[i] = list of group cols active at tier i.
    """
    tiers: list[pd.DataFrame] = field(default_factory=list)
    tier_keys: list[list[str]] = field(default_factory=list)
    min_n: int = 50

    def summary(self) -> pd.DataFrame:
        rows = []
        for i, (t, k) in enumerate(zip(self.tiers, self.tier_keys)):
            rows.append({
                "tier": i,
                "keys": ", ".join(k) if k else "(global)",
                "cells": int(len(t)),
                "loans_total": int(t["n_loans"].sum()) if not t.empty else 0,
            })
        return pd.DataFrame(rows)


def build_lookup_table(
    train_df: pd.DataFrame,
    primary_keys: list[str],
    fallback_drop_order: list[str],
    payin_col: str = "payin_ratio_realized",
    orig_col: str = "OriginatedAmount",
    min_n: int = 50,
) -> LookupTable:
    """Build a tiered lookup. Each successive tier drops one key in
    `fallback_drop_order` until the list is exhausted. A final global tier
    is always added so every loan resolves to something.

    Parameters
    ----------
    train_df : matured loans (one row per loan), already pre-cleaned.
    primary_keys : most-specific group columns.
    fallback_drop_order : keys to drop, in order, to coarsen the lookup.
        Example: ['AppWeek', 'AppMonth', 'CM_Band_Name', 'PortFolioID',
                  'Frequency_group3', 'DM_Band_Name']
    payin_col : the y to average ($-weighted).
    orig_col : the $ weight column.
    min_n : minimum loans per cell. Cells thinner than this are dropped at
        that tier and the loan falls through to the next coarser tier.
    """
    tiers: list[pd.DataFrame] = []
    tier_keys: list[list[str]] = []
    keys = list(primary_keys)

    # Tier 0 — most specific
    tiers.append(_build_single_tier(train_df, keys, payin_col, orig_col, min_n))
    tier_keys.append(list(keys))

    # Progressive fallbacks
    for drop_key in fallback_drop_order:
        if drop_key in keys:
            keys = [k for k in keys if k != drop_key]
        tiers.append(_build_single_tier(train_df, keys, payin_col, orig_col, min_n))
        tier_keys.append(list(keys))
        if not keys:
            break

    # Always end with a global tier (no key constraint, no min_n).
    if tier_keys[-1]:
        tiers.append(_build_single_tier(train_df, [], payin_col, orig_col, min_n=0))
        tier_keys.append([])

    return LookupTable(tiers=tiers, tier_keys=tier_keys, min_n=min_n)


# ---------------------------------------------------------------------------
# Apply lookup
# ---------------------------------------------------------------------------
def apply_lookup(
    loans: pd.DataFrame,
    lookup: LookupTable,
) -> pd.DataFrame:
    """For each loan, take the most specific tier that has a matching cell.

    Returns the input frame with three new columns:
      - expected_payin   : the cell's $-weighted historical mean payin.
      - lookup_tier      : 0 = most specific tier; higher = more fallback.
      - lookup_n         : N loans in the training cell that resolved it.
    """
    out = loans.copy()
    out["expected_payin"] = np.nan
    out["lookup_std"] = np.nan
    out["lookup_tier"] = np.nan
    out["lookup_n"] = np.nan

    remaining = out.index.copy()

    for tier_level, (tier_df, tier_key) in enumerate(zip(lookup.tiers, lookup.tier_keys)):
        if len(remaining) == 0:
            break
        if tier_df is None or tier_df.empty:
            continue

        has_std = "payin_std" in tier_df.columns

        if not tier_key:
            # Global tier: assign the single mean to all remaining.
            row = tier_df.iloc[0]
            out.loc[remaining, "expected_payin"] = float(row["expected_payin"])
            out.loc[remaining, "lookup_std"] = float(row["payin_std"]) if has_std else np.nan
            out.loc[remaining, "lookup_tier"] = tier_level
            out.loc[remaining, "lookup_n"] = int(row["n_loans"])
            remaining = remaining.difference(remaining)
            break

        value_cols = ["expected_payin", "n_loans"] + (["payin_std"] if has_std else [])
        sub = out.loc[remaining, tier_key].reset_index()
        merged = sub.merge(
            tier_df[[*tier_key, *value_cols]],
            on=tier_key,
            how="left",
        )
        matched = merged[merged["expected_payin"].notna()].set_index("index")
        if matched.empty:
            continue
        out.loc[matched.index, "expected_payin"] = matched["expected_payin"].astype(float).values
        if has_std:
            out.loc[matched.index, "lookup_std"] = matched["payin_std"].astype(float).values
        out.loc[matched.index, "lookup_tier"] = tier_level
        out.loc[matched.index, "lookup_n"] = matched["n_loans"].astype(int).values
        remaining = remaining.difference(matched.index)

    return out


# ---------------------------------------------------------------------------
# Cohort scoring
# ---------------------------------------------------------------------------
def score_cohort(
    scored_loans: pd.DataFrame,
    cohort_cols: list[str],
    payin_pred_col: str = "expected_payin",
    weight_col: str = "OriginatedAmount",
    realized_col: Optional[str] = "payin_ratio_realized",
) -> pd.DataFrame:
    """Aggregate per-loan lookup outputs to cohort-level $-weighted projections.

    Returns columns:
        *cohort_cols, n_loans, orig_total,
        projected_payin,
        realized_payin (if realized_col present),
        pct_unmatched, mean_tier, max_tier
    """
    rows = []
    for key, sub in scored_loans.groupby(cohort_cols, dropna=False, sort=True):
        if not isinstance(key, tuple):
            key = (key,)
        row = dict(zip(cohort_cols, key))
        row["n_loans"] = int(len(sub))
        row["orig_total"] = float(pd.to_numeric(sub[weight_col], errors="coerce").fillna(0).sum())
        row["projected_payin"] = _weighted_mean(sub[payin_pred_col], sub[weight_col])
        if realized_col is not None and realized_col in sub.columns:
            row["realized_payin"] = _weighted_mean(sub[realized_col], sub[weight_col])
        if "lookup_tier" in sub.columns:
            row["pct_unmatched"] = float(sub["lookup_tier"].isna().mean())
            row["mean_tier"] = float(sub["lookup_tier"].mean())
            row["max_tier"] = float(sub["lookup_tier"].max())
        rows.append(row)
    return pd.DataFrame(rows)
