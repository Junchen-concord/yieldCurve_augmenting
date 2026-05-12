"""Reusable risk-band feature engineering for projection models."""
from __future__ import annotations

import pandas as pd


DM_RISK_TIER_MAP = {
    "DM01": "high",
    "DM02": "high",
    "DM03": "high",
    "DM04": "med",
    "DM05": "med",
    "DM06": "med",
    "DM07": "low",
    "DM08": "low",
    "UNKNOWN": "unknown",
}

DM_RISK_FEATURES = [
    "DM_risk_high",
    "DM_risk_med",
    "DM_risk_low",
    "DM_risk_unknown",
]


def normalize_dm_band(series: pd.Series) -> pd.Series:
    """Normalize raw DM band labels before mapping to generic risk tiers."""
    return (
        series.where(series.notna(), "UNKNOWN")
        .astype(str)
        .str.upper()
        .str.strip()
        .replace({"": "UNKNOWN", "NAN": "UNKNOWN", "NONE": "UNKNOWN"})
    )


def add_dm_risk_tier_features(
    df: pd.DataFrame,
    band_col: str = "DM_Band_Name",
    tier_col: str = "DM_risk_tier",
) -> pd.DataFrame:
    """Add hardcoded business DM risk-tier features.

    Contract:
      - DM01/DM02/DM03 -> high risk
      - DM04/DM05/DM06 -> medium risk
      - DM07/DM08      -> low risk
      - missing/unseen -> unknown
    """
    if band_col not in df.columns:
        raise KeyError(f"Missing required DM band column: {band_col}")

    out = df.copy()
    dm_band = normalize_dm_band(out[band_col])
    out[band_col] = dm_band
    out[tier_col] = dm_band.map(DM_RISK_TIER_MAP).fillna("unknown")

    for tier in ["high", "med", "low", "unknown"]:
        out[f"DM_risk_{tier}"] = out[tier_col].eq(tier).astype(int)

    return out
