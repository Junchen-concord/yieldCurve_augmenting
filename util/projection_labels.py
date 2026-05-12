"""Label helpers for the payin-projection pipeline."""
from __future__ import annotations

import pandas as pd


# 5-class modeled target for Stage A.
# Merges 4PD/5PD into LatePD and Clean_early/Clean_full into Clean.
# DENY_NEW / ALL_VOIDED remain raw diagnostic labels upstream, but are not
# modeled because they are operational edge cases rather than payin outcomes.
PAYOFF_TYPE_COLLAPSED_ORDER = [
    "FPD",
    "SPD",
    "TPD",
    "LatePD",
    "Clean",
]

_COLLAPSE_MAP = {
    "FPD": "FPD",
    "SPD": "SPD",
    "TPD": "TPD",
    "4PD": "LatePD",
    "5PD": "LatePD",
    "LatePD": "LatePD",
    "Clean_early": "Clean",
    "Clean_full": "Clean",
    # Immature, DENY_NEW, and ALL_VOIDED are intentionally NOT in the collapse
    # map; callers should filter them out via is_training_eligible before
    # invoking collapse_payoff_type for modeling.
}


def collapse_payoff_type(payoff_type: pd.Series) -> pd.Series:
    """Collapse the raw payoff_type label into the 5-class modeled target.

    Unknown / immature / operational edge labels map to NaN so they stand out
    in downstream checks.
    """
    return payoff_type.map(_COLLAPSE_MAP)


def encode_payoff_type_int(payoff_type_collapsed: pd.Series) -> pd.Series:
    """Encode the 5-class string target to 0..4 integer codes (XGBoost-friendly)."""
    cat = pd.Categorical(payoff_type_collapsed, categories=PAYOFF_TYPE_COLLAPSED_ORDER, ordered=False)
    return pd.Series(cat.codes, index=payoff_type_collapsed.index, name="payoff_type_code")


def decode_payoff_type_int(codes: pd.Series | pd.Index) -> pd.Series:
    """Inverse of encode_payoff_type_int."""
    codes = pd.Series(codes) if not isinstance(codes, pd.Series) else codes
    return codes.map(dict(enumerate(PAYOFF_TYPE_COLLAPSED_ORDER)))
