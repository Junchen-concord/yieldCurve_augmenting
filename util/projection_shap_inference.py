"""Lightweight SHAP summaries for inference-time cohorts (persisted Stage A/B/C).

Uses the same feature matrices as ``score_live_projection``. Stage A explains the
persisted XGBoost multinomial head; Stage B the collection *classifier* on a
subsample of installment rows; Stage C the recovery *classifier* on defaulted
rows present in ``stage_c_features``. This is a local/cohort view of what the
frozen models emphasize on the current book, not a substitute for global
training diagnostics.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .plot_style import PRIMARY_COLOR, apply_plot_style
from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER


def _require_shap():
    try:
        import shap as shap_mod  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise ImportError("Install the `shap` package to use projection SHAP helpers.") from exc
    return shap_mod


def _float_feature_matrix(df: pd.DataFrame, cols: list[str]) -> tuple[pd.DataFrame, np.ndarray]:
    X = df[list(cols)].apply(pd.to_numeric, errors="coerce").replace([np.inf, -np.inf], np.nan)
    X = X.fillna(0.0)
    return X, X.to_numpy(dtype=np.float64)


def _mean_abs_shap_multiclass(shap_values: np.ndarray) -> np.ndarray:
    """shap_values: (n_samples, n_features, n_classes) -> mean |SHAP| per feature."""
    return np.abs(shap_values).mean(axis=(0, 2))


def _mean_abs_shap_binary(shap_values) -> np.ndarray:
    """Return mean |SHAP| per feature for a binary LightGBM model."""
    sv = shap_values
    if isinstance(sv, list):
        # Older SHAP: [neg_class, pos_class]
        sv = sv[1] if len(sv) > 1 else sv[0]
    sv = np.asarray(sv)
    return np.abs(sv).mean(axis=0)


def compute_stage_a_mean_abs_shap(
    model_run,
    loan_features: pd.DataFrame,
    *,
    max_rows: Optional[int] = None,
    rng_seed: int = 42,
) -> pd.Series:
    shap_mod = _require_shap()
    feats = list(model_run.stage_a.features)
    df = loan_features.copy()
    if max_rows is not None and len(df) > max_rows:
        df = df.sample(int(max_rows), random_state=rng_seed)
    _, Xn = _float_feature_matrix(df, feats)
    explainer = shap_mod.TreeExplainer(model_run.stage_a.booster)
    sv = explainer.shap_values(Xn)
    sv = np.asarray(sv)
    if sv.ndim == 3:
        mag = _mean_abs_shap_multiclass(sv)
    else:
        mag = _mean_abs_shap_binary(sv)
    return pd.Series(mag, index=feats).sort_values(ascending=False)


def compute_stage_b_clf_mean_abs_shap(
    model_run,
    seq_features: pd.DataFrame,
    *,
    max_rows: int = 8000,
    rng_seed: int = 42,
) -> pd.Series:
    shap_mod = _require_shap()
    feats = list(model_run.stage_b.features)
    df = seq_features.copy()
    if len(df) == 0:
        return pd.Series(dtype=float)
    n = min(int(max_rows), len(df))
    if n < len(df):
        df = df.sample(n, random_state=rng_seed)
    _, Xn = _float_feature_matrix(df, feats)
    explainer = shap_mod.TreeExplainer(model_run.stage_b.clf)
    sv = explainer.shap_values(Xn)
    mag = _mean_abs_shap_binary(sv)
    return pd.Series(mag, index=feats).sort_values(ascending=False)


def compute_stage_c_clf_mean_abs_shap(
    model_run,
    stage_c_features: pd.DataFrame,
    *,
    max_rows: int = 3000,
    rng_seed: int = 42,
) -> pd.Series:
    shap_mod = _require_shap()
    feats = list(model_run.stage_c.features)
    df = stage_c_features.copy()
    if len(df) == 0:
        return pd.Series(dtype=float)
    n = min(int(max_rows), len(df))
    if n < len(df):
        df = df.sample(n, random_state=rng_seed)
    _, Xn = _float_feature_matrix(df, feats)
    explainer = shap_mod.TreeExplainer(model_run.stage_c.clf)
    sv = explainer.shap_values(Xn)
    mag = _mean_abs_shap_binary(sv)
    return pd.Series(mag, index=feats).sort_values(ascending=False)


def compute_stage_a_row_shap_for_class(
    model_run,
    loan_features: pd.DataFrame,
    loan_id: str,
    class_name: str = "Clean",
) -> pd.Series:
    """SHAP vector for one loan and one terminal class (default: Clean)."""
    shap_mod = _require_shap()
    if class_name not in PAYOFF_TYPE_COLLAPSED_ORDER:
        raise ValueError(f"class_name must be one of {PAYOFF_TYPE_COLLAPSED_ORDER}")
    cls_idx = PAYOFF_TYPE_COLLAPSED_ORDER.index(class_name)
    feats = list(model_run.stage_a.features)
    row = loan_features.loc[loan_features["LoanID"].astype(str).eq(str(loan_id))]
    if row.empty:
        raise KeyError(f"LoanID not in loan_features: {loan_id!r}")
    row = row.iloc[:1]
    _, Xn = _float_feature_matrix(row, feats)
    explainer = shap_mod.TreeExplainer(model_run.stage_a.booster)
    sv = np.asarray(explainer.shap_values(Xn))
    if sv.ndim != 3:
        raise RuntimeError("Unexpected Stage A SHAP shape; expected multiclass (1, n_features, n_classes).")
    vec = sv[0, :, cls_idx]
    out = pd.Series(vec, index=feats)
    order = np.argsort(-np.abs(out.to_numpy(dtype=float)))
    return out.iloc[order]


def _barh_top(ax, importance: pd.Series, title: str, top_k: int) -> None:
    s = importance.dropna().abs().sort_values(ascending=True).tail(int(top_k))
    if s.empty:
        ax.text(0.5, 0.5, "No rows", ha="center", va="center", transform=ax.transAxes)
        ax.set_axis_off()
        return
    ax.barh(s.index.astype(str), s.values, color=PRIMARY_COLOR, alpha=0.85)
    ax.set_xlabel("Mean |SHAP| (cohort)")
    ax.set_title(title, fontsize=11)


def plot_inference_shap_overview(
    model_run,
    results: dict,
    *,
    top_k: int = 10,
    max_stage_a_rows: Optional[int] = 15000,
    max_seq_rows: int = 8000,
    max_stage_c_rows: int = 3000,
    drill_loan_id: Optional[str] = None,
    drill_class: str = "Clean",
    rng_seed: int = 42,
) -> tuple[plt.Figure, Optional[plt.Figure]]:
    """Horizontal bar charts: Stage A (loan), Stage B clf (installment sample), Stage C clf (defaulted sample).

    Returns the main figure and an optional second figure for single-loan Stage A drill-down.
    """
    apply_plot_style()
    loan_features = results["loan_features"]
    seq_features = results["seq_features"]
    stage_c_features = results["stage_c_features"]

    imp_a = compute_stage_a_mean_abs_shap(
        model_run, loan_features, max_rows=max_stage_a_rows, rng_seed=rng_seed
    )
    imp_b = compute_stage_b_clf_mean_abs_shap(
        model_run, seq_features, max_rows=max_seq_rows, rng_seed=rng_seed
    )
    imp_c = compute_stage_c_clf_mean_abs_shap(
        model_run, stage_c_features, max_rows=max_stage_c_rows, rng_seed=rng_seed
    )

    fig, axes = plt.subplots(3, 1, figsize=(9, 10), constrained_layout=True)
    n_a = len(loan_features) if max_stage_a_rows is None else min(len(loan_features), max_stage_a_rows)
    _barh_top(
        axes[0],
        imp_a,
        f"Stage A — mean |SHAP| (n={n_a} loans)",
        top_k,
    )
    b_n = min(len(seq_features), max_seq_rows) if len(seq_features) else 0
    _barh_top(
        axes[1],
        imp_b,
        f"Stage B P(collect) — mean |SHAP| (n={b_n} installment rows, sampled)" if b_n else "Stage B — no installment rows",
        top_k,
    )
    c_n = min(len(stage_c_features), max_stage_c_rows) if len(stage_c_features) else 0
    _barh_top(
        axes[2],
        imp_c,
        f"Stage C P(recovery) — mean |SHAP| (n={c_n} defaulted rows, sampled)" if c_n else "Stage C — no defaulted-model rows",
        top_k,
    )
    fig.suptitle(
        "Inference cohort: which inputs move each persisted head most (mean |SHAP|)",
        fontsize=12,
    )

    fig2 = None
    if drill_loan_id is not None:
        row_shap = compute_stage_a_row_shap_for_class(
            model_run, loan_features, str(drill_loan_id), class_name=drill_class
        )
        fig2, ax = plt.subplots(figsize=(8, 5), constrained_layout=True)
        top = row_shap.head(top_k)
        top = top.iloc[np.argsort(np.abs(top.to_numpy(dtype=float)))]
        ax.barh(top.index.astype(str), top.values, color=PRIMARY_COLOR, alpha=0.85)
        ax.axvline(0, color="black", linewidth=0.8)
        ax.set_xlabel(f"SHAP (Stage A, class={drill_class})")
        ax.set_title(f"Single-loan drill-down: {drill_loan_id}", fontsize=11)

    return fig, fig2
