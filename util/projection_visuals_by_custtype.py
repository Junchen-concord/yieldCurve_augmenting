"""Faceted inference charts by customer type (NEW vs RETURN).

This module does not change model scores. It subsets the ``results`` dict from
``score_live_projection`` to a loan set, then reuses ``projection_visuals`` for
the same vintage + decomposition charts as the inference notebook.

**Application week (SQL):** ``plot_inference_by_custtype_recent_application_weeks`` buckets
by ``AppYear`` + ``AppWeek`` from the extract (same as ``DATEPART(WEEK, ApplicationDate)`` in
``jcx_raw_inference_v1.sql``), labeled ``YYYY-Wnn``. The trailing window uses
``OriginationDate`` vs ``as_of`` (calendar days), not extra SQL columns.

Typical notebook usage::

    from util.projection_visuals_by_custtype import plot_inference_faceted_by_custtype

    fig = plot_inference_faceted_by_custtype(results)
    fig.show()
"""
from __future__ import annotations

from typing import Iterable

import matplotlib.pyplot as plt
import pandas as pd

from .projection_visuals import (
    add_loan_level_band_to_summary,
    build_projection_decomposition,
    build_vintage_projection_summary,
    plot_projection_decomposition,
    plot_vintage_projection,
)

# Frames produced by ``score_live_projection`` that are keyed by LoanID.
_LOAN_SLICE_KEYS: dict[str, str] = {
    "loan_features": "column",
    "seq_features": "column",
    "stage_c_features": "column",
    "stage_b_scored_seq": "column",
    "loan_projection": "column",
    "observed_outcomes": "index",
    "prior_probs": "index",
    "posterior_probs": "index",
    "payin_matrix_pre_recovery": "index",
    "recovery_fraction_matrix": "index",
    "payin_matrix": "index",
}


def _loan_ids_for_custtype(loan_features: pd.DataFrame, custtype: str) -> set:
    ct = custtype.strip().upper()
    col = loan_features["CustType"].astype(str).str.upper().str.strip()
    return set(loan_features.loc[col.eq(ct), "LoanID"].dropna().unique())


def default_custtypes_for_plot(results: dict) -> tuple[str, ...]:
    """Return NEW and RETURN when present; otherwise any non-empty types sorted."""
    lf = results["loan_features"]
    if "CustType" not in lf.columns:
        raise ValueError("loan_features has no CustType column")
    s = lf["CustType"].astype(str).str.upper().str.strip()
    preferred = ("NEW", "RETURN")
    out = [c for c in preferred if s.eq(c).any()]
    if out:
        return tuple(out)
    rest = sorted({x for x in s.dropna().unique().tolist() if x})
    if not rest:
        raise ValueError("No CustType values in loan_features")
    return tuple(rest)


def subset_inference_results(results: dict, loan_ids: set) -> dict:
    """Return a shallow copy of ``results`` restricted to the given LoanIDs.

    All loan-indexed frames are filtered consistently so downstream builders match
    the full inference pipeline on a subset.
    """
    ids = set(loan_ids)
    if not ids:
        raise ValueError("loan_ids must be non-empty")

    out: dict = {}
    for key, val in results.items():
        if key == "qc":
            continue
        if key in _LOAN_SLICE_KEYS and isinstance(val, pd.DataFrame):
            if val.empty:
                out[key] = val.copy()
                continue
            if _LOAN_SLICE_KEYS[key] == "column":
                if "LoanID" not in val.columns:
                    raise KeyError(f"{key} missing LoanID column")
                out[key] = val[val["LoanID"].isin(ids)].copy()
            else:
                out[key] = val.loc[val.index.isin(ids)].copy()
        else:
            out[key] = val

    qc = dict(results.get("qc", {}))
    if "loan_features" in out and isinstance(out["loan_features"], pd.DataFrame):
        qc["n_loans"] = int(out["loan_features"]["LoanID"].nunique())
    out["qc"] = qc
    return out


def subset_inference_results_by_custtype(results: dict, custtype: str) -> dict:
    """Subset ``results`` to loans whose CustType matches (case-insensitive)."""
    lf = results["loan_features"]
    ids = _loan_ids_for_custtype(lf, custtype)
    if not ids:
        raise ValueError(f"No loans for CustType={custtype!r}")
    return subset_inference_results(results, ids)


def _as_of_timestamp(results: dict, as_of_date) -> pd.Timestamp:
    if as_of_date is not None:
        return pd.Timestamp(as_of_date).normalize()
    qc = results.get("qc") or {}
    s = qc.get("as_of_date")
    if s:
        return pd.Timestamp(s).normalize()
    lf = results["loan_features"]
    mx = pd.to_datetime(lf["OriginationDate"], errors="coerce").max()
    if pd.isna(mx):
        return pd.Timestamp.today().normalize()
    return mx.normalize()


def subset_inference_results_by_recent_application_weeks(
    results: dict,
    *,
    n_weeks: int = 8,
    as_of_date=None,
) -> dict:
    """Subset ``results`` to loans with ``OriginationDate`` in the last ``n_weeks`` × 7 days before ``as_of``.

    Chart buckets use ``AppYear`` / ``AppWeek`` (already on the inference extract via ``#t17_combined``);
    no extra SQL is required. ``as_of`` defaults to ``results['qc']['as_of_date']`` when set.
    """
    lf = results["loan_features"]
    as_of = _as_of_timestamp(results, as_of_date)
    anchor = pd.to_datetime(lf["OriginationDate"], errors="coerce").dt.normalize()
    start = as_of - pd.Timedelta(days=int(n_weeks) * 7)
    mask = anchor.notna() & (anchor >= start) & (anchor <= as_of)
    ids = set(lf.loc[mask, "LoanID"].dropna().unique())
    if not ids:
        raise ValueError("No loans in the requested origination-date window.")
    return subset_inference_results(results, ids)


def _is_application_year_week_label(s: pd.Series) -> pd.Series:
    return s.astype(str).str.match(r"^\d{4}-W\d{2}$", na=False)


def plot_inference_by_custtype_recent_application_weeks(
    results: dict,
    *,
    custtypes: Iterable[str] | None = None,
    n_weeks: int = 8,
    as_of_date=None,
    group_col: str = "application_year_week",
    n_sims: int = 1000,
    rng_seed: int = 42,
    figsize: tuple[float, float] | None = None,
    decomposition_annotate_realized: bool = True,
    decomposition_annotate_projected_final: bool = True,
) -> plt.Figure:
    """Faceted NEW/RETURN charts: vintage + decomposition by **AppYear + AppWeek** (SQL week).

    Restricts to loans with ``OriginationDate`` in the ``n_weeks`` trailing 7-day windows
    before ``as_of_date`` (default: ``results['qc']['as_of_date']``). Does not re-score.
    """
    windowed = subset_inference_results_by_recent_application_weeks(
        results, n_weeks=n_weeks, as_of_date=as_of_date
    )
    types = tuple(custtypes) if custtypes is not None else default_custtypes_for_plot(windowed)
    n = len(types)
    if n == 0:
        raise ValueError("custtypes is empty")

    w = max(12.0, 6.5 * n)
    h = 10.0
    fig, axes = plt.subplots(2, n, figsize=figsize or (w, h), constrained_layout=True)
    if n == 1:
        axes = axes.reshape(2, 1)

    for j, ct in enumerate(types):
        try:
            sub = subset_inference_results_by_custtype(windowed, ct)
        except ValueError:
            ax_top = axes[0, j]
            ax_top.set_title(f"{ct}: no loans")
            ax_top.axis("off")
            axes[1, j].axis("off")
            continue

        vintage = build_vintage_projection_summary(
            sub, group_col=group_col, n_sims=n_sims, rng_seed=rng_seed
        )
        vintage = add_loan_level_band_to_summary(vintage, sub, group_col=group_col)
        vintage = vintage.loc[_is_application_year_week_label(vintage[group_col])].copy()
        vintage = vintage[vintage[group_col].astype(str).ne("NaT")].copy()

        decomp = build_projection_decomposition(sub, group_col=group_col)
        decomp = decomp.loc[_is_application_year_week_label(decomp[group_col])].copy()
        decomp = decomp[decomp[group_col].astype(str).ne("NaT")].copy()

        if vintage.empty or decomp.empty:
            axes[0, j].set_title(f"{ct}: no cohort rows after filters")
            axes[0, j].axis("off")
            axes[1, j].axis("off")
            continue

        plot_vintage_projection(
            vintage,
            group_col=group_col,
            title=f"Projected final vs realized by AppYear–AppWeek (SQL week) — {ct}",
            ax=axes[0, j],
        )
        plot_projection_decomposition(
            decomp,
            group_col=group_col,
            title=f"Payin decomposition by AppYear–AppWeek — {ct}",
            ax=axes[1, j],
            annotate_realized_payin=decomposition_annotate_realized,
            annotate_projected_final=decomposition_annotate_projected_final,
        )

    return fig


def plot_inference_faceted_by_custtype(
    results: dict,
    *,
    custtypes: Iterable[str] | None = None,
    group_col: str = "origination_month",
    n_sims: int = 1000,
    rng_seed: int = 42,
    figsize: tuple[float, float] | None = None,
    decomposition_annotate_realized: bool = True,
    decomposition_annotate_projected_final: bool = True,
) -> plt.Figure:
    """Two columns (one per cust type): top = vintage projection, bottom = decomposition.

    Mirrors the inference notebook visuals without modifying ``projection_visuals``.

    The bottom row enables short numeric labels on realized (blue) and projected
    final (black line) by default so the facet matches the top row readout style.
    Set ``decomposition_annotate_*`` to False to match the plain inference notebook
    decomposition chart (no point labels).
    """
    types = tuple(custtypes) if custtypes is not None else default_custtypes_for_plot(results)
    n = len(types)
    if n == 0:
        raise ValueError("custtypes is empty")

    w = max(12.0, 6.5 * n)
    h = 10.0
    fig, axes = plt.subplots(2, n, figsize=figsize or (w, h), constrained_layout=True)
    if n == 1:
        axes = axes.reshape(2, 1)

    for j, ct in enumerate(types):
        try:
            sub = subset_inference_results_by_custtype(results, ct)
        except ValueError:
            ax_top = axes[0, j]
            ax_top.set_title(f"{ct}: no loans")
            ax_top.axis("off")
            axes[1, j].axis("off")
            continue

        vintage = build_vintage_projection_summary(
            sub, group_col=group_col, n_sims=n_sims, rng_seed=rng_seed
        )
        vintage = add_loan_level_band_to_summary(vintage, sub, group_col=group_col)
        vintage = vintage[vintage[group_col].astype(str).ne("NaT")].copy()

        decomp = build_projection_decomposition(sub, group_col=group_col)
        decomp = decomp[decomp[group_col].astype(str).ne("NaT")].copy()

        if vintage.empty or decomp.empty:
            axes[0, j].set_title(f"{ct}: no cohort rows after filters")
            axes[0, j].axis("off")
            axes[1, j].axis("off")
            continue

        plot_vintage_projection(
            vintage,
            group_col=group_col,
            title=f"Projected final vs realized by month — {ct}",
            ax=axes[0, j],
        )
        plot_projection_decomposition(
            decomp,
            group_col=group_col,
            title=f"Payin decomposition by month — {ct}",
            ax=axes[1, j],
            annotate_realized_payin=decomposition_annotate_realized,
            annotate_projected_final=decomposition_annotate_projected_final,
        )

    return fig
