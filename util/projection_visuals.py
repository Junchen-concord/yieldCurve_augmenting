"""Visualization helpers for projection inference outputs."""
from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .plot_style import ACCENT_COLOR, PLOT_PALETTE, PRIMARY_COLOR, apply_plot_style
from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER


def _application_year_week_key(frame: pd.DataFrame) -> pd.Series:
    """Application cohort key from ``AppYear`` + ``AppWeek`` (SQL ``DATEPART(WEEK, ApplicationDate)``).

    Labels are ``YYYY-Wnn`` with zero-padded week so lexicographic sort matches time order within a year.
    Rows with missing year or week get ``pd.NA`` (filtered downstream).
    """
    y = pd.to_numeric(frame["AppYear"], errors="coerce")
    w = pd.to_numeric(frame["AppWeek"], errors="coerce")
    valid = y.notna() & w.notna()
    out = pd.Series(pd.NA, index=frame.index, dtype=object)
    yi = y.loc[valid].round().astype(int)
    wi = w.loc[valid].round().astype(int).clip(lower=1, upper=53)
    lab = yi.astype(str) + "-W" + wi.astype(int).astype(str).str.zfill(2)
    out.loc[valid] = lab
    return out


def _group_key(frame: pd.DataFrame, group_col: str) -> pd.Series:
    if group_col in frame.columns:
        return frame[group_col].astype(str)
    if group_col == "origination_month":
        return pd.to_datetime(frame["OriginationDate"], errors="coerce").dt.to_period("M").astype(str)
    if group_col == "application_year_week":
        return _application_year_week_key(frame).astype(str)
    raise KeyError(f"Missing group column: {group_col}")


def _sort_group_frame(frame: pd.DataFrame, group_col: str) -> pd.DataFrame:
    out = frame.copy()
    if group_col == "application_year_week":
        bad = out[group_col].astype(str).isin(["nan", "NaT", "<NA>", "None"])
        return out.assign(_bad=bad).sort_values(["_bad", group_col]).drop(columns=["_bad"]).reset_index(drop=True)
    is_nat = out[group_col].astype(str).eq("NaT")
    sort_key = pd.to_datetime(out[group_col].where(~is_nat), errors="coerce")
    out["_sort_key"] = sort_key
    out["_is_nat"] = is_nat
    out = out.sort_values(["_is_nat", "_sort_key", group_col]).drop(columns=["_sort_key", "_is_nat"])
    return out.reset_index(drop=True)


def _weighted_average(values: pd.Series, weights: pd.Series) -> float:
    denom = weights.sum()
    if denom <= 0:
        return float("nan")
    return float(values.mul(weights).sum() / denom)


def _normalize_probs(prob_frame: pd.DataFrame, classes: list[str]) -> np.ndarray:
    cols = [f"P_{c}" for c in classes]
    probs = prob_frame[cols].to_numpy(dtype=float)
    return probs / np.clip(probs.sum(axis=1, keepdims=True), 1e-12, None)


def build_vintage_projection_summary(
    results: dict,
    group_col: str = "origination_month",
    n_sims: int = 1000,
    rng_seed: int = 42,
) -> pd.DataFrame:
    """Build group-level realized/projected payin with cohort-level simulation bands.

    The interval is the model's terminal-class simulation band for each cohort.
    It is not the average loan-level CI width.
    """
    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    loan_features = results["loan_features"].copy()
    posterior_probs = results["posterior_probs"].copy()
    payin_matrix = results["payin_matrix"].copy()

    loans = loan_features.set_index("LoanID").copy()
    common = payin_matrix.index.intersection(loans.index).intersection(posterior_probs.index)
    loans = loans.reindex(common)
    posterior_probs = posterior_probs.reindex(common)
    payin_matrix = payin_matrix.reindex(common)

    loans[group_col] = _group_key(loans.reset_index(), group_col).values
    loans["OriginatedAmount"] = pd.to_numeric(loans["OriginatedAmount"], errors="coerce").fillna(0.0)
    loans["TotalRealizedPayment"] = pd.to_numeric(loans["TotalRealizedPayment"], errors="coerce").fillna(0.0)
    loans["realized_payin_to_date"] = (
        loans["TotalRealizedPayment"] / loans["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    probs = pd.DataFrame(_normalize_probs(posterior_probs, classes), index=common, columns=classes)
    expected_payin = (probs[classes].to_numpy() * payin_matrix[classes].to_numpy(dtype=float)).sum(axis=1)
    loans["projected_final_payin_mean"] = expected_payin

    rng = np.random.default_rng(rng_seed)
    rows = []
    for group, ids in loans.groupby(group_col, sort=False).groups.items():
        idx = list(ids)
        sub_loans = loans.loc[idx]
        orig = sub_loans["OriginatedAmount"].to_numpy(dtype=float)
        orig_total = orig.sum()
        if orig_total <= 0:
            continue

        p = probs.loc[idx, classes].to_numpy(dtype=float)
        m = payin_matrix.loc[idx, classes].to_numpy(dtype=float)
        cum_p = np.cumsum(p, axis=1)
        sims = np.empty(n_sims)
        for s in range(n_sims):
            draws = rng.random(len(idx))
            class_idx = (cum_p < draws[:, None]).sum(axis=1).clip(max=len(classes) - 1)
            sims[s] = (orig * m[np.arange(len(idx)), class_idx]).sum() / orig_total

        rows.append({
            group_col: group,
            "loans": int(len(idx)),
            "originated_amount": float(orig_total),
            "realized_payin_to_date": float(sub_loans["TotalRealizedPayment"].sum() / orig_total),
            "projected_final_payin_mean": _weighted_average(
                sub_loans["projected_final_payin_mean"], sub_loans["OriginatedAmount"]
            ),
            "projected_final_payin_p05": float(np.quantile(sims, 0.05)),
            "projected_final_payin_p50": float(np.quantile(sims, 0.50)),
            "projected_final_payin_p95": float(np.quantile(sims, 0.95)),
            "cohort_band_width": float(np.quantile(sims, 0.95) - np.quantile(sims, 0.05)),
        })

    return _sort_group_frame(pd.DataFrame(rows), group_col)


def add_loan_level_band_to_summary(
    summary: pd.DataFrame,
    results: dict,
    group_col: str = "origination_month",
) -> pd.DataFrame:
    """Add average loan-level P05/P95 columns to a cohort summary.

    This is intentionally different from the cohort-level simulation band:
    it shows the typical individual-loan uncertainty before diversification.
    """
    loan_projection = results["loan_projection"].copy()
    if group_col == "application_year_week":
        lf = results.get("loan_features")
        if lf is not None:
            missing = [c for c in ("AppYear", "AppWeek") if c not in loan_projection.columns and c in lf.columns]
            if missing:
                loan_projection = loan_projection.merge(
                    lf[["LoanID", *missing]].drop_duplicates(subset=["LoanID"]),
                    on="LoanID",
                    how="left",
                )
    loan_projection[group_col] = _group_key(loan_projection, group_col).values
    loan_projection["OriginatedAmount"] = pd.to_numeric(
        loan_projection["OriginatedAmount"], errors="coerce"
    ).fillna(0.0)

    rows = []
    for group, df in loan_projection.groupby(group_col, sort=False):
        weights = df["OriginatedAmount"]
        rows.append({
            group_col: group,
            "loan_level_avg_p05": _weighted_average(df["pred_payin_lo05"], weights),
            "loan_level_avg_p95": _weighted_average(df["pred_payin_hi95"], weights),
            "loan_level_avg_band_width": _weighted_average(df["pred_payin_ci_width"], weights),
        })

    band = pd.DataFrame(rows)
    out = summary.merge(band, on=group_col, how="left")
    return _sort_group_frame(out, group_col)


def build_projection_decomposition(
    results: dict,
    group_col: str = "origination_month",
) -> pd.DataFrame:
    """Build realized + remaining normal + recovery decomposition by cohort."""
    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    loan_features = results["loan_features"].copy()
    posterior_probs = results["posterior_probs"].copy()
    pre_recovery = results["payin_matrix_pre_recovery"].copy()
    final = results["payin_matrix"].copy()

    loans = loan_features.set_index("LoanID").copy()
    common = final.index.intersection(loans.index).intersection(posterior_probs.index)
    loans = loans.reindex(common)
    posterior_probs = posterior_probs.reindex(common)
    pre_recovery = pre_recovery.reindex(common)
    final = final.reindex(common)

    loans[group_col] = _group_key(loans.reset_index(), group_col).values
    loans["OriginatedAmount"] = pd.to_numeric(loans["OriginatedAmount"], errors="coerce").fillna(0.0)
    loans["TotalRealizedPayment"] = pd.to_numeric(loans["TotalRealizedPayment"], errors="coerce").fillna(0.0)
    probs = pd.DataFrame(_normalize_probs(posterior_probs, classes), index=common, columns=classes)

    p = probs[classes].to_numpy(dtype=float)
    pre = pre_recovery[classes].to_numpy(dtype=float)
    fin = final[classes].to_numpy(dtype=float)

    loans["realized_payin_to_date"] = (
        loans["TotalRealizedPayment"] / loans["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    loans["expected_pre_recovery_payin"] = (p * pre).sum(axis=1)
    loans["expected_final_payin"] = (p * fin).sum(axis=1)
    loans["projected_remaining_normal"] = (
        loans["expected_pre_recovery_payin"] - loans["realized_payin_to_date"]
    )
    loans["projected_recovery"] = loans["expected_final_payin"] - loans["expected_pre_recovery_payin"]

    rows = []
    for group, df in loans.groupby(group_col, sort=False):
        weights = df["OriginatedAmount"]
        rows.append({
            group_col: group,
            "loans": int(len(df)),
            "originated_amount": float(weights.sum()),
            "realized_payin_to_date": _weighted_average(df["realized_payin_to_date"], weights),
            "projected_remaining_normal": _weighted_average(df["projected_remaining_normal"], weights),
            "projected_recovery": _weighted_average(df["projected_recovery"], weights),
            "projected_final_payin": _weighted_average(df["expected_final_payin"], weights),
        })

    return _sort_group_frame(pd.DataFrame(rows), group_col)


def plot_vintage_projection(
    summary: pd.DataFrame,
    group_col: str = "origination_month",
    title: str = "Projected Final Payin vs Realized To Date",
    ax=None,
    *,
    annotate_realized_payin: bool = True,
    realized_label_fmt: str = "{:.2f}",
    annotate_projected_payin: bool = True,
    projected_label_fmt: str = "{:.2f}",
):
    """Plot realized payin, projected final mean, and cohort-level simulation band.

    When ``annotate_realized_payin`` is True, each cohort bar is labeled with the
    dollar-weighted realized pay-in ratio (minimal numeric text above the bar).

    When ``annotate_projected_payin`` is True, each projected final mean is shown
    as a short number only (no extra jargon), in the same color as the trend line,
    placed just above that month's point on the line.
    """
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(11, 5))[1]
    x = np.arange(len(summary))
    labels = summary[group_col].astype(str)

    realized = summary["realized_payin_to_date"]
    ax.bar(x, realized, color=PRIMARY_COLOR, alpha=0.65, label="Realized to date")
    if {"loan_level_avg_p05", "loan_level_avg_p95"}.issubset(summary.columns):
        ax.fill_between(
            x,
            summary["loan_level_avg_p05"],
            summary["loan_level_avg_p95"],
            color=ACCENT_COLOR,
            alpha=0.16,
            label="Average loan-level P05-P95 band",
        )
    y = summary["projected_final_payin_mean"]
    yerr = np.vstack([
        y - summary["projected_final_payin_p05"],
        summary["projected_final_payin_p95"] - y,
    ])
    ax.errorbar(
        x,
        y,
        yerr=yerr,
        fmt="o-",
        color=ACCENT_COLOR,
        capsize=4,
        linewidth=2,
        label="Projected final mean with P05-P95 band",
    )
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title(title)
    ax.legend(loc="best")

    ylim_top = ax.get_ylim()[1]
    pad = 0.02 * max(ylim_top, 1e-6)

    if annotate_realized_payin and len(summary):
        for xi, val in zip(x, realized):
            if pd.isna(val):
                continue
            ax.text(
                xi,
                float(val) + pad,
                realized_label_fmt.format(float(val)),
                ha="center",
                va="bottom",
                fontsize=8,
                color="#333333",
            )

    if annotate_projected_payin and len(summary):
        for xi, yv in zip(x, y.to_numpy(dtype=float)):
            if pd.isna(yv):
                continue
            ax.annotate(
                projected_label_fmt.format(float(yv)),
                xy=(float(xi), float(yv)),
                xytext=(0, 10),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
                color=ACCENT_COLOR,
            )
        ax.margins(y=0.08)

    return ax


def plot_projection_decomposition(
    decomposition: pd.DataFrame,
    group_col: str = "origination_month",
    title: str = "Projected Final Payin Decomposition",
    ax=None,
    *,
    annotate_realized_payin: bool = False,
    realized_label_fmt: str = "{:.2f}",
    annotate_projected_final: bool = False,
    projected_final_label_fmt: str = "{:.2f}",
):
    """Plot realized-to-date plus projected remaining normal and recovery.

    By default, bar segments are not annotated (cleaner stacked view). Set
    ``annotate_realized_payin`` to True to show minimal numeric labels on the
    realized segment only.

    Set ``annotate_projected_final`` to True to label each black-line point with
    the cohort projected final pay-in (same short numeric style as the top chart).
    """
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(11, 5))[1]
    x = np.arange(len(decomposition))
    labels = decomposition[group_col].astype(str)

    realized = decomposition["realized_payin_to_date"]
    remaining = decomposition["projected_remaining_normal"]
    recovery = decomposition["projected_recovery"]

    ax.bar(x, realized, color=PRIMARY_COLOR, alpha=0.75, label="Realized to date")
    ax.bar(x, remaining, bottom=realized, color=ACCENT_COLOR, alpha=0.65, label="Projected remaining normal")
    ax.bar(x, recovery, bottom=realized + remaining, color=PLOT_PALETTE[2], alpha=0.65, label="Projected recovery")
    ax.plot(x, decomposition["projected_final_payin"], color="black", marker="o", linewidth=1.5, label="Projected final")

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title(title)
    ax.legend(loc="best")

    if annotate_realized_payin and len(decomposition):
        ylim_top = ax.get_ylim()[1]
        pad = 0.02 * max(ylim_top, 1e-6)
        for xi, r in zip(x, realized):
            if pd.isna(r):
                continue
            ax.text(
                xi,
                float(r) + pad,
                realized_label_fmt.format(float(r)),
                ha="center",
                va="bottom",
                fontsize=8,
                color="#333333",
            )

    if annotate_projected_final and len(decomposition):
        y_pf = decomposition["projected_final_payin"].to_numpy(dtype=float)
        for xi, yv in zip(x, y_pf):
            if pd.isna(yv):
                continue
            ax.annotate(
                projected_final_label_fmt.format(float(yv)),
                xy=(float(xi), float(yv)),
                xytext=(0, 10),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
                color="black",
            )
        ax.margins(y=0.08)

    return ax
