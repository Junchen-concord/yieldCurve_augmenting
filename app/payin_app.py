"""Payin projection demo app (NEW cohorts).

Run from repo root:  streamlit run app/payin_app.py

Reads the small artifacts exported by jcx_2026_projection_inference_V1.ipynb
(projection_data/app/). No database access from the app — refresh = rerun the
inference notebook.

Design (per skills/0603_payin_guardrail_devband_v1.md):
- The POINT estimate is flat: model capped at the lookup baseline. The slider
  never moves it.
- The slider sets "installments observed (k)". It drives ONLY the confidence
  band, via the model-free development curve: at maturity share w = k / schedule,
  the band is the observed remaining-payin quantile spread, centered on the
  point through the median development path. It collapses onto the point at
  full maturity.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from util.plot_style import apply_plot_style, PRIMARY_COLOR, ACCENT_COLOR  # noqa: E402
from util.projection_lookup_blend import _interp_remaining  # noqa: E402

APP_DATA_DIR = REPO_ROOT / "projection_data" / "app"
SEGMENT = "NEW"


@st.cache_data
def load_artifacts():
    summary = pd.read_csv(APP_DATA_DIR / "cohort_summary.csv")
    dev_curve = pd.read_csv(APP_DATA_DIR / "dev_curve.csv")
    meta = json.loads((APP_DATA_DIR / "meta.json").read_text())
    return summary, dev_curve, meta


def band_at_k(summary: pd.DataFrame, curve_seg: pd.DataFrame, k: float) -> pd.DataFrame:
    """Recompute the band per cohort at hypothetical observed-installment count k.

    Band = point +/- the development-curve remaining spread at w = k / schedule,
    centered through the median path; pinned so point stays inside.
    """
    out = summary.copy()
    w_list, lo_list, hi_list = [], [], []
    for _, row in out.iterrows():
        total = float(row.get("avg_total_installs") or 0.0)
        w = float(np.clip(k / total, 0.0, 1.0)) if total > 0 else 0.0
        rem_lo, rem_md, rem_hi = _interp_remaining(curve_seg, w)
        point = float(row["point_payin"])
        lo = max(point - rem_md + rem_lo, 0.0)
        hi = point - rem_md + rem_hi
        lo, hi = min(lo, point), max(hi, point)
        w_list.append(w)
        lo_list.append(lo)
        hi_list.append(hi)
    out["w_at_k"] = w_list
    out["band_lo_at_k"] = lo_list
    out["band_hi_at_k"] = hi_list
    out["band_width_at_k"] = out["band_hi_at_k"] - out["band_lo_at_k"]
    return out


def vintage_chart(view: pd.DataFrame, k: int) -> plt.Figure:
    apply_plot_style(style="white", axes_grid=False)
    fig, ax = plt.subplots(figsize=(11, 5))
    x = np.arange(len(view))
    labels = view["application_year_week"]

    ax.bar(x, view["realized_payin_to_date"], color=PRIMARY_COLOR, alpha=0.6,
           label="Realized to date (actual)")
    ax.fill_between(x, view["band_lo_at_k"], view["band_hi_at_k"], color=ACCENT_COLOR,
                    alpha=0.18, label=f"90% band at k={k} observed installments")
    ax.plot(x, view["point_payin"], "o-", color=ACCENT_COLOR, linewidth=2,
            label="Projected final payin (stable point)")
    for xi, row in zip(x, view.itertuples()):
        flag = " *" if row.clamped_lookup else ""
        ax.annotate(f"{row.point_payin:.2f}{flag}", (xi, row.point_payin),
                    textcoords="offset points", xytext=(0, 9), ha="center",
                    color=ACCENT_COLOR, fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title(f"{SEGMENT} cohorts — stable projected final, band at k={k} "
                 "(* = capped to historical baseline)")
    ax.grid(alpha=0.25)
    ax.legend(loc="upper left")
    fig.tight_layout()
    return fig


@st.cache_data
def load_v2_artifacts():
    """Survival-lookup artifacts (tab 2). Returns None if not yet exported."""
    try:
        table = pd.read_csv(APP_DATA_DIR / "survival_payin_table.csv")
        cohorts = pd.read_csv(APP_DATA_DIR / "live_cohorts_v2.csv")
        backtest = pd.read_csv(APP_DATA_DIR / "survival_backtest.csv")
        meta = json.loads((APP_DATA_DIR / "meta_v2.json").read_text())
        return table, cohorts, backtest, meta
    except FileNotFoundError:
        return None


@st.cache_data
def load_cohort_accuracy():
    """Cohort-grain backtest. Detail (per segment) preferred; overall fallback."""
    try:
        return pd.read_csv(APP_DATA_DIR / "survival_backtest_cohort_detail.csv"), "detail"
    except FileNotFoundError:
        pass
    try:
        return pd.read_csv(APP_DATA_DIR / "survival_backtest_cohort.csv"), "overall"
    except FileNotFoundError:
        return None, None


@st.cache_data
def load_install_curves():
    """Per-installment collected $ sums (hist + live cohorts). None if not exported."""
    try:
        hist = pd.read_csv(APP_DATA_DIR / "install_curve_hist.csv")
        live = pd.read_csv(APP_DATA_DIR / "install_curve_live.csv")
        return hist, live
    except FileNotFoundError:
        return None, None


ALL = "ALL (blended)"


def _filter_segment(df: pd.DataFrame, cust: str, freq: str) -> pd.DataFrame:
    """Filter to a segment; ALL keeps every segment (blend happens at aggregation)."""
    out = df
    if cust != ALL:
        out = out[out["CustType"] == cust]
    if freq != ALL:
        out = out[out["Frequency_group3"] == freq]
    return out


def _blend_cohorts(cohorts: pd.DataFrame) -> pd.DataFrame:
    """$-weighted bottom-up blend of per-segment cohort rows into one row per week."""
    rows = []
    for key, g in cohorts.groupby(["AppYear", "AppWeek"]):
        w = g["orig_total"].clip(lower=0)
        wsum = float(w.sum())
        if wsum <= 0:
            continue
        def wavg(col):
            return float((g[col] * w).sum() / wsum)
        rows.append({
            "AppYear": key[0], "AppWeek": key[1],
            "n_loans": int(g["n_loans"].sum()), "orig_total": wsum,
            "realized_payin": wavg("realized_payin"), "projected_payin": wavg("projected_payin"),
            "band_lo": wavg("band_lo"), "band_hi": wavg("band_hi"), "avg_k": wavg("avg_k"),
            "pct_alive": wavg("pct_alive"), "pct_default": wavg("pct_default"),
            "pct_paid_off": wavg("pct_paid_off"),
        })
    out = pd.DataFrame(rows).sort_values(["AppYear", "AppWeek"])
    out["band_width"] = out["band_hi"] - out["band_lo"]
    out["week_label"] = (out["AppYear"].astype(int).astype(str) + "-W"
                         + out["AppWeek"].astype(int).astype(str).str.zfill(2))
    return out


def render_survival_tab() -> None:
    """Tab 2 — installment-status tracking from the survival x payin lookup.

    Faceted by frequency (Michael's 'blob' fix). The band is historical
    state-conditional quantiles — no simulation.
    """
    v2 = load_v2_artifacts()
    if v2 is None:
        st.info("Run yield_projections_notebooks/jcx_payin_lookup_v2.ipynb to export "
                "the survival-lookup artifacts (survival_payin_table.csv, ...).")
        return
    table, cohorts, backtest, meta = v2
    k_max = int(meta.get("k_max", 5))

    st.caption(
        f"As of {meta['as_of_date']} · built from {meta['matured_loans']:,} matured loans · "
        "band = historical q05–q95 of loans in the same installment status"
    )

    c1, c2, c3 = st.columns(3)
    cust = c1.selectbox("Customer type", [ALL, *sorted(table["CustType"].dropna().unique())], index=1)
    freq = c2.selectbox("Frequency", [ALL, *sorted(f for f in table["Frequency_group3"].dropna().unique()
                                                   if f != "UNKNOWN")])
    k = c3.slider("Installments observed (k)", 0, k_max, 0)
    seg = _filter_segment(table, cust, freq)

    # --- KPI cards: most recent cohort week as of today (actual observed status, not the slider) ---
    sub_all = _filter_segment(cohorts, cust, freq)
    coh_view = _blend_cohorts(sub_all[sub_all["n_loans"] > 0]) if not sub_all.empty else pd.DataFrame()
    coh_view = coh_view[coh_view["n_loans"] >= 20] if not coh_view.empty else coh_view
    if not coh_view.empty:
        latest = coh_view.iloc[-1]
        st.markdown(f"### Most recent cohort — {latest['week_label']} ({cust} / {freq})")
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Projected final payin", f"{latest['projected_payin']:.2f}")
        k2.metric("Realized to date", f"{latest['realized_payin']:.2f}",
                  delta=f"k ≈ {latest['avg_k']:.1f} installments observed")
        k3.metric("Individual-loan range", f"{latest['band_lo']:.2f} – {latest['band_hi']:.2f}")
        k4.metric("Loans / originated $", f"{int(latest['n_loans']):,} / ${latest['orig_total']:,.0f}")
        st.caption("KPIs reflect today's actual observed status. The k slider below is a "
                   "what-if on historical states; it does not move these numbers.")
    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader(f"State mix at k={k} (survival table)")
        cell_k = seg[seg["k"] == k]
        mix = (
            cell_k.groupby("status")
            .apply(lambda g: pd.Series({
                "share": float((g["share"] * g["n_loans"]).sum() / max(g["n_loans"].sum(), 1) )
                         if len(g) > 1 else float(g["share"].iloc[0]),
                "n_loans": int(g["n_loans"].sum()),
                "expected_payin": float(np.average(g["expected_payin"], weights=g["n_loans"])),
                "q05": float(np.average(g["q05"], weights=g["n_loans"])),
                "q95": float(np.average(g["q95"], weights=g["n_loans"])),
            }))
            .reset_index().sort_values("share", ascending=False)
        )
        mix["share"] = mix["share"] / mix["share"].sum()  # renormalize across statuses
        mix["band_width"] = mix["q95"] - mix["q05"]
        st.dataframe(
            mix.style.format({"share": "{:.1%}", "expected_payin": "{:.3f}",
                              "q05": "{:.3f}", "q95": "{:.3f}", "band_width": "{:.3f}"}),
            use_container_width=True, hide_index=True,
        )

    with right:
        st.subheader("Survivor band narrows with k")
        alive = seg[seg["status"] == "alive"]
        alive = (
            alive.groupby("k")
            .apply(lambda g: float(np.average(g["band_width"], weights=g["n_loans"])))
            .rename("band_width").reset_index().sort_values("k")
        )
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(alive["k"], alive["band_width"], "o-", color=ACCENT_COLOR)
        ax.axvline(k, color="gray", linestyle="--", alpha=0.6)
        ax.set_xlabel("Installments observed (k)")
        ax.set_ylabel("q95 − q05 (payin ratio)")
        ax.set_title(f"{cust} / {freq} — survivor confidence by k")
        ax.grid(alpha=0.25)
        st.pyplot(fig)

    # --- Accuracy panel: cohort-level MAE shrinking as k grows (out-of-time backtest) ---
    acc, acc_kind = load_cohort_accuracy()
    if acc is not None:
        if acc_kind == "detail":
            seg_err = acc[(acc["CustType"] == cust) & (acc["Frequency_group3"] == freq)]
            if seg_err.empty:
                seg_err = acc
                acc_label = "all segments"
            else:
                acc_label = f"{cust} / {freq}"
            mae_by_k = seg_err.groupby("k")["cohort_err"].apply(lambda s: s.abs().mean())
        else:
            mae_by_k = acc.set_index("k")["mae"]
            acc_label = "all segments"

        st.subheader("How accurate is the lookup? (out-of-time cohort backtest)")
        m1, m2, m3 = st.columns(3)
        mae_k = float(mae_by_k.get(k, np.nan))
        mae_0 = float(mae_by_k.get(0, np.nan))
        m1.metric(f"Cohort-level MAE at k={k}", f"{mae_k:.3f}" if np.isfinite(mae_k) else "n/a")
        if np.isfinite(mae_k) and np.isfinite(mae_0) and mae_0 > 0:
            m2.metric("vs day zero (k=0)", f"{mae_0:.3f}",
                      delta=f"{(mae_k - mae_0) / mae_0:+.0%} error", delta_color="inverse")
        m3.caption(f"Segment: {acc_label}. Cohorts ≥30 loans, $-weighted, "
                   "older cohorts build the table, newer matured cohorts are scored.")

        fig, ax = plt.subplots(figsize=(7, 3))
        ax.plot(mae_by_k.index, mae_by_k.values, "o-", color=ACCENT_COLOR)
        ax.axvline(k, color="gray", linestyle="--", alpha=0.6)
        if np.isfinite(mae_k):
            ax.annotate(f"{mae_k:.3f}", (k, mae_k), textcoords="offset points",
                        xytext=(8, 8), color=ACCENT_COLOR)
        ax.set_xlabel("Installments observed (k)")
        ax.set_ylabel("Cohort MAE (payin ratio)")
        ax.set_title("Projection error shrinks as installments land")
        ax.grid(alpha=0.25)
        st.pyplot(fig)

    # --- Cumulative collections by installment: realized vs historical expected ---
    hist_curve, live_curve = load_install_curves()
    if hist_curve is not None:
        st.subheader(f"Cumulative collections by installment — {cust} / {freq}")
        h = _filter_segment(hist_curve, cust, freq)
        h_steps = h.groupby("InstallmentNumber")["step_amount"].sum().sort_index()
        h_orig = h.drop_duplicates(subset=["CustType", "Frequency_group3"])["orig_total"].sum()
        h_cum = h_steps.cumsum() / max(h_orig, 1e-9)

        lv = _filter_segment(live_curve, cust, freq)
        fig, ax = plt.subplots(figsize=(11, 4.5))
        ax.plot(h_cum.index, h_cum.values, "--", color="black", linewidth=2,
                label="Historical expected path (matured loans)")
        weeks = sorted(lv.groupby(["AppYear", "AppWeek"]).groups.keys())[-8:]
        cmap = plt.get_cmap("viridis")
        for i, wk in enumerate(weeks):
            g = lv[(lv["AppYear"] == wk[0]) & (lv["AppWeek"] == wk[1])]
            steps = g.groupby("InstallmentNumber")["step_amount"].sum().sort_index()
            orig = g.drop_duplicates(subset=["CustType", "Frequency_group3"])["orig_total"].sum()
            if orig <= 0 or steps.empty:
                continue
            cum = steps.cumsum() / orig
            label = f"{int(wk[0])}-W{int(wk[1]):02d}"
            ax.plot(cum.index, cum.values, "o-", color=cmap(i / max(len(weeks) - 1, 1)),
                    alpha=0.8, label=label)
        ax.set_xlabel("Installment number")
        ax.set_ylabel("Cumulative collected / originated")
        ax.set_title("Each cohort's realized path vs the historical curve — early read on drift")
        ax.grid(alpha=0.25)
        ax.legend(loc="upper left", fontsize=8, ncol=2)
        st.pyplot(fig)
        st.caption("Live cohort lines cover settled installments only (due date ≤ as-of). "
                   "A cohort tracking below the dashed line is collecting slower than history.")
    else:
        st.info("Rerun jcx_payin_lookup_v2.ipynb to export install_curve_hist.csv / "
                "install_curve_live.csv for the collections-by-installment view.")

    st.subheader(f"Live cohorts — {cust} / {freq}")
    sub = coh_view.copy() if not coh_view.empty else pd.DataFrame()
    if sub.empty:
        st.write("No live cohorts with ≥20 loans in this segment.")
    else:
        fig, ax = plt.subplots(figsize=(11, 4.5))
        x = np.arange(len(sub))
        ax.bar(x, sub["realized_payin"], color=PRIMARY_COLOR, alpha=0.6, label="Realized to date")
        ax.fill_between(x, sub["band_lo"], sub["band_hi"], color=ACCENT_COLOR, alpha=0.18,
                        label="Historical q05–q95 (state-conditional)")
        ax.plot(x, sub["projected_payin"], "o-", color=ACCENT_COLOR, linewidth=2,
                label="Projected final (survival lookup)")
        ax.set_xticks(x)
        ax.set_xticklabels(sub["week_label"], rotation=45, ha="right")
        ax.set_ylabel("Payin ratio")
        ax.grid(alpha=0.25)
        ax.legend(loc="upper left")
        st.pyplot(fig)
        st.dataframe(
            sub[["week_label", "n_loans", "avg_k", "pct_alive", "pct_default", "pct_paid_off",
                 "realized_payin", "projected_payin", "band_lo", "band_hi", "band_width"]]
            .style.format({"avg_k": "{:.1f}", "pct_alive": "{:.0%}", "pct_default": "{:.0%}",
                           "pct_paid_off": "{:.0%}", "realized_payin": "{:.3f}",
                           "projected_payin": "{:.3f}", "band_lo": "{:.3f}",
                           "band_hi": "{:.3f}", "band_width": "{:.3f}"}),
            use_container_width=True, hide_index=True,
        )

    with st.expander("Out-of-time backtest (older cohorts build the table, newer matured cohorts scored)"):
        st.dataframe(backtest.round(4), use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Payin Projection", layout="wide")

    tab1, tab2 = st.tabs(["Final payin projection (model)", "Installment tracking (survival lookup)"])
    with tab2:
        render_survival_tab()
    with tab1:
        render_model_tab()


def render_model_tab() -> None:
    summary, dev_curve, meta = load_artifacts()
    curve_seg = dev_curve[dev_curve["CustType"].astype(str).str.upper() == SEGMENT]

    st.title("Payin Projection — NEW cohorts")
    st.caption(
        f"As of {meta['as_of_date']} · model run {meta.get('run_tag') or 'n/a'} · "
        "point = model capped at historical baseline · band = observed development curve"
    )

    avg_sched = float(summary["avg_total_installs"].mean())
    k_max = int(np.ceil(summary["avg_total_installs"].max()))

    with st.sidebar:
        st.header("Scenario")
        k = st.slider("Installments observed (k)", min_value=0, max_value=k_max, value=0)
        share = min(k / avg_sched, 1.0) if avg_sched > 0 else 0.0
        st.caption(f"k = {k} ≈ **{share:.0%} of schedule** "
                   f"(avg {SEGMENT} schedule ≈ {avg_sched:.1f} installments)")
        st.divider()
        st.slider("FPD/FA rate (coming soon)", 0.0, 1.0, disabled=True,
                  help="Pending conversion/FPD R² review — phase 2.")

    view = band_at_k(summary, curve_seg, k)

    st.pyplot(vintage_chart(view, k))

    st.subheader("Cohort detail")
    table = view[[
        "application_year_week", "loans", "originated_amount",
        "realized_payin_to_date", "point_payin",
        "band_lo_at_k", "band_hi_at_k", "band_width_at_k",
        "w_at_k", "clamped_lookup",
    ]].rename(columns={
        "application_year_week": "Cohort week",
        "loans": "Loans",
        "originated_amount": "Originated $",
        "realized_payin_to_date": "Realized",
        "point_payin": "Projected final",
        "band_lo_at_k": f"P05 @ k={k}",
        "band_hi_at_k": f"P95 @ k={k}",
        "band_width_at_k": "Band width",
        "w_at_k": "Maturity share",
        "clamped_lookup": "Capped to baseline",
    })
    st.dataframe(
        table.style.format({
            "Originated $": "{:,.0f}",
            "Realized": "{:.3f}",
            "Projected final": "{:.3f}",
            f"P05 @ k={k}": "{:.3f}",
            f"P95 @ k={k}": "{:.3f}",
            "Band width": "{:.3f}",
            "Maturity share": "{:.0%}",
        }),
        use_container_width=True, hide_index=True,
    )

    with st.expander("Current actuals (today's observed maturity, not the slider)"):
        st.dataframe(
            summary[[
                "application_year_week", "avg_weight_w", "point_p05", "point_p95",
                "band_width", "band_source", "model_final_payin", "baseline_payin",
            ]].round(3),
            use_container_width=True, hide_index=True,
        )


if __name__ == "__main__":
    main()
