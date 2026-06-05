"""Guardrail the 3-stage model projection with the historical lookup baseline.

Design (revised 2026-06-03 after stakeholder feedback):

  The model is the trusted point estimate. We do NOT scale it by maturity.
  Two separate layers:

  * POINT ESTIMATE — stable, trust the model, only rein it in when it is
    off-base. Two value-triggered caps (not maturity weights):

        point = model_cohort_mean
        if model > lookup_cohort_mean:
            point = min(point, lookup_cohort_mean)   # lookup baseline (~1.45 NEW)
        point = min(point, segment_ceiling)          # hard backstop (p95 ~1.79 NEW)
        point = max(point, realized_to_date)

    A confident number *below* the lookup (e.g. 1.312 when lookup ~1.40) passes
    through untouched. Only when the model reads *hotter* than historically similar
    loans do we cap to the lookup. ``segment_ceiling`` is the p95 of historical
    weekly cohort means — a coarse backstop only; it must NOT be the primary cap
    (it sits ~0.35 above the lookup and was leaving W21/W22 at 1.79 instead of ~1.45).

  * CONFIDENCE INTERVAL — the only thing that moves with maturity. Wide when few
    installments are observed, narrowing as installments come due. There are two
    ways to size it; both keep the point fixed and move only the band:

    1. DEVELOPMENT BAND (preferred, observable; ``build_development_curve``).
       Measured, not assumed. From matured cohorts we observe how much payin
       still arrives after a cohort has reached observed-installment share
       ``w_bar`` -- ``remaining(w_bar) = final - realized(w_bar)`` -- and how
       variable that tail is. The live band is then the empirical predictive
       interval for the final, anchored on the cohort's realized-to-date:

           band = [ realized + q05_remaining(w_bar),
                    realized + q95_remaining(w_bar) ]

       It collapses to zero width as ``w_bar -> 1`` *because the data shows the
       tail vanishing*, and it doubles as an independent check on the model
       point. The convergence funnel + coverage table make the narrowing
       auditable. This is model-free (pure payment history), so no replay or
       per-retrain recalibration is required.

    2. PARAMETRIC BAND (fallback when no curve is supplied):

           band_half = z * sqrt( ((1 - w_bar) * sigma_segment)^2 + div_sigma^2 )

       where ``sigma_segment`` is the historical cohort-to-cohort std of the
       segment and ``div_sigma`` is the per-loan class-mixture spread. Cheaper
       but the narrowing is assumed rather than observed.

  This neither retrains nor modifies persisted model artifacts.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .payin_lookup import LookupTable, apply_lookup, _weighted_mean
from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER
from .projection_visuals import _group_key, _sort_group_frame, _weighted_average

try:  # styling is optional for the pure-data helpers
    import matplotlib.pyplot as plt

    from .plot_style import ACCENT_COLOR, PLOT_PALETTE, PRIMARY_COLOR, apply_plot_style
except Exception:  # pragma: no cover
    plt = None


DEFAULT_LOOKUP_KEYS = [
    "DM_Band_Name",
    "CM_Band_Name",
    "CustType",
    "PortFolioID",
    "AppMonth",
    "AppWeek",
    "Frequency_group3",
]

Z_90 = 1.6448536269514722  # 90% two-sided normal quantile


# ---------------------------------------------------------------------------
# Key normalization + baseline scoring
# ---------------------------------------------------------------------------
def _normalize_lookup_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Match the key normalization used when the lookup table was built."""
    out = df.copy()
    for c in ("DM_Band_Name", "CM_Band_Name", "CustType"):
        if c in out.columns:
            out[c] = (
                out[c].astype(str).str.strip().str.upper()
                .replace({"NAN": "UNKNOWN", "": "UNKNOWN", "NONE": "UNKNOWN"})
            )
    if "Frequency_group3" in out.columns:
        out["Frequency_group3"] = out["Frequency_group3"].astype(str).str.strip().str.upper()
    for c in ("AppMonth", "AppWeek"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def attach_lookup_baseline(results: dict, lookup: LookupTable) -> pd.DataFrame:
    """Score inference ``loan_features`` against the historical lookup (mu/std/tier/n)."""
    loan_features = _normalize_lookup_keys(results["loan_features"])
    return apply_lookup(loan_features, lookup)


def _normalized_posterior(posterior_probs: pd.DataFrame, index) -> np.ndarray:
    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    cols = [f"P_{c}" for c in classes]
    P = posterior_probs.reindex(index)[cols].to_numpy(dtype=float)
    return P / np.clip(P.sum(axis=1, keepdims=True), 1e-12, None)


# ---------------------------------------------------------------------------
# Historical cohort-level plausibility envelope (the guardrail source)
# ---------------------------------------------------------------------------
def compute_segment_cohort_history(
    train_df: pd.DataFrame,
    segment_col: str = "CustType",
    cohort_cols: tuple[str, ...] = ("AppYear", "AppWeek"),
    payin_col: str = "payin_ratio_realized",
    orig_col: str = "OriginatedAmount",
    ceiling_q: float = 0.95,
    min_cohort_loans: int = 30,
) -> dict:
    """Distribution of historical $-weighted WEEKLY cohort-mean payins per segment.

    Returns ``{segment: {center, sigma, ceiling, floor, n_cohorts}}`` where
    ``ceiling`` is the ``ceiling_q`` quantile of weekly cohort means (the point
    guardrail) and ``sigma`` is the cohort-to-cohort std (the systematic band
    component). Thin weeks (< ``min_cohort_loans``) are dropped so the envelope
    is not distorted by sparse cohorts.
    """
    df = train_df.copy()
    df[segment_col] = df[segment_col].astype(str).str.strip().str.upper()
    rows = []
    for key, sub in df.groupby([segment_col, *cohort_cols], dropna=False):
        if len(sub) < min_cohort_loans:
            continue
        seg = key[0] if isinstance(key, tuple) else key
        rows.append({
            segment_col: seg,
            "cohort_mean": _weighted_mean(sub[payin_col], sub[orig_col]),
            "loans": int(len(sub)),
        })
    cm = pd.DataFrame(rows)
    out: dict = {}
    for seg, g in cm.groupby(segment_col):
        means = g["cohort_mean"].astype(float).dropna()
        if means.empty:
            continue
        out[seg] = {
            "center": float(means.mean()),
            "sigma": float(means.std(ddof=0)),
            "ceiling": float(means.quantile(ceiling_q)),
            "floor": float(means.quantile(1.0 - ceiling_q)),
            "n_cohorts": int(len(means)),
        }
    return out


# ---------------------------------------------------------------------------
# Development curve: observable, model-free maturity -> remaining-payin band
# ---------------------------------------------------------------------------
def build_cohort_trajectories(
    raw_df: pd.DataFrame,
    loan_features: pd.DataFrame,
    segment_col: str = "CustType",
    cohort_cols: tuple[str, ...] = ("AppYear", "AppWeek"),
    age_grid_days: tuple[int, ...] | None = None,
    min_cohort_loans: int = 30,
    install_mode: int = 144,
) -> pd.DataFrame:
    """Reconstruct each matured cohort's realized-payin trajectory vs maturity.

    Model-free. For every cohort (segment x cohort_cols) and every age ``t`` on
    the grid (days since the cohort's origination) we compute, $-weighted:

      * ``w_bar``    = installments due by age ``t`` / total scheduled installments
                       (the same observed-installment share the live cohorts use),
      * ``realized`` = payments received by age ``t`` / originated $,
      * ``final``    = the cohort's fully-matured payin (= payin_ratio_realized),
      * ``remaining``= final - realized  (the still-to-come tail).

    Payments sum ``InstallRealizedPayment`` across *all* modes by ``PaymentDate``
    (matching ``payin_ratio_realized``); due dates come from the normal-installment
    rows (``iPaymentMode == install_mode``). Returns one row per (cohort, age).
    """
    if age_grid_days is None:
        age_grid_days = tuple(range(7, 372, 7))
    age_arr = np.asarray(sorted(set(int(a) for a in age_grid_days)), dtype=float)

    raw = raw_df.copy()
    for c in ("OriginationDate", "InstallmentDueDate", "PaymentDate"):
        if c in raw.columns:
            raw[c] = pd.to_datetime(raw[c], errors="coerce")

    lf = _normalize_lookup_keys(loan_features).copy()
    lf[segment_col] = lf[segment_col].astype(str).str.strip().str.upper()
    keep = ["LoanID", "OriginationDate", "OriginatedAmount", "TotalInstallsNumber",
            "payin_ratio_realized", segment_col, *cohort_cols]
    lf = lf[[c for c in keep if c in lf.columns]].dropna(subset=["LoanID"]).copy()
    lf["OriginationDate"] = pd.to_datetime(lf["OriginationDate"], errors="coerce")
    lf["OriginatedAmount"] = pd.to_numeric(lf["OriginatedAmount"], errors="coerce").fillna(0.0)
    lf["TotalInstallsNumber"] = pd.to_numeric(lf["TotalInstallsNumber"], errors="coerce").fillna(0.0)

    # Per-loan payment stream: (days since origination, $ paid).
    pay = raw[["LoanID", "PaymentDate", "InstallRealizedPayment"]].copy()
    pay["amt"] = pd.to_numeric(pay["InstallRealizedPayment"], errors="coerce").fillna(0.0)
    pay = pay[pay["PaymentDate"].notna() & (pay["amt"] != 0.0)]
    pay = pay.merge(lf[["LoanID", "OriginationDate"]], on="LoanID", how="inner")
    pay["age"] = (pay["PaymentDate"] - pay["OriginationDate"]).dt.days

    # Per-loan due stream: (days since origination) for each scheduled installment.
    due = raw.loc[raw.get("iPaymentMode", pd.Series(index=raw.index)) == install_mode,
                  ["LoanID", "InstallmentDueDate"]].copy()
    due = due[due["InstallmentDueDate"].notna()]
    due = due.merge(lf[["LoanID", "OriginationDate"]], on="LoanID", how="inner")
    due["age"] = (due["InstallmentDueDate"] - due["OriginationDate"]).dt.days

    # Map each payment/due row to its cohort once (vectorized), then evaluate the
    # cumulative curves at the age grid per cohort via searchsorted.
    cohort_group_cols = [segment_col, *cohort_cols]
    lf["_cohort"] = list(zip(*[lf[c] for c in cohort_group_cols]))
    cohort_stats = (
        lf.groupby("_cohort")
        .agg(orig_total=("OriginatedAmount", "sum"),
             total_installs=("TotalInstallsNumber", "sum"),
             loans=("LoanID", "size"))
    )
    cohort_final = lf.groupby("_cohort").apply(
        lambda g: _weighted_mean(g["payin_ratio_realized"], g["OriginatedAmount"])
    ).to_dict()
    id_to_cohort = lf.set_index("LoanID")["_cohort"]
    pay = pay.assign(_cohort=pay["LoanID"].map(id_to_cohort))
    due = due.assign(_cohort=due["LoanID"].map(id_to_cohort))
    pay_by = {k: g.sort_values("age") for k, g in pay.groupby("_cohort")}
    due_by = {k: np.sort(g["age"].to_numpy(dtype=float)) for k, g in due.groupby("_cohort")}

    rows = []
    for cohort, stat in cohort_stats.iterrows():
        if stat["loans"] < min_cohort_loans:
            continue
        orig_total = float(stat["orig_total"]); total_installs = float(stat["total_installs"])
        if orig_total <= 0 or total_installs <= 0 or cohort not in pay_by or cohort not in due_by:
            continue
        final = float(cohort_final[cohort])
        pg = pay_by[cohort]
        pa = pg["age"].to_numpy(dtype=float)
        cum_amt = np.cumsum(pg["amt"].to_numpy(dtype=float))
        da = due_by[cohort]

        # cumulative paid $ at each grid age, and # installments due at each grid age
        paid_idx = np.searchsorted(pa, age_arr, side="right")
        realized = np.where(paid_idx > 0, cum_amt[np.clip(paid_idx - 1, 0, len(cum_amt) - 1)], 0.0) / orig_total
        due_cnt = np.searchsorted(da, age_arr, side="right")
        w_bar = np.clip(due_cnt / total_installs, 0.0, 1.0)

        seg = cohort[0]
        cohort_rest = {c: cohort[i + 1] for i, c in enumerate(cohort_cols)}
        for j, t in enumerate(age_arr):
            rows.append({
                segment_col: seg, **cohort_rest,
                "age_days": float(t), "w_bar": float(w_bar[j]),
                "realized": float(realized[j]), "final": final,
                "remaining": float(final - realized[j]),
                "loans": int(stat["loans"]), "orig_total": orig_total,
            })
    return pd.DataFrame(rows)


def build_development_curve(
    trajectories: pd.DataFrame,
    segment_col: str = "CustType",
    n_bins: int = 20,
    q_lo: float = 0.05,
    q_hi: float = 0.95,
    min_points: int = 20,
) -> pd.DataFrame:
    """Bin cohort trajectories by ``w_bar`` and summarise the remaining-payin tail.

    Returns one row per (segment, w_bar bin): the median / low / high quantiles of
    ``remaining = final - realized``. These quantiles, anchored on a live cohort's
    realized-to-date, are the confidence band. By construction the spread shrinks
    to ~0 as ``w_bar -> 1`` because the observed tail vanishes -- a *measured*
    narrowing, not an assumed one.
    """
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    centers = 0.5 * (edges[:-1] + edges[1:])
    out_rows = []
    for seg, g in trajectories.groupby(segment_col, sort=True):
        w = g["w_bar"].to_numpy(dtype=float)
        rem = g["remaining"].to_numpy(dtype=float)
        idx = np.clip(np.digitize(w, edges[1:-1], right=False), 0, n_bins - 1)
        for b in range(n_bins):
            sel = rem[idx == b]
            if sel.size < min_points:
                continue
            out_rows.append({
                segment_col: seg,
                "w_lo": float(edges[b]),
                "w_hi": float(edges[b + 1]),
                "w_mid": float(centers[b]),
                "rem_q_lo": float(np.quantile(sel, q_lo)),
                "rem_median": float(np.quantile(sel, 0.5)),
                "rem_q_hi": float(np.quantile(sel, q_hi)),
                "n_points": int(sel.size),
            })
    curve = pd.DataFrame(out_rows)
    curve.attrs["q_lo"] = q_lo
    curve.attrs["q_hi"] = q_hi
    return curve


def _interp_remaining(curve_seg: pd.DataFrame, w: float) -> tuple[float, float, float]:
    """Interpolate (q_lo, median, q_hi) of remaining at observed-share ``w``.

    Monotone-safe: results are clipped so the tail never widens with maturity and
    never goes negative on the low side (a matured cohort cannot lose payin).
    """
    if curve_seg.empty:
        return (0.0, 0.0, 0.0)
    c = curve_seg.sort_values("w_mid")
    xw = c["w_mid"].to_numpy(dtype=float)
    lo = float(np.interp(w, xw, c["rem_q_lo"].to_numpy(dtype=float)))
    md = float(np.interp(w, xw, c["rem_median"].to_numpy(dtype=float)))
    hi = float(np.interp(w, xw, c["rem_q_hi"].to_numpy(dtype=float)))
    lo = max(lo, 0.0)
    hi = max(hi, lo)
    md = min(max(md, lo), hi)
    return (lo, md, hi)


# ---------------------------------------------------------------------------
# Per-loan model + baseline frame (point is the model; no maturity scaling)
# ---------------------------------------------------------------------------
def build_projection_loan_frame(
    results: dict,
    lookup: LookupTable | None = None,
    baseline_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Per-loan model projection, class-mixture variance, baseline mu, and weight w.

    ``weight_w`` is carried for the CONFIDENCE band only (observed installment
    share); it never scales the point estimate.
    """
    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    payin_matrix = results["payin_matrix"].copy()
    pre_recovery = results["payin_matrix_pre_recovery"].copy()
    posterior_probs = results["posterior_probs"].copy()
    loan_features = results["loan_features"].copy().set_index("LoanID")
    observed = results["observed_outcomes"].copy()

    if baseline_df is None and lookup is not None:
        baseline_df = attach_lookup_baseline(results, lookup)
    base = baseline_df.copy() if baseline_df is not None else None
    if base is not None and "LoanID" in base.columns:
        base = base.set_index("LoanID")

    idx = payin_matrix.index
    P = _normalized_posterior(posterior_probs, idx)
    M = payin_matrix.reindex(idx)[classes].to_numpy(dtype=float)
    M_pre = pre_recovery.reindex(idx)[classes].to_numpy(dtype=float)

    f_ml = (P * M).sum(axis=1)
    var_ml = np.clip((P * M ** 2).sum(axis=1) - f_ml ** 2, 0.0, None)
    pre = (P * M_pre).sum(axis=1)
    rec_ml = np.clip(f_ml - pre, 0.0, None)

    lf = loan_features.reindex(idx)
    r = pd.to_numeric(lf["payin_ratio_realized"], errors="coerce").fillna(0.0).clip(lower=0.0).to_numpy()
    orig = pd.to_numeric(lf["OriginatedAmount"], errors="coerce").fillna(0.0).to_numpy()
    remaining_normal_ml = np.clip(pre - r, 0.0, None)

    if base is not None and "expected_payin" in base.columns:
        mu = pd.to_numeric(base.reindex(idx)["expected_payin"], errors="coerce").to_numpy()
    else:
        mu = np.full(len(idx), np.nan)
    mu = np.where(np.isfinite(mu), mu, f_ml)
    lookup_tier = (
        pd.to_numeric(base.reindex(idx).get("lookup_tier"), errors="coerce").to_numpy()
        if base is not None and "lookup_tier" in base.columns else np.full(len(idx), np.nan)
    )

    obs = observed.reindex(idx)
    last_k = pd.to_numeric(obs["last_observed_k"], errors="coerce").fillna(0.0).to_numpy()
    total = pd.to_numeric(obs["TotalInstallsNumber"], errors="coerce").fillna(0.0).to_numpy()
    with np.errstate(divide="ignore", invalid="ignore"):
        w = np.where(total > 0, last_k / total, 0.0)
    w = np.clip(w, 0.0, 1.0)

    out = pd.DataFrame(index=idx)
    out.index.name = "LoanID"
    out["OriginatedAmount"] = orig
    out["CustType"] = lf["CustType"].astype(str).str.upper().to_numpy()
    out["AppYear"] = lf["AppYear"].to_numpy()
    out["AppWeek"] = lf["AppWeek"].to_numpy()
    out["OriginationDate"] = lf["OriginationDate"].to_numpy()
    out["realized_payin_to_date"] = r
    out["model_final"] = f_ml
    out["model_var"] = var_ml
    out["model_recovery"] = rec_ml
    out["model_remaining_normal"] = remaining_normal_ml
    out["baseline_mu"] = mu
    out["weight_w"] = w
    out["lookup_tier"] = lookup_tier
    return out.reset_index()


# ---------------------------------------------------------------------------
# Cohort rollup: guardrailed point + maturity-driven band + decomposition
# ---------------------------------------------------------------------------
def build_guardrailed_summary(
    loan_frame: pd.DataFrame,
    segment_history: dict,
    dev_curve: pd.DataFrame | None = None,
    group_col: str = "application_year_week",
    segment_col: str = "CustType",
    z: float = Z_90,
    sys_scale: float = 1.0,
) -> pd.DataFrame:
    """$-weighted cohort rollup with the lookup ceiling guardrail and CI band.

    If ``dev_curve`` (from ``build_development_curve``) is supplied, the band is the
    observable development-curve predictive interval (``realized + remaining
    quantiles at w_bar``); otherwise it falls back to the parametric band.
    """
    df = loan_frame.copy()
    df[group_col] = _group_key(df, group_col).values
    df["OriginatedAmount"] = pd.to_numeric(df["OriginatedAmount"], errors="coerce").fillna(0.0)
    use_dev = dev_curve is not None and not dev_curve.empty

    rows = []
    for group, sub in df.groupby(group_col, sort=False):
        weights = sub["OriginatedAmount"]
        wsum = float(weights.sum())
        if wsum <= 0:
            continue
        a = weights.to_numpy() / wsum  # $-share per loan

        seg = str(sub[segment_col].iloc[0]).upper()
        hist = segment_history.get(seg, {})
        ceiling = float(hist.get("ceiling", np.inf))
        sigma_seg = float(hist.get("sigma", 0.0))

        realized = _weighted_average(sub["realized_payin_to_date"], weights)
        model_mean = _weighted_average(sub["model_final"], weights)
        baseline = _weighted_average(sub["baseline_mu"], weights)
        w_bar = _weighted_average(sub["weight_w"], weights)

        # POINT: trust the model; cap only when hotter than lookup, then hard ceiling.
        point = float(model_mean)
        clamped_lookup = False
        if np.isfinite(baseline) and model_mean > baseline + 1e-9:
            point = min(point, baseline)
            clamped_lookup = True
        point_before_ceiling = point
        point = min(point, ceiling)
        clamped_ceiling = point_before_ceiling > ceiling + 1e-9
        point = max(point, realized)
        clamped = bool(model_mean > point + 1e-9)

        band_source = "parametric"
        if use_dev:
            # BAND (preferred): observed remaining-payin tail at this maturity,
            # anchored on realized-to-date. Collapses to 0 as w_bar -> 1.
            curve_seg = dev_curve[dev_curve[segment_col].astype(str).str.upper() == seg]
            rem_lo, rem_md, rem_hi = _interp_remaining(curve_seg, w_bar)
            p05 = realized + rem_lo
            p95 = realized + rem_hi
            # keep the displayed point inside its own observed interval
            p05 = min(p05, point)
            p95 = max(p95, point)
            band_source = "development" if not curve_seg.empty else "parametric"

        if not use_dev or band_source == "parametric":
            # BAND (fallback): systematic (cohort-to-cohort) + diversified.
            div_var = float((a ** 2 * sub["model_var"].to_numpy()).sum())
            sys_half = z * (1.0 - w_bar) * sigma_seg * sys_scale
            div_half = z * np.sqrt(max(div_var, 0.0))
            band_half = float(np.sqrt(sys_half ** 2 + div_half ** 2))
            p05 = max(point - band_half, realized)
            p95 = point + band_half

        # Decomposition of the (guardrailed) point: split remainder by model ratio.
        remainder = max(point - realized, 0.0)
        rec_norm = _weighted_average(sub["model_remaining_normal"], weights)
        rec_rec = _weighted_average(sub["model_recovery"], weights)
        denom = rec_norm + rec_rec
        rec_share = (rec_rec / denom) if denom > 0 else 0.0
        blended_recovery = remainder * rec_share
        blended_remaining_normal = remainder - blended_recovery

        rows.append({
            group_col: group,
            "loans": int(len(sub)),
            "originated_amount": wsum,
            "realized_payin_to_date": realized,
            "baseline_payin": baseline,
            "model_final_payin": model_mean,
            "point_payin": point,
            "ceiling": ceiling,
            "clamped": clamped,
            "clamped_lookup": clamped_lookup,
            "clamped_ceiling": clamped_ceiling,
            "point_p05": p05,
            "point_p95": p95,
            "band_width": p95 - p05,
            "band_source": band_source,
            "remaining_normal": blended_remaining_normal,
            "recovery": blended_recovery,
            "avg_weight_w": w_bar,
            "avg_lookup_tier": _weighted_average(sub["lookup_tier"].fillna(0), weights),
        })
    return _sort_group_frame(pd.DataFrame(rows), group_col)


def build_guardrailed_cohort_view(
    results: dict,
    segment_history: dict,
    lookup: LookupTable | None = None,
    baseline_df: pd.DataFrame | None = None,
    dev_curve: pd.DataFrame | None = None,
    group_col: str = "application_year_week",
    segment_col: str = "CustType",
    z: float = Z_90,
    sys_scale: float = 1.0,
) -> dict:
    """One-call: per-loan frame + guardrailed cohort summary.

    Pass ``dev_curve`` (from ``build_development_curve``) to use the observable
    development band; omit it to fall back to the parametric band.
    """
    loan_frame = build_projection_loan_frame(results, lookup=lookup, baseline_df=baseline_df)
    summary = build_guardrailed_summary(
        loan_frame, segment_history, dev_curve=dev_curve, group_col=group_col,
        segment_col=segment_col, z=z, sys_scale=sys_scale,
    )
    return {"loan_frame": loan_frame, "summary": summary}


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
def plot_guardrailed_vintage(
    summary: pd.DataFrame,
    group_col: str = "application_year_week",
    title: str = "Projected final payin (model, capped at historical ceiling) vs realized",
    ax=None,
):
    """Realized bars + guardrailed point with maturity-driven band; model and ceiling overlaid."""
    if plt is None:
        raise RuntimeError("matplotlib is unavailable in this environment")
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(11, 5))[1]
    x = np.arange(len(summary))
    labels = summary[group_col].astype(str)

    ax.bar(x, summary["realized_payin_to_date"], color=PRIMARY_COLOR, alpha=0.55, label="Realized to date")
    if {"point_p05", "point_p95"}.issubset(summary.columns):
        _bl = ("90% confidence band (observed remaining-payin tail; narrows as installments mature)"
               if "band_source" in summary.columns and (summary["band_source"] == "development").any()
               else "90% confidence band (narrows as installments mature)")
        ax.fill_between(x, summary["point_p05"], summary["point_p95"], color=ACCENT_COLOR,
                        alpha=0.18, label=_bl)
    ax.plot(x, summary["point_payin"], "o-", color=ACCENT_COLOR, linewidth=2.2, label="Projected final (guardrailed)")
    if "model_final_payin" in summary.columns:
        ax.plot(x, summary["model_final_payin"], "x--", color=PLOT_PALETTE[3], linewidth=1.2,
                alpha=0.9, label="Model only (pre-guardrail)")
    if "baseline_payin" in summary.columns:
        ax.plot(x, summary["baseline_payin"], "^-", color=PLOT_PALETTE[1], linewidth=1.2,
                alpha=0.85, label="Lookup baseline (cohort $-weighted)")
    if "ceiling" in summary.columns:
        ax.plot(x, summary["ceiling"], ":", color="#666666", linewidth=1.2, label="Hard ceiling (p95 cohort means)")

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title(title)
    ax.legend(loc="best", fontsize=8)
    cl_lookup = summary["clamped_lookup"].to_numpy() if "clamped_lookup" in summary.columns else summary["clamped"].to_numpy()
    for xi, yv, cl in zip(x, summary["point_payin"].to_numpy(dtype=float), cl_lookup):
        if not np.isfinite(yv):
            continue
        ax.annotate(f"{yv:.2f}{'*' if cl else ''}", xy=(float(xi), float(yv)), xytext=(0, 9),
                    textcoords="offset points", ha="center", va="bottom", fontsize=8, color=ACCENT_COLOR)
    ax.margins(y=0.10)
    return ax


# ---------------------------------------------------------------------------
# Evidence artifacts: convergence funnel + coverage backtest
# ---------------------------------------------------------------------------
def plot_convergence_funnel(
    trajectories: pd.DataFrame,
    dev_curve: pd.DataFrame,
    segment: str,
    segment_col: str = "CustType",
    title: str | None = None,
    ax=None,
):
    """Show historical cohorts' realized payin funnelling toward their finals.

    Each grey line is one matured cohort's realized-payin path against maturity
    ``w_bar``; the orange band is the development predictive interval
    (median final +/- the remaining-tail quantiles). This is the proof the band
    narrows: the cohorts visibly converge as ``w_bar -> 1``.
    """
    if plt is None:
        raise RuntimeError("matplotlib is unavailable in this environment")
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(10, 5.5))[1]
    seg = str(segment).upper()
    tr = trajectories[trajectories[segment_col].astype(str).str.upper() == seg]
    cur = dev_curve[dev_curve[segment_col].astype(str).str.upper() == seg].sort_values("w_mid")

    cohort_keys = [c for c in ("AppYear", "AppWeek") if c in tr.columns]
    for _, path in tr.groupby(cohort_keys, sort=False) if cohort_keys else [((), tr)]:
        p = path.sort_values("w_bar")
        ax.plot(p["w_bar"], p["realized"], color="#9aa0a6", alpha=0.25, linewidth=0.8)

    if not cur.empty:
        w = cur["w_mid"].to_numpy(dtype=float)
        # Typical realized-to-date in each curve bin, so the band sits on the real path.
        typ_real = np.array([
            float(tr.loc[(tr["w_bar"] >= row.w_lo) & (tr["w_bar"] < row.w_hi), "realized"].median())
            if ((tr["w_bar"] >= row.w_lo) & (tr["w_bar"] < row.w_hi)).any()
            else np.nan
            for row in cur.itertuples()
        ])
        lo = typ_real + cur["rem_q_lo"].to_numpy(dtype=float)
        hi = typ_real + cur["rem_q_hi"].to_numpy(dtype=float)
        ax.fill_between(w, lo, hi, color=ACCENT_COLOR, alpha=0.20,
                        label="Development band (realized + remaining-tail quantiles)")
        ax.plot(w, typ_real + cur["rem_median"].to_numpy(dtype=float), color=ACCENT_COLOR,
                linewidth=2.0, label="Typical projected final")

    finals = tr.groupby(cohort_keys)["final"].first() if cohort_keys else tr["final"]
    ax.axhline(float(np.nanmedian(finals)), color="#444444", linestyle=":", linewidth=1.0,
               label="Median matured final")
    ax.set_xlabel("Observed-installment share (w\u0304)")
    ax.set_ylabel("Payin ratio")
    ax.set_title(title or f"{seg} — convergence funnel (matured cohorts -> final)")
    ax.set_xlim(0, 1)
    ax.legend(loc="best", fontsize=8)
    return ax


def compute_band_coverage(
    trajectories: pd.DataFrame,
    dev_curve: pd.DataFrame,
    segment_col: str = "CustType",
    n_report_bins: int = 5,
) -> pd.DataFrame:
    """Backtest: did the realized final land inside the development band?

    For every (cohort, age) trajectory point we form the band from the curve at
    that ``w_bar`` (``realized + [q_lo, q_hi]``) and check whether the cohort's
    final fell inside. Equivalently, whether ``remaining`` sat within
    ``[q_lo, q_hi]``. Reported as coverage by maturity bin. With a ``q_lo/q_hi``
    of 0.05/0.95 a well-calibrated band covers ~90%. Use a train/test split on
    ``trajectories`` (older cohorts to build ``dev_curve``, newer to score here)
    for an honest, out-of-sample read.
    """
    q_lo = float(dev_curve.attrs.get("q_lo", 0.05))
    q_hi = float(dev_curve.attrs.get("q_hi", 0.95))
    rows = []
    edges = np.linspace(0.0, 1.0, n_report_bins + 1)
    for seg, g in trajectories.groupby(segment_col, sort=True):
        cur = dev_curve[dev_curve[segment_col].astype(str).str.upper() == str(seg).upper()]
        if cur.empty:
            continue
        gg = g.copy()
        interp = gg["w_bar"].map(lambda w: _interp_remaining(cur, float(w)))
        gg["lo"] = [t[0] for t in interp]
        gg["hi"] = [t[2] for t in interp]
        gg["inside"] = (gg["remaining"] >= gg["lo"] - 1e-9) & (gg["remaining"] <= gg["hi"] + 1e-9)
        gg["mbin"] = np.clip(np.digitize(gg["w_bar"], edges[1:-1]), 0, n_report_bins - 1)
        for b, sub in gg.groupby("mbin"):
            rows.append({
                segment_col: seg,
                "w_lo": float(edges[b]),
                "w_hi": float(edges[b + 1]),
                "n_points": int(len(sub)),
                "coverage": float(sub["inside"].mean()),
                "target": q_hi - q_lo,
                "mean_band_width": float((sub["hi"] - sub["lo"]).mean()),
            })
    return pd.DataFrame(rows)


def plot_guardrailed_decomposition(
    summary: pd.DataFrame,
    group_col: str = "application_year_week",
    title: str = "Projected payin decomposition (realized + remaining + recovery)",
    ax=None,
):
    """Stacked realized + remaining-normal + recovery to the guardrailed point; model overlaid."""
    if plt is None:
        raise RuntimeError("matplotlib is unavailable in this environment")
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(11, 5))[1]
    x = np.arange(len(summary))
    labels = summary[group_col].astype(str)

    realized = summary["realized_payin_to_date"]
    remaining = summary["remaining_normal"]
    recovery = summary["recovery"]

    ax.bar(x, realized, color=PRIMARY_COLOR, alpha=0.75, label="Realized to date")
    ax.bar(x, remaining, bottom=realized, color=ACCENT_COLOR, alpha=0.65, label="Projected remaining normal")
    ax.bar(x, recovery, bottom=realized + remaining, color=PLOT_PALETTE[2], alpha=0.65, label="Projected recovery")
    ax.plot(x, summary["point_payin"], color="black", marker="o", linewidth=1.6, label="Projected final (guardrailed)")
    if "model_final_payin" in summary.columns:
        ax.plot(x, summary["model_final_payin"], color=PLOT_PALETTE[3], marker="x", linestyle="--",
                linewidth=1.2, alpha=0.9, label="Model only (pre-guardrail)")

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title(title)
    ax.legend(loc="best", fontsize=8)
    for xi, yv in zip(x, summary["point_payin"].to_numpy(dtype=float)):
        if not np.isfinite(yv):
            continue
        ax.annotate(f"{yv:.2f}", xy=(float(xi), float(yv)), xytext=(0, 9),
                    textcoords="offset points", ha="center", va="bottom", fontsize=8, color="black")
    ax.margins(y=0.10)
    return ax
