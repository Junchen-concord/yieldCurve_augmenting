"""Monte Carlo simulator + Bayesian update for the payin projection pipeline (MVP).

Philosophy
----------
For the stake-in-the-ground MVP we project payin ratio via an empirical
class-conditional approach, not a full per-installment simulation:

  E[payin_ratio | loan] = Σ_c  P(class=c | features)  *  payin_lookup[c]
  Var[...] comes from sampling the multinomial N times.

The confidence-narrowing story is driven by the posterior-update step:
as installments are observed, incompatible classes lose probability mass,
and the re-sampled distribution tightens.

Stage B + C are NOT yet used inside the MC trajectory here -- they are
reported alongside for transparency. The MVP prioritizes the end-to-end
wiring over per-installment realism; Stage B will be folded into the
trajectory generator in the next iteration.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

import numpy as np
import pandas as pd

from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER
from .projection_stage_a import StageAModel, predict_proba


# Class -> default-installment map used by the Stage B trajectory integration.
# xPD classes default at the matching installment; LatePD fixed at 4
# (conservative MVP); Clean never defaults (None => sum all installments).
DEFAULT_INST_BY_CLASS: Dict[str, Optional[int]] = {
    "FPD": 1,
    "SPD": 2,
    "TPD": 3,
    "LatePD": 4,
    "Clean": None,
}


@dataclass
class ClassPayinLookup:
    """Empirical payin-ratio reference keyed by (collapsed) payoff_type.

    Two views on the same training data:
      - `table`  : summary stats per class (n, mean, std, q05, q50, q95)
                   used for QC / reporting / stakeholder-facing printouts.
      - `values` : the raw payin values per class, used as the bootstrap
                   pool for the Monte Carlo sampler.
    """
    table: pd.DataFrame
    values: Dict[str, np.ndarray] = field(default_factory=dict)


def build_class_payin_lookup(
    loan_base: pd.DataFrame,
    payin_col: str = "payin_ratio_realized",
    class_col: str = "payoff_type_collapsed",
    eligible_col: str = "is_training_eligible",
    holdout_col: str = "is_holdout",
) -> ClassPayinLookup:
    """Compute class-conditional payin summaries AND raw value pools
    from the training slice only. The raw pools power the bootstrap
    sampler (`_sample_payin_given_class`); the summary stats remain
    for inspection / reporting.
    """
    train = loan_base[(loan_base[eligible_col] == True) & (~loan_base[holdout_col].astype(bool))]  # noqa: E712
    rows = []
    values: Dict[str, np.ndarray] = {}
    for cls in PAYOFF_TYPE_COLLAPSED_ORDER:
        sub = train.loc[train[class_col] == cls, payin_col].dropna()
        rows.append({
            "class": cls,
            "n": int(len(sub)),
            "mean": float(sub.mean()) if len(sub) else 0.0,
            "std": float(sub.std(ddof=0)) if len(sub) else 0.0,
            "q05": float(sub.quantile(0.05)) if len(sub) else 0.0,
            "q50": float(sub.quantile(0.50)) if len(sub) else 0.0,
            "q95": float(sub.quantile(0.95)) if len(sub) else 0.0,
        })
        values[cls] = sub.to_numpy(dtype=float, copy=True)
    return ClassPayinLookup(table=pd.DataFrame(rows), values=values)


def _sample_payin_given_class(
    cls: str,
    lookup: ClassPayinLookup,
    rng: np.random.Generator,
) -> float:
    """Sample a payin ratio consistent with a terminal class.

    Bootstrap draw from the class's empirical training values.
    This replaces the previous Normal(mean, std) + clip approach
    which required a magic-number cap. By construction, bootstrap
    values are always within the real data envelope, so no clip
    is needed and the per-class distribution shape (skew, bimodality,
    etc.) is preserved exactly.

    Returns 0.0 if the class has no training examples.
    """
    vals = lookup.values.get(cls)
    if vals is None or len(vals) == 0:
        return 0.0
    return float(rng.choice(vals))


def simulate_loan_payin(
    stage_a_probs_row: np.ndarray,
    lookup: ClassPayinLookup,
    n_sims: int = 500,
    rng: Optional[np.random.Generator] = None,
) -> np.ndarray:
    """Draw n_sims payin_ratio samples for a single loan given its Stage A posterior."""
    rng = rng or np.random.default_rng()
    probs = stage_a_probs_row / stage_a_probs_row.sum()
    class_draws = rng.choice(PAYOFF_TYPE_COLLAPSED_ORDER, size=n_sims, p=probs)
    return np.array([_sample_payin_given_class(c, lookup, rng) for c in class_draws])


def simulate_portfolio_payin(
    loan_df: pd.DataFrame,
    stage_a_model: StageAModel,
    lookup: ClassPayinLookup,
    n_sims: int = 500,
    random_state: int = 42,
) -> pd.DataFrame:
    """Monte Carlo simulation across a portfolio. Returns per-loan mean + CI.

    loan_df must carry the Stage A features and an `OriginatedAmount` column for $-weighting.
    """
    rng = np.random.default_rng(random_state)
    probs = predict_proba(stage_a_model, loan_df).values  # shape (n_loans, n_classes)

    means, lows, highs = [], [], []
    for i in range(len(loan_df)):
        sims = simulate_loan_payin(probs[i], lookup, n_sims=n_sims, rng=rng)
        means.append(sims.mean())
        lows.append(np.quantile(sims, 0.05))
        highs.append(np.quantile(sims, 0.95))

    out = loan_df[["LoanID", "OriginatedAmount"]].copy()
    out["pred_payin_mean"] = means
    out["pred_payin_lo05"] = lows
    out["pred_payin_hi95"] = highs
    out["ci_width"] = out["pred_payin_hi95"] - out["pred_payin_lo05"]
    return out


def compatibility_mask_after_k(
    observed_outcomes_by_k: pd.DataFrame,
) -> pd.DataFrame:
    """Given the observed installment outcomes up to installment k, mark which terminal classes remain compatible.

    observed_outcomes_by_k : DataFrame indexed by LoanID with columns
        - last_observed_k : int, the latest matured installment index
        - defaulted_at : int | NaN, installment number where isLoanDefault=1 observed (NaN if not defaulted yet)
        - cleanly_finished : bool, whether the loan has cleanly paid off by last_observed_k

    Returns DataFrame indexed by LoanID with one boolean column per class in PAYOFF_TYPE_COLLAPSED_ORDER.
    """
    classes = PAYOFF_TYPE_COLLAPSED_ORDER
    compat = pd.DataFrame(True, index=observed_outcomes_by_k.index, columns=classes)

    # Per-loan compatibility logic.
    for lid, row in observed_outcomes_by_k.iterrows():
        k = int(row["last_observed_k"])
        defaulted_at = row.get("defaulted_at", np.nan)
        cleanly_finished = bool(row.get("cleanly_finished", False))

        default_inst = int(defaulted_at) if pd.notna(defaulted_at) else None

        if default_inst is not None:
            # We saw a terminal default at exactly this installment.
            cls_map = {1: "FPD", 2: "SPD", 3: "TPD"}
            obs_cls = cls_map.get(default_inst, "LatePD")
            compat.loc[lid, :] = False
            compat.loc[lid, obs_cls] = True
        elif cleanly_finished:
            compat.loc[lid, :] = False
            compat.loc[lid, "Clean"] = True
        else:
            # In-flight: haven't defaulted and haven't finished. Eliminate classes already ruled out.
            #   - If k>=1, FPD is ruled out (we would have seen a default at installment 1).
            #   - If k>=2, SPD is ruled out. ... and so on.
            #   - Clean is still possible (loan may finish cleanly).
            if k >= 1:
                compat.loc[lid, "FPD"] = False
            if k >= 2:
                compat.loc[lid, "SPD"] = False
            if k >= 3:
                compat.loc[lid, "TPD"] = False
    return compat


def bayes_update_stage_a(
    prior_probs: pd.DataFrame,
    observed_outcomes_by_k: pd.DataFrame,
) -> pd.DataFrame:
    """Update Stage A posterior given observed installment outcomes.

    MVP implementation: hard-compatibility zeroing + renormalization. This is a
    'likelihood = indicator' shortcut that's exact for the class definitions above.
    A full soft-likelihood update (using Stage B) is the next iteration.
    """
    compat = compatibility_mask_after_k(observed_outcomes_by_k)
    compat = compat.reindex(prior_probs.index).fillna(True)

    prob_cols = [f"P_{c}" for c in PAYOFF_TYPE_COLLAPSED_ORDER]
    posterior = prior_probs.copy()
    for cls, col in zip(PAYOFF_TYPE_COLLAPSED_ORDER, prob_cols):
        posterior[col] = np.where(compat[cls].values, posterior[col].values, 0.0)

    row_sums = posterior[prob_cols].sum(axis=1).replace(0, np.nan)
    posterior[prob_cols] = posterior[prob_cols].div(row_sums, axis=0).fillna(1.0 / len(prob_cols))
    return posterior


def observed_outcomes_from_seq_base(
    seq_base: pd.DataFrame,
    loan_base: pd.DataFrame,
    k: int,
) -> pd.DataFrame:
    """Derive per-loan 'what has been observed by installment k' from seq_base + loan_base.

    Useful for the confidence-narrowing demo: sweep k = 1..max and watch the CI tighten.
    """
    seen = seq_base[seq_base["InstallmentNumber"] <= k].copy()

    # Flag any isLoanDefault = 1 events observed on or before k.
    defaulted = (
        seen.loc[seen["isLoanDefault"] == 1]
        .groupby("LoanID")["InstallmentNumber"]
        .min()
        .rename("defaulted_at")
    )
    last_k = seen.groupby("LoanID")["InstallmentNumber"].max().rename("last_observed_k")

    # Cleanly finished if the loan's TotalInstallsNumber <= k AND no default observed
    # AND LoanPaidOffThisInstall=1 was seen.
    paid_off_seen = (
        seen.loc[seen["LoanPaidOffThisInstall"] == 1]
        .groupby("LoanID")["InstallmentNumber"]
        .min()
        .notna()
    )

    out = loan_base[["LoanID", "TotalInstallsNumber"]].set_index("LoanID")
    out = out.join(last_k, how="left")
    out = out.join(defaulted, how="left")
    out["last_observed_k"] = out["last_observed_k"].fillna(0).astype(int)
    out["cleanly_finished"] = out.index.isin(paid_off_seen.index[paid_off_seen.values])
    out["cleanly_finished"] = out["cleanly_finished"] & (out["TotalInstallsNumber"] <= k)
    return out


# ---------------------------------------------------------------------------
# Tier 1 — Stage B folded into the MC trajectory.
# ---------------------------------------------------------------------------


def _capped_model_amount(
    preds: pd.DataFrame,
    seq: pd.DataFrame,
    use_unconditional: bool,
    cap_at_due: bool,
) -> np.ndarray:
    """Per-row model amount for the payin matrices.

    use_unconditional (default False): multiply by marginal p_collected.
    2026-06-11: default flipped back to CONDITIONAL. The matrix cells are
    class-conditional paths ("payin GIVEN the loan ends Clean") and Stage A's
    class probabilities already price default risk -- multiplying by the
    marginal p_collected double-counts it (live points collapsed to ~1.1).
    The day-zero spike was the regressor's inflated amounts, which cap_at_due
    fixes on its own.

    cap_at_due: cap each row at its contractual InstallmentDueAmount when the
    column is present. A normal-stream installment cannot be expected to
    collect more than the schedule asks for; this also stops early-payoff
    lumps learned by the regressor from being counted once per future row.
    Skipped when the column is absent (older extracts).
    """
    amount_col = "e_amount" if use_unconditional else "e_amount_if_collected"
    amount = preds[amount_col].to_numpy(dtype=float)
    if cap_at_due and "InstallmentDueAmount" in seq.columns:
        due = pd.to_numeric(seq["InstallmentDueAmount"], errors="coerce").to_numpy(dtype=float)
        has_due = np.isfinite(due) & (due > 0)
        amount = np.where(has_due, np.minimum(amount, due), amount)
    return amount


def build_loan_class_payin_matrix(
    holdout_loans: pd.DataFrame,
    holdout_seq: pd.DataFrame,
    stage_b_model,
    default_inst_by_class: Optional[Dict[str, Optional[int]]] = None,
    use_unconditional: bool = False,
    cap_at_due: bool = True,
) -> pd.DataFrame:
    """Pre-compute per-(loan, class) expected payin from Stage B predictions.

    For each class c with default installment d(c):
      - Clean (d = None):  sum the per-row model amount over all installments.
      - xPD  (d = int):    sum over installments 1..d-1 only (pre-default).
    The matrix is computed once and re-used across all MC sims + all k-values,
    so Stage B scoring stays O(n_installments) rather than O(n_sims * n_loans).

    2026-06-11: rows use the CONDITIONAL amount capped at the contractual
    InstallmentDueAmount (class-conditional semantics; see _capped_model_amount
    and skills/0610_dayzero_projection_fix_plan_v1.md).
    """
    from .projection_stage_b import predict_expected_amount

    dmap = default_inst_by_class or DEFAULT_INST_BY_CLASS
    preds = predict_expected_amount(stage_b_model, holdout_seq)

    scored = holdout_seq[["LoanID", "InstallmentNumber"]].copy()
    scored["model_amount"] = _capped_model_amount(preds, holdout_seq, use_unconditional, cap_at_due)

    orig = holdout_loans.set_index("LoanID")["OriginatedAmount"].astype(float)
    matrix = pd.DataFrame(0.0, index=orig.index, columns=PAYOFF_TYPE_COLLAPSED_ORDER)

    for cls in PAYOFF_TYPE_COLLAPSED_ORDER:
        d = dmap.get(cls)
        sub = scored if d is None else scored[scored["InstallmentNumber"] < d]
        summed = sub.groupby("LoanID")["model_amount"].sum()
        matrix[cls] = (summed / orig).reindex(matrix.index).fillna(0.0)

    return matrix


def build_live_loan_class_payin_matrix(
    loan_features: pd.DataFrame,
    seq_features: pd.DataFrame,
    stage_b_model,
    default_inst_by_class: Optional[Dict[str, Optional[int]]] = None,
    use_unconditional: bool = False,
    cap_at_due: bool = True,
    floor_at_realized: bool = True,
) -> pd.DataFrame:
    """Build a live inference payin matrix anchored to observed dollars.

    Observed installments use actual collected dollars as of the valuation date.
    Future installments use Stage B's CONDITIONAL expected amount capped at the
    contractual InstallmentDueAmount when available (class-conditional
    semantics; see _capped_model_amount). Each terminal class still controls
    how far the normal-payment stream is allowed to run.

    floor_at_realized: every class path is floored at the loan's realized payin
    to date -- dollars already collected cannot be lost whatever class the loan
    ends in (closes the Confluence "floor class-conditional paths" gap).
    """
    from .projection_stage_b import predict_expected_amount

    dmap = default_inst_by_class or DEFAULT_INST_BY_CLASS
    orig = loan_features.set_index("LoanID")["OriginatedAmount"].astype(float)
    matrix = pd.DataFrame(0.0, index=orig.index, columns=PAYOFF_TYPE_COLLAPSED_ORDER)
    if seq_features.empty:
        return matrix

    preds = predict_expected_amount(stage_b_model, seq_features)
    scored = seq_features[["LoanID", "InstallmentNumber", "collected_amount_k"]].copy()
    scored["is_observed"] = (
        seq_features["is_observed"].astype(bool).values
        if "is_observed" in seq_features.columns
        else True
    )
    scored["model_amount"] = _capped_model_amount(preds, seq_features, use_unconditional, cap_at_due)
    scored["projected_amount"] = np.where(
        scored["is_observed"],
        pd.to_numeric(scored["collected_amount_k"], errors="coerce").fillna(0.0),
        scored["model_amount"],
    )

    for cls in PAYOFF_TYPE_COLLAPSED_ORDER:
        d = dmap.get(cls)
        sub = scored if d is None else scored[scored["InstallmentNumber"] < d]
        summed = sub.groupby("LoanID")["projected_amount"].sum()
        matrix[cls] = (summed / orig.replace(0, np.nan)).reindex(matrix.index).fillna(0.0)

    matrix = matrix.clip(lower=0.0)
    if floor_at_realized and "payin_ratio_realized" in loan_features.columns:
        realized = (
            pd.to_numeric(
                loan_features.set_index("LoanID")["payin_ratio_realized"], errors="coerce"
            )
            .reindex(matrix.index)
            .fillna(0.0)
            .clip(lower=0.0)
        )
        matrix = matrix.clip(lower=realized, axis=0)
    return matrix


def simulate_portfolio_ci_stage_b(
    probs_df: pd.DataFrame,
    orig_amts: pd.Series,
    payin_matrix: pd.DataFrame,
    n_sims: int = 500,
    rng_seed: int = 42,
) -> dict:
    """Dollar-weighted portfolio payin MC via Stage-B-derived (loan, class) payins.

    Identical control flow to the earlier class-bootstrap sampler; the only
    change is the lookup source: instead of drawing a payin from the class's
    empirical pool, we read the pre-computed per-loan payin for the sampled
    class from `payin_matrix`.
    """
    rng = np.random.default_rng(rng_seed)
    classes = PAYOFF_TYPE_COLLAPSED_ORDER

    probs_df = probs_df.reindex(payin_matrix.index)
    orig = orig_amts.reindex(payin_matrix.index).astype(float).values
    orig_total = orig.sum()

    P = probs_df.values
    P = P / P.sum(axis=1, keepdims=True).clip(min=1e-12)
    cumP = np.cumsum(P, axis=1)

    M = payin_matrix[classes].values
    n_loans = len(P)

    sim_means = np.empty(n_sims)
    for s in range(n_sims):
        u = rng.random(n_loans)
        class_idx = (cumP < u[:, None]).sum(axis=1).clip(max=len(classes) - 1)
        payins = M[np.arange(n_loans), class_idx]
        sim_means[s] = (orig * payins).sum() / max(orig_total, 1e-9)

    return {
        "mean": float(sim_means.mean()),
        "lo05": float(np.quantile(sim_means, 0.05)),
        "hi95": float(np.quantile(sim_means, 0.95)),
        "sims": sim_means,
    }


# ---------------------------------------------------------------------------
# Tier 3 — Stage C post-default recovery composed onto the payin matrix.
# ---------------------------------------------------------------------------


def apply_stage_c_recovery(
    payin_matrix: pd.DataFrame,
    recovery_fraction_matrix: pd.DataFrame,
) -> pd.DataFrame:
    """Add Stage C post-default recovery on top of the Stage B payin matrix.

    For each (loan, class) cell:

        new_payin = old_payin + recovery_fraction * (1 - old_payin)

    where `1 - old_payin` is the outstanding-fraction proxy (principal not yet
    collected pre-default under that class) and `recovery_fraction` is the
    expected terminal recovery fraction of outstanding. Clean should arrive
    with recovery_fraction = 0 so that column is unchanged.

    The function is pure: Option 1 (class broadcast) and Option 2 (per-loan
    recovery model) both produce a DataFrame of the same shape and flow
    through this same call.

    The outstanding proxy is clipped at zero: when a path already sits at or
    above 1.0 (e.g. floored at realized payin > 1), there is no outstanding
    principal left to recover -- recovery must never SUBTRACT payin.
    """
    if not payin_matrix.index.equals(recovery_fraction_matrix.index):
        raise ValueError("payin_matrix and recovery_fraction_matrix index mismatch")
    if not payin_matrix.columns.equals(recovery_fraction_matrix.columns):
        raise ValueError("payin_matrix and recovery_fraction_matrix column mismatch")

    return payin_matrix + recovery_fraction_matrix * (1.0 - payin_matrix).clip(lower=0.0)


# ---------------------------------------------------------------------------
# Tier 2 — Soft Bayesian update using Stage B P(collected) as likelihood.
# ---------------------------------------------------------------------------


def bayes_update_stage_a_soft(
    prior_probs: pd.DataFrame,
    observed_outcomes_by_k: pd.DataFrame,
    stage_b_scored_seq: pd.DataFrame,
    default_inst_by_class: Optional[Dict[str, Optional[int]]] = None,
) -> pd.DataFrame:
    """Bayesian update with a soft likelihood from Stage B's P(collected).

    For each class c with hard-compat = True, likelihood is the Bernoulli
    product over the class's alive installments:

        log P(y_1..y_k | c) = sum_i in alive(c,k)  log( y_i p_i + (1-y_i)(1-p_i) )

    where p_i = Stage B p_collected at (LoanID, InstallmentNumber = i) and
    y_i = observed collected_flag_k. Hard-incompatible classes are zeroed
    via `compatibility_mask_after_k`. For Clean the alive set is all
    observed installments; for xPD it is installments 1..d(c)-1.

    Parameters
    ----------
    prior_probs : DataFrame
        Stage A posterior, indexed by LoanID, columns = `P_<class>`.
    observed_outcomes_by_k : DataFrame
        Output of `observed_outcomes_from_seq_base`, indexed by LoanID.
    stage_b_scored_seq : DataFrame
        Installment-level rows for the holdout, with columns
        `LoanID`, `InstallmentNumber`, `p_collected`, `collected_flag_k`.
    """
    dmap = default_inst_by_class or DEFAULT_INST_BY_CLASS
    classes = PAYOFF_TYPE_COLLAPSED_ORDER
    prob_cols = [f"P_{c}" for c in classes]

    compat = compatibility_mask_after_k(observed_outcomes_by_k)
    compat = compat.reindex(prior_probs.index).fillna(True)

    last_obs = (
        observed_outcomes_by_k["last_observed_k"]
        .reindex(prior_probs.index)
        .fillna(0)
        .astype(int)
        .values
    )
    k_max = int(last_obs.max()) if len(last_obs) else 0
    if k_max == 0:
        # Nothing observed yet -> soft update reduces to the prior (hard mask only).
        return bayes_update_stage_a(prior_probs, observed_outcomes_by_k)

    # Build dense (n_loans, k_max) arrays of p_collected and observed collected_flag_k.
    p_wide = (
        stage_b_scored_seq.pivot_table(
            index="LoanID", columns="InstallmentNumber",
            values="p_collected", aggfunc="first",
        )
        .reindex(prior_probs.index)
    )
    y_wide = (
        stage_b_scored_seq.pivot_table(
            index="LoanID", columns="InstallmentNumber",
            values="collected_flag_k", aggfunc="first",
        )
        .reindex(prior_probs.index)
    )

    inst_cols = list(range(1, k_max + 1))
    p_wide = p_wide.reindex(columns=inst_cols)
    y_wide = y_wide.reindex(columns=inst_cols)

    observed_mask = ~y_wide.isna().values  # (n_loans, k_max) — True where we actually saw the row
    p_mat = np.clip(p_wide.values.astype(float), 1e-6, 1 - 1e-6)
    y_mat = np.nan_to_num(y_wide.values.astype(float), nan=0.0)

    # Per-row Bernoulli log-likelihood.
    ll_per = y_mat * np.log(p_mat) + (1.0 - y_mat) * np.log(1.0 - p_mat)

    k_range = np.arange(1, k_max + 1)  # (k_max,)
    horizon_mask = (k_range[None, :] <= last_obs[:, None]) & observed_mask  # (n_loans, k_max)

    n_loans = len(prior_probs)
    log_lik = np.zeros((n_loans, len(classes)))
    for ci, cls in enumerate(classes):
        d = dmap.get(cls)
        alive = horizon_mask if d is None else (horizon_mask & (k_range[None, :] < d))
        log_lik[:, ci] = np.where(alive, ll_per, 0.0).sum(axis=1)

    prior = prior_probs.reindex(prior_probs.index)[prob_cols].values
    log_prior = np.log(np.clip(prior, 1e-12, None))
    log_post = log_prior + log_lik

    compat_mat = compat[classes].reindex(prior_probs.index).values.astype(bool)
    log_post = np.where(compat_mat, log_post, -np.inf)

    row_max = np.max(log_post, axis=1, keepdims=True)
    row_max = np.where(np.isfinite(row_max), row_max, 0.0)
    exp_shifted = np.where(np.isfinite(log_post), np.exp(log_post - row_max), 0.0)
    row_sums = exp_shifted.sum(axis=1, keepdims=True)
    posterior = np.where(row_sums > 0, exp_shifted / row_sums, 1.0 / len(classes))

    out = prior_probs.copy()
    out[prob_cols] = posterior
    return out
