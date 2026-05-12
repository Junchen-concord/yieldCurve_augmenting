"""High-level live inference orchestration for persisted projection runs."""
from __future__ import annotations

import numpy as np
import pandas as pd

from .projection_feature_builder import (
    build_loan_features,
    build_observed_outcomes,
    build_seq_features,
    build_stage_c_features,
)
from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER
from .projection_stage_a import predict_proba
from .projection_stage_b import predict_expected_amount
from .projection_stage_c import build_stage_c_recovery_fraction_matrix
from .projection_simulator import (
    apply_stage_c_recovery,
    bayes_update_stage_a_soft,
    build_live_loan_class_payin_matrix,
    simulate_portfolio_ci_stage_b,
)


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, q: float) -> float:
    order = np.argsort(values)
    values = values[order]
    weights = weights[order]
    total = weights.sum()
    if total <= 0:
        return float("nan")
    cdf = np.cumsum(weights) / total
    return float(values[np.searchsorted(cdf, q, side="left").clip(max=len(values) - 1)])


def build_loan_level_projection(
    loan_features: pd.DataFrame,
    posterior_probs: pd.DataFrame,
    payin_matrix: pd.DataFrame,
) -> pd.DataFrame:
    """Return loan-level mean and discrete class-mixture uncertainty."""
    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    prob_cols = [f"P_{c}" for c in classes]
    P = posterior_probs.reindex(payin_matrix.index)[prob_cols].to_numpy(dtype=float)
    P = P / np.clip(P.sum(axis=1, keepdims=True), 1e-12, None)
    M = payin_matrix[classes].to_numpy(dtype=float)

    means = (P * M).sum(axis=1)
    lo05 = np.array([_weighted_quantile(M[i], P[i], 0.05) for i in range(len(M))])
    hi95 = np.array([_weighted_quantile(M[i], P[i], 0.95) for i in range(len(M))])

    out = loan_features.set_index("LoanID").reindex(payin_matrix.index)[
        [
            "OriginatedAmount",
            "TotalRealizedPayment",
            "payin_ratio_realized",
            "OriginationDate",
            "AppYear",
            "AppWeek",
            "CustType",
            "DM_risk_tier",
        ]
    ].copy()
    out["pred_payin_mean"] = means
    out["pred_payin_lo05"] = lo05
    out["pred_payin_hi95"] = hi95
    out["pred_payin_ci_width"] = out["pred_payin_hi95"] - out["pred_payin_lo05"]
    out = out.join(posterior_probs.reindex(payin_matrix.index)[prob_cols])
    return out.reset_index().rename(columns={"index": "LoanID"})


def score_live_projection(
    model_run,
    raw_df: pd.DataFrame,
    payment_normal_df: pd.DataFrame,
    payment_arr_df: pd.DataFrame,
    payment_3p_df: pd.DataFrame,
    as_of_date=None,
    n_sims: int = 500,
    rng_seed: int = 42,
) -> dict:
    """Score a live/recent cohort using a persisted model run."""
    feature_contract = model_run.feature_contract

    loan_features = build_loan_features(raw_df, feature_contract, as_of_date=as_of_date)
    seq_features = build_seq_features(
        raw_df,
        loan_features,
        payment_normal_df,
        feature_contract,
        as_of_date=as_of_date,
    )
    observed = build_observed_outcomes(seq_features, loan_features, as_of_date=as_of_date)

    prior_probs = predict_proba(model_run.stage_a, loan_features)
    prior_probs.index = loan_features["LoanID"].values

    if seq_features.empty:
        stage_b_scored = seq_features.copy()
        posterior_probs = prior_probs.copy()
    else:
        stage_b_preds = predict_expected_amount(model_run.stage_b, seq_features)
        stage_b_scored = seq_features[["LoanID", "InstallmentNumber", "collected_flag_k"]].copy()
        stage_b_scored["p_collected"] = stage_b_preds["p_collected"].values
        posterior_probs = bayes_update_stage_a_soft(prior_probs, observed, stage_b_scored)

    payin_matrix_pre_recovery = build_live_loan_class_payin_matrix(
        loan_features,
        seq_features,
        model_run.stage_b,
    )
    stage_c_features = build_stage_c_features(
        raw_df,
        loan_features,
        seq_features,
        payment_arr_df,
        payment_3p_df,
        feature_contract,
        as_of_date=as_of_date,
    )
    recovery_fraction_matrix = build_stage_c_recovery_fraction_matrix(
        model_run.stage_c,
        loan_features,
        payin_matrix_pre_recovery,
        stage_c_features,
    )
    payin_matrix = apply_stage_c_recovery(payin_matrix_pre_recovery, recovery_fraction_matrix)

    orig = loan_features.set_index("LoanID")["OriginatedAmount"]
    portfolio_ci = simulate_portfolio_ci_stage_b(
        posterior_probs,
        orig,
        payin_matrix,
        n_sims=n_sims,
        rng_seed=rng_seed,
    )
    loan_projection = build_loan_level_projection(loan_features, posterior_probs, payin_matrix)

    qc = {
        "as_of_date": str(pd.to_datetime(as_of_date).date()) if as_of_date is not None else None,
        "n_loans": int(len(loan_features)),
        "n_installment_rows": int(len(seq_features)),
        "n_stage_c_rows": int(len(stage_c_features)),
        "loans_without_payment_attempts": int(
            loan_features["LoanID"].nunique()
            - payment_normal_df["LoanID"].nunique()
            if "LoanID" in payment_normal_df.columns
            else loan_features["LoanID"].nunique()
        ),
        "stage_a_missing_features": [
            c for c in model_run.stage_a.features if c not in loan_features.columns
        ],
        "stage_b_missing_features": [
            c for c in model_run.stage_b.features if c not in seq_features.columns
        ],
        "stage_c_missing_features": [
            c for c in model_run.stage_c.features if c not in stage_c_features.columns
        ],
    }

    return {
        "loan_features": loan_features,
        "seq_features": seq_features,
        "stage_c_features": stage_c_features,
        "observed_outcomes": observed,
        "prior_probs": prior_probs,
        "posterior_probs": posterior_probs,
        "stage_b_scored_seq": stage_b_scored,
        "payin_matrix_pre_recovery": payin_matrix_pre_recovery,
        "recovery_fraction_matrix": recovery_fraction_matrix,
        "payin_matrix": payin_matrix,
        "portfolio_ci": portfolio_ci,
        "loan_projection": loan_projection,
        "qc": qc,
    }
