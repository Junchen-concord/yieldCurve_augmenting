"""Holdout evaluation helpers for persisted projection models."""
from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from .plot_style import ACCENT_COLOR, PLOT_PALETTE, PRIMARY_COLOR, apply_plot_style
from .projection_feature_builder import (
    FREQ_BIZ3_MAP,
    FREQ_NATIVE_MAP,
    build_seq_features,
    build_stage_c_features,
    encode_category,
)
from .projection_labels import PAYOFF_TYPE_COLLAPSED_ORDER, collapse_payoff_type
from .projection_risk_features import add_dm_risk_tier_features
from .projection_stage_a import predict_proba
from .projection_stage_c import build_stage_c_recovery_fraction_matrix
from .projection_simulator import (
    apply_stage_c_recovery,
    build_loan_class_payin_matrix,
    simulate_portfolio_ci_stage_b,
)


INT_COLS = [
    "InstallmentNumber",
    "installStatus",
    "iPaymentMode",
    "TotalInstallsNumber",
    "isRecentLoan",
    "LoanPaidOffThisInstall",
    "isLoanDefault",
    "isInstallDefault",
    "ThirdPartyCollected",
    "PartialCollected",
    "InstallCollected",
    "EarlyCollected",
    "isDenyNew",
    "isAllVoided",
    "isArrangementInstall",
    "is3rdPartyInstall",
    "AppYear",
    "AppMonth",
    "AppWeek",
]
FLOAT_COLS = ["InstallRealizedPayment", "TotalRealizedPayment", "OriginatedAmount"]
DATE_COLS = ["InstallmentDueDate", "PaymentDate", "OriginationDate"]


def _normalize_date(value=None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.today().normalize()
    out = pd.Timestamp(pd.to_datetime(value))
    if out.tzinfo is not None:
        out = out.tz_convert(None)
    return out.normalize()


def prepare_training_raw_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply the same basic cleanup used by V5 before building model bases."""
    out = raw_df.copy()
    for c in INT_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)
    for c in FLOAT_COLS:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype(float)
    for c in DATE_COLS:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    for c in ["CustType", "LoanStatus", "Frequency"]:
        if c in out.columns:
            out[c] = out[c].astype(str).str.upper().str.strip()
    for c in ["DM_Band_Name", "CM_Band_Name"]:
        if c in out.columns:
            out[c] = (
                out[c]
                .where(out[c].notna(), "UNKNOWN")
                .astype(str)
                .str.strip()
                .replace({"": "UNKNOWN", "nan": "UNKNOWN", "None": "UNKNOWN"})
            )

    out = out.drop_duplicates(["LoanID", "InstallmentNumber", "iPaymentMode"])
    out["Frequency_norm"] = out["Frequency"].map(FREQ_NATIVE_MAP)
    out["Frequency_group3"] = out["Frequency_norm"].map(FREQ_BIZ3_MAP)
    return out


def _first_inst_where(df: pd.DataFrame, flag_col: str) -> pd.Series:
    mask = df[flag_col].eq(1)
    if not mask.any():
        return pd.Series(dtype="Int64")
    return df.loc[mask].groupby("LoanID")["InstallmentNumber"].min().astype("Int64")


def derive_loan_flags(
    raw_df: pd.DataFrame,
    min_loan_age_days: int = 120,
    evaluation_as_of_date=None,
) -> pd.DataFrame:
    """Derive V5 payoff labels and training eligibility at loan grain."""
    normal = raw_df[raw_df["iPaymentMode"].eq(144)].copy()
    first_default_inst = _first_inst_where(normal, "isLoanDefault").rename("first_default_inst")
    paidoff_inst = _first_inst_where(normal, "LoanPaidOffThisInstall").rename("paidoff_inst")

    flags = (
        normal.groupby("LoanID", as_index=False)
        .agg(
            is_deny_new_loan=("isDenyNew", "max"),
            is_all_voided_loan=("isAllVoided", "max"),
            is_recent_loan=("isRecentLoan", "max"),
            TotalInstallsNumber=("TotalInstallsNumber", "max"),
            AppYear=("AppYear", "first"),
            AppWeek=("AppWeek", "first"),
        )
        .merge(first_default_inst, on="LoanID", how="left")
        .merge(paidoff_inst, on="LoanID", how="left")
    )
    flags["ApplicationDate"] = (
        pd.to_datetime(flags["AppYear"].astype(str) + "-01-01", errors="coerce")
        + pd.to_timedelta((pd.to_numeric(flags["AppWeek"], errors="coerce").fillna(1) - 1) * 7, unit="D")
    )
    flags = flags.drop(columns=["AppYear", "AppWeek"])

    def _payoff_type(row) -> str:
        if row["is_deny_new_loan"] == 1:
            return "DENY_NEW"
        if row["is_all_voided_loan"] == 1:
            return "ALL_VOIDED"
        d = row["first_default_inst"]
        if pd.notna(d):
            d = int(d)
            if d == 1:
                return "FPD"
            if d == 2:
                return "SPD"
            if d == 3:
                return "TPD"
            if d == 4:
                return "4PD"
            if d == 5:
                return "5PD"
            return "LatePD"
        p = row["paidoff_inst"]
        t = row["TotalInstallsNumber"]
        if pd.notna(p) and pd.notna(t):
            return "Clean_early" if int(p) < int(t) else "Clean_full"
        return "Immature"

    flags["payoff_type"] = flags.apply(_payoff_type, axis=1)
    terminal_buckets = {"FPD", "SPD", "TPD", "4PD", "5PD", "LatePD", "Clean_early", "Clean_full"}
    app_cutoff = _normalize_date(evaluation_as_of_date) - pd.Timedelta(days=min_loan_age_days)
    flags["is_training_eligible"] = (
        flags["payoff_type"].isin(terminal_buckets)
        & flags["is_recent_loan"].eq(0)
        & flags["ApplicationDate"].lt(app_cutoff)
    )
    flags["payoff_type_collapsed"] = collapse_payoff_type(flags["payoff_type"])
    return flags


def build_evaluation_loan_base(
    raw_df: pd.DataFrame,
    feature_contract: dict,
    min_loan_age_days: int = 120,
    evaluation_as_of_date=None,
) -> pd.DataFrame:
    """Build V5-style loan_base with labels, features, and holdout split."""
    flags = derive_loan_flags(
        raw_df,
        min_loan_age_days=min_loan_age_days,
        evaluation_as_of_date=evaluation_as_of_date,
    )
    attrs = (
        raw_df.groupby("LoanID", as_index=False)
        .agg(
            PortFolioID=("PortFolioID", "first"),
            Application_ID=("Application_ID", "first"),
            OriginatedAmount=("OriginatedAmount", "first"),
            OriginationDate=("OriginationDate", "min"),
            TotalRealizedPayment=("TotalRealizedPayment", "first"),
            AppYear=("AppYear", "first"),
            AppMonth=("AppMonth", "first"),
            AppWeek=("AppWeek", "first"),
            LoanStatus=("LoanStatus", "first"),
            CustType=("CustType", "first"),
            Frequency=("Frequency", "first"),
            Frequency_norm=("Frequency_norm", "first"),
            Frequency_group3=("Frequency_group3", "first"),
            DM_Band_Name=("DM_Band_Name", "first"),
            CM_Band_Name=("CM_Band_Name", "first"),
        )
    )
    loan_base = attrs.merge(flags, on="LoanID", how="left")
    loan_base["payin_ratio_realized"] = (
        loan_base["TotalRealizedPayment"] / loan_base["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    maps = feature_contract.get("category_maps", {})
    loan_base["CustType_bin"] = loan_base["CustType"].astype(str).str.upper().str.strip().eq("RETURN").astype(int)
    loan_base["Frequency_enc"] = encode_category(loan_base["Frequency_norm"], maps.get("Frequency_enc"))
    loan_base["Frequency3_enc"] = encode_category(loan_base["Frequency_group3"], maps.get("Frequency3_enc"))
    loan_base["log_orig_amt"] = np.log1p(
        pd.to_numeric(loan_base["OriginatedAmount"], errors="coerce").fillna(0.0).clip(lower=0.0)
    )
    loan_base["month_sin"] = np.sin(2 * np.pi * pd.to_numeric(loan_base["AppMonth"], errors="coerce").fillna(1) / 12.0)
    loan_base["month_cos"] = np.cos(2 * np.pi * pd.to_numeric(loan_base["AppMonth"], errors="coerce").fillna(1) / 12.0)
    loan_base["week_sin"] = np.sin(2 * np.pi * pd.to_numeric(loan_base["AppWeek"], errors="coerce").fillna(1) / 52.0)
    loan_base["week_cos"] = np.cos(2 * np.pi * pd.to_numeric(loan_base["AppWeek"], errors="coerce").fillna(1) / 52.0)
    loan_base = add_dm_risk_tier_features(loan_base, band_col="DM_Band_Name")
    loan_base["CM_Band_Name"] = loan_base["CM_Band_Name"].fillna("UNKNOWN").astype(str)
    loan_base["DM_Band_enc"] = encode_category(loan_base["DM_Band_Name"], maps.get("DM_Band_enc"), normalize_dm=True)
    loan_base["CM_Band_enc"] = encode_category(loan_base["CM_Band_Name"], maps.get("CM_Band_enc"))

    matured_dates = loan_base.loc[loan_base["is_training_eligible"], "OriginationDate"].dropna()
    cutoff_date = loan_base["OriginationDate"].quantile(0.80) if matured_dates.empty else matured_dates.quantile(0.80)
    loan_base["is_holdout"] = loan_base["OriginationDate"].ge(cutoff_date)

    for c in feature_contract.get("stage_a_features", []):
        if c not in loan_base.columns:
            loan_base[c] = 0.0
        loan_base[c] = pd.to_numeric(loan_base[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return loan_base


def score_holdout_projection(
    model_run,
    raw_df: pd.DataFrame,
    payment_normal_df: pd.DataFrame,
    payment_arr_df: pd.DataFrame,
    payment_3p_df: pd.DataFrame,
    min_loan_age_days: int = 120,
    evaluation_as_of_date=None,
) -> dict:
    """Score the V5 holdout set and compare predicted vs actual realized payin."""
    feature_contract = model_run.feature_contract
    raw = prepare_training_raw_df(raw_df)
    loan_base = build_evaluation_loan_base(
        raw,
        feature_contract,
        min_loan_age_days=min_loan_age_days,
        evaluation_as_of_date=evaluation_as_of_date,
    )
    seq_base = build_seq_features(raw, loan_base, payment_normal_df, feature_contract, as_of_date=None)
    stage_c_features = build_stage_c_features(
        raw,
        loan_base,
        seq_base,
        payment_arr_df,
        payment_3p_df,
        feature_contract,
        as_of_date=None,
    )

    holdout_loans = loan_base[loan_base["is_training_eligible"] & loan_base["is_holdout"]].copy()
    holdout_seq = seq_base[seq_base["LoanID"].isin(set(holdout_loans["LoanID"]))].copy()

    probs = predict_proba(model_run.stage_a, holdout_loans)
    probs.index = holdout_loans["LoanID"].values

    pre_recovery = build_loan_class_payin_matrix(holdout_loans, holdout_seq, model_run.stage_b)
    recovery_fraction = build_stage_c_recovery_fraction_matrix(
        model_run.stage_c,
        holdout_loans,
        pre_recovery,
        stage_c_features,
    )
    payin_matrix = apply_stage_c_recovery(pre_recovery, recovery_fraction)

    classes = list(PAYOFF_TYPE_COLLAPSED_ORDER)
    prob_cols = [f"P_{c}" for c in classes]
    p = probs[prob_cols].to_numpy(dtype=float)
    p = p / np.clip(p.sum(axis=1, keepdims=True), 1e-12, None)
    m = payin_matrix[classes].to_numpy(dtype=float)

    actual = holdout_loans.set_index("LoanID")["payin_ratio_realized"].reindex(payin_matrix.index)
    orig = holdout_loans.set_index("LoanID")["OriginatedAmount"].reindex(payin_matrix.index)
    pred_mean = (p * m).sum(axis=1)
    eval_df = pd.DataFrame({
        "LoanID": payin_matrix.index,
        "OriginatedAmount": orig.values,
        "OriginationDate": holdout_loans.set_index("LoanID")["OriginationDate"].reindex(payin_matrix.index).values,
        "actual_payin": actual.values,
        "predicted_payin": pred_mean,
    })
    eval_df["prediction_error"] = eval_df["predicted_payin"] - eval_df["actual_payin"]
    eval_df["abs_error"] = eval_df["prediction_error"].abs()
    eval_df["origination_month"] = pd.to_datetime(eval_df["OriginationDate"], errors="coerce").dt.to_period("M").astype(str)
    eval_df = eval_df.join(probs.reindex(eval_df["LoanID"]).reset_index(drop=True))

    portfolio_ci = simulate_portfolio_ci_stage_b(probs, orig, payin_matrix, n_sims=1000)
    metrics = summarize_evaluation_metrics(eval_df, portfolio_ci)
    monthly = summarize_evaluation_by_month(eval_df)
    return {
        "loan_base": loan_base,
        "seq_base": seq_base,
        "stage_c_features": stage_c_features,
        "holdout_loans": holdout_loans,
        "holdout_seq": holdout_seq,
        "probs": probs,
        "payin_matrix": payin_matrix,
        "eval_df": eval_df,
        "metrics": metrics,
        "monthly": monthly,
        "portfolio_ci": portfolio_ci,
    }


def summarize_evaluation_metrics(eval_df: pd.DataFrame, portfolio_ci: dict | None = None) -> dict:
    weights = pd.to_numeric(eval_df["OriginatedAmount"], errors="coerce").fillna(0.0)
    actual = eval_df["actual_payin"]
    pred = eval_df["predicted_payin"]
    w_sum = weights.sum()
    weighted_actual = float((actual * weights).sum() / w_sum) if w_sum else float("nan")
    weighted_pred = float((pred * weights).sum() / w_sum) if w_sum else float("nan")
    metrics = {
        "loans": int(len(eval_df)),
        "originated_amount": float(w_sum),
        "mae": float((pred - actual).abs().mean()),
        "rmse": float(np.sqrt(np.mean((pred - actual) ** 2))),
        "weighted_mae": float(((pred - actual).abs() * weights).sum() / w_sum) if w_sum else float("nan"),
        "portfolio_actual_payin": weighted_actual,
        "portfolio_predicted_payin": weighted_pred,
        "portfolio_error": weighted_pred - weighted_actual,
    }
    if portfolio_ci:
        metrics.update({
            "portfolio_p05": portfolio_ci.get("lo05"),
            "portfolio_p95": portfolio_ci.get("hi95"),
            "portfolio_ci_width": portfolio_ci.get("hi95") - portfolio_ci.get("lo05"),
        })
    return metrics


def summarize_evaluation_by_month(eval_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for month, df in eval_df.groupby("origination_month", dropna=False):
        weights = pd.to_numeric(df["OriginatedAmount"], errors="coerce").fillna(0.0)
        w_sum = weights.sum()
        actual = float((df["actual_payin"] * weights).sum() / w_sum) if w_sum else float("nan")
        pred = float((df["predicted_payin"] * weights).sum() / w_sum) if w_sum else float("nan")
        rows.append({
            "origination_month": month,
            "loans": int(len(df)),
            "originated_amount": float(w_sum),
            "actual_payin": actual,
            "predicted_payin": pred,
            "error": pred - actual,
            "mae": float(df["abs_error"].mean()),
            "weighted_mae": float((df["abs_error"] * weights).sum() / w_sum) if w_sum else float("nan"),
        })
    return pd.DataFrame(rows).sort_values("origination_month").reset_index(drop=True)


def plot_actual_vs_predicted(eval_df: pd.DataFrame, ax=None):
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(6, 6))[1]
    ax.scatter(eval_df["actual_payin"], eval_df["predicted_payin"], s=10, alpha=0.25, color=PRIMARY_COLOR)
    lo = min(eval_df["actual_payin"].min(), eval_df["predicted_payin"].min())
    hi = max(eval_df["actual_payin"].max(), eval_df["predicted_payin"].max())
    ax.plot([lo, hi], [lo, hi], color=ACCENT_COLOR, linewidth=2, label="Perfect prediction")
    ax.set_xlabel("Actual realized payin")
    ax.set_ylabel("Predicted final payin")
    ax.set_title("Holdout Actual vs Predicted Payin")
    ax.legend(loc="best")
    return ax


def plot_monthly_actual_vs_predicted(monthly: pd.DataFrame, ax=None):
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(10, 5))[1]
    x = np.arange(len(monthly))
    ax.plot(x, monthly["actual_payin"], marker="o", color=PRIMARY_COLOR, linewidth=2, label="Actual realized")
    ax.plot(x, monthly["predicted_payin"], marker="o", color=ACCENT_COLOR, linewidth=2, label="Predicted")
    ax.bar(x, monthly["error"], color=PLOT_PALETTE[3], alpha=0.25, label="Prediction error")
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(monthly["origination_month"].astype(str), rotation=45, ha="right")
    ax.set_ylabel("Payin ratio")
    ax.set_title("Holdout Payin Calibration by Origination Month")
    ax.legend(loc="best")
    return ax


def plot_error_distribution(eval_df: pd.DataFrame, ax=None):
    apply_plot_style()
    ax = ax or plt.subplots(figsize=(8, 4))[1]
    ax.hist(eval_df["prediction_error"], bins=60, color=PRIMARY_COLOR, alpha=0.75)
    ax.axvline(0, color="black", linewidth=1.2)
    ax.set_xlabel("Prediction error (predicted - actual)")
    ax.set_ylabel("Loan count")
    ax.set_title("Holdout Loan-Level Error Distribution")
    return ax
