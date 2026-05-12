"""Reusable feature builders for projection training and inference."""
from __future__ import annotations

from typing import Mapping

import numpy as np
import pandas as pd

from .projection_payment_features import append_payment_history_features
from .projection_risk_features import add_dm_risk_tier_features, normalize_dm_band
from .projection_stage_c import build_stage_c_recovery_model_base


CATEGORY_MAP_KEYS = [
    "Frequency_enc",
    "Frequency3_enc",
    "DM_Band_enc",
    "CM_Band_enc",
    "CustType_enc",
    "Frequency_norm_enc",
    "LoanStatus_enc",
    "payoff_type_collapsed_enc",
]

FREQ_NATIVE_MAP = {
    "W": "W",
    "WEEKLY": "W",
    "WEEK": "W",
    "1": "W",
    "4": "W",
    "B": "B",
    "BIWEEKLY": "B",
    "BI-WEEKLY": "B",
    "S": "S",
    "SEMI": "S",
    "SEMIMONTHLY": "S",
    "SEMI-MONTHLY": "S",
    "2": "S",
    "5": "S",
    "M": "M",
    "MONTHLY": "M",
    "3": "M",
    "6": "M",
}
FREQ_BIZ3_MAP = {"W": "W", "B": "B", "S": "B", "M": "M"}


def _as_timestamp(value) -> pd.Timestamp | None:
    if value is None:
        return None
    out = pd.to_datetime(value)
    if pd.isna(out):
        return None
    return pd.Timestamp(out).normalize()


def _string_values(series: pd.Series, normalize_dm: bool = False) -> pd.Series:
    if normalize_dm:
        return normalize_dm_band(series)
    return (
        series.where(series.notna(), "UNKNOWN")
        .astype(str)
        .str.upper()
        .str.strip()
        .replace({"": "UNKNOWN", "NAN": "UNKNOWN", "NONE": "UNKNOWN"})
    )


def build_category_map(series: pd.Series, normalize_dm: bool = False) -> dict[str, int]:
    """Build a deterministic string-to-code map compatible with pandas category order."""
    values = sorted(set(_string_values(series, normalize_dm=normalize_dm).dropna().tolist()) | {"UNKNOWN"})
    return {value: idx for idx, value in enumerate(values)}


def encode_category(
    series: pd.Series,
    mapping: Mapping[str, int] | None,
    normalize_dm: bool = False,
    unknown_value: int = -1,
) -> pd.Series:
    values = _string_values(series, normalize_dm=normalize_dm)
    if not mapping:
        mapping = build_category_map(values)
    return values.map(mapping).fillna(unknown_value).astype(float)


def build_category_maps(
    loan_base: pd.DataFrame,
    stage_c_model_base: pd.DataFrame | None = None,
) -> dict[str, dict[str, int]]:
    """Build the category maps that must be persisted with a trained run."""
    maps: dict[str, dict[str, int]] = {}
    if "Frequency_norm" in loan_base:
        maps["Frequency_enc"] = build_category_map(loan_base["Frequency_norm"])
        maps["Frequency_norm_enc"] = build_category_map(
            stage_c_model_base["Frequency_norm"] if stage_c_model_base is not None and "Frequency_norm" in stage_c_model_base else loan_base["Frequency_norm"]
        )
    if "Frequency_group3" in loan_base:
        maps["Frequency3_enc"] = build_category_map(loan_base["Frequency_group3"])
    if "DM_Band_Name" in loan_base:
        maps["DM_Band_enc"] = build_category_map(loan_base["DM_Band_Name"], normalize_dm=True)
    if "CM_Band_Name" in loan_base:
        maps["CM_Band_enc"] = build_category_map(loan_base["CM_Band_Name"])
    source = stage_c_model_base if stage_c_model_base is not None else loan_base
    for key, col in [
        ("CustType_enc", "CustType"),
        ("LoanStatus_enc", "LoanStatus"),
        ("payoff_type_collapsed_enc", "payoff_type_collapsed"),
    ]:
        if col in source:
            maps[key] = build_category_map(source[col])
    return maps


def _category_maps(feature_contract: dict | None) -> dict:
    return (feature_contract or {}).get("category_maps", {})


def _prepare_raw_as_of(raw_df: pd.DataFrame, as_of_date=None) -> pd.DataFrame:
    out = raw_df.copy()
    for c in ["OriginationDate", "InstallmentDueDate", "PaymentDate"]:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], errors="coerce")

    as_of = _as_timestamp(as_of_date)
    if as_of is None:
        return out

    if "OriginationDate" in out.columns:
        out = out[out["OriginationDate"].isna() | (out["OriginationDate"] <= as_of)].copy()

    payment_after_as_of = out.get("PaymentDate", pd.Series(pd.NaT, index=out.index)).gt(as_of)
    due_after_as_of = out.get("InstallmentDueDate", pd.Series(pd.NaT, index=out.index)).gt(as_of)

    for c in ["InstallRealizedPayment"]:
        if c in out.columns:
            out.loc[payment_after_as_of, c] = 0.0

    for c in ["InstallCollected", "EarlyCollected", "PartialCollected", "ThirdPartyCollected"]:
        if c in out.columns:
            out.loc[payment_after_as_of | due_after_as_of, c] = 0

    for c in ["isLoanDefault", "LoanPaidOffThisInstall", "isInstallDefault"]:
        if c in out.columns:
            out.loc[due_after_as_of, c] = 0

    return out


def _filter_payment_as_of(payment_df: pd.DataFrame, as_of_date=None) -> pd.DataFrame:
    out = payment_df.copy()
    if "PaymentDate" in out.columns:
        out["PaymentDate"] = pd.to_datetime(out["PaymentDate"], errors="coerce")
    as_of = _as_timestamp(as_of_date)
    if as_of is not None and "PaymentDate" in out.columns:
        out = out[out["PaymentDate"].isna() | (out["PaymentDate"] <= as_of)].copy()
    return out


def _ensure_columns(df: pd.DataFrame, columns: list[str], fill_value=0.0) -> pd.DataFrame:
    out = df.copy()
    for c in columns:
        if c not in out.columns:
            out[c] = fill_value
    return out


def _zero_series(df: pd.DataFrame) -> pd.Series:
    return pd.Series(0, index=df.index)


def build_loan_features(
    raw_df: pd.DataFrame,
    feature_contract: dict | None = None,
    as_of_date=None,
    mode: str = "inference",
) -> pd.DataFrame:
    """Build one row per loan with Stage A-compatible origination features."""
    raw = _prepare_raw_as_of(raw_df, as_of_date)
    maps = _category_maps(feature_contract)

    required_defaults = {
        "PortFolioID": 0,
        "Application_ID": "",
        "OriginatedAmount": 0.0,
        "OriginationDate": pd.NaT,
        "InstallRealizedPayment": 0.0,
        "AppYear": np.nan,
        "AppMonth": np.nan,
        "AppWeek": np.nan,
        "LoanStatus": "UNKNOWN",
        "CustType": "UNKNOWN",
        "Frequency": "UNKNOWN",
        "Frequency_norm": "UNKNOWN",
        "Frequency_group3": "UNKNOWN",
        "DM_Band_Name": "UNKNOWN",
        "CM_Band_Name": "UNKNOWN",
        "TotalInstallsNumber": 0,
    }
    for c, default in required_defaults.items():
        if c not in raw.columns:
            raw[c] = default
    raw["Frequency"] = _string_values(raw["Frequency"])
    if "Frequency_norm" not in raw.columns or raw["Frequency_norm"].eq("UNKNOWN").all():
        raw["Frequency_norm"] = raw["Frequency"].map(FREQ_NATIVE_MAP).fillna("UNKNOWN")
    if "Frequency_group3" not in raw.columns or raw["Frequency_group3"].eq("UNKNOWN").all():
        raw["Frequency_group3"] = raw["Frequency_norm"].map(FREQ_BIZ3_MAP).fillna("UNKNOWN")

    realized = (
        raw.groupby("LoanID", as_index=False)["InstallRealizedPayment"]
        .sum()
        .rename(columns={"InstallRealizedPayment": "TotalRealizedPayment"})
    )
    loan_attrs = (
        raw.groupby("LoanID", as_index=False)
        .agg(
            PortFolioID=("PortFolioID", "first"),
            Application_ID=("Application_ID", "first"),
            OriginatedAmount=("OriginatedAmount", "first"),
            OriginationDate=("OriginationDate", "min"),
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
            TotalInstallsNumber=("TotalInstallsNumber", "max"),
        )
        .merge(realized, on="LoanID", how="left")
    )

    if loan_attrs["AppYear"].isna().any() and loan_attrs["OriginationDate"].notna().any():
        iso = loan_attrs["OriginationDate"].dt.isocalendar()
        loan_attrs["AppYear"] = loan_attrs["AppYear"].fillna(loan_attrs["OriginationDate"].dt.year)
        loan_attrs["AppMonth"] = loan_attrs["AppMonth"].fillna(loan_attrs["OriginationDate"].dt.month)
        loan_attrs["AppWeek"] = loan_attrs["AppWeek"].fillna(iso.week.astype(float))

    out = loan_attrs.copy()
    out["payin_ratio_realized"] = (
        out["TotalRealizedPayment"] / out["OriginatedAmount"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    out["CustType_bin"] = _string_values(out["CustType"]).eq("RETURN").astype(int)
    out["Frequency_enc"] = encode_category(out["Frequency_norm"], maps.get("Frequency_enc"))
    out["Frequency3_enc"] = encode_category(out["Frequency_group3"], maps.get("Frequency3_enc"))
    out["log_orig_amt"] = np.log1p(pd.to_numeric(out["OriginatedAmount"], errors="coerce").fillna(0.0).clip(lower=0.0))
    out["month_sin"] = np.sin(2 * np.pi * pd.to_numeric(out["AppMonth"], errors="coerce").fillna(1) / 12.0)
    out["month_cos"] = np.cos(2 * np.pi * pd.to_numeric(out["AppMonth"], errors="coerce").fillna(1) / 12.0)
    out["week_sin"] = np.sin(2 * np.pi * pd.to_numeric(out["AppWeek"], errors="coerce").fillna(1) / 52.0)
    out["week_cos"] = np.cos(2 * np.pi * pd.to_numeric(out["AppWeek"], errors="coerce").fillna(1) / 52.0)

    out = add_dm_risk_tier_features(out, band_col="DM_Band_Name")
    out["CM_Band_Name"] = _string_values(out["CM_Band_Name"])
    out["DM_Band_enc"] = encode_category(out["DM_Band_Name"], maps.get("DM_Band_enc"), normalize_dm=True)
    out["CM_Band_enc"] = encode_category(out["CM_Band_Name"], maps.get("CM_Band_enc"))

    out["is_training_eligible"] = False if mode == "inference" else out.get("is_training_eligible", False)
    out["is_holdout"] = False
    out["payoff_type_collapsed"] = out.get("payoff_type_collapsed", "UNKNOWN")

    feature_cols = (feature_contract or {}).get("stage_a_features", [])
    out = _ensure_columns(out, feature_cols, fill_value=0.0)
    for c in feature_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return out


def build_seq_features(
    raw_df: pd.DataFrame,
    loan_features: pd.DataFrame,
    payment_normal_df: pd.DataFrame,
    feature_contract: dict | None = None,
    as_of_date=None,
) -> pd.DataFrame:
    """Build one row per normal installment with Stage B-compatible features."""
    raw = _prepare_raw_as_of(raw_df, as_of_date)
    as_of = _as_timestamp(as_of_date)

    aux = raw[raw.get("iPaymentMode", pd.Series(index=raw.index)).isin([679, 685])].copy()
    if as_of is not None and "InstallmentDueDate" in aux.columns:
        aux = aux[aux["InstallmentDueDate"].le(as_of)].copy()
    aux_events = aux[["LoanID", "iPaymentMode", "InstallmentDueDate"]].dropna(subset=["InstallmentDueDate"])
    arr_first = (
        aux_events.loc[aux_events["iPaymentMode"] == 679]
        .groupby("LoanID")["InstallmentDueDate"]
        .min()
        .rename("first_arrangement_date")
    )
    tp_first = (
        aux_events.loc[aux_events["iPaymentMode"] == 685]
        .groupby("LoanID")["InstallmentDueDate"]
        .min()
        .rename("first_3rdparty_date")
    )

    seq = raw[raw["iPaymentMode"] == 144].copy()
    seq = seq.sort_values(["LoanID", "InstallmentNumber"]).reset_index(drop=True)
    if seq.empty:
        return _ensure_columns(seq, (feature_contract or {}).get("stage_b_features", []))

    collected_cols = ["InstallCollected", "EarlyCollected", "PartialCollected", "ThirdPartyCollected"]
    seq = _ensure_columns(seq, collected_cols + ["InstallRealizedPayment", "isInstallDefault", "isLoanDefault", "LoanPaidOffThisInstall"])
    flag_cols = collected_cols + ["isInstallDefault", "isLoanDefault", "LoanPaidOffThisInstall"]
    for c in flag_cols:
        seq[c] = pd.to_numeric(seq[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0).astype(int)
    seq["collected_flag_k"] = seq[collected_cols].max(axis=1).fillna(0).astype(int)
    seq["collected_amount_k"] = pd.to_numeric(seq["InstallRealizedPayment"], errors="coerce").fillna(0.0)
    seq["step_payin_ratio_k"] = (
        seq["collected_amount_k"] / seq["OriginatedAmount"].replace(0, np.nan)
    ).fillna(0.0)
    seq["is_observed"] = True if as_of is None else pd.to_datetime(seq["InstallmentDueDate"], errors="coerce").le(as_of)

    seq = seq.merge(loan_features[["LoanID", "OriginationDate"]], on="LoanID", how="left", suffixes=("", "_loan"))
    seq["OriginationDate"] = seq["OriginationDate"].fillna(seq["OriginationDate_loan"])
    seq = seq.drop(columns=[c for c in ["OriginationDate_loan"] if c in seq.columns])
    seq = seq.merge(arr_first, on="LoanID", how="left").merge(tp_first, on="LoanID", how="left")
    seq["days_since_origination"] = (seq["InstallmentDueDate"] - seq["OriginationDate"]).dt.days.fillna(0)

    g = seq.groupby("LoanID", sort=False)
    seq["cum_collected_amt_lag"] = g["collected_amount_k"].cumsum() - seq["collected_amount_k"]
    seq["cum_payin_ratio_lag"] = (
        seq["cum_collected_amt_lag"] / seq["OriginatedAmount"].replace(0, np.nan)
    ).fillna(0.0)

    for src, out_col in [
        ("collected_flag_k", "cum_collected_rate_lag"),
        ("PartialCollected", "cum_partial_rate_lag"),
        ("ThirdPartyCollected", "cum_3rdparty_rate_lag"),
        ("EarlyCollected", "cum_early_rate_lag"),
        ("isInstallDefault", "prior_distress_lag"),
    ]:
        agg = "sum" if out_col == "prior_distress_lag" else "mean"
        seq[out_col] = g[src].transform(lambda s: s.shift(1).expanding().agg(agg)).fillna(0.0)

    seq["install_progress_lag"] = (
        (seq["InstallmentNumber"] - 1) / seq["TotalInstallsNumber"].replace(0, np.nan)
    ).fillna(0.0).clip(0, 1)
    seq["had_arrangement_by_lag"] = (
        seq["first_arrangement_date"].notna() & (seq["first_arrangement_date"] < seq["InstallmentDueDate"])
    ).astype(int)
    seq["had_3rdparty_by_lag"] = (
        seq["first_3rdparty_date"].notna() & (seq["first_3rdparty_date"] < seq["InstallmentDueDate"])
    ).astype(int)

    loan_cols = [
        "LoanID",
        "CustType_bin",
        "Frequency_enc",
        "Frequency3_enc",
        "log_orig_amt",
        "month_sin",
        "month_cos",
        "week_sin",
        "week_cos",
        "is_training_eligible",
        "is_holdout",
        "payoff_type_collapsed",
    ]
    seq = seq.merge(loan_features[[c for c in loan_cols if c in loan_features.columns]], on="LoanID", how="left")
    seq["payoff_type"] = seq.get("payoff_type_collapsed", "UNKNOWN")

    seq = append_payment_history_features(seq, _filter_payment_as_of(payment_normal_df, as_of_date))
    feature_cols = (feature_contract or {}).get("stage_b_features", [])
    seq = _ensure_columns(seq, feature_cols, fill_value=0.0)
    for c in feature_cols:
        seq[c] = pd.to_numeric(seq[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return seq


def build_observed_outcomes(
    seq_features: pd.DataFrame,
    loan_features: pd.DataFrame,
    as_of_date=None,
) -> pd.DataFrame:
    """Derive per-loan observed state as of the valuation date."""
    if seq_features.empty:
        out = loan_features[["LoanID", "TotalInstallsNumber"]].set_index("LoanID")
        out["last_observed_k"] = 0
        out["defaulted_at"] = np.nan
        out["cleanly_finished"] = False
        return out

    as_of = _as_timestamp(as_of_date)
    if "is_observed" in seq_features:
        seen = seq_features[seq_features["is_observed"].astype(bool)].copy()
    elif as_of is not None:
        seen = seq_features[pd.to_datetime(seq_features["InstallmentDueDate"], errors="coerce").le(as_of)].copy()
    else:
        seen = seq_features.copy()

    is_loan_default = seen["isLoanDefault"] if "isLoanDefault" in seen.columns else _zero_series(seen)
    paid_off_flag = seen["LoanPaidOffThisInstall"] if "LoanPaidOffThisInstall" in seen.columns else _zero_series(seen)

    defaulted = (
        seen.loc[is_loan_default == 1]
        .groupby("LoanID")["InstallmentNumber"]
        .min()
        .rename("defaulted_at")
    )
    last_k = seen.groupby("LoanID")["InstallmentNumber"].max().rename("last_observed_k")
    paid_off_seen = (
        seen.loc[paid_off_flag == 1]
        .groupby("LoanID")["InstallmentNumber"]
        .min()
        .notna()
    )
    out = loan_features[["LoanID", "TotalInstallsNumber"]].set_index("LoanID")
    out = out.join(last_k, how="left").join(defaulted, how="left")
    out["last_observed_k"] = out["last_observed_k"].fillna(0).astype(int)
    out["cleanly_finished"] = out.index.isin(paid_off_seen.index[paid_off_seen.values])
    out["cleanly_finished"] = out["cleanly_finished"] & (out["TotalInstallsNumber"] <= out["last_observed_k"])
    return out


def _default_class(default_inst) -> str:
    if pd.isna(default_inst):
        return "UNKNOWN"
    inst = int(default_inst)
    if inst == 1:
        return "FPD"
    if inst == 2:
        return "SPD"
    if inst == 3:
        return "TPD"
    return "LatePD"


def build_stage_c_base(
    raw_df: pd.DataFrame,
    loan_features: pd.DataFrame,
    seq_features: pd.DataFrame,
    as_of_date=None,
) -> pd.DataFrame:
    """Build the delinquent/recovery loan-grain base as of a valuation date."""
    raw = _prepare_raw_as_of(raw_df, as_of_date)
    as_of = _as_timestamp(as_of_date) or pd.Timestamp.today().normalize()

    is_loan_default = seq_features["isLoanDefault"] if "isLoanDefault" in seq_features.columns else _zero_series(seq_features)
    default_event = (
        seq_features.loc[is_loan_default == 1]
        .sort_values(["LoanID", "InstallmentNumber"])
        .groupby("LoanID", as_index=False)
        .agg(default_inst=("InstallmentNumber", "first"), default_due_date=("InstallmentDueDate", "first"))
    )
    paid_up_to_default = (
        seq_features.merge(default_event[["LoanID", "default_inst"]], on="LoanID", how="inner")
        .query("InstallmentNumber <= default_inst")
        .groupby("LoanID", as_index=False)["collected_amount_k"]
        .sum()
        .rename(columns={"collected_amount_k": "paid_by_default"})
    )

    aux = raw[raw.get("iPaymentMode", pd.Series(index=raw.index)).isin([679, 685])].copy()
    aux["InstallRealizedPayment"] = pd.to_numeric(aux.get("InstallRealizedPayment", 0.0), errors="coerce").fillna(0.0)
    aux = aux.merge(default_event[["LoanID", "default_due_date"]], on="LoanID", how="inner")
    aux["is_post_default"] = pd.to_datetime(aux["InstallmentDueDate"], errors="coerce") >= pd.to_datetime(aux["default_due_date"], errors="coerce")
    recovery_sums = (
        aux[aux["is_post_default"]]
        .groupby(["LoanID", "iPaymentMode"], as_index=False)["InstallRealizedPayment"]
        .sum()
        .pivot(index="LoanID", columns="iPaymentMode", values="InstallRealizedPayment")
        .rename(columns={679: "arrangement_realized", 685: "third_party_realized"})
        .reset_index()
        .fillna(0.0)
    )
    for c in ["arrangement_realized", "third_party_realized"]:
        if c not in recovery_sums.columns:
            recovery_sums[c] = 0.0

    delinquent = set(loan_features.loc[loan_features["LoanStatus"].isin({"R", "T", "L"}), "LoanID"])
    defaulted = set(default_event["LoanID"])
    pool = loan_features[loan_features["LoanID"].isin(delinquent | defaulted)].copy()
    cols = [
        "LoanID",
        "PortFolioID",
        "OriginatedAmount",
        "LoanStatus",
        "OriginationDate",
        "CustType",
        "Frequency_norm",
        "is_holdout",
        "is_training_eligible",
        "DM_Band_enc",
        "CM_Band_enc",
    ]
    out = (
        pool[[c for c in cols if c in pool.columns]]
        .merge(default_event, on="LoanID", how="left")
        .merge(paid_up_to_default, on="LoanID", how="left")
        .merge(recovery_sums[["LoanID", "arrangement_realized", "third_party_realized"]], on="LoanID", how="left")
    )
    for c in ["paid_by_default", "arrangement_realized", "third_party_realized"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0.0)
    out["recovery_realized"] = out["arrangement_realized"] + out["third_party_realized"]
    out["outstanding_at_default"] = (out["OriginatedAmount"] - out["paid_by_default"]).clip(lower=0.0)
    out["recovery_rate_realized"] = (
        out["recovery_realized"] / out["outstanding_at_default"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
    out["snapshot_date"] = as_of
    out["days_since_default"] = (as_of - pd.to_datetime(out["default_due_date"], errors="coerce")).dt.days.fillna(0)
    out["payoff_type_collapsed"] = out["default_inst"].map(_default_class)
    return out


def build_stage_c_features(
    raw_df: pd.DataFrame,
    loan_features: pd.DataFrame,
    seq_features: pd.DataFrame,
    payment_arr_df: pd.DataFrame,
    payment_3p_df: pd.DataFrame,
    feature_contract: dict | None = None,
    as_of_date=None,
) -> pd.DataFrame:
    """Build Stage C recovery-model features for already-defaulted loans."""
    stage_c_base = build_stage_c_base(raw_df, loan_features, seq_features, as_of_date=as_of_date)
    if stage_c_base.empty:
        return stage_c_base

    loan_for_stage_c = loan_features.copy()
    inferred_class = stage_c_base.set_index("LoanID")["payoff_type_collapsed"]
    loan_for_stage_c["payoff_type_collapsed"] = (
        loan_for_stage_c["LoanID"].map(inferred_class).fillna(loan_for_stage_c.get("payoff_type_collapsed", "UNKNOWN"))
    )

    model_base = build_stage_c_recovery_model_base(
        stage_c_base=stage_c_base,
        loan_base=loan_for_stage_c,
        payment_arr_df=_filter_payment_as_of(payment_arr_df, as_of_date),
        payment_3p_df=_filter_payment_as_of(payment_3p_df, as_of_date),
    )
    maps = _category_maps(feature_contract)
    for target, source in [
        ("CustType_enc", "CustType"),
        ("Frequency_norm_enc", "Frequency_norm"),
        ("LoanStatus_enc", "LoanStatus"),
        ("payoff_type_collapsed_enc", "payoff_type_collapsed"),
    ]:
        if source in model_base:
            model_base[target] = encode_category(model_base[source], maps.get(target))

    feature_cols = (feature_contract or {}).get("stage_c_recovery_features", [])
    model_base = _ensure_columns(model_base, feature_cols, fill_value=0.0)
    for c in feature_cols:
        model_base[c] = pd.to_numeric(model_base[c], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return model_base
