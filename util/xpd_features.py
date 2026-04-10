import pandas as pd


REQUIRED_V6_XPD_COLUMNS = {
    "LoanID",
    "InstallmentNumber",
    "isInstallDefault",
    "LoanPaidOff",
}

REQUIRED_V6_LOAN_COLUMNS = {
    "LoanID",
    "OriginatedAmount",
    "PaidOffThisInstall",
    "TotalRealizedPayin",
}


def _validate_required_columns(df: pd.DataFrame, required: set[str], context: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns for {context}: {missing}")


def build_xpd_features_v6(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build loan-level XPD feature columns from v6 installment-level output.

    Expected input columns:
    - LoanID
    - InstallmentNumber
    - isInstallDefault
    - LoanPaidOff

    Returned columns:
    - LoanID
    - fpd_flag
    - spd_flag
    - tpd_flag
    - xpd_count            (sum of defaults in first 5 installments)
    - earliest_default_inst (0 if no default)
    - payoff_by_inst_5
    """
    _validate_required_columns(df, REQUIRED_V6_XPD_COLUMNS, "v6 XPD build")

    work = df[list(REQUIRED_V6_XPD_COLUMNS)].copy()
    work["InstallmentNumber"] = pd.to_numeric(work["InstallmentNumber"], errors="coerce")
    work["isInstallDefault"] = pd.to_numeric(work["isInstallDefault"], errors="coerce").fillna(0).astype(int)
    work["LoanPaidOff"] = pd.to_numeric(work["LoanPaidOff"], errors="coerce").fillna(0).astype(int)

    work = work.dropna(subset=["LoanID", "InstallmentNumber"]).copy()
    work["InstallmentNumber"] = work["InstallmentNumber"].astype(int)
    work = work.sort_values(["LoanID", "InstallmentNumber"]).drop_duplicates(
        subset=["LoanID", "InstallmentNumber"], keep="last"
    )

    base = pd.DataFrame({"LoanID": work["LoanID"].drop_duplicates().values})

    def _flag_at_install(k: int, out_col: str) -> pd.DataFrame:
        tmp = work.loc[work["InstallmentNumber"] == k, ["LoanID", "isInstallDefault"]].copy()
        tmp = tmp.rename(columns={"isInstallDefault": out_col})
        tmp[out_col] = (tmp[out_col] == 1).astype(int)
        return tmp.groupby("LoanID", as_index=False)[out_col].max()

    fpd = _flag_at_install(1, "fpd_flag")
    spd = _flag_at_install(2, "spd_flag")
    tpd = _flag_at_install(3, "tpd_flag")

    xpd_first5 = (
        work.loc[work["InstallmentNumber"] <= 5]
        .groupby("LoanID", as_index=False)["isInstallDefault"]
        .sum()
        .rename(columns={"isInstallDefault": "xpd_count"})
    )
    xpd_first5["xpd_count"] = xpd_first5["xpd_count"].astype(int)

    earliest_default = (
        work.loc[work["isInstallDefault"] == 1]
        .groupby("LoanID", as_index=False)["InstallmentNumber"]
        .min()
        .rename(columns={"InstallmentNumber": "earliest_default_inst"})
    )

    payoff_by_inst_5 = (
        work.assign(
            payoff_by_inst_5=((work["LoanPaidOff"] == 1) & (work["InstallmentNumber"] <= 5)).astype(int)
        )
        .groupby("LoanID", as_index=False)["payoff_by_inst_5"]
        .max()
    )

    out = (
        base.merge(fpd, on="LoanID", how="left")
        .merge(spd, on="LoanID", how="left")
        .merge(tpd, on="LoanID", how="left")
        .merge(xpd_first5, on="LoanID", how="left")
        .merge(earliest_default, on="LoanID", how="left")
        .merge(payoff_by_inst_5, on="LoanID", how="left")
    )

    for col in ["fpd_flag", "spd_flag", "tpd_flag", "xpd_count", "payoff_by_inst_5"]:
        out[col] = out[col].fillna(0).astype(int)
    out["earliest_default_inst"] = out["earliest_default_inst"].fillna(0).astype(int)

    return out.sort_values("LoanID").reset_index(drop=True)


def build_loan_level_payin_v6(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build loan-level payin metrics from v6 installment-level output.

    Returned columns:
    - LoanID
    - OriginatedAmount
    - TotalRealizedPayin_sql           (first value from SQL output)
    - TotalRealizedPayin_canonical     (alias of SQL total; use for modeling)
    - TotalRealizedPayin_from_install  (sum of PaidOffThisInstall)
    - payin_ratio_canonical            (canonical_total / originated)
    - payin_ratio_sql                  (sql_total / originated)
    - payin_ratio_from_install         (install_sum / originated)
    - payin_ratio_gap                  (from_install - sql)
    """
    _validate_required_columns(df, REQUIRED_V6_LOAN_COLUMNS, "v6 loan-level payin build")

    work = df[list(REQUIRED_V6_LOAN_COLUMNS)].copy()
    work["OriginatedAmount"] = pd.to_numeric(work["OriginatedAmount"], errors="coerce")
    work["PaidOffThisInstall"] = pd.to_numeric(work["PaidOffThisInstall"], errors="coerce").fillna(0.0)
    work["TotalRealizedPayin"] = pd.to_numeric(work["TotalRealizedPayin"], errors="coerce")
    work = work.dropna(subset=["LoanID"]).copy()

    out = (
        work.groupby("LoanID", as_index=False)
        .agg(
            OriginatedAmount=("OriginatedAmount", "first"),
            TotalRealizedPayin_sql=("TotalRealizedPayin", "first"),
            TotalRealizedPayin_from_install=("PaidOffThisInstall", "sum"),
        )
    )
    out["TotalRealizedPayin_canonical"] = out["TotalRealizedPayin_sql"]

    denom = out["OriginatedAmount"].replace(0, pd.NA)
    out["payin_ratio_canonical"] = out["TotalRealizedPayin_canonical"] / denom
    out["payin_ratio_sql"] = out["TotalRealizedPayin_sql"] / denom
    out["payin_ratio_from_install"] = out["TotalRealizedPayin_from_install"] / denom
    out["payin_ratio_gap"] = out["payin_ratio_from_install"] - out["payin_ratio_sql"]

    num_cols = [
        "TotalRealizedPayin_sql",
        "TotalRealizedPayin_from_install",
        "payin_ratio_sql",
        "payin_ratio_from_install",
        "payin_ratio_gap",
    ]
    out[num_cols] = out[num_cols].fillna(0.0)
    return out.sort_values("LoanID").reset_index(drop=True)
