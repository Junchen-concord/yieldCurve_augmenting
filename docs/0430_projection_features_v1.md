Version: v1 | Date: 2026-04-30 | Repo: yieldCurve_augmenting

# Projection features (status)

Source of truth for **column names in persisted runs**: [`prediction_models/runs/20260429T212208Z/feature_contract.json`](../prediction_models/runs/20260429T212208Z/feature_contract.json) (representative tag; other runs follow the same schema). Code: [`util/projection_feature_builder.py`](../util/projection_feature_builder.py), [`util/projection_labels.py`](../util/projection_labels.py), [`util/projection_risk_features.py`](../util/projection_risk_features.py), [`util/projection_payment_features.py`](../util/projection_payment_features.py), [`util/projection_stage_c.py`](../util/projection_stage_c.py).

## Grain

| Grain | Use |
|-------|-----|
| Loan | Stage A, loan-level aggregates in SQL |
| Installment, normal stream | `iPaymentMode = 144` — Stage B rows |
| Payment attempts | Joined into Stage B lags; arrangement / 3p streams for Stage C aggregates |

## Stage A (origination)

**Target:** `payoff_type_collapsed` — 5 classes: `FPD`, `SPD`, `TPD`, `LatePD`, `Clean` ([`PAYOFF_TYPE_COLLAPSED_ORDER`](../util/projection_labels.py)). Raw `DENY_NEW` / `ALL_VOIDED` / `Immature` excluded from training target (see V5 notebook).

**Features** (`stage_a_features`):

- `CustType_bin`, `Frequency_enc`, `log_orig_amt`, `AppYear`, `month_sin`, `month_cos`, `week_sin`, `week_cos`, `PortFolioID`, `DM_Band_enc`, `DM_risk_high`, `DM_risk_med`, `DM_risk_low`, `DM_risk_unknown`

DM tier flags from business map (`dm_risk_tier_map` in contract; helper [`add_dm_risk_tier_features`](../util/projection_risk_features.py)).

## Stage B (installment)

**Targets:** `collected_flag_k` (clf), `collected_amount_k` (reg, conditional branch in training).

**Features** (`stage_b_features`): lag + loan block (see contract), then `payment_history_features` in the **same order** as below. Builder: [`build_seq_features`](../util/projection_feature_builder.py); payment lags: [`append_payment_history_features`](../util/projection_payment_features.py).

**`payment_history_features`** (verbatim from contract):

- `hist_payment_count_lag`, `hist_success_count_lag`, `hist_fail_count_lag`, `hist_success_rate_lag`, `hist_total_amount_lag`, `hist_avg_amount_lag`, `hist_last_payment_amount_lag`, `log_hist_last_payment_amount_lag`, `hist_last_attempt_was_success_lag`, `hist_last_attempt_was_fail_lag`, `hist_days_since_last_payment_lag`, `hist_days_since_last_success_lag`, `hist_fail_streak_lag`, `hist_recent_fail_count_last_3_lag`, `due_day_of_week`, `due_day_of_month`, `due_is_month_end`

## Stage C (recovery, Option 2)

**Targets:** `any_recovery`, `recovery_fraction` (`stage_c_target_cols` in contract).

**Features** (`stage_c_recovery_features`): identical to [`STAGE_C_RECOVERY_FEATURES`](../util/projection_stage_c.py) and to the contract list — `PortFolioID`, `OriginatedAmount`, `log_originated_amount`, `outstanding_at_default`, `log_outstanding_at_default`, `paid_by_default`, `paid_by_default_ratio`, `default_inst`, `days_since_default`, `CustType_enc`, `Frequency_norm_enc`, `LoanStatus_enc`, `payoff_type_collapsed_enc`, `DM_Band_enc`, `CM_Band_enc`, then `arr_*`, `tp_*`, `rec_*`, and share columns `arr_attempt_share`, `arr_success_amount_share`, `tp_attempt_share`, `tp_success_amount_share` (full ordering in JSON link above).

**Training filter:** `stage_c_min_days_since_default` (180 in sample run) in contract.

## Encodings

- **`category_maps`:** keys `Frequency_enc`, `Frequency3_enc`, `Frequency_norm_enc`, `DM_Band_enc`, `CM_Band_enc`, `CustType_enc`, `LoanStatus_enc`, `payoff_type_collapsed_enc` (levels per contract file; `payoff_type_collapsed_enc` uses uppercase keys `CLEAN`, `FPD`, … in JSON).
- **`dm_risk_tier_map`:** `DM01`–`DM08` + `UNKNOWN` → `high` / `med` / `low` / `unknown` (persisted in contract; applied via [`add_dm_risk_tier_features`](../util/projection_risk_features.py)).
- **Stage A multiclass order:** `class_order` in contract matches [`PAYOFF_TYPE_COLLAPSED_ORDER`](../util/projection_labels.py) (`FPD`, `SPD`, `TPD`, `LatePD`, `Clean`).

## Leakage / as-of

- [`_prepare_raw_as_of`](../util/projection_feature_builder.py): zero future `InstallRealizedPayment`; zero collected flags when payment/due after `as_of_date`; zero default/payoff flags on future due rows.
- Payment attempts: [`_filter_payment_as_of`](../util/projection_feature_builder.py) on `PaymentDate`.
- Stage B lags: prior installments only (built in `build_seq_features`).

## SQL

| Mode | Raw combined temp | Payment script |
|------|---------------------|----------------|
| Training | `jcx_raw_harvey_v14.sql` → e.g. `#t17_combined` | `SP_payment_data_v1.sql` (multi result sets) |
| Inference | `jcx_raw_inference_v1.sql` — recent application window + **full future schedule** for non-terminal loans | `SP_payment_data_inference_v1.sql` — same window |

Inference scripts align with notebook **lookback** (e.g. 120 days) so extracts stay bounded.

## See also

- [0430_projection_design_v1.md](0430_projection_design_v1.md) — pipeline and persistence.
- [`.cursor/plans/inference_pipeline_7f27c6d0.plan.md`](../.cursor/plans/inference_pipeline_7f27c6d0.plan.md) — inference, `as_of_date`, category maps.
- [`.cursor/plans/stage_c_recovery_9a3db458.plan.md`](../.cursor/plans/stage_c_recovery_9a3db458.plan.md) — Stage C Option 2, recovery matrix.
- [`.cursor/plans/payin_projection_v1_plan_bf451ad6.plan.md`](../.cursor/plans/payin_projection_v1_plan_bf451ad6.plan.md) — early V1 arc (superseded; lineage only).
