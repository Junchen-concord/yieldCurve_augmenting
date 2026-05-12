"""Plotting helpers for the payin projection MVP."""
from __future__ import annotations

from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_stage_a_calibration(calib: pd.DataFrame, ax=None):
    """Bar chart of predicted mean vs observed rate per class."""
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    x = np.arange(len(calib))
    width = 0.4
    ax.bar(x - width / 2, calib["pred_mean"], width, label="Predicted mean")
    ax.bar(x + width / 2, calib["obs_rate"], width, label="Observed rate")
    ax.set_xticks(x)
    ax.set_xticklabels(calib["class"], rotation=30, ha="right")
    ax.set_ylabel("Rate")
    ax.set_title("Stage A calibration (holdout)")
    ax.legend()
    return ax


def plot_stage_b_calibration(calib: pd.DataFrame, ax=None):
    """Per-installment predicted vs observed collect rate + amount."""
    if ax is None:
        fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    else:
        axes = ax  # assume caller passed a 2-ax array

    ax1, ax2 = axes
    ax1.plot(calib["InstallmentNumber"], calib["pred_collect_rate"], marker="o", label="Predicted")
    ax1.plot(calib["InstallmentNumber"], calib["obs_collect_rate"], marker="s", label="Observed")
    ax1.set_xlabel("InstallmentNumber")
    ax1.set_ylabel("Collect rate")
    ax1.set_title("Stage B: collect rate by installment")
    ax1.legend()

    ax2.plot(calib["InstallmentNumber"], calib["pred_amount_mean"], marker="o", label="Predicted $")
    ax2.plot(calib["InstallmentNumber"], calib["obs_amount_mean"], marker="s", label="Observed $")
    ax2.set_xlabel("InstallmentNumber")
    ax2.set_ylabel("Mean collected $")
    ax2.set_title("Stage B: $ by installment")
    ax2.legend()
    return axes


def plot_recovery_curve(curve_df: pd.DataFrame, ax=None):
    """Plot Stage C empirical recovery fraction curve."""
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(curve_df["days_bucket"], curve_df["mean_recovery_fraction"], marker="o", label="Mean")
    ax.plot(curve_df["days_bucket"], curve_df["median_recovery_fraction"], marker="s", linestyle="--", label="Median")
    ax.set_xlabel("Days since default (bucket start)")
    ax.set_ylabel("Recovery fraction of outstanding")
    ax.set_title("Stage C: empirical recovery curve")
    ax.legend()
    return ax


def plot_portfolio_ci_narrowing(
    ci_by_k: pd.DataFrame,
    ax=None,
):
    """Show portfolio-level CI width + actual payin as k grows.

    ci_by_k columns expected: k, pred_mean, pred_lo05, pred_hi95, realized_so_far (optional).
    realized_so_far is the cumulative realized payin ratio through installment k.
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 5))

    ax.fill_between(ci_by_k["k"], ci_by_k["pred_lo05"], ci_by_k["pred_hi95"], alpha=0.3, label="90% CI")
    ax.plot(ci_by_k["k"], ci_by_k["pred_mean"], marker="o", label="Predicted mean")
    if "realized_so_far" in ci_by_k.columns:
        ax.plot(ci_by_k["k"], ci_by_k["realized_so_far"], marker="s", color="red", label="Realized so far")

    ax.set_xlabel("Observed installments (k)")
    ax.set_ylabel("Portfolio payin ratio")
    ax.set_title("Portfolio payin: CI tightens as installments mature")
    ax.legend()
    return ax


def plot_portfolio_ci_width_vs_k(ci_by_k: pd.DataFrame, ax=None):
    """Companion plot: show the CI *width* shrinking as k grows (the money shot)."""
    if ax is None:
        fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(ci_by_k["k"], ci_by_k["pred_hi95"] - ci_by_k["pred_lo05"], marker="o")
    ax.set_xlabel("Observed installments (k)")
    ax.set_ylabel("CI width (hi95 - lo05)")
    ax.set_title("Confidence tightens: CI width vs k")
    return ax
