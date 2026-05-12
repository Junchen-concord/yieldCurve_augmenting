#!/usr/bin/env python3
"""Discoverable entry for custtype-faceted inference plots (import-only).

After ``results = score_live_projection(...)`` in a notebook or script::

    from util.projection_visuals_by_custtype import plot_inference_faceted_by_custtype

    fig = plot_inference_faceted_by_custtype(results)
    fig.show()

This file intentionally does not pull SQL or load models; keep scoring in one place.
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from util.projection_visuals_by_custtype import (  # noqa: E402
    default_custtypes_for_plot,
    plot_inference_faceted_by_custtype,
    subset_inference_results,
    subset_inference_results_by_custtype,
)

__all__ = [
    "default_custtypes_for_plot",
    "plot_inference_faceted_by_custtype",
    "subset_inference_results",
    "subset_inference_results_by_custtype",
]
