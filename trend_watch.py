"""Emerging, confirming, and strong trend-watch state logic."""

from dashboard_core import (
    _trend_sep_ratio,
    compute_trend_watch_state,
)

__all__ = [name for name in globals() if not name.startswith("__")]
