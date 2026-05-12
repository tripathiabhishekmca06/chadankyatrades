from __future__ import annotations

import http.cookiejar
import html
import hashlib
import json
import logging
import os
import re
import socket
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components
import yfinance as yf

from settings import get_alpha_vantage_api_key, get_eodhd_api_key

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
FNO_LIST_PATH = DATA_DIR / "fno_list.csv"
OPTIONS_LIST_PATH = DATA_DIR / "options_list.csv"
def _pick_runtime_dir() -> Path:
    preferred = APP_DIR
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        probe = preferred / ".write_test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return preferred
    except Exception:
        fallback = Path("/tmp/chadankyatrades")
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback


RUNTIME_DIR = _pick_runtime_dir()
SIGNALS_DB_PATH = RUNTIME_DIR / "signals.db"
LOG_DIR = RUNTIME_DIR / "logs"
LOG_PATH = LOG_DIR / "trading_dashboard.log"
_TRADING_LOG_BOOTSTRAPPED = False

OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

NSE_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

FUTURES_COLUMNS = [
    "Stock",
    "Signal",
    "Signal Type",
    "Structure Signal",
    "Entry",
    "Current Price",
    "EMA20",
    "EMA50",
    "Stop Loss",
    "Target",
    "RSI",
    "ATR",
    "Volume",
    "Avg Volume",
    "Volume Strength",
    "Market Structure",
    "Structure Break",
    "Distance from EMA20 %",
    "Confidence Score",
    "Trade Quality",
    "trend_direction",
    "ema_condition",
    "rsi_value",
    "rsi_condition",
    "volume_value",
    "volume_condition",
    "distance_from_ema",
    "distance_condition",
    "trend_strength",
    "strategy_type",
    "failed_conditions",
    "Exit Signal",
    "Reason",
    "trend_watch_direction",
    "trend_stage",
    "trend_age",
    "trend_watch_note",
    "Early RS Label",
    "Relative Strength Score",
    "Pre Event Label",
    "Post Event Label",
    "Event Strength Score",
    "Event Warning",
    "Risk ₹",
    "Suggested Qty",
    "Suggested Lots",
    "Reward ₹",
    "Position Risk %",
]

OPTIONS_COLUMNS = [
    "Stock",
    "Option Symbol",
    "Strike",
    "Option Type",
    "Entry Premium (approx)",
    "SL Premium",
    "Target Premium",
]

HISTORY_COLUMNS = [
    "id",
    "timestamp",
    "stock",
    "signal_type",
    "entry_price",
    "stop_loss",
    "target",
    "confidence_score",
    "status",
    "exit_reason",
    "exit_price",
    "pnl_percent",
    "original_entry_price",
    "original_stop_loss",
    "original_target",
    "original_signal_type",
    "original_selected_timestamp",
    "final_exit_price",
    "final_exit_reason",
    "final_pnl_percent",
]

ACTIVE_COLUMNS = [
    "Stock",
    "Direction",
    "Entry",
    "Current Price",
    "P&L %",
    "SL",
    "Target",
    "Confidence",
]

CLOSED_COLUMNS = [
    "Stock",
    "Entry",
    "Exit",
    "P&L %",
    "Exit Reason",
]

SIGNAL_TYPE_PRIORITY = {
    "PULLBACK_LONG": 1,
    "PULLBACK_SHORT": 1,
    "BREAKOUT_LONG": 2,
    "BREAKOUT_SHORT": 2,
    "STRUCTURE_BREAK": 2,
    "TREND_LONG": 3,
    "TREND_SHORT": 3,
    "": 9,
}

SIGNAL_PRIORITY = {
    "STRONG_LONG": 1,
    "STRONG_SHORT": 1,
    "EARLY_LONG": 2,
    "EARLY_SHORT": 2,
    "WEAK_LONG": 3,
    "WEAK_SHORT": 3,
    "EARLY_RS_LONG": 3,
    "WAIT": 4,
}

ALPHA_API_KEY = get_alpha_vantage_api_key()
EODHD_API_KEY = get_eodhd_api_key()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5-mini").strip()

ALPHA_API_MINUTE_LIMIT = 5
ALPHA_API_DAY_LIMIT = 500
EODHD_API_MINUTE_LIMIT = 20
CACHE_TTL = 60.0
YAHOO_FETCH_CACHE_TTL_SECONDS = CACHE_TTL
YAHOO_MIN_CALL_GAP_SECONDS = 2.0
NETWORK_REQUEST_TIMEOUT_SECONDS = 8
API_USAGE_STATE_KEY = "_api_usage_tracker_v1"
API_USAGE_PERSIST_PATH = DATA_DIR / "api_usage_state.json"
MARKET_SESSION_CACHE_TTL = 90.0
# Frontend bar/button must match get_market_data_with_cache TTL and force window (seconds of TTL left).
MARKET_FORCE_REFRESH_ENABLE_REMAINING_SEC = 30
MARKET_UI_WARNING_REMAINING_SEC = 15
MARKET_FORCE_NEXT_KEY = "_market_force_refresh_next"
MARKET_FETCH_META_KEY = "_market_fetch_meta"
MARKET_QUOTA_FLAG_KEY = "_market_quota_low_flag"
MARKET_SCAN_NONCE_KEY = "_market_scan_refresh_nonce"
FULL_SCAN_CACHE_KEY = "full_scan_cache"
FNO_SELECTED_CACHE_KEY = "fno_selected_cache"
FNO_FORCE_REFRESH_KEY = "_fno_force_refresh_next"
FNO_SELECTED_CACHE_TTL_SECONDS = 60
# Persists last successful LIVE market fetch time across browser sessions (for timer + skipping redundant API).
MARKET_API_META_PATH = DATA_DIR / "market_api_meta.json"
# Last merged OHLCV snapshot from a LIVE get_market_data run (same TTL as MARKET_SESSION_CACHE_TTL).
MARKET_CACHE_DF_PATH = DATA_DIR / "market_cache_latest.pkl"
# EODHD intraday is one symbol per request; use parallel workers + Session to cut wall time.
EODHD_FALLBACK_MAX_WORKERS = 4
_ALPHA_LAST_CALL_TS = 0.0
_EODHD_LAST_CALL_TS = 0.0
_YAHOO_LAST_CALL_TS = 0.0
_YAHOO_FETCH_CACHE: dict[tuple[Any, ...], tuple[float, pd.DataFrame]] = {}
_YAHOO_CACHE_LOCK = threading.Lock()
# Set when Alpha returns a premium / unavailable intraday message so we skip further intraday calls.
_ALPHA_INTRADAY_BLOCKED = False
# Set when network/proxy blocks Alpha intraday endpoint in current process.
_ALPHA_NETWORK_BLOCKED = False
_EODHD_NETWORK_BLOCKED = False
_PROVIDER_STATUS: dict[str, str] = {}
# EODHD ``BASE.suffix`` for India (resolved once; override with EODHD_NSE_EXCHANGE_CODE).
_EODHD_NSE_EXCHANGE_SUFFIX: str | None = None
_EODHD_SUFFIX_LOCK = threading.Lock()

SYMBOL_COLUMN_CANDIDATES = (
    "symbol",
    "symbols",
    "stock",
    "stocks",
    "ticker",
    "tickers",
    "name",
    "underlying",
)

MAX_STOCKS = 50
DEFAULT_STREAMLIT_HOST = os.getenv("HOST", "0.0.0.0")
DEFAULT_STREAMLIT_PORT = int(os.getenv("PORT", os.getenv("STREAMLIT_SERVER_PORT", "8501")))

logger = logging.getLogger("trading_dashboard")


def configure_trading_dashboard_logging() -> None:
    """
    File logging under logs/trading_dashboard.log (rotated). Idempotent across Streamlit reruns.

    Env:
      TRADING_DASHBOARD_CONSOLE_LOG=1 — mirror INFO+ to stderr (terminal).
      TRADING_DASHBOARD_VERBOSE_API=1 — DEBUG for this logger (per-request API detail).
    """
    global _TRADING_LOG_BOOTSTRAPPED
    if _TRADING_LOG_BOOTSTRAPPED:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    verbose = os.getenv("TRADING_DASHBOARD_VERBOSE_API", "").strip().lower() in {"1", "true", "yes", "on"}
    level = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(level)
    logger.propagate = False
    if logger.handlers:
        logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    file_handler = RotatingFileHandler(
        LOG_PATH,
        maxBytes=5_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    if os.getenv("TRADING_DASHBOARD_CONSOLE_LOG", "").strip().lower() in {"1", "true", "yes", "on"}:
        console = logging.StreamHandler(sys.stderr)
        console.setLevel(level)
        console.setFormatter(fmt)
        logger.addHandler(console)
    _TRADING_LOG_BOOTSTRAPPED = True
    logger.info(
        "Logging initialized path=%s verbose_api=%s console=%s",
        LOG_PATH,
        verbose,
        os.getenv("TRADING_DASHBOARD_CONSOLE_LOG", ""),
    )


def configure_runtime_environment() -> None:
    configure_trading_dashboard_logging()
    # Ensure local writable paths exist for cloud/container environments.
    SIGNALS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

@dataclass(frozen=True)
class StrategyConfig:
    ema_fast: int = 20
    ema_slow: int = 50
    rsi_period: int = 14
    atr_period: int = 14
    adx_period: int = 14
    volume_window: int = 20
    breakout_window: int = 20
    trend_strength_min: float = 0.005
    overextended_max: float = 0.03
    pullback_distance_max: float = 0.02
    volume_confirmation_multiplier: float = 1.20
    volume_spike_multiplier: float = 1.50
    long_rsi: float = 55.0
    short_rsi: float = 45.0
    atr_multiplier: float = 1.5
    target_rr: float = 2.0
    option_sl_pct: float = 0.30
    option_target_pct: float = 0.60
    swing_lookback: int = 2

    @property
    def minimum_rows(self) -> int:
        return max(
            self.ema_slow,
            self.volume_window,
            self.breakout_window + 1,
            self.atr_period,
        ) + 10


@dataclass(frozen=True)
class RiskSettings:
    trading_capital: float = 100_000.0
    max_risk_per_trade_pct: float = 1.0
    max_active_trades: int = 5
    max_total_portfolio_risk_pct: float = 5.0


def empty_futures_table() -> pd.DataFrame:
    return pd.DataFrame(columns=FUTURES_COLUMNS)


def empty_options_table() -> pd.DataFrame:
    return pd.DataFrame(columns=OPTIONS_COLUMNS)


def empty_history_table() -> pd.DataFrame:
    return pd.DataFrame(columns=HISTORY_COLUMNS)


def empty_active_table() -> pd.DataFrame:
    return pd.DataFrame(columns=ACTIVE_COLUMNS)


def empty_closed_table() -> pd.DataFrame:
    return pd.DataFrame(columns=CLOSED_COLUMNS)


def clean_stock_symbol(value: object) -> str | None:
    if pd.isna(value):
        return None

    symbol = str(value).strip().upper()
    symbol = re.sub(r"\s+", "", symbol)
    if not symbol or symbol in {"NAN", "NONE", "NULL"}:
        return None
    if not re.fullmatch(r"[A-Z0-9&.-]+", symbol):
        print(f"Skipped symbol {symbol}: invalid characters")
        logger.info("Skipped symbol %s: invalid characters", symbol)
        return None
    return symbol or None


def clean_underlying_symbol(value: object) -> str | None:
    symbol = clean_stock_symbol(value)
    if symbol and symbol.endswith(".NS"):
        return symbol[:-3]
    return symbol


def pick_symbol_column(data: pd.DataFrame) -> str:
    normalized_columns = {str(column).strip().lower(): column for column in data.columns}

    for candidate in SYMBOL_COLUMN_CANDIDATES:
        if candidate in normalized_columns:
            return normalized_columns[candidate]

    text_columns = list(data.select_dtypes(include=["object", "string"]).columns)
    return text_columns[0] if text_columns else data.columns[0]


@st.cache_data(ttl=900, show_spinner=False)
def load_stock_list(path: str = str(FNO_LIST_PATH), limit: int | None = MAX_STOCKS) -> tuple[list[str], str | None]:
    csv_path = Path(path)
    if not csv_path.exists():
        print(f"Skipped loading stocks: {csv_path.name} not found")
        logger.warning("Skipped loading stocks: %s not found", csv_path)
        return [], f"{csv_path.name} not found in {csv_path.parent}"

    try:
        data = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("Failed to read %s", csv_path)
        print(f"Skipped loading stocks: unable to read {csv_path.name}: {exc}")
        return [], f"Unable to read {csv_path.name}: {exc}"

    if data.empty:
        print(f"Skipped loading stocks: {csv_path.name} is empty")
        logger.warning("Skipped loading stocks: %s is empty", csv_path)
        return [], f"{csv_path.name} is empty"

    normalized_columns = {str(column).strip().lower(): column for column in data.columns}
    if "symbol" not in normalized_columns:
        print(f"Skipped loading stocks: {csv_path.name} missing required symbol column")
        logger.warning("Skipped loading stocks: %s missing required symbol column", csv_path)
        return [], f"{csv_path.name} must contain a 'symbol' column"

    raw_symbols = data[normalized_columns["symbol"]].tolist()
    symbols = []
    seen = set()

    for raw_symbol in raw_symbols:
        symbol = clean_stock_symbol(raw_symbol)
        if not symbol:
            print(f"Skipped symbol {raw_symbol}: empty or invalid")
            logger.info("Skipped symbol %s: empty or invalid", raw_symbol)
            continue
        if symbol in seen:
            print(f"Skipped symbol {symbol}: duplicate")
            logger.info("Skipped symbol %s: duplicate", symbol)
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        print(f"Skipped loading stocks: no valid symbols found in {csv_path.name}")
        logger.warning("Skipped loading stocks: no valid symbols found in %s", csv_path)
        return [], f"No valid symbols found in {csv_path.name}"

    if limit is not None and len(symbols) > limit:
        print(f"Stock list limited to first {limit} symbols for performance")
        logger.info("Stock list limited to first %s symbols for performance", limit)
        symbols = symbols[:limit]

    return symbols, None


@st.cache_data(ttl=900, show_spinner=False)
def load_lot_size_map(path: str = str(FNO_LIST_PATH)) -> dict[str, int]:
    csv_path = Path(path)
    if not csv_path.exists():
        return {}
    try:
        data = pd.read_csv(csv_path)
    except Exception:
        logger.info("Unable to read lot sizes from %s", csv_path, exc_info=True)
        return {}
    if data.empty:
        return {}

    normalized_columns = {str(column).strip().lower(): column for column in data.columns}
    symbol_column = normalized_columns.get("symbol") or pick_symbol_column(data)
    lot_column = None
    for candidate in ("lot_size", "lotsize", "lot size", "lot", "market_lot"):
        if candidate in normalized_columns:
            lot_column = normalized_columns[candidate]
            break
    if lot_column is None:
        return {}

    lot_sizes: dict[str, int] = {}
    for _, row in data.iterrows():
        symbol = clean_stock_symbol(row.get(symbol_column))
        lot_size = pd.to_numeric(row.get(lot_column), errors="coerce")
        if symbol and pd.notna(lot_size) and int(lot_size) > 0:
            lot_sizes[symbol.upper()] = int(lot_size)
            if not symbol.upper().endswith(".NS"):
                lot_sizes[f"{symbol.upper()}.NS"] = int(lot_size)
    return lot_sizes


@st.cache_data(ttl=900, show_spinner=False)
def load_option_list(path: str = str(OPTIONS_LIST_PATH)) -> tuple[list[str], str | None]:
    csv_path = Path(path)
    if not csv_path.exists():
        return [], None

    try:
        data = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("Failed to read %s", csv_path)
        print(f"Skipped loading options: unable to read {csv_path.name}: {exc}")
        return [], f"Unable to read {csv_path.name}: {exc}"

    if data.empty:
        print(f"Skipped loading options: {csv_path.name} is empty")
        logger.warning("Skipped loading options: %s is empty", csv_path)
        return [], f"{csv_path.name} is empty"

    symbol_column = pick_symbol_column(data)
    symbols = []
    seen = set()
    for raw_symbol in data[symbol_column].tolist():
        symbol = clean_underlying_symbol(raw_symbol)
        if not symbol:
            print(f"Skipped option symbol {raw_symbol}: empty or invalid")
            logger.info("Skipped option symbol %s: empty or invalid", raw_symbol)
            continue
        if symbol in seen:
            print(f"Skipped option symbol {symbol}: duplicate")
            logger.info("Skipped option symbol %s: duplicate", symbol)
            continue
        seen.add(symbol)
        symbols.append(symbol)

    if not symbols:
        print(f"Skipped loading options: no valid symbols found in {csv_path.name}")
        logger.warning("Skipped loading options: no valid symbols found in %s", csv_path)
        return [], f"No valid symbols found in {csv_path.name}"

    return symbols, None


def parse_manual_symbols(raw_symbols: str) -> list[str]:
    symbols = []
    for item in raw_symbols.replace("\n", ",").split(","):
        symbol = clean_stock_symbol(item)
        if symbol:
            symbols.append(symbol)
    return list(dict.fromkeys(symbols))


def to_market_ticker_key(symbol: str) -> str:
    """Column key for merged OHLCV (NSE-style *.NS suffix for MultiIndex columns)."""
    return symbol.upper() if symbol.upper().endswith(".NS") else f"{symbol.upper()}.NS"


def _normalize_ohlcv_frame(data: pd.DataFrame) -> pd.DataFrame:
    if data.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    frame = data.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(-1)
    frame = frame.rename(columns=str.title)
    if "Adj Close" in frame.columns and "Close" not in frame.columns:
        frame["Close"] = frame["Adj Close"]
    for col in OHLCV_COLUMNS:
        if col not in frame.columns:
            frame[col] = np.nan
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    frame = frame[OHLCV_COLUMNS].dropna(subset=["Open", "High", "Low", "Close"])
    return frame.sort_index()


def _extract_merged_ticker_frame(payload: pd.DataFrame, ticker: str, total: int) -> pd.DataFrame:
    if payload.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    if total == 1 and not isinstance(payload.columns, pd.MultiIndex):
        return _normalize_ohlcv_frame(payload)
    if isinstance(payload.columns, pd.MultiIndex) and ticker in payload.columns.get_level_values(0):
        return _normalize_ohlcv_frame(payload[ticker])
    return pd.DataFrame(columns=OHLCV_COLUMNS)


def _to_alpha_symbol(market_ticker: str) -> str:
    """
    Alpha Vantage expects NSE listings as ``NSE:SYMBOL`` (or ``SYMBOL.NS``); bare symbols
    return Invalid API call for Indian names.
    """
    base = clean_underlying_symbol(market_ticker) or market_ticker.replace(".NS", "")
    base = (base or "").strip().upper()
    if not base:
        return ""
    mt = str(market_ticker).upper()
    if mt.endswith(".NS") or ".NS" in mt:
        return f"NSE:{base}"
    return base


def _ensure_eodhd_nse_exchange_suffix(session: requests.Session | None = None) -> str:
    """
    EODHD uses ``BASE.EXCHANGE``; NSE India is usually ``.NSE`` but some accounts/eras use
    other codes. Probe once per process (override with env EODHD_NSE_EXCHANGE_CODE).
    """
    global _EODHD_NSE_EXCHANGE_SUFFIX, _EODHD_NETWORK_BLOCKED
    if _EODHD_NSE_EXCHANGE_SUFFIX is not None:
        return _EODHD_NSE_EXCHANGE_SUFFIX
    with _EODHD_SUFFIX_LOCK:
        if _EODHD_NSE_EXCHANGE_SUFFIX is not None:
            return _EODHD_NSE_EXCHANGE_SUFFIX
        override = os.getenv("EODHD_NSE_EXCHANGE_CODE", "").strip().upper()
        if override:
            _EODHD_NSE_EXCHANGE_SUFFIX = override
            logger.info("EODHD India exchange suffix from env: %s", override)
            return _EODHD_NSE_EXCHANGE_SUFFIX
        if not EODHD_API_KEY:
            _EODHD_NSE_EXCHANGE_SUFFIX = "NSE"
            return _EODHD_NSE_EXCHANGE_SUFFIX
        sess = session if session is not None else requests.Session()
        candidates = ("NSE", "XNSE", "NS", "IN")
        for suf in candidates:
            probe = f"RELIANCE.{suf}"
            try:
                r = sess.get(
                    f"https://eodhd.com/api/eod/{probe}",
                    params={"api_token": EODHD_API_KEY, "period": "d", "fmt": "json"},
                    timeout=NETWORK_REQUEST_TIMEOUT_SECONDS,
                )
                record_eodhd_api_calls(1)
            except Exception as exc:
                logger.info("EODHD exchange probe failed for %s: %s", probe, _sanitize_provider_error(exc))
                if _is_dns_or_network_block_error(exc):
                    _EODHD_NETWORK_BLOCKED = True
                    _mark_provider_status("EODHD", "network/DNS unavailable")
                    logger.warning("EODHD appears network/DNS blocked; skipping further EODHD calls this run.")
                    break
                continue
            if not r.ok:
                continue
            try:
                data = r.json()
            except Exception:
                continue
            if isinstance(data, list) and len(data) >= 1:
                _EODHD_NSE_EXCHANGE_SUFFIX = suf
                logger.info(
                    "EODHD India exchange auto-selected: %s (probe %s returned %s rows)",
                    suf,
                    probe,
                    len(data),
                )
                return _EODHD_NSE_EXCHANGE_SUFFIX
        _EODHD_NSE_EXCHANGE_SUFFIX = "NSE"
        logger.warning(
            "EODHD India exchange auto-detect failed (tried %s); using NSE. "
            "If all tickers 404, set EODHD_NSE_EXCHANGE_CODE in the environment or verify your EODHD plan includes India.",
            candidates,
        )
        return _EODHD_NSE_EXCHANGE_SUFFIX


def _to_eodhd_symbol(market_ticker: str, session: requests.Session | None = None) -> str:
    base = clean_underlying_symbol(market_ticker) or market_ticker.replace(".NS", "")
    suffix = _ensure_eodhd_nse_exchange_suffix(session)
    return f"{base}.{suffix}"


def _alpha_wait() -> None:
    global _ALPHA_LAST_CALL_TS
    now = time.monotonic()
    wait_for = 12.0 - (now - _ALPHA_LAST_CALL_TS)
    if wait_for > 0:
        time.sleep(wait_for)
    _ALPHA_LAST_CALL_TS = time.monotonic()


def _eodhd_wait() -> None:
    global _EODHD_LAST_CALL_TS
    now = time.monotonic()
    wait_for = 1.0 - (now - _EODHD_LAST_CALL_TS)
    if wait_for > 0:
        time.sleep(wait_for)
    _EODHD_LAST_CALL_TS = time.monotonic()


def _map_interval_to_alpha(interval: str) -> str:
    iv = (interval or "15m").strip().lower()
    return {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min", "1h": "60min"}.get(iv, "15min")


def _map_interval_to_eodhd(interval: str) -> str:
    """
    EODHD intraday API accepts only 1m, 5m, and 1h (see eodhd.com intraday docs). Other UI
    intervals are mapped to the closest supported bucket so requests are not rejected/empty.
    """
    iv = (interval or "15m").strip().lower()
    if iv == "1m":
        return "1m"
    if iv in {"5m", "15m"}:
        return "5m"
    if iv in {"30m", "60m", "1h"}:
        return "1h"
    return "5m"


def _map_interval_to_yahoo(interval: str) -> str:
    iv = (interval or "15m").strip().lower()
    return {
        "1m": "1m",
        "2m": "2m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "60m": "60m",
        "90m": "90m",
        "1h": "60m",
        "1d": "1d",
    }.get(iv, "15m")


def _map_period_to_yahoo(period: str) -> str:
    """Map UI period strings to yfinance `period` values.

    yfinance only accepts a fixed set (e.g. 1d, 7d, 1mo, …). Values like ``30d``
    are not valid; using the old ``else "7d"`` default caused ~45 hourly bars for
    a "30d" scan and every symbol failed ``StrategyConfig.minimum_rows`` (60).
    """
    p = (period or "7d").strip().lower()
    allowed = {
        "1d",
        "5d",
        "7d",
        "1mo",
        "3mo",
        "6mo",
        "1y",
        "2y",
        "5y",
        "10y",
        "ytd",
        "max",
    }
    if p in allowed:
        return p
    if p.endswith("d"):
        try:
            days = int(p[:-1])
        except ValueError:
            return "7d"
        if days <= 5:
            mapped = "5d"
        elif days <= 7:
            mapped = "7d"
        elif days <= 31:
            mapped = "1mo"
        elif days <= 93:
            mapped = "3mo"
        elif days <= 186:
            mapped = "6mo"
        elif days <= 400:
            mapped = "1y"
        elif days <= 800:
            mapped = "2y"
        else:
            mapped = "5y"
        if p != mapped:
            logger.info("Yahoo period: UI %r maps to yfinance period=%r (need enough bars for indicators)", period, mapped)
        return mapped
    return "7d"


def _yahoo_rate_limit_wait() -> None:
    global _YAHOO_LAST_CALL_TS
    now = time.monotonic()
    wait_for = float(YAHOO_MIN_CALL_GAP_SECONDS) - (now - _YAHOO_LAST_CALL_TS)
    if wait_for > 0:
        time.sleep(wait_for)
    _YAHOO_LAST_CALL_TS = time.monotonic()


def _yahoo_cache_get(key: tuple[Any, ...]) -> pd.DataFrame | None:
    now = time.time()
    with _YAHOO_CACHE_LOCK:
        hit = _YAHOO_FETCH_CACHE.get(key)
        if not hit:
            return None
        ts, frame = hit
        if (now - float(ts)) > float(YAHOO_FETCH_CACHE_TTL_SECONDS):
            _YAHOO_FETCH_CACHE.pop(key, None)
            return None
        return frame.copy()


def _yahoo_cache_set(key: tuple[Any, ...], frame: pd.DataFrame) -> None:
    if frame is None or frame.empty:
        return
    with _YAHOO_CACHE_LOCK:
        _YAHOO_FETCH_CACHE[key] = (time.time(), frame.copy())


def _mark_provider_status(provider: str, message: str) -> None:
    if not provider:
        return
    _PROVIDER_STATUS[str(provider).upper()] = str(message).strip()


def _is_proxy_block_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = (
        "proxyerror",
        "tunnel connection failed",
        "unable to connect to proxy",
        "connect tunnel failed",
        "407 proxy",
        "403 forbidden",
    )
    return any(part in msg for part in needles)


def _is_dns_or_network_block_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    needles = (
        "nameresolutionerror",
        "failed to resolve",
        "nodename nor servname",
        "name or service not known",
        "temporary failure in name resolution",
        "getaddrinfo failed",
        "could not resolve host",
    )
    return _is_proxy_block_error(exc) or any(part in msg for part in needles)


def _sanitize_provider_error(exc: Exception | str) -> str:
    msg = str(exc)
    msg = re.sub(r"([?&](?:apikey|api_token)=)[^&\s)]+", r"\1***", msg, flags=re.IGNORECASE)
    return msg


def _provider_dns_ok(host: str, provider: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception as exc:
        logger.warning("%s DNS probe failed for %s: %s", provider, host, _sanitize_provider_error(exc))
        _mark_provider_status(provider, "network/DNS unavailable")
        return False


def _provider_unavailable_messages() -> list[str]:
    labels = {
        "YAHOO": "Yahoo unavailable",
        "ALPHA": "Alpha Vantage unavailable",
        "EODHD": "EODHD unavailable",
    }
    messages = []
    for key in ("YAHOO", "ALPHA", "EODHD"):
        reason = _PROVIDER_STATUS.get(key)
        if reason:
            messages.append(f"{labels[key]}: {reason}")
    return messages


def _fetch_alpha_intraday(
    market_ticker: str,
    interval: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    global _ALPHA_INTRADAY_BLOCKED
    if not ALPHA_API_KEY:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    alpha_iv = _map_interval_to_alpha(interval)
    sym = _to_alpha_symbol(market_ticker)
    logger.info("API Alpha Vantage: TIME_SERIES_INTRADAY request symbol=%s interval=%s", sym, alpha_iv)
    _alpha_wait()
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": sym,
        "interval": alpha_iv,
        "outputsize": "compact",
        "apikey": ALPHA_API_KEY,
    }
    sess = session if session is not None else requests
    response = sess.get(
        "https://www.alphavantage.co/query",
        params=params,
        timeout=NETWORK_REQUEST_TIMEOUT_SECONDS,
    )
    record_alpha_api_call()
    if not response.ok:
        logger.warning(
            "API Alpha Vantage: HTTP %s for symbol=%s interval=%s",
            response.status_code,
            sym,
            alpha_iv,
        )
    payload = response.json() if response.ok else {}
    series = payload.get(f"Time Series ({alpha_iv})")
    if not isinstance(series, dict) or not series:
        note = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
        if note:
            logger.info("API Alpha Vantage: no series for %s — %s", sym, str(note)[:200])
            low = str(note).lower()
            if "premium" in low or "subscribe" in low:
                _ALPHA_INTRADAY_BLOCKED = True
                logger.warning(
                    "Alpha TIME_SERIES_INTRADAY not available on this key (premium/subscription message). "
                    "Further intraday calls skipped this process; use EODHD or Alpha daily."
                )
        else:
            logger.info("API Alpha Vantage: empty intraday series for %s interval=%s", sym, alpha_iv)
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    rows = pd.DataFrame.from_dict(series, orient="index")
    rows.index = pd.to_datetime(rows.index, errors="coerce")
    rows = rows.rename(
        columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume",
        }
    )
    out = _normalize_ohlcv_frame(rows)
    logger.info(
        "API Alpha Vantage: success symbol=%s rows=%s",
        sym,
        len(out.index),
    )
    return out


def _fetch_alpha_daily(
    market_ticker: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """TIME_SERIES_DAILY (compact) — usually included on free Alpha keys when intraday is not."""
    if not ALPHA_API_KEY:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    sym = _to_alpha_symbol(market_ticker)
    logger.info("API Alpha Vantage: TIME_SERIES_DAILY request symbol=%s", sym)
    _alpha_wait()
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": sym,
        "outputsize": "compact",
        "apikey": ALPHA_API_KEY,
    }
    sess = session if session is not None else requests
    response = sess.get(
        "https://www.alphavantage.co/query",
        params=params,
        timeout=NETWORK_REQUEST_TIMEOUT_SECONDS,
    )
    record_alpha_api_call()
    if not response.ok:
        logger.warning("API Alpha Vantage: daily HTTP %s for %s", response.status_code, sym)
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    payload = response.json()
    series = payload.get("Time Series (Daily)")
    if not isinstance(series, dict) or not series:
        note = payload.get("Note") or payload.get("Information") or payload.get("Error Message")
        if note:
            logger.info("API Alpha Vantage: no daily series for %s — %s", sym, str(note)[:200])
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    rows = pd.DataFrame.from_dict(series, orient="index")
    rows.index = pd.to_datetime(rows.index, errors="coerce")
    rows = rows.rename(
        columns={
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume",
        }
    )
    out = _normalize_ohlcv_frame(rows)
    logger.info("API Alpha Vantage: daily success symbol=%s rows=%s", sym, len(out.index))
    return out


def _fetch_eodhd_intraday_or_daily(
    market_ticker: str,
    interval: str,
    session: requests.Session | None = None,
    throttle: bool = True,
) -> tuple[pd.DataFrame, int]:
    if not EODHD_API_KEY:
        return pd.DataFrame(columns=OHLCV_COLUMNS), 0
    if _EODHD_NETWORK_BLOCKED:
        return pd.DataFrame(columns=OHLCV_COLUMNS), 0
    sess = session if session is not None else requests
    eod_symbol = _to_eodhd_symbol(market_ticker, sess)
    eod_iv = _map_interval_to_eodhd(interval)
    http_calls = 0

    if throttle:
        _eodhd_wait()
    intraday_url = f"https://eodhd.com/api/intraday/{eod_symbol}"
    intraday_params = {"api_token": EODHD_API_KEY, "interval": eod_iv, "fmt": "json"}
    logger.debug("API EODHD: intraday GET %s interval=%s throttle=%s", eod_symbol, eod_iv, throttle)
    intraday_resp = sess.get(
        intraday_url,
        params=intraday_params,
        timeout=NETWORK_REQUEST_TIMEOUT_SECONDS,
    )
    http_calls += 1
    if intraday_resp.ok:
        try:
            payload = intraday_resp.json()
        except Exception as exc:
            logger.warning(
                "API EODHD: intraday JSON parse failed for %s: %s body_prefix=%r",
                eod_symbol,
                exc,
                (intraday_resp.text or "")[:200],
            )
            payload = None
        if isinstance(payload, dict):
            logger.warning(
                "API EODHD: intraday non-list response for %s interval=%s (from UI %s): %s",
                eod_symbol,
                eod_iv,
                interval,
                str(payload)[:400],
            )
        elif isinstance(payload, list) and payload:
            rows = pd.DataFrame(payload)
            rows["Datetime"] = pd.to_datetime(rows.get("datetime"), errors="coerce")
            rows = rows.set_index("Datetime").rename(
                columns={
                    "open": "Open",
                    "high": "High",
                    "low": "Low",
                    "close": "Close",
                    "volume": "Volume",
                }
            )
            frame = _normalize_ohlcv_frame(rows)
            if not frame.empty:
                logger.info(
                    "API EODHD: intraday OK %s rows=%s interval=%s",
                    eod_symbol,
                    len(frame.index),
                    eod_iv,
                )
                return frame, http_calls
        elif isinstance(payload, list) and not payload:
            logger.info(
                "API EODHD: intraday empty list for %s interval=%s — trying daily EOD",
                eod_symbol,
                eod_iv,
            )
    else:
        logger.warning(
            "API EODHD: intraday HTTP %s for %s body_prefix=%r",
            intraday_resp.status_code,
            eod_symbol,
            (intraday_resp.text or "")[:240],
        )

    if throttle:
        _eodhd_wait()
    daily_url = f"https://eodhd.com/api/eod/{eod_symbol}"
    daily_params = {"api_token": EODHD_API_KEY, "period": "d", "fmt": "json"}
    logger.debug("API EODHD: daily EOD GET %s (intraday empty or missing)", eod_symbol)
    daily_resp = sess.get(
        daily_url,
        params=daily_params,
        timeout=NETWORK_REQUEST_TIMEOUT_SECONDS,
    )
    http_calls += 1
    if not daily_resp.ok:
        logger.warning(
            "API EODHD: daily HTTP %s for %s body_prefix=%r",
            daily_resp.status_code,
            eod_symbol,
            (daily_resp.text or "")[:240],
        )
        return pd.DataFrame(columns=OHLCV_COLUMNS), http_calls
    try:
        payload = daily_resp.json()
    except Exception as exc:
        logger.warning(
            "API EODHD: daily JSON parse failed for %s: %s body_prefix=%r",
            eod_symbol,
            exc,
            (daily_resp.text or "")[:200],
        )
        return pd.DataFrame(columns=OHLCV_COLUMNS), http_calls
    if not isinstance(payload, list) or not payload:
        logger.warning(
            "API EODHD: daily empty or non-list payload for %s: %s",
            eod_symbol,
            str(payload)[:400],
        )
        return pd.DataFrame(columns=OHLCV_COLUMNS), http_calls
    rows = pd.DataFrame(payload)
    rows["Datetime"] = pd.to_datetime(rows.get("date"), errors="coerce")
    rows = rows.set_index("Datetime").rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )
    daily_frame = _normalize_ohlcv_frame(rows)
    if daily_frame.empty:
        logger.warning(
            "API EODHD: daily bars normalized to empty for %s (check symbol on EODHD)",
            eod_symbol,
        )
    else:
        logger.info(
            "API EODHD: daily fallback OK %s rows=%s http_calls=%s",
            eod_symbol,
            len(daily_frame.index),
            http_calls,
        )
    return daily_frame, http_calls


def _get_market_data_bulk_eodhd(
    symbols: tuple[str, ...],
    tickers: tuple[str, ...],
    interval: str,
) -> pd.DataFrame:
    """Parallel EODHD-only fetch for large symbol lists (avoids Alpha's 5/min serial bottleneck)."""
    t0 = time.perf_counter()
    frames_by_ticker: dict[str, pd.DataFrame] = {}
    failed_symbols: list[str] = []
    eod_http_total = 0
    n_sym = len(symbols)
    max_workers = min(EODHD_FALLBACK_MAX_WORKERS, max(1, n_sym))
    logger.info(
        "get_market_data bulk EODHD: starting parallel fetch symbols=%s workers=%s interval=%s",
        n_sym,
        max_workers,
        interval,
    )

    def _one(i: int) -> tuple[str, str, pd.DataFrame, int]:
        raw_sym = symbols[i]
        ticker = tickers[i]
        sess = requests.Session()
        try:
            frame, n_http = _fetch_eodhd_intraday_or_daily(
                ticker, interval, session=sess, throttle=False
            )
            return str(raw_sym), ticker, frame, int(n_http)
        except Exception as exc:
            logger.info("EODHD fetch failed for %s: %s", ticker, exc)
            return str(raw_sym), ticker, pd.DataFrame(columns=OHLCV_COLUMNS), 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_one, i): i for i in range(n_sym)}
        for fut in as_completed(futures):
            raw_sym, ticker, frame, nh = fut.result()
            eod_http_total += nh
            if frame.empty:
                failed_symbols.append(raw_sym)
                logger.info("No EODHD OHLC data for %s", raw_sym)
            else:
                frames_by_ticker[ticker] = frame
    record_eodhd_api_calls(eod_http_total)
    elapsed = time.perf_counter() - t0
    print("Alpha success count: 0")
    print(f"EODHD success count: {len(frames_by_ticker)}")
    if failed_symbols:
        print(f"Failed symbols: {', '.join(failed_symbols)}")
    logger.info(
        "get_market_data bulk EODHD: done tickers_ok=%s failed=%s http_calls=%s elapsed_s=%.2f",
        len(frames_by_ticker),
        len(failed_symbols),
        eod_http_total,
        elapsed,
    )

    if not frames_by_ticker:
        logger.warning("get_market_data bulk EODHD: no frames returned")
        return pd.DataFrame()
    merged = pd.concat(frames_by_ticker, axis=1, sort=True).sort_index()
    logger.info(
        "get_market_data bulk EODHD: merged OHLCV shape=%s index_len=%s",
        merged.shape,
        len(merged.index),
    )
    return merged


def get_market_data(symbols: tuple[str, ...], interval: str = "15m", period: str = "7d") -> pd.DataFrame:
    """
    Multi-source OHLCV pipeline:
    1) Yahoo batch download for all symbols.
    2) Alpha intraday only for symbols still missing after Yahoo.
    3) EODHD intraday/daily only for symbols still missing after Alpha.

    A small in-process Yahoo cache avoids re-downloading identical request tuples for 60s.
    """
    if not symbols:
        logger.info("get_market_data: empty symbol list, returning empty DataFrame")
        return pd.DataFrame()

    tickers = tuple(to_market_ticker_key(symbol) for symbol in symbols)
    n_sym = len(symbols)
    cache_key: tuple[Any, ...] = ("multi_source_market_data", tickers, str(interval or "15m"), str(period or "7d"))
    cached = _yahoo_cache_get(cache_key)
    if cached is not None and not cached.empty:
        logger.info("get_market_data: yahoo process-cache HIT key=%s shape=%s", cache_key[0], cached.shape)
        return cached

    logger.info(
        "get_market_data: start symbols=%s interval=%s period=%s alpha_key=%s eodhd_key=%s",
        n_sym,
        interval,
        period,
        bool(ALPHA_API_KEY),
        bool(EODHD_API_KEY),
    )

    t_seq = time.perf_counter()
    frames_by_ticker: dict[str, pd.DataFrame] = {}
    failed_symbols: list[str] = []
    yahoo_success = 0
    alpha_success = 0
    eodhd_success = 0
    global _ALPHA_NETWORK_BLOCKED, _EODHD_NETWORK_BLOCKED
    logged_alpha_limit = False
    _PROVIDER_STATUS.clear()
    if _ALPHA_NETWORK_BLOCKED:
        _mark_provider_status("ALPHA", "network/DNS unavailable")
    if _EODHD_NETWORK_BLOCKED:
        _mark_provider_status("EODHD", "network/DNS unavailable")
    if ALPHA_API_KEY and not _ALPHA_NETWORK_BLOCKED and not _provider_dns_ok("www.alphavantage.co", "ALPHA"):
        _ALPHA_NETWORK_BLOCKED = True
    if EODHD_API_KEY and not _EODHD_NETWORK_BLOCKED and not _provider_dns_ok("eodhd.com", "EODHD"):
        _EODHD_NETWORK_BLOCKED = True
    yahoo_dns_ok = _provider_dns_ok("guce.yahoo.com", "YAHOO")
    logger.info(
        "get_market_data: provider preflight alpha_blocked=%s eodhd_blocked=%s yahoo_dns_ok=%s",
        _ALPHA_NETWORK_BLOCKED,
        _EODHD_NETWORK_BLOCKED,
        yahoo_dns_ok,
    )

    with requests.Session() as http_session:
        # 1) Yahoo batch for all symbols.
        yahoo_payload = pd.DataFrame()
        yahoo_iv = _map_interval_to_yahoo(interval)
        yahoo_period = _map_period_to_yahoo(period)
        try:
            if yahoo_dns_ok:
                _yahoo_rate_limit_wait()
                yahoo_payload = yf.download(
                    tickers=list(tickers),
                    period=yahoo_period,
                    interval=yahoo_iv,
                    auto_adjust=False,
                    group_by="ticker",
                    threads=True,
                    progress=False,
                )
            else:
                logger.warning("Yahoo fetch skipped because guce.yahoo.com DNS is unavailable.")
            logger.info(
                "Yahoo batch download complete symbols=%s period=%s interval=%s shape=%s",
                n_sym,
                yahoo_period,
                yahoo_iv,
                yahoo_payload.shape if isinstance(yahoo_payload, pd.DataFrame) else None,
            )
            if yahoo_dns_ok and isinstance(yahoo_payload, pd.DataFrame) and yahoo_payload.empty:
                logger.warning(
                    "Yahoo batch returned empty frame for symbols=%s period=%s interval=%s; "
                    "if stderr shows 'Could not resolve host: guce.yahoo.com', DNS/network blocks Yahoo consent host.",
                    n_sym,
                    yahoo_period,
                    yahoo_iv,
                )
                _mark_provider_status("YAHOO", "no data returned from Yahoo Finance")
        except Exception as exc:
            logger.info("Yahoo batch download failed; will use Alpha/EODHD fallback: %s", exc)
            _mark_provider_status("YAHOO", str(exc))
            yahoo_payload = pd.DataFrame()

        missing_after_yahoo: list[tuple[str, str]] = []
        for raw_sym, ticker in zip(symbols, tickers):
            frame = _extract_merged_ticker_frame(yahoo_payload, ticker, n_sym)
            if frame.empty:
                missing_after_yahoo.append((str(raw_sym), ticker))
            else:
                frames_by_ticker[ticker] = frame
                yahoo_success += 1

        # 2) Alpha fallback for symbols still missing after Yahoo.
        missing_after_alpha: list[tuple[str, str]] = []
        for raw_sym, ticker in missing_after_yahoo:
            frame = pd.DataFrame(columns=OHLCV_COLUMNS)
            use_alpha_intraday = False
            if (
                ALPHA_API_KEY
                and not _ALPHA_INTRADAY_BLOCKED
                and not _ALPHA_NETWORK_BLOCKED
            ):
                try:
                    du = compute_api_usage_display()
                    if str(du.get("alpha_status")) == "BLOCKED" or int(du.get("alpha_minute_remaining", 0)) <= 0:
                        use_alpha_intraday = False
                        if not logged_alpha_limit:
                            print("Alpha limit reached → using EODHD")
                            logger.info("Alpha limit reached → using EODHD")
                            logged_alpha_limit = True
                    else:
                        use_alpha_intraday = True
                except Exception:
                    use_alpha_intraday = True

            if use_alpha_intraday:
                try:
                    frame = _fetch_alpha_intraday(ticker, interval, http_session)
                except Exception as exc:
                    safe_exc = _sanitize_provider_error(exc)
                    logger.info("Alpha Vantage intraday failed for %s: %s", ticker, safe_exc)
                    _mark_provider_status("ALPHA", safe_exc)
                    if _is_dns_or_network_block_error(exc):
                        _ALPHA_NETWORK_BLOCKED = True
                        logger.warning(
                            "Alpha intraday appears network/DNS blocked; skipping further Alpha calls this run."
                        )
                    frame = pd.DataFrame(columns=OHLCV_COLUMNS)
                if not frame.empty:
                    alpha_success += 1
                    frames_by_ticker[ticker] = frame
                elif use_alpha_intraday and "ALPHA" not in _PROVIDER_STATUS:
                    _mark_provider_status("ALPHA", "no data returned from Alpha Vantage")

            if frame.empty:
                missing_after_alpha.append((raw_sym, ticker))

        # 3) EODHD fallback for symbols still missing after Alpha (small thread pool).
        eod_http_total = 0
        if missing_after_alpha and EODHD_API_KEY and not _EODHD_NETWORK_BLOCKED:
            workers = min(EODHD_FALLBACK_MAX_WORKERS, max(1, len(missing_after_alpha)))

            def _eod_one(item: tuple[str, str]) -> tuple[str, str, pd.DataFrame, int]:
                global _EODHD_NETWORK_BLOCKED
                raw_sym, ticker = item
                sess = requests.Session()
                try:
                    frame, n_http = _fetch_eodhd_intraday_or_daily(
                        ticker, interval, session=sess, throttle=False
                    )
                    return raw_sym, ticker, frame, int(n_http)
                except Exception as exc:
                    safe_exc = _sanitize_provider_error(exc)
                    logger.info("EODHD fetch failed for %s: %s", ticker, safe_exc)
                    _mark_provider_status("EODHD", safe_exc)
                    if _is_dns_or_network_block_error(exc):
                        _EODHD_NETWORK_BLOCKED = True
                        _mark_provider_status("EODHD", "network/DNS unavailable")
                    return raw_sym, ticker, pd.DataFrame(columns=OHLCV_COLUMNS), 0

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = [executor.submit(_eod_one, item) for item in missing_after_alpha]
                total_fallback = len(futures)
                for i, fut in enumerate(as_completed(futures), start=1):
                    raw_sym, ticker, frame, nh = fut.result()
                    eod_http_total += int(nh)
                    if not frame.empty:
                        eodhd_success += 1
                        frames_by_ticker[ticker] = frame
                    elif "EODHD" not in _PROVIDER_STATUS:
                        _mark_provider_status("EODHD", "no data returned from EODHD")
                    if i == 1 or i % 10 == 0 or i == total_fallback:
                        logger.info(
                            "get_market_data: EODHD fallback progress %s/%s (success=%s)",
                            i,
                            total_fallback,
                            eodhd_success,
                        )
        if eod_http_total:
            record_eodhd_api_calls(eod_http_total)

        for raw_sym, ticker in missing_after_alpha:
            frame = frames_by_ticker.get(ticker)
            if frame is not None and not frame.empty:
                continue
            failed_symbols.append(str(raw_sym))
            logger.info("No OHLC data for %s after Yahoo, Alpha, EODHD", raw_sym)

        # Safety: if Yahoo succeeded but no fallback list was built.
        if not missing_after_alpha and not missing_after_yahoo and yahoo_success == 0:
            for raw_sym in symbols:
                failed_symbols.append(str(raw_sym))
                logger.info("No OHLC data for %s after Yahoo, Alpha, EODHD", raw_sym)

    print(f"Yahoo success count: {yahoo_success}")
    print(f"Alpha success count: {alpha_success}")
    print(f"EODHD success count: {eodhd_success}")
    if failed_symbols:
        print(f"Failed symbols: {', '.join(failed_symbols)}")
    elapsed_seq = time.perf_counter() - t_seq
    logger.info(
        "get_market_data: done yahoo_ok=%s alpha_ok=%s eodhd_ok=%s failed=%s elapsed_s=%.2f",
        yahoo_success,
        alpha_success,
        eodhd_success,
        len(failed_symbols),
        elapsed_seq,
    )

    if not frames_by_ticker:
        logger.warning("get_market_data: pipeline returned no ticker frames")
        return pd.DataFrame()
    merged_seq = pd.concat(frames_by_ticker, axis=1, sort=True).sort_index()
    _yahoo_cache_set(cache_key, merged_seq)
    logger.info("get_market_data: merged OHLCV shape=%s", merged_seq.shape)
    return merged_seq


def download_market_data(symbols: tuple[str, ...], period: str, interval: str) -> pd.DataFrame:
    df, src, age = get_market_data_with_cache(
        symbols,
        str(period or "7d"),
        str(interval or "15m"),
    )
    shape = df.shape if isinstance(df, pd.DataFrame) else ()
    logger.info(
        "download_market_data: done source=%s age_seconds=%.2f shape=%s symbols_requested=%s",
        src,
        float(age),
        shape,
        len(symbols),
    )
    return df if isinstance(df, pd.DataFrame) else _safe_empty_market_df()


def clear_market_data_cache() -> None:
    global _ALPHA_LAST_CALL_TS, _EODHD_LAST_CALL_TS, _YAHOO_LAST_CALL_TS
    global _ALPHA_INTRADAY_BLOCKED, _ALPHA_NETWORK_BLOCKED, _EODHD_NETWORK_BLOCKED, _EODHD_NSE_EXCHANGE_SUFFIX
    _ALPHA_LAST_CALL_TS = 0.0
    _EODHD_LAST_CALL_TS = 0.0
    _YAHOO_LAST_CALL_TS = 0.0
    _ALPHA_INTRADAY_BLOCKED = False
    _ALPHA_NETWORK_BLOCKED = False
    _EODHD_NETWORK_BLOCKED = False
    _PROVIDER_STATUS.clear()
    _EODHD_NSE_EXCHANGE_SUFFIX = None
    with _YAHOO_CACHE_LOCK:
        _YAHOO_FETCH_CACHE.clear()
    logger.info("Market cache: clear requested (session cache + disk meta/pickle)")
    try:
        st.session_state["market_cache"] = {"data": None, "timestamp": 0.0, "params": None}
        st.session_state[MARKET_FETCH_META_KEY] = {"source": "FAILED", "age_seconds": 0.0, "ts": time.time()}
    except Exception:
        pass
    try:
        if MARKET_API_META_PATH.exists():
            MARKET_API_META_PATH.unlink()
    except OSError:
        logger.info("Could not remove market_api_meta.json during cache clear", exc_info=True)
    try:
        if MARKET_CACHE_DF_PATH.exists():
            MARKET_CACHE_DF_PATH.unlink()
    except OSError:
        logger.info("Could not remove market_cache_latest.pkl during cache clear", exc_info=True)


def _canonical_market_download_params(symbols: list[str], period: str, interval: str) -> tuple[list[str], str, str]:
    """Match download_market_data / get_market_data_with_cache period & interval fallbacks."""
    period_s = str(period or "7d")
    interval_s = str(interval or "15m")
    return list(symbols), period_s, interval_s


def _market_symbols_key(symbols: list[str] | tuple[str, ...]) -> str:
    return "|".join(sorted(str(s) for s in symbols))


def _write_market_api_live_meta(symbols: tuple[str, ...], period_s: str, interval_s: str, epoch: float) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_live_epoch": float(epoch),
            "symbols": list(symbols),
            "symbols_key": _market_symbols_key(symbols),
            "period": period_s,
            "interval": interval_s,
        }
        MARKET_API_META_PATH.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        logger.debug(
            "Market cache: wrote API meta last_live_epoch=%s symbols_key=%s",
            payload.get("last_live_epoch"),
            payload.get("symbols_key"),
        )
    except OSError:
        logger.info("Could not write market_api_meta.json", exc_info=True)


def _read_market_api_live_meta() -> dict[str, Any] | None:
    if not MARKET_API_META_PATH.exists():
        return None
    try:
        raw = json.loads(MARKET_API_META_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_market_cache_df_to_disk(df: pd.DataFrame) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        df.to_pickle(MARKET_CACHE_DF_PATH)
        logger.info(
            "Market cache: wrote disk pickle path=%s shape=%s",
            MARKET_CACHE_DF_PATH,
            df.shape,
        )
    except OSError:
        logger.info("Could not write market_cache_latest.pkl", exc_info=True)


def _read_market_cache_df_from_disk() -> pd.DataFrame | None:
    if not MARKET_CACHE_DF_PATH.exists():
        return None
    try:
        obj = pd.read_pickle(MARKET_CACHE_DF_PATH)
        if isinstance(obj, pd.DataFrame) and not obj.empty:
            logger.info(
                "Market cache: read disk pickle path=%s shape=%s",
                MARKET_CACHE_DF_PATH,
                obj.shape,
            )
        return obj if isinstance(obj, pd.DataFrame) else None
    except Exception:
        logger.info("Could not read market_cache_latest.pkl", exc_info=True)
        return None


def _disk_meta_matches_scan(meta: dict[str, Any], symbols: list[str], period: str, interval: str) -> bool:
    sym_c, ps, ins = _canonical_market_download_params(symbols, period, interval)
    if str(meta.get("period") or "") != ps or str(meta.get("interval") or "") != ins:
        return False
    key = _market_symbols_key(sym_c)
    if str(meta.get("symbols_key") or "") == key:
        return True
    return list(meta.get("symbols") or []) == sym_c


def _init_market_session_cache() -> None:
    if "market_cache" not in st.session_state:
        st.session_state.market_cache = {"data": None, "timestamp": 0.0, "params": None}


def _safe_empty_market_df() -> pd.DataFrame:
    return pd.DataFrame()


def get_market_data_with_cache(
    symbols: tuple[str, ...],
    period: str,
    interval: str,
) -> tuple[pd.DataFrame, str, float]:
    """
    Market OHLCV cache for get_market_data (Yahoo batch → Alpha gaps → EODHD gaps, as implemented there).

    - MARKET_SESSION_CACHE_TTL (seconds): in-process + disk snapshot; no new provider calls
      while (now - last_live_epoch) < TTL unless MARKET_FORCE_NEXT_KEY forces refresh.
    - Disk: MARKET_API_META_PATH (last LIVE epoch + params) + MARKET_CACHE_DF_PATH (pickle).
    """
    _init_market_session_cache()
    now = time.time()
    cache = st.session_state.market_cache
    period_s = str(period or "7d")
    interval_s = str(interval or "15m")
    params_key = (tuple(symbols), period_s, interval_s)

    try:
        st.session_state[MARKET_QUOTA_FLAG_KEY] = False
    except Exception:
        pass

    force_refresh = False
    try:
        force_refresh = bool(st.session_state.pop(MARKET_FORCE_NEXT_KEY, False))
    except Exception:
        force_refresh = False

    logger.info(
        "get_market_data_with_cache: entry symbol_count=%s period=%s interval=%s force_refresh=%s ttl_s=%s",
        len(symbols),
        period_s,
        interval_s,
        force_refresh,
        MARKET_SESSION_CACHE_TTL,
    )

    def _write_meta(source: str, age_seconds: float) -> None:
        try:
            st.session_state[MARKET_FETCH_META_KEY] = {
                "source": source,
                "age_seconds": float(age_seconds),
                "ts": now,
            }
        except Exception:
            pass

    # Reload last LIVE snapshot from disk (new browser session / refresh) — no Alpha/EODHD
    # until TTL expires or user forces refresh.
    if not force_refresh:
        dm = _read_market_api_live_meta()
        if dm and _disk_meta_matches_scan(dm, list(symbols), period, interval):
            live_ts = float(dm.get("last_live_epoch") or 0.0)
            if live_ts > 0 and (now - live_ts) < float(MARKET_SESSION_CACHE_TTL):
                on_disk = _read_market_cache_df_from_disk()
                if isinstance(on_disk, pd.DataFrame) and not on_disk.empty:
                    cache["data"] = on_disk.copy()
                    cache["timestamp"] = live_ts
                    cache["params"] = params_key
                    age = max(0.0, now - live_ts)
                    _write_meta("CACHE", age)
                    logger.info(
                        "get_market_data_with_cache: CACHE from disk pickle + meta (no API) age_s=%.2f shape=%s",
                        age,
                        on_disk.shape,
                    )
                    return on_disk.copy(), "CACHE", age
                logger.warning(
                    "get_market_data_with_cache: disk meta TTL valid but pickle missing/empty — will refetch if needed path=%s",
                    MARKET_CACHE_DF_PATH,
                )

    quota_low = False
    try:
        if ALPHA_API_KEY:
            usage_view = compute_api_usage_display()
            if int(usage_view.get("alpha_minute_remaining", 99)) <= 1:
                quota_low = True
                st.session_state[MARKET_QUOTA_FLAG_KEY] = True
                logger.info(
                    "get_market_data_with_cache: Alpha minute quota nearly exhausted — prefer cache over new API calls"
                )
    except Exception:
        pass

    cached_data = cache.get("data")
    cached_ts = float(cache.get("timestamp") or 0.0)
    cached_params = cache.get("params")
    age_from_cache_ts = max(0.0, now - cached_ts) if cached_ts > 0 else 0.0

    if quota_low:
        if (
            cached_data is not None
            and isinstance(cached_data, pd.DataFrame)
            and cached_params == params_key
            and not cached_data.empty
        ):
            src = "CACHE" if age_from_cache_ts < MARKET_SESSION_CACHE_TTL else "STALE_CACHE"
            _write_meta(src, age_from_cache_ts)
            logger.info(
                "get_market_data_with_cache: serving %s (quota_low) age_s=%.2f shape=%s — no new API",
                src,
                age_from_cache_ts,
                cached_data.shape,
            )
            return cached_data.copy(), src, age_from_cache_ts
        logger.warning(
            "get_market_data_with_cache: Alpha quota low and no matching cache; continuing to LIVE fetch "
            "so Yahoo/EODHD can still provide data."
        )

    if (
        not force_refresh
        and cached_data is not None
        and isinstance(cached_data, pd.DataFrame)
        and cached_params == params_key
        and (now - cached_ts) < MARKET_SESSION_CACHE_TTL
    ):
        age = max(0.0, now - cached_ts)
        _write_meta("CACHE", age)
        logger.info(
            "get_market_data_with_cache: CACHE from in-process session age_s=%.2f shape=%s — no API",
            age,
            cached_data.shape,
        )
        return cached_data.copy(), "CACHE", age

    if force_refresh:
        logger.info("get_market_data_with_cache: force_refresh set — calling providers (ignoring TTL cache)")

    fresh: pd.DataFrame = _safe_empty_market_df()
    logger.info(
        "get_market_data_with_cache: cache miss or stale — calling get_market_data for %s symbols",
        len(symbols),
    )
    try:
        got = get_market_data(tuple(symbols), interval=interval_s, period=period_s)
        if isinstance(got, pd.DataFrame):
            fresh = got
    except Exception as exc:
        logger.warning("get_market_data_with_cache: get_market_data raised: %s", exc, exc_info=True)
        fresh = _safe_empty_market_df()

    if isinstance(fresh, pd.DataFrame) and not fresh.empty:
        cache["data"] = fresh.copy()
        cache["timestamp"] = now
        cache["params"] = params_key
        _write_meta("LIVE", 0.0)
        _write_market_api_live_meta(tuple(symbols), period_s, interval_s, now)
        _write_market_cache_df_to_disk(fresh)
        logger.info(
            "get_market_data_with_cache: LIVE provider data stored session+disk shape=%s",
            fresh.shape,
        )
        return fresh.copy(), "LIVE", 0.0

    if cached_data is not None and isinstance(cached_data, pd.DataFrame) and cached_params == params_key and not cached_data.empty:
        _write_meta("STALE_CACHE", age_from_cache_ts)
        logger.info(
            "get_market_data_with_cache: live fetch empty; STALE_CACHE fallback age_s=%.2f shape=%s",
            age_from_cache_ts,
            cached_data.shape,
        )
        return cached_data.copy(), "STALE_CACHE", age_from_cache_ts

    _write_meta("FAILED", 0.0)
    logger.warning("get_market_data_with_cache: FAILED — empty live result and no stale cache")
    return _safe_empty_market_df(), "FAILED", 0.0


def _ensure_market_cache_after_scan(
    symbols: list[str],
    period: str,
    interval: str,
    use_sample_data: bool,
) -> None:
    """
    If scan_symbols hit @st.cache_data and skipped download, session market_cache may be empty.
    Refill via download_market_data unless disk meta shows a recent LIVE fetch for the same
    scan params (then skip API — UI uses disk timestamp in render_market_data_cache_status_bar).
    """
    if use_sample_data or not symbols:
        logger.info(
            "Post-scan market cache sync: skipped (use_sample_data=%s symbols=%s)",
            use_sample_data,
            bool(symbols),
        )
        return
    try:
        _init_market_session_cache()
        ts = float(st.session_state.market_cache.get("timestamp") or 0.0)
        if ts > 0:
            logger.info("Post-scan market cache sync: skipped (session cache timestamp already set)")
            return
        disk = _read_market_api_live_meta()
        if disk and _disk_meta_matches_scan(disk, symbols, period, interval):
            live_ts = float(disk.get("last_live_epoch") or 0.0)
            if live_ts > 0 and (time.time() - live_ts) < float(MARKET_SESSION_CACHE_TTL):
                logger.info(
                    "Post-scan market cache sync: skipped (disk LIVE meta still within TTL, age_s=%.2f)",
                    time.time() - live_ts,
                )
                return
        logger.info(
            "Post-scan market cache sync: calling download_market_data for %s symbols",
            len(symbols),
        )
        download_market_data(tuple(symbols), period, interval)
    except Exception:
        logger.warning("Post-scan market cache sync failed", exc_info=True)


def _normalize_api_usage_from_disk(raw: dict[str, Any]) -> dict[str, Any] | None:
    try:
        a = raw.get("alpha")
        e = raw.get("eodhd")
        if not isinstance(a, dict) or not isinstance(e, dict):
            return None
        today = datetime.now().date().isoformat()
        now = time.time()
        return {
            "alpha": {
                "minute_calls": max(0, int(a.get("minute_calls", 0))),
                "day_calls": max(0, int(a.get("day_calls", 0))),
                "last_reset_minute": float(a.get("last_reset_minute", now)),
                "last_reset_day": str(a.get("last_reset_day") or today),
            },
            "eodhd": {
                "minute_calls": max(0, int(e.get("minute_calls", 0))),
                "last_reset_minute": float(e.get("last_reset_minute", now)),
            },
        }
    except (TypeError, ValueError):
        return None


def _load_api_usage_state_from_disk() -> dict[str, Any] | None:
    if not API_USAGE_PERSIST_PATH.exists():
        return None
    try:
        raw = json.loads(API_USAGE_PERSIST_PATH.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _persist_api_usage_state() -> None:
    try:
        usage = st.session_state.get(API_USAGE_STATE_KEY)
        if not isinstance(usage, dict):
            return
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        API_USAGE_PERSIST_PATH.write_text(json.dumps(usage, separators=(",", ":")), encoding="utf-8")
    except OSError:
        logger.info("Could not persist api_usage_state.json", exc_info=True)


def _apply_api_usage_window_resets(usage: dict[str, Any]) -> None:
    now = time.time()
    today = datetime.now().date().isoformat()
    a = usage["alpha"]
    if now - float(a["last_reset_minute"]) >= 60.0:
        a["minute_calls"] = 0
        a["last_reset_minute"] = now
    if str(a.get("last_reset_day")) != today:
        a["day_calls"] = 0
        a["last_reset_day"] = today

    e = usage["eodhd"]
    if now - float(e["last_reset_minute"]) >= 60.0:
        e["minute_calls"] = 0
        e["last_reset_minute"] = now


def _ensure_api_usage_state() -> dict[str, Any]:
    if API_USAGE_STATE_KEY not in st.session_state:
        loaded_raw = _load_api_usage_state_from_disk()
        normalized = _normalize_api_usage_from_disk(loaded_raw) if loaded_raw else None
        if normalized is not None:
            _apply_api_usage_window_resets(normalized)
            st.session_state[API_USAGE_STATE_KEY] = normalized
        else:
            now = time.time()
            today = datetime.now().date().isoformat()
            st.session_state[API_USAGE_STATE_KEY] = {
                "alpha": {
                    "minute_calls": 0,
                    "day_calls": 0,
                    "last_reset_minute": now,
                    "last_reset_day": today,
                },
                "eodhd": {"minute_calls": 0, "last_reset_minute": now},
            }
    return st.session_state[API_USAGE_STATE_KEY]


def record_alpha_api_call() -> None:
    usage = _ensure_api_usage_state()
    _apply_api_usage_window_resets(usage)
    usage["alpha"]["minute_calls"] += 1
    usage["alpha"]["day_calls"] += 1
    _persist_api_usage_state()


def record_eodhd_api_calls(count: int) -> None:
    if count <= 0:
        return
    usage = _ensure_api_usage_state()
    _apply_api_usage_window_resets(usage)
    usage["eodhd"]["minute_calls"] += int(count)
    _persist_api_usage_state()


def compute_api_usage_display() -> dict[str, Any]:
    usage = _ensure_api_usage_state()
    _apply_api_usage_window_resets(usage)
    now = time.time()
    a = usage["alpha"]
    e = usage["eodhd"]
    am = int(a["minute_calls"])
    ad = int(a["day_calls"])
    em = int(e["minute_calls"])

    min_rem = ALPHA_API_MINUTE_LIMIT - am
    day_rem = ALPHA_API_DAY_LIMIT - ad
    e_rem = EODHD_API_MINUTE_LIMIT - em

    alpha_window_elapsed = now - float(a["last_reset_minute"])
    alpha_next_safe_sec = max(0.0, 60.0 - alpha_window_elapsed)
    e_window_elapsed = now - float(e["last_reset_minute"])
    eodhd_next_safe_sec = max(0.0, 60.0 - e_window_elapsed)

    alpha_status = "OK"
    if am >= ALPHA_API_MINUTE_LIMIT or ad >= ALPHA_API_DAY_LIMIT:
        alpha_status = "BLOCKED"
    elif min_rem <= 1 or day_rem <= 10:
        alpha_status = "WARNING"

    eodhd_status = "OK"
    if em >= EODHD_API_MINUTE_LIMIT:
        eodhd_status = "BLOCKED"
    elif e_rem <= 1:
        eodhd_status = "WARNING"

    out = {
        "alpha_minute_used": am,
        "alpha_minute_cap": ALPHA_API_MINUTE_LIMIT,
        "alpha_minute_remaining": max(0, min_rem),
        "alpha_day_used": ad,
        "alpha_day_cap": ALPHA_API_DAY_LIMIT,
        "alpha_day_remaining": max(0, day_rem),
        "alpha_status": alpha_status,
        "alpha_next_safe_sec": alpha_next_safe_sec,
        "alpha_last_reset_epoch": float(a["last_reset_minute"]),
        "eodhd_minute_used": em,
        "eodhd_minute_cap": EODHD_API_MINUTE_LIMIT,
        "eodhd_minute_remaining": max(0, e_rem),
        "eodhd_status": eodhd_status,
        "eodhd_next_safe_sec": eodhd_next_safe_sec,
        "eodhd_last_reset_epoch": float(e["last_reset_minute"]),
        "server_now_epoch": now,
        "alpha_refresh_blocked": alpha_status == "BLOCKED",
        "refresh_blocked": alpha_status == "BLOCKED" or eodhd_status == "BLOCKED",
    }
    _persist_api_usage_state()
    return out


def _render_api_minute_bucket_live_row(d: dict[str, Any]) -> None:
    """Live-updating minute-bucket countdown under Alpha / EODHD (no Streamlit rerun)."""
    has_alpha = bool(ALPHA_API_KEY)
    has_eod = bool(EODHD_API_KEY)
    if not has_alpha and not has_eod:
        return

    cfg = {
        "has_alpha": has_alpha,
        "has_eod": has_eod,
        "alpha_last": float(d.get("alpha_last_reset_epoch", 0.0)),
        "eod_last": float(d.get("eodhd_last_reset_epoch", 0.0)),
        "server_now": float(d.get("server_now_epoch", time.time())),
    }
    cfg_json = json.dumps(cfg, separators=(",", ":")).replace("<", "\\u003c")

    html = """
<script type="application/json" id="api-bucket-cfg">""" + cfg_json + """</script>
<style>.api-bucket-row{font-family:system-ui,sans-serif;font-size:0.92rem;color:#eceff1;margin-top:4px;
  padding:10px 12px;background:#262730;border:1px solid #464b5e;border-radius:8px;display:flex;gap:20px;flex-wrap:wrap;}
.api-bucket-row .col{flex:1;min-width:140px;}</style>
<div class="api-bucket-row">
  <div class="col" id="alphaBucketCol" style="display:none;"><strong>Alpha Vantage</strong><br/>
    Next minute-bucket reset in <span id="apiAlphaSec">—</span> s</div>
  <div class="col" id="eodBucketCol" style="display:none;"><strong>EODHD</strong><br/>
    Next minute-bucket reset in <span id="apiEodSec">—</span> s</div>
</div>
<script>
(function(){
  var cfg = {};
  try { cfg = JSON.parse(document.getElementById("api-bucket-cfg").textContent || "{}"); } catch (e) {}
  var hasA = !!cfg.has_alpha, hasE = !!cfg.has_eod;
  var alphaLast = Number(cfg.alpha_last) || 0, eodLast = Number(cfg.eod_last) || 0;
  var serverNow = Number(cfg.server_now) || 0;
  var clientAt = Date.now() / 1000;
  var skew = serverNow > 0 ? (serverNow - clientAt) : 0;
  function now(){ return Date.now() / 1000 + skew; }
  function rem60(t0){ var e = Math.max(0, now() - t0); return Math.max(0, 60 - e); }
  var aCol = document.getElementById("alphaBucketCol"), eCol = document.getElementById("eodBucketCol");
  var aEl = document.getElementById("apiAlphaSec"), eEl = document.getElementById("apiEodSec");
  if (hasA) { aCol.style.display = "block"; }
  if (hasE) { eCol.style.display = "block"; }
  function tick(){
    if (hasA && aEl) aEl.textContent = String(Math.max(0, Math.ceil(rem60(alphaLast))));
    if (hasE && eEl) eEl.textContent = String(Math.max(0, Math.ceil(rem60(eodLast))));
  }
  tick();
  setInterval(tick, 500);
})();
</script>
"""
    components.html(html, height=110, scrolling=False)


def render_api_usage_panel() -> None:
    if not ALPHA_API_KEY and not EODHD_API_KEY:
        return
    d = compute_api_usage_display()
    st.subheader("📊 API Usage")

    def _pill(label: str, status: str) -> str:
        if status == "BLOCKED":
            color = "#c62828"
        elif status == "WARNING":
            color = "#f9a825"
        else:
            color = "#2e7d32"
        return f'<span style="display:inline-block;padding:2px 10px;border-radius:999px;background:{color};color:#fff;font-size:0.78rem;font-weight:700;">{label}: {status}</span>'

    st.markdown(
        f'<div style="margin-bottom:6px;">{_pill("Alpha Vantage", d["alpha_status"])} &nbsp; '
        f'{_pill("EODHD", d["eodhd_status"])}</div>',
        unsafe_allow_html=True,
    )

    c1, c2 = st.columns(2)
    with c1:
        if ALPHA_API_KEY:
            st.markdown("**Alpha Vantage**")
            st.caption(
                f"Minute: {d['alpha_minute_used']}/{d['alpha_minute_cap']} used · "
                f"{d['alpha_minute_remaining']} remaining"
            )
            st.caption(
                f"Day: {d['alpha_day_used']}/{d['alpha_day_cap']} used · {d['alpha_day_remaining']} remaining"
            )
        else:
            st.markdown("**Alpha Vantage**")
            st.caption("API key not configured.")
    with c2:
        if EODHD_API_KEY:
            st.markdown("**EODHD**")
            st.caption(
                f"Minute: {d['eodhd_minute_used']}/{d['eodhd_minute_cap']} used · "
                f"{d['eodhd_minute_remaining']} remaining (buffer limit)"
            )
        else:
            st.markdown("**EODHD**")
            st.caption("API key not configured.")

    _render_api_minute_bucket_live_row(d)

    if d["alpha_status"] in ("WARNING", "BLOCKED") or d["eodhd_status"] in ("WARNING", "BLOCKED"):
        wait_candidates: list[int] = []
        if d["alpha_status"] != "OK":
            wait_candidates.append(int(np.ceil(d["alpha_next_safe_sec"])))
        if d["eodhd_status"] != "OK":
            wait_candidates.append(int(np.ceil(d["eodhd_next_safe_sec"])))
        wait_s = max(1, max(wait_candidates) if wait_candidates else 1)
        st.warning(f"⚠️ Avoid refreshing for **{wait_s}** seconds (API window resets).")


def render_market_data_cache_status_bar(
    symbols: list[str],
    period: str,
    interval: str,
    use_sample_data: bool,
) -> None:
    """
    Market bar: timer / bar / force button use last LIVE API epoch from this session when set,
    otherwise from persisted disk meta (same symbols+period+interval) so browser refresh does
    not reset the clock unless TTL expired or a new fetch ran.
    """
    if use_sample_data:
        st.markdown("##### Market data")
        st.caption("Sample mode: no live market API timer.")
        return

    try:
        _init_market_session_cache()
        mc = st.session_state.get("market_cache") or {}
    except Exception:
        mc = {}
    ts_sess = float(mc.get("timestamp") or 0.0)

    try:
        meta = st.session_state.get(MARKET_FETCH_META_KEY) or {}
    except Exception:
        meta = {}
    source_sess = str(meta.get("source", "FAILED"))

    disk = _read_market_api_live_meta()
    disk_match = bool(disk and symbols and _disk_meta_matches_scan(disk, symbols, period, interval))
    disk_ts = float(disk.get("last_live_epoch") or 0.0) if disk_match else 0.0

    # Prefer latest LIVE time across session + disk so a browser refresh does not "restart" the
    # clock when this run did not fetch but disk still has a fresh last_live_epoch (symbols_key match).
    if disk_match and disk_ts > 0:
        if ts_sess > 0:
            last_api_refresh_epoch = max(ts_sess, disk_ts)
            used_persisted_timer = False
            source = source_sess
        else:
            last_api_refresh_epoch = disk_ts
            used_persisted_timer = True
            source = "CACHE"
    elif ts_sess > 0:
        last_api_refresh_epoch = ts_sess
        used_persisted_timer = False
        source = source_sess
    else:
        last_api_refresh_epoch = 0.0
        used_persisted_timer = False
        source = source_sess

    try:
        quota_low = bool(st.session_state.get(MARKET_QUOTA_FLAG_KEY))
    except Exception:
        quota_low = False

    server_now_epoch = time.time()
    ttl_sec = float(MARKET_SESSION_CACHE_TTL)
    force_enable_sec = int(MARKET_FORCE_REFRESH_ENABLE_REMAINING_SEC)
    warn_sec = int(MARKET_UI_WARNING_REMAINING_SEC)

    cfg = {
        "last_api_refresh_epoch": last_api_refresh_epoch,
        "server_now_epoch": server_now_epoch,
        "cache_ttl_seconds": ttl_sec,
        "force_enable_remaining_sec": force_enable_sec,
        "warning_remaining_sec": warn_sec,
        "source": source,
        "quota_low": quota_low,
        "used_persisted_timer": used_persisted_timer,
    }
    cfg_json = json.dumps(cfg, separators=(",", ":")).replace("<", "\\u003c")

    st.markdown("##### Market data")
    html_fragment = """
<style>
  .mc-timer-root { font-family: system-ui, sans-serif; font-size: 14px; margin: 4px 0 12px 0;
    padding: 12px 14px; border-radius: 10px;
    background: #262730; color: #eceff1; border: 1px solid #464b5e;
    box-sizing: border-box; }
  .mc-timer-root #statusText { color: #eceff1 !important; font-size: 0.95rem; line-height: 1.5;
    letter-spacing: 0.01em; }
  .mc-timer-track { background: #3d4454 !important; border-radius: 10px; height: 12px; overflow: hidden; }
</style>
<div class="mc-timer-root">
  <div id="sourceLine" style="font-weight:600;margin-bottom:8px;"></div>
  <div id="persistNote" style="display:none;font-size:0.82rem;color:#90caf9;margin-bottom:6px;"></div>
  <div id="quotaLine" style="display:none;margin-bottom:8px;color:#ffcc80;"></div>
  <div id="statusText" style="color:#eceff1;"></div>
  <div style="margin-top:10px;">
    <div class="mc-timer-track">
      <div id="progressBar" style="
        height:12px;
        width:100%;
        border-radius:10px;
        transition: width 1s linear, background-color 1s linear;
      "></div>
    </div>
  </div>
  <div id="warningText" style="margin-top:10px; font-weight:bold; min-height:1.2em; color:#ffab91;"></div>
  <button id="refreshBtn" disabled style="
    margin-top:10px;
    padding:6px 12px;
    border-radius:6px;
    border:none;
    background:#1976d2;
    color:white;
    cursor:not-allowed;
  ">
    🔄 Force Refresh
  </button>
</div>
<script type="application/json" id="mc-api-timer-cfg">""" + cfg_json + """</script>
<script>
(function () {
  let cfg;
  try {
    cfg = JSON.parse(document.getElementById("mc-api-timer-cfg").textContent || "{}");
  } catch (e) {
    cfg = {};
  }
  const lastApi = Number(cfg.last_api_refresh_epoch) || 0;
  const serverNowEmbed = Number(cfg.server_now_epoch) || 0;
  const TTL = Number(cfg.cache_ttl_seconds) || 90;
  const forceEnableRem = Number(cfg.force_enable_remaining_sec) || 30;
  const warnRem = Number(cfg.warning_remaining_sec) || 15;
  const source = String(cfg.source || "FAILED");
  const quotaLow = !!cfg.quota_low;
  const usedPersisted = !!cfg.used_persisted_timer;

  const clientAtEmbed = Date.now() / 1000;
  const serverSkew = serverNowEmbed > 0 ? (serverNowEmbed - clientAtEmbed) : 0;
  function apiNow() {
    return Date.now() / 1000 + serverSkew;
  }

  const sourceLine = document.getElementById("sourceLine");
  const persistNote = document.getElementById("persistNote");
  const quotaLine = document.getElementById("quotaLine");
  if (usedPersisted) {
    persistNote.style.display = "block";
    persistNote.innerText = "Timer from last server-side API fetch (no new request this page load).";
  }
  if (source === "LIVE") {
    sourceLine.style.color = "#a5d6a7";
    sourceLine.innerText = "✅ Live data fetched";
  } else if (source === "CACHE") {
    sourceLine.style.color = "#ffe082";
    sourceLine.innerText = "⚠️ Showing cached data (API not called)";
  } else if (source === "STALE_CACHE") {
    sourceLine.style.color = "#ffab91";
    sourceLine.innerText = "⚠️ API failed, showing last available data";
  } else {
    sourceLine.style.color = "#ff8a80";
    sourceLine.innerText = "🚫 No data available";
  }
  if (quotaLow) {
    quotaLine.style.display = "block";
    quotaLine.innerText = "⚠️ API quota low, using cached data";
  }

  function getColor(percent) {
    if (percent > 60) {
      return "#00c853";
    } else if (percent > 30) {
      return "#ffd600";
    } else {
      return "#d50000";
    }
  }

  const statusEl = document.getElementById("statusText");
  const bar = document.getElementById("progressBar");
  const warning = document.getElementById("warningText");
  const btn = document.getElementById("refreshBtn");
  let lastSecTick = -1;

  function updateUI() {
    const now = apiNow();
    let elapsed = 0;
    if (lastApi > 0) {
      elapsed = Math.max(0, now - lastApi);
    }
    let remaining = TTL - elapsed;
    if (remaining < 0) remaining = 0;

    const secTick = Math.floor(now);
    if (secTick !== lastSecTick) {
      lastSecTick = secTick;
      if (lastApi <= 0) {
        statusEl.innerText = "Last API refresh: — | Next API refresh in: 0 sec";
      } else {
        statusEl.innerText =
          "Last API refresh: " + Math.floor(elapsed) + " sec ago | Next API refresh in: " + Math.floor(remaining) + " sec";
      }
      statusEl.style.color = "#eceff1";
    }

    const percent = TTL > 0 ? (remaining / TTL) * 100 : 0;
    bar.style.width = percent + "%";
    bar.style.backgroundColor = getColor(percent);

    if (remaining <= warnRem && lastApi > 0) {
      warning.innerText = "⚠️ Refresh available soon!";
      warning.style.color = "red";
      warning.style.visibility = (Math.floor(now) % 2 === 0) ? "visible" : "hidden";
    } else {
      warning.innerText = "";
      warning.style.visibility = "visible";
    }

    if (remaining <= forceEnableRem) {
      btn.disabled = false;
      btn.style.cursor = "pointer";
      btn.style.background = "#2e7d32";
    } else {
      btn.disabled = true;
      btn.style.cursor = "not-allowed";
      btn.style.background = "#1976d2";
    }
  }

  function loop() {
    updateUI();
    requestAnimationFrame(loop);
  }
  requestAnimationFrame(loop);

  document.getElementById("refreshBtn").onclick = function () {
    if (document.getElementById("refreshBtn").disabled) return;
    const target = window.parent && window.parent !== window ? window.parent : window;
    target.location.reload();
  };
})();
</script>
"""
    components.html(html_fragment, height=290, scrolling=False)


def _nse_corporates_pit_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("data", "Data", "records", "result"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [row for row in rows if isinstance(row, dict)]
    return []


def _nse_numeric_field(row: dict[str, Any], *names: str) -> float:
    for name in names:
        if name in row and row[name] not in (None, "", "-"):
            try:
                return float(row[name])
            except (TypeError, ValueError):
                continue
    return 0.0


NSE_INSIDER_NET_THRESHOLD = 1.0  # min |net| to classify BUYING/SELLING vs NEUTRAL


def _nse_empty_insider_detail() -> dict[str, Any]:
    return {
        "activity": "NO_DATA",
        "net_qty": None,
        "last_date": "—",
        "interpretation": "No recent promoter disclosures",
    }


def _nse_is_promoter_or_promoter_group_category(category: str) -> bool:
    u = str(category or "").strip().upper()
    if not u:
        return False
    if u == "PROMOTER":
        return True
    if u == "PROMOTER GROUP" or "PROMOTER GROUP" in u:
        return True
    return False


def _nse_row_trade_date(row: dict[str, Any]) -> datetime | None:
    for key in (
        "date",
        "tradeDate",
        "acqfromDt",
        "acquisitionDateFrom",
        "pefromDate",
        "transactionDate",
        "disclosureDate",
        "acqFromDate",
    ):
        raw = row.get(key) or row.get(key.lower() if key != key.lower() else key)
        if raw in (None, "", "-"):
            continue
        try:
            dt = pd.to_datetime(raw, errors="coerce")
            if pd.isna(dt):
                continue
            return dt.to_pydatetime()
        except (TypeError, ValueError):
            continue
    return None


def _nse_underlying_symbol(row: dict[str, Any]) -> str | None:
    sym_raw = row.get("symbol") or row.get("sym") or row.get("tradingSymbol") or row.get("company")
    return clean_underlying_symbol(sym_raw)


@st.cache_data(ttl=86400, show_spinner=False)
def fetch_nse_promoter_insider_details_by_symbol(candidate_symbols: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    """
    Single NSE corporates-pit call per cache window (~1 day). Aggregates secAcq/secSale for
    Promoter + Promoter Group only, last 10 calendar days, for the requested underlying symbols.
    """
    want = {str(s).strip().upper() for s in candidate_symbols if str(s).strip()}
    out: dict[str, dict[str, Any]] = {s: _nse_empty_insider_detail() for s in want}
    if not want:
        return out

    try:
        cookie_jar = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
        opener.addheaders = [(key, value) for key, value in NSE_BROWSER_HEADERS.items()]
        opener.open(urllib.request.Request("https://www.nseindia.com/"), timeout=12).read()
        api_url = "https://www.nseindia.com/api/corporates-pit?index=equities"
        with opener.open(urllib.request.Request(api_url), timeout=25) as response:
            raw = response.read().decode("utf-8", errors="replace")
        payload = json.loads(raw)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
        logger.info("NSE corporates-pit fetch failed: %s", exc)
        return out
    except Exception as exc:
        logger.exception("Unexpected NSE corporates-pit error: %s", exc)
        return out

    records = _nse_corporates_pit_records(payload)
    if not records:
        return out

    cutoff = datetime.now() - timedelta(days=10)
    agg_net: dict[str, float] = {s: 0.0 for s in want}
    agg_last: dict[str, datetime] = {}
    touched: set[str] = set()

    for row in records:
        if not isinstance(row, dict):
            continue
        cat = str(row.get("personCategory") or row.get("personcategory") or "")
        if not _nse_is_promoter_or_promoter_group_category(cat):
            continue

        sym = _nse_underlying_symbol(row)
        if not sym or sym.upper() not in want:
            continue

        dt = _nse_row_trade_date(row)
        if dt is not None and dt < cutoff:
            continue

        sym_u = sym.upper()
        touched.add(sym_u)

        buy_qty = _nse_numeric_field(
            row,
            "secAcq",
            "secacq",
            "secAcqQty",
            "buyQuantity",
            "buyquantity",
            "buyQty",
            "buyqty",
        )
        sell_qty = _nse_numeric_field(
            row,
            "secSale",
            "secsale",
            "secSaleQty",
            "sellQuantity",
            "sellquantity",
            "sellQty",
            "sellqty",
        )
        agg_net[sym_u] = agg_net.get(sym_u, 0.0) + (buy_qty - sell_qty)
        if dt is not None:
            prev = agg_last.get(sym_u)
            if prev is None or dt > prev:
                agg_last[sym_u] = dt

    for sym_u in want:
        if sym_u not in touched:
            out[sym_u] = _nse_empty_insider_detail()
            continue

        net = float(agg_net.get(sym_u, 0.0))
        last_dt = agg_last.get(sym_u)
        last_s = last_dt.strftime("%Y-%m-%d") if last_dt is not None else "—"

        if net > NSE_INSIDER_NET_THRESHOLD:
            act = "BUYING"
            interp = "Promoters have increased stake recently (positive long-term signal)"
        elif net < -NSE_INSIDER_NET_THRESHOLD:
            act = "SELLING"
            interp = "Promoters have reduced stake (use caution, not necessarily bearish short-term)"
        else:
            act = "NEUTRAL"
            interp = "No significant promoter activity"

        out[sym_u] = {
            "activity": act,
            "net_qty": net,
            "last_date": last_s,
            "interpretation": interp,
        }

    return out


def gather_nse_insider_candidate_symbols(signals: pd.DataFrame, active_history: pd.DataFrame) -> set[str]:
    """STRONG + WEAK signals and ACTIVE DB trades only (no EARLY)."""
    out: set[str] = set()
    if not signals.empty:
        strong_weak = {"STRONG_LONG", "STRONG_SHORT", "WEAK_LONG", "WEAK_SHORT"}
        for _, row in signals.iterrows():
            if str(row.get("Signal", "")) in strong_weak:
                sym = clean_underlying_symbol(row.get("Stock")) or ""
                if sym:
                    out.add(sym.upper())
    if not active_history.empty and "stock" in active_history.columns:
        for sym in active_history["stock"].tolist():
            s = clean_underlying_symbol(sym) or ""
            if s:
                out.add(s.upper())
    return out


def refresh_nse_insider_context(signals: pd.DataFrame, active_history: pd.DataFrame, use_sample_data: bool) -> None:
    """Populate session insider map for details panel only; does not mutate signals or scores."""
    if use_sample_data:
        st.session_state["_nse_insider_by_symbol"] = {}
        return
    candidates = gather_nse_insider_candidate_symbols(signals, active_history)
    if not candidates:
        st.session_state["_nse_insider_by_symbol"] = {}
        return
    key = tuple(sorted(candidates))
    st.session_state["_nse_insider_by_symbol"] = fetch_nse_promoter_insider_details_by_symbol(key)


def insider_detail_for_stock(stock: object) -> dict[str, Any]:
    sym = clean_underlying_symbol(stock) or ""
    if not sym:
        return _nse_empty_insider_detail()
    bag = st.session_state.get("_nse_insider_by_symbol") or {}
    if isinstance(bag, dict) and sym.upper() in bag:
        return dict(bag[sym.upper()])
    return _nse_empty_insider_detail()


def strategy_type_label(signal_type: str) -> str:
    if not signal_type:
        return "—"
    upper = str(signal_type).upper()
    if upper.startswith("PULLBACK"):
        return "Pullback"
    if upper.startswith("BREAKOUT") or upper.startswith("STRUCTURE"):
        return "Breakout"
    if upper.startswith("TREND"):
        return "Trend"
    return "—"


def trade_quality_label(signal: str, failed_conditions: list[str]) -> str:
    fail_n = len(failed_conditions)
    if str(signal).startswith("STRONG") and fail_n == 0:
        return "High Quality"
    if str(signal).startswith("WEAK"):
        return "Moderate"
    if fail_n >= 2:
        return "Low Confidence"
    return "Moderate"


def build_explainability(
    signal: str,
    signal_type: str,
    close: float,
    ema20: float,
    ema50: float,
    rsi: float,
    volume: float,
    avg_volume: float,
    distance_from_ema20: float,
    trend_strength: float,
    config: StrategyConfig,
    market_type: str,
) -> dict[str, Any]:
    bullish = ema20 > ema50
    bearish = ema20 < ema50
    if bullish:
        trend_direction = "LONG"
    elif bearish:
        trend_direction = "SHORT"
    else:
        trend_direction = "NEUTRAL"

    direction = signal_direction(signal)
    if direction == "LONG":
        ema_condition = bool(bullish)
    elif direction == "SHORT":
        ema_condition = bool(bearish)
    else:
        ema_condition = bool(bullish or bearish)
    rsi_ok_long = rsi >= config.long_rsi
    rsi_ok_short = rsi <= config.short_rsi
    if direction == "LONG":
        rsi_condition = "RSI ok" if rsi_ok_long else "RSI weak"
    elif direction == "SHORT":
        rsi_condition = "RSI ok" if rsi_ok_short else "RSI weak"
    else:
        rsi_condition = "Neutral"

    volume_ok = volume > avg_volume if avg_volume and avg_volume > 0 else False
    volume_condition = "Above avg" if volume_ok else "Below avg"

    distance_ok = distance_from_ema20 < config.overextended_max
    distance_condition = "Near EMA20" if distance_ok else "Too far from EMA"

    trend_ok = trend_strength > config.trend_strength_min
    trend_strength_val = float(trend_strength)

    failed: list[str] = []
    if direction == "LONG" and not bullish:
        failed.append("EMA trend misaligned")
    if direction == "SHORT" and not bearish:
        failed.append("EMA trend misaligned")
    if direction == "LONG" and not rsi_ok_long:
        failed.append("RSI weak")
    if direction == "SHORT" and not rsi_ok_short:
        failed.append("RSI weak")
    if not volume_ok:
        failed.append("Low volume")
    if not distance_ok:
        failed.append("Too far from EMA")
    if not trend_ok:
        failed.append("Weak trend")

    if signal == "WAIT":
        if market_type not in {"TRENDING_BULLISH", "TRENDING_BEARISH"}:
            failed.append("Market regime not trending")
        elif market_type == "TRENDING_BULLISH" and not bullish:
            failed.append("Stock not bullish vs regime")
        elif market_type == "TRENDING_BEARISH" and not bearish:
            failed.append("Stock not bearish vs regime")
        else:
            failed.append("Setup not ready")

    failed = list(dict.fromkeys(failed))

    return {
        "trend_direction": trend_direction,
        "ema_condition": ema_condition,
        "rsi_value": float(rsi),
        "rsi_condition": rsi_condition,
        "volume_value": float(volume),
        "volume_condition": volume_condition,
        "distance_from_ema": float(distance_from_ema20),
        "distance_condition": distance_condition,
        "trend_strength": trend_strength_val,
        "strategy_type": strategy_type_label(signal_type),
        "failed_conditions": failed,
        "trade_quality": trade_quality_label(signal, failed),
    }


def make_sample_history(symbol: str, periods: int = 180) -> pd.DataFrame:
    seed = sum(ord(char) for char in symbol)
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range(end=pd.Timestamp.today().normalize(), periods=periods)
    drift = 0.0008 if seed % 2 == 0 else -0.0004
    returns = rng.normal(loc=drift, scale=0.018, size=periods)
    close = 500 + (seed % 1_500) + np.cumsum(returns * 100)
    close = np.maximum(close, 50)

    return pd.DataFrame(
        {
            "Open": close * (1 + rng.normal(0, 0.006, size=periods)),
            "High": close * (1 + rng.uniform(0.003, 0.018, size=periods)),
            "Low": close * (1 - rng.uniform(0.003, 0.018, size=periods)),
            "Close": close,
            "Volume": rng.integers(700_000, 8_000_000, size=periods),
        },
        index=dates,
    )


def extract_symbol_history(market_data: pd.DataFrame, symbol: str, total_symbols: int) -> pd.DataFrame:
    ticker_key = to_market_ticker_key(symbol)

    if total_symbols == 1 and not isinstance(market_data.columns, pd.MultiIndex):
        data = market_data.copy()
    elif isinstance(market_data.columns, pd.MultiIndex) and ticker_key in market_data.columns.get_level_values(0):
        data = market_data[ticker_key].copy()
    elif isinstance(market_data.columns, pd.MultiIndex) and symbol in market_data.columns.get_level_values(0):
        data = market_data[symbol].copy()
    else:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(-1)

    data = data.rename(columns=str.title)
    missing_columns = [column for column in OHLCV_COLUMNS if column not in data.columns]
    if missing_columns:
        logger.info("Skipping %s: missing columns in source payload (%s)", symbol, ", ".join(missing_columns))
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    data = data[OHLCV_COLUMNS].copy()
    for column in OHLCV_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    data = data.dropna(subset=["Open", "High", "Low", "Close"])
    data["Volume"] = data["Volume"].fillna(0)
    return data


def calculate_rsi(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calculate_atr(data: pd.DataFrame, period: int) -> pd.Series:
    previous_close = data["Close"].shift(1)
    true_range = pd.concat(
        [
            data["High"] - data["Low"],
            (data["High"] - previous_close).abs(),
            (data["Low"] - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return true_range.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def calculate_adx(data: pd.DataFrame, period: int) -> pd.Series:
    high = data["High"]
    low = data["Low"]
    previous_high = high.shift(1)
    previous_low = low.shift(1)

    up_move = high - previous_high
    down_move = previous_low - low
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)

    atr = calculate_atr(data, period)
    plus_di = 100 * pd.Series(plus_dm, index=data.index).ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr
    minus_di = 100 * pd.Series(minus_dm, index=data.index).ewm(alpha=1 / period, min_periods=period, adjust=False).mean() / atr
    dx = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    return dx.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()


def add_indicators(data: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    enriched = data.copy()
    enriched["EMA20"] = enriched["Close"].ewm(span=config.ema_fast, adjust=False).mean()
    enriched["EMA50"] = enriched["Close"].ewm(span=config.ema_slow, adjust=False).mean()
    enriched["RSI"] = calculate_rsi(enriched["Close"], config.rsi_period)
    enriched["ATR"] = calculate_atr(enriched, config.atr_period)
    enriched["Avg Volume"] = enriched["Volume"].rolling(config.volume_window).mean()
    enriched["ATR Avg"] = enriched["ATR"].rolling(config.volume_window).mean()
    enriched["ATR Median"] = enriched["ATR"].rolling(config.volume_window).median()
    return enriched


def add_regime_indicators(data: pd.DataFrame, config: StrategyConfig) -> pd.DataFrame:
    enriched = add_indicators(data, config)
    enriched["ADX"] = calculate_adx(enriched, config.adx_period)
    return enriched


def default_market_regime(error: str = "") -> dict[str, str]:
    return {
        "market_type": "UNKNOWN",
        "direction": "NEUTRAL",
        "suggested_strategy": "NO TRADE",
        "trend_confirmation": "WEAK",
        "nifty_trend": "UNKNOWN",
        "banknifty_trend": "UNKNOWN",
        "adx": "",
        "rsi": "",
        "nifty_return_pct": "-",
        "market_drawdown_pct": "-",
        "error": error,
    }


def classify_index_trend(data: pd.DataFrame, config: StrategyConfig) -> tuple[str, float, float]:
    enriched = add_regime_indicators(data, config).dropna()
    if enriched.empty:
        raise RuntimeError("Insufficient index indicator data")

    latest = enriched.iloc[-1]
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    rsi = float(latest["RSI"])
    adx_raw = pd.to_numeric(latest.get("ADX"), errors="coerce")
    adx = float(adx_raw) if pd.notna(adx_raw) else np.nan

    adx_confirmed = adx > 20 if np.isfinite(adx) else True
    bullish = ema20 > ema50 and adx_confirmed and rsi > 55
    bearish = ema20 < ema50 and adx_confirmed and rsi < 45
    sideways = adx < 20 if np.isfinite(adx) else 45 <= rsi <= 55

    if bullish:
        trend = "BULLISH"
    elif bearish:
        trend = "BEARISH"
    elif sideways:
        trend = "SIDEWAYS"
    else:
        trend = "NEUTRAL"

    return trend, adx, rsi


def _fetch_regime_history(symbol: str) -> pd.DataFrame:
    ticker_key = to_market_ticker_key(symbol)
    payload = get_market_data((symbol,), interval="15m", period="7d")
    return _extract_merged_ticker_frame(payload, ticker_key, total=1)


def _trend_score(trend: str) -> int:
    if trend == "BULLISH":
        return 1
    if trend == "BEARISH":
        return -1
    return 0


def _score_to_trend(score: float) -> str:
    if score > 0.2:
        return "BULLISH"
    if score < -0.2:
        return "BEARISH"
    return "SIDEWAYS"


def get_market_regime() -> dict[str, str]:
    try:
        config = StrategyConfig()
        source = "UNKNOWN"
        nifty_trend = "UNKNOWN"
        adx = np.nan
        rsi = np.nan
        banknifty_trend = "UNKNOWN"
        nifty_return_pct = np.nan
        market_drawdown_pct = np.nan

        niftybees_data = _fetch_regime_history("NIFTYBEES.NS")
        if not niftybees_data.empty:
            nifty_trend, adx, rsi = classify_index_trend(niftybees_data, config)
            nifty_return_pct = _window_return_pct(niftybees_data["Close"], lookback=20)
            market_drawdown_pct = _window_drawdown_pct(niftybees_data["Close"], lookback=20)
            source = "NIFTYBEES"
            print("Market regime source: NIFTYBEES")
        else:
            basket = ["RELIANCE.NS", "HDFCBANK.NS", "ICICIBANK.NS"]
            basket_trends: list[str] = []
            adx_values: list[float] = []
            rsi_values: list[float] = []
            for symbol in basket:
                try:
                    symbol_data = _fetch_regime_history(symbol)
                    if symbol_data.empty:
                        continue
                    symbol_trend, symbol_adx, symbol_rsi = classify_index_trend(symbol_data, config)
                    basket_trends.append(symbol_trend)
                    if np.isfinite(symbol_adx):
                        adx_values.append(float(symbol_adx))
                    if np.isfinite(symbol_rsi):
                        rsi_values.append(float(symbol_rsi))
                except Exception as exc:
                    logger.info("Basket regime fetch failed for %s: %s", symbol, exc)
                    continue

            if basket_trends:
                avg_score = float(np.mean([_trend_score(t) for t in basket_trends]))
                nifty_trend = _score_to_trend(avg_score)
                adx = float(np.mean(adx_values)) if adx_values else np.nan
                rsi = float(np.mean(rsi_values)) if rsi_values else np.nan
                banknifty_trend = "BASKET"
                source = "Basket"
                print("Market regime source: Basket")
            else:
                source = "Unavailable"
                print("Market regime source: Unavailable")
                logger.info("No market regime data from NIFTYBEES or basket; returning UNKNOWN regime")
                return default_market_regime("")

        if nifty_trend == "BULLISH":
            market_type = "TRENDING_BULLISH"
            direction = "UP"
            suggested_strategy = "PULLBACK_LONG"
        elif nifty_trend == "BEARISH":
            market_type = "TRENDING_BEARISH"
            direction = "DOWN"
            suggested_strategy = "PULLBACK_SHORT"
        elif nifty_trend == "SIDEWAYS":
            market_type = "SIDEWAYS"
            direction = "NEUTRAL"
            suggested_strategy = "PULLBACK"
        else:
            market_type = "NEUTRAL"
            direction = "NEUTRAL"
            suggested_strategy = "NO TRADE"

        if nifty_trend in {"BULLISH", "BEARISH"} and nifty_trend == banknifty_trend:
            trend_confirmation = "STRONG"
        else:
            trend_confirmation = "WEAK"

        return {
            "market_type": market_type,
            "direction": direction,
            "suggested_strategy": suggested_strategy,
            "trend_confirmation": trend_confirmation,
            "nifty_trend": nifty_trend,
            "banknifty_trend": banknifty_trend,
            "adx": f"{adx:.1f}" if np.isfinite(adx) else "-",
            "rsi": f"{rsi:.1f}" if np.isfinite(rsi) else "-",
            "nifty_return_pct": f"{nifty_return_pct:.2f}" if np.isfinite(nifty_return_pct) else "-",
            "market_drawdown_pct": f"{market_drawdown_pct:.2f}" if np.isfinite(market_drawdown_pct) else "-",
            "error": "",
        }
    except Exception as exc:
        logger.exception("Failed to detect market regime")
        return default_market_regime(str(exc))


def failed_conditions(conditions: dict[str, bool]) -> list[str]:
    return [label for label, passed in conditions.items() if not passed]


def rejection_reason(long_conditions: dict[str, bool], short_conditions: dict[str, bool]) -> str:
    long_failures = ", ".join(failed_conditions(long_conditions)) or "none"
    short_failures = ", ".join(failed_conditions(short_conditions)) or "none"
    return f"Rejected long: {long_failures}. Rejected short: {short_failures}"


def is_long_signal(signal: str) -> bool:
    return signal in {"STRONG_LONG", "WEAK_LONG", "EARLY_LONG"}


def is_short_signal(signal: str) -> bool:
    return signal in {"STRONG_SHORT", "WEAK_SHORT", "EARLY_SHORT"}


def is_trade_signal(signal: str) -> bool:
    return is_long_signal(signal) or is_short_signal(signal)


def signal_direction(signal: str) -> str:
    if is_long_signal(signal):
        return "LONG"
    if is_short_signal(signal):
        return "SHORT"
    return "WAIT"


def signal_strength(signal: str) -> str:
    if signal.startswith("STRONG"):
        return "STRONG"
    if signal.startswith("EARLY"):
        return "EARLY"
    if signal.startswith("WEAK"):
        return "WEAK"
    return "WAIT"


def pnl_pct(direction: str, entry_price: float, current_price: float) -> float:
    if not entry_price or np.isnan(entry_price) or np.isnan(current_price):
        return np.nan
    if direction == "LONG":
        return (current_price - entry_price) / entry_price * 100
    if direction == "SHORT":
        return (entry_price - current_price) / entry_price * 100
    return np.nan


def _window_return_pct(series: pd.Series, lookback: int = 20) -> float:
    if series is None or len(series) < 2:
        return np.nan
    end_val = pd.to_numeric(series.iloc[-1], errors="coerce")
    start_idx = max(0, len(series) - 1 - max(1, int(lookback)))
    start_val = pd.to_numeric(series.iloc[start_idx], errors="coerce")
    if pd.isna(start_val) or pd.isna(end_val) or float(start_val) == 0.0:
        return np.nan
    return (float(end_val) - float(start_val)) / float(start_val) * 100.0


def _window_drawdown_pct(series: pd.Series, lookback: int = 20) -> float:
    if series is None or len(series) < 2:
        return np.nan
    window = pd.to_numeric(series.tail(max(2, int(lookback))), errors="coerce").dropna()
    if window.empty:
        return np.nan
    peak = float(window.max())
    last = float(window.iloc[-1])
    if peak <= 0:
        return np.nan
    return (peak - last) / peak * 100.0


@st.cache_data(ttl=6 * 60 * 60, show_spinner=False)
def _days_to_next_event(symbol: str) -> float:
    try:
        ticker = yf.Ticker(symbol)
        calendar = getattr(ticker, "calendar", None)
        if calendar is None or len(calendar) == 0:
            return np.nan
        if isinstance(calendar, pd.DataFrame):
            values: list[Any] = []
            if "Earnings Date" in calendar.index:
                values = list(np.ravel(calendar.loc["Earnings Date"].values))
            elif "Earnings Date" in calendar.columns:
                values = list(np.ravel(calendar["Earnings Date"].values))
            else:
                values = list(np.ravel(calendar.values))
            for value in values:
                ts = pd.to_datetime(value, errors="coerce")
                if pd.notna(ts):
                    delta = (ts.date() - datetime.now().date()).days
                    return float(delta)
        if isinstance(calendar, dict):
            for key in ("Earnings Date", "earningsDate", "earnings_date"):
                if key in calendar:
                    raw = calendar.get(key)
                    if isinstance(raw, (list, tuple)) and raw:
                        raw = raw[0]
                    ts = pd.to_datetime(raw, errors="coerce")
                    if pd.notna(ts):
                        delta = (ts.date() - datetime.now().date()).days
                        return float(delta)
    except Exception:
        return np.nan
    return np.nan


def monthly_expiry_date(today: datetime | None = None) -> datetime.date:
    reference = today or datetime.now()
    first_next_month = (reference.replace(day=28) + timedelta(days=4)).replace(day=1)
    last_day = first_next_month - timedelta(days=1)
    expiry = last_day
    while expiry.weekday() != 3:
        expiry -= timedelta(days=1)
    return expiry.date()


def days_to_expiry(today: datetime | None = None) -> int:
    reference = today or datetime.now()
    return (monthly_expiry_date(reference) - reference.date()).days


def is_expiry_window(today: datetime | None = None) -> bool:
    return 0 <= days_to_expiry(today) <= 3


def get_db_connection(path: str = str(SIGNALS_DB_PATH)) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    return connection


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            stock TEXT,
            signal_type TEXT,
            entry_price REAL,
            stop_loss REAL,
            target REAL,
            confidence_score INTEGER,
            status TEXT,
            exit_reason TEXT,
            exit_price REAL,
            pnl_percent REAL
        )
        """
    )
    existing_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(signals)").fetchall()
    }
    signal_migrations = {
        "pnl_percent": "REAL",
        "original_entry_price": "REAL",
        "original_stop_loss": "REAL",
        "original_target": "REAL",
        "original_signal_type": "TEXT",
        "original_selected_timestamp": "TEXT",
        "final_exit_price": "REAL",
        "final_exit_reason": "TEXT",
        "final_pnl_percent": "REAL",
    }
    for column, column_type in signal_migrations.items():
        if column not in existing_columns:
            connection.execute(f"ALTER TABLE signals ADD COLUMN {column} {column_type}")
    connection.execute(
        """
        UPDATE signals
        SET original_entry_price = COALESCE(original_entry_price, entry_price),
            original_stop_loss = COALESCE(original_stop_loss, stop_loss),
            original_target = COALESCE(original_target, target),
            original_signal_type = COALESCE(original_signal_type, signal_type),
            original_selected_timestamp = COALESCE(original_selected_timestamp, timestamp),
            final_exit_price = COALESCE(final_exit_price, exit_price),
            final_exit_reason = COALESCE(final_exit_reason, exit_reason),
            final_pnl_percent = COALESCE(final_pnl_percent, pnl_percent)
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_stock ON signals(stock)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_signals_active_stock ON signals(stock, status)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            updated_at TEXT,
            total_trades INTEGER,
            wins INTEGER,
            losses INTEGER,
            win_rate REAL,
            avg_profit REAL,
            avg_loss REAL,
            expectancy REAL,
            profit_factor REAL,
            total_pnl REAL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS selected_trades (
            stock TEXT PRIMARY KEY,
            signal_type TEXT,
            entry_price REAL,
            stop_loss REAL,
            target REAL,
            selected_timestamp TEXT,
            status TEXT
        )
        """
    )
    selected_columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(selected_trades)").fetchall()
    }
    selected_migrations = {
        "original_entry_price": "REAL",
        "original_stop_loss": "REAL",
        "original_target": "REAL",
        "original_signal_type": "TEXT",
        "original_selected_timestamp": "TEXT",
        "final_exit_price": "REAL",
        "final_exit_reason": "TEXT",
        "final_pnl_percent": "REAL",
        "ai_auto_insight_done": "INTEGER DEFAULT 0",
    }
    for column, column_type in selected_migrations.items():
        if column not in selected_columns:
            connection.execute(f"ALTER TABLE selected_trades ADD COLUMN {column} {column_type}")
    connection.execute(
        """
        UPDATE selected_trades
        SET original_entry_price = COALESCE(original_entry_price, entry_price),
            original_stop_loss = COALESCE(original_stop_loss, stop_loss),
            original_target = COALESCE(original_target, target),
            original_signal_type = COALESCE(original_signal_type, signal_type),
            original_selected_timestamp = COALESCE(original_selected_timestamp, selected_timestamp),
            ai_auto_insight_done = COALESCE(ai_auto_insight_done, 0)
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_selected_status ON selected_trades(status)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS selected_fno_stocks (
            stock TEXT PRIMARY KEY,
            signal_type TEXT,
            selected_timestamp TEXT
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_selected_fno_ts ON selected_fno_stocks(selected_timestamp)"
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ai_trade_insights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            signal_type TEXT,
            payload_hash TEXT NOT NULL,
            generated_timestamp TEXT NOT NULL,
            insight_text TEXT NOT NULL,
            provider_name TEXT NOT NULL
        )
        """
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_insight_symbol_ts ON ai_trade_insights(symbol, generated_timestamp DESC)"
    )
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_ai_insight_payload ON ai_trade_insights(payload_hash, provider_name)"
    )
    connection.commit()


def rows_to_dataframe(rows: list[sqlite3.Row]) -> pd.DataFrame:
    if not rows:
        return empty_history_table()
    return pd.DataFrame([dict(row) for row in rows]).reindex(columns=HISTORY_COLUMNS)


def insert_signal(connection: sqlite3.Connection, data: dict[str, Any]) -> None:
    stock = str(data["stock"]).upper()
    active_exists = connection.execute(
        "SELECT 1 FROM signals WHERE stock = ? AND status = 'ACTIVE' LIMIT 1",
        (stock,),
    ).fetchone()
    if active_exists:
        return

    try:
        connection.execute(
            """
            INSERT INTO signals (
                timestamp, stock, signal_type, entry_price, stop_loss, target,
                confidence_score, status, exit_reason, exit_price,
                original_entry_price, original_stop_loss, original_target,
                original_signal_type, original_selected_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', '', NULL, ?, ?, ?, ?, ?)
            """,
            (
                data["timestamp"],
                stock,
                data["signal_type"],
                data["entry_price"],
                data["stop_loss"],
                data["target"],
                data["confidence_score"],
                data["entry_price"],
                data["stop_loss"],
                data["target"],
                data["signal_type"],
                data["timestamp"],
            ),
        )
    except sqlite3.OperationalError as exc:
        logger.exception("Failed to insert signal for %s: %s", stock, exc)


def get_active_trades(connection: sqlite3.Connection) -> pd.DataFrame:
    rows = connection.execute(
        """
        SELECT *
        FROM signals
        WHERE status = 'ACTIVE'
        ORDER BY timestamp DESC, id DESC
        """
    ).fetchall()
    return rows_to_dataframe(rows)


def close_trade(connection: sqlite3.Connection, stock: str, exit_price: float, reason: str) -> None:
    try:
        active_trade = connection.execute(
            """
            SELECT id, signal_type, entry_price, original_signal_type, original_entry_price
            FROM signals
            WHERE stock = ? AND status = 'ACTIVE'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(stock).upper(),),
        ).fetchone()
        if active_trade is None:
            return

        direction = signal_direction(str(active_trade["original_signal_type"] or active_trade["signal_type"]))
        entry_price = _first_numeric(active_trade["original_entry_price"], active_trade["entry_price"])
        pnl_percent = pnl_pct(direction, entry_price, float(exit_price))

        connection.execute(
            """
            UPDATE signals
            SET status = 'CLOSED',
                exit_price = ?,
                pnl_percent = ?,
                exit_reason = ?,
                final_exit_price = ?,
                final_pnl_percent = ?,
                final_exit_reason = ?,
                timestamp = ?
            WHERE id = ?
            """,
            (
                exit_price,
                pnl_percent,
                reason,
                exit_price,
                pnl_percent,
                reason,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                int(active_trade["id"]),
            ),
        )
        connection.execute(
            """
            UPDATE selected_trades
            SET status = 'CLOSED',
                final_exit_price = ?,
                final_pnl_percent = ?,
                final_exit_reason = ?
            WHERE stock = ? AND status = 'ACTIVE'
            """,
            (exit_price, pnl_percent, reason, str(stock).upper()),
        )
    except sqlite3.OperationalError as exc:
        logger.exception("Failed to close trade for %s: %s", stock, exc)


def get_active_selected_trades(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM selected_trades
        WHERE status = 'ACTIVE'
        ORDER BY selected_timestamp DESC
        """
    ).fetchall()


def insert_selected_trade_from_row(connection: sqlite3.Connection, row: pd.Series) -> bool:
    signal = str(row.get("Signal", ""))
    if not is_trade_signal(signal):
        return False
    if str(row.get("trend_stage", "")).upper() == "EMERGING":
        return False
    stock = str(row["Stock"]).upper()
    entry = float(row.get("Entry", np.nan))
    sl = float(row.get("Stop Loss", np.nan))
    tgt = float(row.get("Target", np.nan))
    if any(np.isnan(v) for v in (entry, sl, tgt)):
        return False
    sig_type = str(row.get("Signal Type", "") or "")
    original_signal_type = signal or sig_type
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = connection.execute(
        """
        INSERT OR IGNORE INTO selected_trades (
            stock, signal_type, entry_price, stop_loss, target, selected_timestamp, status,
            original_entry_price, original_stop_loss, original_target,
            original_signal_type, original_selected_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?)
        """,
        (stock, sig_type, entry, sl, tgt, ts, entry, sl, tgt, original_signal_type, ts),
    )
    return (cur.rowcount or 0) > 0


def delete_selected_trade(connection: sqlite3.Connection, stock: str) -> None:
    connection.execute("DELETE FROM selected_trades WHERE stock = ?", (str(stock).upper(),))


def mark_selected_trade_closed_manual(
    connection: sqlite3.Connection,
    stock: str,
    exit_price: float | None = None,
    reason: str = "Manual Close",
) -> None:
    trade = connection.execute(
        """
        SELECT original_signal_type, signal_type, original_entry_price, entry_price
        FROM selected_trades
        WHERE stock = ? AND status = 'ACTIVE'
        LIMIT 1
        """,
        (str(stock).upper(),),
    ).fetchone()
    final_exit = pd.to_numeric(exit_price, errors="coerce") if exit_price is not None else np.nan
    final_pnl = np.nan
    if trade is not None and pd.notna(final_exit):
        entry = _first_numeric(trade["original_entry_price"], trade["entry_price"])
        direction = signal_direction(str(trade["original_signal_type"] or trade["signal_type"]))
        if pd.notna(entry):
            final_pnl = pnl_pct(direction, entry, float(final_exit))
    connection.execute(
        """
        UPDATE selected_trades
        SET status = 'CLOSED',
            final_exit_price = ?,
            final_exit_reason = ?,
            final_pnl_percent = ?
        WHERE stock = ?
        """,
        (
            float(final_exit) if pd.notna(final_exit) else None,
            reason,
            float(final_pnl) if pd.notna(final_pnl) else None,
            str(stock).upper(),
        ),
    )


def get_selected_fno_stocks(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT stock, signal_type, selected_timestamp
        FROM selected_fno_stocks
        ORDER BY selected_timestamp DESC
        """
    ).fetchall()


def insert_selected_fno_from_row(connection: sqlite3.Connection, row: pd.Series, signal_type: str) -> bool:
    stock = str(row.get("Stock", "")).upper()
    if not stock:
        return False
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = connection.execute(
        """
        INSERT OR REPLACE INTO selected_fno_stocks (stock, signal_type, selected_timestamp)
        VALUES (?, ?, ?)
        """,
        (stock, str(row.get("Signal", signal_type or "UNKNOWN")), ts),
    )
    return (cur.rowcount or 0) > 0


def delete_selected_fno_stock(connection: sqlite3.Connection, stock: str) -> None:
    connection.execute("DELETE FROM selected_fno_stocks WHERE stock = ?", (str(stock).upper(),))


def signals_row_for_stock(signals: pd.DataFrame, stock: str) -> pd.Series | None:
    if signals.empty or "Stock" not in signals.columns:
        return None
    mask = signals["Stock"].astype(str).str.upper() == str(stock).upper()
    if not mask.any():
        return None
    return signals.loc[mask].iloc[0]


def direction_from_stored_signal_type(signal_type: str) -> str:
    t = str(signal_type).upper()
    if "SHORT" in t:
        return "SHORT"
    if "LONG" in t:
        return "LONG"
    return "WAIT"


def monitoring_exit_triggered(direction: str, price: float, ema50: float, rsi: float) -> bool:
    if direction == "LONG":
        return bool(price < ema50 or rsi < 45)
    if direction == "SHORT":
        return bool(price > ema50 or rsi > 55)
    return False


def sl_target_distance_pct_of_entry(
    direction: str, entry: float, current: float, sl: float, tgt: float
) -> tuple[float, float]:
    if not entry or np.isnan(entry):
        return np.nan, np.nan
    if direction == "LONG":
        return (
            (current - sl) / entry * 100.0,
            (tgt - current) / entry * 100.0,
        )
    if direction == "SHORT":
        return (
            (sl - current) / entry * 100.0,
            (current - tgt) / entry * 100.0,
        )
    return np.nan, np.nan


def _pct_change_text(original: float, live: float, label: str) -> str:
    if original == 0 or np.isnan(original) or np.isnan(live):
        return f"{label} change unavailable"
    pct = (live - original) / abs(original) * 100.0
    moved = "up" if pct > 0 else "down" if pct < 0 else "unchanged"
    return f"{label} {moved} by {abs(pct):.2f}%"


def _first_numeric(*values: Any, default: float = np.nan) -> float:
    for value in values:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric):
            return float(numeric)
    return float(default)


def _selected_trade_live_plan(sel: sqlite3.Row, live: pd.Series | None) -> dict[str, Any]:
    original_signal = str(sel["original_signal_type"] or sel["signal_type"] or "")
    original_direction = signal_direction(original_signal)
    if original_direction == "WAIT":
        original_direction = direction_from_stored_signal_type(original_signal)
    if live is None:
        return {
            "status": "SIGNAL_NOT_FOUND",
            "entry": np.nan,
            "stop_loss": np.nan,
            "target": np.nan,
            "signal": "",
            "changes": ["No latest signal available"],
        }

    live_signal = str(live.get("Signal", ""))
    live_direction = signal_direction(live_signal)
    if live_signal == "WAIT" or live_direction == "WAIT" or live_direction != original_direction:
        status = "INVALIDATED"
    else:
        live_strength = signal_strength(live_signal)
        original_strength = signal_strength(original_signal)
        if live_strength == "WEAK" and original_strength in {"STRONG", "EARLY"}:
            status = "WEAKENED"
        else:
            original_entry = _first_numeric(sel["original_entry_price"], sel["entry_price"])
            original_sl = _first_numeric(sel["original_stop_loss"], sel["stop_loss"])
            original_target = _first_numeric(sel["original_target"], sel["target"])
            live_entry = float(live.get("Entry", np.nan))
            live_sl = float(live.get("Stop Loss", np.nan))
            live_target = float(live.get("Target", np.nan))
            changed = any(
                pd.notna(v)
                and abs(float(o) - float(v)) > max(abs(float(o)) * 0.001, 0.01)
                for o, v in (
                    (original_entry, live_entry),
                    (original_sl, live_sl),
                    (original_target, live_target),
                )
            )
            status = "UPDATED" if changed else "STILL_VALID"

    original_entry = _first_numeric(sel["original_entry_price"], sel["entry_price"])
    original_sl = _first_numeric(sel["original_stop_loss"], sel["stop_loss"])
    original_target = _first_numeric(sel["original_target"], sel["target"])
    live_entry = float(live.get("Entry", np.nan))
    live_sl = float(live.get("Stop Loss", np.nan))
    live_target = float(live.get("Target", np.nan))
    if original_direction == "LONG":
        sl_descriptor = "SL tightened" if live_sl > original_sl else "SL loosened" if live_sl < original_sl else "SL unchanged"
    elif original_direction == "SHORT":
        sl_descriptor = "SL tightened" if live_sl < original_sl else "SL loosened" if live_sl > original_sl else "SL unchanged"
    else:
        sl_descriptor = "SL change unavailable"
    return {
        "status": status,
        "entry": live_entry,
        "stop_loss": live_sl,
        "target": live_target,
        "signal": live_signal,
        "changes": [
            _pct_change_text(original_entry, live_entry, "Entry"),
            f"{sl_descriptor} ({_pct_change_text(original_sl, live_sl, 'SL')})",
            _pct_change_text(original_target, live_target, "Target"),
        ],
    }


def get_trade_history(connection: sqlite3.Connection, limit: int = 20) -> pd.DataFrame:
    rows = connection.execute(
        """
        SELECT *
        FROM signals
        WHERE status = 'CLOSED'
        ORDER BY timestamp DESC, id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return rows_to_dataframe(rows)


def default_performance_metrics() -> dict[str, float | int | str]:
    return {
        "updated_at": "",
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "win_rate": 0.0,
        "avg_profit": 0.0,
        "avg_loss": 0.0,
        "expectancy": 0.0,
        "profit_factor": 0.0,
        "total_pnl": 0.0,
    }


def calculate_performance_metrics(connection: sqlite3.Connection) -> dict[str, float | int | str]:
    row = connection.execute(
        """
        SELECT
            COUNT(*) AS total_trades,
            COALESCE(SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END), 0) AS wins,
            COALESCE(SUM(CASE WHEN pnl_percent < 0 THEN 1 ELSE 0 END), 0) AS losses,
            AVG(CASE WHEN pnl_percent > 0 THEN pnl_percent END) AS avg_profit,
            AVG(CASE WHEN pnl_percent < 0 THEN pnl_percent END) AS avg_loss,
            COALESCE(SUM(CASE WHEN pnl_percent > 0 THEN pnl_percent ELSE 0 END), 0) AS gross_profit,
            COALESCE(SUM(CASE WHEN pnl_percent < 0 THEN pnl_percent ELSE 0 END), 0) AS gross_loss,
            COALESCE(SUM(COALESCE(pnl_percent, 0)), 0) AS total_pnl
        FROM signals
        WHERE status = 'CLOSED'
          AND pnl_percent IS NOT NULL
        """
    ).fetchone()

    if row is None:
        return default_performance_metrics()

    total_trades = int(row["total_trades"] or 0)
    wins = int(row["wins"] or 0)
    losses = int(row["losses"] or 0)
    avg_profit = float(row["avg_profit"] or 0.0)
    avg_loss = float(row["avg_loss"] or 0.0)
    gross_profit = float(row["gross_profit"] or 0.0)
    gross_loss = float(row["gross_loss"] or 0.0)
    total_pnl = float(row["total_pnl"] or 0.0)
    win_rate = (wins / total_trades * 100) if total_trades else 0.0
    expectancy = (total_pnl / total_trades) if total_trades else 0.0

    if gross_loss < 0:
        profit_factor = gross_profit / abs(gross_loss)
    elif gross_profit > 0:
        profit_factor = np.inf
    else:
        profit_factor = 0.0

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "avg_profit": avg_profit,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "total_pnl": total_pnl,
    }


def store_performance_metrics(connection: sqlite3.Connection, metrics: dict[str, float | int | str]) -> None:
    connection.execute(
        """
        INSERT INTO performance_metrics (
            id, updated_at, total_trades, wins, losses, win_rate, avg_profit, avg_loss,
            expectancy, profit_factor, total_pnl
        )
        VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            updated_at = excluded.updated_at,
            total_trades = excluded.total_trades,
            wins = excluded.wins,
            losses = excluded.losses,
            win_rate = excluded.win_rate,
            avg_profit = excluded.avg_profit,
            avg_loss = excluded.avg_loss,
            expectancy = excluded.expectancy,
            profit_factor = excluded.profit_factor,
            total_pnl = excluded.total_pnl
        """,
        (
            str(metrics["updated_at"]),
            int(metrics["total_trades"]),
            int(metrics["wins"]),
            int(metrics["losses"]),
            float(metrics["win_rate"]),
            float(metrics["avg_profit"]),
            float(metrics["avg_loss"]),
            float(metrics["expectancy"]),
            float(metrics["profit_factor"]),
            float(metrics["total_pnl"]),
        ),
    )


def refresh_performance_metrics(connection: sqlite3.Connection) -> dict[str, float | int | str]:
    metrics = calculate_performance_metrics(connection)
    store_performance_metrics(connection, metrics)
    return metrics


def update_signals(connection: sqlite3.Connection, signals: pd.DataFrame, timestamp: str) -> None:
    if is_expiry_window():
        return

    if signals.empty:
        return

    for _, signal in signals.iterrows():
        signal_name = str(signal["Signal"])
        if not is_trade_signal(signal_name):
            continue
        if str(signal.get("trend_stage", "")).upper() == "EMERGING":
            continue

        insert_signal(
            connection,
            {
                "timestamp": timestamp,
                "stock": str(signal["Stock"]).upper(),
                "signal_type": signal_name,
                "entry_price": float(signal["Entry"]),
                "stop_loss": float(signal["Stop Loss"]),
                "target": float(signal["Target"]),
                "confidence_score": int(signal["Confidence Score"]),
            },
        )


def process_exits(connection: sqlite3.Connection, signals: pd.DataFrame) -> None:
    active = get_active_trades(connection)
    if active.empty or signals.empty:
        return

    latest_by_stock = signals.drop_duplicates(subset=["Stock"], keep="last").set_index("Stock")
    now = datetime.now()
    expiry_window = is_expiry_window(now)

    for _, trade in active.iterrows():
        stock = str(trade["stock"]).upper()
        if stock not in latest_by_stock.index:
            continue

        latest = latest_by_stock.loc[stock]
        current_signal = str(latest["Signal"])
        current_price = float(latest.get("Current Price", latest.get("Entry", np.nan)))
        entry_signal = str(trade.get("original_signal_type", "") or trade["signal_type"])
        direction = signal_direction(entry_signal)
        current_direction = signal_direction(current_signal)
        stop_loss = _first_numeric(trade.get("original_stop_loss"), trade["stop_loss"])
        target = _first_numeric(trade.get("original_target"), trade["target"])
        rsi = float(latest.get("RSI", np.nan))
        ema20 = float(latest.get("EMA20", np.nan))
        opened_at = pd.to_datetime(trade["timestamp"], errors="coerce")
        original_entry = _first_numeric(trade.get("original_entry_price"), trade["entry_price"])
        live_pnl = pnl_pct(direction, original_entry, current_price)

        exit_reason = ""
        if direction == "LONG" and not np.isnan(stop_loss) and current_price <= stop_loss:
            exit_reason = "Stop Loss Hit"
        elif direction == "SHORT" and not np.isnan(stop_loss) and current_price >= stop_loss:
            exit_reason = "Stop Loss Hit"
        elif direction == "LONG" and not np.isnan(target) and current_price >= target:
            exit_reason = "Target Hit"
        elif direction == "SHORT" and not np.isnan(target) and current_price <= target:
            exit_reason = "Target Hit"
        elif current_direction in {"LONG", "SHORT"} and current_direction != direction:
            exit_reason = "Trend Reversal"
        elif direction == "LONG" and (
            (not np.isnan(rsi) and rsi < 45)
            and (not np.isnan(ema20) and current_price < ema20 * 0.995)
        ):
            exit_reason = "Early Exit"
        elif direction == "SHORT" and (
            (not np.isnan(rsi) and rsi > 55)
            and (not np.isnan(ema20) and current_price > ema20 * 1.005)
        ):
            exit_reason = "Early Exit"
        elif not pd.isna(opened_at) and now - opened_at.to_pydatetime() > timedelta(days=3) and not np.isnan(live_pnl) and live_pnl <= 0:
            exit_reason = "Time Exit"
        elif expiry_window and str(trade["signal_type"]).startswith("WEAK"):
            exit_reason = "Expiry Risk"

        if exit_reason:
            close_trade(connection, stock, current_price, exit_reason)


def classify_setup(
    data: pd.DataFrame,
    latest: pd.Series,
    distance_from_ema20: float,
    avg_volume: float,
    config: StrategyConfig,
    market_type: str,
) -> tuple[str, float]:
    close = float(latest["Close"])
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    rsi = float(latest["RSI"])
    prior_window = data.iloc[-(config.breakout_window + 1) : -1]
    highest_high = float(prior_window["High"].max())
    lowest_low = float(prior_window["Low"].min())
    volume_spike = float(latest["Volume"]) > config.volume_spike_multiplier * avg_volume
    near_ema20 = distance_from_ema20 <= config.overextended_max

    stock_bullish = ema20 > ema50
    stock_bearish = ema20 < ema50

    # Use market regime only as a directional veto.
    # Neutral/sideways markets should not force all symbols to WAIT.
    allow_long = stock_bullish and market_type != "TRENDING_BEARISH"
    allow_short = stock_bearish and market_type != "TRENDING_BULLISH"
    market_trending = allow_long or allow_short

    long_pullback_min = max(45.0, config.long_rsi - 5)
    long_pullback_max = min(75.0, config.long_rsi + 10)
    short_pullback_min = max(25.0, config.short_rsi - 10)
    short_pullback_max = min(55.0, config.short_rsi + 5)

    if allow_long and near_ema20 and long_pullback_min <= rsi <= long_pullback_max:
        return "PULLBACK_LONG", close
    if allow_short and near_ema20 and short_pullback_min <= rsi <= short_pullback_max:
        return "PULLBACK_SHORT", close
    if market_trending and allow_long and close > highest_high and volume_spike:
        return "BREAKOUT_LONG", close
    if market_trending and allow_short and close < lowest_low and volume_spike:
        return "BREAKOUT_SHORT", close
    if allow_long:
        return "TREND_LONG", close
    if allow_short:
        return "TREND_SHORT", close
    return "", np.nan


def detect_swings(data: pd.DataFrame, lookback: int = 2) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    highs = data["High"].astype(float).to_numpy()
    lows = data["Low"].astype(float).to_numpy()
    swing_highs: list[tuple[int, float]] = []
    swing_lows: list[tuple[int, float]] = []

    for idx in range(lookback, len(data) - lookback):
        high_window = highs[idx - lookback : idx + lookback + 1]
        low_window = lows[idx - lookback : idx + lookback + 1]
        high_value = highs[idx]
        low_value = lows[idx]

        if np.isfinite(high_value) and high_value == np.max(high_window):
            if np.sum(high_window == high_value) == 1:
                swing_highs.append((idx, float(high_value)))
        if np.isfinite(low_value) and low_value == np.min(low_window):
            if np.sum(low_window == low_value) == 1:
                swing_lows.append((idx, float(low_value)))

    return swing_highs, swing_lows


def _trend_sep_ratio(row: pd.Series) -> float:
    e20, e50 = float(row["EMA20"]), float(row["EMA50"])
    if abs(e50) < 1e-9:
        return 0.0
    return abs(e20 - e50) / abs(e50)


def compute_trend_watch_state(data: pd.DataFrame) -> dict[str, Any]:
    """
    Watchlist-only trend ladder (EMERGING → CONFIRMING → STRONG; INVALID clears).
    Does not influence trade Signal / entries; used for UI and EMERGING trade blocks only.
    """
    empty = {
        "trend_stage": "",
        "trend_age": 0,
        "trend_watch_note": "",
        "trend_watch_direction": "",
    }
    if len(data) < 25:
        return empty

    stage = ""
    direction = ""
    emerging_streak = 0
    watch_age = 0

    for i in range(len(data)):
        row = data.iloc[i]
        close = float(row["Close"])
        ema20 = float(row["EMA20"])
        ema50 = float(row["EMA50"])
        rsi = float(row["RSI"])
        vol = float(row["Volume"])
        avg_v = float(row["Avg Volume"])
        prev_rsi = float(data.iloc[i - 1]["RSI"]) if i > 0 else float(rsi)
        sep = _trend_sep_ratio(row)
        prev_sep = _trend_sep_ratio(data.iloc[i - 1]) if i > 0 else sep

        vol_soft = avg_v * 1.2 if avg_v > 1e-9 else np.inf

        long_inv = close < ema20 or rsi < 50.0
        short_inv = close > ema20 or rsi > 50.0

        long_enter_em = (
            ema20 > ema50 and close > ema20 and 50.0 <= rsi < 60.0 and vol < vol_soft
        )
        short_enter_em = (
            ema20 < ema50 and close < ema20 and 40.0 < rsi <= 50.0 and vol < vol_soft
        )

        long_hold_em_body = ema20 > ema50 and close > ema20 and 50.0 <= rsi < 60.0
        short_hold_em_body = ema20 < ema50 and close < ema20 and 40.0 < rsi <= 50.0

        if stage in ("", "INVALID"):
            stage = ""
            direction = ""
            emerging_streak = 0
            watch_age = 0
            if long_enter_em:
                stage, direction = "EMERGING", "LONG"
                emerging_streak = 1
                watch_age = 1
            elif short_enter_em:
                stage, direction = "EMERGING", "SHORT"
                emerging_streak = 1
                watch_age = 1
            continue

        if stage == "EMERGING":
            if direction == "LONG":
                if long_inv or not (ema20 > ema50 and close > ema20):
                    stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0
                elif long_hold_em_body:
                    emerging_streak += 1
                    watch_age += 1
                    if emerging_streak >= 2 and close > ema20 and rsi >= prev_rsi - 0.25:
                        stage = "CONFIRMING"
                        emerging_streak = 0
                else:
                    watch_age += 1
            else:
                if short_inv or not (ema20 < ema50 and close < ema20):
                    stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0
                elif short_hold_em_body:
                    emerging_streak += 1
                    watch_age += 1
                    if emerging_streak >= 2 and close < ema20 and rsi <= prev_rsi + 0.25:
                        stage = "CONFIRMING"
                        emerging_streak = 0
                else:
                    watch_age += 1

        elif stage == "CONFIRMING":
            if direction == "LONG":
                if long_inv:
                    stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0
                else:
                    watch_age += 1
                    avg_ok = avg_v > 1e-9 and vol > avg_v * 1.05
                    if sep > prev_sep and rsi > 60.0 and avg_ok:
                        stage = "STRONG"
            else:
                if short_inv:
                    stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0
                else:
                    watch_age += 1
                    avg_ok = avg_v > 1e-9 and vol > avg_v * 1.05
                    if sep > prev_sep and rsi < 40.0 and avg_ok:
                        stage = "STRONG"

        elif stage == "STRONG":
            if direction == "LONG" and long_inv:
                stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0
            elif direction == "SHORT" and short_inv:
                stage, direction, emerging_streak, watch_age = "INVALID", "", 0, 0

    if stage == "INVALID":
        return empty

    note = ""
    if stage == "CONFIRMING":
        note = "Preparing for breakdown" if direction == "SHORT" else "Preparing for breakout"

    return {
        "trend_stage": stage,
        "trend_age": int(watch_age) if stage else 0,
        "trend_watch_note": note,
        "trend_watch_direction": direction,
    }


def structure_state(
    data: pd.DataFrame,
    latest: pd.Series,
    avg_volume: float,
    config: StrategyConfig,
) -> dict[str, Any]:
    swings_high, swings_low = detect_swings(data, lookback=config.swing_lookback)
    last_close = float(latest["Close"])
    last_volume = float(latest["Volume"])
    volume_strength_ratio = (last_volume / avg_volume) if avg_volume and avg_volume > 0 else 0.0
    volume_confirmed = volume_strength_ratio >= 1.5

    high_tag = ""
    low_tag = ""
    if len(swings_high) >= 2:
        high_tag = "HH" if swings_high[-1][1] > swings_high[-2][1] else "LH"
    if len(swings_low) >= 2:
        low_tag = "HL" if swings_low[-1][1] > swings_low[-2][1] else "LL"

    if high_tag and low_tag:
        market_structure = f"{high_tag}/{low_tag}"
    else:
        market_structure = high_tag or low_tag or "UNKNOWN"

    previous_swing_high = swings_high[-1][1] if swings_high else np.nan
    previous_swing_low = swings_low[-1][1] if swings_low else np.nan
    bullish_break = bool(not np.isnan(previous_swing_high) and last_close > previous_swing_high)
    bearish_break = bool(not np.isnan(previous_swing_low) and last_close < previous_swing_low)
    structure_break = bullish_break or bearish_break
    bullish_structure = high_tag == "HH" and low_tag == "HL"
    bearish_structure = high_tag == "LH" and low_tag == "LL"

    structure_signal = ""
    structure_direction = "WAIT"
    if structure_break and volume_confirmed:
        structure_signal = "STRUCTURE_BREAK"
        structure_direction = "LONG" if bullish_break else "SHORT"
    elif bullish_structure and volume_confirmed:
        structure_signal = "STRUCTURE_BULLISH"
        structure_direction = "LONG"
    elif bearish_structure and volume_confirmed:
        structure_signal = "STRUCTURE_BEARISH"
        structure_direction = "SHORT"
    elif bullish_structure:
        structure_direction = "LONG"
    elif bearish_structure:
        structure_direction = "SHORT"

    return {
        "structure_signal": structure_signal,
        "structure_direction": structure_direction,
        "market_structure": market_structure,
        "structure_break": structure_break,
        "volume_strength_ratio": volume_strength_ratio,
        "volume_confirmed": volume_confirmed,
        "bullish_break": bullish_break,
        "bearish_break": bearish_break,
    }


def confidence_score(
    signal: str,
    rsi: float,
    volume: float,
    avg_volume: float,
    distance_from_ema20: float,
    trend_strength: float,
    atr_value: float,
    atr_median: float,
    config: StrategyConfig,
) -> int:
    direction = signal_direction(signal)
    if direction not in {"LONG", "SHORT"}:
        return 0

    score = 0
    score += 1
    score += int((direction == "LONG" and rsi > 60) or (direction == "SHORT" and rsi < 40))
    score += int(volume > avg_volume)
    score += int(distance_from_ema20 < config.overextended_max)
    score += int(trend_strength > config.trend_strength_min)
    return score


def build_trade_plan(signal: str, entry: float, atr_value: float, config: StrategyConfig) -> tuple[float, float]:
    if is_long_signal(signal):
        risk = atr_value * config.atr_multiplier
        return entry - risk, entry + (config.target_rr * risk)
    if is_short_signal(signal):
        risk = atr_value * config.atr_multiplier
        return entry + risk, entry - (config.target_rr * risk)
    return np.nan, np.nan


def _row_lot_size(row: pd.Series, lot_size_by_symbol: dict[str, int] | None = None) -> int | None:
    for column in ("lot_size", "Lot Size", "LotSize", "lot", "Lot"):
        if column in row.index:
            lot_size = pd.to_numeric(row.get(column), errors="coerce")
            if pd.notna(lot_size) and int(lot_size) > 0:
                return int(lot_size)
    if lot_size_by_symbol:
        stock = str(row.get("Stock", "")).upper()
        lot_size = lot_size_by_symbol.get(stock)
        if lot_size is None and stock.endswith(".NS"):
            lot_size = lot_size_by_symbol.get(stock[:-3])
        if lot_size is not None and int(lot_size) > 0:
            return int(lot_size)
    return None


def apply_position_sizing(
    signals: pd.DataFrame,
    risk_settings: RiskSettings,
    lot_size_by_symbol: dict[str, int] | None = None,
) -> pd.DataFrame:
    if signals.empty:
        return signals.reindex(columns=FUTURES_COLUMNS)

    sized = signals.copy()
    capital = max(0.0, float(risk_settings.trading_capital))
    per_trade_pct = max(0.0, float(risk_settings.max_risk_per_trade_pct))
    allowed_risk_amount = capital * per_trade_pct / 100.0

    risk_values: list[Any] = []
    qty_values: list[Any] = []
    lot_values: list[Any] = []
    reward_values: list[Any] = []
    position_risk_values: list[Any] = []

    for _, row in sized.iterrows():
        entry = pd.to_numeric(row.get("Entry"), errors="coerce")
        stop_loss = pd.to_numeric(row.get("Stop Loss"), errors="coerce")
        target = pd.to_numeric(row.get("Target"), errors="coerce")
        if pd.isna(entry) or pd.isna(stop_loss):
            risk_values.append("Invalid risk")
            qty_values.append("Invalid risk")
            lot_values.append("")
            reward_values.append("")
            position_risk_values.append("Invalid risk")
            continue

        risk_per_share = abs(float(entry) - float(stop_loss))
        if risk_per_share <= 0:
            risk_values.append("Invalid risk")
            qty_values.append("Invalid risk")
            lot_values.append("")
            reward_values.append("")
            position_risk_values.append("Invalid risk")
            continue

        suggested_quantity = int(np.floor(allowed_risk_amount / risk_per_share)) if allowed_risk_amount > 0 else 0
        estimated_loss_at_sl = suggested_quantity * risk_per_share
        reward_per_share = abs(float(target) - float(entry)) if pd.notna(target) else np.nan
        estimated_profit_at_target = (
            suggested_quantity * reward_per_share if pd.notna(reward_per_share) else np.nan
        )
        position_risk_pct = (estimated_loss_at_sl / capital * 100.0) if capital > 0 else 0.0
        lot_size = _row_lot_size(row, lot_size_by_symbol)

        risk_values.append(float(estimated_loss_at_sl))
        qty_values.append(int(suggested_quantity))
        lot_values.append(int(np.floor(suggested_quantity / lot_size)) if lot_size else "")
        reward_values.append(float(estimated_profit_at_target) if pd.notna(estimated_profit_at_target) else "")
        position_risk_values.append(float(position_risk_pct))

    sized["Risk ₹"] = risk_values
    sized["Suggested Qty"] = qty_values
    sized["Suggested Lots"] = lot_values
    sized["Reward ₹"] = reward_values
    sized["Position Risk %"] = position_risk_values
    return sized.reindex(columns=FUTURES_COLUMNS)


def evaluate_symbol(
    symbol: str,
    history: pd.DataFrame,
    config: StrategyConfig,
    market_regime: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    if len(history) < config.minimum_rows:
        logger.info("Skipping %s: insufficient data rows=%s", symbol, len(history))
        return None

    data = add_indicators(history, config).dropna()
    if len(data) < config.breakout_window + 2:
        logger.info("Skipping %s: insufficient indicator rows=%s", symbol, len(data))
        return None

    latest = data.iloc[-1]
    close = float(latest["Close"])
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    rsi = float(latest["RSI"])
    atr_value = float(latest["ATR"])
    atr_median = float(latest["ATR Median"])
    volume = float(latest["Volume"])
    avg_volume = float(latest["Avg Volume"])
    distance_from_ema20 = abs(close - ema20) / ema20
    trend_strength = abs(ema20 - ema50) / ema50
    stock_return_pct = _window_return_pct(data["Close"], lookback=20)
    stock_drawdown_pct = _window_drawdown_pct(data["Close"], lookback=20)

    market_type = (market_regime or {}).get("market_type", "UNKNOWN")
    signal_type, entry = classify_setup(
        data=data,
        latest=latest,
        distance_from_ema20=distance_from_ema20,
        avg_volume=avg_volume,
        config=config,
        market_type=market_type,
    )
    bullish_stock = ema20 > ema50
    bearish_stock = ema20 < ema50
    pullback_long = signal_type == "PULLBACK_LONG"
    pullback_short = signal_type == "PULLBACK_SHORT"
    breakout_long = signal_type == "BREAKOUT_LONG"
    breakout_short = signal_type == "BREAKOUT_SHORT"

    if pullback_long or breakout_long:
        signal = "STRONG_LONG"
    elif signal_type == "TREND_LONG":
        signal = "WEAK_LONG"
    elif pullback_short or breakout_short:
        signal = "STRONG_SHORT"
    elif signal_type == "TREND_SHORT":
        signal = "WEAK_SHORT"
    else:
        signal = "WAIT"

    structure = structure_state(
        data=data,
        latest=latest,
        avg_volume=avg_volume,
        config=config,
    )
    structure_signal = str(structure["structure_signal"])
    structure_direction = str(structure["structure_direction"])
    structure_break = bool(structure["structure_break"])
    volume_confirmed = bool(structure["volume_confirmed"])

    # Dow-theory structure acts as an additional overlay.
    # EMA + structure agreement upgrades confidence; structure-led breaks can surface early signals.
    if signal_direction(signal) == structure_direction and is_trade_signal(signal) and structure_signal:
        signal = "STRONG_LONG" if structure_direction == "LONG" else "STRONG_SHORT"
    elif structure_break and volume_confirmed and structure_direction == "LONG" and signal_direction(signal) != "LONG":
        signal = "EARLY_LONG"
        if np.isnan(entry):
            entry = close
        if not signal_type:
            signal_type = "STRUCTURE_BREAK"
    elif structure_break and volume_confirmed and structure_direction == "SHORT" and signal_direction(signal) != "SHORT":
        signal = "EARLY_SHORT"
        if np.isnan(entry):
            entry = close
        if not signal_type:
            signal_type = "STRUCTURE_BREAK"

    stop_loss, target = build_trade_plan(signal, entry, atr_value, config)
    score = confidence_score(
        signal=signal,
        rsi=rsi,
        volume=volume,
        avg_volume=avg_volume,
        distance_from_ema20=distance_from_ema20,
        trend_strength=trend_strength,
        atr_value=atr_value,
        atr_median=atr_median,
        config=config,
    )

    if signal == "WAIT":
        if market_type not in {"TRENDING_BULLISH", "TRENDING_BEARISH"}:
            reason = f"No trade: market regime is {market_type}"
        elif market_type == "TRENDING_BULLISH" and not bullish_stock:
            reason = "No trade: stock trend not bullish"
        elif market_type == "TRENDING_BEARISH" and not bearish_stock:
            reason = "No trade: stock trend not bearish"
        else:
            reason = "No trade: setup not ready"
    elif signal.startswith("STRONG"):
        reason = f"Accepted: {signal_type} swing setup"
    elif signal.startswith("EARLY"):
        reason = "Accepted early: Dow structure break with volume confirmation"
    else:
        reason = f"Watchlist: trend aligned, waiting for pullback or breakout trigger"

    exit_signal = ""
    if is_long_signal(signal) and (rsi < 50 or close < ema20):
        exit_signal = "LONG early exit"
    elif is_short_signal(signal) and (rsi > 50 or close > ema20):
        exit_signal = "SHORT early exit"

    explain = build_explainability(
        signal=signal,
        signal_type=str(signal_type),
        close=close,
        ema20=ema20,
        ema50=ema50,
        rsi=rsi,
        volume=volume,
        avg_volume=avg_volume,
        distance_from_ema20=distance_from_ema20,
        trend_strength=trend_strength,
        config=config,
        market_type=market_type,
    )

    tw = compute_trend_watch_state(data)

    market_return_pct = pd.to_numeric((market_regime or {}).get("nifty_return_pct"), errors="coerce")
    market_drawdown_pct = pd.to_numeric((market_regime or {}).get("market_drawdown_pct"), errors="coerce")
    market_type_upper = str((market_regime or {}).get("market_type", "UNKNOWN")).upper()
    trend_confirmation_upper = str((market_regime or {}).get("trend_confirmation", "WEAK")).upper()
    bearish_or_weak_market = market_type_upper in {"TRENDING_BEARISH", "SIDEWAYS", "NEUTRAL", "UNKNOWN"} or trend_confirmation_upper == "WEAK"
    rs_outperformance = (
        float(stock_return_pct - market_return_pct)
        if pd.notna(stock_return_pct) and pd.notna(market_return_pct)
        else np.nan
    )
    smaller_drawdown = (
        bool(pd.notna(stock_drawdown_pct) and pd.notna(market_drawdown_pct) and float(stock_drawdown_pct) < float(market_drawdown_pct))
    )
    recent_slice = data.tail(6)
    recent_breakdown = bool((recent_slice["Close"] < recent_slice["EMA20"]).any()) if not recent_slice.empty else True
    early_rs_long = (
        bearish_or_weak_market
        and close > ema20
        and pd.notna(rs_outperformance)
        and float(rs_outperformance) > 1.0
        and rsi > 55.0
        and smaller_drawdown
        and not recent_breakdown
    )
    relative_strength_score = 0.0
    if pd.notna(rs_outperformance):
        relative_strength_score += min(4.0, max(0.0, float(rs_outperformance)))
    if rsi > 55.0:
        relative_strength_score += min(2.0, (rsi - 55.0) / 5.0)
    if smaller_drawdown:
        relative_strength_score += 2.0
    if close > ema20:
        relative_strength_score += 1.0
    if not recent_breakdown:
        relative_strength_score += 1.0

    days_to_event = _days_to_next_event(symbol)
    event_within_7_days = bool(pd.notna(days_to_event) and 0 <= float(days_to_event) <= 7)
    rsi_strengthening = bool(len(data) >= 4 and float(data["RSI"].iloc[-1]) > float(data["RSI"].iloc[-4]))
    pre_event_accumulation = (
        event_within_7_days
        and pd.notna(rs_outperformance)
        and float(rs_outperformance) > 0.0
        and close > ema20 > ema50
        and rsi_strengthening
        and smaller_drawdown
    )
    prev_close = pd.to_numeric(data["Close"].iloc[-2], errors="coerce") if len(data) >= 2 else np.nan
    gap_up_pct = (
        (close - float(prev_close)) / float(prev_close) * 100.0
        if pd.notna(prev_close) and float(prev_close) != 0.0
        else np.nan
    )
    atr_spike_ratio = (atr_value / atr_median) if atr_median > 0 else np.nan
    post_event_risk = (
        pd.notna(gap_up_pct)
        and float(gap_up_pct) > 5.0
        and distance_from_ema20 > 0.035
        and pd.notna(atr_spike_ratio)
        and float(atr_spike_ratio) > 1.35
    )
    event_strength_score = 0.0
    if pre_event_accumulation:
        event_strength_score += 5.0
    if pd.notna(rs_outperformance):
        event_strength_score += min(3.0, max(0.0, float(rs_outperformance)))
    if rsi_strengthening:
        event_strength_score += 1.0
    if smaller_drawdown:
        event_strength_score += 1.0

    return {
        "Stock": symbol,
        "Signal": signal,
        "Signal Type": signal_type,
        "Structure Signal": structure_signal,
        "Entry": entry,
        "Current Price": close,
        "EMA20": ema20,
        "EMA50": ema50,
        "Stop Loss": stop_loss,
        "Target": target,
        "RSI": rsi,
        "ATR": atr_value,
        "Volume": volume,
        "Avg Volume": avg_volume,
        "Volume Strength": float(structure["volume_strength_ratio"]),
        "Market Structure": str(structure["market_structure"]),
        "Structure Break": "Yes" if structure_break else "No",
        "Distance from EMA20 %": distance_from_ema20 * 100,
        "Confidence Score": score,
        "Trade Quality": explain["trade_quality"],
        "trend_direction": explain["trend_direction"],
        "ema_condition": explain["ema_condition"],
        "rsi_value": explain["rsi_value"],
        "rsi_condition": explain["rsi_condition"],
        "volume_value": explain["volume_value"],
        "volume_condition": explain["volume_condition"],
        "distance_from_ema": explain["distance_from_ema"],
        "distance_condition": explain["distance_condition"],
        "trend_strength": explain["trend_strength"],
        "strategy_type": explain["strategy_type"],
        "failed_conditions": explain["failed_conditions"],
        "Exit Signal": exit_signal,
        "Reason": reason,
        "trend_watch_direction": tw["trend_watch_direction"],
        "trend_stage": tw["trend_stage"],
        "trend_age": tw["trend_age"],
        "trend_watch_note": tw["trend_watch_note"],
        "Early RS Label": "EARLY_RS_LONG" if early_rs_long else "",
        "Relative Strength Score": round(float(relative_strength_score), 2) if early_rs_long else np.nan,
        "Pre Event Label": "PRE_EVENT_ACCUMULATION" if pre_event_accumulation else "",
        "Post Event Label": "POST_EVENT_RISK" if post_event_risk else "",
        "Event Strength Score": round(float(event_strength_score), 2) if pre_event_accumulation else np.nan,
        "Event Warning": "Late entry risk elevated" if post_event_risk else "",
    }


def analyze_stock(
    symbol: str,
    history: pd.DataFrame,
    config: StrategyConfig,
    market_regime: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    """Scan-time entrypoint: returns full futures row including explainability fields (see evaluate_symbol)."""
    return evaluate_symbol(symbol, history, config, market_regime)


def sort_futures_table(table: pd.DataFrame) -> pd.DataFrame:
    if table.empty:
        return table.reindex(columns=FUTURES_COLUMNS)

    sorted_table = table.copy()
    sorted_table["_signal_priority"] = sorted_table["Signal"].map(SIGNAL_PRIORITY).fillna(9)
    sorted_table["_type_priority"] = sorted_table["Signal Type"].map(SIGNAL_TYPE_PRIORITY).fillna(9)

    sorted_table = sorted_table.sort_values(
        by=["_signal_priority", "_type_priority", "Confidence Score", "Distance from EMA20 %", "Stock"],
        ascending=[True, True, False, True, True],
    )
    return sorted_table.drop(columns=["_signal_priority", "_type_priority"]).reindex(columns=FUTURES_COLUMNS)


@st.cache_data(ttl=180, show_spinner=False)
def scan_symbols(
    symbols: list[str],
    config: StrategyConfig,
    market_regime: dict[str, str],
    period: str = "30d",
    interval: str = "1h",
    use_sample_data: bool = False,
    market_refresh_nonce: int = 0,
) -> tuple[pd.DataFrame, list[str]]:
    if not symbols:
        return empty_futures_table(), ["No symbols available to scan"]

    logger.info(
        "scan_symbols: start symbol_count=%s use_sample_data=%s nonce=%s period=%s interval=%s",
        len(symbols),
        use_sample_data,
        market_refresh_nonce,
        period,
        interval,
    )
    rows = []
    errors = []
    no_data_symbols: list[str] = []
    scan_started = time.perf_counter()

    market_data = pd.DataFrame()
    if not use_sample_data:
        download_started = time.perf_counter()
        logger.info("scan_symbols: downloading market data for %s symbols", len(symbols))
        try:
            market_data = download_market_data(tuple(symbols), period, interval)
            logger.info(
                "scan_symbols: market_data ready shape=%s empty=%s download_elapsed_s=%.2f",
                market_data.shape if isinstance(market_data, pd.DataFrame) else None,
                market_data.empty if isinstance(market_data, pd.DataFrame) else True,
                time.perf_counter() - download_started,
            )
        except Exception as exc:
            logger.exception("scan_symbols: Market data download failed: %s", exc)
            return empty_futures_table(), [
                f"Market data download failed: {exc}. Check Alpha Vantage / EODHD keys, quotas, and network."
            ]
    else:
        logger.info("scan_symbols: using synthetic sample history (no download_market_data)")

    total_symbols = len(symbols)
    for idx, symbol in enumerate(symbols, start=1):
        try:
            history = (
                make_sample_history(symbol)
                if use_sample_data
                else extract_symbol_history(market_data, symbol, len(symbols))
            )
            if history.empty:
                logger.info("Skipping %s: no price data returned", symbol)
                no_data_symbols.append(symbol)
                continue
            row = analyze_stock(symbol, history, config, market_regime)
            if row is not None:
                rows.append(row)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
            logger.exception("Skipping %s", symbol)
        if idx == 1 or idx % 10 == 0 or idx == total_symbols:
            logger.info(
                "scan_symbols: progress %s/%s rows=%s no_data=%s errors=%s elapsed_s=%.2f",
                idx,
                total_symbols,
                len(rows),
                len(no_data_symbols),
                len(errors),
                time.perf_counter() - scan_started,
            )

    if not rows:
        if no_data_symbols and not use_sample_data:
            logger.warning(
                "scan_symbols: no signal rows; no_data_symbols=%s errors=%s",
                len(no_data_symbols),
                len(errors),
            )
            details = errors.copy()
            provider_messages = _provider_unavailable_messages()
            if provider_messages:
                details = provider_messages + details
            details.append(
                f"No valid symbol data found: {len(no_data_symbols)} symbols returned no candles from Alpha Vantage / EODHD. "
                "Check network/API keys and quotas, or enable 'Use sample data'."
            )
            return empty_futures_table(), details
        logger.warning("scan_symbols: no signal rows; errors=%s", len(errors))
        return empty_futures_table(), errors or ["No valid symbol data found"]

    logger.info(
        "scan_symbols: success signal_rows=%s errors=%s no_data_symbols=%s total_elapsed_s=%.2f",
        len(rows),
        len(errors),
        len(no_data_symbols),
        time.perf_counter() - scan_started,
    )
    return sort_futures_table(pd.DataFrame(rows)), errors


def scan_selected_symbols_only(
    symbols: list[str],
    config: StrategyConfig,
    market_regime: dict[str, str],
    period: str = "30d",
    interval: str = "1h",
    use_sample_data: bool = False,
) -> tuple[pd.DataFrame, list[str]]:
    if not symbols:
        return empty_futures_table(), []

    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    market_data = pd.DataFrame()
    if not use_sample_data:
        try:
            market_data = download_market_data(tuple(symbols), period, interval)
        except Exception as exc:
            return empty_futures_table(), [f"Market data download failed: {exc}"]

    for symbol in symbols:
        try:
            history = (
                make_sample_history(symbol)
                if use_sample_data
                else extract_symbol_history(market_data, symbol, len(symbols))
            )
            if history.empty:
                continue
            row = analyze_stock(symbol, history, config, market_regime)
            if row is not None:
                rows.append(row)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
    if not rows:
        return empty_futures_table(), errors
    return sort_futures_table(pd.DataFrame(rows)), errors


def get_fno_selected_snapshot(
    selected_symbols: list[str],
    config: StrategyConfig,
    market_regime: dict[str, str],
    use_sample_data: bool = False,
    period: str = "30d",
    interval: str = "1h",
    force_refresh: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    now = time.time()
    key_symbols = tuple(sorted(str(s).upper() for s in selected_symbols if str(s).strip()))
    cache_obj = st.session_state.get(FNO_SELECTED_CACHE_KEY, {})
    cached_symbols = tuple(cache_obj.get("symbols", ()))
    cached_ts = float(cache_obj.get("timestamp", 0.0) or 0.0)
    cached_df = cache_obj.get("signals")
    cache_age = max(0.0, now - cached_ts) if cached_ts > 0 else np.inf

    usage = compute_api_usage_display()
    quota_low = (
        int(usage.get("alpha_minute_remaining", 99)) <= 1
        or int(usage.get("eodhd_minute_remaining", 99)) <= 1
    )

    if (
        not force_refresh
        and key_symbols == cached_symbols
        and isinstance(cached_df, pd.DataFrame)
        and not cached_df.empty
        and cache_age < float(FNO_SELECTED_CACHE_TTL_SECONDS)
    ):
        return cached_df.copy(), {
            "source": "CACHE",
            "last_refresh_epoch": cached_ts,
            "ttl_seconds": FNO_SELECTED_CACHE_TTL_SECONDS,
            "next_refresh_in_sec": max(0, int(FNO_SELECTED_CACHE_TTL_SECONDS - cache_age)),
            "quota_low": quota_low,
        }

    if quota_low and isinstance(cached_df, pd.DataFrame) and not cached_df.empty and key_symbols == cached_symbols:
        return cached_df.copy(), {
            "source": "CACHE_QUOTA",
            "last_refresh_epoch": cached_ts,
            "ttl_seconds": FNO_SELECTED_CACHE_TTL_SECONDS,
            "next_refresh_in_sec": max(0, int(FNO_SELECTED_CACHE_TTL_SECONDS - cache_age)),
            "quota_low": True,
        }

    live_df, _errors = scan_selected_symbols_only(
        symbols=list(key_symbols),
        config=config,
        market_regime=market_regime,
        period=period,
        interval=interval,
        use_sample_data=use_sample_data,
    )
    if isinstance(live_df, pd.DataFrame) and not live_df.empty:
        st.session_state[FNO_SELECTED_CACHE_KEY] = {
            "symbols": key_symbols,
            "timestamp": now,
            "signals": live_df.copy(),
        }
        return live_df.copy(), {
            "source": "LIVE",
            "last_refresh_epoch": now,
            "ttl_seconds": FNO_SELECTED_CACHE_TTL_SECONDS,
            "next_refresh_in_sec": int(FNO_SELECTED_CACHE_TTL_SECONDS),
            "quota_low": quota_low,
        }

    if isinstance(cached_df, pd.DataFrame) and not cached_df.empty and key_symbols == cached_symbols:
        return cached_df.copy(), {
            "source": "CACHE_FALLBACK",
            "last_refresh_epoch": cached_ts,
            "ttl_seconds": FNO_SELECTED_CACHE_TTL_SECONDS,
            "next_refresh_in_sec": max(0, int(FNO_SELECTED_CACHE_TTL_SECONDS - cache_age)),
            "quota_low": quota_low,
        }

    return empty_futures_table(), {
        "source": "EMPTY",
        "last_refresh_epoch": 0.0,
        "ttl_seconds": FNO_SELECTED_CACHE_TTL_SECONDS,
        "next_refresh_in_sec": 0,
        "quota_low": quota_low,
    }


def ranked_trades(signals: pd.DataFrame, allowed_signals: set[str], limit: int | None = None) -> pd.DataFrame:
    if signals.empty:
        return empty_futures_table()

    trades = signals[signals["Signal"].isin(allowed_signals)].copy()
    if trades.empty:
        return empty_futures_table()

    trades = trades.sort_values(
        by=["Confidence Score", "Distance from EMA20 %", "Stock"],
        ascending=[False, True, True],
    )
    if limit is not None:
        trades = trades.head(limit)
    return trades.reindex(columns=FUTURES_COLUMNS)


def filter_by_market_strategy(trades: pd.DataFrame, suggested_strategy: str) -> pd.DataFrame:
    if trades.empty:
        return trades

    if suggested_strategy == "BREAKOUT_LONG":
        filtered = trades[
            trades["Signal"].isin(["STRONG_LONG", "WEAK_LONG"])
            & trades["Signal Type"].eq("Breakout")
        ]
    elif suggested_strategy == "BREAKOUT_SHORT":
        filtered = trades[
            trades["Signal"].isin(["STRONG_SHORT", "WEAK_SHORT"])
            & trades["Signal Type"].eq("Breakout")
        ]
    elif suggested_strategy == "PULLBACK":
        filtered = trades[trades["Signal Type"].eq("Pullback")]
    elif suggested_strategy == "NO TRADE":
        filtered = trades.iloc[0:0]
    else:
        filtered = trades

    return filtered.reindex(columns=FUTURES_COLUMNS)


def strike_step(price: float) -> int:
    if price < 200:
        return 5
    if price < 500:
        return 10
    if price < 1_000:
        return 20
    if price < 2_500:
        return 50
    return 100


def closest_atm_strike(price: float) -> int:
    step = strike_step(price)
    return int(round(price / step) * step)


def approximate_option_premium(futures_price: float, strike: float, option_type: str, atr_value: float) -> float:
    intrinsic_value = max(futures_price - strike, 0) if option_type == "CE" else max(strike - futures_price, 0)
    time_value = max(atr_value * 0.45, futures_price * 0.006)
    return max(intrinsic_value + time_value, 0.05)


def build_options_plan(
    trades: pd.DataFrame,
    option_symbols: list[str],
    config: StrategyConfig,
) -> pd.DataFrame:
    if trades.empty or not option_symbols:
        return empty_options_table()

    option_set = set(option_symbols)
    rows = []

    for _, trade in trades.iterrows():
        stock = clean_underlying_symbol(trade["Stock"])
        signal = str(trade["Signal"])
        if not stock or stock not in option_set or not is_trade_signal(signal):
            continue

        option_type = "CE" if is_long_signal(signal) else "PE"
        futures_entry = float(trade["Entry"])
        atr_value = float(trade["ATR"])
        strike = closest_atm_strike(futures_entry)
        entry_premium = approximate_option_premium(futures_entry, strike, option_type, atr_value)

        rows.append(
            {
                "Stock": stock,
                "Option Symbol": f"{stock} {strike} {option_type}",
                "Strike": strike,
                "Option Type": option_type,
                "Entry Premium (approx)": entry_premium,
                "SL Premium": entry_premium * (1 - config.option_sl_pct),
                "Target Premium": entry_premium * (1 + config.option_target_pct),
            }
        )

    return pd.DataFrame(rows).reindex(columns=OPTIONS_COLUMNS) if rows else empty_options_table()


def active_trades_table(active: pd.DataFrame, signals: pd.DataFrame) -> pd.DataFrame:
    if active.empty:
        return empty_active_table()

    current_prices = {}
    if not signals.empty:
        current_prices = (
            signals.drop_duplicates(subset=["Stock"], keep="last")
            .set_index("Stock")["Current Price"]
            .to_dict()
        )

    rows = []
    for _, trade in active.iterrows():
        stock = str(trade["stock"]).upper()
        direction = signal_direction(str(trade.get("original_signal_type") or trade["signal_type"]))
        entry_price = _first_numeric(trade.get("original_entry_price"), trade["entry_price"])
        current_price = float(current_prices.get(stock, entry_price))
        rows.append(
            {
                "Stock": stock,
                "Direction": direction,
                "Entry": entry_price,
                "Current Price": current_price,
                "P&L %": pnl_pct(direction, entry_price, current_price),
                "SL": _first_numeric(trade.get("original_stop_loss"), trade["stop_loss"]),
                "Target": _first_numeric(trade.get("original_target"), trade["target"]),
                "Confidence": int(trade["confidence_score"]) if not pd.isna(trade["confidence_score"]) else 0,
            }
        )

    return pd.DataFrame(rows).reindex(columns=ACTIVE_COLUMNS)


def closed_trades_table(closed: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    if closed.empty:
        return empty_closed_table()

    closed["_timestamp"] = pd.to_datetime(closed["timestamp"], errors="coerce")
    closed = closed.sort_values("_timestamp", ascending=False).head(limit)

    rows = []
    for _, trade in closed.iterrows():
        direction = signal_direction(str(trade.get("original_signal_type") or trade["signal_type"]))
        entry_price = _first_numeric(trade.get("original_entry_price"), trade["entry_price"])
        exit_price = _first_numeric(trade.get("final_exit_price"), trade["exit_price"])
        stored_pnl = pd.to_numeric(trade.get("final_pnl_percent", trade.get("pnl_percent")), errors="coerce")
        final_pnl = float(stored_pnl) if not pd.isna(stored_pnl) else pnl_pct(direction, entry_price, exit_price)
        rows.append(
            {
                "Stock": str(trade["stock"]).upper(),
                "Entry": entry_price,
                "Exit": exit_price,
                "P&L %": final_pnl,
                "Exit Reason": trade.get("final_exit_reason") or trade["exit_reason"],
            }
        )

    return pd.DataFrame(rows).reindex(columns=CLOSED_COLUMNS)


def highlight_pullback_rows(row: pd.Series) -> list[str]:
    if str(row.get("Signal Type", "")).startswith("PULLBACK"):
        return ["background-color: #fff7cc; color: #2f2600"] * len(row)
    return [""] * len(row)


def _format_money_value(value: Any) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.notna(numeric):
        return f"{float(numeric):,.2f}"
    return str(value) if str(value).strip() else "-"


def _format_int_value(value: Any) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.notna(numeric):
        return f"{int(numeric):,}"
    return str(value) if str(value).strip() else "-"


def _format_pct_value(value: Any) -> str:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.notna(numeric):
        return f"{float(numeric):,.2f}%"
    return str(value) if str(value).strip() else "-"


def format_futures_table(data: pd.DataFrame):
    return data.style.apply(highlight_pullback_rows, axis=1).format(
        {
            "Entry": "{:,.2f}",
            "EMA20": "{:,.2f}",
            "EMA50": "{:,.2f}",
            "Stop Loss": "{:,.2f}",
            "Target": "{:,.2f}",
            "RSI": "{:,.1f}",
            "rsi_value": "{:,.1f}",
            "ATR": "{:,.2f}",
            "Volume": "{:,.0f}",
            "Avg Volume": "{:,.0f}",
            "volume_value": "{:,.0f}",
            "Volume Strength": "{:,.2f}x",
            "Distance from EMA20 %": "{:,.2f}",
            "distance_from_ema": "{:,.4f}",
            "trend_strength": "{:,.4f}",
            "Confidence Score": "{:.0f}",
            "trend_age": "{:.0f}",
            "Risk ₹": _format_money_value,
            "Suggested Qty": _format_int_value,
            "Suggested Lots": _format_int_value,
            "Reward ₹": _format_money_value,
            "Position Risk %": _format_pct_value,
        },
        na_rep="-",
    )


def format_options_table(data: pd.DataFrame):
    return data.style.format(
        {
            "Strike": "{:,.0f}",
            "Entry Premium (approx)": "{:,.2f}",
            "SL Premium": "{:,.2f}",
            "Target Premium": "{:,.2f}",
        },
        na_rep="-",
    )


def format_active_table(data: pd.DataFrame):
    def pnl_style(row: pd.Series) -> list[str]:
        styles = [""] * len(row.index)
        pnl_value = pd.to_numeric(row.get("P&L %"), errors="coerce")
        if pd.isna(pnl_value):
            return styles
        try:
            pnl_idx = list(row.index).index("P&L %")
        except ValueError:
            return styles
        if pnl_value > 0:
            styles[pnl_idx] = "color: #16803c; font-weight: 700;"
        elif pnl_value < 0:
            styles[pnl_idx] = "color: #c62828; font-weight: 700;"
        return styles

    return data.style.format(
        {
            "Entry": "{:,.2f}",
            "Current Price": "{:,.2f}",
            "P&L %": "{:,.2f}",
            "SL": "{:,.2f}",
            "Target": "{:,.2f}",
            "Confidence": "{:.0f}",
        },
        na_rep="-",
    ).apply(pnl_style, axis=1)


def format_closed_table(data: pd.DataFrame):
    return data.style.format(
        {
            "Entry": "{:,.2f}",
            "Exit": "{:,.2f}",
            "P&L %": "{:,.2f}",
        },
        na_rep="-",
    )


def inject_css() -> None:
    st.markdown(
        """
        <style>
        /* Space below Streamlit fixed header (Share / ⋮ / Deploy on Community Cloud). */
        .block-container {
            padding-top: 4.25rem;
            padding-bottom: 1.1rem;
        }
        h2, h3 {
            margin-top: 0.35rem !important;
            margin-bottom: 0.4rem !important;
            font-size: 1.05rem !important;
        }
        div[data-testid="metric-container"] {
            padding: 0.4rem 0.55rem;
            border: 1px solid rgba(120, 120, 120, 0.22);
            border-radius: 8px;
        }
        div[data-testid="metric-container"] label {
            font-size: 0.72rem !important;
        }
        div[data-testid="metric-container"] [data-testid="stMetricValue"] {
            font-size: 1rem !important;
        }
        div[data-testid="stVerticalBlock"] > div {
            gap: 0.45rem;
        }
        .compact-card {
            border: 1px solid rgba(0, 0, 0, 0.1);
            border-radius: 8px;
            margin-bottom: 8px;
            padding: 8px 10px;
        }
        .long-card {
            background: #edf9f0;
            border-left: 4px solid #16803c;
            color: #12311d;
        }
        .short-card {
            background: #ffefef;
            border-left: 4px solid #c62828;
            color: #421111;
        }
        .selected-focus-card {
            border: 3px solid #1565c0;
            border-radius: 14px;
            margin-bottom: 14px;
            padding: 18px 20px;
            background: linear-gradient(180deg, rgba(21, 101, 192, 0.09) 0%, rgba(255, 255, 255, 0.97) 55%);
            box-shadow: 0 8px 22px rgba(21, 101, 192, 0.14);
        }
        .selected-focus-card .card-stock {
            font-size: 1.2rem;
            margin-bottom: 8px;
        }
        .selected-focus-card .card-line {
            font-size: 0.88rem;
            margin: 4px 0;
        }
        .selected-focus-card .focus-banner {
            font-size: 1.05rem;
            font-weight: 800;
            margin: 10px 0 6px 0;
        }
        .card-stock {
            font-size: 0.95rem;
            font-weight: 800;
            margin-bottom: 5px;
        }
        .card-line {
            font-size: 0.78rem;
            margin: 2px 0;
        }
        .card-score {
            font-size: 0.76rem;
            font-weight: 700;
            margin-top: 3px;
        }
        .top-trade-highlight {
            border-radius: 12px;
            margin-bottom: 8px;
            padding: 14px 16px;
            background: linear-gradient(135deg, #0b2f1a 0%, #1e8f46 100%);
            color: #ffffff;
            border: 1px solid rgba(255, 255, 255, 0.18);
            box-shadow: 0 10px 24px rgba(8, 39, 21, 0.26);
        }
        .top-trade-highlight.short {
            background: linear-gradient(135deg, #441313 0%, #c62828 100%);
            box-shadow: 0 10px 24px rgba(66, 17, 17, 0.25);
        }
        .top-trade-title {
            font-size: 0.8rem;
            font-weight: 700;
            letter-spacing: 0.02em;
            margin-bottom: 6px;
            opacity: 0.95;
        }
        .top-trade-stock {
            font-size: 1.35rem;
            font-weight: 800;
            margin-bottom: 8px;
            line-height: 1.2;
        }
        .top-trade-line {
            font-size: 0.9rem;
            margin: 2px 0;
        }
        @media (max-width: 768px) {
            .block-container {
                padding-top: max(4.5rem, env(safe-area-inset-top, 0px) + 0.5rem);
                padding-left: 0.75rem;
                padding-right: 0.75rem;
                padding-bottom: 1.5rem;
            }
            [data-testid="stHorizontalBlock"] {
                flex-direction: column !important;
                gap: 0.5rem !important;
            }
            [data-testid="stHorizontalBlock"] > div {
                width: 100% !important;
                flex: 1 1 100% !important;
            }
            .card-stock {
                font-size: 0.9rem;
            }
            .card-line,
            .card-score {
                font-size: 0.76rem;
            }
            .top-trade-highlight {
                padding: 11px 12px;
            }
            .top-trade-stock {
                font-size: 1.05rem;
            }
            .top-trade-line {
                font-size: 0.8rem;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _failed_messages(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(item) for item in raw if str(item).strip()]
    if isinstance(raw, str) and raw.strip():
        return [part.strip() for part in raw.split("||") if part.strip()]
    return []


class AIInsightProvider:
    provider_name = "base"

    def generate_trade_insight(self, trade_payload: dict[str, Any]) -> str:
        raise NotImplementedError


def _env_or_secret(name: str, default: str = "") -> str:
    env_val = os.getenv(name)
    if isinstance(env_val, str) and env_val.strip():
        return env_val.strip()
    try:
        secret_val = st.secrets.get(name)  # type: ignore[attr-defined]
        if isinstance(secret_val, str) and secret_val.strip():
            return secret_val.strip()
    except Exception:
        pass
    return default


class OpenAIInsightProvider(AIInsightProvider):
    provider_name = "openai"

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self.api_key = str(api_key or _env_or_secret("OPENAI_API_KEY", OPENAI_API_KEY)).strip()
        self.model = str(model or _env_or_secret("OPENAI_MODEL", OPENAI_MODEL)).strip() or "gpt-5-mini"

    def generate_trade_insight(self, trade_payload: dict[str, Any]) -> str:
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY not configured.")
        instructions = (
            "You are an advisory trade analyst. You must never change signals, SL, target, confidence, or exits. "
            "You are commentary only.\n\n"
            "Generate exactly these sections:\n"
            "1. Trade Thesis\n"
            "2. Strengths\n"
            "3. Risks\n"
            "4. Market Context\n"
            "5. Derivative Suitability\n"
            "6. Risk Management Notes\n\n"
            "Rules:\n"
            "- No guaranteed profit language.\n"
            "- No exact price prediction.\n"
            "- No instructions to modify SL/target or signal."
        )
        url = "https://api.openai.com/v1/responses"
        body = {
            "model": self.model,
            "input": [
                {"role": "system", "content": [{"type": "input_text", "text": instructions}]},
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": f"Payload:\n{json.dumps(trade_payload, sort_keys=True, ensure_ascii=True)}",
                        }
                    ],
                },
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(url, headers=headers, json=body, timeout=25)
            if not response.ok:
                raise RuntimeError(f"OpenAI request failed with HTTP {response.status_code}.")
            payload = response.json()
            text = str(payload.get("output_text", "") or "").strip()
            if not text:
                raise RuntimeError("OpenAI returned empty text.")
            return text
        except Exception as exc:
            raise RuntimeError(f"OpenAI exception: {exc}") from exc


class GeminiInsightProvider(AIInsightProvider):
    provider_name = "gemini"

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self.api_key = str(api_key or _env_or_secret("GEMINI_API_KEY", GEMINI_API_KEY)).strip()
        self.model = str(model or _env_or_secret("GEMINI_MODEL", GEMINI_MODEL)).strip() or "gemini-1.5-flash"

    def generate_trade_insight(self, trade_payload: dict[str, Any]) -> str:
        if not self.api_key:
            raise RuntimeError("Gemini API key not configured.")
        prompt = (
            "You are an advisory trade analyst. You must never change signals, SL, target, confidence, or exits. "
            "You are commentary only.\n\n"
            "Use the provided JSON payload and generate exactly these sections:\n"
            "1. Trade Thesis\n"
            "2. Strengths\n"
            "3. Risks\n"
            "4. Market Context\n"
            "5. Derivative Suitability\n"
            "6. Risk Management Notes\n\n"
            "Rules:\n"
            "- No guaranteed profit language.\n"
            "- No exact price prediction.\n"
            "- No instructions to modify SL/target or signal.\n\n"
            f"Payload:\n{json.dumps(trade_payload, sort_keys=True, ensure_ascii=True)}"
        )
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
            f"?key={self.api_key}"
        )
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        try:
            response = requests.post(url, json=body, timeout=25)
            if not response.ok:
                raise RuntimeError(f"Gemini request failed with HTTP {response.status_code}.")
            payload = response.json()
            candidates = payload.get("candidates") or []
            if not candidates:
                raise RuntimeError("Gemini returned no candidates.")
            parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
            text = "\n".join(str(part.get("text", "")).strip() for part in parts if str(part.get("text", "")).strip())
            if not text:
                raise RuntimeError("Gemini returned empty text.")
            return text
        except Exception as exc:
            raise RuntimeError(f"Gemini exception: {exc}") from exc


def get_default_ai_insight_provider() -> AIInsightProvider:
    return GeminiInsightProvider()


class FinancialSentimentProvider:
    provider_name = "financial_sentiment_base"

    def generate_sentiment(self, sentiment_payload: dict[str, Any]) -> str:
        raise NotImplementedError


class GeminiSentimentProvider(FinancialSentimentProvider):
    provider_name = "gemini_sentiment"

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self.api_key = str(api_key or _env_or_secret("GEMINI_API_KEY", GEMINI_API_KEY)).strip()
        self.model = str(model or _env_or_secret("GEMINI_MODEL", GEMINI_MODEL)).strip() or "gemini-1.5-flash"

    def generate_sentiment(self, sentiment_payload: dict[str, Any]) -> str:
        if not self.api_key:
            raise RuntimeError("Gemini API key not configured.")
        prompt = (
            "You are an advisory-only market sentiment assistant.\n"
            "Never change signals, stop-loss, target, confidence, or exits.\n"
            "Return concise output in this exact structure:\n"
            "Sentiment: bullish|neutral|bearish\n\n"
            "AI Summary:\n"
            "- trade thesis\n"
            "- recent sentiment\n"
            "- momentum context\n\n"
            "Key Risks:\n"
            "- ...\n\n"
            "Top Headlines:\n"
            "- optional, if unavailable say not available.\n\n"
            "No guaranteed returns. No exact price prediction.\n\n"
            f"Payload:\n{json.dumps(sentiment_payload, sort_keys=True, ensure_ascii=True)}"
        )
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
            f"?key={self.api_key}"
        )
        body = {"contents": [{"parts": [{"text": prompt}]}]}
        try:
            response = requests.post(url, json=body, timeout=25)
            if not response.ok:
                raise RuntimeError(f"Gemini request failed with HTTP {response.status_code}.")
            payload = response.json()
            candidates = payload.get("candidates") or []
            if not candidates:
                raise RuntimeError("Gemini returned no candidates.")
            parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
            text = "\n".join(str(part.get("text", "")).strip() for part in parts if str(part.get("text", "")).strip())
            if not text:
                raise RuntimeError("Gemini returned empty text.")
            return text
        except Exception as exc:
            raise RuntimeError(f"Gemini exception: {exc}") from exc


def get_default_financial_sentiment_provider() -> FinancialSentimentProvider:
    return GeminiSentimentProvider()


def _ai_payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        ts = pd.to_datetime(value, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def get_cached_ai_insight(
    connection: sqlite3.Connection,
    symbol: str,
    signal_type: str,
    payload_hash: str,
    provider_name: str,
    ttl_minutes: int = 30,
) -> sqlite3.Row | None:
    row = connection.execute(
        """
        SELECT *
        FROM ai_trade_insights
        WHERE symbol = ? AND signal_type = ? AND payload_hash = ? AND provider_name = ?
        ORDER BY generated_timestamp DESC
        LIMIT 1
        """,
        (str(symbol).upper(), str(signal_type), str(payload_hash), str(provider_name)),
    ).fetchone()
    if row is None:
        return None
    generated_at = _parse_timestamp(row["generated_timestamp"])
    if generated_at is None:
        return None
    if datetime.now() - generated_at > timedelta(minutes=max(1, int(ttl_minutes))):
        return None
    return row


def get_latest_ai_insight(
    connection: sqlite3.Connection,
    symbol: str,
    provider_name: str,
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT *
        FROM ai_trade_insights
        WHERE symbol = ? AND provider_name = ?
        ORDER BY generated_timestamp DESC
        LIMIT 1
        """,
        (str(symbol).upper(), str(provider_name)),
    ).fetchone()


def save_ai_insight(
    connection: sqlite3.Connection,
    symbol: str,
    signal_type: str,
    payload_hash: str,
    provider_name: str,
    insight_text: str,
) -> sqlite3.Row:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    connection.execute(
        """
        INSERT INTO ai_trade_insights (
            symbol, signal_type, payload_hash, generated_timestamp, insight_text, provider_name
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (str(symbol).upper(), str(signal_type), str(payload_hash), ts, str(insight_text), str(provider_name)),
    )
    return connection.execute(
        """
        SELECT *
        FROM ai_trade_insights
        WHERE symbol = ? AND signal_type = ? AND payload_hash = ? AND provider_name = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (str(symbol).upper(), str(signal_type), str(payload_hash), str(provider_name)),
    ).fetchone()


def generate_or_get_trade_ai_insight(
    connection: sqlite3.Connection,
    provider: AIInsightProvider,
    trade_payload: dict[str, Any],
    force_refresh: bool = False,
) -> dict[str, Any]:
    symbol = str(trade_payload.get("symbol", "")).upper()
    signal_type = str(trade_payload.get("signal_type", "UNKNOWN"))
    payload_hash = _ai_payload_hash(trade_payload)
    provider_name = str(getattr(provider, "provider_name", "unknown"))
    cached = None if force_refresh else get_cached_ai_insight(
        connection,
        symbol=symbol,
        signal_type=signal_type,
        payload_hash=payload_hash,
        provider_name=provider_name,
        ttl_minutes=30,
    )
    if cached is not None:
        return {
            "cached": True,
            "insight_text": str(cached["insight_text"]),
            "generated_timestamp": str(cached["generated_timestamp"]),
            "provider_name": str(cached["provider_name"]),
            "payload_hash": payload_hash,
        }
    insight_text = provider.generate_trade_insight(trade_payload)
    saved = save_ai_insight(
        connection,
        symbol=symbol,
        signal_type=signal_type,
        payload_hash=payload_hash,
        provider_name=provider_name,
        insight_text=insight_text,
    )
    return {
        "cached": False,
        "insight_text": str(saved["insight_text"]),
        "generated_timestamp": str(saved["generated_timestamp"]),
        "provider_name": str(saved["provider_name"]),
        "payload_hash": payload_hash,
    }


def _insider_summary_for_payload(stock: str) -> str:
    detail = insider_detail_for_stock(stock)
    return (
        f"activity={detail.get('activity', 'NO_DATA')}; "
        f"net_qty={detail.get('net_qty', 'NA')}; "
        f"last_date={detail.get('last_date', 'NA')}"
    )


def build_ai_trade_payload(
    symbol: str,
    signal_type: str,
    market_regime: dict[str, Any] | None,
    confidence: float,
    trend_stage: str,
    move_speed: str,
    expected_hold: str,
    theta_risk: str,
    entry_price: float,
    stop_loss: float,
    target: float,
    current_price: float,
    rsi: float,
    atr: float,
    atr_expansion: float,
    volume_spike: float,
    ema_alignment: str,
    regime_alignment: str,
    insider_summary: str,
    instrument_recommendation: str,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "signal_type": signal_type,
        "market_regime": (market_regime or {}).get("market_type", "UNKNOWN"),
        "confidence": confidence,
        "trend_stage": trend_stage,
        "move_speed": move_speed,
        "expected_hold": expected_hold,
        "theta_risk": theta_risk,
        "entry_price": entry_price,
        "stop_loss": stop_loss,
        "target": target,
        "current_price": current_price,
        "rsi": rsi,
        "atr": atr,
        "atr_expansion": atr_expansion,
        "volume_spike": volume_spike,
        "ema_alignment": ema_alignment,
        "regime_alignment": regime_alignment,
        "insider_summary": insider_summary,
        "instrument_recommendation": instrument_recommendation,
        "future_placeholders": {
            "news_sentiment": "NOT_IMPLEMENTED",
            "option_chain_sentiment": "NOT_IMPLEMENTED",
            "sector_analysis": "NOT_IMPLEMENTED",
            "earnings_risk": "NOT_IMPLEMENTED",
        },
    }


def build_ai_sentiment_payload(
    symbol: str,
    signal_type: str,
    market_regime: dict[str, Any] | None,
    trend_stage: str,
    insider_summary: str,
    momentum_context: str,
    confidence: float,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "signal_type": signal_type,
        "market_regime": (market_regime or {}).get("market_type", "UNKNOWN"),
        "regime_direction": (market_regime or {}).get("direction", "UNKNOWN"),
        "trend_stage": trend_stage,
        "insider_summary": insider_summary,
        "momentum_context": momentum_context,
        "confidence": confidence,
    }


def render_ai_insight_panel(
    connection: sqlite3.Connection,
    provider: AIInsightProvider,
    payload: dict[str, Any],
    panel_key: str,
) -> dict[str, Any]:
    symbol = str(payload.get("symbol", "")).upper()
    latest = get_latest_ai_insight(connection, symbol, str(provider.provider_name))
    label = "🔄 Refresh AI Insight" if latest is not None else "🧠 Generate AI Insight"
    with st.expander("🧠 AI Trade Insight", expanded=False):
        run_generate = st.button(label, key=f"ai_btn_{panel_key}", use_container_width=True)
        insight_row = None
        insight_result = None
        if run_generate:
            try:
                insight_result = generate_or_get_trade_ai_insight(
                    connection=connection,
                    provider=provider,
                    trade_payload=payload,
                    force_refresh=(latest is not None),
                )
                connection.commit()
            except Exception as exc:
                st.warning(f"AI insight refresh failed: {exc}")
        if insight_result is not None:
            if insight_result["cached"]:
                st.caption("Using cached AI insight")
            insight_row = insight_result
        else:
            cached = get_cached_ai_insight(
                connection=connection,
                symbol=symbol,
                signal_type=str(payload.get("signal_type", "UNKNOWN")),
                payload_hash=_ai_payload_hash(payload),
                provider_name=str(provider.provider_name),
                ttl_minutes=30,
            )
            if cached is not None:
                st.caption("Using cached AI insight")
                insight_row = {
                    "insight_text": str(cached["insight_text"]),
                    "generated_timestamp": str(cached["generated_timestamp"]),
                    "provider_name": str(cached["provider_name"]),
                }
            elif latest is not None:
                insight_row = {
                    "insight_text": str(latest["insight_text"]),
                    "generated_timestamp": str(latest["generated_timestamp"]),
                    "provider_name": str(latest["provider_name"]),
                }
        if insight_row is None:
            st.caption("No cached AI insight yet. Click refresh to generate.")
            return {"generated": bool(run_generate), "shown": False}
        st.caption(
            f"Generated: {insight_row['generated_timestamp']} · Provider: {insight_row['provider_name']}"
        )
        text_html = html.escape(str(insight_row["insight_text"])).replace("\n", "<br>")
        st.markdown(
            f"""
            <div style="
                max-height: 260px;
                overflow-y: auto;
                border: 1px solid rgba(148, 163, 184, 0.45);
                border-radius: 10px;
                padding: 10px 12px;
                background: rgba(248, 250, 252, 0.75);
                line-height: 1.45;
                font-size: 0.9rem;
            ">{text_html}</div>
            """,
            unsafe_allow_html=True,
        )
        return {"generated": bool(run_generate), "shown": True}


def render_ai_sentiment_panel(
    connection: sqlite3.Connection,
    provider: FinancialSentimentProvider,
    payload: dict[str, Any],
    panel_key: str,
) -> None:
    symbol = str(payload.get("symbol", "")).upper()
    signal_type = str(payload.get("signal_type", "UNKNOWN"))
    payload_hash = _ai_payload_hash(payload)
    provider_name = str(getattr(provider, "provider_name", "financial_sentiment_base"))

    with st.expander("📰 AI News & Sentiment", expanded=False):
        run_generate = st.button("🧠 Refresh AI Sentiment", key=f"ai_sent_{panel_key}", use_container_width=True)
        result_row: dict[str, Any] | None = None
        latest = get_latest_ai_insight(connection, symbol, provider_name)
        if run_generate:
            try:
                sentiment_text = provider.generate_sentiment(payload)
                saved = save_ai_insight(
                    connection=connection,
                    symbol=symbol,
                    signal_type=signal_type,
                    payload_hash=payload_hash,
                    provider_name=provider_name,
                    insight_text=sentiment_text,
                )
                connection.commit()
                result_row = {
                    "insight_text": str(saved["insight_text"]),
                    "generated_timestamp": str(saved["generated_timestamp"]),
                    "provider_name": str(saved["provider_name"]),
                }
            except Exception as exc:
                st.warning(f"AI sentiment refresh failed: {exc}")
                if latest is not None:
                    result_row = {
                        "insight_text": str(latest["insight_text"]),
                        "generated_timestamp": str(latest["generated_timestamp"]),
                        "provider_name": str(latest["provider_name"]),
                    }
        else:
            cached = get_cached_ai_insight(
                connection=connection,
                symbol=symbol,
                signal_type=signal_type,
                payload_hash=payload_hash,
                provider_name=provider_name,
                ttl_minutes=30,
            )
            if cached is not None:
                st.caption("Using cached AI sentiment")
                result_row = {
                    "insight_text": str(cached["insight_text"]),
                    "generated_timestamp": str(cached["generated_timestamp"]),
                    "provider_name": str(cached["provider_name"]),
                }
            elif latest is not None:
                result_row = {
                    "insight_text": str(latest["insight_text"]),
                    "generated_timestamp": str(latest["generated_timestamp"]),
                    "provider_name": str(latest["provider_name"]),
                }
        if result_row is None:
            st.caption("No cached AI sentiment yet. Click refresh to generate.")
            return
        st.caption(
            f"Generated: {result_row['generated_timestamp']} · Provider: {result_row['provider_name']}"
        )
        text_html = html.escape(str(result_row["insight_text"])).replace("\n", "<br>")
        st.markdown(
            f"""
            <div style="
                max-height: 240px;
                overflow-y: auto;
                border: 1px solid rgba(148, 163, 184, 0.45);
                border-radius: 10px;
                padding: 10px 12px;
                background: rgba(248, 250, 252, 0.75);
                line-height: 1.45;
                font-size: 0.9rem;
            ">{text_html}</div>
            """,
            unsafe_allow_html=True,
        )


def mark_selected_trade_ai_bootstrap_done(connection: sqlite3.Connection, stock: str) -> None:
    connection.execute(
        """
        UPDATE selected_trades
        SET ai_auto_insight_done = 1
        WHERE stock = ?
        """,
        (str(stock).upper(),),
    )


def _derive_ema_alignment(signal_type: str, current_price: float, ema20: float, ema50: float) -> str:
    direction = signal_direction(signal_type)
    if any(np.isnan(v) for v in (current_price, ema20, ema50)):
        return "UNKNOWN"
    if direction == "LONG":
        if current_price > ema20 >= ema50:
            return "BULLISH_STACKED"
        if current_price > ema20:
            return "BULLISH_PARTIAL"
        return "BULLISH_WEAK"
    if direction == "SHORT":
        if current_price < ema20 <= ema50:
            return "BEARISH_STACKED"
        if current_price < ema20:
            return "BEARISH_PARTIAL"
        return "BEARISH_WEAK"
    return "NEUTRAL"


def _derive_regime_alignment(signal_type: str, market_regime: dict[str, Any] | None) -> str:
    direction = signal_direction(signal_type)
    regime_direction = str((market_regime or {}).get("direction", "UNKNOWN")).upper()
    if direction not in {"LONG", "SHORT"}:
        return "UNKNOWN"
    if (direction == "LONG" and regime_direction == "BULLISH") or (
        direction == "SHORT" and regime_direction == "BEARISH"
    ):
        return "ALIGNED"
    if regime_direction in {"SIDEWAYS", "UNKNOWN"}:
        return "NEUTRAL"
    return "COUNTER_TREND"


def _safe_float(value: Any, default: float = 0.0) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    return float(numeric) if pd.notna(numeric) else float(default)


def derivative_selection_payload(row: pd.Series, signal_type: str) -> dict[str, Any]:
    return {
        "symbol": str(row.get("Stock", "")).upper(),
        "signal_type": signal_type,
        "confidence": int(_safe_float(row.get("Confidence Score"), 0)),
        "trend_strength": _safe_float(row.get("trend_strength"), 0.0),
        "signal_metadata": {
            "signal": str(row.get("Signal", "")),
            "strategy_type": str(row.get("Signal Type", "")),
            "trend_stage": str(row.get("trend_stage", "")),
            "trend_watch_direction": str(row.get("trend_watch_direction", "")),
            "entry": _safe_float(row.get("Entry"), 0.0),
            "current_price": _safe_float(row.get("Current Price"), 0.0),
            "atr": _safe_float(row.get("ATR"), 0.0),
            "volume_strength": _safe_float(row.get("Volume Strength"), 0.0),
            "rsi": _safe_float(row.get("RSI"), 50.0),
            "ema20": _safe_float(row.get("EMA20"), 0.0),
            "ema50": _safe_float(row.get("EMA50"), 0.0),
            "distance_from_ema20_pct": _safe_float(row.get("Distance from EMA20 %"), 0.0),
            "structure_break": bool(row.get("Structure Break", False)),
            "market_structure": str(row.get("Market Structure", "")),
        },
    }


def derivative_move_speed_label(score: float) -> str:
    if score >= 3.5:
        return "FAST"
    if score >= 2.2:
        return "MEDIUM"
    return "SLOW"


def derivative_hold_bucket(speed: str, trend_strength: float) -> str:
    if speed == "FAST":
        return "Intraday" if trend_strength < 0.02 else "1-2 days"
    if speed == "MEDIUM":
        return "1-2 days" if trend_strength < 0.03 else "3-5 days"
    return "3-5 days" if trend_strength < 0.04 else "1+ week"


def derivative_theta_risk_label(hold_bucket: str) -> str:
    high_buckets = {"3-5 days", "1+ week"}
    if hold_bucket in high_buckets:
        return "HIGH"
    if hold_bucket == "1-2 days":
        return "MEDIUM"
    return "LOW"


def derive_instrument_recommendation(speed: str, trend_direction: str) -> dict[str, str]:
    suffix = "CE" if trend_direction == "LONG" else "PE"
    if speed == "FAST":
        return {
            "best": f"ATM {suffix} option",
            "alternate": f"Slight ITM {suffix} option",
            "avoid": "Deep ITM option / Futures lag move",
        }
    if speed == "MEDIUM":
        return {
            "best": f"ITM {suffix} option",
            "alternate": "Futures",
            "avoid": f"Far OTM {suffix}",
        }
    return {
        "best": "Futures",
        "alternate": f"Deep ITM {suffix}",
        "avoid": f"ATM {suffix}",
    }


def trading_days_until_expiry(current_date: datetime | None = None, expiry: datetime.date | None = None) -> int:
    current = current_date or datetime.now()
    expiry_date = expiry or monthly_expiry_date(current)
    start = current.date()
    if expiry_date < start:
        return 0
    days = pd.bdate_range(start=start, end=expiry_date)
    return max(0, len(days) - 1)


def derive_expiry_recommendation(
    hold_bucket: str,
    trend_direction: str,
    current_date: datetime | None = None,
) -> dict[str, Any]:
    now = current_date or datetime.now()
    expiry = monthly_expiry_date(now)
    trading_days_left = trading_days_until_expiry(now, expiry)
    suffix = "CE" if trend_direction == "LONG" else "PE"
    near_expiry = trading_days_left <= 3

    if near_expiry:
        return {
            "monthly_expiry": expiry.isoformat(),
            "trading_days_to_expiry": trading_days_left,
            "recommended_expiry_bucket": "Avoid new option buying near expiry",
            "expiry_risk": "HIGH",
            "recommendation": {
                "best": "Futures or wait",
                "alternate": "Wait for post-expiry setup",
                "avoid": "New ATM / ITM option buying",
            },
            "warning": "Within 3 trading days of monthly expiry: avoid fresh option buying due to accelerated theta decay.",
        }

    if hold_bucket in {"Intraday", "1-2 days"}:
        return {
            "monthly_expiry": expiry.isoformat(),
            "trading_days_to_expiry": trading_days_left,
            "recommended_expiry_bucket": "Nearest expiry acceptable",
            "expiry_risk": "LOW" if trading_days_left > 5 else "MEDIUM",
            "recommendation": {
                "best": f"ATM / slight ITM {suffix} option",
                "alternate": "Futures",
                "avoid": f"Far OTM {suffix}",
            },
            "warning": "Nearest expiry is acceptable because it is more than 3 trading days away.",
        }

    if hold_bucket == "3-5 days":
        return {
            "monthly_expiry": expiry.isoformat(),
            "trading_days_to_expiry": trading_days_left,
            "recommended_expiry_bucket": "Next monthly expiry",
            "expiry_risk": "MEDIUM",
            "recommendation": {
                "best": f"ITM {suffix} option or Futures",
                "alternate": "Next monthly expiry option",
                "avoid": "Weekly / near-expiry option buying",
            },
            "warning": "For a 3-5 day hold, avoid weekly or near-expiry option buying; prefer ITM option or futures.",
        }

    return {
        "monthly_expiry": expiry.isoformat(),
        "trading_days_to_expiry": trading_days_left,
        "recommended_expiry_bucket": "Next monthly / farther monthly expiry",
        "expiry_risk": "HIGH",
        "recommendation": {
            "best": "Futures",
            "alternate": f"Next monthly deep ITM {suffix}",
            "avoid": f"ATM {suffix}",
        },
        "warning": "For a 1+ week hold, prefer futures or next monthly deep ITM option; avoid ATM options.",
    }


def _is_bearish_reversal_candle(data: pd.DataFrame) -> bool:
    if len(data) < 2:
        return False
    latest = data.iloc[-1]
    previous = data.iloc[-2]
    open_price = float(latest["Open"])
    close = float(latest["Close"])
    high = float(latest["High"])
    low = float(latest["Low"])
    prev_open = float(previous["Open"])
    prev_close = float(previous["Close"])
    body = abs(close - open_price)
    candle_range = max(high - low, 1e-9)
    upper_wick = high - max(open_price, close)
    bearish_engulfing = close < open_price and open_price >= prev_close and close <= prev_open
    shooting_star = close < open_price and upper_wick >= body * 1.5 and body / candle_range <= 0.45
    return bool(bearish_engulfing or shooting_star)


def _is_bullish_reversal_candle(data: pd.DataFrame) -> bool:
    if len(data) < 2:
        return False
    latest = data.iloc[-1]
    previous = data.iloc[-2]
    open_price = float(latest["Open"])
    close = float(latest["Close"])
    high = float(latest["High"])
    low = float(latest["Low"])
    prev_open = float(previous["Open"])
    prev_close = float(previous["Close"])
    body = abs(close - open_price)
    candle_range = max(high - low, 1e-9)
    lower_wick = min(open_price, close) - low
    bullish_engulfing = close > open_price and open_price <= prev_close and close >= prev_open
    hammer = close > open_price and lower_wick >= body * 1.5 and body / candle_range <= 0.45
    return bool(bullish_engulfing or hammer)


def entry_timing_status_from_15m(data: pd.DataFrame, direction: str) -> dict[str, Any]:
    empty = {
        "entry_timing_status": "AVOID",
        "confirmation_note": "15m confirmation unavailable",
        "price_15m": np.nan,
        "ema20_15m": np.nan,
        "ema50_15m": np.nan,
        "rsi_15m": np.nan,
        "atr_15m": np.nan,
        "bearish_reversal_15m": False,
        "bullish_reversal_15m": False,
    }
    if data.empty or direction not in {"LONG", "SHORT"}:
        return empty

    enriched = add_indicators(data, StrategyConfig()).dropna()
    if enriched.empty:
        return empty

    latest = enriched.iloc[-1]
    close = float(latest["Close"])
    ema20 = float(latest["EMA20"])
    ema50 = float(latest["EMA50"])
    rsi = float(latest["RSI"])
    atr = float(latest["ATR"])
    distance_from_ema20 = abs(close - ema20) / ema20 if ema20 > 0 else np.inf
    bearish_reversal = _is_bearish_reversal_candle(enriched)
    bullish_reversal = _is_bullish_reversal_candle(enriched)

    if direction == "LONG":
        if bearish_reversal or close < ema50 or rsi < 45:
            status = "AVOID"
            note = "15m bearish reversal or trend weakness"
        elif close > ema20 and rsi > 50:
            status = "READY" if distance_from_ema20 <= 0.015 else "LATE_ENTRY"
            note = "15m long confirmation active" if status == "READY" else "15m long confirmation is extended from EMA20"
        else:
            status = "WAIT_FOR_PULLBACK"
            note = "Wait for price above EMA20 with RSI > 50"
    else:
        if bullish_reversal or close > ema50 or rsi > 55:
            status = "AVOID"
            note = "15m bullish reversal or trend weakness"
        elif close < ema20 and rsi < 50:
            status = "READY" if distance_from_ema20 <= 0.015 else "LATE_ENTRY"
            note = "15m short confirmation active" if status == "READY" else "15m short confirmation is extended from EMA20"
        else:
            status = "WAIT_FOR_PULLBACK"
            note = "Wait for price below EMA20 with RSI < 50"

    return {
        "entry_timing_status": status,
        "confirmation_note": note,
        "price_15m": close,
        "ema20_15m": ema20,
        "ema50_15m": ema50,
        "rsi_15m": rsi,
        "atr_15m": atr,
        "bearish_reversal_15m": bearish_reversal,
        "bullish_reversal_15m": bullish_reversal,
    }


def fetch_fno_15m_confirmations(symbols: list[str], signals: pd.DataFrame) -> dict[str, dict[str, Any]]:
    cleaned = [str(symbol).upper() for symbol in symbols if str(symbol).strip()]
    if not cleaned:
        return {}
    try:
        market_data = get_market_data(tuple(cleaned), interval="15m", period="5d")
    except Exception as exc:
        logger.warning("F&O 15m confirmation fetch failed: %s", exc, exc_info=True)
        return {
            symbol: {
                "entry_timing_status": "AVOID",
                "confirmation_note": f"15m confirmation fetch failed: {exc}",
                "price_15m": np.nan,
                "ema20_15m": np.nan,
                "ema50_15m": np.nan,
                "rsi_15m": np.nan,
                "atr_15m": np.nan,
                "bearish_reversal_15m": False,
                "bullish_reversal_15m": False,
            }
            for symbol in cleaned
        }

    confirmations: dict[str, dict[str, Any]] = {}
    for symbol in cleaned:
        row = signals_row_for_stock(signals, symbol)
        direction = signal_direction(str(row.get("Signal", ""))) if row is not None else "WAIT"
        history = extract_symbol_history(market_data, symbol, len(cleaned))
        confirmations[symbol] = entry_timing_status_from_15m(history, direction)
    return confirmations


def run_derivative_analysis(selected: dict[str, Any], signals: pd.DataFrame) -> dict[str, Any]:
    symbol = str(selected.get("symbol", "")).upper()
    row = signals_row_for_stock(signals, symbol)
    metadata = dict(selected.get("signal_metadata", {}))
    if row is not None:
        metadata = derivative_selection_payload(row, str(selected.get("signal_type", "UNKNOWN"))).get("signal_metadata", {})
    trend_direction = signal_direction(str(metadata.get("signal", "")))
    trend_direction = trend_direction if trend_direction in {"LONG", "SHORT"} else "WAIT"
    atr = _safe_float(metadata.get("atr"), 0.0)
    current_price = _safe_float(metadata.get("current_price"), 0.0)
    volume_strength = _safe_float(metadata.get("volume_strength"), 1.0)
    rsi = _safe_float(metadata.get("rsi"), 50.0)
    ema20 = _safe_float(metadata.get("ema20"), 0.0)
    ema50 = _safe_float(metadata.get("ema50"), 0.0)
    distance_pct = abs(_safe_float(metadata.get("distance_from_ema20_pct"), 0.0))
    structure_break = bool(metadata.get("structure_break", False))
    trend_strength = _safe_float(selected.get("trend_strength"), _safe_float(metadata.get("trend_strength"), 0.0))

    atr_expansion = (atr / current_price) if current_price > 0 else 0.0
    rsi_acceleration = abs(rsi - 50.0) / 50.0
    ema_slope = abs((ema20 - ema50) / ema50) if ema50 > 0 else 0.0
    breakout_strength = (1.0 if structure_break else 0.0) + min(distance_pct / 5.0, 1.0)
    volume_spike = max(volume_strength - 1.0, 0.0)

    score = (
        min(atr_expansion / 0.02, 1.0)
        + min(volume_spike / 1.0, 1.0)
        + min(rsi_acceleration / 0.35, 1.0)
        + min(ema_slope / 0.03, 1.0)
        + min(breakout_strength / 2.0, 1.0)
    )

    if trend_direction == "WAIT":
        move_speed = "SIDEWAYS"
    else:
        move_speed = derivative_move_speed_label(score)
    hold_bucket = derivative_hold_bucket(move_speed, trend_strength) if move_speed != "SIDEWAYS" else "1+ week"
    theta_risk = derivative_theta_risk_label(hold_bucket)

    recommendation = (
        {"best": "Avoid option buying", "alternate": "Wait for directional setup", "avoid": "ATM option buying"}
        if move_speed == "SIDEWAYS"
        else derive_instrument_recommendation(move_speed, trend_direction)
    )
    expiry_view = derive_expiry_recommendation(hold_bucket, trend_direction)
    if move_speed != "SIDEWAYS":
        recommendation = expiry_view["recommendation"]

    volatility_expanded = atr_expansion > 0.018 or volume_strength >= 1.8
    warnings: list[str] = []
    if expiry_view["warning"]:
        warnings.append(f"⚠️ {expiry_view['warning']}")
    if theta_risk == "HIGH":
        warnings.append("⚠️ ATM option buying risky due to time decay")
    if volatility_expanded:
        warnings.append("⚠️ Option premium may already be expensive")
    if move_speed == "SIDEWAYS":
        warnings.append("⚠️ Sideways setup can erode option premiums without directional follow-through")

    return {
        "symbol": symbol,
        "signal_type": str(selected.get("signal_type", "UNKNOWN")),
        "confidence": int(_safe_float(selected.get("confidence"), 0)),
        "trend_direction": trend_direction,
        "move_speed": move_speed,
        "hold_bucket": hold_bucket,
        "theta_risk": theta_risk,
        "monthly_expiry": expiry_view["monthly_expiry"],
        "trading_days_to_expiry": expiry_view["trading_days_to_expiry"],
        "recommended_expiry_bucket": expiry_view["recommended_expiry_bucket"],
        "expiry_risk": expiry_view["expiry_risk"],
        "expiry_warning": expiry_view["warning"],
        "recommendation": recommendation,
        "warnings": warnings,
    }


def render_derivative_stock_action(
    row: pd.Series,
    signal_type: str,
    key: str,
    label: str = "📈",
    use_container_width: bool = True,
) -> None:
    if st.button(
        label,
        key=key,
        help=f"Analyze {row.get('Stock', 'stock')} for F&O",
        use_container_width=use_container_width,
    ):
        with get_db_connection() as conn:
            init_db(conn)
            inserted = insert_selected_fno_from_row(conn, row, signal_type)
            conn.commit()
        if inserted:
            st.toast(f"{row.get('Stock', 'Stock')} added to F&O analysis", icon="📈")
        else:
            st.toast(f"{row.get('Stock', 'Stock')} is already in F&O analysis", icon="📈")
        st.rerun()


def _signal_row_colors(row: pd.Series) -> tuple[str, str, str]:
    signal = str(row.get("Signal", "")).upper()
    direction = signal_direction(signal)
    strength = signal_strength(signal)
    if direction == "LONG":
        return (
            "#d8f0dd" if strength == "STRONG" else "#f1fbf4",
            "#14331d",
            "#2d8a45",
        )
    if direction == "SHORT":
        return (
            "#f7d9d9" if strength == "STRONG" else "#fff5f5",
            "#421111",
            "#c62828",
        )
    return "#f7f7f8", "#2f3136", "#b8bcc4"


def _render_signal_cell(
    value: Any,
    row: pd.Series,
    column: str,
    emphasized: bool = False,
    position: str = "middle",
) -> None:
    bg, fg, border = _signal_row_colors(row)
    weight = "800" if emphasized else "600"
    text = html.escape(_format_trend_watch_cell(value, column))
    radius = {
        "first": "8px 0 0 8px",
        "last": "0 8px 8px 0",
        "only": "8px",
    }.get(position, "0")
    left_border = f"3px solid {border}" if position in {"first", "only"} else "0"
    st.markdown(
        f"""
        <div style="
            min-height: 42px;
            padding: 8px 10px;
            margin: 1px 0;
            background: {bg};
            color: {fg};
            border-top: 1px solid rgba(31, 41, 55, 0.08);
            border-bottom: 1px solid rgba(31, 41, 55, 0.08);
            border-right: 1px solid rgba(31, 41, 55, 0.06);
            border-left: {left_border};
            border-radius: {radius};
            box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.55);
            font-weight: {weight};
            font-size: 0.84rem;
            line-height: 1.2;
            overflow-wrap: anywhere;
        ">{text}</div>
        """,
        unsafe_allow_html=True,
    )


def render_select_trade_action(row: pd.Series, key: str) -> None:
    eligible = (
        is_trade_signal(str(row.get("Signal", "")))
        and str(row.get("trend_stage", "")).upper() != "EMERGING"
    )
    if st.button(
        "⭐",
        key=key,
        help=f"Select {row.get('Stock', 'stock')}",
        use_container_width=True,
        disabled=not eligible,
    ):
        with get_db_connection() as wconn:
            init_db(wconn)
            inserted = insert_selected_trade_from_row(wconn, row)
            wconn.commit()
        if inserted:
            st.toast(f"{row['Stock']} added to selected trades", icon="⭐")
        else:
            st.toast("Already selected or invalid row", icon="ℹ️")
        st.rerun()


def render_why_trade_action(row: pd.Series, key_suffix: str) -> None:
    # Streamlit popover does not expose key=; keep uniqueness via invisible suffix only.
    invisible_suffix = "\u200b" * max(1, min(len(str(key_suffix)), 24))
    _why_label = "ℹ️" + invisible_suffix
    with st.popover(_why_label, help="Why this trade?"):
        st.markdown("**Why this trade?**")
        render_trade_explain_body(row)


def _format_trend_watch_cell(value: Any, column: str) -> str:
    if pd.isna(value):
        return ""
    if column in {"Entry", "Current Price", "Stop Loss", "Target", "RSI", "EMA20", "Risk ₹", "Reward ₹"}:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric):
            return f"{float(numeric):.2f}"
    if column == "Position Risk %":
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric):
            return f"{float(numeric):.2f}%"
    if column in {"trend_age", "Confidence Score", "Suggested Qty", "Suggested Lots"}:
        numeric = pd.to_numeric(value, errors="coerce")
        if pd.notna(numeric):
            return str(int(numeric))
    return str(value)


def render_trend_action_table(
    data: pd.DataFrame,
    table_cols: list[str],
    signal_type: str,
    key_prefix: str,
    max_rows: int = 10,
) -> None:
    visible = data.head(max_rows).copy()
    if visible.empty:
        return

    headers = table_cols + ["F&O"]
    weights = [1.25, 1.2, 1.45, 1.05, 0.75, 1.7, 1.0, 0.75, 0.95, 0.45]
    col_weights = weights[: len(headers)]

    header_cols = st.columns(col_weights, gap="small")
    for col, label in zip(header_cols, headers):
        col.markdown(f"**{label}**")

    for idx, (_, row) in enumerate(visible.iterrows()):
        row_cols = st.columns(col_weights, gap=None, vertical_alignment="center")
        for cell_idx, (col_obj, column_name) in enumerate(zip(row_cols[:-1], table_cols)):
            with col_obj:
                _render_signal_cell(
                    row.get(column_name),
                    row,
                    column_name,
                    emphasized=column_name in {"Stock", "Signal"},
                    position=(
                        "only"
                        if len(table_cols) == 1
                        else "first"
                        if cell_idx == 0
                        else "last"
                        if cell_idx == len(table_cols) - 1
                        else "middle"
                    ),
                )
        with row_cols[-1]:
            render_derivative_stock_action(
                row=row,
                signal_type=signal_type,
                key=f"{key_prefix}_{idx}",
            )

    if len(data) > max_rows:
        st.caption(f"Showing first {max_rows} of {len(data)} rows.")


def render_colored_signal_table(data: pd.DataFrame, table_cols: list[str], max_rows: int = 10) -> None:
    visible = data.head(max_rows).copy()
    if visible.empty:
        return

    weights = [1.25, 1.2, 1.45, 1.05, 0.75, 1.7, 1.0, 0.75, 0.95]
    col_weights = weights[: len(table_cols)]
    header_cols = st.columns(col_weights, gap="small")
    for col, label in zip(header_cols, table_cols):
        col.markdown(f"**{label}**")

    for _, row in visible.iterrows():
        row_cols = st.columns(col_weights, gap=None, vertical_alignment="center")
        for cell_idx, (col_obj, column_name) in enumerate(zip(row_cols, table_cols)):
            with col_obj:
                _render_signal_cell(
                    row.get(column_name),
                    row,
                    column_name,
                    emphasized=column_name in {"Stock", "Signal"},
                    position=(
                        "only"
                        if len(table_cols) == 1
                        else "first"
                        if cell_idx == 0
                        else "last"
                        if cell_idx == len(table_cols) - 1
                        else "middle"
                    ),
                )

    if len(data) > max_rows:
        st.caption(f"Showing first {max_rows} of {len(data)} rows.")


def render_trade_action_table(data: pd.DataFrame, side: str) -> None:
    if data.empty:
        st.info("No long setups today" if side == "LONG" else "No short setups today")
        return

    base_cols = [
        ("Stock", "Stock"),
        ("Signal", "Signal"),
        ("Entry", "Entry"),
        ("Current", "Current Price"),
        ("SL", "Stop Loss"),
        ("Target", "Target"),
        ("RSI", "RSI"),
        ("Score", "Confidence Score"),
        ("Quality", "Trade Quality"),
    ]
    present_base = [(label, col) for label, col in base_cols if col in data.columns]
    headers = [label for label, _ in present_base] + ["Actions"]
    weights = [1.35, 1.12, 0.82, 0.88, 0.82, 0.82, 0.58, 0.52, 0.72, 1.35]
    col_weights = weights[: len(headers)]

    header_cols = st.columns(col_weights, gap="small")
    for col, label in zip(header_cols, headers):
        col.markdown(f"**{label}**")

    for idx, (_, row) in enumerate(data.iterrows()):
        row_cols = st.columns(col_weights, gap="small", vertical_alignment="center")
        for cell_idx, (col_obj, (header_label, column_name)) in enumerate(zip(row_cols[:-1], present_base)):
            with col_obj:
                _render_signal_cell(
                    row.get(column_name),
                    row,
                    column_name,
                    emphasized=header_label in {"Stock", "Signal"},
                    position=(
                        "only"
                        if len(present_base) == 1
                        else "first"
                        if cell_idx == 0
                        else "last"
                        if cell_idx == len(present_base) - 1
                        else "middle"
                    ),
                )
        with row_cols[-1]:
            a1, a2, a3 = st.columns([1, 1, 1], gap="small")
            stable_id = f"{side}_{idx}_{str(row.get('Stock', 'NA')).upper().replace('.', '_')}"
            with a1:
                render_select_trade_action(row, key=f"select_trade_{stable_id}")
            with a2:
                render_why_trade_action(row, key_suffix=stable_id)
            with a3:
                render_derivative_stock_action(
                    row=row,
                    signal_type=str(row.get("Signal", "UNKNOWN")),
                    key=f"analyze_fno_{stable_id}",
                )


def selected_fno_payload_from_db_row(db_row: sqlite3.Row, signals: pd.DataFrame) -> dict[str, Any]:
    stock = str(db_row["stock"]).upper()
    current_row = signals_row_for_stock(signals, stock)
    signal_type = str(db_row["signal_type"] or "UNKNOWN")
    if current_row is not None:
        return derivative_selection_payload(current_row, str(current_row.get("Signal", signal_type)))
    return {
        "symbol": stock,
        "signal_type": signal_type,
        "confidence": 0,
        "trend_strength": 0.0,
        "signal_metadata": {"signal": signal_type, "current_price": 0.0},
    }


def render_selected_fno_main(signals: pd.DataFrame) -> None:
    with get_db_connection() as conn:
        init_db(conn)
        selected = get_selected_fno_stocks(conn)
    if not selected:
        return

    st.subheader("📈 SELECTED F&O WATCH")
    headers = ["Stock", "Signal", "Current Price", "Move", "Hold", "Theta"]
    weights = [1.2, 1.2, 0.95, 0.9, 0.9, 0.75]
    header_cols = st.columns(weights, gap="small")
    for col, label in zip(header_cols, headers):
        col.markdown(f"**{label}**")
    for db_row in selected:
        payload = selected_fno_payload_from_db_row(db_row, signals)
        analysis = run_derivative_analysis(payload, signals)
        row = signals_row_for_stock(signals, analysis["symbol"])
        row_for_color = row if row is not None else pd.Series({"Signal": payload.get("signal_type", "")})
        values = [
            analysis["symbol"],
            str(payload.get("signal_metadata", {}).get("signal") or payload.get("signal_type", "")),
            _safe_float(payload.get("signal_metadata", {}).get("current_price"), 0.0),
            analysis["move_speed"],
            analysis["hold_bucket"],
            analysis["theta_risk"],
        ]
        row_cols = st.columns(weights, gap=None, vertical_alignment="center")
        for idx, (col, value, column_name) in enumerate(zip(row_cols, values, headers)):
            with col:
                _render_signal_cell(
                    value,
                    row_for_color,
                    "Current Price" if column_name == "Current Price" else column_name,
                    emphasized=idx in {0, 1},
                    position=(
                        "first"
                        if idx == 0
                        else "last"
                        if idx == len(headers) - 1
                        else "middle"
                    ),
                )


def render_derivative_analysis_tab(
    signals: pd.DataFrame,
    market_regime: dict[str, Any] | None = None,
    fno_signals: pd.DataFrame | None = None,
    fno_status: dict[str, Any] | None = None,
) -> None:
    st.subheader("📈 Futures & Options Analysis")
    with get_db_connection() as conn:
        init_db(conn)
        selected_rows = get_selected_fno_stocks(conn)
    if not selected_rows:
        st.info("Select stocks from the main dashboard using the 📈 F&O button.")
        return

    fno_view = fno_signals if isinstance(fno_signals, pd.DataFrame) else signals
    st.caption("Persisted F&O watchlist. Values refresh from selected-stock cache/live snapshot.")
    if fno_status:
        last_epoch = float(fno_status.get("last_refresh_epoch", 0.0) or 0.0)
        last_txt = datetime.fromtimestamp(last_epoch).strftime("%d %b %Y, %H:%M:%S") if last_epoch > 0 else "—"
        source_txt = str(fno_status.get("source", "UNKNOWN"))
        next_in = int(fno_status.get("next_refresh_in_sec", 0) or 0)
        s1, s2, s3 = st.columns(3)
        s1.caption(f"F&O last refresh: {last_txt}")
        s2.caption(f"F&O source: {source_txt}")
        s3.caption(f"Next refresh available in: {max(0, next_in)} sec")
        if bool(fno_status.get("quota_low")):
            st.info("API quota is low. Showing cached F&O data.")
    confirmation_by_symbol = fetch_fno_15m_confirmations([str(row["stock"]) for row in selected_rows], fno_view)
    for idx, db_row in enumerate(selected_rows):
        selected = selected_fno_payload_from_db_row(db_row, fno_view)
        analysis = run_derivative_analysis(selected, fno_view)
        timing = confirmation_by_symbol.get(analysis["symbol"], {})
        st.markdown(f"#### {analysis['symbol']}")

        c1, c2, c3, c4, c5 = st.columns([1.4, 1, 1, 1.15, 0.45])
        c1.metric("Signal", f"{analysis['trend_direction']} · {analysis['signal_type']}")
        c2.metric("Move", analysis["move_speed"])
        c3.metric("Hold", analysis["hold_bucket"])
        c4.metric("Entry Timing", str(timing.get("entry_timing_status", "AVOID")))
        with c5:
            if st.button("✕", key=f"remove_fno_{analysis['symbol']}_{idx}", help="Remove from F&O analysis"):
                with get_db_connection() as conn:
                    init_db(conn)
                    delete_selected_fno_stock(conn, analysis["symbol"])
                    conn.commit()
                st.toast(f"{analysis['symbol']} removed from F&O analysis", icon="🗑️")
                st.rerun()

        inst_col, risk_col, expiry_col = st.columns(3)
        with inst_col:
            st.markdown("**Recommended Instrument**")
            st.markdown(f"✅ **{analysis['recommendation']['best']}**")
            st.markdown(f"⚠️ **{analysis['recommendation']['alternate']}**")
            st.markdown(f"❌ **{analysis['recommendation']['avoid']}**")
        with risk_col:
            st.markdown("**Theta Risk**")
            st.metric("Risk", analysis["theta_risk"])
            st.markdown("**Expiry Risk**")
            st.metric("Risk", analysis["expiry_risk"])
        with expiry_col:
            st.markdown("**Recommended Expiry Bucket**")
            st.markdown(f"**{analysis['recommended_expiry_bucket']}**")
            st.caption(
                f"Monthly expiry: {analysis['monthly_expiry']} · "
                f"Trading days left: {analysis['trading_days_to_expiry']}"
            )
            if analysis["warnings"]:
                for warning in analysis["warnings"]:
                    st.warning(warning)
            else:
                st.success("No major derivative risk warnings detected.")

        st.markdown("**15m Entry Confirmation**")
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("15m Price", f"{_safe_float(timing.get('price_15m'), 0.0):,.2f}" if pd.notna(pd.to_numeric(timing.get("price_15m"), errors="coerce")) else "—")
        t2.metric("15m EMA20", f"{_safe_float(timing.get('ema20_15m'), 0.0):,.2f}" if pd.notna(pd.to_numeric(timing.get("ema20_15m"), errors="coerce")) else "—")
        t3.metric("15m RSI", f"{_safe_float(timing.get('rsi_15m'), 0.0):.1f}" if pd.notna(pd.to_numeric(timing.get("rsi_15m"), errors="coerce")) else "—")
        t4.metric("15m ATR", f"{_safe_float(timing.get('atr_15m'), 0.0):,.2f}" if pd.notna(pd.to_numeric(timing.get("atr_15m"), errors="coerce")) else "—")
        st.caption(str(timing.get("confirmation_note", "15m confirmation unavailable")))
        ai_payload = build_ai_trade_payload(
            symbol=analysis["symbol"],
            signal_type=str(analysis.get("signal_type", selected.get("signal_type", "UNKNOWN"))),
            market_regime=market_regime,
            confidence=_safe_float(selected.get("signal_metadata", {}).get("confidence"), 0.0),
            trend_stage=str(selected.get("signal_metadata", {}).get("trend_stage", "UNKNOWN")),
            move_speed=str(analysis.get("move_speed", "UNKNOWN")),
            expected_hold=str(analysis.get("hold_bucket", "UNKNOWN")),
            theta_risk=str(analysis.get("theta_risk", "UNKNOWN")),
            entry_price=_safe_float(selected.get("signal_metadata", {}).get("entry"), 0.0),
            stop_loss=_safe_float(selected.get("signal_metadata", {}).get("stop_loss"), 0.0),
            target=_safe_float(selected.get("signal_metadata", {}).get("target"), 0.0),
            current_price=_safe_float(selected.get("signal_metadata", {}).get("current_price"), 0.0),
            rsi=_safe_float(selected.get("signal_metadata", {}).get("rsi"), 0.0),
            atr=_safe_float(selected.get("signal_metadata", {}).get("atr"), 0.0),
            atr_expansion=_safe_float(selected.get("signal_metadata", {}).get("atr_expansion"), 1.0),
            volume_spike=_safe_float(selected.get("signal_metadata", {}).get("volume_spike"), 1.0),
            ema_alignment=_derive_ema_alignment(
                str(analysis.get("signal_type", selected.get("signal_type", "UNKNOWN"))),
                _safe_float(selected.get("signal_metadata", {}).get("current_price"), np.nan),
                _safe_float(selected.get("signal_metadata", {}).get("ema20"), np.nan),
                _safe_float(selected.get("signal_metadata", {}).get("ema50"), np.nan),
            ),
            regime_alignment=_derive_regime_alignment(
                str(analysis.get("signal_type", selected.get("signal_type", "UNKNOWN"))),
                market_regime,
            ),
            insider_summary=_insider_summary_for_payload(str(analysis["symbol"])),
            instrument_recommendation=str(analysis["recommendation"]["best"]),
        )
        with get_db_connection() as ai_conn:
            init_db(ai_conn)
            provider = get_default_ai_insight_provider()
            render_ai_insight_panel(
                connection=ai_conn,
                provider=provider,
                payload=ai_payload,
                panel_key=f"fno_{analysis['symbol']}_{idx}",
            )
        st.divider()

def render_selected_trades_focus(signals: pd.DataFrame, market_regime: dict[str, Any] | None = None) -> None:
    st.subheader("⭐ SELECTED TRADES")
    with get_db_connection() as conn:
        init_db(conn)
        selected_rows = get_active_selected_trades(conn)
        if not selected_rows:
            st.caption(
                "No stocks in your focus list. Expand **LONG TRADES** or **SHORT TRADES** and press **⭐** on a row."
            )
            return
        for sel in selected_rows:
            stock = str(sel["stock"]).upper()
            entry = _first_numeric(sel["original_entry_price"], sel["entry_price"])
            sl = _first_numeric(sel["original_stop_loss"], sel["stop_loss"])
            tgt = _first_numeric(sel["original_target"], sel["target"])
            original_signal = str(sel["original_signal_type"] or sel["signal_type"] or "")
            live = signals_row_for_stock(signals, stock)
            direction = signal_direction(original_signal)
            if direction == "WAIT":
                direction = direction_from_stored_signal_type(original_signal)
            live_plan = _selected_trade_live_plan(sel, live)

            cur_price = float(live["Current Price"]) if live is not None else np.nan
            ema50v = np.nan
            rsiv = np.nan
            if live is not None:
                if "EMA50" in live.index and pd.notna(live.get("EMA50")):
                    ema50v = float(live["EMA50"])
                if pd.notna(live.get("RSI")):
                    rsiv = float(live["RSI"])

            pnl_s = "—"
            exit_sig = False
            if direction in {"LONG", "SHORT"} and not np.isnan(cur_price):
                pnl_val = pnl_pct(direction, entry, cur_price)
                if not np.isnan(pnl_val):
                    pnl_s = f"{pnl_val:+.2f}%"
                if not np.isnan(ema50v) and not np.isnan(rsiv):
                    exit_sig = monitoring_exit_triggered(direction, cur_price, ema50v, rsiv)

            if direction in {"LONG", "SHORT"} and not np.isnan(cur_price):
                d_sl, d_tgt = sl_target_distance_pct_of_entry(direction, entry, cur_price, sl, tgt)
            else:
                d_sl, d_tgt = np.nan, np.nan

            price_s = f"{cur_price:,.2f}" if not np.isnan(cur_price) else "—"
            dist_sl_s = f"{d_sl:+.2f}% of entry" if not np.isnan(d_sl) else "—"
            dist_tgt_s = f"{d_tgt:+.2f}% of entry" if not np.isnan(d_tgt) else "—"

            banner_html = (
                '<div class="focus-banner" style="color:#b71c1c;">🚨 EXIT SIGNAL</div>'
                if exit_sig
                else '<div class="focus-banner" style="color:#1b5e20;">✅ HOLD</div>'
            )
            dir_label = direction if direction != "WAIT" else "—"
            live_entry_s = f"{live_plan['entry']:,.2f}" if pd.notna(pd.to_numeric(live_plan["entry"], errors="coerce")) else "—"
            live_sl_s = f"{live_plan['stop_loss']:,.2f}" if pd.notna(pd.to_numeric(live_plan["stop_loss"], errors="coerce")) else "—"
            live_target_s = f"{live_plan['target']:,.2f}" if pd.notna(pd.to_numeric(live_plan["target"], errors="coerce")) else "—"
            changes_html = "".join(f"<div class=\"card-line\">{html.escape(str(change))}</div>" for change in live_plan["changes"])

            st.markdown(
                f"""
                <div class="selected-focus-card">
                    <div class="card-stock">{stock}</div>
                    <div class="card-line">Direction <strong>{dir_label}</strong> · Current <strong>{price_s}</strong> · P&amp;L <strong>{pnl_s}</strong></div>
                    <div class="card-line"><strong>Original Trade Plan</strong></div>
                    <div class="card-line">Entry <strong>{entry:,.2f}</strong> · SL <strong>{sl:,.2f}</strong> · Target <strong>{tgt:,.2f}</strong></div>
                    <div class="card-line">Signal at selection <strong>{html.escape(original_signal or '—')}</strong></div>
                    <div class="card-line"><strong>Current System Recommendation</strong></div>
                    <div class="card-line">Entry <strong>{live_entry_s}</strong> · SL <strong>{live_sl_s}</strong> · Target <strong>{live_target_s}</strong></div>
                    <div class="card-line">Current signal <strong>{html.escape(str(live_plan['signal'] or '—'))}</strong> · Status <strong>{html.escape(str(live_plan['status']))}</strong></div>
                    {changes_html}
                    <div class="card-line">Distance from SL (vs entry %): <strong>{dist_sl_s}</strong></div>
                    <div class="card-line">Distance from target (vs entry %): <strong>{dist_tgt_s}</strong></div>
                    {banner_html}
                </div>
                """,
                unsafe_allow_html=True,
            )
            c1, c2, _sp = st.columns([1, 1, 6])
            with c1:
                if st.button("Remove Trade", key=f"sel_remove_{stock}"):
                    delete_selected_trade(conn, stock)
                    conn.commit()
                    st.rerun()
            with c2:
                if st.button("Mark as Closed", key=f"sel_closed_{stock}"):
                    mark_selected_trade_closed_manual(conn, stock, cur_price if not np.isnan(cur_price) else None)
                    conn.commit()
                    st.rerun()
            signal_for_payload = str(
                live_plan.get("signal")
                or original_signal
                or sel.get("signal_type")
                or "UNKNOWN"
            )
            confidence_val = _safe_float(
                sel["confidence_score"] if "confidence_score" in sel.keys() else np.nan,
                0.0,
            )
            if pd.isna(confidence_val):
                confidence_val = 0.0
            stage_val = str(live.get("trend_stage", "UNKNOWN")) if live is not None else "UNKNOWN"
            move_speed = "UNKNOWN"
            expected_hold = "UNKNOWN"
            theta_risk = "UNKNOWN"
            instrument_reco = "UNKNOWN"
            if live is not None:
                selected_payload = selected_fno_payload_from_db_row(
                    {"stock": stock, "signal_type": signal_for_payload, "signal_metadata": "{}"},
                    signals,
                )
                analysis = run_derivative_analysis(selected_payload, signals)
                move_speed = str(analysis.get("move_speed", "UNKNOWN"))
                expected_hold = str(analysis.get("hold_bucket", "UNKNOWN"))
                theta_risk = str(analysis.get("theta_risk", "UNKNOWN"))
                instrument_reco = str(analysis.get("recommendation", {}).get("best", "UNKNOWN"))
            ai_payload = build_ai_trade_payload(
                symbol=stock,
                signal_type=signal_for_payload,
                market_regime=market_regime,
                confidence=float(confidence_val),
                trend_stage=stage_val,
                move_speed=move_speed,
                expected_hold=expected_hold,
                theta_risk=theta_risk,
                entry_price=_safe_float(entry, 0.0),
                stop_loss=_safe_float(sl, 0.0),
                target=_safe_float(tgt, 0.0),
                current_price=_safe_float(cur_price, 0.0) if not np.isnan(cur_price) else 0.0,
                rsi=_safe_float(rsiv, 0.0) if not np.isnan(rsiv) else 0.0,
                atr=_safe_float(float(live.get("ATR", np.nan)) if live is not None else np.nan, 0.0),
                atr_expansion=_safe_float(float(live.get("ATR Expansion", np.nan)) if live is not None else np.nan, 1.0),
                volume_spike=_safe_float(float(live.get("Volume Spike", np.nan)) if live is not None else np.nan, 1.0),
                ema_alignment=_derive_ema_alignment(
                    signal_for_payload,
                    _safe_float(cur_price, np.nan),
                    _safe_float(float(live.get("EMA20", np.nan)) if live is not None else np.nan, np.nan),
                    _safe_float(ema50v, np.nan),
                ),
                regime_alignment=_derive_regime_alignment(signal_for_payload, market_regime),
                insider_summary=_insider_summary_for_payload(stock),
                instrument_recommendation=instrument_reco,
            )
            provider = get_default_ai_insight_provider()
            render_ai_insight_panel(
                connection=conn,
                provider=provider,
                payload=ai_payload,
                panel_key=f"selected_{stock}",
            )
            momentum_context = (
                f"signal={signal_for_payload}; "
                f"rsi={_safe_float(rsiv, 0.0) if not np.isnan(rsiv) else 0.0:.2f}; "
                f"price={_safe_float(cur_price, 0.0) if not np.isnan(cur_price) else 0.0:.2f}; "
                f"ema50={_safe_float(ema50v, 0.0) if not np.isnan(ema50v) else 0.0:.2f}; "
                f"live_status={live_plan.get('status', 'UNKNOWN')}"
            )
            sentiment_payload = build_ai_sentiment_payload(
                symbol=stock,
                signal_type=signal_for_payload,
                market_regime=market_regime,
                trend_stage=stage_val,
                insider_summary=_insider_summary_for_payload(stock),
                momentum_context=momentum_context,
                confidence=float(confidence_val),
            )
            sentiment_provider = get_default_financial_sentiment_provider()
            render_ai_sentiment_panel(
                connection=conn,
                provider=sentiment_provider,
                payload=sentiment_payload,
                panel_key=f"selected_sent_{stock}",
            )


def render_trade_explain_body(row: pd.Series) -> None:
    stock = str(row.get("Stock", "UNKNOWN"))
    st.markdown(f"**Stock:** {stock}")
    st.markdown(f"**Trend direction:** {row.get('trend_direction', '—')}")
    st.markdown(f"**EMA aligned:** {row.get('ema_condition', '—')}")
    st.markdown(
        f"**RSI:** {float(row.get('rsi_value', row.get('RSI', 0)) or 0):.1f} — {row.get('rsi_condition', '—')}"
    )
    st.markdown(
        f"**Volume:** {float(row.get('volume_value', row.get('Volume', 0)) or 0):,.0f} — {row.get('volume_condition', '—')}"
    )
    st.markdown(
        f"**Distance from EMA20:** {float(row.get('distance_from_ema', 0) or 0) * 100:.2f}% — {row.get('distance_condition', '—')}"
    )
    st.markdown(f"**Strategy:** {row.get('strategy_type', '—')}")
    ins = insider_detail_for_stock(row.get("Stock"))
    st.markdown("**Insider Activity (NSE):**")
    st.markdown(f"- **Promoter activity:** {ins.get('activity', 'NO_DATA')}")
    nq = ins.get("net_qty")
    if nq is not None and isinstance(nq, (int, float)) and not (isinstance(nq, float) and np.isnan(nq)):
        st.markdown(f"- **Net quantity (10d, promoter / promoter group):** {nq:,.0f}")
    else:
        st.markdown("- **Net quantity (10d):** —")
    st.markdown(f"- **Last transaction date:** {ins.get('last_date', '—')}")
    st.caption(str(ins.get("interpretation", "")))
    st.markdown(
        f"**Confidence:** {int(row.get('Confidence Score', 0) or 0)}/5 · **Quality:** {row.get('Trade Quality', '—')}"
    )
    if row.get("trend_stage"):
        st.markdown(
            f"**Trend watch:** {row.get('trend_watch_direction', '—')} · "
            f"stage **{row.get('trend_stage', '—')}** · age **{row.get('trend_age', '—')}** candles"
        )
        if str(row.get("trend_watch_note", "")).strip():
            st.markdown(f"**Note:** {row.get('trend_watch_note')}")
    failures = _failed_messages(row.get("failed_conditions"))
    if failures:
        st.markdown("**Why not perfect:**")
        for item in failures:
            st.markdown(f"- {item}")


def render_trade_cards(data: pd.DataFrame, side: str) -> None:
    if data.empty:
        st.info("No long setups today" if side == "LONG" else "No short setups today")
        return

    cards_per_row = min(4, len(data))
    for start in range(0, len(data), cards_per_row):
        chunk = data.iloc[start : start + cards_per_row]
        columns = st.columns(len(chunk))
        for pos, (column, (_, row)) in enumerate(zip(columns, chunk.iterrows())):
            direction = signal_direction(str(row["Signal"]))
            card_class = "long-card" if direction == "LONG" else "short-card"
            quality = str(row.get("Trade Quality", "—") or "—")

            with column:
                card_col, fno_col, why_col = st.columns([5.2, 0.8, 0.8], gap="small")
                with card_col:
                    st.markdown(
                        f"""
                        <div class="compact-card {card_class}">
                            <div class="card-stock">{"🟢" if direction == "LONG" else "🔴"} {row["Stock"]}</div>
                            <div class="card-line">Entry <strong>{row["Entry"]:,.2f}</strong></div>
                            <div class="card-line">SL <strong>{row["Stop Loss"]:,.2f}</strong></div>
                            <div class="card-line">Target <strong>{row["Target"]:,.2f}</strong></div>
                            <div class="card-score">Conf {int(row["Confidence Score"])}/5 · {signal_strength(str(row["Signal"]))}</div>
                            <div class="card-line">Quality <strong>{quality}</strong></div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                with fno_col:
                    if str(row.get("Signal", "")).upper() in {"STRONG_LONG", "STRONG_SHORT"}:
                        render_derivative_stock_action(
                            row=row,
                            signal_type=str(row.get("Signal", "UNKNOWN")),
                            key=f"analyze_fno_{side}_{start}_{pos}",
                            use_container_width=True,
                        )
                with why_col:
                    # Streamlit 1.34 popover has no `key=`; unique invisible suffix avoids duplicate widget ids.
                    _why_label = "ℹ️" + ("\u200c" * (start * 10 + pos + 1))
                    with st.popover(
                        _why_label,
                        help="Why this trade? (click again or outside to close)",
                    ):
                        st.markdown("**Why this trade?**")
                        render_trade_explain_body(row)
                if is_trade_signal(str(row.get("Signal", ""))) and str(
                    row.get("trend_stage", "")
                ).upper() != "EMERGING":
                    btn_key = f"select_trade_{side}_{start}_{pos}"
                    if st.button("Select Trade", key=btn_key, use_container_width=True):
                        with get_db_connection() as wconn:
                            init_db(wconn)
                            inserted = insert_selected_trade_from_row(wconn, row)
                            wconn.commit()
                        if inserted:
                            st.toast(f"{row['Stock']} added to selected trades", icon="⭐")
                        else:
                            st.toast("Already selected or invalid row", icon="ℹ️")
                        st.rerun()


def render_trade_section(
    title: str,
    futures_trades: pd.DataFrame,
    option_trades: pd.DataFrame,
    side: str,
) -> None:
    side_signals = {
        "LONG": {"STRONG_LONG", "EARLY_LONG", "WEAK_LONG"},
        "SHORT": {"STRONG_SHORT", "EARLY_SHORT", "WEAK_SHORT"},
    }
    allowed = side_signals.get(side.upper(), set())
    futures_trades = futures_trades[futures_trades["Signal"].isin(allowed)].copy() if allowed and not futures_trades.empty else futures_trades
    option_trades = option_trades[
        option_trades["Option Type"].isin(["CE"] if side.upper() == "LONG" else ["PE"])
    ].copy() if not option_trades.empty and side.upper() in {"LONG", "SHORT"} else option_trades

    trade_count = len(futures_trades)

    with st.expander(f"{title} ({trade_count})", expanded=False):
        if futures_trades.empty:
            st.info("No trades available in this section.")
            return

        render_trade_action_table(futures_trades, side)

        with st.expander("Details", expanded=False):
            if not option_trades.empty:
                st.dataframe(format_options_table(option_trades), use_container_width=True, hide_index=True)
            else:
                st.caption("No matching option rows available.")


def render_active_trades(active_history: pd.DataFrame, signals: pd.DataFrame) -> None:
    with st.expander("📊 Active Trades", expanded=False):
        active = active_trades_table(active_history, signals)
        if active.empty:
            st.info("No active trades.")
        else:
            st.dataframe(format_active_table(active), use_container_width=True, hide_index=True)


def render_closed_trades(closed_history: pd.DataFrame) -> None:
    with st.expander("📜 Trade History", expanded=False):
        closed = closed_trades_table(closed_history)
        if closed.empty:
            st.info("No closed trades yet.")
        else:
            st.dataframe(format_closed_table(closed), use_container_width=True, hide_index=True)


def render_exit_signals(closed_history: pd.DataFrame) -> None:
    with st.expander("🚨 Exit Signals", expanded=False):
        closed = closed_trades_table(closed_history)
        if closed.empty:
            st.info("No exit signals.")
        else:
            st.dataframe(format_closed_table(closed), use_container_width=True, hide_index=True)


def render_sidebar(fno_symbols: list[str], option_symbols: list[str]) -> tuple[list[str], StrategyConfig, str, str, bool, RiskSettings]:
    with st.sidebar:
        st.header("Controls")
        source = st.radio(
            "Symbol source",
            ["fno_list.csv", "Manual"],
            index=0 if fno_symbols else 1,
        )

        if source == "Manual":
            manual_symbols = st.text_area("Symbols", height=120, placeholder="RELIANCE, HDFCBANK")
            symbols = parse_manual_symbols(manual_symbols)
        else:
            symbols = fno_symbols

        only_options = st.checkbox(
            "Only option-tradable stocks",
            value=False,
            disabled=not bool(option_symbols),
        )
        if only_options and option_symbols:
            option_set = set(option_symbols)
            symbols = [
                symbol
                for symbol in symbols
                if (clean_underlying_symbol(symbol) or "") in option_set
            ]

        st.caption(f"FNO symbols loaded: {len(fno_symbols)}")
        st.caption(f"Options symbols loaded: {len(option_symbols)}")

        st.divider()
        use_sample_data = st.checkbox(
            "Use sample data",
            value=False,
            help="Use this only to test the dashboard when live market APIs are unavailable or rate-limited.",
        )
        period = "30d"
        interval = "1h"
        st.caption("Swing scan: 30 days of 1-hour candles")
        long_rsi = st.slider("Long RSI threshold", 50, 70, 55)
        short_rsi = st.slider("Short RSI threshold", 30, 50, 45)
        ema_distance_pct = st.slider("EMA distance %", 1.0, 10.0, 3.0, 0.5)
        atr_multiplier = st.slider("ATR multiplier", 0.5, 4.0, 1.5, 0.25)
        target_rr = st.slider("Target RR", 1.0, 5.0, 2.0, 0.5)

        st.divider()
        st.caption("Risk management")
        trading_capital = st.number_input(
            "Trading capital",
            min_value=0.0,
            value=100_000.0,
            step=10_000.0,
            format="%.2f",
        )
        max_risk_per_trade_pct = st.number_input(
            "Max risk per trade %",
            min_value=0.0,
            max_value=100.0,
            value=1.0,
            step=0.25,
            format="%.2f",
        )
        max_active_trades = st.number_input(
            "Max active trades",
            min_value=1,
            value=5,
            step=1,
        )
        max_total_portfolio_risk_pct = st.number_input(
            "Max total portfolio risk %",
            min_value=0.0,
            max_value=100.0,
            value=5.0,
            step=0.5,
            format="%.2f",
        )

        _usage_sidebar = compute_api_usage_display()
        _refresh_blocked = bool(_usage_sidebar.get("refresh_blocked"))

        if st.button(
            "Refresh",
            type="primary",
            use_container_width=True,
            disabled=_refresh_blocked,
            help="Disabled when Alpha Vantage or EODHD per-minute quota is exhausted.",
        ):
            clear_market_data_cache()
            try:
                st.session_state[MARKET_SCAN_NONCE_KEY] = int(st.session_state.get(MARKET_SCAN_NONCE_KEY, 0)) + 1
            except Exception:
                pass
            load_stock_list.clear()
            load_option_list.clear()
            st.rerun()

    config = StrategyConfig(
        long_rsi=float(long_rsi),
        short_rsi=float(short_rsi),
        overextended_max=float(ema_distance_pct) / 100,
        atr_multiplier=float(atr_multiplier),
        target_rr=float(target_rr),
    )
    risk_settings = RiskSettings(
        trading_capital=float(trading_capital),
        max_risk_per_trade_pct=float(max_risk_per_trade_pct),
        max_active_trades=int(max_active_trades),
        max_total_portfolio_risk_pct=float(max_total_portfolio_risk_pct),
    )
    return symbols, config, period, interval, use_sample_data, risk_settings


def render_summary(signals: pd.DataFrame, last_updated: str) -> None:
    strong_count = int(signals["Signal"].isin(["STRONG_LONG", "STRONG_SHORT"]).sum()) if not signals.empty else 0
    weak_count = int(signals["Signal"].isin(["WEAK_LONG", "WEAK_SHORT"]).sum()) if not signals.empty else 0
    wait_count = int((signals["Signal"] == "WAIT").sum()) if not signals.empty else 0

    row_one = st.columns(3)
    row_one[0].metric("Scanned", len(signals))
    row_one[1].metric("Strong", strong_count)
    row_one[2].metric("Watchlist", weak_count)

    row_two = st.columns(2)
    row_two[0].metric("WAIT", wait_count)
    row_two[1].metric("Last updated", last_updated)


def render_performance_dashboard(metrics: dict[str, float | int | str]) -> None:
    with st.expander("📈 Trade Performance", expanded=False):
        win_rate = float(metrics.get("win_rate", 0.0))
        profit_factor = float(metrics.get("profit_factor", 0.0))
        total_pnl = float(metrics.get("total_pnl", 0.0))
        total_trades = int(metrics.get("total_trades", 0))
        avg_profit = float(metrics.get("avg_profit", 0.0))
        avg_loss = float(metrics.get("avg_loss", 0.0))
        expectancy = float(metrics.get("expectancy", 0.0))

        display_profit_factor = "∞" if np.isinf(profit_factor) else f"{profit_factor:.2f}"

        summary_columns = st.columns(3)
        summary_columns[0].metric("Win %", f"{win_rate:.1f}%")
        summary_columns[1].metric("Profit factor", display_profit_factor)
        summary_columns[2].metric("Total P&L %", f"{total_pnl:.2f}")

        detail_columns = st.columns(3)
        detail_columns[0].metric("Total trades", total_trades)
        detail_columns[1].metric("Avg profit/loss %", f"{avg_profit:.2f} / {avg_loss:.2f}")
        detail_columns[2].metric("Expectancy %", f"{expectancy:.2f}")


def render_market_regime(regime: dict[str, str], signals: pd.DataFrame, last_updated: str) -> None:
    strong_count = int(signals["Signal"].isin(["STRONG_LONG", "STRONG_SHORT"]).sum()) if not signals.empty else 0
    watchlist_count = int(
        signals["Signal"].isin(["WEAK_LONG", "WEAK_SHORT", "EARLY_LONG", "EARLY_SHORT"]).sum()
    ) if not signals.empty else 0

    st.subheader("📌 Market Regime")
    row_one = st.columns(3)
    row_one[0].metric("Market Type", regime["market_type"])
    row_one[1].metric("Direction", regime["direction"])
    row_one[2].metric("Strategy", regime["suggested_strategy"])

    row_two = st.columns(3)
    row_two[0].metric("ADX", regime.get("adx", "-") or "-")
    row_two[1].metric("RSI", regime.get("rsi", "-") or "-")
    row_two[2].metric("Last updated", last_updated)

    row_three = st.columns(3)
    row_three[0].metric("Scanned", len(signals))
    row_three[1].metric("Strong", strong_count)
    row_three[2].metric("Watchlist", watchlist_count)

    if regime.get("error"):
        st.warning(f"Market regime unavailable: {regime['error']}")


def render_top_trade_highlight(signals: pd.DataFrame) -> None:
    trade_signals = {"STRONG_LONG", "EARLY_LONG", "WEAK_LONG", "STRONG_SHORT", "EARLY_SHORT", "WEAK_SHORT"}
    if signals.empty or "Signal" not in signals.columns:
        return

    candidates = signals[signals["Signal"].isin(trade_signals)].copy()
    if "trend_stage" in candidates.columns:
        candidates = candidates[candidates["trend_stage"].astype(str).str.upper() != "EMERGING"]
    if candidates.empty:
        st.info("Top Trade Highlight: no actionable trade signal right now.")
        return

    candidates["_priority"] = candidates["Signal"].map(SIGNAL_PRIORITY).fillna(99)
    candidates = candidates.sort_values(
        by=["Confidence Score", "_priority", "Distance from EMA20 %"],
        ascending=[False, True, True],
    )
    top = candidates.iloc[0]
    direction = signal_direction(str(top["Signal"]))
    direction_emoji = "🟢 LONG" if direction == "LONG" else "🔴 SHORT"
    card_class = "" if direction == "LONG" else "short"
    score = int(pd.to_numeric(top.get("Confidence Score"), errors="coerce") or 0)

    st.markdown(
        f"""
        <div class="top-trade-highlight {card_class}">
            <div class="top-trade-title">⭐ TOP TRADE HIGHLIGHT · Score {score}/5 · {direction_emoji}</div>
            <div class="top-trade-stock">{top["Stock"]}</div>
            <div class="top-trade-line">Entry: <strong>{float(top["Entry"]):,.2f}</strong></div>
            <div class="top-trade-line">SL: <strong>{float(top["Stop Loss"]):,.2f}</strong></div>
            <div class="top-trade-line">Target: <strong>{float(top["Target"]):,.2f}</strong></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_scan_status(signals: pd.DataFrame, errors: list[str]) -> None:
    if errors:
        st.error(errors[0])

    if signals.empty:
        st.warning("No market data was available to scan. Check the error above or enable 'Use sample data' in the sidebar.")
        return

    trade_count = int(
        signals["Signal"].isin(
            ["STRONG_LONG", "STRONG_SHORT", "EARLY_LONG", "EARLY_SHORT", "WEAK_LONG", "WEAK_SHORT"]
        ).sum()
    )
    if trade_count == 0:
        st.warning("Market data loaded, but no stocks produced strong or weak trade signals. All scanned symbols are WAIT.")

    with st.expander("Scan diagnostics"):
        st.dataframe(format_futures_table(signals), use_container_width=True, hide_index=True)


def render_runtime_log_tail(lines: int = 80) -> None:
    with st.expander("Runtime logs (latest)", expanded=False):
        try:
            if not LOG_PATH.exists():
                st.caption("No runtime log file yet.")
                return
            content = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = content[-max(1, int(lines)) :]
            if not tail:
                st.caption("Log file is empty.")
                return
            st.code("\n".join(tail), language="text")
        except Exception as exc:
            st.caption(f"Unable to read runtime logs: {exc}")


def render_trend_watch_sections(signals: pd.DataFrame) -> None:
    """Emerging / confirming / strong trend ladder — watchlist only; does not change Signal or filters."""
    st.subheader("Trend watchlist")
    st.caption(
        "Stages are informational. LONG/SHORT signals and filters are unchanged. "
        "EMERGING: no Select Trade and no new rows in the signals history table."
    )
    if signals.empty or "trend_stage" not in signals.columns:
        st.info("No scan data for trend watchlist.")
        return

    table_cols = [
        c
        for c in (
            "Stock",
            "Signal",
            "trend_watch_direction",
            "trend_stage",
            "trend_age",
            "trend_watch_note",
            "Current Price",
            "RSI",
            "EMA20",
        )
        if c in signals.columns
    ]

    st.markdown("#### 🟡 Emerging Trends (Watchlist)")
    em = signals[signals["trend_stage"].astype(str).str.upper() == "EMERGING"].copy()
    if em.empty:
        st.caption("None right now.")
    else:
        render_colored_signal_table(em, table_cols)

    st.markdown("#### 🟠 Confirming Trends")
    cf = signals[signals["trend_stage"].astype(str).str.upper() == "CONFIRMING"].copy()
    if cf.empty:
        st.caption("None right now.")
    else:
        render_trend_action_table(
            data=cf,
            table_cols=table_cols,
            signal_type="TRENDING",
            key_prefix="analyze_fno_trending_confirming",
        )

    st.markdown("#### 🟢 Strong Trades (trend ladder)")
    sg = signals[signals["trend_stage"].astype(str).str.upper() == "STRONG"].copy()
    if sg.empty:
        st.caption("None right now.")
    else:
        st.caption("Trend-stage STRONG (EMA separation + RSI + volume); not the same as STRONG_LONG / STRONG_SHORT alone.")
        render_trend_action_table(
            data=sg,
            table_cols=table_cols,
            signal_type="TRENDING",
            key_prefix="analyze_fno_trending_strong",
        )

    st.markdown("#### 🟢 Early Relative Strength")
    rs_cols = [c for c in ("Stock", "Signal", "Current Price", "RSI", "EMA20", "Relative Strength Score", "Reason") if c in signals.columns]
    rs = signals[signals["Early RS Label"].astype(str).str.upper() == "EARLY_RS_LONG"].copy() if "Early RS Label" in signals.columns else pd.DataFrame()
    if rs.empty:
        st.caption("None right now.")
    else:
        rs["Signal"] = "EARLY_RS_LONG"
        st.caption("Hidden strength watchlist during weak/bearish market. Watchlist-only; not auto-promoted to STRONG_LONG.")
        render_trend_action_table(
            data=rs.sort_values(by=["Relative Strength Score", "RSI"], ascending=[False, False]),
            table_cols=rs_cols,
            signal_type="EARLY_RS_LONG",
            key_prefix="analyze_fno_early_rs",
        )

    st.markdown("#### 🟢 Pre-Event Strength")
    pe_cols = [c for c in ("Stock", "Signal", "Current Price", "RSI", "EMA20", "EMA50", "Event Strength Score", "Reason") if c in signals.columns]
    pe = signals[signals["Pre Event Label"].astype(str).str.upper() == "PRE_EVENT_ACCUMULATION"].copy() if "Pre Event Label" in signals.columns else pd.DataFrame()
    if pe.empty:
        st.caption("None right now.")
    else:
        pe["Signal"] = "PRE_EVENT_ACCUMULATION"
        st.caption("Watchlist-only event accumulation candidates before potential breakout.")
        render_trend_action_table(
            data=pe.sort_values(by=["Event Strength Score", "RSI"], ascending=[False, False]),
            table_cols=pe_cols,
            signal_type="PRE_EVENT_ACCUMULATION",
            key_prefix="analyze_fno_pre_event",
        )

    st.markdown("#### 🔴 Post-Event Risk")
    po_cols = [c for c in ("Stock", "Signal", "Current Price", "Distance from EMA20 %", "ATR", "Event Warning") if c in signals.columns]
    po = signals[signals["Post Event Label"].astype(str).str.upper() == "POST_EVENT_RISK"].copy() if "Post Event Label" in signals.columns else pd.DataFrame()
    if po.empty:
        st.caption("None right now.")
    else:
        po["Signal"] = "POST_EVENT_RISK"
        po["Event Warning"] = "Late entry risk elevated"
        render_trend_action_table(
            data=po.sort_values(by=["Distance from EMA20 %", "ATR"], ascending=[False, False]),
            table_cols=po_cols,
            signal_type="POST_EVENT_RISK",
            key_prefix="analyze_fno_post_event",
        )


def main() -> None:
    configure_runtime_environment()
    logger.info(
        "App session: main() started log_file=%s (set TRADING_DASHBOARD_CONSOLE_LOG=1 for stderr mirror)",
        LOG_PATH,
    )
    st.set_page_config(page_title="Futures Trading Dashboard", layout="wide", initial_sidebar_state="collapsed")
    inject_css()

    fno_symbols, fno_error = load_stock_list()
    option_symbols, options_error = load_option_list()
    symbols, config, period, interval, use_sample_data, risk_settings = render_sidebar(fno_symbols, option_symbols)

    st.subheader("📊 Trading Dashboard")
    last_updated = datetime.now().strftime("%d %b %Y, %H:%M:%S")
    try:
        if MARKET_SCAN_NONCE_KEY not in st.session_state:
            st.session_state[MARKET_SCAN_NONCE_KEY] = 0
    except Exception:
        pass
    render_api_usage_panel()

    if fno_error:
        st.warning(fno_error)
    if options_error:
        st.info(options_error)

    regime = get_market_regime()

    with st.spinner("Scanning market data..."):
        signals, errors = scan_symbols(
            symbols=symbols,
            config=config,
            market_regime=regime,
            period=period,
            interval=interval,
            use_sample_data=use_sample_data,
            market_refresh_nonce=int(st.session_state.get(MARKET_SCAN_NONCE_KEY, 0)),
        )

    _ensure_market_cache_after_scan(symbols, period, interval, use_sample_data)
    render_market_data_cache_status_bar(symbols, period, interval, use_sample_data)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db_connection() as connection:
            init_db(connection)
            active_history = get_active_trades(connection)
            refresh_nse_insider_context(signals, active_history, use_sample_data)
            if not signals.empty:
                signals = sort_futures_table(signals)
            process_exits(connection, signals)
            update_signals(connection, signals, timestamp)
            performance_metrics = refresh_performance_metrics(connection)
            connection.commit()
            active_history = get_active_trades(connection)
            closed_history = get_trade_history(connection)
    except sqlite3.Error as exc:
        logger.exception("Database operation failed")
        st.error(f"Database error: {exc}")
        active_history = empty_history_table()
        closed_history = empty_history_table()
        performance_metrics = default_performance_metrics()
        refresh_nse_insider_context(signals, active_history, use_sample_data)
        if not signals.empty:
            signals = sort_futures_table(signals)

    signals = apply_position_sizing(signals, risk_settings, load_lot_size_map())
    long_trades = ranked_trades(signals, {"STRONG_LONG", "EARLY_LONG", "WEAK_LONG"})
    short_trades = ranked_trades(signals, {"STRONG_SHORT", "EARLY_SHORT", "WEAK_SHORT"})
    wait_trades = ranked_trades(signals, {"WAIT"})
    long_options = build_options_plan(long_trades, option_symbols, config)
    short_options = build_options_plan(short_trades, option_symbols, config)

    print(f"Total stocks scanned: {len(signals)}")
    print(f"LONG signals: {len(long_trades)}")
    print(f"SHORT signals: {len(short_trades)}")
    logger.info("Total stocks scanned: %s", len(signals))
    logger.info("LONG signals: %s", len(long_trades))
    logger.info("SHORT signals: %s", len(short_trades))

    main_tab, derivative_tab = st.tabs(["📊 Main Dashboard", "📈 Futures & Options Analysis"])

    with main_tab:
        render_selected_trades_focus(signals, regime)
        render_selected_fno_main(signals)
        render_top_trade_highlight(signals)
        render_market_regime(regime, signals, last_updated)
        render_scan_status(signals, errors)
        render_trend_watch_sections(signals)
        render_trade_section("🟢 LONG TRADES", long_trades, long_options, "LONG")
        render_trade_section("🔴 SHORT TRADES", short_trades, short_options, "SHORT")
        render_active_trades(active_history, signals)
        render_exit_signals(closed_history)
        render_closed_trades(closed_history)

        with st.expander(f"⏸️ No Trade ({len(wait_trades)})", expanded=False):
            st.dataframe(format_futures_table(wait_trades.head(15)), use_container_width=True, hide_index=True)

        render_performance_dashboard(performance_metrics)

        if errors:
            with st.expander(f"Skipped Symbols / Errors ({len(errors)})"):
                for error in errors[:250]:
                    st.write(error)
        render_runtime_log_tail(100)

    with derivative_tab:
        render_derivative_analysis_tab(signals, regime)


if __name__ == "__main__":
    main()
