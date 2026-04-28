from __future__ import annotations

import http.cookiejar
import json
import logging
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import streamlit as st
import streamlit.components.v1 as components

from settings import get_alpha_vantage_api_key, get_eodhd_api_key

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
FNO_LIST_PATH = DATA_DIR / "fno_list.csv"
OPTIONS_LIST_PATH = DATA_DIR / "options_list.csv"
SIGNALS_DB_PATH = APP_DIR / "signals.db"
LOG_PATH = APP_DIR / "dashboard_errors.log"

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
    "WAIT": 4,
}

CACHE_TTL = 60
ALPHA_API_KEY = get_alpha_vantage_api_key()
EODHD_API_KEY = get_eodhd_api_key()

ALPHA_API_MINUTE_LIMIT = 5
ALPHA_API_DAY_LIMIT = 500
EODHD_API_MINUTE_LIMIT = 20
API_USAGE_STATE_KEY = "_api_usage_tracker_v1"
MARKET_SESSION_CACHE_TTL = 90.0
MARKET_FORCE_NEXT_KEY = "_market_force_refresh_next"
MARKET_FETCH_META_KEY = "_market_fetch_meta"
MARKET_QUOTA_FLAG_KEY = "_market_quota_low_flag"
MARKET_SCAN_NONCE_KEY = "_market_scan_refresh_nonce"
# EODHD intraday is one symbol per request; use parallel workers + Session to cut wall time.
EODHD_FALLBACK_MAX_WORKERS = 4
YAHOO_FETCH_CACHE_TTL_SECONDS = CACHE_TTL
YAHOO_MIN_CALL_GAP_SECONDS = 2.0
_YAHOO_FETCH_CACHE: dict[tuple[Any, ...], tuple[float, pd.DataFrame]] = {}
_YAHOO_LAST_CALL_TS = 0.0
_ALPHA_LAST_CALL_TS = 0.0
_EODHD_LAST_CALL_TS = 0.0

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
if not logger.handlers:
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def configure_runtime_environment() -> None:
    # Ensure local writable paths exist for cloud/container environments.
    SIGNALS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # yfinance attempts to use a user cache directory that may be unavailable in cloud sandboxes.
    try:
        import yfinance as yf
    except Exception:
        return

    cache_dir = APP_DIR / ".yfinance_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    set_cache_location = getattr(yf, "set_tz_cache_location", None)
    if callable(set_cache_location):
        try:
            set_cache_location(str(cache_dir))
        except Exception:
            logger.info("Unable to set yfinance cache directory at %s", cache_dir)


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


def to_yahoo_symbol(symbol: str) -> str:
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


def _extract_yahoo_symbol_frame(payload: pd.DataFrame, ticker: str, total: int) -> pd.DataFrame:
    if payload.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    if total == 1 and not isinstance(payload.columns, pd.MultiIndex):
        return _normalize_ohlcv_frame(payload)
    if isinstance(payload.columns, pd.MultiIndex) and ticker in payload.columns.get_level_values(0):
        return _normalize_ohlcv_frame(payload[ticker])
    return pd.DataFrame(columns=OHLCV_COLUMNS)


def _to_alpha_symbol(yahoo_symbol: str) -> str:
    return clean_underlying_symbol(yahoo_symbol) or yahoo_symbol.replace(".NS", "")


def _to_eodhd_symbol(yahoo_symbol: str) -> str:
    base = clean_underlying_symbol(yahoo_symbol) or yahoo_symbol.replace(".NS", "")
    return f"{base}.NSE"


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


def _fetch_alpha_intraday(yahoo_symbol: str, session: requests.Session | None = None) -> pd.DataFrame:
    if not ALPHA_API_KEY:
        return pd.DataFrame(columns=OHLCV_COLUMNS)
    _alpha_wait()
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": _to_alpha_symbol(yahoo_symbol),
        "interval": "15min",
        "outputsize": "compact",
        "apikey": ALPHA_API_KEY,
    }
    sess = session if session is not None else requests
    response = sess.get("https://www.alphavantage.co/query", params=params, timeout=20)
    record_alpha_api_call()
    payload = response.json() if response.ok else {}
    series = payload.get("Time Series (15min)")
    if not isinstance(series, dict) or not series:
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
    return _normalize_ohlcv_frame(rows)


def _fetch_eodhd_intraday_or_daily(
    yahoo_symbol: str,
    session: requests.Session | None = None,
    throttle: bool = True,
) -> tuple[pd.DataFrame, int]:
    if not EODHD_API_KEY:
        return pd.DataFrame(columns=OHLCV_COLUMNS), 0
    eod_symbol = _to_eodhd_symbol(yahoo_symbol)
    sess = session if session is not None else requests
    http_calls = 0

    if throttle:
        _eodhd_wait()
    intraday_url = f"https://eodhd.com/api/intraday/{eod_symbol}"
    intraday_params = {"api_token": EODHD_API_KEY, "interval": "15m", "fmt": "json"}
    intraday_resp = sess.get(intraday_url, params=intraday_params, timeout=20)
    http_calls += 1
    if intraday_resp.ok:
        payload = intraday_resp.json()
        if isinstance(payload, list) and payload:
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
                return frame, http_calls

    if throttle:
        _eodhd_wait()
    daily_url = f"https://eodhd.com/api/eod/{eod_symbol}"
    daily_params = {"api_token": EODHD_API_KEY, "period": "d", "fmt": "json"}
    daily_resp = sess.get(daily_url, params=daily_params, timeout=20)
    http_calls += 1
    if not daily_resp.ok:
        return pd.DataFrame(columns=OHLCV_COLUMNS), http_calls
    payload = daily_resp.json()
    if not isinstance(payload, list) or not payload:
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
    return _normalize_ohlcv_frame(rows), http_calls


def get_market_data(symbols: tuple[str, ...], interval: str = "15m", period: str = "7d") -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()

    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt") from exc

    tickers = tuple(to_yahoo_symbol(symbol) for symbol in symbols)
    cache_key = ("multi_source_market_data", tickers, interval, period)
    cached = _yahoo_cache_get(cache_key)
    if cached is not None:
        print("Cache used")
        return cached

    _yahoo_rate_limit_wait()
    yahoo_payload = pd.DataFrame()
    try:
        yahoo_payload = yf.download(
            tickers=list(tickers),
            interval=interval,
            period=period,
            auto_adjust=False,
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as exc:
        logger.info("Yahoo batch fetch failed: %s", exc)

    frames_by_ticker: dict[str, pd.DataFrame] = {}
    missing: list[str] = []
    for ticker in tickers:
        frame = _extract_yahoo_symbol_frame(yahoo_payload, ticker, len(tickers))
        if frame.empty:
            missing.append(ticker)
        else:
            frames_by_ticker[ticker] = frame

    yahoo_success_count = len(frames_by_ticker)
    alpha_fallback_count = 0
    eodhd_fallback_count = 0

    # Yahoo: yf.download is already multi-ticker. Alpha Vantage TIME_SERIES_INTRADAY is one symbol
    # per request (no official batch for NSE intraday). EODHD intraday is per symbol; we use a
    # shared Session plus limited parallel workers for the EODHD fallback to reduce wall time.

    with requests.Session() as http_session:
        still_missing: list[str] = []
        for ticker in missing:
            try:
                frame = _fetch_alpha_intraday(ticker, http_session)
            except Exception as exc:
                logger.info("Alpha Vantage fetch failed for %s: %s", ticker, exc)
                frame = pd.DataFrame(columns=OHLCV_COLUMNS)
            if frame.empty:
                still_missing.append(ticker)
            else:
                frames_by_ticker[ticker] = frame
                alpha_fallback_count += 1

        def _eodhd_one(ticker: str) -> tuple[str, pd.DataFrame, int]:
            try:
                frame, n_http = _fetch_eodhd_intraday_or_daily(
                    ticker, session=http_session, throttle=False
                )
                return ticker, frame, n_http
            except Exception as exc:
                logger.info("EODHD fetch failed for %s: %s", ticker, exc)
                return ticker, pd.DataFrame(columns=OHLCV_COLUMNS), 0

        if still_missing:
            max_workers = min(EODHD_FALLBACK_MAX_WORKERS, len(still_missing))
            if max_workers <= 1:
                eod_results = [_eodhd_one(t) for t in still_missing]
            else:
                eod_results = []
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_map = {executor.submit(_eodhd_one, t): t for t in still_missing}
                    for fut in as_completed(future_map):
                        try:
                            eod_results.append(fut.result())
                        except Exception as exc:
                            t_failed = future_map[fut]
                            logger.info("EODHD worker failed for %s: %s", t_failed, exc)
                            eod_results.append((t_failed, pd.DataFrame(columns=OHLCV_COLUMNS), 0))
            eod_http_total = 0
            for ticker, frame, n_http in eod_results:
                eod_http_total += int(n_http)
                if frame.empty:
                    continue
                frames_by_ticker[ticker] = frame
                eodhd_fallback_count += 1
            record_eodhd_api_calls(eod_http_total)

    print(f"Yahoo success count: {yahoo_success_count}")
    print(f"Alpha fallback count: {alpha_fallback_count}")
    print(f"EODHD fallback count: {eodhd_fallback_count}")

    if not frames_by_ticker:
        return pd.DataFrame()

    merged = pd.concat(frames_by_ticker, axis=1, sort=True).sort_index()
    _yahoo_cache_set(cache_key, merged)
    return merged


def download_market_data(symbols: tuple[str, ...], period: str, interval: str) -> pd.DataFrame:
    df, _src, _age = get_market_data_with_cache(
        symbols,
        str(period or "7d"),
        str(interval or "15m"),
    )
    return df if isinstance(df, pd.DataFrame) else _safe_empty_market_df()


def fetch_index_data(symbol: str = "^NSEI", period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt") from exc

    ticker = to_yahoo_symbol(symbol) if not str(symbol).startswith("^") else symbol
    cache_key = ("index", ticker, period, interval)
    cached = _yahoo_cache_get(cache_key)
    if cached is not None:
        print("Cache used")
        data = cached
    else:
        def _fetch(use_proxy: bool) -> pd.DataFrame:
            _yahoo_rate_limit_wait()
            if use_proxy:
                print("Proxy fallback")
                return yf.download(
                    tickers=ticker,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )

            print("Direct fetch")
            proxy_keys = (
                "http_proxy",
                "https_proxy",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "all_proxy",
                "ALL_PROXY",
                "socks_proxy",
                "SOCKS_PROXY",
                "socks5_proxy",
                "SOCKS5_PROXY",
                "GIT_HTTP_PROXY",
                "GIT_HTTPS_PROXY",
            )
            previous_env: dict[str, str] = {}
            try:
                for key in proxy_keys:
                    value = os.environ.pop(key, None)
                    if value is not None:
                        previous_env[key] = value
                return yf.download(
                    tickers=ticker,
                    period=period,
                    interval=interval,
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )
            finally:
                for key, value in previous_env.items():
                    os.environ[key] = value

        direct_error: Exception | None = None
        data = pd.DataFrame()
        try:
            data = _fetch(use_proxy=False)
        except Exception as exc:
            direct_error = exc

        if (data.empty or data.dropna(how="all").empty) and direct_error is not None and _is_http_429_error(direct_error):
            try:
                data = _fetch(use_proxy=True)
            except Exception as exc:
                logger.info("Yahoo index proxy fallback failed for %s: %s", symbol, exc)

        if data.empty:
            if direct_error is not None:
                logger.info("Yahoo index direct fetch failed for %s: %s", symbol, direct_error)
            raise RuntimeError(f"No data returned from Yahoo Finance for {symbol}")

        _yahoo_cache_set(cache_key, data)

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data = data.rename(columns=str.title)
    missing_columns = [column for column in OHLCV_COLUMNS if column not in data.columns]
    if missing_columns:
        raise RuntimeError(f"Index data missing columns for {symbol}: {', '.join(missing_columns)}")

    data = data[OHLCV_COLUMNS].copy()
    for column in OHLCV_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    cleaned = data.dropna(subset=["Open", "High", "Low", "Close"])
    if cleaned.empty:
        raise RuntimeError(f"Index response had no valid OHLC rows for {symbol}")
    return cleaned


def _yahoo_cache_get(key: tuple[Any, ...]) -> pd.DataFrame | None:
    entry = _YAHOO_FETCH_CACHE.get(key)
    if not entry:
        return None
    ts, value = entry
    if (time.time() - ts) > YAHOO_FETCH_CACHE_TTL_SECONDS:
        _YAHOO_FETCH_CACHE.pop(key, None)
        return None
    return value.copy()


def _yahoo_cache_set(key: tuple[Any, ...], value: pd.DataFrame) -> None:
    _YAHOO_FETCH_CACHE[key] = (time.time(), value.copy())


def clear_market_data_cache() -> None:
    global _YAHOO_LAST_CALL_TS, _ALPHA_LAST_CALL_TS, _EODHD_LAST_CALL_TS
    _YAHOO_FETCH_CACHE.clear()
    _YAHOO_LAST_CALL_TS = 0.0
    _ALPHA_LAST_CALL_TS = 0.0
    _EODHD_LAST_CALL_TS = 0.0
    try:
        st.session_state["market_cache"] = {"data": None, "timestamp": 0.0, "params": None}
        st.session_state[MARKET_FETCH_META_KEY] = {"source": "FAILED", "age_seconds": 0.0, "ts": time.time()}
    except Exception:
        pass


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
    Short-lived session cache (60s) around get_market_data. Returns (df, source, age_seconds).
    Never raises; never returns None.
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

    quota_low = False
    try:
        if ALPHA_API_KEY:
            usage_view = compute_api_usage_display()
            if int(usage_view.get("alpha_minute_remaining", 99)) <= 1:
                quota_low = True
                st.session_state[MARKET_QUOTA_FLAG_KEY] = True
    except Exception:
        pass

    def _write_meta(source: str, age_seconds: float) -> None:
        try:
            st.session_state[MARKET_FETCH_META_KEY] = {
                "source": source,
                "age_seconds": float(age_seconds),
                "ts": now,
            }
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
            return cached_data.copy(), src, age_from_cache_ts
        _write_meta("FAILED", 0.0)
        return _safe_empty_market_df(), "FAILED", 0.0

    if (
        not force_refresh
        and cached_data is not None
        and isinstance(cached_data, pd.DataFrame)
        and cached_params == params_key
        and (now - cached_ts) < MARKET_SESSION_CACHE_TTL
    ):
        age = max(0.0, now - cached_ts)
        _write_meta("CACHE", age)
        return cached_data.copy(), "CACHE", age

    fresh: pd.DataFrame = _safe_empty_market_df()
    try:
        got = get_market_data(tuple(symbols), interval=interval_s, period=period_s)
        if isinstance(got, pd.DataFrame):
            fresh = got
    except Exception as exc:
        logger.info("get_market_data failed: %s", exc)
        fresh = _safe_empty_market_df()

    if isinstance(fresh, pd.DataFrame) and not fresh.empty:
        cache["data"] = fresh.copy()
        cache["timestamp"] = now
        cache["params"] = params_key
        _write_meta("LIVE", 0.0)
        return fresh.copy(), "LIVE", 0.0

    if cached_data is not None and isinstance(cached_data, pd.DataFrame) and cached_params == params_key and not cached_data.empty:
        _write_meta("STALE_CACHE", age_from_cache_ts)
        return cached_data.copy(), "STALE_CACHE", age_from_cache_ts

    _write_meta("FAILED", 0.0)
    return _safe_empty_market_df(), "FAILED", 0.0


def _ensure_api_usage_state() -> dict[str, Any]:
    if API_USAGE_STATE_KEY not in st.session_state:
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


def record_alpha_api_call() -> None:
    usage = _ensure_api_usage_state()
    _apply_api_usage_window_resets(usage)
    usage["alpha"]["minute_calls"] += 1
    usage["alpha"]["day_calls"] += 1


def record_eodhd_api_calls(count: int) -> None:
    if count <= 0:
        return
    usage = _ensure_api_usage_state()
    _apply_api_usage_window_resets(usage)
    usage["eodhd"]["minute_calls"] += int(count)


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

    return {
        "alpha_minute_used": am,
        "alpha_minute_cap": ALPHA_API_MINUTE_LIMIT,
        "alpha_minute_remaining": max(0, min_rem),
        "alpha_day_used": ad,
        "alpha_day_cap": ALPHA_API_DAY_LIMIT,
        "alpha_day_remaining": max(0, day_rem),
        "alpha_status": alpha_status,
        "alpha_next_safe_sec": alpha_next_safe_sec,
        "eodhd_minute_used": em,
        "eodhd_minute_cap": EODHD_API_MINUTE_LIMIT,
        "eodhd_minute_remaining": max(0, e_rem),
        "eodhd_status": eodhd_status,
        "eodhd_next_safe_sec": eodhd_next_safe_sec,
        "alpha_refresh_blocked": alpha_status == "BLOCKED",
        "refresh_blocked": alpha_status == "BLOCKED" or eodhd_status == "BLOCKED",
    }


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
            st.caption(f"Next minute-bucket reset in ~{int(np.ceil(d['alpha_next_safe_sec']))}s")
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
            st.caption(f"Next minute-bucket reset in ~{int(np.ceil(d['eodhd_next_safe_sec']))}s")
        else:
            st.markdown("**EODHD**")
            st.caption("API key not configured.")

    if d["alpha_status"] in ("WARNING", "BLOCKED") or d["eodhd_status"] in ("WARNING", "BLOCKED"):
        wait_candidates: list[int] = []
        if d["alpha_status"] != "OK":
            wait_candidates.append(int(np.ceil(d["alpha_next_safe_sec"])))
        if d["eodhd_status"] != "OK":
            wait_candidates.append(int(np.ceil(d["eodhd_next_safe_sec"])))
        wait_s = max(1, max(wait_candidates) if wait_candidates else 1)
        st.warning(f"⚠️ Avoid refreshing for **{wait_s}** seconds (API window resets).")


def render_market_data_cache_status_bar() -> None:
    """Market cache: frontend-only live TTL bar, colors, warning blink, reload button (no timer-driven Python reruns)."""
    try:
        mc = st.session_state.get("market_cache") or {}
    except Exception:
        mc = {}
    last_updated = float(mc.get("timestamp") or 0.0)

    try:
        meta = st.session_state.get(MARKET_FETCH_META_KEY) or {}
    except Exception:
        meta = {}
    source = str(meta.get("source", "FAILED"))

    try:
        quota_low = bool(st.session_state.get(MARKET_QUOTA_FLAG_KEY))
    except Exception:
        quota_low = False

    ttl = int(MARKET_SESSION_CACHE_TTL)
    lu_js = json.dumps(last_updated)
    src_js = json.dumps(source)
    quota_js = "true" if quota_low else "false"

    st.markdown("##### Market data")
    html_fragment = """
<div style="font-family:sans-serif;font-size:14px;margin:4px 0 12px 0;">
  <div id="sourceLine" style="font-weight:600;margin-bottom:6px;"></div>
  <div id="quotaLine" style="display:none;margin-bottom:8px;color:#b8860b;"></div>
  <div id="statusText"></div>
  <div style="margin-top:10px;">
    <div style="background:#eee; border-radius:10px; height:12px; overflow:hidden;">
      <div id="progressBar" style="
        height:12px;
        width:100%;
        border-radius:10px;
        transition: width 1s linear, background-color 1s linear;
      "></div>
    </div>
  </div>
  <div id="warningText" style="margin-top:10px; font-weight:bold; min-height:1.2em;"></div>
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
<script>
(function () {
  const TTL = """ + str(ttl) + """;
  const lastUpdated = """ + lu_js + """;
  const source = """ + src_js + """;
  const quotaLow = """ + quota_js + """;

  const sourceLine = document.getElementById("sourceLine");
  const quotaLine = document.getElementById("quotaLine");
  if (source === "LIVE") {
    sourceLine.style.color = "#1b5e20";
    sourceLine.innerText = "✅ Live data fetched";
  } else if (source === "CACHE") {
    sourceLine.style.color = "#b8860b";
    sourceLine.innerText = "⚠️ Showing cached data (API not called)";
  } else if (source === "STALE_CACHE") {
    sourceLine.style.color = "#e65100";
    sourceLine.innerText = "⚠️ API failed, showing last available data";
  } else {
    sourceLine.style.color = "#c62828";
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
    const now = Date.now() / 1000;
    let elapsed = 0;
    if (lastUpdated > 0) {
      elapsed = Math.max(0, now - lastUpdated);
    }
    let remaining = TTL - elapsed;
    if (remaining < 0) remaining = 0;

    const secTick = Math.floor(now);
    if (secTick !== lastSecTick) {
      lastSecTick = secTick;
      if (lastUpdated <= 0) {
        statusEl.innerText = "Last updated: — | Next refresh in: 0 sec";
      } else {
        statusEl.innerText =
          "Last updated: " + Math.floor(elapsed) + " sec ago | Next refresh in: " + Math.floor(remaining) + " sec";
      }
    }

    const percent = TTL > 0 ? (remaining / TTL) * 100 : 0;
    bar.style.width = percent + "%";
    bar.style.backgroundColor = getColor(percent);

    if (remaining <= 15 && lastUpdated > 0) {
      warning.innerText = "⚠️ Refresh available soon!";
      warning.style.color = "red";
      warning.style.visibility = (Math.floor(now) % 2 === 0) ? "visible" : "hidden";
    } else {
      warning.innerText = "";
      warning.style.visibility = "visible";
    }

    if (remaining <= 30) {
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
    components.html(html_fragment, height=260, scrolling=False)


def _yahoo_rate_limit_wait() -> None:
    global _YAHOO_LAST_CALL_TS
    now = time.monotonic()
    elapsed = now - _YAHOO_LAST_CALL_TS
    if elapsed < YAHOO_MIN_CALL_GAP_SECONDS:
        time.sleep(YAHOO_MIN_CALL_GAP_SECONDS - elapsed)
    _YAHOO_LAST_CALL_TS = time.monotonic()


def _is_http_429_error(exc: Exception) -> bool:
    message = str(exc)
    return "429" in message or "Too Many Requests" in message


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
    yahoo_symbol = to_yahoo_symbol(symbol)

    if total_symbols == 1 and not isinstance(market_data.columns, pd.MultiIndex):
        data = market_data.copy()
    elif isinstance(market_data.columns, pd.MultiIndex) and yahoo_symbol in market_data.columns.get_level_values(0):
        data = market_data[yahoo_symbol].copy()
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
    yahoo_symbol = to_yahoo_symbol(symbol)
    payload = get_market_data((symbol,), interval="15m", period="7d")
    return _extract_yahoo_symbol_frame(payload, yahoo_symbol, total=1)


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

        niftybees_data = _fetch_regime_history("NIFTYBEES.NS")
        if not niftybees_data.empty:
            nifty_trend, adx, rsi = classify_index_trend(niftybees_data, config)
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
    if "pnl_percent" not in existing_columns:
        connection.execute("ALTER TABLE signals ADD COLUMN pnl_percent REAL")
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
    connection.execute(
        "CREATE INDEX IF NOT EXISTS idx_selected_status ON selected_trades(status)"
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
                confidence_score, status, exit_reason, exit_price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', '', NULL)
            """,
            (
                data["timestamp"],
                stock,
                data["signal_type"],
                data["entry_price"],
                data["stop_loss"],
                data["target"],
                data["confidence_score"],
            ),
        )
    except sqlite3.OperationalError as exc:
        logger.exception("Failed to insert signal for %s: %s", stock, exc)


def get_active_trades(connection: sqlite3.Connection) -> pd.DataFrame:
    rows = connection.execute(
        """
        SELECT id, timestamp, stock, signal_type, entry_price, stop_loss, target,
               confidence_score, status, exit_reason, exit_price, pnl_percent
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
            SELECT id, signal_type, entry_price
            FROM signals
            WHERE stock = ? AND status = 'ACTIVE'
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(stock).upper(),),
        ).fetchone()
        if active_trade is None:
            return

        direction = signal_direction(str(active_trade["signal_type"]))
        entry_price = float(active_trade["entry_price"])
        pnl_percent = pnl_pct(direction, entry_price, float(exit_price))

        connection.execute(
            """
            UPDATE signals
            SET status = 'CLOSED',
                exit_price = ?,
                pnl_percent = ?,
                exit_reason = ?,
                timestamp = ?
            WHERE id = ?
            """,
            (
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
            SET status = 'CLOSED'
            WHERE stock = ? AND status = 'ACTIVE'
            """,
            (str(stock).upper(),),
        )
    except sqlite3.OperationalError as exc:
        logger.exception("Failed to close trade for %s: %s", stock, exc)


def get_active_selected_trades(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT stock, signal_type, entry_price, stop_loss, target, selected_timestamp, status
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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = connection.execute(
        """
        INSERT OR IGNORE INTO selected_trades (
            stock, signal_type, entry_price, stop_loss, target, selected_timestamp, status
        )
        VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
        """,
        (stock, sig_type, entry, sl, tgt, ts),
    )
    return (cur.rowcount or 0) > 0


def delete_selected_trade(connection: sqlite3.Connection, stock: str) -> None:
    connection.execute("DELETE FROM selected_trades WHERE stock = ?", (str(stock).upper(),))


def mark_selected_trade_closed_manual(connection: sqlite3.Connection, stock: str) -> None:
    connection.execute(
        "UPDATE selected_trades SET status = 'CLOSED' WHERE stock = ?",
        (str(stock).upper(),),
    )


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


def get_trade_history(connection: sqlite3.Connection, limit: int = 20) -> pd.DataFrame:
    rows = connection.execute(
        """
        SELECT id, timestamp, stock, signal_type, entry_price, stop_loss, target,
               confidence_score, status, exit_reason, exit_price, pnl_percent
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
        entry_signal = str(trade["signal_type"])
        direction = signal_direction(entry_signal)
        current_direction = signal_direction(current_signal)
        stop_loss = float(trade["stop_loss"])
        target = float(trade["target"])
        rsi = float(latest.get("RSI", np.nan))
        ema20 = float(latest.get("EMA20", np.nan))
        opened_at = pd.to_datetime(trade["timestamp"], errors="coerce")
        live_pnl = pnl_pct(direction, float(trade["entry_price"]), current_price)

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

    rows = []
    errors = []
    no_data_symbols: list[str] = []

    market_data = pd.DataFrame()
    if not use_sample_data:
        try:
            market_data = download_market_data(tuple(symbols), period, interval)
        except Exception as exc:
            logger.exception("Market data download failed")
            return empty_futures_table(), [
                f"Market data download failed: {exc}. Yahoo Finance may be rate-limited or unavailable."
            ]

    for symbol in symbols:
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

    if not rows:
        if no_data_symbols and not use_sample_data:
            return empty_futures_table(), errors + [
                f"No valid symbol data found: {len(no_data_symbols)} symbols returned no candles from Yahoo/Alpha/EODHD. "
                "Check network/proxy/API keys, or enable 'Use sample data'."
            ]
        return empty_futures_table(), errors or ["No valid symbol data found"]

    return sort_futures_table(pd.DataFrame(rows)), errors


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
        direction = signal_direction(str(trade["signal_type"]))
        entry_price = float(trade["entry_price"])
        current_price = float(current_prices.get(stock, entry_price))
        rows.append(
            {
                "Stock": stock,
                "Direction": direction,
                "Entry": entry_price,
                "Current Price": current_price,
                "P&L %": pnl_pct(direction, entry_price, current_price),
                "SL": float(trade["stop_loss"]),
                "Target": float(trade["target"]),
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
        direction = signal_direction(str(trade["signal_type"]))
        entry_price = float(trade["entry_price"])
        exit_price = float(trade["exit_price"])
        stored_pnl = pd.to_numeric(trade.get("pnl_percent"), errors="coerce")
        final_pnl = float(stored_pnl) if not pd.isna(stored_pnl) else pnl_pct(direction, entry_price, exit_price)
        rows.append(
            {
                "Stock": str(trade["stock"]).upper(),
                "Entry": entry_price,
                "Exit": exit_price,
                "P&L %": final_pnl,
                "Exit Reason": trade["exit_reason"],
            }
        )

    return pd.DataFrame(rows).reindex(columns=CLOSED_COLUMNS)


def highlight_pullback_rows(row: pd.Series) -> list[str]:
    if str(row.get("Signal Type", "")).startswith("PULLBACK"):
        return ["background-color: #fff7cc; color: #2f2600"] * len(row)
    return [""] * len(row)


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


def render_selected_trades_focus(signals: pd.DataFrame) -> None:
    st.subheader("⭐ SELECTED TRADES")
    with get_db_connection() as conn:
        init_db(conn)
        selected_rows = get_active_selected_trades(conn)
        if not selected_rows:
            st.caption(
                "No stocks in your focus list. Expand **LONG TRADES** or **SHORT TRADES** and press **Select Trade** on a card."
            )
            return
        for sel in selected_rows:
            stock = str(sel["stock"]).upper()
            entry = float(sel["entry_price"])
            sl = float(sel["stop_loss"])
            tgt = float(sel["target"])
            live = signals_row_for_stock(signals, stock)
            if live is not None and is_trade_signal(str(live.get("Signal", ""))):
                direction = signal_direction(str(live["Signal"]))
            else:
                direction = direction_from_stored_signal_type(str(sel["signal_type"] or ""))

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

            st.markdown(
                f"""
                <div class="selected-focus-card">
                    <div class="card-stock">{stock}</div>
                    <div class="card-line">Direction <strong>{dir_label}</strong></div>
                    <div class="card-line">Entry <strong>{entry:,.2f}</strong> · Current <strong>{price_s}</strong> · P&amp;L <strong>{pnl_s}</strong></div>
                    <div class="card-line">Stop loss <strong>{sl:,.2f}</strong> · Target <strong>{tgt:,.2f}</strong></div>
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
                    mark_selected_trade_closed_manual(conn, stock)
                    conn.commit()
                    st.rerun()


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
                card_col, why_col = st.columns([5.2, 1], gap="small")
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
    preview_count = 3
    show_all_key = f"show_{side.lower()}_all"
    show_all = bool(st.session_state.get(show_all_key, False))
    visible_trades = futures_trades if show_all else futures_trades.head(preview_count)

    with st.expander(f"{title} ({trade_count})", expanded=False):
        if visible_trades.empty:
            st.info("No trades available in this section.")
            return

        render_trade_cards(visible_trades, side)

        if trade_count > preview_count and not show_all:
            if st.button("Show More", key=f"{show_all_key}_btn"):
                st.session_state[show_all_key] = True
                st.rerun()
        elif trade_count > preview_count and show_all:
            if st.button("Show Less", key=f"{show_all_key}_less_btn"):
                st.session_state[show_all_key] = False
                st.rerun()

        with st.expander("Details", expanded=False):
            st.dataframe(format_futures_table(visible_trades), use_container_width=True, hide_index=True)
            if not option_trades.empty:
                st.dataframe(format_options_table(option_trades), use_container_width=True, hide_index=True)


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


def render_sidebar(fno_symbols: list[str], option_symbols: list[str]) -> tuple[list[str], StrategyConfig, str, str, bool]:
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
            help="Use this only to test the dashboard when Yahoo Finance is blocked or rate-limited.",
        )
        period = "30d"
        interval = "1h"
        st.caption("Swing scan: 30 days of 1-hour candles")
        long_rsi = st.slider("Long RSI threshold", 50, 70, 55)
        short_rsi = st.slider("Short RSI threshold", 30, 50, 45)
        ema_distance_pct = st.slider("EMA distance %", 1.0, 10.0, 3.0, 0.5)
        atr_multiplier = st.slider("ATR multiplier", 0.5, 4.0, 1.5, 0.25)
        target_rr = st.slider("Target RR", 1.0, 5.0, 2.0, 0.5)

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
    return symbols, config, period, interval, use_sample_data


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
        st.dataframe(em[table_cols], use_container_width=True, hide_index=True)

    st.markdown("#### 🟠 Confirming Trends")
    cf = signals[signals["trend_stage"].astype(str).str.upper() == "CONFIRMING"].copy()
    if cf.empty:
        st.caption("None right now.")
    else:
        st.dataframe(cf[table_cols], use_container_width=True, hide_index=True)

    st.markdown("#### 🟢 Strong Trades (trend ladder)")
    sg = signals[signals["trend_stage"].astype(str).str.upper() == "STRONG"].copy()
    if sg.empty:
        st.caption("None right now.")
    else:
        st.caption("Trend-stage STRONG (EMA separation + RSI + volume); not the same as STRONG_LONG / STRONG_SHORT alone.")
        st.dataframe(sg[table_cols], use_container_width=True, hide_index=True)


def main() -> None:
    configure_runtime_environment()
    st.set_page_config(page_title="Futures Trading Dashboard", layout="wide", initial_sidebar_state="collapsed")
    inject_css()

    fno_symbols, fno_error = load_stock_list()
    option_symbols, options_error = load_option_list()
    symbols, config, period, interval, use_sample_data = render_sidebar(fno_symbols, option_symbols)

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

    render_market_data_cache_status_bar()

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

    render_selected_trades_focus(signals)
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


if __name__ == "__main__":
    main()
