from __future__ import annotations

import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import streamlit as st


APP_DIR = Path(__file__).resolve().parent
FNO_LIST_PATH = APP_DIR / "fno_list.csv"
OPTIONS_LIST_PATH = APP_DIR / "options_list.csv"
SIGNALS_DB_PATH = APP_DIR / "signals.db"
LOG_PATH = APP_DIR / "dashboard_errors.log"

OHLCV_COLUMNS = ["Open", "High", "Low", "Close", "Volume"]

FUTURES_COLUMNS = [
    "Stock",
    "Signal",
    "Signal Type",
    "Entry",
    "Current Price",
    "EMA20",
    "Stop Loss",
    "Target",
    "RSI",
    "ATR",
    "Volume",
    "Avg Volume",
    "Distance from EMA20 %",
    "Confidence Score",
    "Exit Signal",
    "Reason",
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
    "TREND_LONG": 3,
    "TREND_SHORT": 3,
    "": 9,
}

SIGNAL_PRIORITY = {
    "STRONG_LONG": 1,
    "STRONG_SHORT": 1,
    "WEAK_LONG": 2,
    "WEAK_SHORT": 2,
    "WAIT": 3,
}

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


@st.cache_data(ttl=300, show_spinner=False)
def download_market_data(symbols: tuple[str, ...], period: str, interval: str) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()

    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt") from exc

    tickers = [to_yahoo_symbol(symbol) for symbol in symbols]
    try:
        data = yf.download(
            tickers=tickers,
            period=period,
            interval=interval,
            auto_adjust=False,
            group_by="ticker",
            progress=False,
            threads=True,
        )
    except Exception as exc:
        raise RuntimeError(f"Yahoo Finance request failed: {exc}") from exc

    if data.empty:
        raise RuntimeError("No data returned from Yahoo Finance")

    if isinstance(data, pd.DataFrame) and data.dropna(how="all").empty:
        raise RuntimeError("Yahoo Finance returned only empty rows")

    return data


@st.cache_data(ttl=300, show_spinner=False)
def fetch_index_data(symbol: str = "^NSEI", period: str = "1y", interval: str = "1d") -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError as exc:
        raise RuntimeError("yfinance is not installed. Run pip install -r requirements.txt") from exc

    try:
        data = yf.download(
            tickers=symbol,
            period=period,
            interval=interval,
            auto_adjust=False,
            progress=False,
            threads=False,
        )
    except Exception as exc:
        raise RuntimeError(f"NIFTY 50 request failed: {exc}") from exc

    if data.empty:
        raise RuntimeError("No NIFTY 50 data returned from Yahoo Finance")

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)

    data = data.rename(columns=str.title)
    missing_columns = [column for column in OHLCV_COLUMNS if column not in data.columns]
    if missing_columns:
        raise RuntimeError(f"NIFTY 50 data missing columns: {', '.join(missing_columns)}")

    data = data[OHLCV_COLUMNS].copy()
    for column in OHLCV_COLUMNS:
        data[column] = pd.to_numeric(data[column], errors="coerce")

    cleaned = data.dropna(subset=["Open", "High", "Low", "Close"])
    if cleaned.empty:
        raise RuntimeError("NIFTY 50 response had no valid OHLC rows")
    return cleaned


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
        raise ValueError("No price data returned")

    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(-1)

    data = data.rename(columns=str.title)
    missing_columns = [column for column in OHLCV_COLUMNS if column not in data.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {', '.join(missing_columns)}")

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
        "adx": "",
        "rsi": "",
        "error": error,
    }


def get_market_regime() -> dict[str, str]:
    try:
        index_data = fetch_index_data()
        config = StrategyConfig()
        data = add_regime_indicators(index_data, config).dropna()
        if data.empty:
            return default_market_regime("Insufficient NIFTY 50 indicator data")

        latest = data.iloc[-1]
        ema20 = float(latest["EMA20"])
        ema50 = float(latest["EMA50"])
        rsi = float(latest["RSI"])
        adx = float(latest["ADX"])

        bullish = ema20 > ema50 and adx > 20 and rsi > 55
        bearish = ema20 < ema50 and adx > 20 and rsi < 45
        sideways = adx < 20

        if bullish:
            market_type = "TRENDING_BULLISH"
            direction = "UP"
            suggested_strategy = "PULLBACK_LONG"
        elif bearish:
            market_type = "TRENDING_BEARISH"
            direction = "DOWN"
            suggested_strategy = "PULLBACK_SHORT"
        elif sideways:
            market_type = "SIDEWAYS"
            direction = "NEUTRAL"
            suggested_strategy = "PULLBACK"
        else:
            market_type = "NEUTRAL"
            direction = "NEUTRAL"
            suggested_strategy = "NO TRADE"

        return {
            "market_type": market_type,
            "direction": direction,
            "suggested_strategy": suggested_strategy,
            "adx": f"{adx:.1f}",
            "rsi": f"{rsi:.1f}",
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
    return signal in {"STRONG_LONG", "WEAK_LONG"}


def is_short_signal(signal: str) -> bool:
    return signal in {"STRONG_SHORT", "WEAK_SHORT"}


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
    except sqlite3.OperationalError as exc:
        logger.exception("Failed to close trade for %s: %s", stock, exc)


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


def update_signals(connection: sqlite3.Connection, signals: pd.DataFrame, timestamp: str) -> None:
    if is_expiry_window():
        return

    if signals.empty:
        return

    for _, signal in signals.iterrows():
        signal_name = str(signal["Signal"])
        if not is_trade_signal(signal_name):
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
    else:
        reason = f"Watchlist: trend aligned, waiting for pullback or breakout trigger"

    exit_signal = ""
    if is_long_signal(signal) and (rsi < 50 or close < ema20):
        exit_signal = "LONG early exit"
    elif is_short_signal(signal) and (rsi > 50 or close > ema20):
        exit_signal = "SHORT early exit"

    return {
        "Stock": symbol,
        "Signal": signal,
        "Signal Type": signal_type,
        "Entry": entry,
        "Current Price": close,
        "EMA20": ema20,
        "Stop Loss": stop_loss,
        "Target": target,
        "RSI": rsi,
        "ATR": atr_value,
        "Volume": volume,
        "Avg Volume": avg_volume,
        "Distance from EMA20 %": distance_from_ema20 * 100,
        "Confidence Score": score,
        "Exit Signal": exit_signal,
        "Reason": reason,
    }


def analyze_stock(
    symbol: str,
    history: pd.DataFrame,
    config: StrategyConfig,
    market_regime: dict[str, str] | None = None,
) -> dict[str, Any] | None:
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
) -> tuple[pd.DataFrame, list[str]]:
    if not symbols:
        return empty_futures_table(), ["No symbols available to scan"]

    rows = []
    errors = []

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
            row = analyze_stock(symbol, history, config, market_regime)
            if row is not None:
                rows.append(row)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
            logger.exception("Skipping %s", symbol)

    if not rows:
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
            "Stop Loss": "{:,.2f}",
            "Target": "{:,.2f}",
            "RSI": "{:,.1f}",
            "ATR": "{:,.2f}",
            "Volume": "{:,.0f}",
            "Avg Volume": "{:,.0f}",
            "Distance from EMA20 %": "{:,.2f}",
            "Confidence Score": "{:.0f}",
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
    )


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
        .block-container {
            padding-top: 1.5rem;
        }
        .trade-card {
            border: 1px solid rgba(0, 0, 0, 0.08);
            border-radius: 8px;
            box-shadow: 0 8px 24px rgba(0, 0, 0, 0.08);
            margin-bottom: 12px;
            min-height: 190px;
            padding: 16px;
        }
        .long-card {
            background: #e8f7ed;
            border-left: 6px solid #16803c;
            color: #102f1b;
        }
        .short-card {
            background: #ffe9e9;
            border-left: 6px solid #c62828;
            color: #421111;
        }
        .pullback-card {
            border-top: 4px solid #d8a600;
            box-shadow: 0 10px 30px rgba(145, 110, 0, 0.18);
        }
        .card-signal {
            font-size: 0.85rem;
            font-weight: 800;
            letter-spacing: 0;
            margin-bottom: 4px;
        }
        .card-stock {
            font-size: 1.35rem;
            font-weight: 800;
            margin-bottom: 6px;
        }
        .card-type {
            background: rgba(255, 255, 255, 0.65);
            border-radius: 8px;
            display: inline-block;
            font-size: 0.78rem;
            font-weight: 800;
            margin-bottom: 10px;
            padding: 3px 8px;
        }
        .card-line {
            font-size: 0.92rem;
            margin: 6px 0;
        }
        .card-score {
            font-size: 0.9rem;
            font-weight: 800;
            margin-top: 14px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_trade_cards(data: pd.DataFrame, side: str) -> None:
    if data.empty:
        st.info("No long setups today" if side == "LONG" else "No short setups today")
        return

    columns = st.columns(min(len(data), 5))
    for column, (_, row) in zip(columns, data.iterrows()):
        direction = signal_direction(str(row["Signal"]))
        card_class = "long-card" if direction == "LONG" else "short-card"
        if str(row["Signal Type"]).startswith("PULLBACK"):
            card_class = f"{card_class} pullback-card"

        with column:
            st.markdown(
                f"""
                <div class="trade-card {card_class}">
                    <div class="card-signal">{direction}</div>
                    <div class="card-stock">{row["Stock"]}</div>
                    <div class="card-type">{signal_strength(str(row["Signal"]))} · {row["Signal Type"]}</div>
                    <div class="card-line">Entry <strong>{row["Entry"]:,.2f}</strong></div>
                    <div class="card-line">SL <strong>{row["Stop Loss"]:,.2f}</strong></div>
                    <div class="card-line">Target <strong>{row["Target"]:,.2f}</strong></div>
                    <div class="card-score">Score {int(row["Confidence Score"])}/5</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_trade_section(
    title: str,
    futures_trades: pd.DataFrame,
    option_trades: pd.DataFrame,
    side: str,
) -> None:
    side_signals = {"LONG": {"STRONG_LONG", "WEAK_LONG"}, "SHORT": {"STRONG_SHORT", "WEAK_SHORT"}}
    allowed = side_signals.get(side.upper(), set())
    futures_trades = futures_trades[futures_trades["Signal"].isin(allowed)].copy() if allowed and not futures_trades.empty else futures_trades
    option_trades = option_trades[
        option_trades["Option Type"].isin(["CE"] if side.upper() == "LONG" else ["PE"])
    ].copy() if not option_trades.empty and side.upper() in {"LONG", "SHORT"} else option_trades

    st.subheader(title)
    render_trade_cards(futures_trades, side)

    futures_column, options_column = st.columns(2)
    with futures_column:
        st.markdown("**Futures**")
        st.dataframe(format_futures_table(futures_trades), use_container_width=True, hide_index=True)

    with options_column:
        st.markdown("**Options**")
        if option_trades.empty:
            st.info("No option-tradable symbols for these futures trades.")
        else:
            st.dataframe(format_options_table(option_trades), use_container_width=True, hide_index=True)


def render_active_trades(active_history: pd.DataFrame, signals: pd.DataFrame) -> None:
    st.subheader("🟢 ACTIVE TRADES")
    active = active_trades_table(active_history, signals)
    if active.empty:
        st.info("No active trades.")
    else:
        st.dataframe(format_active_table(active), use_container_width=True, hide_index=True)


def render_closed_trades(closed_history: pd.DataFrame) -> None:
    st.subheader("📜 TRADE HISTORY")
    closed = closed_trades_table(closed_history)
    if closed.empty:
        st.info("No closed trades yet.")
    else:
        st.dataframe(format_closed_table(closed), use_container_width=True, hide_index=True)


def render_exit_signals(closed_history: pd.DataFrame) -> None:
    st.subheader("🚨 EXIT SIGNALS")
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

        if st.button("Refresh", type="primary", use_container_width=True):
            download_market_data.clear()
            fetch_index_data.clear()
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

    metric_columns = st.columns(5)
    metric_columns[0].metric("Scanned", len(signals))
    metric_columns[1].metric("Strong", strong_count)
    metric_columns[2].metric("Watchlist", weak_count)
    metric_columns[3].metric("WAIT", wait_count)
    metric_columns[4].metric("Last updated", last_updated)


def render_market_regime(regime: dict[str, str]) -> None:
    st.subheader("Market Regime")
    columns = st.columns(5)
    columns[0].metric("Market Type", regime["market_type"])
    columns[1].metric("Direction", regime["direction"])
    columns[2].metric("Strategy", regime["suggested_strategy"])
    columns[3].metric("ADX", regime.get("adx", "-") or "-")
    columns[4].metric("RSI", regime.get("rsi", "-") or "-")
    if regime.get("error"):
        st.warning(f"Market regime unavailable: {regime['error']}")


def render_scan_status(signals: pd.DataFrame, errors: list[str]) -> None:
    if errors:
        st.error(errors[0])

    if signals.empty:
        st.warning("No market data was available to scan. Check the error above or enable 'Use sample data' in the sidebar.")
        return

    trade_count = int(signals["Signal"].isin(["STRONG_LONG", "STRONG_SHORT", "WEAK_LONG", "WEAK_SHORT"]).sum())
    if trade_count == 0:
        st.warning("Market data loaded, but no stocks produced strong or weak trade signals. All scanned symbols are WAIT.")

    with st.expander("Scan diagnostics"):
        st.dataframe(format_futures_table(signals), use_container_width=True, hide_index=True)


def main() -> None:
    configure_runtime_environment()
    st.set_page_config(page_title="Futures Trading Dashboard", layout="wide")
    inject_css()

    fno_symbols, fno_error = load_stock_list()
    option_symbols, options_error = load_option_list()
    symbols, config, period, interval, use_sample_data = render_sidebar(fno_symbols, option_symbols)

    st.title("Futures Trading Dashboard")
    last_updated = datetime.now().strftime("%d %b %Y, %H:%M:%S")
    st.caption(f"Server binding: {DEFAULT_STREAMLIT_HOST}:{DEFAULT_STREAMLIT_PORT}")

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
        )

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with get_db_connection() as connection:
            init_db(connection)
            process_exits(connection, signals)
            update_signals(connection, signals, timestamp)
            connection.commit()
            active_history = get_active_trades(connection)
            closed_history = get_trade_history(connection)
    except sqlite3.Error as exc:
        logger.exception("Database operation failed")
        st.error(f"Database error: {exc}")
        active_history = empty_history_table()
        closed_history = empty_history_table()

    long_trades = ranked_trades(signals, {"STRONG_LONG", "WEAK_LONG"})
    short_trades = ranked_trades(signals, {"STRONG_SHORT", "WEAK_SHORT"})
    wait_trades = ranked_trades(signals, {"WAIT"})
    long_options = build_options_plan(long_trades, option_symbols, config)
    short_options = build_options_plan(short_trades, option_symbols, config)

    print(f"Total stocks scanned: {len(signals)}")
    print(f"LONG signals: {len(long_trades)}")
    print(f"SHORT signals: {len(short_trades)}")
    logger.info("Total stocks scanned: %s", len(signals))
    logger.info("LONG signals: %s", len(long_trades))
    logger.info("SHORT signals: %s", len(short_trades))

    render_market_regime(regime)
    render_summary(signals, last_updated)
    render_scan_status(signals, errors)
    render_active_trades(active_history, signals)
    render_exit_signals(closed_history)
    render_closed_trades(closed_history)
    render_trade_section("🟢 LONG SWING TRADES", long_trades, long_options, "LONG")
    render_trade_section("🔴 SHORT SWING TRADES", short_trades, short_options, "SHORT")

    with st.expander("🔴 No Trade"):
        st.dataframe(format_futures_table(wait_trades), use_container_width=True, hide_index=True)

    if errors:
        with st.expander(f"Skipped Symbols / Errors ({len(errors)})"):
            for error in errors[:250]:
                st.write(error)


if __name__ == "__main__":
    main()
