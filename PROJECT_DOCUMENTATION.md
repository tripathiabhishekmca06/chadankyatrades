# Trading Dashboard Project Documentation

This document explains the project in knowledge sections: code modules, UI and UX logic, selections, database state, backend flow, market-data handling, trading rules, trend-watch logic, F&O analysis, indicators, and operational notes.

The main implementation lives in `app.py`. Configuration helpers live in `settings.py`.

## 1. Project Purpose

The app is a Streamlit trading dashboard for scanning NSE stocks, ranking long and short trade setups, monitoring selected trades, and running Futures & Options analysis on selected stocks.

The dashboard is designed around these workflows:

1. Load a stock universe from `data/fno_list.csv` or manual sidebar input.
2. Fetch recent OHLCV market data.
3. Calculate technical indicators.
4. Classify each stock as long, short, weak, early, wait, emerging, confirming, or strong trend-watch state.
5. Show trade tables with action buttons.
6. Persist selected trades and selected F&O watchlist stocks in SQLite.
7. Refresh selected F&O stocks from latest scan values on every page load.
8. Track active trades, exits, closed trades, and performance metrics.

## 2. File And Module Structure

### `app.py`

This is the main Streamlit app. It contains:

- Runtime setup and logging.
- Strategy configuration.
- Symbol loading and cleaning.
- Market-data provider integration.
- Cache and API quota handling.
- Indicator calculations.
- Market-regime detection.
- Signal classification.
- SQLite database setup and persistence helpers.
- Trade history, active trade, selected trade, and selected F&O logic.
- Options approximation.
- F&O analysis logic.
- Streamlit UI rendering.
- Main app orchestration.

### `settings.py`

Loads API keys from environment variables or Streamlit secrets:

- `ALPHA_VANTAGE_API_KEY` or `ALPHAVANTAGE_API_KEY`
- `EODHD_API_KEY`

Environment variables take priority. Streamlit secrets are fallback.

### `requirements.txt`

Runtime dependencies:

- `streamlit`
- `pandas`
- `numpy`
- `requests`
- `yfinance`

### `data/fno_list.csv`

Default stock universe. The app expects a `symbol` column.

### Optional `data/options_list.csv`

If present, it is used to determine option-tradable underlyings.

### Runtime Files

The app creates or uses:

- `signals.db`: SQLite database for trades, metrics, selected trades, selected F&O stocks.
- `logs/trading_dashboard.log`: rotating app log.
- `data/api_usage_state.json`: persisted Alpha/EODHD usage counters.
- `data/market_api_meta.json`: persisted last live market-data fetch metadata.
- `data/market_cache_latest.pkl`: persisted market-data snapshot.

## 3. Runtime And Backend Flow

The backend flow starts in `main()`:

1. Configure runtime folders and logging.
2. Set Streamlit page config.
3. Load F&O symbols and option symbols.
4. Render sidebar controls.
5. Render API usage panel.
6. Detect market regime.
7. Scan symbols using selected period and interval.
8. Sync market cache status.
9. Initialize SQLite DB.
10. Refresh NSE insider context.
11. Sort signals.
12. Process exits for active trades.
13. Insert newly active signals into trade history.
14. Refresh performance metrics.
15. Build long, short, and wait trade tables.
16. Build approximate option plans.
17. Render main dashboard tab.
18. Render F&O analysis tab.

The default scan configuration is:

- Period: `30d`
- Interval: `1h`
- Stock list limit: first 50 symbols from `fno_list.csv`

## 4. Configuration Knowledge

`StrategyConfig` controls strategy thresholds:

| Field | Default | Meaning |
| --- | ---: | --- |
| `ema_fast` | 20 | Fast trend EMA |
| `ema_slow` | 50 | Slow trend EMA |
| `rsi_period` | 14 | RSI period |
| `atr_period` | 14 | ATR period |
| `adx_period` | 14 | ADX period for regime |
| `volume_window` | 20 | Rolling average volume window |
| `breakout_window` | 20 | Prior high/low breakout window |
| `trend_strength_min` | 0.005 | Minimum EMA separation strength |
| `overextended_max` | 0.03 | Max distance from EMA20, adjustable in UI |
| `pullback_distance_max` | 0.02 | Reserved pullback distance setting |
| `volume_confirmation_multiplier` | 1.20 | General volume confirmation setting |
| `volume_spike_multiplier` | 1.50 | Breakout volume spike threshold |
| `long_rsi` | 55 | Adjustable long RSI threshold |
| `short_rsi` | 45 | Adjustable short RSI threshold |
| `atr_multiplier` | 1.5 | Stop-loss ATR multiplier |
| `target_rr` | 2.0 | Target risk-reward multiple |
| `option_sl_pct` | 0.30 | Approx option stop-loss percentage |
| `option_target_pct` | 0.60 | Approx option target percentage |
| `swing_lookback` | 2 | Swing high/low detection lookback |

`minimum_rows` requires enough candles for EMA, ATR, volume average, and breakout window.

## 5. Market Data Knowledge

The market-data pipeline attempts multiple providers:

1. Yahoo Finance through `yfinance`.
2. Alpha Vantage for missing data.
3. EODHD for missing data.

Important behavior:

- NSE tickers are normalized with `.NS` where needed.
- Alpha Vantage NSE symbols are converted to `NSE:SYMBOL`.
- EODHD symbols use `SYMBOL.EXCHANGE`; the app probes likely NSE suffixes.
- DNS/network failures are detected and logged.
- API keys are sanitized in errors before logging.
- Provider status messages are shown when data cannot be fetched.

## 6. Market Cache And API Quota Logic

The app avoids unnecessary API calls using:

- Streamlit in-session cache.
- Disk metadata for last live fetch.
- Disk pickle of the latest market-data frame.
- API usage counters for Alpha and EODHD.

Cache TTL:

- `MARKET_SESSION_CACHE_TTL = 60` seconds.

Quota caps used by the app:

- Alpha minute limit: 5
- Alpha day limit: 500
- EODHD minute buffer limit: 20

Refresh behavior:

- The sidebar Refresh button clears market cache and increments a scan nonce.
- Refresh is disabled when Alpha or EODHD quota is blocked.
- The market-data status bar shows cache age and force-refresh availability.

## 7. Database Knowledge

The SQLite DB is `signals.db`. The app uses WAL mode and a busy timeout.

### Table: `signals`

Stores active and closed trade records generated from scan signals.

Columns:

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | INTEGER PK | Trade row id |
| `timestamp` | TEXT | Insert/update timestamp |
| `stock` | TEXT | Stock symbol |
| `signal_type` | TEXT | Signal at entry, such as `STRONG_LONG` |
| `entry_price` | REAL | Entry price |
| `stop_loss` | REAL | Stop-loss price |
| `target` | REAL | Target price |
| `confidence_score` | INTEGER | 0-5 confidence score |
| `status` | TEXT | `ACTIVE` or `CLOSED` |
| `exit_reason` | TEXT | Reason trade closed |
| `exit_price` | REAL | Exit price |
| `pnl_percent` | REAL | Closed P&L percentage |

Indexes:

- `idx_signals_stock`
- `idx_signals_status`
- `idx_signals_active_stock`

Insert rules:

- Only trade signals are inserted.
- `WAIT` rows are ignored.
- `EMERGING` trend-stage rows are ignored.
- No duplicate active trade is inserted for the same stock.
- New inserts are skipped during monthly expiry window.

### Table: `performance_metrics`

Stores one summary row with id `1`.

Columns:

| Column | Meaning |
| --- | --- |
| `updated_at` | Last metrics refresh |
| `total_trades` | Closed trade count |
| `wins` | Count of positive P&L trades |
| `losses` | Count of negative P&L trades |
| `win_rate` | Win percentage |
| `avg_profit` | Average positive P&L |
| `avg_loss` | Average negative P&L |
| `expectancy` | Total P&L divided by trade count |
| `profit_factor` | Gross profit divided by absolute gross loss |
| `total_pnl` | Sum of closed P&L |

### Table: `selected_trades`

Stores user-selected focus trades.

Columns:

| Column | Meaning |
| --- | --- |
| `stock` | Primary key |
| `signal_type` | Stored strategy/signal type |
| `entry_price` | Entry at selection time |
| `stop_loss` | Stop loss at selection time |
| `target` | Target at selection time |
| `selected_timestamp` | Selection time |
| `status` | `ACTIVE` or `CLOSED` |

Selection rules:

- Only valid trade signals can be selected.
- `EMERGING` rows cannot be selected as trades.
- Missing entry, stop loss, or target blocks selection.

### Table: `selected_fno_stocks`

Stores persistent F&O watchlist selections.

Columns:

| Column | Meaning |
| --- | --- |
| `stock` | Primary key |
| `signal_type` | Last selected signal |
| `selected_timestamp` | Last selection time |

Selection rules:

- Clicking the F&O icon writes or replaces the stock row.
- Multiple stocks can be selected.
- Refreshing the app reloads this table.
- The latest scan row is used when available.
- Stocks can be removed from the F&O analysis tab.

## 8. UI Structure

The app has two main tabs:

1. Main Dashboard
2. Futures & Options Analysis

### Sidebar UI

Controls:

- Symbol source: `fno_list.csv` or Manual.
- Manual symbols text area.
- Only option-tradable stocks checkbox.
- Use sample data checkbox.
- Long RSI threshold slider.
- Short RSI threshold slider.
- EMA distance percent slider.
- ATR multiplier slider.
- Target RR slider.
- Refresh button.

The sidebar returns:

- Symbols to scan.
- Strategy config.
- Period.
- Interval.
- Sample-data mode.

### Main Dashboard UI Order

The main dashboard renders:

1. Selected trades focus section.
2. Selected F&O watch section.
3. Top trade highlight.
4. Market regime.
5. Scan status and diagnostics.
6. Trend watchlist.
7. Long trades.
8. Short trades.
9. Active trades.
10. Exit signals.
11. Closed trade history.
12. No trade table.
13. Performance dashboard.
14. Skipped symbol/errors.
15. Runtime logs.

### Futures & Options Analysis Tab

The F&O tab renders:

- One analysis block per selected F&O stock.
- Signal direction and signal type.
- Move speed.
- Expected hold bucket.
- Remove button.
- Recommended instrument.
- Theta risk.
- Risk warnings.

If no F&O stocks are selected, it tells the user to select stocks using the F&O button.

## 9. UI Tables And Actions

### Trend Watch Tables

Trend watch sections:

- Emerging Trends.
- Confirming Trends.
- Strong Trades (trend ladder).

Columns typically shown:

- Stock
- Signal
- `trend_watch_direction`
- `trend_stage`
- `trend_age`
- `trend_watch_note`
- Current Price
- RSI
- EMA20
- F&O action where applicable

Rows are colored by direction:

- Long rows: green.
- Short rows: red.
- Strong rows: darker shade.
- Weak rows: lighter shade.

### Long Trades Table

Shows long trade candidates:

- `STRONG_LONG`
- `EARLY_LONG`
- `WEAK_LONG`

Columns:

- Stock
- Signal
- Entry
- Current Price
- Stop Loss
- Target
- RSI
- Confidence Score
- Trade Quality
- Select
- Why
- F&O

### Short Trades Table

Shows short trade candidates:

- `STRONG_SHORT`
- `EARLY_SHORT`
- `WEAK_SHORT`

Same columns as long trades.

### Action Columns

| Action | Icon | Meaning |
| --- | --- | --- |
| Select | Star icon | Adds row to selected trade focus list |
| Why | Info icon | Opens explainability popover |
| F&O | Chart icon | Adds stock to persistent F&O watchlist |
| Remove F&O | X button | Removes stock from F&O watchlist |

The F&O icon is shown only for `STRONG_LONG` and `STRONG_SHORT` rows in long/short trade tables. Confirming and strong trend-watch tables also include F&O actions.

## 10. UX Knowledge

The UI is optimized for scanning:

- Tables are row-banded instead of isolated cards.
- Long and short direction is visible through color.
- Strong vs weak is visible through color intensity.
- Action columns are compact icon buttons to fit inside tables.
- Why-this-trade details are hidden in popovers to reduce clutter.
- Selected trades and selected F&O stocks appear near the top of the main dashboard.
- F&O watchlist persistence prevents selections from being lost on refresh.
- Runtime logs are available inside the app for debugging.

Important UX rules in the current implementation:

- Long trade rows are green.
- Short trade rows are red.
- `EMERGING` rows are watchlist-only and not selectable as active trades.
- Main F&O watch always uses latest scan values if the selected stock exists in the current scan.
- F&O analysis supports multiple selected stocks.
- Static F&O educational text was removed from the bottom of the F&O tab so selected analyses remain the focus.

## 11. Selection Logic

### Selected Trades

Selected trades are different from active DB signals.

Selected trades:

- Are user-chosen focus items.
- Persist in `selected_trades`.
- Show live price, P&L, SL distance, target distance, and hold/exit banner.
- Can be removed.
- Can be manually marked closed.

Selection blocks:

- Non-trade signals.
- `EMERGING` trend-stage rows.
- Rows without entry, stop loss, or target.

### Selected F&O Stocks

Selected F&O stocks:

- Are user-chosen derivative-analysis watchlist items.
- Persist in `selected_fno_stocks`.
- Can include multiple stocks.
- Are rehydrated after refresh.
- Are analyzed using latest scan values when present.
- Can be removed from the F&O tab.

## 12. Active Trade And Exit Logic

Active trades are inserted into `signals` when a scan finds a valid trade signal.

Exit processing closes active trades when any condition is met:

Long exit:

- Current price <= stop loss.
- Current price >= target.
- Current scan direction flips to short.
- RSI < 45 and current price < EMA20 * 0.995.

Short exit:

- Current price >= stop loss.
- Current price <= target.
- Current scan direction flips to long.
- RSI > 55 and current price > EMA20 * 1.005.

Other exits:

- Time exit: trade older than 3 days and live P&L <= 0.
- Expiry risk: weak trades during monthly expiry window.

Monthly expiry window:

- Last Thursday of the month is calculated.
- Expiry window is 0 to 3 days before expiry.
- New auto-inserts are skipped during this window.
- Weak active trades may be closed for expiry risk.

## 13. Trading Signal Knowledge

### Core Indicators

The strategy uses:

- EMA20
- EMA50
- RSI14
- ATR14
- Average Volume over 20 candles
- ATR median and average
- ADX14 for market regime
- Swing highs and swing lows
- Distance from EMA20
- EMA20/EMA50 separation

### Indicator Meaning

EMA20:

- Shorter trend anchor.
- Used for trend direction, pullback proximity, exit logic, and trend-watch invalidation.

EMA50:

- Slower trend anchor.
- Used to define bullish or bearish stock trend.

RSI:

- Momentum confirmation.
- Long strength generally uses RSI above 55 or 60.
- Short strength generally uses RSI below 45 or 40.
- Trend-watch uses 50/60 and 40/50 zones.

ATR:

- Volatility measure.
- Used to set stop loss and target.
- Used in F&O move-speed analysis.

Volume:

- Used for breakout confirmation.
- Used for structure confirmation.
- Used in confidence scoring and F&O velocity.

ADX:

- Used in market-regime detection.
- ADX above 20 confirms trending regime.

## 14. Market Regime Logic

Market regime is detected from:

1. `NIFTYBEES.NS` if available.
2. Fallback basket: `RELIANCE.NS`, `HDFCBANK.NS`, `ICICIBANK.NS`.

Index trend classification:

- Bullish when EMA20 > EMA50, ADX > 20, RSI > 55.
- Bearish when EMA20 < EMA50, ADX > 20, RSI < 45.
- Sideways when ADX < 20, or RSI is neutral if ADX is unavailable.
- Otherwise neutral.

Regime outputs:

| Index Trend | Market Type | Direction | Suggested Strategy |
| --- | --- | --- | --- |
| Bullish | `TRENDING_BULLISH` | `UP` | `PULLBACK_LONG` |
| Bearish | `TRENDING_BEARISH` | `DOWN` | `PULLBACK_SHORT` |
| Sideways | `SIDEWAYS` | `NEUTRAL` | `PULLBACK` |
| Other | `NEUTRAL` | `NEUTRAL` | `NO TRADE` |

Market regime is used as a directional veto:

- Bearish regime blocks new long setup classification.
- Bullish regime blocks new short setup classification.
- Sideways and neutral do not force all rows to wait.

## 15. Setup Classification Logic

The function `classify_setup()` classifies each stock before structure overlay.

### Long Permission

A long setup is allowed when:

- EMA20 > EMA50.
- Market is not `TRENDING_BEARISH`.

### Short Permission

A short setup is allowed when:

- EMA20 < EMA50.
- Market is not `TRENDING_BULLISH`.

### Pullback Long

Classified as `PULLBACK_LONG` when:

- Long is allowed.
- Price is near EMA20.
- RSI is in the long pullback range.

Long pullback RSI range:

- Minimum: `max(45, long_rsi - 5)`
- Maximum: `min(75, long_rsi + 10)`

### Pullback Short

Classified as `PULLBACK_SHORT` when:

- Short is allowed.
- Price is near EMA20.
- RSI is in the short pullback range.

Short pullback RSI range:

- Minimum: `max(25, short_rsi - 10)`
- Maximum: `min(55, short_rsi + 5)`

### Breakout Long

Classified as `BREAKOUT_LONG` when:

- Market/stock trend supports long.
- Close breaks above the prior breakout-window high.
- Volume is greater than `volume_spike_multiplier * avg_volume`.

### Breakout Short

Classified as `BREAKOUT_SHORT` when:

- Market/stock trend supports short.
- Close breaks below the prior breakout-window low.
- Volume is greater than `volume_spike_multiplier * avg_volume`.

### Trend Long / Trend Short

If direction is allowed but pullback or breakout is not ready:

- Long becomes `TREND_LONG`.
- Short becomes `TREND_SHORT`.

If neither direction is allowed, the setup is blank and later becomes `WAIT`.

## 16. Signal Labels

After setup classification:

| Setup Type | Signal |
| --- | --- |
| `PULLBACK_LONG` | `STRONG_LONG` |
| `BREAKOUT_LONG` | `STRONG_LONG` |
| `TREND_LONG` | `WEAK_LONG` |
| `PULLBACK_SHORT` | `STRONG_SHORT` |
| `BREAKOUT_SHORT` | `STRONG_SHORT` |
| `TREND_SHORT` | `WEAK_SHORT` |
| No setup | `WAIT` |

Structure overlay can upgrade or add early signals:

- EMA signal and structure direction agree with structure signal: upgrade to `STRONG_LONG` or `STRONG_SHORT`.
- Structure break with volume confirmation and no matching EMA signal: `EARLY_LONG` or `EARLY_SHORT`.

Signal helpers:

- Long signals: `STRONG_LONG`, `WEAK_LONG`, `EARLY_LONG`.
- Short signals: `STRONG_SHORT`, `WEAK_SHORT`, `EARLY_SHORT`.
- Trade signal: any long or short signal.
- Wait: not actionable.

## 17. Dow Structure Logic

The structure module detects swing highs and lows.

Swing detection:

- Uses a lookback window.
- A swing high is a unique maximum in the local window.
- A swing low is a unique minimum in the local window.

Market structure tags:

- `HH`: higher high.
- `LH`: lower high.
- `HL`: higher low.
- `LL`: lower low.

Bullish structure:

- Latest high tag is `HH`.
- Latest low tag is `HL`.

Bearish structure:

- Latest high tag is `LH`.
- Latest low tag is `LL`.

Structure break:

- Bullish break: close above previous swing high.
- Bearish break: close below previous swing low.

Volume confirmation:

- Current volume / average volume >= 1.5.

Structure output:

- `STRUCTURE_BREAK`
- `STRUCTURE_BULLISH`
- `STRUCTURE_BEARISH`
- Direction: `LONG`, `SHORT`, or `WAIT`
- Volume strength ratio
- Break flags

## 18. Confidence Score Logic

Confidence score ranges from 0 to 5.

For long or short signals:

1. Base point for being a directional signal.
2. RSI momentum point:
   - Long: RSI > 60.
   - Short: RSI < 40.
3. Volume point:
   - Current volume > average volume.
4. Distance point:
   - Distance from EMA20 < max configured distance.
5. Trend-strength point:
   - EMA20/EMA50 separation > minimum trend strength.

`WAIT` gets 0.

## 19. Trade Plan Logic

Entry:

- Usually current close at signal time.

Stop loss:

- Long: entry - ATR * ATR multiplier.
- Short: entry + ATR * ATR multiplier.

Target:

- Long: entry + target RR * risk.
- Short: entry - target RR * risk.

Defaults:

- ATR multiplier: 1.5.
- Target RR: 2.0.

## 20. Trade Quality And Explainability

Explainability fields include:

- Trend direction.
- EMA alignment.
- RSI condition.
- Volume condition.
- Distance from EMA20 condition.
- Strategy type.
- Failed conditions.
- Trade quality.

Trade quality:

- `High Quality`: strong signal with zero failed conditions.
- `Moderate`: weak signal or minor imperfections.
- `Low Confidence`: two or more failed conditions.

The Why popover shows:

- Stock.
- Trend direction.
- EMA aligned.
- RSI status.
- Volume status.
- Distance from EMA20.
- Strategy.
- NSE insider activity.
- Confidence and quality.
- Trend-watch stage.
- Failed conditions.

## 21. Trend Watch Knowledge

Trend watch is informational and does not directly change the trade signal.

Stages:

- `EMERGING`
- `CONFIRMING`
- `STRONG`
- Invalid clears the state.

Important UX rule:

- `EMERGING` rows are watchlist-only.
- `EMERGING` rows cannot be selected as trades.
- `EMERGING` rows are not inserted into `signals`.

### Emerging Long

Starts when:

- EMA20 > EMA50.
- Close > EMA20.
- RSI is between 50 and 60.
- Volume is below soft threshold of average volume * 1.2.

### Emerging Short

Starts when:

- EMA20 < EMA50.
- Close < EMA20.
- RSI is between 40 and 50.
- Volume is below soft threshold of average volume * 1.2.

### Confirming Long

Emerging long becomes confirming when:

- Emerging body holds for at least two candles.
- Close remains above EMA20.
- RSI does not deteriorate materially.

### Confirming Short

Emerging short becomes confirming when:

- Emerging body holds for at least two candles.
- Close remains below EMA20.
- RSI does not deteriorate materially.

### Strong Trend Watch Long

Confirming long becomes strong when:

- EMA separation expands.
- RSI > 60.
- Volume > average volume * 1.05.

### Strong Trend Watch Short

Confirming short becomes strong when:

- EMA separation expands.
- RSI < 40.
- Volume > average volume * 1.05.

### Invalidation

Long watch invalidates when:

- Close < EMA20, or RSI < 50.

Short watch invalidates when:

- Close > EMA20, or RSI > 50.

Confirming notes:

- Long confirming note: preparing for breakout.
- Short confirming note: preparing for breakdown.

## 22. Options Plan Knowledge

The app builds approximate options rows for option-tradable symbols.

Option type:

- Long signal -> CE.
- Short signal -> PE.

Strike selection:

- Uses closest ATM strike.
- Strike step depends on price:
  - Price < 200: step 5.
  - Price < 500: step 10.
  - Price < 1000: step 20.
  - Price < 2500: step 50.
  - Price >= 2500: step 100.

Approx premium:

- Intrinsic value plus time value.
- Time value is max of ATR * 0.45 and futures price * 0.006.
- Minimum premium is 0.05.

Option stop and target:

- SL premium = entry premium * 0.70.
- Target premium = entry premium * 1.60.

This is an approximation only. It is not live option-chain pricing.

## 23. F&O Analysis Knowledge

F&O analysis is separate from the core signal engine.

Selected F&O payload includes:

- Symbol.
- Signal type.
- Confidence.
- Trend strength.
- Signal.
- Strategy type.
- Trend stage.
- Trend-watch direction.
- Entry.
- Current price.
- ATR.
- Volume strength.
- RSI.
- EMA20.
- EMA50.
- Distance from EMA20.
- Structure break.
- Market structure.

When a selected stock is present in the latest scan, the app rebuilds the payload from latest scan values. This is how persistence stays fresh after refresh.

### F&O Move Speed Score

The F&O module calculates a score from:

1. ATR expansion: ATR / current price.
2. Volume spike: volume strength above 1.
3. RSI acceleration: distance of RSI from 50.
4. EMA slope: EMA20 and EMA50 separation.
5. Breakout strength: structure break plus distance from EMA20.

Each component is capped before being added.

Move speed:

- Score >= 3.5: `FAST`.
- Score >= 2.2: `MEDIUM`.
- Otherwise: `SLOW`.
- If direction is `WAIT`: `SIDEWAYS`.

### Hold Bucket

FAST:

- Trend strength < 0.02 -> Intraday.
- Otherwise -> 1-2 days.

MEDIUM:

- Trend strength < 0.03 -> 1-2 days.
- Otherwise -> 3-5 days.

SLOW:

- Trend strength < 0.04 -> 3-5 days.
- Otherwise -> 1+ week.

SIDEWAYS:

- 1+ week.

### Theta Risk

- Intraday -> LOW.
- 1-2 days -> MEDIUM.
- 3-5 days -> HIGH.
- 1+ week -> HIGH.

### Instrument Recommendation

FAST move:

- Best: ATM CE/PE option.
- Alternate: slight ITM CE/PE.
- Avoid: deep ITM option or futures lag.

MEDIUM move:

- Best: ITM CE/PE option.
- Alternate: futures.
- Avoid: far OTM CE/PE.

SLOW move:

- Best: futures.
- Alternate: deep ITM CE/PE.
- Avoid: ATM CE/PE.

SIDEWAYS:

- Best: avoid option buying.
- Alternate: wait for directional setup.
- Avoid: ATM option buying.

### F&O Warnings

Warnings are shown when:

- Theta risk is HIGH.
- ATR expansion > 0.018.
- Volume strength >= 1.8.
- Move is sideways.

## 24. NSE Insider Context

The app fetches NSE promoter/promoter-group activity for:

- Strong and weak signal stocks.
- Active DB trades.

It does not mutate scores or signals.

It is shown inside the Why popover as context:

- Promoter activity.
- Net quantity over recent window.
- Last transaction date.
- Interpretation.

## 25. Sorting And Ranking Knowledge

Full futures table sorting:

1. Signal priority.
2. Signal type priority.
3. Confidence score descending.
4. Distance from EMA20 ascending.
5. Stock symbol.

Signal priority:

- Strong long/short first.
- Early long/short next.
- Weak long/short next.
- Wait last.

Signal type priority:

- Pullback.
- Breakout / structure.
- Trend.
- Blank.

Long and short trade ranking:

1. Confidence score descending.
2. Distance from EMA20 ascending.
3. Stock symbol.

Top trade highlight:

- Uses actionable long/short signals.
- Excludes `EMERGING`.
- Sorts by confidence, signal priority, distance from EMA20.

## 26. Logging And Diagnostics

Logging:

- File: `logs/trading_dashboard.log`.
- Rotating file handler.
- Console logging can be enabled with `TRADING_DASHBOARD_CONSOLE_LOG=1`.
- Verbose API logs can be enabled with `TRADING_DASHBOARD_VERBOSE_API=1`.

UI diagnostics:

- API usage panel.
- Market data cache status.
- Scan diagnostics expander.
- Skipped symbols/errors expander.
- Runtime logs expander.

## 27. Development Knowledge

Run app locally:

```bash
streamlit run app.py
```

Common verification:

```bash
python3 -m py_compile app.py
```

Key implementation notes:

- The project is currently mostly a single-file Streamlit app.
- UI state that must survive refresh is stored in SQLite, not only `st.session_state`.
- `st.cache_data` is used for stock lists and scan results.
- Market fetch cache is both in-session and on disk.
- User selections are intentionally separate:
  - `selected_trades` is for trade focus.
  - `selected_fno_stocks` is for derivative analysis.

## 28. Known Limitations

- Option premiums are approximate, not live option-chain data.
- Market data quality depends on Yahoo, Alpha Vantage, and EODHD availability.
- NSE insider context is supplemental and not used in scoring.
- The app does not place trades.
- Expiry logic is simplified to monthly expiry.
- Most logic currently lives in `app.py`, so future maintainability would benefit from splitting into modules.

## 29. Suggested Future Module Split

A clean future refactor could split `app.py` into:

- `config.py`: constants and `StrategyConfig`.
- `data_sources.py`: Yahoo, Alpha, EODHD, NSE helpers.
- `cache.py`: market-data and API quota persistence.
- `indicators.py`: RSI, ATR, ADX, EMA enrichment.
- `strategy.py`: setup classification, signals, scoring.
- `trend_watch.py`: emerging/confirming/strong logic.
- `db.py`: SQLite schema and persistence helpers.
- `options.py`: approximate options plan.
- `derivatives.py`: F&O analysis.
- `ui_sidebar.py`: sidebar controls.
- `ui_tables.py`: table renderers and action controls.
- `ui_pages.py`: main dashboard and F&O tab rendering.

This would make each knowledge area easier to test and evolve independently.
