from __future__ import annotations

import sqlite3
from datetime import datetime

import streamlit as st

from cache_layer import _ensure_market_cache_after_scan, render_api_usage_panel, render_market_data_cache_status_bar
from config import LOG_PATH, MARKET_SCAN_NONCE_KEY, configure_runtime_environment, empty_history_table, logger
from data_sources import load_lot_size_map, load_option_list, load_stock_list, refresh_nse_insider_context
from db import (
    default_performance_metrics,
    get_active_trades,
    get_db_connection,
    get_trade_history,
    init_db,
    process_exits,
    refresh_performance_metrics,
    update_signals,
)
from strategy import apply_position_sizing, build_options_plan, get_market_regime, ranked_trades, scan_symbols, sort_futures_table
from ui_components import (
    format_futures_table,
    inject_css,
    render_active_trades,
    render_closed_trades,
    render_derivative_analysis_tab,
    render_exit_signals,
    render_market_regime,
    render_performance_dashboard,
    render_runtime_log_tail,
    render_scan_status,
    render_selected_fno_main,
    render_selected_trades_focus,
    render_sidebar,
    render_top_trade_highlight,
    render_trade_section,
    render_trend_watch_sections,
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

    lot_size_by_symbol = load_lot_size_map()
    signals = apply_position_sizing(signals, risk_settings, lot_size_by_symbol)
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
        render_selected_trades_focus(signals)
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
        render_derivative_analysis_tab(signals)


if __name__ == "__main__":
    main()
