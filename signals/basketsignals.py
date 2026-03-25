"""Basket Signal Generation — Phase 3
Processes basket equity curves, signals, breadth, and correlations for all baskets.
Run: python basketsignals.py
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
import hashlib
import json
from pathlib import Path

from foundation import (
    # Constants
    SIZE,
    BENCHMARK_BASKETS,
    BENCHMARK_TIMING,
    CHART_SCHEMA_VERSION,
    BASKET_SIGNALS_CACHE_SCHEMA_VERSION,
    FORCE_REBUILD_BASKET_SIGNALS,
    EQUITY_CACHE_SCHEMA_VERSION,
    EQUITY_SIGNAL_LOGIC_VERSION,
    EQUITY_UNIVERSE_LOGIC_VERSION,
    SIGNALS,
    RV_MULT,
    EMA_MULT,
    RV_EMA_ALPHA,
    FORCE_REBUILD_EQUITY_CACHE,
    # Paths
    paths,
    DATA_FOLDER,
    THEMATIC_CHARTS_FOLDER,
    SECTOR_CHARTS_FOLDER,
    INDUSTRY_CHARTS_FOLDER,
    SIGNALS_CACHE_FILE,
    CORR_CACHE_FOLDER,
    # Signal engine
    _build_signals_from_df,
    _build_signals_next_row,
    calc_rolling_stats,
    # Basket cache helpers
    _cache_slugify_label,
    _get_data_signature,
    reset_data_signature_cache,
    _build_universe_signature,
    _equity_cache_paths,
    _load_equity_cache,
    _save_equity_cache,
    _build_equity_meta,
    _is_equity_cache_valid,
    _basket_cache_folder,
    _cache_file_stem,
    _basket_cache_paths,
    _find_basket_parquet,
    _find_basket_meta,
    _get_chart_schema_version_from_parquet,
    _build_basket_signals_meta,
    _is_basket_signals_cache_valid,
    # Quarter/filter helpers
    _build_quarter_lookup,
    _find_active_quarter,
    _build_membership_df,
    _vectorized_quarter_filter,
    compute_breadth_from_trend,
    compute_breadth_from_breakout,
    _build_quarter_weights,
    compute_breadth_pivots,
    # Quarter key helpers
    _quarter_end_from_key,
    # Universe loaders
    load_all_universes,
    build_all_basket_specs,
    get_current_quarter_key,
    # Utilities
    WriteThroughPath,
    reset_cell_timer,
    _needs_write_and_mirror,
    _basket_timing,
    _basket_timing_names,
)

# ---------------------------------------------------------------------------
# Module-level state — set in __main__ before functions are called
# ---------------------------------------------------------------------------

all_signals_df = pd.DataFrame()

# ---------------------------------------------------------------------------
# Basket processing functions
# ---------------------------------------------------------------------------


def compute_signal_trades(df, entry_col, exit_col, direction='long',
                          price_col='Close', high_col='High', low_col='Low', date_col='Date'):
    """Compute simple entry/exit trades based on entry/exit signals."""
    trades = []
    in_pos = False
    entry_idx = None
    entry_price = None
    max_high = None
    min_low = None

    for i in range(len(df)):
        if in_pos:
            hi = df.at[i, high_col]
            lo = df.at[i, low_col]
            if pd.notna(hi):
                max_high = hi if max_high is None else max(max_high, hi)
            if pd.notna(lo):
                min_low = lo if min_low is None else min(min_low, lo)

            if bool(df.at[i, exit_col]):
                exit_price = df.at[i, price_col]
                if pd.isna(exit_price) or entry_price is None or entry_price == 0:
                    in_pos = False
                    entry_idx = None
                    continue

                if direction == 'short':
                    change = (entry_price - exit_price) / entry_price
                    mfe = (entry_price - min_low) / entry_price if min_low is not None else np.nan
                    mae = (entry_price - max_high) / entry_price if max_high is not None else np.nan
                else:
                    change = (exit_price - entry_price) / entry_price
                    mfe = (max_high - entry_price) / entry_price if max_high is not None else np.nan
                    mae = (min_low - entry_price) / entry_price if min_low is not None else np.nan

                trades.append({
                    'entry_date': df.at[entry_idx, date_col],
                    'exit_date': df.at[i, date_col],
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'change': change,
                    'mfe': mfe,
                    'mae': mae,
                    'bars': i - entry_idx,
                })
                in_pos = False
                entry_idx = None
                entry_price = None
                max_high = None
                min_low = None

        if not in_pos and bool(df.at[i, entry_col]):
            price = df.at[i, price_col]
            if pd.isna(price) or price == 0:
                continue
            in_pos = True
            entry_idx = i
            entry_price = price
            max_high = df.at[i, high_col] if pd.notna(df.at[i, high_col]) else None
            min_low = df.at[i, low_col] if pd.notna(df.at[i, low_col]) else None

    return trades


def compute_equity_ohlc(
    all_df,
    universe_by_date,
    start_after_date=None,
    initial_state=None,
    return_state=False,
    returns_matrix=None,
    ohlc_ret_matrices=None,
    return_contributions=False,
):
    """Build synthetic OHLC equity curve for a basket using dollar-volume weights.

    When returns_matrix and ohlc_ret_matrices are provided and start_after_date is None,
    uses a fast vectorized per-quarter path. Otherwise falls back to the incremental loop.

    If return_contributions=True, also returns a contributions DataFrame as a third element.
    """
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_date)
    quarter_weights = _build_quarter_weights(all_df, universe_by_date, quarter_labels)

    # --- FAST VECTORIZED PATH: full build with pre-computed matrices ---
    if (start_after_date is None and initial_state is None
            and returns_matrix is not None and ohlc_ret_matrices is not None):
        ohlc_parts = []
        contrib_parts = []
        equity_prev_close = 1.0
        last_state = {'current_quarter': None, 'equity_prev_close': 1.0, 'weights': {}}

        for q_idx, q_key in enumerate(quarter_labels):
            w_dict = quarter_weights.get(q_key)
            if not w_dict:
                continue
            tickers = [t for t in w_dict if t in returns_matrix.columns]
            if not tickers:
                continue

            # Date range (exclude next quarter's start)
            q_start = quarter_ends[q_idx]
            if q_idx + 1 < len(quarter_ends):
                q_end = quarter_ends[q_idx + 1] - pd.Timedelta(days=1)
            else:
                q_end = returns_matrix.index[-1]

            close_ret = returns_matrix.loc[q_start:q_end, tickers].fillna(0.0)
            if close_ret.empty:
                continue
            open_ret = ohlc_ret_matrices['Open_Ret'].loc[q_start:q_end, tickers].fillna(0.0)
            high_ret = ohlc_ret_matrices['High_Ret'].loc[q_start:q_end, tickers].fillna(0.0)
            low_ret = ohlc_ret_matrices['Low_Ret'].loc[q_start:q_end, tickers].fillna(0.0)

            # Initial weights
            w0 = pd.Series({t: w_dict[t] for t in tickers})
            w0 = w0 / w0.sum()

            # Cumulative growth and portfolio value (buy-and-hold within quarter)
            cum_growth = (1 + close_ret).cumprod()
            portfolio_value = (cum_growth * w0).sum(axis=1)

            # Equity close = previous quarter's last close * portfolio growth
            eq_close = equity_prev_close * portfolio_value

            # Previous close for OHLC computation
            eq_prev_close_series = eq_close.shift(1).fillna(equity_prev_close)

            # BOD weights (drifted)
            cum_growth_prev = cum_growth.shift(1).fillna(1.0)
            portfolio_value_prev = portfolio_value.shift(1).fillna(1.0)
            bod_weights = cum_growth_prev.multiply(w0, axis=1).div(portfolio_value_prev, axis=0)

            # Weighted portfolio OHLC returns
            port_open_ret = (bod_weights * open_ret).sum(axis=1)
            port_high_ret = (bod_weights * high_ret).sum(axis=1)
            port_low_ret = (bod_weights * low_ret).sum(axis=1)

            eq_open = eq_prev_close_series * (1 + port_open_ret)
            eq_high = eq_prev_close_series * (1 + port_high_ret)
            eq_low = eq_prev_close_series * (1 + port_low_ret)

            # OHLC constraint
            eq_high = np.maximum(eq_high, np.maximum(eq_open, eq_close))
            eq_low = np.minimum(eq_low, np.minimum(eq_open, eq_close))

            q_ohlc = pd.DataFrame({
                'Date': close_ret.index,
                'Open': eq_open.values,
                'High': eq_high.values,
                'Low': eq_low.values,
                'Close': eq_close.values,
            })
            ohlc_parts.append(q_ohlc)

            # Contributions as byproduct
            if return_contributions:
                contributions = bod_weights * close_ret
                bod_long = bod_weights.stack().rename('Weight_BOD')
                ret_long = close_ret.stack().rename('Daily_Return')
                contrib_long = contributions.stack().rename('Contribution')
                q_contrib = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
                q_contrib.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
                contrib_parts.append(q_contrib)

            # Carry forward equity_prev_close for next quarter
            equity_prev_close = float(eq_close.iloc[-1])
            # Build state for caching
            last_weights = bod_weights.iloc[-1] * (1 + close_ret.iloc[-1])
            lw_sum = last_weights.sum()
            if lw_sum > 0:
                last_weights = last_weights / lw_sum
            last_state = {
                'current_quarter': q_key,
                'equity_prev_close': equity_prev_close,
                'weights': {str(k): float(v) for k, v in last_weights.items() if pd.notna(v)},
            }

        if not ohlc_parts:
            out = pd.DataFrame()
        else:
            out = pd.concat(ohlc_parts, ignore_index=True).drop_duplicates(subset=['Date'], keep='last').sort_values('Date').reset_index(drop=True)

        contrib_df = None
        if return_contributions and contrib_parts:
            contrib_df = pd.concat(contrib_parts, ignore_index=True).drop_duplicates(subset=['Date', 'Ticker'], keep='last').sort_values(['Date', 'Ticker']).reset_index(drop=True)

        if return_state and return_contributions:
            return out, last_state, contrib_df
        elif return_state:
            return out, last_state
        elif return_contributions:
            return out, contrib_df
        return out

    # --- INCREMENTAL LOOP PATH: for appending a few new days ---
    needed_cols = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close']
    if 'Volume' in all_df.columns:
        needed_cols.append('Volume')
    df = all_df[needed_cols].copy()
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
    df = df.dropna(subset=['Close'])
    df = df.sort_values(['Ticker', 'Date'])

    df['Ret'] = df.groupby('Ticker')['Close'].pct_change()
    prev_close = df.groupby('Ticker')['Close'].shift(1)
    df['Open_Ret'] = (df['Open'] / prev_close) - 1
    df['High_Ret'] = (df['High'] / prev_close) - 1
    df['Low_Ret'] = (df['Low'] / prev_close) - 1

    dates = sorted(df['Date'].unique())
    date_groups = {d: g for d, g in df.groupby('Date')}

    ohlc_rows = []
    current_weights_series = None
    current_quarter = None
    equity_prev_close = 1.0

    if initial_state:
        current_quarter = initial_state.get('current_quarter')
        try:
            equity_prev_close = float(initial_state.get('equity_prev_close', 1.0))
        except Exception:
            equity_prev_close = 1.0
        w_dict = initial_state.get('weights', {})
        if isinstance(w_dict, dict) and w_dict:
            current_weights_series = pd.Series(w_dict, dtype=float)

    start_after_ts = None
    if start_after_date is not None:
        start_after_ts = pd.to_datetime(start_after_date, errors='coerce')
        if pd.notna(start_after_ts):
            start_after_ts = start_after_ts.normalize()
        else:
            start_after_ts = None

    for d in dates:
        if start_after_ts is not None and d <= start_after_ts:
            continue
        active_key = _find_active_quarter(d, quarter_labels, quarter_ends)
        if active_key is None:
            continue
        if active_key != current_quarter:
            current_quarter = active_key
            w_dict = quarter_weights.get(current_quarter, {})
            if not w_dict:
                current_weights_series = None
                continue
            current_weights_series = pd.Series(w_dict)

        if current_weights_series is None:
            continue

        day_df = date_groups.get(d)
        if day_df is None or day_df.empty:
            continue
        day_df = day_df[day_df['Ticker'].isin(universe_by_date[current_quarter])]
        if day_df.empty:
            continue

        day_data = day_df.set_index('Ticker')
        common = current_weights_series.index.intersection(day_data.index)
        if len(common) == 0:
            continue
        w = current_weights_series[common]
        o_ret = (w * day_data.loc[common, 'Open_Ret'].fillna(0)).sum()
        h_ret = (w * day_data.loc[common, 'High_Ret'].fillna(0)).sum()
        l_ret = (w * day_data.loc[common, 'Low_Ret'].fillna(0)).sum()
        c_ret = (w * day_data.loc[common, 'Ret'].fillna(0)).sum()

        eq_open  = equity_prev_close * (1 + o_ret)
        eq_high  = equity_prev_close * (1 + h_ret)
        eq_low   = equity_prev_close * (1 + l_ret)
        eq_close = equity_prev_close * (1 + c_ret)
        eq_high = max(eq_high, eq_open, eq_close)
        eq_low  = min(eq_low, eq_open, eq_close)

        ohlc_rows.append({
            'Date': d, 'Open': eq_open, 'High': eq_high,
            'Low': eq_low, 'Close': eq_close,
        })
        equity_prev_close = eq_close

        rets = day_data.loc[common, 'Ret'].fillna(0.0)
        updated = w * (1 + rets)
        total = updated.sum()
        if total > 0:
            current_weights_series = updated / total
        else:
            current_weights_series = updated

    state = {
        'current_quarter': current_quarter,
        'equity_prev_close': float(equity_prev_close),
        'weights': {},
    }
    if current_weights_series is not None and len(current_weights_series) > 0:
        state['weights'] = {
            str(k): float(v)
            for k, v in current_weights_series.items()
            if pd.notna(v)
        }

    if not ohlc_rows:
        out = pd.DataFrame()
    else:
        out = pd.DataFrame(ohlc_rows).sort_values('Date').reset_index(drop=True)

    if return_state:
        return out, state
    return out


def compute_equity_ohlc_cached(all_df, universe_by_date, basket_name, slug, basket_type='sector',
                               returns_matrix=None, ohlc_ret_matrices=None):
    data_sig = _get_data_signature(all_df)
    universe_sig = _build_universe_signature(universe_by_date)
    cached_df, meta = _load_equity_cache(slug, basket_type, universe_by_date)

    if FORCE_REBUILD_EQUITY_CACHE or FORCE_REBUILD_BASKET_SIGNALS:
        cached_df, meta = None, None

    if cached_df is None or not _is_equity_cache_valid(meta, data_sig, universe_sig):
        result = compute_equity_ohlc(all_df, universe_by_date, return_state=True,
                                     returns_matrix=returns_matrix, ohlc_ret_matrices=ohlc_ret_matrices,
                                     return_contributions=True)
        rebuilt_df, state, contrib_df = result
        if rebuilt_df.empty:
            return rebuilt_df, None
        new_meta = _build_equity_meta(data_sig, universe_sig, state, rebuilt_df)
        _save_equity_cache(slug, rebuilt_df, new_meta, basket_type, universe_by_date)
        return rebuilt_df, contrib_df

    last_cached = pd.to_datetime(meta.get('last_cached_date'), errors='coerce')
    latest_source = data_sig.get('latest_date')
    if pd.notna(last_cached) and pd.notna(latest_source) and last_cached.normalize() >= latest_source.normalize():
        return cached_df, None  # contributions not recomputed when fully cached

    state = meta.get('state', {})
    appended_df, new_state = compute_equity_ohlc(
        all_df,
        universe_by_date,
        start_after_date=last_cached,
        initial_state=state,
        return_state=True,
    )
    if appended_df.empty:
        refreshed_meta = _build_equity_meta(data_sig, universe_sig, state, cached_df)
        _save_equity_cache(slug, cached_df, refreshed_meta, basket_type, universe_by_date)
        print(f"[{basket_name}] no new equity rows to append")
        return cached_df, None

    combined = (
        pd.concat([cached_df, appended_df], ignore_index=True)
        .drop_duplicates(subset=['Date'], keep='last')
        .sort_values('Date')
        .reset_index(drop=True)
    )
    refreshed_meta = _build_equity_meta(data_sig, universe_sig, new_state, combined)
    _save_equity_cache(slug, combined, refreshed_meta, basket_type, universe_by_date)
    return combined, None


def compute_equity_curve(all_df, universe_by_date):
    """Compatibility helper for Cells 11-12 (returns Date + Equity)."""
    ohlc_df = compute_equity_ohlc(all_df, universe_by_date)
    if ohlc_df.empty:
        return pd.DataFrame()
    eq_df = ohlc_df[['Date', 'Close']].copy()
    eq_df.rename(columns={'Close': 'Equity'}, inplace=True)
    return eq_df


def _fmt_price(x):
    return f"${x:,.2f}" if pd.notna(x) else ""


def _fmt_bars(x):
    return str(int(round(x))) if pd.notna(x) else ""


def _fmt_pct(x):
    return f"{x * 100:.2f}%" if pd.notna(x) else ""


def _append_trade_rows(rows, signal_name, direction, trades, bull_div_dates, bear_div_dates):
    closed_changes, closed_mfe, closed_mae, closed_bars = [], [], [], []
    for t in trades:
        entry_dt = pd.to_datetime(t['entry_date']).normalize()
        if entry_dt in bull_div_dates:
            div_type = 'Bullish'
        elif entry_dt in bear_div_dates:
            div_type = 'Bearish'
        else:
            div_type = ''
        closed_changes.append(t['change'])
        closed_mfe.append(t['mfe'])
        closed_mae.append(t['mae'])
        closed_bars.append(t['bars'])
        rolling_stats = calc_rolling_stats(closed_changes, closed_mfe, closed_mae, bars_list=closed_bars)
        rows.append({
            'Signal': signal_name,
            'Divergence_Type': div_type,
            'Direction': direction,
            'Entry_Date': t['entry_date'],
            'Exit_Date': t['exit_date'],
            'Entry_Price': t['entry_price'],
            'Exit_Price': t['exit_price'],
            'Final_Change': t['change'],
            'MFE': t['mfe'],
            'MAE': t['mae'],
            'Bars': t['bars'],
            'Win_Rate': rolling_stats.get('Win_Rate', np.nan),
            'Avg_Winner': rolling_stats.get('Avg_Winner', np.nan),
            'Avg_Loser': rolling_stats.get('Avg_Loser', np.nan),
            'Avg_Winner_Bars': rolling_stats.get('Avg_Winner_Bars', np.nan),
            'Avg_Loser_Bars': rolling_stats.get('Avg_Loser_Bars', np.nan),
            'Avg_MFE': rolling_stats.get('Avg_MFE', np.nan),
            'Avg_MAE': rolling_stats.get('Avg_MAE', np.nan),
            'Historical_EV': rolling_stats.get('Historical_EV', np.nan),
            'Std_Dev': rolling_stats.get('Std_Dev', np.nan),
            'Risk_Adj_EV': rolling_stats.get('Risk_Adj_EV', np.nan),
            'EV_Last_3': rolling_stats.get('EV_Last_3', np.nan),
            'Risk_Adj_EV_Last_3': rolling_stats.get('Risk_Adj_EV_Last_3', np.nan),
            'Count': rolling_stats.get('Count', 0),
        })


def _compute_within_basket_correlation(universe_by_qtr, returns_matrix, window=21):
    """Compute rolling within-basket average pairwise correlation via variance decomposition.

    For each rolling window, z-scores returns WITHIN that window, then applies:
        avg_pairwise_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)
    This gives the exact simple average of pairwise correlations.

    Uses numpy for the inner loop (O(n*w) per date vs O(n^2*w) for full .corr()).

    Returns a DataFrame with columns ['Date', 'Correlation_Pct'] (scaled 0-100).
    """
    if returns_matrix is None or returns_matrix.empty:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])

    if not universe_by_qtr:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    min_obs = max(10, int(window * 0.70))
    all_dates = []
    all_corrs = []

    for q_idx, q_key in enumerate(quarter_labels):
        tickers = [t for t in universe_by_qtr.get(q_key, set()) if t in returns_matrix.columns]
        if len(tickers) < 2:
            continue

        # Date range for this quarter
        q_start = quarter_ends[q_idx]
        q_end = quarter_ends[q_idx + 1] if q_idx + 1 < len(quarter_ends) else returns_matrix.index[-1]
        # Include window days before q_start for rolling warm-up
        warmup_start_idx = returns_matrix.index.searchsorted(q_start) - window
        warmup_start = returns_matrix.index[max(0, warmup_start_idx)]
        sub_ret = returns_matrix.loc[warmup_start:q_end, tickers]

        if len(sub_ret) < window:
            continue

        # Filter to tickers with sufficient data in the quarter period
        q_data = sub_ret.loc[q_start:]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            continue
        sub_ret = sub_ret[valid_tickers]

        # Convert to numpy for fast inner loop
        ret_arr = sub_ret.values  # (T, n)
        dates_arr = sub_ret.index
        q_start_idx = sub_ret.index.searchsorted(q_start)
        # Exclusive upper bound for output: exclude next quarter's start date
        if q_idx + 1 < len(quarter_ends):
            q_output_end = q_end - pd.Timedelta(days=1)
        else:
            q_output_end = q_end

        for d_idx in range(window, len(dates_arr)):
            d = dates_arr[d_idx]
            if d < q_start or d > q_output_end:
                continue
            w_slice = ret_arr[d_idx - window + 1:d_idx + 1, :]  # (window, n)
            # Valid columns: enough non-NaN in this window
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            w = w_slice[:, col_valid]
            # Z-score within this window
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            # EW portfolio of z-scores
            z_port = np.nanmean(z, axis=1)
            z_valid = z_port[~np.isnan(z_port)]
            if len(z_valid) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            var_z = np.var(z_valid, ddof=1)
            avg_corr = (nv * var_z - 1) / (nv - 1)
            all_dates.append(d)
            all_corrs.append(np.clip(avg_corr * 100, -100, 100))

    if not all_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    result = pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})
    result = result.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')
    return result


def _compute_within_basket_correlation_incremental(universe_by_qtr, returns_matrix, new_dates, window=21):
    """Compute rolling within-basket correlation for only the specified new dates.

    Same z-score variance decomposition as the full version, but only processes
    the active quarter(s) for the new dates instead of all quarters x all dates.
    """
    if returns_matrix is None or returns_matrix.empty or not universe_by_qtr or not new_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    min_obs = max(10, int(window * 0.70))
    new_dates_norm = sorted(pd.to_datetime(d).normalize() for d in new_dates)

    all_dates = []
    all_corrs = []

    # Group new dates by their active quarter to avoid redundant setup
    quarter_date_groups = {}
    for d in new_dates_norm:
        q_key = _find_active_quarter(d, quarter_labels, quarter_ends)
        if q_key is not None:
            quarter_date_groups.setdefault(q_key, []).append(d)

    for q_key, dates_in_q in quarter_date_groups.items():
        tickers = [t for t in universe_by_qtr.get(q_key, set()) if t in returns_matrix.columns]
        if len(tickers) < 2:
            for d in dates_in_q:
                all_dates.append(d)
                all_corrs.append(np.nan)
            continue

        # Get the quarter's index in the lookup for date range
        q_idx = quarter_labels.index(q_key)
        q_start = quarter_ends[q_idx]

        # Filter to tickers with sufficient data in the quarter
        q_data = returns_matrix.loc[q_start:, tickers]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            for d in dates_in_q:
                all_dates.append(d)
                all_corrs.append(np.nan)
            continue

        for d in dates_in_q:
            # Extract window slice ending at d
            d_idx_in_matrix = returns_matrix.index.searchsorted(d)
            if d_idx_in_matrix >= len(returns_matrix.index) or returns_matrix.index[d_idx_in_matrix] != d:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            start_idx = max(0, d_idx_in_matrix - window + 1)
            w_slice = returns_matrix.iloc[start_idx:d_idx_in_matrix + 1][valid_tickers].values

            if len(w_slice) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue

            # Valid columns: enough non-NaN in this window
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            w = w_slice[:, col_valid]
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            z_port = np.nanmean(z, axis=1)
            z_valid = z_port[~np.isnan(z_port)]
            if len(z_valid) < min_obs:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            var_z = np.var(z_valid, ddof=1)
            avg_corr = (nv * var_z - 1) / (nv - 1)
            all_dates.append(d)
            all_corrs.append(np.clip(avg_corr * 100, -100, 100))

    if not all_dates:
        return pd.DataFrame(columns=['Date', 'Correlation_Pct'])
    return pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})


def _augment_basket_signals_with_breadth(signals_df, universe_by_qtr):
    _do_timing = BENCHMARK_TIMING or BENCHMARK_BASKETS > 0

    # Build membership table once, shared by both breadth calls
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)
    membership_df = _build_membership_df(universe_by_qtr)

    if _do_timing:
        _t0 = time.perf_counter()
    b = compute_breadth_from_trend(all_signals_df, universe_by_qtr, membership_df=membership_df).copy()
    b['Breadth_EMA'] = b['Breadth_Ratio'].ewm(span=10, adjust=False).mean()
    b['Uptrend_Pct'] = (b['Uptrend_Count'] / b['Total_Stocks']) * 100.0
    b['Downtrend_Pct'] = (b['Downtrend_Count'] / b['Total_Stocks']) * 100.0
    if _do_timing:
        _augment_basket_signals_with_breadth._last_trend_time = time.perf_counter() - _t0

    if _do_timing:
        _t0 = time.perf_counter()
    bo = compute_breadth_from_breakout(all_signals_df, universe_by_qtr, membership_df=membership_df).copy()
    if not bo.empty:
        bo['BO_Breadth_EMA'] = bo['BO_Breadth_Ratio'].ewm(span=10, adjust=False).mean()
        bo['Breakout_Pct'] = (bo['Breakout_Count'] / bo['BO_Total_Stocks']) * 100.0
        bo['Breakdown_Pct'] = (bo['Breakdown_Count'] / bo['BO_Total_Stocks']) * 100.0
    if _do_timing:
        _augment_basket_signals_with_breadth._last_breakout_time = time.perf_counter() - _t0

    if _do_timing:
        _t0 = time.perf_counter()
    signals_df = signals_df.copy()
    signals_df['Date'] = pd.to_datetime(signals_df['Date']).dt.normalize()
    # Drop any breadth columns already present (e.g. carried forward from prev_row
    # via _build_signals_next_row when running the incremental-append path).
    _breadth_cols = ['Uptrend_Pct', 'Downtrend_Pct', 'Breadth_EMA',
                     'Breakout_Pct', 'Breakdown_Pct', 'BO_Breadth_EMA']
    signals_df = signals_df.drop(columns=[c for c in _breadth_cols if c in signals_df.columns])
    merged_all = pd.merge(
        signals_df,
        b[['Date', 'Uptrend_Pct', 'Downtrend_Pct', 'Breadth_EMA']],
        on='Date',
        how='inner',
    ).sort_values('Date').reset_index(drop=True)
    if not bo.empty:
        merged_all = pd.merge(
            merged_all,
            bo[['Date', 'Breakout_Pct', 'Breakdown_Pct', 'BO_Breadth_EMA']],
            on='Date',
            how='left',
        ).sort_values('Date').reset_index(drop=True)
    else:
        merged_all['Breakout_Pct'] = np.nan
        merged_all['Breakdown_Pct'] = np.nan
        merged_all['BO_Breadth_EMA'] = np.nan
    if _do_timing:
        _augment_basket_signals_with_breadth._last_merge_time = time.perf_counter() - _t0
    return merged_all


def _finalize_basket_signals_output(name, slug, hist_folder, merged_all, data_sig, universe_sig, universe_by_qtr, basket_type='sector', returns_matrix=None, contrib_df=None, incremental_dates=None):
    _do_timing = BENCHMARK_TIMING or BENCHMARK_BASKETS > 0

    if _do_timing:
        _t0 = time.perf_counter()
    breadth_pivots = compute_breadth_pivots(merged_all['Breadth_EMA'].values)
    if breadth_pivots is None or breadth_pivots.empty:
        for col in ['B_Trend', 'B_Resistance', 'B_Support', 'B_Rot_High', 'B_Rot_Low']:
            merged_all[col] = np.nan
        for col in ['B_Up_Rot', 'B_Down_Rot', 'B_Bull_Div', 'B_Bear_Div']:
            merged_all[col] = False
    else:
        for col in breadth_pivots.columns:
            merged_all[col] = breadth_pivots[col].values

    bo_pivots = compute_breadth_pivots(merged_all['BO_Breadth_EMA'].values)
    if bo_pivots is None or bo_pivots.empty:
        for col in ['BO_B_Trend', 'BO_B_Resistance', 'BO_B_Support', 'BO_B_Rot_High', 'BO_B_Rot_Low']:
            merged_all[col] = np.nan
        for col in ['BO_B_Up_Rot', 'BO_B_Down_Rot', 'BO_B_Bull_Div', 'BO_B_Bear_Div']:
            merged_all[col] = False
    else:
        for col in bo_pivots.columns:
            merged_all[f'BO_{col}'] = bo_pivots[col].values

    if _do_timing:
        _finalize_basket_signals_output._last_pivots_time = time.perf_counter() - _t0

    # Compute within-basket correlation and merge as Correlation_Pct column
    if _do_timing:
        _t0 = time.perf_counter()
    if incremental_dates and 'Correlation_Pct' in merged_all.columns:
        # Incremental path: only compute correlation for new dates
        incr_corr = _compute_within_basket_correlation_incremental(
            universe_by_qtr, returns_matrix, incremental_dates)
        if not incr_corr.empty:
            merged_all = merged_all.copy()
            merged_all['Date'] = pd.to_datetime(merged_all['Date']).dt.normalize()
            incr_corr['Date'] = pd.to_datetime(incr_corr['Date']).dt.normalize()
            # Update only the new dates' correlation values
            incr_map = incr_corr.set_index('Date')['Correlation_Pct']
            mask = merged_all['Date'].isin(incr_map.index)
            merged_all.loc[mask, 'Correlation_Pct'] = merged_all.loc[mask, 'Date'].map(incr_map).values
    else:
        # Full recompute
        corr_df = _compute_within_basket_correlation(universe_by_qtr, returns_matrix)
        if not corr_df.empty:
            merged_all = merged_all.copy()
            merged_all['Date'] = pd.to_datetime(merged_all['Date']).dt.normalize()
            corr_df['Date'] = pd.to_datetime(corr_df['Date']).dt.normalize()
            if 'Correlation_Pct' in merged_all.columns:
                merged_all = merged_all.drop(columns=['Correlation_Pct'])
            merged_all = pd.merge(merged_all, corr_df[['Date', 'Correlation_Pct']], on='Date', how='left')
        elif 'Correlation_Pct' not in merged_all.columns:
            merged_all['Correlation_Pct'] = np.nan
    if _do_timing:
        _finalize_basket_signals_output._last_correlation_time = time.perf_counter() - _t0

    if _do_timing:
        _t0 = time.perf_counter()
    hist_folder.mkdir(parents=True, exist_ok=True)

    merged_all['Source'] = 'norgate'

    # Save consolidated basket parquet + meta (single file per basket)
    stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'signals')
    basket_folder = _basket_cache_folder(basket_type)
    parquet_path = basket_folder / f'{stem}.parquet'
    meta_path = basket_folder / f'{stem}_meta.json'

    # Build combined meta (merges old equity_meta + basket_signals_meta)
    last_cached_date = None
    if isinstance(merged_all, pd.DataFrame) and not merged_all.empty and 'Date' in merged_all.columns:
        last_cached_date = pd.to_datetime(merged_all['Date'], errors='coerce').max()

    # Get equity state (weights) from equity cache if available
    equity_state = {}
    _, eq_meta_path = _equity_cache_paths(slug, basket_type, universe_by_qtr)
    if eq_meta_path.exists():
        try:
            with open(eq_meta_path, 'r', encoding='utf-8') as f:
                eq_meta = json.load(f)
            equity_state = eq_meta.get('state', {})
        except Exception:
            pass

    combined_meta = {
        'schema_version': BASKET_SIGNALS_CACHE_SCHEMA_VERSION,
        'signal_logic_version': EQUITY_SIGNAL_LOGIC_VERSION,
        'universe_logic_version': EQUITY_UNIVERSE_LOGIC_VERSION,
        'data_fingerprint': data_sig.get('fingerprint'),
        'latest_source_date': (
            data_sig['latest_date'].strftime('%Y-%m-%d')
            if pd.notna(data_sig.get('latest_date')) else None
        ),
        'last_cached_date': (
            pd.to_datetime(last_cached_date).strftime('%Y-%m-%d')
            if pd.notna(last_cached_date) else None
        ),
        'universe_signature': universe_sig,
        'basket_type': basket_type,
        'state': equity_state,
    }

    # Save parquet with chart_schema_version embedded
    table = pa.Table.from_pandas(merged_all, preserve_index=False)
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta,
                b'chart_schema_version': str(CHART_SCHEMA_VERSION).encode()}
    pq.write_table(table.replace_schema_metadata(new_meta),
                   parquet_path, compression='snappy')
    WriteThroughPath(parquet_path).sync()
    print(f"Saved: {parquet_path}")

    # Save meta JSON
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(combined_meta, f, indent=2)
    WriteThroughPath(meta_path).sync()
    print(f"Saved: {meta_path}")
    if _do_timing:
        _finalize_basket_signals_output._last_save_time = time.perf_counter() - _t0

    # Save per-constituent contributions (pre-computed from equity OHLC or compute now)
    if _do_timing:
        _t0 = time.perf_counter()
    if contrib_df is None or contrib_df.empty:
        if incremental_dates:
            _compute_and_save_contributions_incremental(
                slug, basket_type, universe_by_qtr, returns_matrix, incremental_dates)
        else:
            _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=returns_matrix)
    else:
        basket_folder_c = _basket_cache_folder(basket_type)
        stem_c = _cache_file_stem(slug, basket_type, universe_by_qtr, 'contributions')
        contrib_path = basket_folder_c / f'{stem_c}.parquet'
        pq.write_table(
            pa.Table.from_pandas(contrib_df, preserve_index=False),
            contrib_path, compression='snappy',
        )
        WriteThroughPath(contrib_path).sync()
        print(f"Saved: {contrib_path}")
    if _do_timing:
        _finalize_basket_signals_output._last_contributions_time = time.perf_counter() - _t0

    return (merged_all, slug, hist_folder, universe_by_qtr)


def _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=None):
    """Compute per-constituent daily weights and contributions, save as parquet.

    Vectorized per-quarter: uses returns_matrix pivot + cumprod weight drift.
    """
    all_df = all_signals_df
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)

    # Build per-quarter initial weights (same logic as compute_equity_ohlc)
    needed_cols = ['Date', 'Ticker', 'Close']
    if 'Volume' in all_df.columns:
        needed_cols.append('Volume')
    df_w = all_df[needed_cols].copy()
    df_w['Date'] = pd.to_datetime(df_w['Date']).dt.normalize()
    df_w = df_w.dropna(subset=['Close'])
    if 'Volume' in df_w.columns:
        df_w['Dollar_Vol'] = df_w['Close'] * df_w['Volume']

    quarter_weights = {}
    if 'Dollar_Vol' in df_w.columns:
        dv_q = (
            df_w[['Date', 'Ticker', 'Dollar_Vol']]
            .dropna(subset=['Dollar_Vol'])
            .groupby(['Ticker', pd.Grouper(key='Date', freq='QE-DEC')])['Dollar_Vol']
            .mean()
        )
    else:
        dv_q = None

    for label in quarter_labels:
        if label not in universe_by_qtr or dv_q is None:
            continue
        prev_universe = universe_by_qtr[label]
        if isinstance(label, str):
            y, q = label.split()
            yr, qn = int(y), int(q.replace("Q", ""))
            if qn == 1:
                ranking_date = _quarter_end_from_key(f"{yr - 1} Q4")
            else:
                ranking_date = _quarter_end_from_key(f"{yr} Q{qn - 1}")
        else:
            ranking_date = label
        weights = {}
        total = 0.0
        for t in prev_universe:
            val = dv_q.get((t, ranking_date), np.nan)
            if pd.notna(val) and val > 0:
                weights[t] = float(val)
                total += float(val)
        if total > 0:
            quarter_weights[label] = {t: v / total for t, v in weights.items()}
    del df_w

    # Build returns matrix if not provided
    if returns_matrix is None:
        _ret_df = all_df[['Date', 'Ticker', 'Close']].copy()
        _ret_df['Date'] = pd.to_datetime(_ret_df['Date']).dt.normalize()
        _ret_df = _ret_df.sort_values(['Ticker', 'Date'])
        _ret_df['Ret'] = _ret_df.groupby('Ticker')['Close'].pct_change()
        returns_matrix = _ret_df.pivot(index='Date', columns='Ticker', values='Ret').sort_index()
        del _ret_df

    # Vectorized per-quarter contribution computation
    quarter_dfs = []

    for q_idx, q_key in enumerate(quarter_labels):
        w_dict = quarter_weights.get(q_key)
        if not w_dict:
            continue
        tickers = [t for t in w_dict if t in returns_matrix.columns]
        if not tickers:
            continue

        # Date range for this quarter (exclude next quarter's start)
        q_start = quarter_ends[q_idx]
        if q_idx + 1 < len(quarter_ends):
            q_end = quarter_ends[q_idx + 1] - pd.Timedelta(days=1)
        else:
            q_end = returns_matrix.index[-1]
        rets_q = returns_matrix.loc[q_start:q_end, tickers].copy()
        if rets_q.empty:
            continue
        rets_q = rets_q.fillna(0.0)

        # Initial weights vector
        w0 = pd.Series({t: w_dict[t] for t in tickers})
        # Normalize in case some tickers are missing from the matrix
        w0 = w0 / w0.sum()

        # Drift weights via cumprod: w_t = w0 * cumprod(1 + ret) / sum(...)
        cum_growth = (1 + rets_q).cumprod()
        # Weighted cum_growth for each ticker
        weighted_cum = cum_growth.multiply(w0, axis=1)
        # Normalize each row to get drifted weights (beginning-of-day for NEXT day)
        row_sums = weighted_cum.sum(axis=1)
        row_sums = row_sums.replace(0, np.nan)
        drifted_weights = weighted_cum.div(row_sums, axis=0)

        # BOD weights: first day uses w0, subsequent days use previous day's drifted weights
        bod_weights = drifted_weights.shift(1)
        bod_weights.iloc[0] = w0

        # Contributions = BOD weight * daily return
        contributions = bod_weights * rets_q

        # Melt to long form
        for col_name, matrix, val_name in [
            ('Weight_BOD', bod_weights, 'Weight_BOD'),
            ('Daily_Return', rets_q, 'Daily_Return'),
            ('Contribution', contributions, 'Contribution'),
        ]:
            pass  # handled below via stack

        # Stack all three matrices at once
        bod_long = bod_weights.stack().rename('Weight_BOD')
        ret_long = rets_q.stack().rename('Daily_Return')
        contrib_long = contributions.stack().rename('Contribution')
        q_df = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
        q_df.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
        quarter_dfs.append(q_df)

    if not quarter_dfs:
        return

    contrib_df = pd.concat(quarter_dfs, ignore_index=True)
    # Remove duplicate dates at quarter boundaries (keep last quarter's values)
    contrib_df = contrib_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    contrib_df = contrib_df.sort_values(['Date', 'Ticker']).reset_index(drop=True)

    basket_folder = _basket_cache_folder(basket_type)
    stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'contributions')
    contrib_path = basket_folder / f'{stem}.parquet'
    pq.write_table(
        pa.Table.from_pandas(contrib_df, preserve_index=False),
        contrib_path, compression='snappy',
    )
    WriteThroughPath(contrib_path).sync()
    print(f"Saved: {contrib_path}")


def _compute_and_save_contributions_incremental(slug, basket_type, universe_by_qtr, returns_matrix, new_dates):
    """Incrementally update contributions parquet for newly appended dates.

    Loads existing contributions, computes only new rows for the current quarter,
    appends, and saves.  Falls back to full recompute if no existing file.
    """
    basket_folder = _basket_cache_folder(basket_type)
    stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'contributions')
    contrib_path = basket_folder / f'{stem}.parquet'

    # Load existing contributions
    if not contrib_path.exists():
        _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=returns_matrix)
        return

    try:
        existing_df = pd.read_parquet(contrib_path)
    except Exception:
        _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=returns_matrix)
        return

    if existing_df.empty:
        _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=returns_matrix)
        return

    new_dates_norm = sorted(pd.to_datetime(d).normalize() for d in new_dates)
    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_qtr)

    # Build quarter weights (same logic as full version)
    all_df = all_signals_df
    needed_cols = ['Date', 'Ticker', 'Close']
    if 'Volume' in all_df.columns:
        needed_cols.append('Volume')
    df_w = all_df[needed_cols].copy()
    df_w['Date'] = pd.to_datetime(df_w['Date']).dt.normalize()
    df_w = df_w.dropna(subset=['Close'])
    if 'Volume' in df_w.columns:
        df_w['Dollar_Vol'] = df_w['Close'] * df_w['Volume']

    quarter_weights = {}
    if 'Dollar_Vol' in df_w.columns:
        dv_q = (
            df_w[['Date', 'Ticker', 'Dollar_Vol']]
            .dropna(subset=['Dollar_Vol'])
            .groupby(['Ticker', pd.Grouper(key='Date', freq='QE-DEC')])['Dollar_Vol']
            .mean()
        )
    else:
        dv_q = None

    for label in quarter_labels:
        if label not in universe_by_qtr or dv_q is None:
            continue
        prev_universe = universe_by_qtr[label]
        if isinstance(label, str):
            y, q = label.split()
            yr, qn = int(y), int(q.replace("Q", ""))
            if qn == 1:
                ranking_date = _quarter_end_from_key(f"{yr - 1} Q4")
            else:
                ranking_date = _quarter_end_from_key(f"{yr} Q{qn - 1}")
        else:
            ranking_date = label
        weights = {}
        total = 0.0
        for t in prev_universe:
            val = dv_q.get((t, ranking_date), np.nan)
            if pd.notna(val) and val > 0:
                weights[t] = float(val)
                total += float(val)
        if total > 0:
            quarter_weights[label] = {t: v / total for t, v in weights.items()}
    del df_w

    # Determine which quarter the new dates fall into
    existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.normalize()
    existing_max_date = existing_df['Date'].max()

    new_rows = []
    # Track drifted weights across consecutive new dates so multi-day appends chain correctly
    _carried_bod = None  # will hold BOD weights for the next date
    _carried_q_key = None

    for d in new_dates_norm:
        if d <= existing_max_date:
            continue  # already in existing data
        q_key = _find_active_quarter(d, quarter_labels, quarter_ends)
        if q_key is None:
            continue
        w_dict = quarter_weights.get(q_key)
        if not w_dict:
            continue
        tickers = [t for t in w_dict if t in returns_matrix.columns]
        if not tickers:
            continue

        # If we have carried-forward weights from the previous new date in the same quarter, use them
        if _carried_bod is not None and _carried_q_key == q_key:
            bod_weights = _carried_bod
        else:
            # Seed from existing cached data
            q_idx = quarter_labels.index(q_key)
            q_start = quarter_ends[q_idx]
            existing_q = existing_df[existing_df['Date'] >= q_start]

            if existing_q.empty:
                # New quarter: use initial weights as BOD weights
                w0 = pd.Series({t: w_dict[t] for t in tickers})
                w0 = w0 / w0.sum()
                bod_weights = w0
            else:
                # Get latest day's drifted weights from existing data
                last_day = existing_q[existing_q['Date'] == existing_q['Date'].max()]
                if last_day.empty:
                    continue
                eod = {}
                for _, row in last_day.iterrows():
                    t = row['Ticker']
                    if t in tickers:
                        eod[t] = row['Weight_BOD'] * (1 + row['Daily_Return'])
                if not eod:
                    continue
                total_eod = sum(eod.values())
                if total_eod == 0:
                    continue
                bod_weights = pd.Series({t: v / total_eod for t, v in eod.items()})

        # Get daily returns for this date
        if d not in returns_matrix.index:
            continue
        day_rets = returns_matrix.loc[d, [t for t in tickers if t in bod_weights.index]].fillna(0.0)

        for t in bod_weights.index:
            if t in day_rets.index:
                new_rows.append({
                    'Date': d,
                    'Ticker': t,
                    'Weight_BOD': bod_weights[t],
                    'Daily_Return': day_rets[t],
                    'Contribution': bod_weights[t] * day_rets[t],
                })

        # Carry forward: compute end-of-day drifted weights for the next date
        eod_weights = bod_weights * (1 + day_rets.reindex(bod_weights.index, fill_value=0.0))
        total_eod = eod_weights.sum()
        if total_eod > 0:
            _carried_bod = eod_weights / total_eod
        else:
            _carried_bod = None
        _carried_q_key = q_key

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        contrib_df = pd.concat([existing_df, new_df], ignore_index=True)
        contrib_df = contrib_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        contrib_df = contrib_df.sort_values(['Date', 'Ticker']).reset_index(drop=True)
    else:
        contrib_df = existing_df

    pq.write_table(
        pa.Table.from_pandas(contrib_df, preserve_index=False),
        contrib_path, compression='snappy',
    )
    WriteThroughPath(contrib_path).sync()
    print(f"Saved: {contrib_path}")


def _record_basket_timing(name, **timings):
    """Record per-step timings for a basket into the global accumulator."""
    _basket_timing_names.append(name)
    for k in _basket_timing:
        _basket_timing[k].append(timings.get(k, 0.0))
    parts = " | ".join(f"{k}={timings.get(k, 0.0):.1f}s" for k in _basket_timing if k != 'total')
    print(f"  [TIMING] {name}: total={timings.get('total', 0.0):.1f}s | {parts}")


def process_basket_signals(name, universe_by_qtr, charts_folder, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None):
    if not universe_by_qtr:
        print(f"[{name}] skipped (no universe data)")
        return

    _do_timing = BENCHMARK_TIMING or BENCHMARK_BASKETS > 0
    _t_basket_start = time.perf_counter()

    slug = _cache_slugify_label(name)
    basket_ticker = slug.upper()
    hist_folder = charts_folder / f'{slug.lower()}_historical'

    # --- basket cache check (new single-file format) ---
    if _do_timing:
        _t0 = time.perf_counter()
    _bsig_data_sig     = _get_data_signature(all_signals_df)
    _bsig_universe_sig = _build_universe_signature(universe_by_qtr)

    # Try loading from new consolidated basket file first
    _existing_pq = _find_basket_parquet(slug)
    _existing_meta_path = _find_basket_meta(slug)
    _cached_merged = None
    _cached_meta = None
    if _existing_pq and _existing_meta_path:
        try:
            _cached_merged = pd.read_parquet(_existing_pq)
            with open(_existing_meta_path, 'r', encoding='utf-8') as f:
                _cached_meta = json.load(f)
        except Exception:
            _cached_merged, _cached_meta = None, None

    # Strip live rows from cached basket before using as cache base
    if _cached_merged is not None and 'Source' in _cached_merged.columns:
        _cached_merged = _cached_merged[_cached_merged['Source'] != 'live'].copy()

    cache_valid = (
        not FORCE_REBUILD_BASKET_SIGNALS
        and _cached_merged is not None
        and _is_basket_signals_cache_valid(_cached_meta, _bsig_data_sig, _bsig_universe_sig)
    )
    cached_last = pd.to_datetime((_cached_meta or {}).get('last_cached_date'), errors='coerce')
    latest_source = pd.to_datetime(_bsig_data_sig.get('latest_date'), errors='coerce')
    if _do_timing:
        _t_cache_check = time.perf_counter() - _t0
    if cache_valid and pd.notna(cached_last) and pd.notna(latest_source) and cached_last.normalize() >= latest_source.normalize():
        if _do_timing:
            _record_basket_timing(name, cache_check=_t_cache_check, total=time.perf_counter() - _t_basket_start)
        return (_cached_merged, slug, hist_folder, universe_by_qtr)
    # --- end cache check ---

    if _do_timing:
        _t0 = time.perf_counter()
    ohlc_df, _contrib_df = compute_equity_ohlc_cached(
        all_signals_df, universe_by_qtr, name, slug, basket_type,
        returns_matrix=returns_matrix, ohlc_ret_matrices=ohlc_ret_matrices,
    )
    if _do_timing:
        _t_equity_ohlc = time.perf_counter() - _t0
    if ohlc_df.empty:
        print(f"[{name}] skipped (no OHLC data)")
        return

    if cache_valid and pd.notna(cached_last):
        appended_ohlc = (
            ohlc_df[pd.to_datetime(ohlc_df['Date']).dt.normalize() > cached_last.normalize()]
            .sort_values('Date')
            .reset_index(drop=True)
        )
        if not appended_ohlc.empty:
            if _do_timing:
                _t0 = time.perf_counter()
            prev_row = _cached_merged.sort_values('Date').iloc[-1]
            appended_rows = []
            for _, r in appended_ohlc.iterrows():
                next_row = _build_signals_next_row(
                    prev_row,
                    live_price=float(r['Close']),
                    live_dt=pd.to_datetime(r['Date']).normalize(),
                    live_high=float(r['High']),
                    live_low=float(r['Low']),
                    live_open=float(r['Open']),
                )
                if next_row is None:
                    appended_rows = []
                    break
                next_row['Ticker'] = basket_ticker
                next_row['Volume'] = 0
                appended_rows.append(next_row)
                prev_row = next_row
            if _do_timing:
                _t_build_signals = time.perf_counter() - _t0
            if appended_rows:
                appended_signals_df = pd.DataFrame(appended_rows)
                appended_merged = _augment_basket_signals_with_breadth(appended_signals_df, universe_by_qtr)
                if _do_timing:
                    _t_breadth_trend = getattr(_augment_basket_signals_with_breadth, '_last_trend_time', 0.0)
                    _t_breadth_breakout = getattr(_augment_basket_signals_with_breadth, '_last_breakout_time', 0.0)
                    _t_breadth_merge = getattr(_augment_basket_signals_with_breadth, '_last_merge_time', 0.0)
                merged_all = (
                    pd.concat([_cached_merged, appended_merged], ignore_index=True)
                    .drop_duplicates(subset=['Date'], keep='last')
                    .sort_values('Date')
                    .reset_index(drop=True)
                )
                _new_dates = sorted(pd.to_datetime(appended_ohlc['Date']).dt.normalize().unique())
                result = _finalize_basket_signals_output(
                    name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig, universe_by_qtr, basket_type,
                    returns_matrix=returns_matrix, contrib_df=_contrib_df,
                    incremental_dates=_new_dates,
                )
                if _do_timing:
                    _record_basket_timing(
                        name,
                        cache_check=_t_cache_check, equity_ohlc=_t_equity_ohlc,
                        build_signals=_t_build_signals,
                        breadth_trend=_t_breadth_trend, breadth_breakout=_t_breadth_breakout,
                        breadth_merge=_t_breadth_merge,
                        breadth_pivots=getattr(_finalize_basket_signals_output, '_last_pivots_time', 0.0),
                        correlation=getattr(_finalize_basket_signals_output, '_last_correlation_time', 0.0),
                        save_parquet=getattr(_finalize_basket_signals_output, '_last_save_time', 0.0),
                        contributions=getattr(_finalize_basket_signals_output, '_last_contributions_time', 0.0),
                        total=time.perf_counter() - _t_basket_start,
                    )
                return result

    if _do_timing:
        _t0 = time.perf_counter()
    ohlc_df = ohlc_df.copy()
    ohlc_df['Volume'] = 0
    signals_df = _build_signals_from_df(ohlc_df.set_index('Date'), basket_ticker)
    if _do_timing:
        _t_build_signals = time.perf_counter() - _t0
    if signals_df is None or signals_df.empty:
        return
    merged_all = _augment_basket_signals_with_breadth(signals_df, universe_by_qtr)
    if _do_timing:
        _t_breadth_trend = getattr(_augment_basket_signals_with_breadth, '_last_trend_time', 0.0)
        _t_breadth_breakout = getattr(_augment_basket_signals_with_breadth, '_last_breakout_time', 0.0)
        _t_breadth_merge = getattr(_augment_basket_signals_with_breadth, '_last_merge_time', 0.0)
    result = _finalize_basket_signals_output(
        name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig, universe_by_qtr, basket_type,
        returns_matrix=returns_matrix, contrib_df=_contrib_df,
    )
    if _do_timing:
        _record_basket_timing(
            name,
            cache_check=_t_cache_check, equity_ohlc=_t_equity_ohlc,
            build_signals=_t_build_signals,
            breadth_trend=_t_breadth_trend, breadth_breakout=_t_breadth_breakout,
            breadth_merge=_t_breadth_merge,
            breadth_pivots=getattr(_finalize_basket_signals_output, '_last_pivots_time', 0.0),
            correlation=getattr(_finalize_basket_signals_output, '_last_correlation_time', 0.0),
            save_parquet=getattr(_finalize_basket_signals_output, '_last_save_time', 0.0),
            contributions=getattr(_finalize_basket_signals_output, '_last_contributions_time', 0.0),
            total=time.perf_counter() - _t_basket_start,
        )
    return result


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    reset_cell_timer("Basket Signal Generation")

    # Load universes from cache
    universes = load_all_universes()
    QUARTER_UNIVERSE = universes['QUARTER_UNIVERSE']
    print(f"Loaded universe: {len(QUARTER_UNIVERSE)} quarters")

    # Load signals from cache (filter out live rows)
    all_signals_df = pd.read_parquet(SIGNALS_CACHE_FILE)
    if 'Source' in all_signals_df.columns:
        all_signals_df = all_signals_df[all_signals_df['Source'] != 'live'].copy()
    all_signals_df['Date'] = pd.to_datetime(all_signals_df['Date'])
    print(f"Loaded signals: {len(all_signals_df)} rows, {all_signals_df['Ticker'].nunique()} tickers")

    # Reset data signature cache so _get_data_signature picks up fresh data
    reset_data_signature_cache()

    # --- Build returns matrix (Date x Ticker) for basket correlation ---
    print("Building basket signals (consolidated: OHLC + signals + breadth + correlation)...")

    _ret_matrix_path = DATA_FOLDER / 'returns_matrix_500.parquet'
    _ret_fingerprint = f"{all_signals_df.shape}_{all_signals_df['Date'].max()}_{sorted(all_signals_df['Ticker'].unique())}"
    _ret_fp_hash = hashlib.md5(_ret_fingerprint.encode()).hexdigest()
    _ret_fp_path = DATA_FOLDER / 'returns_matrix_500.fingerprint'
    _rebuild_ret_matrix = True
    if _ret_matrix_path.exists() and _ret_fp_path.exists():
        try:
            if _ret_fp_path.read_text().strip() == _ret_fp_hash:
                returns_matrix = pd.read_parquet(_ret_matrix_path)
                _rebuild_ret_matrix = False
                print(f"  Returns matrix loaded from cache ({returns_matrix.shape})")
                # Build OHLC returns matrices (not cached -- fast pivot from all_signals_df)
                _t0_ohlc_rm = time.perf_counter()
                _ret_df = all_signals_df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close']].copy()
                _ret_df['Date'] = pd.to_datetime(_ret_df['Date']).dt.normalize()
                _ret_df = _ret_df.sort_values(['Ticker', 'Date'])
                _prev_close = _ret_df.groupby('Ticker')['Close'].shift(1)
                _ret_df['Open_Ret'] = (_ret_df['Open'] / _prev_close) - 1
                _ret_df['High_Ret'] = (_ret_df['High'] / _prev_close) - 1
                _ret_df['Low_Ret'] = (_ret_df['Low'] / _prev_close) - 1
                ohlc_ret_matrices = {
                    'Open_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='Open_Ret').sort_index(),
                    'High_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='High_Ret').sort_index(),
                    'Low_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='Low_Ret').sort_index(),
                }
                del _ret_df, _prev_close
                print(f"  OHLC returns matrices built in {time.perf_counter() - _t0_ohlc_rm:.1f}s")
        except Exception:
            pass
    if _rebuild_ret_matrix:
        _t0_rm = time.perf_counter()
        _ret_df = all_signals_df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close']].copy()
        _ret_df['Date'] = pd.to_datetime(_ret_df['Date']).dt.normalize()
        _ret_df = _ret_df.sort_values(['Ticker', 'Date'])
        _ret_df['Ret'] = _ret_df.groupby('Ticker')['Close'].pct_change()
        _prev_close = _ret_df.groupby('Ticker')['Close'].shift(1)
        _ret_df['Open_Ret'] = (_ret_df['Open'] / _prev_close) - 1
        _ret_df['High_Ret'] = (_ret_df['High'] / _prev_close) - 1
        _ret_df['Low_Ret'] = (_ret_df['Low'] / _prev_close) - 1
        returns_matrix = _ret_df.pivot(index='Date', columns='Ticker', values='Ret').sort_index()
        ohlc_ret_matrices = {
            'Open_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='Open_Ret').sort_index(),
            'High_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='High_Ret').sort_index(),
            'Low_Ret': _ret_df.pivot(index='Date', columns='Ticker', values='Low_Ret').sort_index(),
        }
        returns_matrix.to_parquet(_ret_matrix_path, engine='pyarrow')
        _ret_fp_path.write_text(_ret_fp_hash)
        print(f"  Returns matrix built and cached ({returns_matrix.shape}) in {time.perf_counter() - _t0_rm:.1f}s")
        del _ret_df, _prev_close

    # Process all baskets
    all_baskets = build_all_basket_specs(universes)
    total_baskets = len(all_baskets)
    BASKET_RESULTS = {}
    last_milestone = 0
    for i, (basket_name, basket_universe, basket_folder, basket_type) in enumerate(all_baskets, start=1):
        result = process_basket_signals(basket_name, basket_universe, basket_folder, basket_type,
                                         returns_matrix=returns_matrix, ohlc_ret_matrices=ohlc_ret_matrices)
        if result is not None:
            BASKET_RESULTS[basket_name] = result
        percent = int((i / total_baskets) * 100)
        milestone = percent // 10 * 10
        if milestone > last_milestone and milestone % 10 == 0:
            print(f"  {milestone}% complete ({i} / {total_baskets} baskets)")
            last_milestone = milestone
        if BENCHMARK_BASKETS > 0 and i >= BENCHMARK_BASKETS:
            print(f"  BENCHMARK_BASKETS={BENCHMARK_BASKETS} limit reached, stopping early.")
            break

    print(f"Basket signals complete: {len(BASKET_RESULTS)} / {total_baskets} baskets processed.")

    # --- Timing summary ---
    if (BENCHMARK_TIMING or BENCHMARK_BASKETS > 0) and _basket_timing_names:
        _steps = ['cache_check', 'equity_ohlc', 'build_signals', 'breadth_trend',
                  'breadth_breakout', 'breadth_merge', 'breadth_pivots',
                  'correlation', 'contributions', 'save_parquet', 'total']
        _hdr = f"{'Basket':<30s}" + "".join(f"{s:>15s}" for s in _steps)
        print("\n" + "=" * len(_hdr))
        print("BASKET PROCESSING TIMING SUMMARY")
        print("=" * len(_hdr))
        print(_hdr)
        print("-" * len(_hdr))
        for _idx, _bname in enumerate(_basket_timing_names):
            _row = f"{_bname:<30s}"
            for _s in _steps:
                _val = _basket_timing[_s][_idx] if _idx < len(_basket_timing[_s]) else 0.0
                _row += f"{_val:>14.1f}s"
            print(_row)
        print("-" * len(_hdr))
        _row_mean = f"{'MEAN':<30s}"
        _row_total = f"{'TOTAL':<30s}"
        for _s in _steps:
            _vals = _basket_timing[_s]
            if _vals:
                _row_mean += f"{sum(_vals)/len(_vals):>14.1f}s"
                _row_total += f"{sum(_vals):>14.1f}s"
            else:
                _row_mean += f"{'N/A':>15s}"
                _row_total += f"{'N/A':>15s}"
        print(_row_mean)
        print(_row_total)
        _total_time = sum(_basket_timing['total']) if _basket_timing['total'] else 1.0
        _row_pct = f"{'% OF TOTAL':<30s}"
        for _s in _steps:
            if _s == 'total':
                _row_pct += f"{'100.0%':>15s}"
            else:
                _vals = _basket_timing[_s]
                _pct = (sum(_vals) / _total_time * 100) if _vals and _total_time > 0 else 0.0
                _row_pct += f"{_pct:>14.1f}%"
        print(_row_pct)
        print("=" * len(_hdr))
