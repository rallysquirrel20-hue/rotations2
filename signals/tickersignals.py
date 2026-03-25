"""Ticker Signal Generation — Phase 2
Builds/updates daily signal caches for 500 stocks and 50 ETFs.
Run: python tickersignals.py
"""
from foundation import (
    # constants
    SIZE, SIGNALS, RV_MULT, EMA_MULT, RV_EMA_ALPHA,
    INCREMENTAL_MAX_DAYS,
    # paths
    DATA_FOLDER, SIGNALS_CACHE_FILE, ETF_SIGNALS_CACHE_FILE,
    # signal engine
    _build_signals_from_df, _build_signals_next_row,
    _get_latest_norgate_date,
    # universe loaders
    load_universe_from_cache, load_etf_universe_from_cache,
    get_current_quarter_key,
    # utilities
    WriteThroughPath, reset_cell_timer,
)
import norgatedata
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
import os
from concurrent.futures import ThreadPoolExecutor


# ---------------------------------------------------------------------------
# Module-level state (set in __main__)
# ---------------------------------------------------------------------------
QUARTER_UNIVERSE = {}
ETF_UNIVERSE = {}
all_signals_df = None
etf_signals_df = None

# Timing reference for full rebuilds
cell3_start_time = None


# ---------------------------------------------------------------------------
# Equity signal generation
# ---------------------------------------------------------------------------

def build_signals_for_ticker(ticker):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or df.empty:
        return None
    return _build_signals_from_df(df, ticker)


def _build_signals_append_ticker(ticker, cached_last_row, limit=2):
    """Fetch the latest Norgate bars for `ticker`, detect TOTALRETURN adjustments,
    and either return new incremental row(s) or signal that a full rebuild is needed.

    Returns one of:
      ('append', list[pd.Series]) — no adjustment; new EOD row(s) computed
      ('full_rebuild', str)       — reason string for diagnostics
      ('no_new_data', None)       — cache already has the latest date
      ('error', str)              — exception description
    """
    try:
        df = norgatedata.price_timeseries(
            ticker,
            stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
            padding_setting=norgatedata.PaddingType.NONE,
            timeseriesformat='pandas-dataframe',
            limit=limit,
        )
        if df is None or len(df) < 1:
            return ('error', 'no_data_returned')

        df = df.reset_index()
        date_col = df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
        df = df.rename(columns={date_col: 'Date'})
        df = df.sort_values('Date').reset_index(drop=True)

        cached_date = pd.to_datetime(cached_last_row['Date']).normalize()

        # Find the Norgate bar matching the cached date for adjustment detection
        cached_date_bar = df[df['Date'] == cached_date]
        if cached_date_bar.empty:
            # Cached date not in Norgate's returned bars
            newest_date = df.iloc[-1]['Date'].normalize()
            if newest_date <= cached_date:
                return ('no_new_data', None)
            return ('full_rebuild', f'date_not_found: cached={cached_date.date()}, norgate_range={df.iloc[0]["Date"].date()}..{newest_date.date()}')

        # Adjustment detection: compare Norgate's close for cached date vs our cached close
        norgate_close = float(cached_date_bar.iloc[0]['Close'])
        cached_close = float(cached_last_row['Close'])
        if cached_close != 0:
            rel_diff = abs(norgate_close - cached_close) / abs(cached_close)
            if rel_diff > 1e-5:
                return ('full_rebuild', f'price_adj: diff={rel_diff:.8f} cached={cached_close:.6f} norgate={norgate_close:.6f}')

        # Find all bars newer than the cached date
        new_bars = df[df['Date'] > cached_date].sort_values('Date')
        if new_bars.empty:
            return ('no_new_data', None)

        # Sequentially append each new bar
        current_row = cached_last_row
        all_new_rows = []
        for _, bar in new_bars.iterrows():
            bar_close = float(bar['Close'])
            bar_high  = float(bar['High'])  if 'High'   in bar.index else bar_close
            bar_low   = float(bar['Low'])   if 'Low'    in bar.index else bar_close
            bar_open  = float(bar['Open'])  if 'Open'   in bar.index else bar_close
            bar_vol   = int(bar['Volume'])  if 'Volume' in bar.index else 0

            new_row = _build_signals_next_row(
                current_row,
                live_price=bar_close,
                live_dt=bar['Date'].normalize(),
                live_high=bar_high,
                live_low=bar_low,
                live_open=bar_open,
            )
            if new_row is None:
                return ('error', f'next_row_failed on {bar["Date"].date()}')

            new_row['Volume'] = bar_vol
            all_new_rows.append(new_row)
            current_row = new_row

        return ('append', all_new_rows)

    except Exception as exc:
        return ('error', str(exc))


def _incremental_update_signals(cached_df, days_stale=1):
    """Append new trading day(s) to the signals cache without a full rebuild.

    For tickers with TOTALRETURN adjustments (~5-50/day), falls back to
    build_signals_for_ticker (full rebuild per ticker). For the remaining
    ~2600+ tickers, uses _build_signals_append_ticker (fetch 2 bars, append 1 row).
    """
    inc_start = time.time()
    print("=" * 60)
    print("Running incremental signals update...")

    all_tickers = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    universe_set = set(all_tickers)

    cached_df = cached_df.copy()
    # Drop any live rows from the cached data before combining with new Norgate rows
    if 'Source' in cached_df.columns:
        cached_df = cached_df[cached_df['Source'] != 'live'].copy()
    cached_df['Date'] = pd.to_datetime(cached_df['Date'])
    last_rows = {}
    for ticker_val, grp in cached_df.groupby('Ticker', sort=False):
        last_rows[ticker_val] = grp.sort_values('Date').iloc[-1]

    cached_tickers  = set(last_rows.keys())
    new_tickers     = universe_set - cached_tickers      # need full rebuild (new entrants)
    dropped_tickers = cached_tickers - universe_set      # preserve as-is, no append

    print(f"  Universe: {len(universe_set)} | Cached: {len(cached_tickers)} | "
          f"New: {len(new_tickers)} | Dropped: {len(dropped_tickers)}")

    check_tickers = [t for t in all_tickers if t in cached_tickers]
    append_rows    = {}   # ticker -> list of pd.Series
    rebuild_tickers = list(new_tickers)
    rebuild_info = {}     # ticker -> reason string (for diagnostics)

    def _safe_append(t):
        return t, _build_signals_append_ticker(t, last_rows[t], limit=days_stale + 1)

    total_check = len(check_tickers)
    processed = 0
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_append, t) for t in check_tickers]
        for fut in futures:
            t, (action, payload) = fut.result()
            processed += 1
            percent = int((processed / total_check) * 100) if total_check else 100
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  Append check {milestone}% ({processed}/{total_check})")
                last_milestone = milestone
            if action == 'append':
                append_rows[t] = payload  # list of pd.Series
            elif action == 'full_rebuild':
                rebuild_tickers.append(t)
                rebuild_info[t] = payload or 'unknown'
            elif action == 'error':
                rebuild_tickers.append(t)
                rebuild_info[t] = f'error: {payload}'
            # 'no_new_data' -> ticker halted or cache already current; skip

    print(f"  Append: {len(append_rows)} tickers | Full rebuild: {len(rebuild_tickers)} tickers")

    # Diagnostic breakdown of rebuild reasons
    if rebuild_info:
        reason_counts = {'date_not_found': 0, 'price_adj': 0, 'error': 0, 'other': 0}
        reason_examples = {'date_not_found': [], 'price_adj': [], 'error': [], 'other': []}
        for t, reason in rebuild_info.items():
            matched = False
            for key in ['date_not_found', 'price_adj', 'error']:
                if reason.startswith(key):
                    reason_counts[key] += 1
                    if len(reason_examples[key]) < 3:
                        reason_examples[key].append(f"{t}: {reason}")
                    matched = True
                    break
            if not matched:
                reason_counts['other'] += 1
                if len(reason_examples['other']) < 3:
                    reason_examples['other'].append(f"{t}: {reason}")
        print(f"  Rebuild breakdown: new={len(new_tickers)} | {reason_counts}")
        for key, examples in reason_examples.items():
            for ex in examples:
                print(f"    [{key}] {ex}")

    # Full rebuild for adjusted + new tickers
    rebuild_results = {}
    if rebuild_tickers:
        print(f"  Rebuilding {len(rebuild_tickers)} tickers (adjustments + new)...")
        rb_total = len(rebuild_tickers)
        rb_processed = 0
        rb_last_milestone = 0

        def _safe_rebuild(t):
            try:
                return t, build_signals_for_ticker(t)
            except Exception:
                return t, None

        with ThreadPoolExecutor(max_workers=4) as ex:
            rb_futures = [ex.submit(_safe_rebuild, t) for t in rebuild_tickers]
            for fut in rb_futures:
                t, result = fut.result()
                rb_processed += 1
                percent = int((rb_processed / rb_total) * 100) if rb_total else 100
                milestone = percent // 10 * 10
                if milestone > rb_last_milestone and milestone % 10 == 0:
                    print(f"  Rebuild {milestone}% ({rb_processed}/{rb_total})")
                    rb_last_milestone = milestone
                if result is not None:
                    rebuild_results[t] = result

    # Assemble updated DataFrame
    keep_set = set(cached_df.columns)

    if rebuild_results:
        cached_df = cached_df[~cached_df['Ticker'].isin(rebuild_results.keys())].copy()
        rebuild_frames = []
        for t, df_rebuilt in rebuild_results.items():
            trim_cols = [c for c in df_rebuilt.columns if c in keep_set]
            if trim_cols:
                trimmed = df_rebuilt[trim_cols].dropna(axis=1, how='all')
                if not trimmed.empty:
                    rebuild_frames.append(trimmed)
        if rebuild_frames:
            cached_df = pd.concat([cached_df] + rebuild_frames, ignore_index=True)

    if append_rows:
        new_rows_list = []
        for t, row_list in append_rows.items():
            for row_series in row_list:
                row_dict = {k: v for k, v in row_series.items() if k in keep_set}
                new_rows_list.append(row_dict)
        new_rows_df = pd.DataFrame(new_rows_list,
                                   columns=[c for c in cached_df.columns if c in keep_set])
        cached_df = pd.concat([cached_df, new_rows_df], ignore_index=True)

    # Apply dtype optimizations (same as full rebuild) to keep parquet schema consistent
    bool_cols = ['Is_Up_Rotation', 'Is_Down_Rotation', 'Is_Breakout', 'Is_Breakdown',
                 'Is_BTFD', 'Is_STFR', 'Is_Breakout_Sequence', 'BTFD_Triggered', 'STFR_Triggered']
    for col in bool_cols:
        if col in cached_df.columns:
            cached_df[col] = cached_df[col].fillna(False).astype(bool)

    if 'Trend' in cached_df.columns:
        # New rows have bool Trend; cached rows have float32. Normalize to float32.
        cached_df['Trend'] = cached_df['Trend'].map(
            {True: 1.0, False: 0.0, 1.0: 1.0, 0.0: 0.0, None: float('nan')}
        ).astype('float32')

    if 'Rotation_ID' in cached_df.columns:
        cached_df['Rotation_ID'] = cached_df['Rotation_ID'].fillna(0).astype('int32')

    float32_prefixes = ['Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
                        'Avg_MFE', 'Avg_MAE', 'Historical_EV', 'Std_Dev', 'Risk_Adj_EV',
                        'EV_Last_3', 'Risk_Adj_EV_Last_3', 'Count']
    for col in cached_df.columns:
        for prefix in float32_prefixes:
            if col.endswith(prefix) and cached_df[col].dtype == 'float64':
                cached_df[col] = cached_df[col].astype('float32')
                break

    cached_df['Source'] = 'norgate'

    # Atomic save: write to temp file then replace (safe against crash mid-write)
    tmp_path = SIGNALS_CACHE_FILE.with_suffix('.parquet.tmp')
    cached_df.to_parquet(tmp_path, index=False, compression='snappy')
    tmp_path.replace(SIGNALS_CACHE_FILE)
    WriteThroughPath(SIGNALS_CACHE_FILE).sync()

    # Drop Source from in-memory DF — only needed on disk, not for basket processing
    cached_df = cached_df.drop(columns=['Source'])

    print(f"  Incremental update complete: {len(cached_df)} total rows "
          f"({time.time() - inc_start:.1f}s)")
    print("=" * 60)
    return cached_df


def _signals_cache_is_current(df):
    if df is None or df.empty or 'Date' not in df.columns:
        return False

    # Pre-convert to datetime if still string/object to speed up comparisons
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])

    # If live rows are present at the latest date, cache is not current — Norgate should rebuild/replace
    if 'Source' in df.columns:
        latest_cache_date_all = df['Date'].max()
        if pd.notna(latest_cache_date_all):
            latest_rows = df[df['Date'] == latest_cache_date_all]
            if (latest_rows['Source'] == 'live').any():
                return False

    latest_cache_date = df['Date'].max()
    if pd.isna(latest_cache_date):
        return False

    latest_norgate_date = _get_latest_norgate_date()
    if latest_norgate_date is None:
        return False  # Can't determine, rebuild to be safe

    if latest_cache_date.normalize() < latest_norgate_date:
        return False

    # Check required stat columns exist; stale caches built before new columns were added won't have them.
    _required_stats = ['Avg_Winner_Bars', 'Avg_Loser_Bars']
    for sig in ['Up_Rot', 'Breakout', 'Down_Rot', 'Breakdown', 'BTFD', 'STFR']:
        if f'{sig}_Win_Rate' in df.columns:
            if any(f'{sig}_{s}' not in df.columns for s in _required_stats):
                return False
            break

    return True


def load_or_build_signals():
    global cell3_start_time
    cell3_start_time = time.time()

    if SIGNALS_CACHE_FILE.exists():
        cached = pd.read_parquet(SIGNALS_CACHE_FILE)
        # Always convert Date to datetime immediately to save memory and speed up filter calls
        if 'Date' in cached.columns and not pd.api.types.is_datetime64_any_dtype(cached['Date']):
            cached['Date'] = pd.to_datetime(cached['Date'])

        if _signals_cache_is_current(cached):
            print("Signals loaded from cache (up to date)")
            if 'Source' in cached.columns:
                del cached['Source']
            return cached

        # Strip live rows — Norgate rebuild/incremental will replace them
        if 'Source' in cached.columns:
            cached = cached[cached['Source'] != 'live'].copy()

        # Compute staleness (days) and column schema validity inline.
        # This avoids a second Norgate SPY call that _signals_cache_is_current already made.
        days_stale = None
        columns_ok = True
        if 'Date' in cached.columns and not cached.empty:
            latest_cache_date   = cached['Date'].max().normalize()
            latest_norgate_date = _get_latest_norgate_date()
            if latest_norgate_date is not None and not pd.isna(latest_cache_date):
                days_stale = int(np.busday_count(latest_cache_date.date(), latest_norgate_date.date()))
        _required_stats = ['Avg_Winner_Bars', 'Avg_Loser_Bars']
        for sig in SIGNALS:
            if f'{sig}_Win_Rate' in cached.columns:
                if any(f'{sig}_{s}' not in cached.columns for s in _required_stats):
                    columns_ok = False
                break

        if days_stale is not None and days_stale == 0 and columns_ok:
            print("Signals loaded from cache (up to date after stripping live rows)")
            return cached
        elif (days_stale is not None and 1 <= days_stale <= INCREMENTAL_MAX_DAYS and columns_ok):
            print(f"Signals cache stale by {days_stale} trading day(s), running incremental update...")
            try:
                res = _incremental_update_signals(cached, days_stale=days_stale)
                if res['Date'].dtype == object:
                    res['Date'] = pd.to_datetime(res['Date'])
                return res
            except Exception as exc:
                print(f"Incremental update failed ({exc}), falling back to full rebuild...")
        else:
            if not columns_ok:
                print("Signals cache outdated (schema changed), full rebuild...")
            else:
                print(f"Signals cache outdated (stale by {days_stale} days), full rebuild...")
    else:
        print("No signals cache found, building from scratch...")

    all_tickers = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Total unique tickers in universe: {len(all_tickers)}")
    print("=" * 60)

    # Pre-compute keep columns for early trimming (7c: reduce peak memory)
    base_keep = [
        'Date', 'Ticker',
        'Open', 'High', 'Low', 'Close', 'Volume',
        'RV', 'RV_EMA', 'Trend',
        'Resistance_Pivot', 'Support_Pivot',
        'Rotation_Open', 'Up_Range', 'Down_Range',
        'Up_Range_EMA', 'Down_Range_EMA',
        'Upper_Target', 'Lower_Target',
        'Is_Up_Rotation', 'Is_Down_Rotation',
        'Is_Breakout', 'Is_Breakdown', 'Is_BTFD', 'Is_STFR',
        'Is_Breakout_Sequence',
        'Rotation_ID', 'BTFD_Triggered', 'STFR_Triggered',
    ]
    signal_cols = []
    for sig in SIGNALS:
        signal_cols.extend([
            f'{sig}_Entry_Price', f'{sig}_Exit_Date', f'{sig}_Exit_Price',
            f'{sig}_Final_Change', f'{sig}_MFE', f'{sig}_MAE',
            f'{sig}_Win_Rate', f'{sig}_Avg_Winner', f'{sig}_Avg_Loser',
            f'{sig}_Avg_Winner_Bars', f'{sig}_Avg_Loser_Bars',
            f'{sig}_Avg_MFE', f'{sig}_Avg_MAE',
            f'{sig}_Std_Dev', f'{sig}_Historical_EV', f'{sig}_EV_Last_3',
            f'{sig}_Risk_Adj_EV', f'{sig}_Risk_Adj_EV_Last_3', f'{sig}_Count'
        ])
    keep_set = set(base_keep + signal_cols)

    all_signals = []
    failed_tickers = []

    def _safe_build(t):
        try:
            return t, build_signals_for_ticker(t)
        except Exception:
            return t, None

    max_workers = min(8, (os.cpu_count() or 4))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_safe_build, t) for t in all_tickers]
        processed = 0
        total = len(all_tickers)
        last_milestone = 0
        for fut in futures:
            t, result = fut.result()
            processed += 1

            percent = int((processed / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({processed} / {total} stocks)")
                last_milestone = milestone

            if result is not None:
                # Trim columns early to reduce peak memory (7c)
                trim_cols = [c for c in result.columns if c in keep_set]
                if not trim_cols:
                    failed_tickers.append(t)
                    continue
                trimmed = result[trim_cols].dropna(axis=1, how='all')
                # Avoid concat deprecation warning from empty/all-NA entries.
                if trimmed.empty:
                    failed_tickers.append(t)
                    continue
                all_signals.append(trimmed)
            else:
                failed_tickers.append(t)

    if not all_signals:
        raise ValueError("No signals generated for any tickers.")

    print("=" * 60)
    print(f"Signal generation complete")
    print(f"  Successful: {len(all_signals)} tickers")
    print(f"  Failed: {len(failed_tickers)} tickers")
    print(f"  Total time: {time.time() - cell3_start_time:.1f}s")
    print("=" * 60)

    concat_start = time.time()
    print("Concatenating results...")
    clean_frames = [df.dropna(axis=1, how='all') for df in all_signals if not df.empty]
    all_signals_df = pd.concat(clean_frames, ignore_index=True)
    del all_signals  # Free intermediate list
    del clean_frames
    all_signals_df = all_signals_df.dropna(axis=1, how='all')
    print(f"  Concatenation complete: {len(all_signals_df)} rows ({time.time() - concat_start:.1f}s)")

    # Dtype optimization (7b): downcast to save ~50% memory
    bool_cols = ['Is_Up_Rotation', 'Is_Down_Rotation', 'Is_Breakout', 'Is_Breakdown',
                 'Is_BTFD', 'Is_STFR', 'Is_Breakout_Sequence', 'BTFD_Triggered', 'STFR_Triggered']
    for col in bool_cols:
        if col in all_signals_df.columns:
            all_signals_df[col] = all_signals_df[col].fillna(False).astype(bool)

    if 'Trend' in all_signals_df.columns:
        all_signals_df['Trend'] = all_signals_df['Trend'].map({True: 1.0, False: 0.0, None: np.nan}).astype('float32')

    if 'Rotation_ID' in all_signals_df.columns:
        all_signals_df['Rotation_ID'] = all_signals_df['Rotation_ID'].fillna(0).astype('int32')

    float32_prefixes = ['Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars', 'Avg_MFE', 'Avg_MAE',
                        'Historical_EV', 'Std_Dev', 'Risk_Adj_EV', 'EV_Last_3',
                        'Risk_Adj_EV_Last_3', 'Count']
    for col in all_signals_df.columns:
        for prefix in float32_prefixes:
            if col.endswith(prefix) and all_signals_df[col].dtype == 'float64':
                all_signals_df[col] = all_signals_df[col].astype('float32')
                break

    all_signals_df = all_signals_df.copy()  # defragment after dtype optimizations
    all_signals_df['Source'] = 'norgate'
    all_signals_df.to_parquet(SIGNALS_CACHE_FILE, index=False, compression='snappy')
    print(f"Saved: {SIGNALS_CACHE_FILE} ({len(all_signals_df)} rows, {all_signals_df['Ticker'].nunique()} tickers)")
    # Drop Source from in-memory DF — only needed on disk, not for basket processing
    del all_signals_df['Source']
    return all_signals_df


# ---------------------------------------------------------------------------
# ETF signal generation
# ---------------------------------------------------------------------------

def _etf_signals_cache_is_current(df):
    """Same logic as _signals_cache_is_current but for the ETF parquet."""
    if df is None or df.empty or 'Date' not in df.columns:
        return False
    if 'Source' in df.columns:
        latest_all = pd.to_datetime(df['Date']).max()
        if pd.notna(latest_all):
            latest_rows = df[pd.to_datetime(df['Date']) == latest_all]
            if (latest_rows['Source'] == 'live').any():
                return False
    latest_cache_date = pd.to_datetime(df['Date']).max()
    if pd.isna(latest_cache_date):
        return False
    latest_norgate_date = _get_latest_norgate_date()
    if latest_norgate_date is None:
        return False
    if latest_cache_date.normalize() < latest_norgate_date:
        return False
    return True


def _incremental_update_etf_signals(cached_df, days_stale=1):
    """Incremental update for ETF signals — mirrors _incremental_update_signals."""
    inc_start = time.time()
    print("=" * 60)
    print("[ETF] Running incremental signals update...")

    all_tickers = sorted({t for tickers in ETF_UNIVERSE.values() for t in tickers})
    universe_set = set(all_tickers)

    cached_df = cached_df.copy()
    if 'Source' in cached_df.columns:
        cached_df = cached_df[cached_df['Source'] != 'live'].copy()
    cached_df['Date'] = pd.to_datetime(cached_df['Date'])
    last_rows = {}
    for ticker_val, grp in cached_df.groupby('Ticker', sort=False):
        last_rows[ticker_val] = grp.sort_values('Date').iloc[-1]

    cached_tickers = set(last_rows.keys())
    new_tickers = universe_set - cached_tickers
    dropped_tickers = cached_tickers - universe_set

    print(f"  [ETF] Universe: {len(universe_set)} | Cached: {len(cached_tickers)} | "
          f"New: {len(new_tickers)} | Dropped: {len(dropped_tickers)}")

    check_tickers = [t for t in all_tickers if t in cached_tickers]
    append_rows = {}
    rebuild_tickers = list(new_tickers)

    def _safe_append(t):
        return t, _build_signals_append_ticker(t, last_rows[t], limit=days_stale + 1)

    total_check = len(check_tickers)
    processed = 0
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_append, t) for t in check_tickers]
        for fut in futures:
            t, (action, payload) = fut.result()
            processed += 1
            percent = int((processed / total_check) * 100) if total_check else 100
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  [ETF] Append check {milestone}% ({processed}/{total_check})")
                last_milestone = milestone
            if action == 'append':
                append_rows[t] = payload
            elif action in ('full_rebuild', 'error'):
                rebuild_tickers.append(t)

    print(f"  [ETF] Append: {len(append_rows)} | Full rebuild: {len(rebuild_tickers)}")

    rebuild_results = {}
    if rebuild_tickers:
        print(f"  [ETF] Rebuilding {len(rebuild_tickers)} tickers...")
        def _safe_rebuild(t):
            try:
                return t, build_signals_for_ticker(t)
            except Exception:
                return t, None
        with ThreadPoolExecutor(max_workers=4) as ex:
            for fut in [ex.submit(_safe_rebuild, t) for t in rebuild_tickers]:
                t, result = fut.result()
                if result is not None:
                    rebuild_results[t] = result

    keep_set = set(cached_df.columns)

    if rebuild_results:
        cached_df = cached_df[~cached_df['Ticker'].isin(rebuild_results.keys())].copy()
        rebuild_frames = []
        for t, df_rebuilt in rebuild_results.items():
            trim_cols = [c for c in df_rebuilt.columns if c in keep_set]
            if trim_cols:
                trimmed = df_rebuilt[trim_cols].dropna(axis=1, how='all')
                if not trimmed.empty:
                    rebuild_frames.append(trimmed)
        if rebuild_frames:
            cached_df = pd.concat([cached_df] + rebuild_frames, ignore_index=True)

    if append_rows:
        new_rows_list = []
        for t, row_list in append_rows.items():
            for row_series in row_list:
                row_dict = {k: v for k, v in row_series.items() if k in keep_set}
                new_rows_list.append(row_dict)
        new_rows_df = pd.DataFrame(new_rows_list,
                                   columns=[c for c in cached_df.columns if c in keep_set])
        cached_df = pd.concat([cached_df, new_rows_df], ignore_index=True)

    # Dtype optimizations
    bool_cols = ['Is_Up_Rotation', 'Is_Down_Rotation', 'Is_Breakout', 'Is_Breakdown',
                 'Is_BTFD', 'Is_STFR', 'Is_Breakout_Sequence', 'BTFD_Triggered', 'STFR_Triggered']
    for col in bool_cols:
        if col in cached_df.columns:
            cached_df[col] = cached_df[col].fillna(False).astype(bool)
    if 'Trend' in cached_df.columns:
        cached_df['Trend'] = cached_df['Trend'].map(
            {True: 1.0, False: 0.0, 1.0: 1.0, 0.0: 0.0, None: float('nan')}
        ).astype('float32')
    if 'Rotation_ID' in cached_df.columns:
        cached_df['Rotation_ID'] = cached_df['Rotation_ID'].fillna(0).astype('int32')
    float32_prefixes = ['Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
                        'Avg_MFE', 'Avg_MAE', 'Historical_EV', 'Std_Dev', 'Risk_Adj_EV',
                        'EV_Last_3', 'Risk_Adj_EV_Last_3', 'Count']
    for col in cached_df.columns:
        for prefix in float32_prefixes:
            if col.endswith(prefix) and cached_df[col].dtype == 'float64':
                cached_df[col] = cached_df[col].astype('float32')
                break

    cached_df['Source'] = 'norgate'
    tmp_path = ETF_SIGNALS_CACHE_FILE.with_suffix('.parquet.tmp')
    pq.write_table(pa.Table.from_pandas(cached_df, preserve_index=False),
                    tmp_path, compression='snappy', use_dictionary=False)
    tmp_path.replace(ETF_SIGNALS_CACHE_FILE)
    WriteThroughPath(ETF_SIGNALS_CACHE_FILE).sync()
    del cached_df['Source']

    print(f"  [ETF] Incremental update complete: {len(cached_df)} total rows "
          f"({time.time() - inc_start:.1f}s)")
    print("=" * 60)
    return cached_df


def load_or_build_etf_signals():
    """Load or build the ETF signals parquet — mirrors load_or_build_signals."""
    etf_start = time.time()
    if ETF_SIGNALS_CACHE_FILE.exists():
        cached = pd.read_parquet(ETF_SIGNALS_CACHE_FILE)
        if _etf_signals_cache_is_current(cached):
            print("[ETF] Signals loaded from cache (up to date)")
            if 'Source' in cached.columns:
                del cached['Source']
            return cached

        if 'Source' in cached.columns:
            cached = cached[cached['Source'] != 'live'].copy()

        days_stale = None
        columns_ok = True
        if 'Date' in cached.columns and not cached.empty:
            latest_cache_date = pd.to_datetime(cached['Date']).max().normalize()
            latest_norgate_date = _get_latest_norgate_date()
            if latest_norgate_date is not None and not pd.isna(latest_cache_date):
                days_stale = int(np.busday_count(latest_cache_date.date(), latest_norgate_date.date()))
        _required_stats = ['Avg_Winner_Bars', 'Avg_Loser_Bars']
        for sig in SIGNALS:
            if f'{sig}_Win_Rate' in cached.columns:
                if any(f'{sig}_{s}' not in cached.columns for s in _required_stats):
                    columns_ok = False
                break

        if days_stale is not None and days_stale == 0 and columns_ok:
            print("[ETF] Signals loaded from cache (up to date after stripping live rows)")
            return cached
        elif (days_stale is not None and 1 <= days_stale <= INCREMENTAL_MAX_DAYS and columns_ok):
            print(f"[ETF] Signals cache stale by {days_stale} trading day(s), running incremental update...")
            try:
                return _incremental_update_etf_signals(cached, days_stale=days_stale)
            except Exception as exc:
                print(f"[ETF] Incremental update failed ({exc}), falling back to full rebuild...")
        else:
            if not columns_ok:
                print("[ETF] Signals cache outdated (schema changed), full rebuild...")
            else:
                print(f"[ETF] Signals cache outdated (stale by {days_stale} days), full rebuild...")
    else:
        print("[ETF] No signals cache found, building from scratch...")

    all_etf_tickers = sorted({t for tickers in ETF_UNIVERSE.values() for t in tickers})
    print(f"[ETF] Total unique ETFs in universe: {len(all_etf_tickers)}")
    print("=" * 60)

    base_keep = [
        'Date', 'Ticker',
        'Open', 'High', 'Low', 'Close', 'Volume',
        'RV', 'RV_EMA', 'Trend',
        'Resistance_Pivot', 'Support_Pivot',
        'Rotation_Open', 'Up_Range', 'Down_Range',
        'Up_Range_EMA', 'Down_Range_EMA',
        'Upper_Target', 'Lower_Target',
        'Is_Up_Rotation', 'Is_Down_Rotation',
        'Is_Breakout', 'Is_Breakdown', 'Is_BTFD', 'Is_STFR',
        'Is_Breakout_Sequence',
        'Rotation_ID', 'BTFD_Triggered', 'STFR_Triggered',
    ]
    signal_cols = []
    for sig in SIGNALS:
        signal_cols.extend([
            f'{sig}_Entry_Price', f'{sig}_Exit_Date', f'{sig}_Exit_Price',
            f'{sig}_Final_Change', f'{sig}_MFE', f'{sig}_MAE',
            f'{sig}_Win_Rate', f'{sig}_Avg_Winner', f'{sig}_Avg_Loser',
            f'{sig}_Avg_Winner_Bars', f'{sig}_Avg_Loser_Bars',
            f'{sig}_Avg_MFE', f'{sig}_Avg_MAE',
            f'{sig}_Std_Dev', f'{sig}_Historical_EV', f'{sig}_EV_Last_3',
            f'{sig}_Risk_Adj_EV', f'{sig}_Risk_Adj_EV_Last_3', f'{sig}_Count'
        ])
    keep_set = set(base_keep + signal_cols)

    all_signals = []
    failed_tickers = []

    def _safe_build(t):
        try:
            return t, build_signals_for_ticker(t)
        except Exception:
            return t, None

    max_workers = min(8, (os.cpu_count() or 4))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_safe_build, t) for t in all_etf_tickers]
        processed = 0
        total = len(all_etf_tickers)
        last_milestone = 0
        for fut in futures:
            t, result = fut.result()
            processed += 1
            percent = int((processed / total) * 100) if total else 100
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  [ETF] {milestone}% complete ({processed} / {total} ETFs)")
                last_milestone = milestone
            if result is not None:
                trim_cols = [c for c in result.columns if c in keep_set]
                if not trim_cols:
                    failed_tickers.append(t)
                    continue
                trimmed = result[trim_cols].dropna(axis=1, how='all')
                if trimmed.empty:
                    failed_tickers.append(t)
                    continue
                all_signals.append(trimmed)
            else:
                failed_tickers.append(t)

    if not all_signals:
        print("[ETF] No signals generated for any ETFs.")
        return pd.DataFrame()

    print("=" * 60)
    print(f"[ETF] Signal generation complete")
    print(f"  Successful: {len(all_signals)} ETFs")
    print(f"  Failed: {len(failed_tickers)} ETFs")
    print(f"  Total time: {time.time() - etf_start:.1f}s")
    print("=" * 60)

    clean_frames = [df.dropna(axis=1, how='all') for df in all_signals if not df.empty]
    etf_signals_df = pd.concat(clean_frames, ignore_index=True)
    del all_signals, clean_frames
    etf_signals_df = etf_signals_df.dropna(axis=1, how='all')

    # Dtype optimization
    bool_cols = ['Is_Up_Rotation', 'Is_Down_Rotation', 'Is_Breakout', 'Is_Breakdown',
                 'Is_BTFD', 'Is_STFR', 'Is_Breakout_Sequence', 'BTFD_Triggered', 'STFR_Triggered']
    for col in bool_cols:
        if col in etf_signals_df.columns:
            etf_signals_df[col] = etf_signals_df[col].fillna(False).astype(bool)
    if 'Trend' in etf_signals_df.columns:
        etf_signals_df['Trend'] = etf_signals_df['Trend'].map({True: 1.0, False: 0.0, None: np.nan}).astype('float32')
    if 'Rotation_ID' in etf_signals_df.columns:
        etf_signals_df['Rotation_ID'] = etf_signals_df['Rotation_ID'].fillna(0).astype('int32')
    float32_prefixes = ['Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars', 'Avg_MFE', 'Avg_MAE',
                        'Historical_EV', 'Std_Dev', 'Risk_Adj_EV', 'EV_Last_3',
                        'Risk_Adj_EV_Last_3', 'Count']
    for col in etf_signals_df.columns:
        for prefix in float32_prefixes:
            if col.endswith(prefix) and etf_signals_df[col].dtype == 'float64':
                etf_signals_df[col] = etf_signals_df[col].astype('float32')
                break

    etf_signals_df = etf_signals_df.copy()
    etf_signals_df['Source'] = 'norgate'
    pq.write_table(pa.Table.from_pandas(etf_signals_df, preserve_index=False),
                    ETF_SIGNALS_CACHE_FILE, compression='snappy', use_dictionary=False)
    print(f"[ETF] Saved: {ETF_SIGNALS_CACHE_FILE} ({len(etf_signals_df)} rows, {etf_signals_df['Ticker'].nunique()} ETFs)")
    etf_signals_df = etf_signals_df.drop(columns=['Source'])
    return etf_signals_df


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    reset_cell_timer("Signal Generation")

    # Load universes from cache
    QUARTER_UNIVERSE = load_universe_from_cache()
    ETF_UNIVERSE = load_etf_universe_from_cache()
    print(f"Loaded universe: {len(QUARTER_UNIVERSE)} quarters, current = {get_current_quarter_key(QUARTER_UNIVERSE)}")

    # Build/update signals
    all_signals_df = load_or_build_signals()
    print(f"Signals: {len(all_signals_df)} rows, {all_signals_df['Ticker'].nunique()} tickers, "
          f"dates {all_signals_df['Date'].min().date()} to {all_signals_df['Date'].max().date()}")

    etf_signals_df = load_or_build_etf_signals()
    print(f"ETF Signals: {len(etf_signals_df)} rows, {etf_signals_df['Ticker'].nunique()} ETFs")
