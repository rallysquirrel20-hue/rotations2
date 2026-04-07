#!/usr/bin/env python
"""Build basket signals (OHLC + signals + breadth + correlation) for all baskets."""

import argparse
import bisect
import glob as globmod
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from config import (
    SIZE,
    DATA_FOLDER,
    BETA_CACHE_FILE,
    MOMENTUM_CACHE_FILE,
    RISK_ADJ_MOM_CACHE_FILE,
    DIVIDEND_CACHE_FILE,
    SIZE_CACHE_FILE,
    VOLUME_CACHE_FILE,
    SIGNALS_CACHE_FILE,
    WriteThroughPath,
    _quarter_end_from_key,
    _quarter_start_from_key,
    get_current_quarter_key,
    load_universe_from_disk,
    load_gics_from_disk,
    load_thematic_universe_from_disk,
    _install_timed_print,
    reset_cell_timer,
    paths,
)
from build_signals import (
    _build_signals_from_df,
    _build_signals_next_row,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EQUITY_CACHE_SCHEMA_VERSION = 1
EQUITY_SIGNAL_LOGIC_VERSION = '2026-03-13-btfd-stfr-prev-trend'
EQUITY_UNIVERSE_LOGIC_VERSION = '2026-04-01-target-quarter-keying'
FORCE_REBUILD_EQUITY_CACHE = False
BASKET_SIGNALS_CACHE_SCHEMA_VERSION = 1
FORCE_REBUILD_BASKET_SIGNALS = False
CHART_SCHEMA_VERSION = 2
BENCHMARK_BASKETS = 0
BENCHMARK_TIMING = True

RV_MULT = np.sqrt(252) / np.sqrt(21)
RV_EMA_ALPHA = 2.0 / 11.0  # span=10 EMA for RV

# ---------------------------------------------------------------------------
# Module-level accumulators (OK for single-run subprocess)
# ---------------------------------------------------------------------------
_basket_timing = {
    'cache_check': [], 'equity_ohlc': [], 'build_signals': [],
    'breadth_trend': [], 'breadth_breakout': [], 'breadth_merge': [],
    'breadth_pivots': [], 'correlation': [], 'save_parquet': [],
    'contributions': [], 'total': [],
}
_basket_timing_names = []

_DATA_SIGNATURE_CACHE = None


# ---------------------------------------------------------------------------
# Slug / quarter helpers
# ---------------------------------------------------------------------------

def _cache_slugify_label(label):
    return str(label).replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')


def _build_quarter_lookup(universe_by_date):
    """Build sorted quarter-start arrays for O(log n) bisect lookup."""
    if isinstance(next(iter(universe_by_date.keys())), str):
        quarter_keys = [(k, _quarter_start_from_key(k)) for k in universe_by_date.keys()]
    else:
        quarter_keys = [(k, k) for k in universe_by_date.keys()]
    quarter_keys.sort(key=lambda x: x[1])
    quarter_labels = [k for k, _ in quarter_keys]
    quarter_ends = [dt for _, dt in quarter_keys]
    return quarter_labels, quarter_ends


def _find_active_quarter(d, quarter_labels, quarter_ends):
    """O(log n) lookup of active quarter for a given date."""
    idx = bisect.bisect_right(quarter_ends, d) - 1
    if idx < 0:
        return None
    return quarter_labels[idx]


def _build_membership_df(universe_by_date):
    """Build flat (quarter_key, ticker) membership table for vectorized filtering."""
    membership_rows = []
    for key, tickers in universe_by_date.items():
        for t in tickers:
            membership_rows.append((key, t))
    return pd.DataFrame(membership_rows, columns=['_q_key', 'Ticker'])


def _vectorized_quarter_filter(df, universe_by_date, quarter_labels, quarter_ends, membership_df=None):
    """Vectorized quarter-membership filter: assign each row its active quarter,
    then keep only rows whose Ticker is in that quarter's universe.

    Returns the filtered DataFrame with a '_q_key' column.
    """
    quarter_ends_ts = pd.DatetimeIndex(quarter_ends)
    date_vals = df['Date'].values
    idx = np.searchsorted(quarter_ends_ts.values, date_vals, side='right') - 1

    valid_mask = idx >= 0
    df = df[valid_mask].copy()
    idx = idx[valid_mask]

    label_arr = np.array(quarter_labels)
    df['_q_key'] = label_arr[idx]

    if membership_df is None:
        membership_df = _build_membership_df(universe_by_date)
    return df.merge(membership_df, on=['_q_key', 'Ticker'], how='inner')


# ---------------------------------------------------------------------------
# Equity close (simple) for cache prebuild
# ---------------------------------------------------------------------------

def _compute_equity_close_for_cache(all_df, universe_by_date):
    needed = ['Date', 'Ticker', 'Close']
    if 'Volume' in all_df.columns:
        needed.append('Volume')
    df = all_df[needed].copy()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.normalize()
    df = df.dropna(subset=['Date', 'Close']).sort_values(['Ticker', 'Date'])
    if df.empty:
        return pd.DataFrame()

    df['Prev_Close'] = df.groupby('Ticker')['Close'].shift(1)
    df['Ret'] = (df['Close'] / df['Prev_Close']) - 1.0

    q_labels, q_ends = _build_quarter_lookup(universe_by_date)
    if not q_labels:
        return pd.DataFrame()

    equity = 1.0
    rows = []
    for d, g in df.groupby('Date'):
        q_key = _find_active_quarter(d, q_labels, q_ends)
        if q_key is None:
            continue
        uni = universe_by_date.get(q_key, set())
        if not uni:
            continue
        day = g[g['Ticker'].isin(uni)].copy()
        day = day[pd.notna(day['Ret'])].copy()
        if day.empty:
            continue

        if 'Volume' in day.columns:
            day['W'] = day['Prev_Close'] * day['Volume']
            w = day['W'].replace([np.inf, -np.inf], np.nan).fillna(0.0)
            wsum = float(w.sum())
            ret = float((w * day['Ret']).sum() / wsum) if wsum > 0 else float(day['Ret'].mean())
        else:
            ret = float(day['Ret'].mean())
        if not np.isfinite(ret):
            continue
        equity *= (1.0 + ret)
        rows.append({'Date': d, 'Close': equity})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Data signature
# ---------------------------------------------------------------------------

def _get_data_signature(all_df):
    global _DATA_SIGNATURE_CACHE
    if _DATA_SIGNATURE_CACHE is not None:
        return _DATA_SIGNATURE_CACHE

    needed = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close']
    cols = [c for c in needed if c in all_df.columns]
    if len(cols) < 3 or 'Date' not in cols or 'Ticker' not in cols or 'Close' not in cols:
        _DATA_SIGNATURE_CACHE = {'fingerprint': 'missing_required_columns', 'latest_date': None}
        return _DATA_SIGNATURE_CACHE

    fp = all_df[cols].copy()
    fp['Date'] = pd.to_datetime(fp['Date'], errors='coerce').dt.normalize()
    fp = fp.dropna(subset=['Date']).sort_values(['Date', 'Ticker']).reset_index(drop=True)
    latest_date = fp['Date'].max() if not fp.empty else None
    if fp.empty:
        _DATA_SIGNATURE_CACHE = {'fingerprint': 'empty_source_data', 'latest_date': None}
        return _DATA_SIGNATURE_CACHE

    hashed = pd.util.hash_pandas_object(fp, index=False).values
    digest = hashlib.sha256(hashed.tobytes()).hexdigest()
    _DATA_SIGNATURE_CACHE = {'fingerprint': digest, 'latest_date': latest_date}
    return _DATA_SIGNATURE_CACHE


def _build_universe_signature(universe_by_date):
    h = hashlib.sha256()
    for q in sorted(universe_by_date.keys()):
        h.update(str(q).encode('utf-8'))
        for t in sorted(universe_by_date.get(q, set())):
            h.update(b'|')
            h.update(str(t).encode('utf-8'))
        h.update(b';')
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Breadth computations
# ---------------------------------------------------------------------------

def compute_breadth_from_trend(all_df, universe_by_date, membership_df=None):
    """Count uptrend vs downtrend stocks per day (vectorized)."""
    df = all_df[['Date', 'Ticker', 'Trend']].copy()
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
    df = df.dropna(subset=['Trend'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_date)
    df = _vectorized_quarter_filter(df, universe_by_date, quarter_labels, quarter_ends, membership_df=membership_df)
    if df.empty:
        return pd.DataFrame(columns=['Date', 'Uptrend_Count', 'Downtrend_Count', 'Total_Stocks', 'Breadth_Ratio'])

    is_up = (df['Trend'] == 1.0) if df['Trend'].dtype != object else (df['Trend'] == True)
    is_down = (df['Trend'] == 0.0) if df['Trend'].dtype != object else (df['Trend'] == False)

    result = pd.DataFrame({
        'Uptrend_Count': is_up.groupby(df['Date']).sum().astype(int),
        'Downtrend_Count': is_down.groupby(df['Date']).sum().astype(int),
    })
    result['Total_Stocks'] = result['Uptrend_Count'] + result['Downtrend_Count']
    result = result[result['Total_Stocks'] > 0]
    result['Breadth_Ratio'] = (result['Uptrend_Count'] - result['Downtrend_Count']) / result['Total_Stocks']
    result = result.reset_index()
    return result


def compute_breadth_from_breakout(all_df, universe_by_date, membership_df=None):
    """Count breakout-regime vs breakdown-regime stocks per day (vectorized)."""
    df = all_df[['Date', 'Ticker', 'Is_Breakout_Sequence']].copy()
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
    df = df.dropna(subset=['Is_Breakout_Sequence'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_date)
    df = _vectorized_quarter_filter(df, universe_by_date, quarter_labels, quarter_ends, membership_df=membership_df)
    if df.empty:
        return pd.DataFrame(columns=['Date', 'Breakout_Count', 'Breakdown_Count', 'BO_Total_Stocks', 'BO_Breadth_Ratio'])

    bo = df['Is_Breakout_Sequence'].astype(bool)
    result = pd.DataFrame({
        'Breakout_Count': bo.groupby(df['Date']).sum().astype(int),
        'Breakdown_Count': (~bo).groupby(df['Date']).sum().astype(int),
    })
    result['BO_Total_Stocks'] = result['Breakout_Count'] + result['Breakdown_Count']
    result = result[result['BO_Total_Stocks'] > 0]
    result['BO_Breadth_Ratio'] = (result['Breakout_Count'] - result['Breakdown_Count']) / result['BO_Total_Stocks']
    result = result.reset_index()
    return result


def compute_breadth_pivots(ema_values):
    """Apply additive Pass-1 trend/pivot logic to Breadth EMA and detect divergences."""
    n = len(ema_values)
    ema = np.asarray(ema_values, dtype=float)

    # Absolute-change RV and its EMA
    rv_raw = np.zeros(n)
    rv_raw[1:] = np.abs(np.diff(ema))
    rv_ema = np.zeros(n)
    alpha = RV_EMA_ALPHA
    rv_ema[1] = rv_raw[1]
    for i in range(2, n):
        rv_ema[i] = alpha * rv_raw[i] + (1 - alpha) * rv_ema[i - 1]

    # Find first valid index (need nonzero rv_ema)
    start_idx = next((i for i in range(2, n) if rv_ema[i] > 0), None)
    if start_idx is None:
        return pd.DataFrame()

    # --- Pass 1: Trend and Pivots (additive) ---
    trends = np.full(n, None, dtype=object)
    resistance = np.full(n, np.nan)
    support = np.full(n, np.nan)
    is_up_rot = np.zeros(n, dtype=bool)
    is_down_rot = np.zeros(n, dtype=bool)

    trends[start_idx] = False
    resistance[start_idx] = ema[start_idx] + rv_ema[start_idx] * RV_MULT

    for i in range(start_idx + 1, n):
        val = ema[i]
        rv = rv_ema[i] * RV_MULT
        prev_trend = trends[i - 1]
        prev_res = resistance[i - 1]
        prev_sup = support[i - 1]

        if prev_trend == False:  # Downtrend
            resistance[i] = min(val + rv, prev_res)
            if val > prev_res:
                trends[i] = True
                support[i] = val - rv
                resistance[i] = prev_res
                is_up_rot[i] = True
            else:
                trends[i] = False
        else:  # Uptrend
            support[i] = max(val - rv, prev_sup) if not np.isnan(prev_sup) else val - rv
            if val < prev_sup:
                trends[i] = False
                resistance[i] = val + rv
                support[i] = prev_sup
                is_down_rot[i] = True
            else:
                trends[i] = True

    # --- Track rotation extremes and detect divergences ---
    rot_high = np.full(n, np.nan)
    rot_low = np.full(n, np.nan)
    is_bull_div = np.zeros(n, dtype=bool)
    is_bear_div = np.zeros(n, dtype=bool)

    prev_down_low = np.nan
    last_down_low = np.nan
    prev_up_high = np.nan
    last_up_high = np.nan
    cur_high = np.nan
    cur_low = np.nan

    for i in range(start_idx, n):
        t = trends[i]
        if t is None:
            continue

        if is_up_rot[i]:
            if not np.isnan(cur_low):
                prev_down_low = last_down_low
                last_down_low = cur_low
            if not np.isnan(prev_down_low) and not np.isnan(last_down_low):
                if last_down_low > prev_down_low:
                    is_bull_div[i] = True
            cur_high = ema[i]
            cur_low = np.nan

        elif is_down_rot[i]:
            if not np.isnan(cur_high):
                prev_up_high = last_up_high
                last_up_high = cur_high
            if not np.isnan(prev_up_high) and not np.isnan(last_up_high):
                if last_up_high < prev_up_high:
                    is_bear_div[i] = True
            cur_low = ema[i]
            cur_high = np.nan

        if t == True:
            cur_high = np.nanmax([cur_high, ema[i]])
            rot_high[i] = cur_high
        else:
            cur_low = np.nanmin([cur_low, ema[i]])
            rot_low[i] = cur_low

    return pd.DataFrame({
        'B_Trend': trends,
        'B_Resistance': resistance,
        'B_Support': support,
        'B_Up_Rot': is_up_rot,
        'B_Down_Rot': is_down_rot,
        'B_Rot_High': rot_high,
        'B_Rot_Low': rot_low,
        'B_Bull_Div': is_bull_div,
        'B_Bear_Div': is_bear_div,
    })


# ---------------------------------------------------------------------------
# Signal trades
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Quarter weights (shared by equity OHLC and contributions)
# ---------------------------------------------------------------------------

def _build_quarter_weights(all_df, universe_by_date, quarter_labels):
    """Build per-quarter initial dollar-volume weights. Shared by equity OHLC and contributions."""
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
        return {}
    for label in quarter_labels:
        if label not in universe_by_date:
            continue
        prev_universe = universe_by_date[label]
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
    return quarter_weights


# ---------------------------------------------------------------------------
# Equity OHLC computation
# ---------------------------------------------------------------------------

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

            w0 = pd.Series({t: w_dict[t] for t in tickers})
            w0 = w0 / w0.sum()

            cum_growth = (1 + close_ret).cumprod()
            portfolio_value = (cum_growth * w0).sum(axis=1)

            eq_close = equity_prev_close * portfolio_value

            eq_prev_close_series = eq_close.shift(1).fillna(equity_prev_close)

            cum_growth_prev = cum_growth.shift(1).fillna(1.0)
            portfolio_value_prev = portfolio_value.shift(1).fillna(1.0)
            bod_weights = cum_growth_prev.multiply(w0, axis=1).div(portfolio_value_prev, axis=0)

            port_open_ret = (bod_weights * open_ret).sum(axis=1)
            port_high_ret = (bod_weights * high_ret).sum(axis=1)
            port_low_ret = (bod_weights * low_ret).sum(axis=1)

            eq_open = eq_prev_close_series * (1 + port_open_ret)
            eq_high = eq_prev_close_series * (1 + port_high_ret)
            eq_low = eq_prev_close_series * (1 + port_low_ret)

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

            if return_contributions:
                contributions = bod_weights * close_ret
                bod_long = bod_weights.stack().rename('Weight_BOD')
                ret_long = close_ret.stack().rename('Daily_Return')
                contrib_long = contributions.stack().rename('Contribution')
                q_contrib = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
                q_contrib.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
                contrib_parts.append(q_contrib)

            equity_prev_close = float(eq_close.iloc[-1])
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

        # Transition to current calendar quarter if its universe exists
        today = datetime.today()
        today_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
        if today_key in universe_by_date and today_key != last_state.get('current_quarter'):
            w_dict = quarter_weights.get(today_key, {})
            if w_dict:
                last_state = {
                    'current_quarter': today_key,
                    'equity_prev_close': last_state['equity_prev_close'],
                    'weights': {str(k): float(v) for k, v in w_dict.items()},
                }
                # Append rebalance rows to contributions
                if return_contributions and contrib_df is not None:
                    q_start = _quarter_start_from_key(today_key)
                    rebalance_rows = pd.DataFrame([
                        {'Date': q_start, 'Ticker': t, 'Weight_BOD': float(w),
                         'Daily_Return': 0.0, 'Contribution': 0.0}
                        for t, w in w_dict.items()
                    ])
                    contrib_df = pd.concat([contrib_df, rebalance_rows], ignore_index=True)
                    contrib_df = contrib_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last').sort_values(['Date', 'Ticker']).reset_index(drop=True)

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

    # Transition to the current calendar quarter if its universe exists but
    # no trading dates for it appeared in the historical data yet (Q1 day 1).
    today = datetime.today()
    today_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    if today_key in universe_by_date and today_key != current_quarter:
        w_dict = quarter_weights.get(today_key, {})
        if w_dict:
            current_quarter = today_key
            current_weights_series = pd.Series(w_dict)

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


# ---------------------------------------------------------------------------
# Equity cache I/O
# ---------------------------------------------------------------------------

def _basket_cache_folder(basket_type):
    """Return the cache folder for the given basket type."""
    if basket_type == 'thematic':
        return paths.thematic_basket_cache
    elif basket_type == 'industry':
        return paths.industry_basket_cache
    else:
        return paths.sector_basket_cache


def _cache_file_stem(slug, basket_type, universe_by_qtr, suffix):
    """Generate cache file stem: {slug}[_{basket_size}]_of_{SIZE}_{suffix}"""
    if basket_type == 'thematic':
        current_key = get_current_quarter_key()
        if current_key is None:
            keys = sorted(universe_by_qtr.keys()) if universe_by_qtr else []
            current_key = keys[-1] if keys else None
        basket_size = len(universe_by_qtr.get(current_key, set())) if current_key else 0
        return f'{slug}_{basket_size}_of_{SIZE}_{suffix}'
    else:
        return f'{slug}_of_{SIZE}_{suffix}'


def _equity_cache_paths(slug, basket_type='sector', universe_by_qtr=None):
    folder = _basket_cache_folder(basket_type)
    if universe_by_qtr is not None:
        stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'ohlc')
    else:
        stem = f'{slug}_*_of_{SIZE}_ohlc' if basket_type == 'thematic' else f'{slug}_of_{SIZE}_ohlc'
    return (
        folder / f'{stem}.parquet',
        folder / f'{stem}_meta.json',
    )


def _load_equity_cache(slug, basket_type='sector', universe_by_qtr=None):
    ohlc_path, meta_path = _equity_cache_paths(slug, basket_type, universe_by_qtr)
    if '*' in str(ohlc_path):
        matches = sorted(globmod.glob(str(ohlc_path)))
        if not matches:
            return None, None
        ohlc_path = Path(matches[-1])
        meta_path = Path(str(ohlc_path).replace('.parquet', '_meta.json'))
    if not ohlc_path.exists() or not meta_path.exists():
        return None, None
    try:
        cached = pd.read_parquet(ohlc_path)
    except Exception:
        return None, None
    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
    except Exception:
        return None, None
    if isinstance(cached, pd.DataFrame) and not cached.empty and 'Date' in cached.columns:
        cached = cached.copy()
        cached['Date'] = pd.to_datetime(cached['Date'], errors='coerce').dt.normalize()
        cached = cached.dropna(subset=['Date']).sort_values('Date').reset_index(drop=True)
    return cached, meta


def _save_equity_cache(slug, ohlc_df, meta, basket_type='sector', universe_by_qtr=None):
    ohlc_path, meta_path = _equity_cache_paths(slug, basket_type, universe_by_qtr)
    table = pa.Table.from_pandas(ohlc_df, preserve_index=False)
    existing_meta = table.schema.metadata or {}
    new_meta = {**existing_meta,
                b'chart_schema_version': str(CHART_SCHEMA_VERSION).encode()}
    pq.write_table(table.replace_schema_metadata(new_meta),
                   ohlc_path, compression='snappy')
    with open(meta_path, 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)


def _build_equity_meta(data_sig, universe_sig, state, ohlc_df):
    last_cached_date = None
    if isinstance(ohlc_df, pd.DataFrame) and not ohlc_df.empty and 'Date' in ohlc_df.columns:
        last_cached_date = pd.to_datetime(ohlc_df['Date']).max()
    return {
        'schema_version': EQUITY_CACHE_SCHEMA_VERSION,
        'signal_logic_version': EQUITY_SIGNAL_LOGIC_VERSION,
        'universe_logic_version': EQUITY_UNIVERSE_LOGIC_VERSION,
        'data_fingerprint': data_sig.get('fingerprint'),
        'latest_source_date': (
            data_sig['latest_date'].strftime('%Y-%m-%d')
            if pd.notna(data_sig.get('latest_date'))
            else None
        ),
        'last_cached_date': (
            pd.to_datetime(last_cached_date).strftime('%Y-%m-%d')
            if pd.notna(last_cached_date)
            else None
        ),
        'universe_signature': universe_sig,
        'state': state if isinstance(state, dict) else {},
    }


def _is_equity_cache_valid(meta, data_sig, universe_sig):
    if not isinstance(meta, dict):
        return False
    if meta.get('schema_version') != EQUITY_CACHE_SCHEMA_VERSION:
        return False
    if meta.get('signal_logic_version') != EQUITY_SIGNAL_LOGIC_VERSION:
        return False
    if meta.get('universe_logic_version') != EQUITY_UNIVERSE_LOGIC_VERSION:
        return False
    if meta.get('universe_signature') != universe_sig:
        return False
    cached_source_date = pd.to_datetime(meta.get('latest_source_date'), errors='coerce')
    current_source_date = pd.to_datetime(data_sig.get('latest_date'), errors='coerce')
    if pd.isna(cached_source_date) or pd.isna(current_source_date):
        return meta.get('data_fingerprint') == data_sig.get('fingerprint')
    if current_source_date.normalize() < cached_source_date.normalize():
        return False
    if current_source_date.normalize() == cached_source_date.normalize():
        return meta.get('data_fingerprint') == data_sig.get('fingerprint')
    return True


# ---------------------------------------------------------------------------
# Basket signals cache I/O
# ---------------------------------------------------------------------------

def _basket_cache_paths(slug, basket_type='sector', universe_by_qtr=None):
    """Return (parquet_path, meta_path) for the consolidated basket file."""
    folder = _basket_cache_folder(basket_type)
    if universe_by_qtr is not None:
        stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'signals')
    else:
        stem = f'{slug}_*_of_{SIZE}_signals' if basket_type == 'thematic' else f'{slug}_of_{SIZE}_signals'
    return (
        folder / f'{stem}.parquet',
        folder / f'{stem}_meta.json',
    )


def _find_basket_parquet(slug):
    """Glob for a basket parquet by slug prefix. Returns path or None."""
    _search_folders = [
        paths.thematic_basket_cache, paths.sector_basket_cache,
        paths.industry_basket_cache, DATA_FOLDER,
    ]
    for folder in _search_folders:
        matches = list(folder.glob(f'{slug}_*_of_{SIZE}_signals.parquet'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_{SIZE}_signals.parquet'))
        if matches:
            return matches[0]
    return None


def _find_basket_meta(slug):
    """Glob for a basket meta JSON by slug prefix. Returns path or None."""
    _search_folders = [
        paths.thematic_basket_cache, paths.sector_basket_cache,
        paths.industry_basket_cache, DATA_FOLDER,
    ]
    for folder in _search_folders:
        matches = list(folder.glob(f'{slug}_*_of_{SIZE}_signals_meta.json'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_{SIZE}_signals_meta.json'))
        if matches:
            return matches[0]
    return None


def _get_chart_schema_version_from_parquet(slug):
    """Read CHART_SCHEMA_VERSION embedded in basket signals parquet metadata."""
    parquet_path = _find_basket_parquet(slug)
    if parquet_path is None or not parquet_path.exists():
        return None
    try:
        file_meta = pq.read_metadata(str(parquet_path)).metadata or {}
        return int(file_meta.get(b'chart_schema_version', b'0'))
    except Exception:
        return None


def _build_basket_signals_meta(data_sig, universe_sig, merged_all):
    last_cached_date = None
    if isinstance(merged_all, pd.DataFrame) and not merged_all.empty and 'Date' in merged_all.columns:
        last_cached_date = pd.to_datetime(merged_all['Date'], errors='coerce').max()
    return {
        'schema_version': BASKET_SIGNALS_CACHE_SCHEMA_VERSION,
        'signal_logic_version': EQUITY_SIGNAL_LOGIC_VERSION,
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
    }


def _is_basket_signals_cache_valid(meta, data_sig, universe_sig):
    if not isinstance(meta, dict):
        return False
    if meta.get('schema_version') != BASKET_SIGNALS_CACHE_SCHEMA_VERSION:
        return False
    if meta.get('signal_logic_version') != EQUITY_SIGNAL_LOGIC_VERSION:
        return False
    if meta.get('universe_signature') != universe_sig:
        return False
    cached_source_date = pd.to_datetime(meta.get('latest_source_date'), errors='coerce')
    current_source_date = pd.to_datetime(data_sig.get('latest_date'), errors='coerce')
    if pd.isna(cached_source_date) or pd.isna(current_source_date):
        return meta.get('data_fingerprint') == data_sig.get('fingerprint')
    if current_source_date.normalize() < cached_source_date.normalize():
        return False
    if current_source_date.normalize() == cached_source_date.normalize():
        return meta.get('data_fingerprint') == data_sig.get('fingerprint')
    return True


# ---------------------------------------------------------------------------
# Cached equity OHLC builder
# ---------------------------------------------------------------------------

def compute_equity_ohlc_cached(all_df, universe_by_date, basket_name, slug, basket_type='sector',
                               returns_matrix=None, ohlc_ret_matrices=None, force=False):
    data_sig = _get_data_signature(all_df)
    universe_sig = _build_universe_signature(universe_by_date)
    cached_df, meta = _load_equity_cache(slug, basket_type, universe_by_date)

    if FORCE_REBUILD_EQUITY_CACHE or force:
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
        return cached_df, None

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
    """Compatibility helper (returns Date + Equity)."""
    ohlc_df = compute_equity_ohlc(all_df, universe_by_date)
    if ohlc_df.empty:
        return pd.DataFrame()
    eq_df = ohlc_df[['Date', 'Close']].copy()
    eq_df.rename(columns={'Close': 'Equity'}, inplace=True)
    return eq_df


# ---------------------------------------------------------------------------
# Prebuild equity cache
# ---------------------------------------------------------------------------

def _prebuild_equity_cache_from_signals(all_df, basket_specs):
    """Pre-build equity caches for all baskets. *basket_specs* is a list of
    (name, universe_by_qtr, basket_type) tuples."""
    data_sig = _get_data_signature(all_df)
    latest_source = data_sig.get('latest_date')
    if pd.isna(latest_source):
        return
    latest_source = pd.Timestamp(latest_source).normalize()

    built = 0
    skipped = 0
    total = len(basket_specs)
    last_milestone = 0
    for i, (name, universe, btype) in enumerate(basket_specs, start=1):
        if universe:
            slug = _cache_slugify_label(name)
            is_current = False
            cached, meta = _load_equity_cache(slug, btype, universe)
            universe_sig = _build_universe_signature(universe)
            if cached is not None and _is_equity_cache_valid(meta, data_sig, universe_sig):
                cached_last = pd.to_datetime(meta.get('last_cached_date'), errors='coerce')
                if pd.notna(cached_last) and pd.Timestamp(cached_last).normalize() >= latest_source:
                    is_current = True
            if is_current:
                skipped += 1
            else:
                eq, _ = compute_equity_ohlc_cached(all_df, universe, name, slug, btype)
                if isinstance(eq, pd.DataFrame) and not eq.empty:
                    built += 1

        percent = int((i / total) * 100)
        milestone = percent // 10 * 10
        if milestone > last_milestone and milestone % 10 == 0:
            print(f"  {milestone}% complete ({i} / {total} baskets)")
            last_milestone = milestone

    print(f"Equity cache prebuild complete: built={built}, up_to_date={skipped}")


# ---------------------------------------------------------------------------
# Within-basket correlation
# ---------------------------------------------------------------------------

def _compute_within_basket_correlation(universe_by_qtr, returns_matrix, window=21):
    """Compute rolling within-basket average pairwise correlation via variance decomposition.

    For each rolling window, z-scores returns WITHIN that window, then applies:
        avg_pairwise_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)
    This gives the exact simple average of pairwise correlations.

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

        q_start = quarter_ends[q_idx]
        q_end = quarter_ends[q_idx + 1] if q_idx + 1 < len(quarter_ends) else returns_matrix.index[-1]
        warmup_start_idx = returns_matrix.index.searchsorted(q_start) - window
        warmup_start = returns_matrix.index[max(0, warmup_start_idx)]
        sub_ret = returns_matrix.loc[warmup_start:q_end, tickers]

        if len(sub_ret) < window:
            continue

        q_data = sub_ret.loc[q_start:]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            continue
        sub_ret = sub_ret[valid_tickers]

        ret_arr = sub_ret.values
        dates_arr = sub_ret.index
        q_start_idx = sub_ret.index.searchsorted(q_start)
        if q_idx + 1 < len(quarter_ends):
            q_output_end = q_end - pd.Timedelta(days=1)
        else:
            q_output_end = q_end

        for d_idx in range(window, len(dates_arr)):
            d = dates_arr[d_idx]
            if d < q_start or d > q_output_end:
                continue
            w_slice = ret_arr[d_idx - window + 1:d_idx + 1, :]
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
    result = pd.DataFrame({'Date': all_dates, 'Correlation_Pct': all_corrs})
    result = result.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')
    return result


# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Breadth augmentation + finalization
# ---------------------------------------------------------------------------

def _augment_basket_signals_with_breadth(signals_df, universe_by_qtr, all_signals_df):
    """Merge trend + breakout breadth into basket signals.

    *all_signals_df* is the full individual-ticker signals DataFrame (passed
    explicitly instead of read from a module global).
    """
    _do_timing = BENCHMARK_TIMING or BENCHMARK_BASKETS > 0

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


def _finalize_basket_signals_output(name, slug, hist_folder, merged_all, data_sig, universe_sig,
                                    universe_by_qtr, basket_type='sector', returns_matrix=None,
                                    contrib_df=None, all_signals_df=None):
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
        _compute_and_save_contributions(slug, basket_type, universe_by_qtr,
                                        all_signals_df=all_signals_df,
                                        returns_matrix=returns_matrix)
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


# ---------------------------------------------------------------------------
# Per-constituent contributions
# ---------------------------------------------------------------------------

def _compute_and_save_contributions(slug, basket_type, universe_by_qtr,
                                    all_signals_df=None, returns_matrix=None):
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

        q_start = quarter_ends[q_idx]
        if q_idx + 1 < len(quarter_ends):
            q_end = quarter_ends[q_idx + 1] - pd.Timedelta(days=1)
        else:
            q_end = returns_matrix.index[-1]
        rets_q = returns_matrix.loc[q_start:q_end, tickers].copy()
        if rets_q.empty:
            continue
        rets_q = rets_q.fillna(0.0)

        w0 = pd.Series({t: w_dict[t] for t in tickers})
        w0 = w0 / w0.sum()

        cum_growth = (1 + rets_q).cumprod()
        weighted_cum = cum_growth.multiply(w0, axis=1)
        row_sums = weighted_cum.sum(axis=1)
        row_sums = row_sums.replace(0, np.nan)
        drifted_weights = weighted_cum.div(row_sums, axis=0)

        bod_weights = drifted_weights.shift(1)
        bod_weights.iloc[0] = w0

        contributions = bod_weights * rets_q

        bod_long = bod_weights.stack().rename('Weight_BOD')
        ret_long = rets_q.stack().rename('Daily_Return')
        contrib_long = contributions.stack().rename('Contribution')
        q_df = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
        q_df.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
        quarter_dfs.append(q_df)

    if not quarter_dfs:
        return

    contrib_df = pd.concat(quarter_dfs, ignore_index=True)
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


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------

def _record_basket_timing(name, **timings):
    """Record per-step timings for a basket into the module-level accumulator."""
    _basket_timing_names.append(name)
    for k in _basket_timing:
        _basket_timing[k].append(timings.get(k, 0.0))
    parts = " | ".join(f"{k}={timings.get(k, 0.0):.1f}s" for k in _basket_timing if k != 'total')
    print(f"  [TIMING] {name}: total={timings.get('total', 0.0):.1f}s | {parts}")


# ---------------------------------------------------------------------------
# Main basket processor
# ---------------------------------------------------------------------------

def process_basket_signals(name, universe_by_qtr, charts_folder, basket_type='sector',
                           all_signals_df=None, returns_matrix=None,
                           ohlc_ret_matrices=None, force=False):
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
        not force
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
        force=force,
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
                appended_merged = _augment_basket_signals_with_breadth(
                    appended_signals_df, universe_by_qtr, all_signals_df)
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
                result = _finalize_basket_signals_output(
                    name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig,
                    universe_by_qtr, basket_type,
                    returns_matrix=returns_matrix, contrib_df=_contrib_df,
                    all_signals_df=all_signals_df,
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
    merged_all = _augment_basket_signals_with_breadth(signals_df, universe_by_qtr, all_signals_df)
    if _do_timing:
        _t_breadth_trend = getattr(_augment_basket_signals_with_breadth, '_last_trend_time', 0.0)
        _t_breadth_breakout = getattr(_augment_basket_signals_with_breadth, '_last_breakout_time', 0.0)
        _t_breadth_merge = getattr(_augment_basket_signals_with_breadth, '_last_merge_time', 0.0)
    result = _finalize_basket_signals_output(
        name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig,
        universe_by_qtr, basket_type,
        returns_matrix=returns_matrix, contrib_df=_contrib_df,
        all_signals_df=all_signals_df,
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
# Returns matrix builder
# ---------------------------------------------------------------------------

def _build_or_load_returns_matrix(all_signals_df):
    """Build or load the shared returns matrix (Date x Ticker) and OHLC return matrices."""
    _ret_matrix_path = DATA_FOLDER / 'returns_matrix_500.parquet'
    _ret_fingerprint = f"{all_signals_df.shape}_{all_signals_df['Date'].max()}_{sorted(all_signals_df['Ticker'].unique())}"
    _ret_fp_hash = hashlib.md5(_ret_fingerprint.encode()).hexdigest()
    _ret_fp_path = DATA_FOLDER / 'returns_matrix_500.fingerprint'
    _rebuild_ret_matrix = True

    returns_matrix = None
    ohlc_ret_matrices = None

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

    return returns_matrix, ohlc_ret_matrices


# ---------------------------------------------------------------------------
# Timing summary printer
# ---------------------------------------------------------------------------

def _print_timing_summary():
    """Print the basket processing timing summary table."""
    if not _basket_timing_names:
        return
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


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Build basket signals')
    parser.add_argument('--force', action='store_true')
    parser.add_argument('--benchmark', type=int, default=0)
    args = parser.parse_args()

    _install_timed_print()
    reset_cell_timer("Basket Processing")

    quarter_universe = load_universe_from_disk()
    all_signals_df = pd.read_parquet(SIGNALS_CACHE_FILE)
    if 'Source' in all_signals_df.columns:
        all_signals_df = all_signals_df[all_signals_df['Source'] != 'live'].copy()
        if 'Source' in all_signals_df.columns:
            all_signals_df = all_signals_df.drop(columns=['Source'])

    # Load thematic universes from JSON caches
    beta_universe = load_thematic_universe_from_disk(BETA_CACHE_FILE, 'high')
    low_beta_universe = load_thematic_universe_from_disk(BETA_CACHE_FILE, 'low')
    momentum_universe = load_thematic_universe_from_disk(MOMENTUM_CACHE_FILE, 'winners')
    momentum_losers = load_thematic_universe_from_disk(MOMENTUM_CACHE_FILE, 'losers')
    risk_adj_mom = load_thematic_universe_from_disk(RISK_ADJ_MOM_CACHE_FILE, 'winners')
    risk_adj_mom_losers = load_thematic_universe_from_disk(RISK_ADJ_MOM_CACHE_FILE, 'losers')
    high_yield = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'high_yield')
    div_growth = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'growth')
    div_with_growth = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'with_growth')
    size_universe = load_thematic_universe_from_disk(SIZE_CACHE_FILE)
    vol_leaders = load_thematic_universe_from_disk(VOLUME_CACHE_FILE, 'leaders')
    vol_losers = load_thematic_universe_from_disk(VOLUME_CACHE_FILE, 'losers')
    _, _, sector_universes, industry_universes = load_gics_from_disk()

    returns_matrix, ohlc_ret_matrices = _build_or_load_returns_matrix(all_signals_df)

    all_baskets = [
        ('High Beta', beta_universe, paths.thematic_charts, 'thematic'),
        ('Low Beta', low_beta_universe, paths.thematic_charts, 'thematic'),
        ('Momentum Leaders', momentum_universe, paths.thematic_charts, 'thematic'),
        ('Momentum Losers', momentum_losers, paths.thematic_charts, 'thematic'),
        ('High Dividend Yield', high_yield, paths.thematic_charts, 'thematic'),
        ('Dividend Growth', div_growth, paths.thematic_charts, 'thematic'),
        ('Dividend with Growth', div_with_growth, paths.thematic_charts, 'thematic'),
        ('Risk Adjusted Momentum', risk_adj_mom, paths.thematic_charts, 'thematic'),
        ('Risk Adjusted Momentum Losers', risk_adj_mom_losers, paths.thematic_charts, 'thematic'),
        ('Size', size_universe, paths.thematic_charts, 'thematic'),
        ('Volume Leaders', vol_leaders, paths.thematic_charts, 'thematic'),
        ('Volume Losers', vol_losers, paths.thematic_charts, 'thematic'),
    ]
    all_baskets += [(s, u, paths.sector_charts, 'sector') for s, u in sector_universes.items()]
    override_force = args.force or FORCE_REBUILD_BASKET_SIGNALS
    # On force rebuild: process all industries (full history). Otherwise: only current quarter.
    current_key = get_current_quarter_key()
    if override_force:
        all_baskets += [(ind, u, paths.industry_charts, 'industry') for ind, u in industry_universes.items()]
    else:
        all_baskets += [(ind, u, paths.industry_charts, 'industry')
                        for ind, u in industry_universes.items()
                        if current_key in u and len(u[current_key]) > 0]
    total = len(all_baskets)
    limit = args.benchmark if args.benchmark > 0 else total

    for i, (name, universe_by_qtr, charts_folder, basket_type) in enumerate(all_baskets[:limit], 1):
        print(f"\n[{i}/{min(limit, total)}] Processing {name}...")
        process_basket_signals(
            name, universe_by_qtr, charts_folder, basket_type,
            all_signals_df=all_signals_df,
            returns_matrix=returns_matrix,
            ohlc_ret_matrices=ohlc_ret_matrices,
            force=override_force,
        )

    print(f"\nAll {min(limit, total)} baskets processed.")

    if (BENCHMARK_TIMING or args.benchmark > 0) and _basket_timing_names:
        _print_timing_summary()


if __name__ == '__main__':
    main()
