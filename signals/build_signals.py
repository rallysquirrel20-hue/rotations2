#!/usr/bin/env python
"""Build stock and ETF signal DataFrames from universe tickers."""
import os, argparse, time
import norgatedata
import numba
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from config import (
    SIZE, ETF_SIZE, SIGNALS, RV_MULT, EMA_MULT, RV_EMA_ALPHA,
    HL_EMA_LEN, HL_NORM_LEN,
    MARKET_SYMBOL, INCREMENTAL_MAX_DAYS,
    SIGNALS_CACHE_FILE, ETF_SIGNALS_CACHE_FILE,
    load_universe_from_disk, load_etf_universe_from_disk,
    WriteThroughPath, atomic_write_parquet,
    _install_timed_print, reset_cell_timer,
    DATA_FOLDER,
)


pass_times = {'data_load': 0, 'pass1': 0, 'pass5': 0}


# ---------------------------------------------------------------------------
# Numba-accelerated core loops (passes 1-4 and pass 5)
# ---------------------------------------------------------------------------

@numba.njit(cache=True)
def _numba_passes_1_to_4(closes, highs, lows, opens, rv_emas, rv_mult, ema_mult, start_idx, n):
    """Combined passes 1-4: Trend/Pivots, Ranges/Targets, Signal Detection, Regime.
    Returns a flat tuple of numpy arrays."""
    # -- Pass 1: Trend and Pivots --
    # trends: 0=downtrend, 1=uptrend, -1=uninitialized
    trends = np.full(n, np.int8(-1))
    resistance = np.full(n, np.nan)
    support = np.full(n, np.nan)
    is_up_rot = np.zeros(n, dtype=numba.boolean)
    is_down_rot = np.zeros(n, dtype=numba.boolean)

    trends[start_idx] = 0  # start in downtrend
    resistance[start_idx] = closes[start_idx] * (1.0 + rv_emas[start_idx] * rv_mult)

    for i in range(start_idx + 1, n):
        close = closes[i]
        rv = rv_emas[i] * rv_mult
        prev_trend = trends[i - 1]
        prev_res = resistance[i - 1]
        prev_sup = support[i - 1]

        if prev_trend == 0:  # downtrend
            resistance[i] = min(close * (1.0 + rv), prev_res)
            if close > prev_res:
                trends[i] = 1
                support[i] = close * (1.0 - rv)
                resistance[i] = prev_res
                is_up_rot[i] = True
            else:
                trends[i] = 0
        else:  # uptrend
            if not np.isnan(prev_sup):
                support[i] = max(close * (1.0 - rv), prev_sup)
            else:
                support[i] = close * (1.0 - rv)
            if not np.isnan(prev_sup) and close < prev_sup:
                trends[i] = 0
                resistance[i] = close * (1.0 + rv)
                support[i] = prev_sup
                is_down_rot[i] = True
            else:
                trends[i] = 1

    # -- Pass 2: Rotation Ranges and Targets --
    rotation_open = np.full(n, np.nan)
    up_range = np.full(n, np.nan)
    down_range = np.full(n, np.nan)
    up_range_ema = np.full(n, np.nan)
    down_range_ema = np.full(n, np.nan)
    upper_target = np.full(n, np.nan)
    lower_target = np.full(n, np.nan)

    rot_open_price = np.nan
    up_ema = np.nan
    down_ema = np.nan
    prev_upper = np.nan
    prev_lower = np.nan

    for i in range(start_idx, n):
        curr_trend = trends[i]
        prev_trend = trends[i - 1] if i > start_idx else np.int8(-1)
        # New rotation starts
        if i == start_idx or curr_trend != prev_trend:
            if not np.isnan(rot_open_price) and i > start_idx:
                if prev_trend == 1:
                    fr = up_range[i - 1]
                    if np.isnan(up_ema):
                        up_ema = fr
                    else:
                        up_ema = fr * ema_mult + up_ema * (1.0 - ema_mult)
                else:
                    fr = down_range[i - 1]
                    if np.isnan(down_ema):
                        down_ema = fr
                    else:
                        down_ema = fr * ema_mult + down_ema * (1.0 - ema_mult)

            rot_open_price = closes[i - 1] if i > start_idx else closes[i]
            rotation_open[i] = rot_open_price

            if curr_trend == 1 and not np.isnan(up_ema):
                calc = closes[i] * (1.0 + up_ema)
                if np.isnan(prev_upper) or closes[i] > prev_upper or calc < prev_upper:
                    prev_upper = calc
            if curr_trend == 0 and not np.isnan(down_ema):
                calc = closes[i] * (1.0 - down_ema)
                if np.isnan(prev_lower) or closes[i] < prev_lower or calc > prev_lower:
                    prev_lower = calc

        if not np.isnan(rot_open_price) and np.isfinite(rot_open_price) and rot_open_price > 0.0:
            if curr_trend == 1 and np.isfinite(highs[i]):
                up_range[i] = abs((highs[i] - rot_open_price) / rot_open_price)
            elif curr_trend != 1 and np.isfinite(lows[i]):
                down_range[i] = abs((lows[i] - rot_open_price) / rot_open_price)

        up_range_ema[i] = up_ema
        down_range_ema[i] = down_ema
        upper_target[i] = prev_upper
        lower_target[i] = prev_lower

    # -- Pass 3: Signal Detection --
    is_breakout = np.zeros(n, dtype=numba.boolean)
    is_breakdown = np.zeros(n, dtype=numba.boolean)
    is_btfd = np.zeros(n, dtype=numba.boolean)
    is_stfr = np.zeros(n, dtype=numba.boolean)
    btfd_entry_price = np.full(n, np.nan)
    stfr_entry_price = np.full(n, np.nan)
    rotation_ids = np.zeros(n, dtype=np.int64)
    btfd_triggered = np.zeros(n, dtype=numba.boolean)
    stfr_triggered = np.zeros(n, dtype=numba.boolean)

    rotation_id = np.int64(0)
    # Use boolean arrays indexed by rotation_id for set membership
    btfd_seen = np.zeros(n + 1, dtype=numba.boolean)
    stfr_seen = np.zeros(n + 1, dtype=numba.boolean)

    for i in range(start_idx + 1, n):
        if trends[i] != trends[i - 1]:
            rotation_id += 1
        rotation_ids[i] = rotation_id

        if is_up_rot[i] and not np.isnan(upper_target[i - 1]) and closes[i] > upper_target[i - 1]:
            is_breakout[i] = True
        if is_down_rot[i] and not np.isnan(lower_target[i - 1]) and closes[i] < lower_target[i - 1]:
            is_breakdown[i] = True
        if trends[i] == 0 and trends[i - 1] == 0 and not np.isnan(lower_target[i - 1]) and lows[i] <= lower_target[i - 1]:
            if not btfd_seen[rotation_id]:
                is_btfd[i] = True
                if opens[i] <= lower_target[i - 1]:
                    btfd_entry_price[i] = opens[i]
                else:
                    btfd_entry_price[i] = lower_target[i - 1]
                btfd_seen[rotation_id] = True
        if trends[i] == 1 and trends[i - 1] == 1 and not np.isnan(upper_target[i - 1]) and highs[i] >= upper_target[i - 1]:
            if not stfr_seen[rotation_id]:
                is_stfr[i] = True
                if opens[i] >= upper_target[i - 1]:
                    stfr_entry_price[i] = opens[i]
                else:
                    stfr_entry_price[i] = upper_target[i - 1]
                stfr_seen[rotation_id] = True

        btfd_triggered[i] = btfd_seen[rotation_id]
        stfr_triggered[i] = stfr_seen[rotation_id]

    # -- Pass 4: Regime (Breakout Sequence) --
    is_breakout_seq = np.zeros(n, dtype=numba.boolean)
    last_signal = np.int8(0)  # 0=none, 1=breakout, 2=breakdown
    for i in range(n):
        is_breakout_seq[i] = (last_signal == 1)
        if is_breakout[i]:
            last_signal = np.int8(1)
        elif is_breakdown[i]:
            last_signal = np.int8(2)

    return (
        trends, resistance, support, is_up_rot, is_down_rot,
        rotation_open, up_range, down_range, up_range_ema, down_range_ema,
        upper_target, lower_target,
        is_breakout, is_breakdown, is_btfd, is_stfr,
        btfd_entry_price, stfr_entry_price, rotation_ids,
        btfd_triggered, stfr_triggered, is_breakout_seq,
    )


@numba.njit(cache=True)
def _numba_pass5_signal(entry_arr, exit_arr, custom_entry_prices, has_custom,
                        closes, highs, lows, n, is_short):
    """Pass 5 for a single signal type: trade tracking.
    Returns 7 arrays: entry_price, change, exit_idx, exit_price, final_change,
    mfe, mae."""
    MAX_STACK = 32

    entry_price_col = np.full(n, np.nan)
    change_col = np.full(n, np.nan)
    exit_idx_col = np.full(n, np.int64(-1))
    exit_price_col = np.full(n, np.nan)
    final_change_col = np.full(n, np.nan)
    mfe_col = np.full(n, np.nan)
    mae_col = np.full(n, np.nan)


    # Position stack
    p_idx = np.zeros(MAX_STACK, dtype=np.int64)
    p_price = np.zeros(MAX_STACK)
    p_hi = np.zeros(MAX_STACK)
    p_lo = np.zeros(MAX_STACK)
    n_open = 0

    for i in range(n):
        # Update live change
        if n_open > 0 and not entry_arr[i]:
            lep = p_price[n_open - 1]
            if is_short:
                change_col[i] = (lep - closes[i]) / lep
            else:
                change_col[i] = (closes[i] - lep) / lep

        # Update MFE/MAE
        for p in range(n_open):
            if highs[i] > p_hi[p]:
                p_hi[p] = highs[i]
            if lows[i] < p_lo[p]:
                p_lo[p] = lows[i]

        # Exit: close all
        if n_open > 0 and exit_arr[i]:
            for p in range(n_open):
                ep = p_price[p]
                if is_short:
                    fc = (ep - closes[i]) / ep
                    mfe_v = (ep - p_lo[p]) / ep
                    mae_v = (ep - p_hi[p]) / ep
                else:
                    fc = (closes[i] - ep) / ep
                    mfe_v = (p_hi[p] - ep) / ep
                    mae_v = (p_lo[p] - ep) / ep

                eidx = p_idx[p]
                exit_idx_col[eidx] = i
                exit_price_col[eidx] = closes[i]
                final_change_col[eidx] = fc
                mfe_col[eidx] = mfe_v
                mae_col[eidx] = mae_v

            n_open = 0

        # New entry
        if entry_arr[i]:
            if has_custom and not np.isnan(custom_entry_prices[i]):
                ep = custom_entry_prices[i]
            else:
                ep = closes[i]
            if n_open < MAX_STACK:
                p_idx[n_open] = i
                p_price[n_open] = ep
                p_hi[n_open] = highs[i]
                p_lo[n_open] = lows[i]
                n_open += 1

        if n_open > 0:
            entry_price_col[i] = p_price[n_open - 1]

    # Force-close remaining open positions
    if n_open > 0:
        exit_i = n - 1
        while exit_i >= 0 and np.isnan(closes[exit_i]):
            exit_i -= 1
        if exit_i >= 0:
            for p in range(n_open):
                ep = p_price[p]
                if is_short:
                    fc = (ep - closes[exit_i]) / ep
                    mfe_v = (ep - p_lo[p]) / ep
                    mae_v = (ep - p_hi[p]) / ep
                else:
                    fc = (closes[exit_i] - ep) / ep
                    mfe_v = (p_hi[p] - ep) / ep
                    mae_v = (p_lo[p] - ep) / ep

                eidx = p_idx[p]
                exit_idx_col[eidx] = exit_i
                exit_price_col[eidx] = closes[exit_i]
                final_change_col[eidx] = fc
                mfe_col[eidx] = mfe_v
                mae_col[eidx] = mae_v

    return (entry_price_col, change_col, exit_idx_col, exit_price_col,
            final_change_col, mfe_col, mae_col)


@numba.njit(cache=True)
def _numba_hl_reaction(closes, highs, lows, ema_len, n):
    """H/L Reaction EMAs (port of rotations_strategy.txt Pine script).

    On each bar that makes a new high (high > high[-1]) we compute the percent
    distance from close to the prior bar's high; otherwise the raw series is
    NaN and the EMA carries forward. Same logic mirrored for lows. EMAs are
    seeded on the first valid observation. Returns ema_high, ema_low arrays.
    The [-1, +1] normalization is NOT stored here -- callers compute it over
    a rolling window on demand (see HL_NORM_LEN)."""
    alpha = 2.0 / (ema_len + 1.0)
    ema_high = np.full(n, np.nan)
    ema_low = np.full(n, np.nan)
    prev_high_ema = np.nan
    prev_low_ema = np.nan
    for i in range(1, n):
        h = highs[i]
        ph = highs[i - 1]
        if np.isfinite(h) and np.isfinite(ph) and ph > 0 and h > ph:
            src_h = (closes[i] - ph) / ph * 100.0
            prev_high_ema = src_h if np.isnan(prev_high_ema) else alpha * src_h + (1.0 - alpha) * prev_high_ema

        l = lows[i]
        pl = lows[i - 1]
        if np.isfinite(l) and np.isfinite(pl) and pl > 0 and l < pl:
            src_l = (closes[i] - pl) / pl * 100.0
            prev_low_ema = src_l if np.isnan(prev_low_ema) else alpha * src_l + (1.0 - alpha) * prev_low_ema

        ema_high[i] = prev_high_ema
        ema_low[i] = prev_low_ema
    return ema_high, ema_low


@numba.njit(cache=True)
def _numba_price_chg_ema(closes, ema_len, n):
    """EMA of bar-to-bar % price change, no gating condition (every bar contributes).
    Returns ema_pricechg array (raw EMA values; caller normalizes to [-1,+1])."""
    alpha = 2.0 / (ema_len + 1.0)
    ema_pricechg = np.full(n, np.nan)
    prev_ema = np.nan
    for i in range(1, n):
        c = closes[i]
        pc = closes[i - 1]
        if np.isfinite(c) and np.isfinite(pc) and pc > 0:
            pct = (c - pc) / pc * 100.0
            prev_ema = pct if np.isnan(prev_ema) else alpha * pct + (1.0 - alpha) * prev_ema
        ema_pricechg[i] = prev_ema
    return ema_pricechg


def _build_signals_from_df(df, ticker):
    """Core signal builder that expects OHLCV with a Date index or column.
    Uses numba-accelerated passes for ~50-100x speedup on the inner loops."""
    global pass_times

    t0 = time.time()
    df = df.reset_index().rename(columns={'index': 'Date'})
    n = len(df)

    # Core arrays (contiguous float64 for numba)
    closes = np.ascontiguousarray(df['Close'].values, dtype=np.float64)
    highs = np.ascontiguousarray(df['High'].values, dtype=np.float64)
    lows = np.ascontiguousarray(df['Low'].values, dtype=np.float64)
    opens = np.ascontiguousarray(df['Open'].values, dtype=np.float64)
    df['RV'] = abs(df['Close'] - df['Close'].shift(1)) / df['Close'].shift(1)
    df['RV_EMA'] = df['RV'].ewm(span=10, adjust=False).mean()
    rv_emas = np.ascontiguousarray(df['RV_EMA'].values, dtype=np.float64)

    start_idx = df['RV_EMA'].first_valid_index()
    if start_idx is None:
        if n < 2:
            return None
        df['RV'] = df['RV'].fillna(0.0)
        df['RV_EMA'] = df['RV_EMA'].fillna(0.0)
        rv_emas = np.ascontiguousarray(df['RV_EMA'].values, dtype=np.float64)
        start_idx = 1

    pass_times['data_load'] += time.time() - t0
    t1 = time.time()

    # === PASSES 1-4: Numba-accelerated ===
    (
        trends_i8, resistance, support, is_up_rot, is_down_rot,
        rotation_open, up_range, down_range, up_range_ema, down_range_ema,
        upper_target, lower_target,
        is_breakout, is_breakdown, is_btfd, is_stfr,
        btfd_entry_price, stfr_entry_price, rotation_ids,
        btfd_triggered, stfr_triggered, is_breakout_seq,
    ) = _numba_passes_1_to_4(closes, highs, lows, opens, rv_emas, RV_MULT, EMA_MULT, start_idx, n)

    # Convert int8 trends back to object (True/False/None) for downstream compatibility
    trends = np.empty(n, dtype=object)
    for i in range(n):
        v = trends_i8[i]
        if v == 1:
            trends[i] = True
        elif v == 0:
            trends[i] = False
        else:
            trends[i] = None

    df['Trend'] = trends
    df['Resistance_Pivot'] = resistance
    df['Support_Pivot'] = support
    df['Is_Up_Rotation'] = is_up_rot.astype(bool)
    df['Is_Down_Rotation'] = is_down_rot.astype(bool)
    df['Rotation_Open'] = rotation_open
    df['Up_Range'] = up_range
    df['Down_Range'] = down_range
    df['Up_Range_EMA'] = up_range_ema
    df['Down_Range_EMA'] = down_range_ema
    df['Upper_Target'] = upper_target
    df['Lower_Target'] = lower_target
    df['Is_Breakout'] = is_breakout.astype(bool)
    df['Is_Breakdown'] = is_breakdown.astype(bool)
    df['Is_BTFD'] = is_btfd.astype(bool)
    df['Is_STFR'] = is_stfr.astype(bool)
    df['BTFD_Target_Entry'] = btfd_entry_price
    df['STFR_Target_Entry'] = stfr_entry_price
    df['Rotation_ID'] = rotation_ids
    df['BTFD_Triggered'] = btfd_triggered.astype(bool)
    df['STFR_Triggered'] = stfr_triggered.astype(bool)
    df['Is_Breakout_Sequence'] = is_breakout_seq.astype(bool)

    pass_times['pass1'] += time.time() - t1
    t5 = time.time()

    # === PASS 5: Numba-accelerated trade tracking per signal ===
    signal_configs = [
        ('Up_Rot',    is_up_rot,    is_down_rot, None,             False, False),
        ('Down_Rot',  is_down_rot,  is_up_rot,   None,             False, True),
        ('Breakout',  is_breakout,  is_breakdown, None,            False, False),
        ('Breakdown', is_breakdown, is_breakout,  None,            False, True),
        ('BTFD',      is_btfd,      is_breakdown, btfd_entry_price, True,  False),
        ('STFR',      is_stfr,      is_breakout,  stfr_entry_price, True,  True),
    ]

    dates = df['Date'].values
    new_cols = {}
    dummy_prices = np.full(n, np.nan)

    for sig_name, entry_arr, exit_arr, custom_prices, has_custom, is_short in signal_configs:
        cp = custom_prices if has_custom else dummy_prices
        (
            entry_price_col, change_col, exit_idx_col, exit_price_col,
            final_change_col, mfe_col, mae_col,
        ) = _numba_pass5_signal(entry_arr, exit_arr, cp, has_custom,
                                closes, highs, lows, n, is_short)

        exit_date_col = np.empty(n, dtype=object)
        for i in range(n):
            idx = exit_idx_col[i]
            exit_date_col[i] = dates[idx] if idx >= 0 else np.nan

        new_cols[f'{sig_name}_Entry_Price'] = entry_price_col
        new_cols[f'{sig_name}_Change'] = change_col
        new_cols[f'{sig_name}_Exit_Date'] = exit_date_col
        new_cols[f'{sig_name}_Exit_Price'] = exit_price_col
        new_cols[f'{sig_name}_Final_Change'] = final_change_col
        new_cols[f'{sig_name}_MFE'] = mfe_col
        new_cols[f'{sig_name}_MAE'] = mae_col

    pass_times['pass5'] += time.time() - t5

    # === H/L Reaction EMAs ===
    ema_high, ema_low = _numba_hl_reaction(closes, highs, lows, HL_EMA_LEN, n)
    new_cols['EMA_High'] = ema_high
    new_cols['EMA_Low'] = ema_low

    # === Price Change EMA (ungated — every bar contributes) ===
    new_cols['EMA_PriceChg'] = _numba_price_chg_ema(closes, HL_EMA_LEN, n)

    new_cols['Ticker'] = ticker
    df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
    return df


def _build_signals_next_row(prev_row, live_price, live_dt,
                             live_high=None, live_low=None, live_open=None):
    """Incremental one-bar update using cached last row state.

    For EOD updates, pass actual OHLC via live_high/live_low/live_open so that
    BTFD (fires when low <= lower_target) and STFR (fires when high >= upper_target)
    are detected correctly. All new params default to None for backward compatibility
    with the existing live/intraday call sites that only supply a single close price.
    """
    if prev_row is None or pd.isna(live_price):
        return None

    prev = prev_row.to_dict()
    prev_date = pd.to_datetime(prev.get('Date'))
    live_dt = pd.to_datetime(live_dt)
    if pd.notna(prev_date) and live_dt <= prev_date:
        live_dt = prev_date + pd.Timedelta(minutes=1)

    close = float(live_price)
    high  = float(live_high)  if live_high  is not None else close
    low   = float(live_low)   if live_low   is not None else close
    open_ = float(live_open)  if live_open  is not None else close
    prev_close = prev.get('Close', np.nan)
    if pd.isna(prev_close) or prev_close == 0:
        return None

    rv = abs(close - prev_close) / prev_close
    prev_rv_ema = prev.get('RV_EMA', np.nan)
    rv_ema = rv if pd.isna(prev_rv_ema) else (rv * RV_EMA_ALPHA + prev_rv_ema * (1 - RV_EMA_ALPHA))

    prev_trend = prev.get('Trend', False)
    prev_res = prev.get('Resistance_Pivot', np.nan)
    prev_sup = prev.get('Support_Pivot', np.nan)

    is_up_rot = False
    is_down_rot = False
    rv_mult = rv_ema * RV_MULT

    if prev_trend == False:
        base_res = close * (1 + rv_mult)
        resistance = base_res if pd.isna(prev_res) else min(base_res, prev_res)
        if not pd.isna(prev_res) and close > prev_res:
            trend = True
            support = close * (1 - rv_mult)
            resistance = prev_res
            is_up_rot = True
        else:
            trend = False
            support = prev_sup
    else:
        support = close * (1 - rv_mult) if pd.isna(prev_sup) else max(close * (1 - rv_mult), prev_sup)
        if not pd.isna(prev_sup) and close < prev_sup:
            trend = False
            resistance = close * (1 + rv_mult)
            support = prev_sup
            is_down_rot = True
        else:
            trend = True
            resistance = prev_res

    rotation_change = (trend != prev_trend)
    rotation_id = int(prev.get('Rotation_ID', 0))
    if rotation_change:
        rotation_id += 1

    prev_up_ema = prev.get('Up_Range_EMA', np.nan)
    prev_down_ema = prev.get('Down_Range_EMA', np.nan)
    prev_up_range = prev.get('Up_Range', np.nan)
    prev_down_range = prev.get('Down_Range', np.nan)

    up_ema = prev_up_ema
    down_ema = prev_down_ema
    if rotation_change:
        if prev_trend == True:
            if not pd.isna(prev_up_range):
                up_ema = prev_up_range if pd.isna(prev_up_ema) else (prev_up_range * EMA_MULT + prev_up_ema * (1 - EMA_MULT))
        else:
            if not pd.isna(prev_down_range):
                down_ema = prev_down_range if pd.isna(prev_down_ema) else (prev_down_range * EMA_MULT + prev_down_ema * (1 - EMA_MULT))

    prev_rot_open = prev.get('Rotation_Open', np.nan)
    if rotation_change:
        rot_open = prev_close
    else:
        rot_open = prev_rot_open if not pd.isna(prev_rot_open) else prev_close

    if trend == True:
        up_range = abs((high - rot_open) / rot_open) if rot_open else np.nan
        down_range = np.nan
    else:
        down_range = abs((low - rot_open) / rot_open) if rot_open else np.nan
        up_range = np.nan

    prev_upper = prev.get('Upper_Target', np.nan)
    prev_lower = prev.get('Lower_Target', np.nan)
    upper_target = prev_upper
    lower_target = prev_lower

    if rotation_change and trend == True and not pd.isna(up_ema):
        calculated = close * (1 + up_ema)
        if pd.isna(prev_upper) or close > prev_upper or calculated < prev_upper:
            upper_target = calculated
    if rotation_change and trend == False and not pd.isna(down_ema):
        calculated = close * (1 - down_ema)
        if pd.isna(prev_lower) or close < prev_lower or calculated > prev_lower:
            lower_target = calculated

    btfd_triggered = bool(prev.get('BTFD_Triggered', False))
    stfr_triggered = bool(prev.get('STFR_Triggered', False))
    if rotation_change:
        btfd_triggered = False
        stfr_triggered = False

    is_breakout = is_up_rot and not pd.isna(prev_upper) and close > prev_upper
    is_breakdown = is_down_rot and not pd.isna(prev_lower) and close < prev_lower

    is_btfd = False
    btfd_entry = np.nan
    if trend == False and prev_trend == False and not pd.isna(prev_lower) and low <= prev_lower and not btfd_triggered:
        is_btfd = True
        btfd_entry = open_ if open_ <= prev_lower else prev_lower
        btfd_triggered = True

    is_stfr = False
    stfr_entry = np.nan
    if trend == True and prev_trend == True and not pd.isna(prev_upper) and high >= prev_upper and not stfr_triggered:
        is_stfr = True
        stfr_entry = open_ if open_ >= prev_upper else prev_upper
        stfr_triggered = True

    if prev.get('Is_Breakout', False):
        last_signal = 'breakout'
    elif prev.get('Is_Breakdown', False):
        last_signal = 'breakdown'
    elif prev.get('Is_Breakout_Sequence', False):
        last_signal = 'breakout'
    else:
        last_signal = 'breakdown'
    is_breakout_seq = (last_signal == 'breakout')

    # H/L Reaction EMAs -- same one-bar recurrence as the batch numba path.
    alpha_hl = 2.0 / (HL_EMA_LEN + 1.0)
    prev_high = prev.get('High', np.nan)
    prev_low = prev.get('Low', np.nan)
    prev_ema_high = prev.get('EMA_High', np.nan)
    prev_ema_low = prev.get('EMA_Low', np.nan)
    ema_high_val = prev_ema_high
    ema_low_val = prev_ema_low
    if pd.notna(prev_high) and prev_high > 0 and high > prev_high:
        src_h = (close - prev_high) / prev_high * 100.0
        ema_high_val = src_h if pd.isna(prev_ema_high) else alpha_hl * src_h + (1.0 - alpha_hl) * prev_ema_high
    if pd.notna(prev_low) and prev_low > 0 and low < prev_low:
        src_l = (close - prev_low) / prev_low * 100.0
        ema_low_val = src_l if pd.isna(prev_ema_low) else alpha_hl * src_l + (1.0 - alpha_hl) * prev_ema_low

    # Price Change EMA — every bar contributes (no gating).
    prev_ema_pricechg = prev.get('EMA_PriceChg', np.nan)
    ema_pricechg_val = prev_ema_pricechg
    if pd.notna(prev_close) and prev_close > 0:
        pct = (close - prev_close) / prev_close * 100.0
        ema_pricechg_val = pct if pd.isna(prev_ema_pricechg) else alpha_hl * pct + (1.0 - alpha_hl) * prev_ema_pricechg

    new_row = prev.copy()
    new_row.update({
        'Date': live_dt,
        'Open': open_,
        'High': high,
        'Low': low,
        'Close': close,
        'Volume': 0,
        'Turnover': np.nan,
        'RV': rv,
        'RV_EMA': rv_ema,
        'Trend': trend,
        'Resistance_Pivot': resistance,
        'Support_Pivot': support,
        'Is_Up_Rotation': is_up_rot,
        'Is_Down_Rotation': is_down_rot,
        'Rotation_Open': rot_open,
        'Up_Range': up_range,
        'Down_Range': down_range,
        'Up_Range_EMA': up_ema,
        'Down_Range_EMA': down_ema,
        'Upper_Target': upper_target,
        'Lower_Target': lower_target,
        'Is_Breakout': is_breakout,
        'Is_Breakdown': is_breakdown,
        'Is_BTFD': is_btfd,
        'Is_STFR': is_stfr,
        'BTFD_Target_Entry': btfd_entry,
        'STFR_Target_Entry': stfr_entry,
        'Is_Breakout_Sequence': is_breakout_seq,
        'Rotation_ID': rotation_id,
        'BTFD_Triggered': btfd_triggered,
        'STFR_Triggered': stfr_triggered,
        'EMA_High': ema_high_val,
        'EMA_Low': ema_low_val,
        'EMA_PriceChg': ema_pricechg_val,
    })

    if is_up_rot:
        new_row['Up_Rot_Entry_Price'] = close
    if is_down_rot:
        new_row['Down_Rot_Entry_Price'] = close
    if is_breakout:
        new_row['Breakout_Entry_Price'] = upper_target
    if is_breakdown:
        new_row['Breakdown_Entry_Price'] = lower_target
    if is_btfd:
        new_row['BTFD_Entry_Price'] = btfd_entry
    if is_stfr:
        new_row['STFR_Entry_Price'] = stfr_entry

    return pd.Series(new_row)


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
      ('append', list[pd.Series]) -- no adjustment; new EOD row(s) computed
      ('full_rebuild', str)       -- reason string for diagnostics
      ('no_new_data', None)       -- cache already has the latest date
      ('error', str)              -- exception description
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


def _get_latest_norgate_date():
    """Get the most recent date available in Norgate database using the market symbol."""
    df = norgatedata.price_timeseries(
        MARKET_SYMBOL,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or df.empty:
        return None
    df = df.reset_index()
    date_col = df.columns[0]
    return pd.to_datetime(df[date_col]).max().normalize()


# ---------------------------------------------------------------------------
# Shared constants for dtype optimization (used by both stock and ETF paths)
# ---------------------------------------------------------------------------
_BOOL_COLS = [
    'Is_Up_Rotation', 'Is_Down_Rotation', 'Is_Breakout', 'Is_Breakdown',
    'Is_BTFD', 'Is_STFR', 'Is_Breakout_Sequence', 'BTFD_Triggered', 'STFR_Triggered',
]
_BASE_KEEP = [
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
    'EMA_High', 'EMA_Low', 'EMA_PriceChg',
]


def _build_keep_set():
    """Return the set of columns to retain from signal DataFrames."""
    signal_cols = []
    for sig in SIGNALS:
        signal_cols.extend([
            f'{sig}_Entry_Price', f'{sig}_Exit_Date', f'{sig}_Exit_Price',
            f'{sig}_Final_Change', f'{sig}_MFE', f'{sig}_MAE',
        ])
    return set(_BASE_KEEP + signal_cols)


def _optimize_dtypes(df):
    """Apply dtype optimizations for efficient parquet storage."""
    for col in _BOOL_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(False).astype(bool)
    if 'Trend' in df.columns:
        df['Trend'] = df['Trend'].map(
            {True: 1.0, False: 0.0, 1.0: 1.0, 0.0: 0.0, None: float('nan')}
        ).astype('float32')
    if 'Rotation_ID' in df.columns:
        df['Rotation_ID'] = df['Rotation_ID'].fillna(0).astype('int32')
    return df


def _save_signals(df, cache_file):
    """Optimize dtypes, write to disk with Source='norgate', return df without Source."""
    df = _optimize_dtypes(df)
    df['Source'] = 'norgate'
    tmp = cache_file.with_suffix('.parquet.tmp')
    df.to_parquet(tmp, index=False, compression='snappy')
    tmp.replace(cache_file)
    WriteThroughPath(cache_file).sync()
    del df['Source']
    return df


def _progress(processed, total, last_milestone, prefix=""):
    pct = int((processed / total) * 100) if total else 100
    ms = pct // 10 * 10
    if ms > last_milestone[0] and ms % 10 == 0:
        print(f"  {prefix}{ms}% ({processed}/{total})")
        last_milestone[0] = ms


# ---------------------------------------------------------------------------
# Unified cache freshness check
# ---------------------------------------------------------------------------
def _cache_is_current(df):
    if df is None or df.empty or 'Date' not in df.columns:
        return False

    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])

    # If live rows are present at the latest date, cache is not current -- Norgate should rebuild/replace
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
    return True




# ---------------------------------------------------------------------------
# Unified incremental update (works for both stock and ETF)
# ---------------------------------------------------------------------------
def _incremental_update(cached_df, universe, cache_file, days_stale=1, label=""):
    """Append new trading day(s) to the signals cache without a full rebuild."""
    inc_start = time.time()
    pfx = f"[{label}] " if label else ""
    print("=" * 60)
    print(f"{pfx}Running incremental signals update...")

    all_tickers = sorted({t for tickers in universe.values() for t in tickers})
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
    print(f"  {pfx}Universe: {len(universe_set)} | Cached: {len(cached_tickers)} | "
          f"New: {len(new_tickers)} | Dropped: {len(cached_tickers - universe_set)}")

    check_tickers = [t for t in all_tickers if t in cached_tickers]
    append_rows = {}
    rebuild_tickers = list(new_tickers)

    def _safe_append(t):
        return t, _build_signals_append_ticker(t, last_rows[t], limit=days_stale + 1)

    last_ms = [0]
    total_check = len(check_tickers)
    max_w = min(8, os.cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futures = [ex.submit(_safe_append, t) for t in check_tickers]
        for i, fut in enumerate(futures, 1):
            t, (action, payload) = fut.result()
            _progress(i, total_check, last_ms, f"{pfx}Append check ")
            if action == 'append':
                append_rows[t] = payload
            elif action in ('full_rebuild', 'error'):
                rebuild_tickers.append(t)

    print(f"  {pfx}Append: {len(append_rows)} | Rebuild: {len(rebuild_tickers)}")

    rebuild_results = {}
    if rebuild_tickers:
        print(f"  {pfx}Rebuilding {len(rebuild_tickers)} tickers...")
        def _safe_rebuild(t):
            try:
                return t, build_signals_for_ticker(t)
            except Exception:
                return t, None

        last_ms = [0]
        rb_total = len(rebuild_tickers)
        with ThreadPoolExecutor(max_workers=max_w) as ex:
            futures = [ex.submit(_safe_rebuild, t) for t in rebuild_tickers]
            for i, fut in enumerate(futures, 1):
                t, result = fut.result()
                _progress(i, rb_total, last_ms, f"{pfx}Rebuild ")
                if result is not None:
                    rebuild_results[t] = result

    keep_set = set(cached_df.columns)
    if rebuild_results:
        cached_df = cached_df[~cached_df['Ticker'].isin(rebuild_results.keys())].copy()
        frames = [df_r[list(set(df_r.columns) & keep_set)].dropna(axis=1, how='all')
                  for df_r in rebuild_results.values()]
        frames = [f for f in frames if not f.empty]
        if frames:
            cached_df = pd.concat([cached_df] + frames, ignore_index=True)

    if append_rows:
        rows = [{k: v for k, v in rs.items() if k in keep_set}
                for row_list in append_rows.values() for rs in row_list]
        cached_df = pd.concat([cached_df, pd.DataFrame(rows)], ignore_index=True)

    cached_df = _save_signals(cached_df, cache_file)
    print(f"  {pfx}Incremental update: {len(cached_df)} rows ({time.time() - inc_start:.1f}s)")
    print("=" * 60)
    return cached_df


# ---------------------------------------------------------------------------
# Unified load-or-build (works for both stock and ETF)
# ---------------------------------------------------------------------------
def _load_or_build(universe, cache_file, force=False, label=""):
    """Load signals from parquet if current, else incremental or full rebuild."""
    t0 = time.time()
    pfx = f"[{label}] " if label else ""
    cached_df = None

    if not force and cache_file.exists():
        try:
            cached_df = pd.read_parquet(cache_file)
            if 'Date' in cached_df.columns:
                cached_df['Date'] = pd.to_datetime(cached_df['Date'])
            if _cache_is_current(cached_df):
                print(f"{pfx}Signals loaded from cache ({len(cached_df)} rows, "
                      f"{cached_df['Ticker'].nunique()} tickers)")
                if 'Source' in cached_df.columns:
                    cached_df = cached_df[cached_df['Source'] != 'live']
                    cached_df = cached_df.drop(columns=['Source'], errors='ignore')
                return cached_df
        except Exception as e:
            print(f"{pfx}Warning: Failed to load cache: {e}")
            cached_df = None

    if cached_df is not None and not force:
        latest_cache = pd.to_datetime(cached_df['Date']).max()
        latest_norgate = _get_latest_norgate_date()
        if latest_norgate is not None and pd.notna(latest_cache):
            days_stale = (latest_norgate - latest_cache.normalize()).days
            if 0 < days_stale <= INCREMENTAL_MAX_DAYS:
                print(f"{pfx}Cache is {days_stale} day(s) stale, incremental update...")
                try:
                    return _incremental_update(cached_df, universe, cache_file, days_stale, label)
                except Exception as e:
                    print(f"{pfx}Incremental failed ({e}), full rebuild...")
            elif days_stale > INCREMENTAL_MAX_DAYS:
                print(f"{pfx}Cache stale by {days_stale} days, full rebuild...")
    else:
        print(f"{pfx}{'Force rebuild...' if force else 'No cache found, building...'}")

    all_tickers = sorted({t for tickers in universe.values() for t in tickers})
    print(f"{pfx}Total unique tickers: {len(all_tickers)}")
    print("=" * 60)

    keep_set = _build_keep_set()
    all_signals = []
    failed = []

    def _safe_build(t):
        try:
            return t, build_signals_for_ticker(t)
        except Exception:
            return t, None

    last_ms = [0]
    total = len(all_tickers)
    max_w = min(8, os.cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futures = [ex.submit(_safe_build, t) for t in all_tickers]
        for i, fut in enumerate(futures, 1):
            t, result = fut.result()
            _progress(i, total, last_ms, pfx)
            if result is not None:
                cols = list(set(result.columns) & keep_set)
                trimmed = result[cols].dropna(axis=1, how='all') if cols else pd.DataFrame()
                if not trimmed.empty:
                    all_signals.append(trimmed)
                else:
                    failed.append(t)
            else:
                failed.append(t)

    if not all_signals:
        if label:
            print(f"{pfx}Warning: No signals generated.")
            return pd.DataFrame()
        raise ValueError("No signals generated for any tickers.")

    print("=" * 60)
    print(f"{pfx}Done: {len(all_signals)} successful, {len(failed)} failed, {time.time() - t0:.1f}s")

    frames = [f.dropna(axis=1, how='all') for f in all_signals if not f.empty]
    result_df = pd.concat(frames, ignore_index=True).dropna(axis=1, how='all')
    del all_signals, frames

    result_df = _save_signals(result_df, cache_file)
    print(f"{pfx}Saved: {cache_file} ({len(result_df)} rows)")
    return result_df


# Public API
def load_or_build_signals(quarter_universe, force=False):
    return _load_or_build(quarter_universe, SIGNALS_CACHE_FILE, force)


def load_or_build_etf_signals(etf_universe, force=False):
    return _load_or_build(etf_universe, ETF_SIGNALS_CACHE_FILE, force, label="ETF")


def main():
    parser = argparse.ArgumentParser(description='Build signal caches')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    _install_timed_print()
    reset_cell_timer("Signal Generation")
    quarter_universe = load_universe_from_disk()
    all_signals_df = load_or_build_signals(quarter_universe, force=args.force)
    print(f"Stock signals: {len(all_signals_df)} rows, {all_signals_df['Ticker'].nunique()} tickers")
    etf_universe = load_etf_universe_from_disk()
    etf_signals_df = load_or_build_etf_signals(etf_universe, force=args.force)
    print(f"ETF signals: {len(etf_signals_df)} rows, {etf_signals_df['Ticker'].nunique()} tickers")

if __name__ == '__main__':
    main()
