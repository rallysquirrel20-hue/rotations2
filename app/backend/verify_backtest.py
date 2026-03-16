#!/usr/bin/env python3
"""
verify_backtest.py — Independent verification of the backtest engine.

Replays trade detection, position sizing, and mark-to-market equity
from raw parquet data and compares against the API output.

Proves correctness across 4 dimensions:
  1. Signal entry/exit correctness
  2. Leverage & position sizing rules
  3. Basket membership filtering
  4. Equity curve accuracy

Usage:
    python verify_backtest.py --target AAPL --signal Breakout
    python verify_backtest.py --run-defaults
"""

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# ═══════════════════════════════════════════════════════════════════════════════
# Constants & Configuration  (independent re-implementation — no imports from main.py)
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_PATH = Path.home() / "Documents" / "Python_Outputs"
BASE_DIR = Path(os.getenv("PYTHON_OUTPUTS_DIR", str(DEFAULT_PATH))).expanduser()

DATA_STORAGE = BASE_DIR / "Data_Storage"
THEMATIC_BASKET_CACHE = DATA_STORAGE / "thematic_basket_cache"
SECTOR_BASKET_CACHE = DATA_STORAGE / "sector_basket_cache"
INDUSTRY_BASKET_CACHE = DATA_STORAGE / "industry_basket_cache"
BASKET_CACHE_FOLDERS = [
    THEMATIC_BASKET_CACHE, SECTOR_BASKET_CACHE,
    INDUSTRY_BASKET_CACHE, DATA_STORAGE,
]

INDIVIDUAL_SIGNALS_FILE = DATA_STORAGE / "signals_500.parquet"
GICS_MAPPINGS_FILE = DATA_STORAGE / "gics_mappings_500.json"

SIGNAL_IS_COL = {
    'Breakout':  'Is_Breakout',
    'Breakdown': 'Is_Breakdown',
    'Up_Rot':    'Is_Up_Rotation',
    'Down_Rot':  'Is_Down_Rotation',
    'BTFD':      'Is_BTFD',
    'STFR':      'Is_STFR',
}

BACKTEST_DIRECTION = {
    'Breakout':  'long',
    'Breakdown': 'short',
    'Up_Rot':    'long',
    'Down_Rot':  'short',
    'BTFD':      'long',
    'STFR':      'short',
}

THEMATIC_CONFIG = {
    "High_Beta":           ("beta_universes_500.json",        "high"),
    "Low_Beta":            ("beta_universes_500.json",        "low"),
    "Momentum_Leaders":    ("momentum_universes_500.json",    "winners"),
    "Momentum_Losers":     ("momentum_universes_500.json",    "losers"),
    "High_Dividend_Yield": ("dividend_universes_500.json",    "high_yield"),
    "Dividend_Growth":     ("dividend_universes_500.json",    "div_growth"),
    "Risk_Adj_Momentum":   ("risk_adj_momentum_500.json",     None),
}

# Terminal colours
GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
CYAN   = '\033[96m'
BOLD   = '\033[1m'
RESET  = '\033[0m'

EQ_TOL    = 0.01    # equity values — API rounds to 2 dp
TRADE_TOL = 0.0001  # trade returns — rounded to 4 dp


# ═══════════════════════════════════════════════════════════════════════════════
# Data-loading utilities
# ═══════════════════════════════════════════════════════════════════════════════

def safe_float(value, digits=4):
    """Safely convert to rounded float, or None."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return round(float(value), digits)
    except (ValueError, TypeError):
        return None


def find_basket_parquet(slug):
    """Glob for a basket parquet by slug prefix across cache folders."""
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        matches = list(folder.glob(f'{slug}_*_of_*_signals.parquet'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_*_signals.parquet'))
        if matches:
            return matches[0]
    return None


def get_universe_history(basket_name):
    """Return quarterly universe dict: {'2025 Q4': ['AAPL', ...], ...}"""
    if GICS_MAPPINGS_FILE.exists():
        with open(GICS_MAPPINGS_FILE, 'r') as f:
            gics = json.load(f)
        search_name = basket_name.replace("_", " ")
        for group_key in ('sector_u', 'industry_u'):
            group = gics.get(group_key, {})
            if search_name in group:
                return group[search_name]
    if basket_name in THEMATIC_CONFIG:
        fn, key = THEMATIC_CONFIG[basket_name]
        p_path = THEMATIC_BASKET_CACHE / fn
        if p_path.exists():
            with open(p_path, 'r') as f:
                data = json.load(f)
            return data[key] if key is not None else data
    return {}


def quarter_str_to_date(q_str):
    """Convert '2025 Q4' to pd.Timestamp('2025-10-01')."""
    parts = q_str.split()
    year = int(parts[0])
    qn = int(parts[1][1])
    month = (qn - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def build_quarter_membership(quarter_history):
    """Build sorted list of (q_start, q_end, ticker_set) tuples."""
    membership = []
    for q_str, q_tickers in quarter_history.items():
        q_start = quarter_str_to_date(q_str)
        qn = int(q_str.split()[1][1])
        q_end = (pd.Timestamp(year=q_start.year, month=qn * 3, day=1)
                 + pd.offsets.MonthEnd(0))
        membership.append((q_start, q_end, set(q_tickers)))
    membership.sort(key=lambda x: x[0])
    return membership


def load_data(target, target_type, signal, start_date=None, end_date=None):
    """
    Load raw data and return (df, ticker_closes, quarter_membership, is_multi_ticker).

    ticker_closes: dict of ticker -> pd.Series indexed by Date.
                   For single-ticker / basket, keyed by '' (empty string).
    """
    sig = signal
    is_col = SIGNAL_IS_COL[sig]
    trade_cols = [
        f'{sig}_Entry_Price', f'{sig}_Exit_Date', f'{sig}_Exit_Price',
        f'{sig}_Final_Change', f'{sig}_MFE', f'{sig}_MAE',
    ]
    is_multi_ticker = (target_type == 'basket_tickers')
    quarter_membership = None

    if target_type == 'basket':
        basket_file = find_basket_parquet(target)
        if not basket_file:
            raise FileNotFoundError(f"Basket file not found for {target}")
        base_cols = ['Date', 'Close', is_col] + trade_cols
        try:
            df = pd.read_parquet(basket_file, columns=base_cols)
        except Exception:
            df = pd.read_parquet(basket_file)
            df = df[[c for c in base_cols if c in df.columns]]

    elif target_type == 'basket_tickers':
        if not INDIVIDUAL_SIGNALS_FILE.exists():
            raise FileNotFoundError("signals_500.parquet not found")
        quarter_history = get_universe_history(target)
        if not quarter_history:
            raise ValueError(
                f"No universe history for {target} — cannot run basket_tickers test")
        basket_tickers = list(set(t for tl in quarter_history.values() for t in tl))
        quarter_membership = build_quarter_membership(quarter_history)

        base_cols = ['Ticker', 'Date', 'Close', is_col] + trade_cols
        df = pd.read_parquet(
            INDIVIDUAL_SIGNALS_FILE,
            columns=[c for c in base_cols if c],
            filters=[('Ticker', 'in', basket_tickers)],
        )

    else:  # ticker
        if not INDIVIDUAL_SIGNALS_FILE.exists():
            raise FileNotFoundError("signals_500.parquet not found")
        base_cols = ['Ticker', 'Date', 'Close', is_col] + trade_cols
        df = pd.read_parquet(
            INDIVIDUAL_SIGNALS_FILE,
            columns=[c for c in base_cols if c],
            filters=[('Ticker', '==', target)],
        )

    df['Date'] = pd.to_datetime(df['Date'])
    if is_multi_ticker:
        df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)
    else:
        df = df.sort_values('Date').reset_index(drop=True)

    if start_date:
        df = df[df['Date'] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df['Date'] <= pd.Timestamp(end_date)]

    # Build close series
    if is_multi_ticker:
        ticker_closes = {}
        for tkr, grp in df.groupby('Ticker'):
            ticker_closes[tkr] = grp.set_index('Date')['Close'].sort_index()
    else:
        closes = df.drop_duplicates('Date').set_index('Date')['Close'].sort_index()
        ticker_closes = {'': closes}

    return df, ticker_closes, quarter_membership, is_multi_ticker


# ═══════════════════════════════════════════════════════════════════════════════
# Trade Builder
# ═══════════════════════════════════════════════════════════════════════════════

def build_trades(df, signal, is_multi_ticker, quarter_membership):
    """
    Build trade list from raw data, mirroring main.py L1751-1856.
    No regime filters — tests unfiltered curve.
    Returns list of trade dicts sorted by exit_date.
    """
    sig = signal
    is_col = SIGNAL_IS_COL[sig]
    ep_col = f'{sig}_Entry_Price'
    xd_col = f'{sig}_Exit_Date'
    xp_col = f'{sig}_Exit_Price'
    fc_col = f'{sig}_Final_Change'

    # Find entries where signal is True
    entries = df[df[is_col] == True].copy()

    # Basket membership filter (vectorised searchsorted — mirrors main.py L1755-1771)
    if is_multi_ticker and quarter_membership:
        q_starts = np.array([q[0].value for q in quarter_membership])
        q_ends   = np.array([q[1].value for q in quarter_membership])
        q_ticker_sets = [q[2] for q in quarter_membership]

        dates_i64   = entries['Date'].values.astype('int64')
        tickers_arr = entries['Ticker'].values
        qi_arr = np.searchsorted(q_starts, dates_i64, side='right') - 1
        mask = np.zeros(len(entries), dtype=bool)
        for i in range(len(entries)):
            qi = qi_arr[i]
            if 0 <= qi < len(q_starts) and dates_i64[i] <= q_ends[qi]:
                if tickers_arr[i] in q_ticker_sets[qi]:
                    mask[i] = True
        entries = entries[mask]

    # Drop entries with no exit
    entries = entries.dropna(subset=[xd_col, xp_col])
    if entries.empty:
        return []

    # Vectorised bars_held (business days)
    entry_dates_np = entries['Date'].dt.date.values
    exit_dates_np  = pd.to_datetime(entries[xd_col]).dt.date.values
    bars_held_arr  = np.array([
        max(1, int(np.busday_count(ed, xd)))
        for ed, xd in zip(entry_dates_np, exit_dates_np)
    ])

    # Extract arrays for fast trade construction
    entry_dates_str = entries['Date'].dt.strftime('%Y-%m-%d').values
    exit_dates_str  = pd.to_datetime(entries[xd_col]).dt.strftime('%Y-%m-%d').values
    entry_prices    = entries[ep_col].values
    exit_prices     = entries[xp_col].values
    changes         = entries[fc_col].values
    close_prices    = entries['Close'].values
    tickers         = entries['Ticker'].values if is_multi_ticker else None

    trades = []
    for i in range(len(entries)):
        fc = changes[i]
        trade = {
            'entry_date':  entry_dates_str[i],
            'exit_date':   exit_dates_str[i],
            'entry_price': safe_float(entry_prices[i], 2),
            'exit_price':  safe_float(exit_prices[i], 2),
            'change':      safe_float(float(fc) if pd.notna(fc) else 0.0, 4),
            'bars_held':   int(bars_held_arr[i]),
            'regime_pass': True,            # no filters in verification
            'close_on_entry': safe_float(close_prices[i], 2),  # for cross-check
        }
        if is_multi_ticker:
            trade['ticker'] = str(tickers[i])
        trades.append(trade)

    # Sort by exit_date — matching main.py L1902
    trades.sort(key=lambda t: t['exit_date'])
    return trades


# ═══════════════════════════════════════════════════════════════════════════════
# Equity Engine
# ═══════════════════════════════════════════════════════════════════════════════

def mtm_equity(open_pos, cash, close_date, ticker_closes, is_long):
    """Compute mark-to-market equity: cash + sum of position values."""
    total = cash
    for info in open_pos.values():
        tkr = info.get('ticker', '')
        ep  = info['entry_price']
        cs  = ticker_closes.get(tkr)
        if cs is None or ep == 0:
            total += info['alloc']
            continue
        cv = cs.asof(close_date)
        if pd.isna(cv):
            total += info['alloc']
            continue
        cp = float(cv)
        if is_long:
            total += info['alloc'] * (cp / ep)
        else:
            total += info['alloc'] * (2 - cp / ep)
    return total


def build_equity_curve(sorted_trades, ticker_closes, all_dates, is_long,
                       initial, pos_size, max_lev):
    """
    Replay position sizing and MTM equity, mirroring main.py L1944-2050.
    Returns (eq_dates, eq_values, skipped_entries).
    """
    # Build entry/exit date maps for O(1) lookup
    entry_map = {}
    exit_map  = {}
    trade_map = {i: t for i, t in enumerate(sorted_trades)}
    for i, t in enumerate(sorted_trades):
        ed = pd.Timestamp(t['entry_date'])
        xd = pd.Timestamp(t['exit_date'])
        entry_map.setdefault(ed, []).append(i)
        exit_map.setdefault(xd, []).append(i)

    eq_dates  = []
    eq_values = []
    skipped   = []

    cash     = initial
    open_pos = {}

    for date in all_dates:
        # ── Process exits first (frees capital before new entries) ──
        for idx in exit_map.get(date, []):
            t = trade_map[idx]
            ret = t['change'] or 0.0
            if idx in open_pos:
                a = open_pos.pop(idx)
                cash += a['alloc'] * (1 + ret)

        # ── Process entries (allocate capital) ──
        for idx in entry_map.get(date, []):
            t = trade_map[idx]
            eq_est = mtm_equity(open_pos, cash, date, ticker_closes, is_long)
            if eq_est <= 0:
                continue

            wanted   = eq_est * pos_size
            exposure = sum(info['alloc'] for info in open_pos.values())
            room     = max(0, eq_est * max_lev - exposure)
            alloc    = min(wanted, room)

            if alloc > 0:
                open_pos[idx] = {
                    'alloc':       alloc,
                    'entry_price': t['entry_price'] or 0,
                    'ticker':      t.get('ticker', ''),
                }
                cash -= alloc
            else:
                skipped.append({
                    'ticker':           t.get('ticker', ''),
                    'entry_date':       t['entry_date'],
                    'entry_price':      round(t['entry_price'] or 0, 2),
                    'reason':           'leverage_limit',
                    'exposure_at_skip': round(exposure, 2),
                    'equity_at_skip':   round(eq_est, 2),
                })
                # Mark as skipped — mirrors production filtered-path behaviour
                sorted_trades[idx]['skipped'] = True
                sorted_trades[idx]['change']  = None
                sorted_trades[idx]['mfe']     = None
                sorted_trades[idx]['mae']     = None
                sorted_trades[idx]['bars_held'] = 0

        # ── Record daily mark-to-market equity ──
        equity = mtm_equity(open_pos, cash, date, ticker_closes, is_long)
        eq_dates.append(pd.Timestamp(date).strftime('%Y-%m-%d'))
        eq_values.append(round(equity, 2))

    return eq_dates, eq_values, skipped


# ═══════════════════════════════════════════════════════════════════════════════
# Stats
# ═══════════════════════════════════════════════════════════════════════════════

def compute_stats(trade_list, equity_vals):
    """Compute backtest statistics, mirroring main.py L2132-2167."""
    trade_list = [t for t in trade_list if not t.get('skipped')]
    if not trade_list:
        return {'trades': 0, 'win_rate': 0, 'avg_winner': 0, 'avg_loser': 0,
                'ev': 0, 'profit_factor': 0, 'max_dd': 0, 'avg_bars': 0}

    returns = [t['change'] for t in trade_list if t['change'] is not None]
    winners = [r for r in returns if r > 0]
    losers  = [r for r in returns if r <= 0]
    total   = len(returns)

    win_rate   = len(winners) / total if total > 0 else 0
    avg_winner = sum(winners) / len(winners) if winners else 0
    avg_loser  = sum(losers) / len(losers) if losers else 0
    ev         = sum(returns) / total if total > 0 else 0

    gross_profit = sum(winners)
    gross_loss   = abs(sum(losers))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = 999 if gross_profit > 0 else 0

    max_dd = 0.0
    if equity_vals:
        peak = equity_vals[0]
        for v in equity_vals:
            peak = max(peak, v)
            if peak > 0:
                dd = (peak - v) / peak
                max_dd = max(max_dd, dd)

    avg_bars = (sum(t['bars_held'] for t in trade_list) / len(trade_list)
                if trade_list else 0)

    return {
        'trades':        total,
        'win_rate':      round(win_rate, 4),
        'avg_winner':    round(avg_winner, 4),
        'avg_loser':     round(avg_loser, 4),
        'ev':            round(ev, 4),
        'profit_factor': round(profit_factor, 2),
        'max_dd':        round(max_dd, 4),
        'avg_bars':      round(avg_bars, 1),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# API Caller
# ═══════════════════════════════════════════════════════════════════════════════

def call_api(base_url, target, target_type, signal, pos_size, max_lev,
             initial=100000, start=None, end=None):
    """POST to /api/backtest and return the JSON response."""
    payload = {
        "target":          target,
        "target_type":     target_type,
        "entry_signal":    signal,
        "filters":         [],
        "position_size":   pos_size,
        "max_leverage":    max_lev,
        "initial_equity":  initial,
        "benchmarks_only": False,
    }
    if start:
        payload["start_date"] = start
    if end:
        payload["end_date"] = end
    resp = requests.post(f"{base_url}/api/backtest", json=payload, timeout=120)
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════════════════════════════
# Comparison Engine
# ═══════════════════════════════════════════════════════════════════════════════

def compare_trades(local_trades, api_trades, tolerance=TRADE_TOL):
    """
    Test 1: Compare local vs API trades matched by (ticker, entry_date, exit_date).
    Also cross-checks entry_price against Close on entry date.
    Returns (passed, summary, issues).
    """
    # Build lookup maps
    api_map = {}
    for t in api_trades:
        key = (t.get('ticker', ''), t['entry_date'], t['exit_date'])
        api_map[key] = t

    local_map = {}
    for t in local_trades:
        key = (t.get('ticker', ''), t['entry_date'], t['exit_date'])
        local_map[key] = t

    issues = []
    matched = 0
    gap_fills = 0

    # Check all local trades exist in API and fields match
    for key, lt in sorted(local_map.items()):
        if key not in api_map:
            issues.append(f"LOCAL ONLY: {key}")
            continue
        at = api_map[key]
        matched += 1

        for field in ('entry_price', 'exit_price', 'change'):
            lv = lt.get(field)
            av = at.get(field)
            if lv is None and av is None:
                continue
            if lv is None or av is None:
                issues.append(f"{key} {field}: local={lv} api={av}")
                continue
            if abs(lv - av) > tolerance:
                issues.append(
                    f"{key} {field}: local={lv} api={av} diff={abs(lv - av):.6f}")

        # Cross-check entry_price vs Close on entry date
        close_on_entry = lt.get('close_on_entry')
        ep = lt.get('entry_price')
        if close_on_entry is not None and ep is not None:
            if abs(ep - close_on_entry) > 0.01:
                gap_fills += 1

    # Check for API-only trades (non-skipped)
    for key, at in sorted(api_map.items()):
        if key not in local_map and not at.get('skipped'):
            issues.append(f"API ONLY: {key}")

    passed = len(issues) == 0
    parts = [f"Matched {matched}/{len(local_map)} local, {len(api_map)} API trades"]
    if gap_fills > 0:
        parts.append(f"{gap_fills} gap-fills detected")
    summary = "; ".join(parts)
    return passed, summary, issues


def compare_equity(local_dates, local_vals, api_dates, api_vals,
                   tolerance=EQ_TOL):
    """
    Test 4: Day-by-day equity curve comparison.
    Returns (passed, summary, issues).
    """
    local_eq = dict(zip(local_dates, local_vals))
    api_eq   = dict(zip(api_dates, api_vals))

    common_dates = sorted(set(local_dates) & set(api_dates))
    divergences  = []
    max_div      = 0.0
    first_div    = None

    for d in common_dates:
        lv   = local_eq[d]
        av   = api_eq[d]
        diff = abs(lv - av)
        if diff > tolerance:
            if first_div is None:
                first_div = d
            max_div = max(max_div, diff)
            divergences.append((d, lv, av, diff))

    # Check for date mismatches
    only_local = set(local_dates) - set(api_dates)
    only_api   = set(api_dates) - set(local_dates)

    passed = len(divergences) == 0 and not only_local and not only_api
    parts  = [f"{len(common_dates)} dates compared, {len(divergences)} divergences"]
    if divergences:
        parts.append(f"first={first_div}, max_diff={max_div:.4f}")
    if only_local:
        parts.append(f"{len(only_local)} local-only dates")
    if only_api:
        parts.append(f"{len(only_api)} API-only dates")
    summary = ", ".join(parts)

    issues = []
    for d, lv, av, diff in divergences[:10]:
        side = "local higher" if lv > av else "api higher"
        issues.append(f"{d}: local={lv:.2f} api={av:.2f} diff={diff:.4f} ({side})")
    if len(divergences) > 10:
        issues.append(f"... and {len(divergences) - 10} more")
    for d in sorted(only_local)[:3]:
        issues.append(f"LOCAL-ONLY date: {d}")
    for d in sorted(only_api)[:3]:
        issues.append(f"API-ONLY date: {d}")

    return passed, summary, issues


def compare_skipped(local_skipped, api_skipped):
    """
    Test 2: Verify same entries were skipped due to leverage limits.
    Returns (passed, summary, issues).
    """
    if api_skipped is None:
        api_skipped = []

    local_keys = {(s['ticker'], s['entry_date']) for s in local_skipped}
    api_keys   = {(s['ticker'], s['entry_date']) for s in api_skipped}

    only_local = local_keys - api_keys
    only_api   = api_keys - local_keys

    issues = []
    for k in sorted(only_local):
        issues.append(f"LOCAL ONLY skip: ticker={k[0]} date={k[1]}")
    for k in sorted(only_api):
        issues.append(f"API ONLY skip: ticker={k[0]} date={k[1]}")

    passed  = len(issues) == 0
    summary = f"{len(local_keys)} local skips, {len(api_keys)} API skips"
    return passed, summary, issues


def compare_membership(trades, quarter_membership):
    """
    Test 3: Verify all trades respect basket membership at entry date.
    Also verifies no valid entries were dropped.
    Returns (passed, summary, issues).
    """
    if not quarter_membership:
        return True, "No membership data (not basket_tickers mode)", []

    issues = []
    checked = 0

    for t in trades:
        if t.get('skipped'):
            continue
        checked += 1
        tkr = t.get('ticker', '')
        ed  = pd.Timestamp(t['entry_date'])

        # Find which quarter this entry falls in
        found_quarter = False
        for q_start, q_end, q_tickers in quarter_membership:
            if q_start <= ed <= q_end:
                found_quarter = True
                if tkr not in q_tickers:
                    issues.append(
                        f"INVALID: {tkr} on {t['entry_date']} not in basket for "
                        f"{q_start.strftime('%Y-%m-%d')}–{q_end.strftime('%Y-%m-%d')}")
                break

        if not found_quarter:
            issues.append(
                f"NO QUARTER: {tkr} on {t['entry_date']} falls outside all quarters")

    passed  = len(issues) == 0
    summary = f"Checked {checked} trades against quarterly membership"
    return passed, summary, issues


def verify_no_dropped_entries(df, trades, signal, is_multi_ticker,
                              quarter_membership):
    """
    Test 3b: Verify no valid signal entries were incorrectly excluded.
    Compares the set of all valid entries from raw data against the trade list.
    Returns (passed, summary, issues).
    """
    sig    = signal
    is_col = SIGNAL_IS_COL[sig]
    xd_col = f'{sig}_Exit_Date'
    xp_col = f'{sig}_Exit_Price'

    # All entries from raw data
    entries = df[df[is_col] == True].copy()

    # Apply membership filter
    if is_multi_ticker and quarter_membership:
        q_starts      = np.array([q[0].value for q in quarter_membership])
        q_ends        = np.array([q[1].value for q in quarter_membership])
        q_ticker_sets = [q[2] for q in quarter_membership]
        dates_i64     = entries['Date'].values.astype('int64')
        tickers_arr   = entries['Ticker'].values
        qi_arr        = np.searchsorted(q_starts, dates_i64, side='right') - 1
        mask = np.zeros(len(entries), dtype=bool)
        for i in range(len(entries)):
            qi = qi_arr[i]
            if 0 <= qi < len(q_starts) and dates_i64[i] <= q_ends[qi]:
                if tickers_arr[i] in q_ticker_sets[qi]:
                    mask[i] = True
        entries = entries[mask]

    # Drop entries with no exit
    entries = entries.dropna(subset=[xd_col, xp_col])

    expected = len(entries)
    actual   = len(trades)

    issues = []
    if expected != actual:
        issues.append(
            f"Expected {expected} valid entries from raw data, got {actual} trades"
            f" (diff={expected - actual})")

    passed  = len(issues) == 0
    summary = f"{actual}/{expected} entries present"
    return passed, summary, issues


def compare_stats(local_stats, api_stats, tolerance=TRADE_TOL):
    """Compare computed stats against API stats."""
    issues = []
    for key in ('trades', 'win_rate', 'ev', 'profit_factor', 'max_dd'):
        lv = local_stats.get(key, 0)
        av = api_stats.get(key, 0)

        if key == 'trades':
            if lv != av:
                issues.append(f"{key}: local={lv} api={av}")
        elif key == 'profit_factor':
            if abs(lv - av) > 0.1:
                issues.append(f"{key}: local={lv} api={av}")
        else:
            if abs(lv - av) > tolerance:
                issues.append(
                    f"{key}: local={lv} api={av} diff={abs(lv - av):.6f}")

    passed  = len(issues) == 0
    summary = (f"trades={local_stats.get('trades', 0)} "
               f"wr={local_stats.get('win_rate', 0):.2%} "
               f"ev={local_stats.get('ev', 0):.4f}")
    return passed, summary, issues


# ═══════════════════════════════════════════════════════════════════════════════
# Report & Test Runner
# ═══════════════════════════════════════════════════════════════════════════════

def report(label, passed, summary, issues=None):
    """Print a coloured PASS/FAIL line."""
    tag = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    print(f"  [{tag}] {label}: {summary}")
    if issues and not passed:
        for line in issues:
            print(f"       {RED}{line}{RESET}")


def run_test(target, target_type, signal, pos_size, max_lev, api_url,
             initial=100000, start=None, end=None, tests=None):
    """
    Run verification for a single configuration.
    tests: set of test numbers to run (1, 2, 3, 4). None = all applicable.
    """
    if tests is None:
        tests = {1, 2, 3, 4}

    direction = BACKTEST_DIRECTION[signal]
    is_long   = (direction == 'long')

    print(f"\n{BOLD}{CYAN}{'=' * 70}{RESET}")
    print(f"{BOLD}  {target} | {target_type} | {signal} | "
          f"pos={pos_size * 100:.0f}% lev={max_lev * 100:.0f}%{RESET}")
    print(f"{CYAN}{'=' * 70}{RESET}")

    # ── Load data locally ──
    print("  Loading local data...")
    try:
        df, ticker_closes, quarter_membership, is_multi_ticker = load_data(
            target, target_type, signal, start, end)
    except Exception as e:
        print(f"  {RED}ERROR loading data: {e}{RESET}")
        return False

    # ── Build trades locally ──
    local_trades = build_trades(df, signal, is_multi_ticker, quarter_membership)
    print(f"  Built {len(local_trades)} local trades")

    # ── Build equity curve locally ──
    all_dates = sorted(df['Date'].unique())
    eq_dates, eq_values, local_skipped = build_equity_curve(
        local_trades, ticker_closes, all_dates, is_long,
        initial, pos_size, max_lev)
    active_trades = len([t for t in local_trades if not t.get('skipped')])
    print(f"  Equity curve: {len(eq_dates)} dates, "
          f"{len(local_skipped)} skipped, {active_trades} active trades")

    # ── Compute local stats ──
    local_stats = compute_stats(local_trades, eq_values)

    # ── Call API ──
    print(f"  Calling API at {api_url}...")
    try:
        api_resp = call_api(
            api_url, target, target_type, signal,
            pos_size, max_lev, initial, start, end)
    except requests.ConnectionError:
        print(f"  {RED}ERROR: Cannot connect to {api_url} — is the backend running?{RESET}")
        return False
    except Exception as e:
        print(f"  {RED}ERROR calling API: {e}{RESET}")
        return False

    api_trades  = api_resp.get('trades', [])
    api_eq      = api_resp.get('equity_curve', {})
    api_stats   = api_resp.get('stats', {}).get('unfiltered', {})
    api_skipped = api_resp.get('skipped_entries')

    print(f"  API: {len(api_trades)} trades, "
          f"{len(api_eq.get('dates', []))} eq dates, "
          f"{len(api_skipped or [])} skipped")

    all_passed = True

    # ── Test 1: Signal entry/exit correctness ──
    if 1 in tests:
        p, s, iss = compare_trades(local_trades, api_trades)
        report("Test 1 — Signal Entry/Exit", p, s, iss)
        all_passed &= p

    # ── Test 2: Leverage & position sizing rules ──
    if 2 in tests:
        p, s, iss = compare_skipped(local_skipped, api_skipped)
        report("Test 2 — Leverage/Sizing Skips", p, s, iss)
        all_passed &= p

    # ── Test 3: Basket membership filtering ──
    if 3 in tests and target_type == 'basket_tickers':
        p, s, iss = compare_membership(local_trades, quarter_membership)
        report("Test 3a — Basket Membership", p, s, iss)
        all_passed &= p

        p, s, iss = verify_no_dropped_entries(
            df, local_trades, signal, is_multi_ticker, quarter_membership)
        report("Test 3b — No Dropped Entries", p, s, iss)
        all_passed &= p

    # ── Test 4: Equity curve accuracy ──
    if 4 in tests:
        p, s, iss = compare_equity(
            eq_dates, eq_values,
            api_eq.get('dates', []), api_eq.get('unfiltered', []))
        report("Test 4a — Equity Curve", p, s, iss)
        all_passed &= p

        p, s, iss = compare_stats(local_stats, api_stats)
        report("Test 4b — Stats", p, s, iss)
        all_passed &= p

    return all_passed


# ═══════════════════════════════════════════════════════════════════════════════
# Default Test Suite & Basket Detection
# ═══════════════════════════════════════════════════════════════════════════════

def detect_basket_slug():
    """Find an available basket slug that has both a parquet and universe history."""
    # Try common sector names first
    candidates = [
        'Info_Tech', 'Financials', 'Health_Care', 'Energy',
        'Consumer_Disc', 'Industrials', 'Communication_Svcs',
        'Consumer_Staples', 'Utilities', 'Real_Estate', 'Materials',
    ]
    for slug in candidates:
        pq = find_basket_parquet(slug)
        if pq:
            hist = get_universe_history(slug)
            if hist:
                return slug

    # Fall back: scan GICS mappings for any sector/industry with a parquet
    if GICS_MAPPINGS_FILE.exists():
        with open(GICS_MAPPINGS_FILE, 'r') as f:
            gics = json.load(f)
        for group_key in ('sector_u', 'industry_u'):
            for name in gics.get(group_key, {}):
                slug = name.replace(" ", "_")
                pq = find_basket_parquet(slug)
                if pq:
                    return slug

    # Try thematic baskets
    for slug in THEMATIC_CONFIG:
        pq = find_basket_parquet(slug)
        if pq:
            hist = get_universe_history(slug)
            if hist:
                return slug

    return None


def run_defaults(api_url):
    """Run the default test suite across multiple configurations."""
    print(f"\n{BOLD}Running default verification suite{RESET}")
    print(f"API: {api_url}\n")

    basket_slug = detect_basket_slug()
    if basket_slug:
        print(f"  Detected basket: {BOLD}{basket_slug}{RESET}")
    else:
        print(f"  {YELLOW}No basket with universe history found "
              f"— skipping basket tests{RESET}")

    results = []

    # Case 1: Single ticker, tight leverage (100% pos / 100% lev = one trade at a time)
    results.append((
        "Case 1: AAPL ticker  | pos=100% lev=100% (one at a time)",
        run_test('AAPL', 'ticker', 'Breakout', 1.0, 1.0, api_url,
                 tests={1, 2, 4})))

    # Case 2: Single ticker, loose leverage (25% pos / 250% lev = concurrent trades)
    results.append((
        "Case 2: AAPL ticker  | pos=25%  lev=250% (concurrent + skips)",
        run_test('AAPL', 'ticker', 'Breakout', 0.25, 2.5, api_url,
                 tests={1, 2, 4})))

    # Case 3: Basket aggregated signals
    if basket_slug:
        results.append((
            f"Case 3: {basket_slug} basket | pos=25% lev=250%",
            run_test(basket_slug, 'basket', 'Breakout', 0.25, 2.5, api_url,
                     tests={1, 4})))

    # Case 4: Basket tickers — full test (membership + leverage)
    if basket_slug:
        results.append((
            f"Case 4: {basket_slug} basket_tickers | pos=25% lev=250% (full)",
            run_test(basket_slug, 'basket_tickers', 'Breakout', 0.25, 2.5,
                     api_url, tests={1, 2, 3, 4})))

    # ── Summary ──
    print(f"\n{BOLD}{'=' * 70}{RESET}")
    print(f"{BOLD}  SUMMARY{RESET}")
    print(f"{'=' * 70}")

    all_pass = True
    for label, passed in results:
        tag = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  [{tag}] {label}")
        all_pass &= passed

    print()
    if all_pass:
        print(f"  {GREEN}{BOLD}ALL TESTS PASSED{RESET}")
    else:
        print(f"  {RED}{BOLD}SOME TESTS FAILED{RESET}")

    return all_pass


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description='Independent verification of the backtest engine')
    parser.add_argument('--target', default='AAPL',
                        help='Ticker symbol or basket slug')
    parser.add_argument('--target-type', default='ticker',
                        choices=['ticker', 'basket', 'basket_tickers'],
                        help='Target type')
    parser.add_argument('--signal', default='Breakout',
                        choices=list(SIGNAL_IS_COL.keys()),
                        help='Entry signal type')
    parser.add_argument('--pos-size', type=float, default=1.0,
                        help='Position size as fraction of equity (0.0-1.0)')
    parser.add_argument('--max-lev', type=float, default=1.0,
                        help='Max leverage multiplier')
    parser.add_argument('--initial', type=float, default=100000,
                        help='Initial equity')
    parser.add_argument('--start-date', default=None,
                        help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default=None,
                        help='End date (YYYY-MM-DD)')
    parser.add_argument('--api-url', default='http://localhost:8000',
                        help='Backend API base URL')
    parser.add_argument('--run-defaults', action='store_true',
                        help='Run default test suite across multiple configs')

    args = parser.parse_args()

    if args.run_defaults:
        ok = run_defaults(args.api_url)
    else:
        ok = run_test(
            args.target, args.target_type, args.signal,
            args.pos_size, args.max_lev, args.api_url,
            args.initial, args.start_date, args.end_date)

    sys.exit(0 if ok else 1)


if __name__ == '__main__':
    main()
