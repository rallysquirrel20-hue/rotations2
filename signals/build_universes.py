#!/usr/bin/env python
"""Build all quarterly universes and save to JSON caches.

Runs daily after market close. Uses the NYSE trading calendar to detect
quarter boundaries — when Norgate data covers the last trading day of a
quarter, the next quarter's universe is built immediately.

Usage:
    python build_universes.py            # normal run
    python build_universes.py --force    # rebuild everything
"""
import sys
import re
import json
import argparse

import norgatedata
import pandas as pd
import numpy as np
import exchange_calendars as xcals
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from config import (
    SIZE, ETF_SIZE, THEME_SIZE, DIV_THEME_SIZE, START_YEAR,
    LOOKBACK_DAYS, MOMENTUM_LOOKBACK_DAYS,
    MARKET_SYMBOL, SECTOR_LIST,
    CACHE_FILE, ETF_CACHE_FILE, TICKER_NAMES_FILE,
    BETA_CACHE_FILE, MOMENTUM_CACHE_FILE, RISK_ADJ_MOM_CACHE_FILE,
    DIVIDEND_CACHE_FILE, SIZE_CACHE_FILE, VOLUME_GROWTH_CACHE_FILE,
    GICS_CACHE_FILE,
    _universe_to_json, _json_to_universe,
    _gics_to_json, _json_to_gics,
    atomic_write_json,
    _quarter_end_from_key, _quarter_start_from_key,
    get_current_quarter_key,
    _install_timed_print, reset_cell_timer,
)

# ---------------------------------------------------------------------------
# NYSE calendar + Norgate date detection
# ---------------------------------------------------------------------------
_NYSE = xcals.get_calendar('XNYS')
_DELISTED_RE = re.compile(r'-(\d{6})$')


def _last_trading_day_of_quarter(quarter_key):
    q_end = _quarter_end_from_key(quarter_key)
    q_start = _quarter_start_from_key(quarter_key)
    sessions = _NYSE.sessions_in_range(q_start.strftime('%Y-%m-%d'), q_end.strftime('%Y-%m-%d'))
    return sessions[-1] if len(sessions) else q_end


def _latest_norgate_date():
    try:
        df = norgatedata.price_timeseries(
            MARKET_SYMBOL,
            stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
            padding_setting=norgatedata.PaddingType.NONE,
            timeseriesformat='pandas-dataframe',
        )
        if df is not None and not df.empty:
            return pd.Timestamp(df.index[-1]).normalize()
    except Exception:
        pass
    return None


def _needs_rebuild(cached, force=False):
    if force:
        return True
    current = get_current_quarter_key()
    if current not in cached:
        return True
    # If current quarter's last trading day is in Norgate, also need next quarter
    norgate_date = _latest_norgate_date()
    if norgate_date is not None:
        last_td = _last_trading_day_of_quarter(current)
        if norgate_date >= last_td:
            nk = _next_qtr(current)
            if nk not in cached:
                return True
    return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prev_qtr(key):
    y, q = int(key.split()[0]), int(key.split()[1].replace('Q', ''))
    return f"{y - 1} Q4" if q == 1 else f"{y} Q{q - 1}"


def _next_qtr(key):
    y, q = int(key.split()[0]), int(key.split()[1].replace('Q', ''))
    return f"{y + 1} Q1" if q == 4 else f"{y} Q{q + 1}"


def _offset_key(date):
    """Q1 resample date -> '2026 Q2' key."""
    nq = date.quarter % 4 + 1
    ny = date.year + (1 if date.quarter == 4 else 0)
    return f"{ny} Q{nq}"


def _filter_delisted(tickers, quarter_key):
    """Remove tickers delisted before the quarter starts."""
    q_start = _quarter_start_from_key(quarter_key)
    kept = set()
    for t in tickers:
        m = _DELISTED_RE.search(t)
        if m:
            ym = m.group(1)
            if datetime(int(ym[:4]), int(ym[4:]), 1) < q_start.to_pydatetime():
                continue
        kept.add(t)
    return kept


def _fetch_prices(ticker, adjustment=None):
    """Fetch Norgate price DataFrame (date-indexed) or None."""
    adj = adjustment or norgatedata.StockPriceAdjustmentType.TOTALRETURN
    df = norgatedata.price_timeseries(
        ticker, stock_price_adjustment_setting=adj,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe',
    )
    if df is None or df.empty:
        return None
    df = df.reset_index()
    dc = df.columns[0]
    df[dc] = pd.to_datetime(df[dc]).dt.normalize()
    return df.set_index(dc)


def _parallel_fetch(symbols, worker, max_workers=4, prefix=""):
    """Run worker(ticker) in parallel, collect results, print progress."""
    results = []
    total = len(symbols)
    last_ms = [0]
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(worker, t) for t in symbols]
        for i, fut in enumerate(futures, 1):
            results.append(fut.result())
            pct = int((i / total) * 100) if total else 100
            ms = pct // 10 * 10
            if ms > last_ms[0] and ms % 10 == 0:
                print(f"  {prefix}{ms}% ({i}/{total})")
                last_ms[0] = ms
    return results


def _safe_quarterly(calc_fn, extra_filter=None):
    """Return a safe wrapper that calls calc_fn(ticker) and collects quarterly rows."""
    def wrapper(ticker):
        try:
            result = calc_fn(ticker)
            if result is None:
                return []
            rows = [(d, ticker, v) for d, v in result.items()
                    if d.year >= START_YEAR and pd.notna(v)]
            if extra_filter:
                rows = [(d, t, v) for d, t, v in rows if extra_filter(v)]
            return rows
        except Exception:
            return []
    return wrapper


def _rank_per_quarter(df, quarter_universe, col, n, ascending=False):
    """Rank tickers within each quarter by a metric column."""
    result = {}
    for key in sorted(quarter_universe.keys()):
        rd = _quarter_end_from_key(_prev_qtr(key))
        tickers = quarter_universe[key]
        grp = df[(df['Date'] == rd) & (df['Ticker'].isin(tickers))]
        if grp.empty:
            continue
        ranked = grp.sort_values(col, ascending=ascending)
        result[key] = set(ranked.head(n)['Ticker'])
    return result


def _collect_metric(symbols, calc_fn, col_name, extra_filter=None, prefix=""):
    """Parallel-fetch a quarterly metric for all symbols, return DataFrame."""
    worker = _safe_quarterly(calc_fn, extra_filter)
    all_rows = []
    for rows in _parallel_fetch(symbols, worker, prefix=prefix):
        all_rows.extend(rows)
    if not all_rows:
        return pd.DataFrame(columns=['Date', 'Ticker', col_name])
    return pd.DataFrame(all_rows, columns=['Date', 'Ticker', col_name])


def _cached_build(cache_file, builder_fn, serializer, deserializer,
                  name, force=False, **builder_kwargs):
    """Generic cache-first builder. Returns deserialized result."""
    if not force and cache_file.exists():
        try:
            cached = deserializer(cache_file.read_text(encoding='utf-8'))
            if not _needs_rebuild_simple(cached):
                print(f"{name} loaded from cache")
                return cached
            print(f"{name} outdated, rebuilding...")
        except Exception:
            print(f"{name} cache invalid, rebuilding...")

    result = builder_fn(**builder_kwargs)
    atomic_write_json(cache_file, serializer(result))
    print(f"Saved: {cache_file}")
    return result


def _needs_rebuild_simple(cached):
    """Simple check: does the current quarter key exist?"""
    if isinstance(cached, tuple):
        return get_current_quarter_key() not in (cached[0] if isinstance(cached[0], dict) else {})
    return get_current_quarter_key() not in cached


def _all_symbols(quarter_universe):
    return sorted({t for tickers in quarter_universe.values() for t in tickers})


# ===================================================================
#  1. Core Universe
# ===================================================================

def _get_quarterly_volume(ticker):
    try:
        df = _fetch_prices(ticker)
        if df is None:
            return []
        q = (df['Close'] * df['Volume']).resample('QE-DEC').mean()
        return [(d, ticker, v) for d, v in q.items() if d.year >= START_YEAR and v > 0]
    except Exception:
        return []


def _build_core_universe():
    us_eq, us_del = [], []
    try:
        us_eq = [s for s in norgatedata.database_symbols('US Equities')
                 if norgatedata.subtype1(s) == 'Equity']
    except Exception:
        pass
    try:
        us_del = [s for s in norgatedata.database_symbols('US Equities Delisted')
                  if norgatedata.subtype1(s) == 'Equity']
    except Exception:
        pass

    symbols = list(set(us_eq + us_del))
    print(f"Equities: {len(us_eq)} active, {len(us_del)} delisted, {len(symbols)} total")

    all_rows = []
    for rows in _parallel_fetch(symbols, _get_quarterly_volume):
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows, columns=['Date', 'Ticker', 'Volume'])
    universe = {}
    for date, grp in df.groupby('Date'):
        key = _offset_key(date)
        active = _filter_delisted(set(grp['Ticker']), key)
        universe[key] = set(grp[grp['Ticker'].isin(active)].nlargest(SIZE, 'Volume')['Ticker'])
    return universe


def _build_or_load_core(force=False):
    if not force and CACHE_FILE.exists():
        try:
            cached = _json_to_universe(CACHE_FILE.read_text(encoding='utf-8'))
            if not _needs_rebuild(cached):
                print(f"Core universe loaded from cache ({len(cached)} quarters)")
                return cached
        except Exception:
            pass
        print("Core universe rebuilding...")

    universe = _build_core_universe()
    atomic_write_json(CACHE_FILE, _universe_to_json(universe))
    cur = get_current_quarter_key()
    print(f"Saved core universe: {len(universe.get(cur, set()))} tickers for {cur}")
    return universe


# ===================================================================
#  2. ETF Universe
# ===================================================================

def _build_etf_universe():
    etf_syms = []
    for db in ('US Equities', 'US Equities Delisted'):
        try:
            etf_syms.extend(s for s in norgatedata.database_symbols(db)
                            if norgatedata.subtype1(s) == 'Exchange Traded Product')
        except Exception:
            pass
    etf_syms = list(set(etf_syms))
    print(f"[ETF] {len(etf_syms)} unique ETFs")

    all_rows = []
    for rows in _parallel_fetch(etf_syms, _get_quarterly_volume, prefix="[ETF] "):
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows, columns=['Date', 'Ticker', 'Volume'])
    universe = {}
    for date, grp in df.groupby('Date'):
        key = _offset_key(date)
        active = _filter_delisted(set(grp['Ticker']), key)
        universe[key] = set(grp[grp['Ticker'].isin(active)].nlargest(ETF_SIZE, 'Volume')['Ticker'])
    return universe


def _build_or_load_etf(force=False):
    return _cached_build(
        ETF_CACHE_FILE, _build_etf_universe, _universe_to_json, _json_to_universe,
        "[ETF] Universe", force)


# ===================================================================
#  3. Ticker Names
# ===================================================================

def _build_ticker_names(quarter_universe, etf_universe):
    all_tickers = sorted(
        {t for v in quarter_universe.values() for t in v}
        | {t for v in etf_universe.values() for t in v}
    )
    existing = {}
    if TICKER_NAMES_FILE.exists():
        try:
            existing = json.loads(TICKER_NAMES_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass
    missing = [t for t in all_tickers if t not in existing]
    if not missing:
        print(f"Ticker names: {len(existing)} cached, 0 missing")
        return existing
    print(f"Fetching {len(missing)} ticker names...")
    names = dict(existing)
    for t in missing:
        try:
            n = norgatedata.security_name(t)
            if n:
                names[t] = n
        except Exception:
            pass
    atomic_write_json(TICKER_NAMES_FILE, json.dumps(names, sort_keys=True))
    print(f"Saved {len(names)} ticker names")
    return names


# ===================================================================
#  4-9. Thematic Universes (beta, momentum, risk-adj, dividends, size, volume growth)
# ===================================================================

def _build_beta(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Beta: {len(symbols)} stocks")
    mkt = _fetch_prices(MARKET_SYMBOL)
    if mkt is None:
        raise ValueError(f"Cannot load {MARKET_SYMBOL}")
    mkt_rets = mkt['Close'].pct_change()
    mkt_var = mkt_rets.rolling(LOOKBACK_DAYS).var()

    def calc(ticker):
        df = _fetch_prices(ticker)
        if df is None or len(df) < LOOKBACK_DAYS:
            return None
        sr = df['Close'].pct_change()
        al = pd.concat([sr, mkt_rets], axis=1, join='inner')
        al.columns = ['S', 'M']
        return (al['S'].rolling(LOOKBACK_DAYS).cov(al['M']) / mkt_var).resample('QE-DEC').last()

    beta_cache = {}
    for ticker, series in _parallel_fetch(symbols, lambda t: (t, calc(t))):
        if series is not None:
            beta_cache[ticker] = series

    high, low = {}, {}
    for key in sorted(quarter_universe.keys()):
        rd = _quarter_end_from_key(_prev_qtr(key))
        vals = [(t, beta_cache[t].at[rd]) for t in quarter_universe[key]
                if t in beta_cache and rd in beta_cache[t].index
                and pd.notna(beta_cache[t].at[rd])]
        vals.sort(key=lambda x: x[1])
        low[key] = set(t for t, _ in vals[:THEME_SIZE])
        high[key] = set(t for t, _ in vals[-THEME_SIZE:])
    return high, low


def _build_momentum(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Momentum: {len(symbols)} stocks")

    def calc(ticker):
        df = _fetch_prices(ticker)
        if df is None or len(df) < MOMENTUM_LOOKBACK_DAYS:
            return None
        return df['Close'].pct_change(periods=MOMENTUM_LOOKBACK_DAYS).resample('QE-DEC').last()

    df = _collect_metric(symbols, calc, 'Momentum')
    winners = _rank_per_quarter(df, quarter_universe, 'Momentum', THEME_SIZE, ascending=False)
    losers = _rank_per_quarter(df, quarter_universe, 'Momentum', THEME_SIZE, ascending=True)
    return winners, losers


def _build_risk_adj_momentum(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Risk-adj momentum: {len(symbols)} stocks")

    def calc(ticker):
        df = _fetch_prices(ticker)
        if df is None or len(df) < MOMENTUM_LOOKBACK_DAYS:
            return None
        ret = df['Close'].pct_change(periods=MOMENTUM_LOOKBACK_DAYS)
        vol = df['Close'].pct_change().rolling(MOMENTUM_LOOKBACK_DAYS).std()
        return (ret / vol).resample('QE-DEC').last()

    df = _collect_metric(symbols, calc, 'RiskAdjMom',
                         extra_filter=lambda v: np.isfinite(v))
    return _rank_per_quarter(df, quarter_universe, 'RiskAdjMom', THEME_SIZE)


def _build_dividends(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Dividends: {len(symbols)} stocks")

    def calc_yield(ticker):
        try:
            dy = norgatedata.dividend_yield_timeseries(ticker, timeseriesformat='pandas-dataframe')
        except Exception:
            try:
                dy = norgatedata.dividend_yield_timeseries(ticker)
            except Exception:
                return None
        if dy is None or (hasattr(dy, 'empty') and dy.empty):
            return None
        dy_df = pd.DataFrame(dy).reset_index()
        dc = dy_df.columns[0]
        dy_df[dc] = pd.to_datetime(dy_df[dc]).dt.normalize()
        dy_df = dy_df.set_index(dc).sort_index()
        nums = [c for c in dy_df.columns if pd.api.types.is_numeric_dtype(dy_df[c])]
        if not nums:
            return None
        s = dy_df[nums[0]].dropna()
        return s.resample('QE-DEC').last() if not s.empty else None

    def calc_trailing_divs(ticker):
        df = _fetch_prices(ticker, norgatedata.StockPriceAdjustmentType.CAPITALSPECIAL)
        if df is None or 'Dividend' not in df.columns:
            return None
        t12 = df['Dividend'].fillna(0.0).rolling(252, min_periods=1).sum()
        q = t12.resample('QE-DEC').last()
        q = q[q > 0]
        return q if not q.empty else None

    print("  Pass 1/2: dividend yield...")
    df_yield = _collect_metric(symbols, calc_yield, 'Yield',
                               extra_filter=lambda v: float(v) > 0, prefix="  ")
    print("  Pass 2/2: trailing dividends...")
    df_divs = _collect_metric(symbols, calc_trailing_divs, 'TrailingDivs',
                              extra_filter=lambda v: float(v) > 0, prefix="  ")

    high_yield, div_growth, div_with_growth = {}, {}, {}
    for key in sorted(quarter_universe.keys()):
        rd = _quarter_end_from_key(_prev_qtr(key))
        tickers = quarter_universe[key]

        gy = df_yield[(df_yield['Date'] == rd) & (df_yield['Ticker'].isin(tickers))]
        if not gy.empty:
            high_yield[key] = set(gy.sort_values('Yield', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

        if df_divs.empty:
            continue
        cur = df_divs[(df_divs['Date'] == rd) & (df_divs['Ticker'].isin(tickers))]
        if cur.empty:
            continue
        pyd = rd - pd.DateOffset(years=1)
        prior = df_divs[
            df_divs['Date'].between(pyd - pd.Timedelta(days=45), pyd + pd.Timedelta(days=45))
            & df_divs['Ticker'].isin(tickers)
        ].copy()
        if prior.empty:
            continue
        prior['_d'] = (prior['Date'] - pyd).abs()
        prior = prior.sort_values('_d').drop_duplicates('Ticker')[['Ticker', 'TrailingDivs']]
        prior = prior.rename(columns={'TrailingDivs': 'Prior'})
        mg = cur.merge(prior, on='Ticker', how='inner')
        mg = mg[(mg['TrailingDivs'] > 0) & (mg['Prior'] > 0)].copy()
        if mg.empty:
            continue
        mg['Growth'] = mg['TrailingDivs'] / mg['Prior'] - 1.0
        div_growth[key] = set(mg.sort_values('Growth', ascending=False).head(DIV_THEME_SIZE)['Ticker'])
        growers = set(mg.loc[mg['Growth'] > 0, 'Ticker'])
        if growers and not gy.empty:
            gf = gy[gy['Ticker'].isin(growers)]
            if not gf.empty:
                div_with_growth[key] = set(gf.sort_values('Yield', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

    return high_yield, div_growth, div_with_growth


def _build_size(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Size: {len(symbols)} stocks")

    def calc(ticker):
        df = _fetch_prices(ticker)
        if df is None:
            return None
        return (df['Close'] * df['Volume']).resample('QE-DEC').mean()

    df = _collect_metric(symbols, calc, 'DollarVolume', extra_filter=lambda v: v > 0)
    return _rank_per_quarter(df, quarter_universe, 'DollarVolume', THEME_SIZE)


def _build_volume_growth(quarter_universe):
    symbols = _all_symbols(quarter_universe)
    print(f"Volume growth: {len(symbols)} stocks")

    def calc(ticker):
        df = _fetch_prices(ticker)
        if df is None:
            return None
        return (df['Close'] * df['Volume']).resample('QE-DEC').mean()

    df = _collect_metric(symbols, calc, 'DollarVolume', extra_filter=lambda v: v > 0)
    universe = {}
    for key in sorted(quarter_universe.keys()):
        pk = _prev_qtr(key)
        ppk = _prev_qtr(pk)
        rd = _quarter_end_from_key(pk)
        prd = _quarter_end_from_key(ppk)
        tickers = quarter_universe[key]
        cur = df[(df['Date'] == rd) & (df['Ticker'].isin(tickers))].set_index('Ticker')
        prev = df[(df['Date'] == prd) & (df['Ticker'].isin(tickers))].set_index('Ticker')
        if cur.empty or prev.empty:
            continue
        mg = cur[['DollarVolume']].join(prev[['DollarVolume']], lsuffix='_c', rsuffix='_p', how='inner')
        mg = mg[mg['DollarVolume_p'] > 0]
        mg['G'] = (mg['DollarVolume_c'] - mg['DollarVolume_p']) / mg['DollarVolume_p']
        universe[key] = set(mg.sort_values('G', ascending=False).head(THEME_SIZE).index)
    return universe


# ===================================================================
#  10. GICS (Sector & Industry)
# ===================================================================

def _build_gics(quarter_universe):
    all_tickers = _all_symbols(quarter_universe)
    print(f"GICS: mapping {len(all_tickers)} stocks")

    def fetch_gics(t):
        sec, sub = None, None
        try:
            sec = norgatedata.classification_at_level(t, "GICS", "Name", 1)
        except Exception:
            pass
        try:
            sub = norgatedata.classification_at_level(t, "GICS", "Name", 4)
        except Exception:
            pass
        return t, sec, sub

    ticker_sector, ticker_subindustry = {}, {}
    for t, sec, sub in _parallel_fetch(all_tickers, fetch_gics):
        if sec:
            ticker_sector[t] = sec
        if sub:
            ticker_subindustry[t] = sub

    # Sector universes
    sector_u = {s: {} for s in SECTOR_LIST}
    for key, tickers in quarter_universe.items():
        for t in tickers:
            s = ticker_sector.get(t)
            if s in sector_u:
                sector_u[s].setdefault(key, set()).add(t)

    # Industry universes (only those with enough stocks in latest quarter)
    latest_key = max(quarter_universe.keys())
    from collections import Counter
    counts = Counter(ticker_subindustry.get(t) for t in quarter_universe[latest_key])
    selected = sorted(k for k, c in counts.items() if k)
    print(f"  {len(selected)} industries")

    industry_u = {ind: {} for ind in selected}
    for key, tickers in quarter_universe.items():
        for t in tickers:
            g = ticker_subindustry.get(t)
            if g in industry_u:
                industry_u[g].setdefault(key, set()).add(t)

    return ticker_sector, ticker_subindustry, sector_u, industry_u


# ===================================================================
#  Thematic cache wrappers (one-line each using _cached_build)
# ===================================================================

def _ser_pair(pair):
    a, b = pair
    return json.dumps({'a': {k: sorted(v) for k, v in a.items()},
                       'b': {k: sorted(v) for k, v in b.items()}})

def _deser_pair(text):
    d = json.loads(text)
    return ({k: set(v) for k, v in d['a'].items()},
            {k: set(v) for k, v in d['b'].items()})

def _ser_triple(triple):
    a, b, c = triple
    return json.dumps({'a': {k: sorted(v) for k, v in a.items()},
                       'b': {k: sorted(v) for k, v in b.items()},
                       'c': {k: sorted(v) for k, v in c.items()}})

def _deser_triple(text):
    d = json.loads(text)
    return ({k: set(v) for k, v in d['a'].items()},
            {k: set(v) for k, v in d['b'].items()},
            {k: set(v) for k, v in d.get('c', {}).items()})


# ===================================================================
#  Main
# ===================================================================

def main():
    parser = argparse.ArgumentParser(description='Build quarterly universes')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    force = args.force

    _install_timed_print()
    reset_cell_timer("Universe Construction")

    # 1. Core + ETF universes
    qu = _build_or_load_core(force)
    etf = _cached_build(ETF_CACHE_FILE, _build_etf_universe,
                        _universe_to_json, _json_to_universe, "[ETF] Universe", force)

    # 2. Ticker names
    _build_ticker_names(qu, etf)

    # 3. Thematic universes
    _cached_build(BETA_CACHE_FILE, lambda: _build_beta(qu),
                  _ser_pair, _deser_pair, "Beta", force)
    _cached_build(MOMENTUM_CACHE_FILE, lambda: _build_momentum(qu),
                  _ser_pair, _deser_pair, "Momentum", force)
    _cached_build(RISK_ADJ_MOM_CACHE_FILE, lambda: _build_risk_adj_momentum(qu),
                  _universe_to_json, _json_to_universe, "Risk-adj momentum", force)
    _cached_build(DIVIDEND_CACHE_FILE, lambda: _build_dividends(qu),
                  _ser_triple, _deser_triple, "Dividends", force)
    _cached_build(SIZE_CACHE_FILE, lambda: _build_size(qu),
                  _universe_to_json, _json_to_universe, "Size", force)
    _cached_build(VOLUME_GROWTH_CACHE_FILE, lambda: _build_volume_growth(qu),
                  _universe_to_json, _json_to_universe, "Volume growth", force)

    # 4. GICS
    _cached_build(GICS_CACHE_FILE, lambda: _build_gics(qu),
                  _gics_to_json, _json_to_gics, "GICS", force)

    print("All universes built successfully.")


if __name__ == '__main__':
    main()
