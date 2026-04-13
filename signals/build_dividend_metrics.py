#!/usr/bin/env python
"""Build per-ticker dividend metrics (yield, TTM dividends, YoY growth).

For each ticker in the universe, fetches Norgate ``price_timeseries`` with
``CAPITALSPECIAL`` adjustment (which exposes the raw per-share ``Dividend``
column alongside split-adjusted OHLCV). Computes:

  - ``TTM_Dividends``    -- rolling 365-CALENDAR-day sum of per-share dividends
                            (time-based pandas rolling on the DatetimeIndex,
                            not positional). Calendar-aligned because ex-div
                            dates are calendar-spaced; positional trading-day
                            windows produce visible spikes around the rolling
                            boundary every quarter.
  - ``Dividend_Yield``   -- ``TTM_Dividends / Close`` (decimal, e.g. 0.025)
  - ``Div_Growth_1Y``    -- YoY % change in TTM via ``asof(t - 365 days)``;
                            zero when current or prior TTM is zero (non-payer
                            convention: "no dividend = 0% growth")

Emits one long-form parquet per universe:
  - ``dividend_metrics_{SIZE}.parquet``      (equities)
  - ``dividend_metrics_etf_50.parquet``      (ETFs)

Schema: ``Date | Ticker | Dividend_Yield | TTM_Dividends | Div_Growth_1Y | Source``.
"""
import os
import argparse
import time
import norgatedata
import numpy as np
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from config import (
    SIZE, ETF_SIZE, MARKET_SYMBOL, INCREMENTAL_MAX_DAYS,
    DIVIDEND_TTM_WINDOW, DIVIDEND_YOY_LAG,
    DIVIDEND_METRICS_CACHE_FILE, DIVIDEND_METRICS_ETF_CACHE_FILE,
    load_universe_from_disk, load_etf_universe_from_disk,
    WriteThroughPath,
    _install_timed_print, reset_cell_timer,
)


# ---------------------------------------------------------------------------
# Per-ticker computation
# ---------------------------------------------------------------------------
def _fetch_ticker_dividend_metrics(ticker):
    """Fetch one ticker's dividend history + price, compute yield/TTM/growth.

    Returns a long-form DataFrame (Date, Ticker, Dividend_Yield,
    TTM_Dividends, Div_Growth_1Y) or ``None`` on failure / missing data.

    Non-payer convention: when current TTM or prior TTM is zero, growth is 0
    (never NaN / Inf). Yield is 0 whenever TTM is 0 or Close is missing.
    """
    try:
        prc = norgatedata.price_timeseries(
            ticker,
            stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.CAPITALSPECIAL,
            padding_setting=norgatedata.PaddingType.NONE,
            timeseriesformat='pandas-dataframe',
        )
    except Exception:
        return None

    if prc is None or prc.empty or 'Dividend' not in prc.columns or 'Close' not in prc.columns:
        return None

    p = prc.reset_index()
    date_col = p.columns[0]
    p[date_col] = pd.to_datetime(p[date_col]).dt.normalize()
    p = p.set_index(date_col).sort_index()

    close = p['Close'].astype('float64').values
    divs = p['Dividend'].fillna(0.0).astype('float64')

    # Time-based rolling: window is (t - 365 calendar days, t]. Aligns the
    # boundary with calendar-spaced ex-div dates, dramatically reducing the
    # quarterly spike artifact seen with positional 252-trading-day rolling.
    ttm = divs.rolling(f'{DIVIDEND_TTM_WINDOW}D').sum()

    # YoY: TTM exactly N calendar days ago (using asof so non-trading days
    # resolve to the last preceding trading-day value).
    target_idx = ttm.index - pd.Timedelta(days=DIVIDEND_YOY_LAG)
    ttm_prior = ttm.asof(target_idx)

    ttm_v = ttm.values
    prior_v = np.asarray(ttm_prior, dtype='float64')

    with np.errstate(divide='ignore', invalid='ignore'):
        yld = np.where(
            (close > 0) & np.isfinite(close),
            ttm_v / close,
            0.0,
        )
        growth = np.where(
            (ttm_v > 0) & (prior_v > 0),
            ttm_v / prior_v - 1.0,
            0.0,
        )

    out = pd.DataFrame({
        'Date': p.index,
        'Ticker': ticker,
        'Dividend_Yield': yld.astype('float32'),
        'TTM_Dividends': ttm_v.astype('float32'),
        'Div_Growth_1Y': growth.astype('float32'),
    })
    return out


# ---------------------------------------------------------------------------
# Cache freshness
# ---------------------------------------------------------------------------
def _get_latest_norgate_date():
    """Latest date available in Norgate (probed via MARKET_SYMBOL)."""
    df = norgatedata.price_timeseries(
        MARKET_SYMBOL,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe',
    )
    if df is None or df.empty:
        return None
    df = df.reset_index()
    date_col = df.columns[0]
    return pd.to_datetime(df[date_col]).max().normalize()


def _cache_is_current(df):
    if df is None or df.empty or 'Date' not in df.columns:
        return False
    if not pd.api.types.is_datetime64_any_dtype(df['Date']):
        df['Date'] = pd.to_datetime(df['Date'])
    # Live overlay rows invalidate -- force rebuild/replace from norgate
    if 'Source' in df.columns:
        latest = df['Date'].max()
        if pd.notna(latest):
            latest_rows = df[df['Date'] == latest]
            if (latest_rows['Source'] == 'live').any():
                return False
    latest_cache = df['Date'].max()
    if pd.isna(latest_cache):
        return False
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None:
        return False
    return latest_cache.normalize() >= latest_norgate


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------
def _save(df, cache_file):
    """Atomic write with 'Source' column set to 'norgate'."""
    df = df.copy()
    df['Source'] = 'norgate'
    for col in ('Dividend_Yield', 'TTM_Dividends', 'Div_Growth_1Y'):
        if col in df.columns:
            df[col] = df[col].astype('float32')
    tmp = cache_file.with_suffix('.parquet.tmp')
    df.to_parquet(tmp, index=False, compression='snappy')
    tmp.replace(cache_file)
    WriteThroughPath(cache_file).sync()
    return df


def _progress(i, total, last_ms, prefix=""):
    pct = int((i / total) * 100) if total else 100
    ms = pct // 10 * 10
    if ms > last_ms[0] and ms % 10 == 0:
        print(f"  {prefix}{ms}% ({i}/{total})")
        last_ms[0] = ms


# ---------------------------------------------------------------------------
# Load or build
# ---------------------------------------------------------------------------
def _load_or_build(universe, cache_file, force=False, label=""):
    """Load dividend metrics from parquet if current, else full rebuild.

    Note: no incremental path. The 252-day TTM window and 252-day YoY lag
    mean partial updates would have to recompute at least the trailing year
    of growth anyway, so a parallelized full rebuild is simpler and the
    dominant cost is Norgate I/O which we parallelize.
    """
    t0 = time.time()
    pfx = f"[{label}] " if label else ""

    if not force and cache_file.exists():
        try:
            cached_df = pd.read_parquet(cache_file)
            if 'Date' in cached_df.columns:
                cached_df['Date'] = pd.to_datetime(cached_df['Date'])
            if _cache_is_current(cached_df):
                print(f"{pfx}Dividend metrics loaded from cache "
                      f"({len(cached_df)} rows, {cached_df['Ticker'].nunique()} tickers)")
                if 'Source' in cached_df.columns:
                    cached_df = cached_df[cached_df['Source'] != 'live']
                return cached_df
        except Exception as e:
            print(f"{pfx}Warning: failed to load cache: {e}")

    print(f"{pfx}{'Force rebuild...' if force else 'Building dividend metrics...'}")

    all_tickers = sorted({t for tickers in universe.values() for t in tickers})
    print(f"{pfx}Total unique tickers: {len(all_tickers)}")
    print("=" * 60)

    all_frames = []
    failed = []

    def _safe(t):
        try:
            return t, _fetch_ticker_dividend_metrics(t)
        except Exception:
            return t, None

    last_ms = [0]
    total = len(all_tickers)
    max_w = min(8, os.cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=max_w) as ex:
        futures = [ex.submit(_safe, t) for t in all_tickers]
        for i, fut in enumerate(futures, 1):
            t, result = fut.result()
            _progress(i, total, last_ms, pfx)
            if result is not None and not result.empty:
                all_frames.append(result)
            else:
                failed.append(t)

    if not all_frames:
        print(f"{pfx}Warning: no dividend metrics generated.")
        return pd.DataFrame()

    print("=" * 60)
    print(f"{pfx}Done: {len(all_frames)} successful, {len(failed)} no-data, "
          f"{time.time() - t0:.1f}s")

    result_df = pd.concat(all_frames, ignore_index=True)
    result_df = _save(result_df, cache_file)
    print(f"{pfx}Saved: {cache_file} ({len(result_df)} rows)")
    return result_df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def load_or_build_dividend_metrics(quarter_universe, force=False):
    return _load_or_build(quarter_universe, DIVIDEND_METRICS_CACHE_FILE, force)


def load_or_build_dividend_metrics_etf(etf_universe, force=False):
    return _load_or_build(etf_universe, DIVIDEND_METRICS_ETF_CACHE_FILE, force, label="ETF")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='Build per-ticker dividend metrics caches')
    parser.add_argument('--force', action='store_true',
                        help='Force full rebuild even if cache is current')
    args = parser.parse_args()
    _install_timed_print()
    reset_cell_timer("Dividend Metrics")

    quarter_universe = load_universe_from_disk()
    if quarter_universe is None:
        print("ERROR: equity universe cache missing -- run build_universes.py first")
        return
    df_eq = load_or_build_dividend_metrics(quarter_universe, force=args.force)
    n_eq = df_eq['Ticker'].nunique() if not df_eq.empty else 0
    print(f"Equity dividend metrics: {len(df_eq)} rows, {n_eq} tickers")

    etf_universe = load_etf_universe_from_disk()
    if etf_universe is None:
        print("ERROR: ETF universe cache missing -- run build_universes.py first")
        return
    df_etf = load_or_build_dividend_metrics_etf(etf_universe, force=args.force)
    n_etf = df_etf['Ticker'].nunique() if not df_etf.empty else 0
    print(f"ETF dividend metrics: {len(df_etf)} rows, {n_etf} tickers")


if __name__ == "__main__":
    main()
