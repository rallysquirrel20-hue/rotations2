#!/usr/bin/env python
"""Fetch live Databento data, compute intraday signals and basket OHLC."""
import sys, os, argparse, time, json, re
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from zoneinfo import ZoneInfo
from datetime import datetime
from pathlib import Path

from config import (
    SIZE, ETF_SIZE, SIGNALS,
    DATA_FOLDER, paths,
    SIGNALS_CACHE_FILE, ETF_SIGNALS_CACHE_FILE,
    CACHE_FILE, ETF_CACHE_FILE,
    BETA_CACHE_FILE, MOMENTUM_CACHE_FILE, RISK_ADJ_MOM_CACHE_FILE,
    DIVIDEND_CACHE_FILE, SIZE_CACHE_FILE, VOLUME_GROWTH_CACHE_FILE,
    GICS_CACHE_FILE,
    CHART_SCHEMA_VERSION,
    load_universe_from_disk, load_etf_universe_from_disk,
    load_gics_from_disk, load_thematic_universe_from_disk,
    get_current_quarter_key,
    _quarter_end_from_key, _quarter_start_from_key,
    WriteThroughPath, _needs_write_and_mirror, build_pdf,
    _install_timed_print, reset_cell_timer,
    BASE_OUTPUT_FOLDER, LIVE_ROTATIONS_FOLDER, HOLDINGS_FOLDER,
    SECTOR_LIST,
)
from build_signals import (
    _build_signals_from_df, _build_signals_next_row,
    _get_latest_norgate_date,
)


# ---------------------------------------------------------------------------
# Databento environment
# ---------------------------------------------------------------------------
import requests
from dotenv import load_dotenv
import databento as db


def _load_env_file() -> None:
    try:
        base_path = Path(__file__).resolve().parent
    except NameError:
        base_path = Path.cwd()
    # Try local .env first
    env_path = base_path / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
        return
    # Try parent directories (handles Python_Files/.env when run from rotations_signals/)
    for parent in base_path.parents:
        candidate = parent / "Python_Files" / ".env"
        if candidate.exists():
            load_dotenv(candidate, override=False)
            return
    # Last resort: try dotenv's built-in search
    load_dotenv(override=False)


_load_env_file()

DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
DATABENTO_DATASET = os.getenv("DATABENTO_DATASET")
DATABENTO_STYPE_IN = os.getenv("DATABENTO_STYPE_IN", "raw_symbol")
DATABENTO_SCHEMA = os.getenv("DATABENTO_SCHEMA", "ohlcv-1m")
DATABENTO_TIMEOUT_S = float(os.getenv("DATABENTO_TIMEOUT_S", "70"))

# Module-level caches -- safe in subprocess isolation
_LIVE_GATE_CACHE = None
_LIVE_UPDATE_CONTEXT_CACHE = None


# ---------------------------------------------------------------------------
# Databento data fetchers
# ---------------------------------------------------------------------------

def get_realtime_prices(symbols, timeout=None):
    """Fetch latest 1-minute close prices from Databento Live (ohlcv-1m schema)."""
    if not DATABENTO_API_KEY:
        raise ValueError("Databento API key not set. Use DATABENTO_API_KEY in .env.")
    if not DATABENTO_DATASET:
        raise ValueError("Databento dataset not set. Use DATABENTO_DATASET in .env.")
    if not symbols:
        return {}

    _timeout = timeout if timeout is not None else DATABENTO_TIMEOUT_S
    symbols = list(symbols)
    prices = {}
    remaining = symbols[:]

    while remaining:
        symbol_map = {}
        client = db.Live()
        client.subscribe(
            dataset=DATABENTO_DATASET,
            schema=DATABENTO_SCHEMA,
            symbols=remaining,
            stype_in=DATABENTO_STYPE_IN,
        )

        start = time.time()
        try:
            for record in client:
                if isinstance(record, db.SymbolMappingMsg):
                    symbol_map[record.instrument_id] = record.stype_out_symbol
                    continue

                price = getattr(record, "close", None)
                instrument_id = getattr(record, "instrument_id", None)
                if price is None or instrument_id is None:
                    continue
                symbol = symbol_map.get(instrument_id)
                if not symbol:
                    continue
                if isinstance(price, int):
                    price = price / 1_000_000_000
                else:
                    price = float(price)
                prices[symbol] = price

                if len(prices) >= len(symbols):
                    break
                if (time.time() - start) >= _timeout:
                    break
            break
        except db.BentoError as exc:
            print(f"Databento error: {exc}")
            break
        finally:
            client.stop()
            try:
                client.block_for_close(timeout=5)
            except Exception:
                pass

    return prices


def get_realtime_ohlcv(symbols):
    """Fetch today's running daily bar (open/high/low/close/volume) from Databento Live (ohlcv-1d schema)."""
    if not DATABENTO_API_KEY:
        raise ValueError("Databento API key not set. Use DATABENTO_API_KEY in .env.")
    if not DATABENTO_DATASET:
        raise ValueError("Databento dataset not set. Use DATABENTO_DATASET in .env.")
    if not symbols:
        return {}

    symbols = list(symbols)
    ohlcv = {}
    remaining = symbols[:]

    while remaining:
        symbol_map = {}
        client = db.Live()
        client.subscribe(
            dataset=DATABENTO_DATASET,
            schema='ohlcv-1d',
            symbols=remaining,
            stype_in=DATABENTO_STYPE_IN,
        )

        start = time.time()
        try:
            for record in client:
                if isinstance(record, db.SymbolMappingMsg):
                    symbol_map[record.instrument_id] = record.stype_out_symbol
                    continue

                instrument_id = getattr(record, 'instrument_id', None)
                if instrument_id is None:
                    continue
                symbol = symbol_map.get(instrument_id)
                if not symbol:
                    continue

                def _to_price(val):
                    if val is None:
                        return None
                    return val / 1_000_000_000 if isinstance(val, int) else float(val)

                o = _to_price(getattr(record, 'open', None))
                h = _to_price(getattr(record, 'high', None))
                l = _to_price(getattr(record, 'low', None))
                c = _to_price(getattr(record, 'close', None))
                v = getattr(record, 'volume', None)
                if v is not None:
                    v = int(v)

                if any(x is None for x in (o, h, l, c)):
                    continue

                ohlcv[symbol] = {'open': o, 'high': h, 'low': l, 'close': c, 'volume': v}

                if len(ohlcv) >= len(symbols):
                    break
                if (time.time() - start) >= DATABENTO_TIMEOUT_S:
                    break
            break
        except db.BentoError as exc:
            print(f"Databento error (ohlcv-1d): {exc}")
            break
        finally:
            client.stop()
            try:
                client.block_for_close(timeout=5)
            except Exception:
                pass

    return ohlcv


def get_live_ohlc_bars(symbols):
    """Fetch today's OHLC for each symbol.

    All fields come from Historical ohlcv-1m records aggregated since 9:30 ET.
    Close is the last 1m bar's close (last traded price).
    Returns Dict[str, dict] with keys 'Open', 'High', 'Low', 'Close'.
    """
    if not DATABENTO_API_KEY or not DATABENTO_DATASET:
        return {}
    if not symbols:
        return {}

    symbols = list(symbols)

    # --- Historical O/H/L/C via ohlcv-1m (to_df has symbol column built-in) ---
    ohlc = {}
    try:
        hist_client = db.Historical(DATABENTO_API_KEY)
        dataset_range = hist_client.metadata.get_dataset_range(dataset=DATABENTO_DATASET)
        available_end = dataset_range['end']
        today_open_utc = (
            pd.Timestamp.now(tz="America/New_York")
            .replace(hour=9, minute=30, second=0, microsecond=0, nanosecond=0)
            .tz_convert("UTC")
            .isoformat()
        )
        data = hist_client.timeseries.get_range(
            dataset=DATABENTO_DATASET,
            schema='ohlcv-1m',
            symbols=symbols,
            start=today_open_utc,
            end=available_end,
        )
        df = data.to_df()
        if not df.empty and 'symbol' in df.columns:
            for sym, grp in df.groupby('symbol'):
                if not sym or grp.empty:
                    continue
                ohlc[sym] = {
                    'Open':  float(grp['open'].iloc[0]),
                    'High':  float(grp['high'].max()),
                    'Low':   float(grp['low'].min()),
                    'Close': float(grp['close'].iloc[-1]),
                }
    except Exception as exc:
        print(f"[get_live_ohlc_bars] Historical ohlcv-1m failed: {exc}")

    return ohlc


# ---------------------------------------------------------------------------
# Norgate date helpers
# ---------------------------------------------------------------------------

def _get_latest_norgate_date_fallback(all_signals_df=None):
    """Return latest Norgate date, falling back to all_signals_df max date."""
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and all_signals_df is not None and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        latest_norgate = pd.to_datetime(all_signals_df['Date']).max().normalize()
    if latest_norgate is None:
        return None
    ts = pd.Timestamp(latest_norgate)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)
    return ts.normalize()


def _extract_spy_trade_date_from_df(df):
    if df is None or df.empty:
        return None
    out = df.copy()
    if not isinstance(out.index, pd.RangeIndex):
        out = out.reset_index()
    out.columns = [str(c).lower() for c in out.columns]
    if 'index' in out.columns and 'ts_event' not in out.columns:
        out = out.rename(columns={'index': 'ts_event'})

    ts_col = next((c for c in ('ts_event', 'ts_recv', 'ts_out') if c in out.columns), None)
    if ts_col is None:
        return None
    ts = pd.to_datetime(out[ts_col], utc=True, errors='coerce').dropna()
    if ts.empty:
        return None
    latest_et = ts.max().tz_convert("America/New_York")
    return latest_et.tz_localize(None).normalize()


def _get_spy_last_trade_date_databento():
    if not DATABENTO_API_KEY or not DATABENTO_DATASET:
        return None
    try:
        client = db.Historical(DATABENTO_API_KEY)
        dataset_range = client.metadata.get_dataset_range(dataset=DATABENTO_DATASET)
        available_end = dataset_range['end']
        today_open_utc = (
            pd.Timestamp.now(tz="America/New_York")
            .replace(hour=9, minute=30, second=0, microsecond=0, nanosecond=0)
            .tz_convert("UTC")
            .isoformat()
        )
        data = client.timeseries.get_range(
            dataset=DATABENTO_DATASET,
            schema='ohlcv-1m',
            symbols=['SPY'],
            start=today_open_utc,
            end=available_end,
        )
        trade_date = _extract_spy_trade_date_from_df(data.to_df())
        if trade_date is not None:
            print(f"SPY last trade date (Historical ohlcv-1m): {trade_date.date()}")
            return trade_date
    except Exception as exc:
        print(f"Historical SPY date lookup failed: {exc}")
    return None


# ---------------------------------------------------------------------------
# Live update gate
# ---------------------------------------------------------------------------

def _get_live_update_gate(all_signals_df=None):
    """Determine whether a live update should run.

    Parameters
    ----------
    all_signals_df : DataFrame, optional
        Passed through to ``_get_latest_norgate_date_fallback`` so this module
        does not require any module-level global.
    """
    global _LIVE_GATE_CACHE
    # Fast market-hours guard -- skip Databento entirely outside trading hours
    _now_et = datetime.now(ZoneInfo("America/New_York"))
    if _now_et.weekday() >= 5:                                    # Sat=5, Sun=6
        return {'should_live_update': False, 'reason': 'weekend'}
    _mkt_open  = _now_et.replace(hour=9,  minute=25, second=0, microsecond=0)
    _mkt_close = _now_et.replace(hour=16, minute=15, second=0, microsecond=0)
    if not (_mkt_open <= _now_et <= _mkt_close):
        return {'should_live_update': False, 'reason': 'outside_market_hours'}

    latest_norgate = _get_latest_norgate_date_fallback(all_signals_df)
    if latest_norgate is None:
        return {'should_live_update': False, 'reason': 'missing_norgate_date'}

    if isinstance(_LIVE_GATE_CACHE, dict):
        cached_norgate = _LIVE_GATE_CACHE.get('latest_norgate_date')
        if pd.notna(cached_norgate) and pd.Timestamp(cached_norgate).normalize() == pd.Timestamp(latest_norgate).normalize():
            return _LIVE_GATE_CACHE

    spy_trade_date = _get_spy_last_trade_date_databento()
    if spy_trade_date is None:
        # Do NOT cache failures -- allow retry on next call
        return {
            'should_live_update': False,
            'reason': 'missing_spy_databento_trade',
            'latest_norgate_date': latest_norgate,
        }

    latest_norgate = pd.Timestamp(latest_norgate).normalize()
    spy_trade_date = pd.Timestamp(spy_trade_date).normalize()
    gate = {
        'should_live_update': bool(spy_trade_date > latest_norgate),
        'reason': 'databento_newer_than_norgate' if spy_trade_date > latest_norgate else 'norgate_up_to_date',
        'latest_norgate_date': latest_norgate,
        'spy_trade_date': spy_trade_date,
    }
    _LIVE_GATE_CACHE = gate
    return gate


def _is_market_open_via_spy_volume(all_signals_df=None):
    """Compatibility wrapper: live update only when Databento SPY date > Norgate date."""
    gate = _get_live_update_gate(all_signals_df)
    return bool(gate.get('should_live_update', False))


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------

def _append_live_row(df, live_price, live_dt):
    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    live_dt = pd.to_datetime(live_dt)
    last_dt = df.index[-1]
    if live_dt <= last_dt:
        live_dt = last_dt + pd.Timedelta(minutes=1)

    for col in ['Open', 'High', 'Low', 'Close']:
        if col not in df.columns:
            df[col] = np.nan
    if 'Volume' not in df.columns:
        df['Volume'] = np.nan

    df.loc[live_dt, ['Open', 'High', 'Low', 'Close', 'Volume']] = [
        live_price, live_price, live_price, live_price, 0
    ]
    return df


def build_signals_for_ticker_live(ticker, live_price, live_dt, all_signals_df):
    """Build signals for a single ticker using live price, given all_signals_df."""
    if all_signals_df is None or all_signals_df.empty:
        return None
    df = all_signals_df[all_signals_df['Ticker'] == ticker].copy()
    if df.empty:
        return None
    df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']].copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').set_index('Date')
    df_live = _append_live_row(df, live_price, live_dt)
    return _build_signals_from_df(df_live, ticker)


_SIGNAL_ORDER = ['Breakout', 'Up_Rot', 'BTFD', 'STFR', 'Down_Rot', 'Breakdown']
_SIGNAL_RANK  = {s: i for i, s in enumerate(_SIGNAL_ORDER)}

def _sort_signals_df(df):
    """Sort by Signal_Type (custom order), then Industry."""
    df = df.copy()
    df['_sig_rank'] = df['Signal_Type'].map(_SIGNAL_RANK).fillna(len(_SIGNAL_ORDER))
    sort_cols = ['_sig_rank']
    if 'Industry' in df.columns:
        sort_cols.append('Industry')
    df = df.sort_values(sort_cols, ascending=True, na_position='last')
    df = df.drop(columns=['_sig_rank']).reset_index(drop=True)
    return df


def _fmt_price(x):
    return f"${x:,.2f}" if pd.notna(x) else ""


def _slugify_label(label):
    return str(label).replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')


# ---------------------------------------------------------------------------
# Norgate last-row lookups
# ---------------------------------------------------------------------------

def _get_latest_norgate_rows_by_ticker(all_signals_df, before_date=None):
    """Return the most recent signal row per ticker from all_signals_df.

    Parameters
    ----------
    all_signals_df : DataFrame
        Full signals dataframe with Date and Ticker columns.
    before_date : Timestamp-like, optional
        If given, only rows strictly before this date are considered.
    """
    df = all_signals_df
    if before_date is not None:
        cutoff = pd.Timestamp(before_date).normalize()
        df = df[df['Date'] < cutoff]
    return (
        df.sort_values('Date')
        .groupby('Ticker', as_index=False)
        .tail(1)
        .set_index('Ticker')
    )


# ---------------------------------------------------------------------------
# Live basket return / OHLC computation
# ---------------------------------------------------------------------------

def _compute_live_basket_return(universe_by_qtr, live_price_map, last_rows, current_key):
    """Compute one-day basket return from Norgate-close -> live-price move."""
    current_universe = universe_by_qtr.get(current_key, set())
    if not current_universe:
        return np.nan

    tickers = [t for t in current_universe if t in live_price_map and t in last_rows.index]
    if not tickers:
        return np.nan

    weights = []
    rets = []
    for t in tickers:
        prev_close = last_rows.at[t, 'Close'] if 'Close' in last_rows.columns else np.nan
        live_price = live_price_map.get(t, np.nan)
        if pd.isna(prev_close) or pd.isna(live_price) or float(prev_close) <= 0:
            continue
        ret = (float(live_price) / float(prev_close)) - 1.0
        vol = last_rows.at[t, 'Volume'] if 'Volume' in last_rows.columns else np.nan
        if pd.notna(vol) and float(vol) > 0:
            w = float(prev_close) * float(vol)
        else:
            w = 1.0
        rets.append(ret)
        weights.append(w)

    if not rets:
        return np.nan

    w_arr = np.asarray(weights, dtype=float)
    r_arr = np.asarray(rets, dtype=float)
    if np.nansum(w_arr) > 0:
        return float(np.nansum(w_arr * r_arr) / np.nansum(w_arr))
    return float(np.nanmean(r_arr))


def _compute_live_basket_ohlc(universe_by_qtr, live_ohlc_map, last_rows, current_key, prev_basket_close):
    """Return today's basket OHLC as absolute equity-curve values, or None."""
    current_universe = universe_by_qtr.get(current_key, set())
    if not current_universe or prev_basket_close is None or pd.isna(prev_basket_close):
        return None
    tickers = [t for t in current_universe if t in live_ohlc_map and t in last_rows.index]
    if not tickers:
        return None

    weights, o_rets, h_rets, l_rets, c_rets = [], [], [], [], []
    for t in tickers:
        prev_close = last_rows.at[t, 'Close'] if 'Close' in last_rows.columns else np.nan
        if pd.isna(prev_close) or float(prev_close) <= 0:
            continue
        bar = live_ohlc_map[t]
        vol = last_rows.at[t, 'Volume'] if 'Volume' in last_rows.columns else np.nan
        w = float(prev_close) * float(vol) if pd.notna(vol) and float(vol) > 0 else 1.0
        weights.append(w)
        o_rets.append((float(bar['Open'])  / float(prev_close)) - 1.0)
        h_rets.append((float(bar['High'])  / float(prev_close)) - 1.0)
        l_rets.append((float(bar['Low'])   / float(prev_close)) - 1.0)
        c_rets.append((float(bar['Close']) / float(prev_close)) - 1.0)

    if not weights:
        return None
    w = np.asarray(weights); w = w / w.sum()
    def wavg(rets): return float(np.dot(w, rets))
    base = float(prev_basket_close)
    return {
        'Open':  base * (1 + wavg(o_rets)),
        'High':  base * (1 + wavg(h_rets)),
        'Low':   base * (1 + wavg(l_rets)),
        'Close': base * (1 + wavg(c_rets)),
    }


def _compute_live_basket_ohlcv(universe_by_qtr, ohlcv_map, last_rows, current_key, basket_prev_close):
    """Compute a synthetic basket OHLCV for today using dollar-weighted returns per field.

    For each field (open, high, low, close):
        weight_i  = prev_close_i * volume_i  (or 1.0 if no volume)
        ret_i     = ohlcv_map[t][field] / prev_close_i - 1
        basket_field = basket_prev_close * (1 + weighted_avg_ret)

    Returns {'open': x, 'high': x, 'low': x, 'close': x} in equity-curve units,
    or None if insufficient data.
    """
    current_universe = universe_by_qtr.get(current_key, set())
    if not current_universe:
        return None
    if pd.isna(basket_prev_close) or float(basket_prev_close) <= 0:
        return None

    tickers = [t for t in current_universe if t in ohlcv_map and t in last_rows.index]
    if not tickers:
        return None

    # Build weights and per-ticker prev_close once
    ticker_data = []
    for t in tickers:
        prev_close = last_rows.at[t, 'Close'] if 'Close' in last_rows.columns else np.nan
        if pd.isna(prev_close) or float(prev_close) <= 0:
            continue
        bar = ohlcv_map[t]
        if any(bar.get(f) is None or pd.isna(bar.get(f)) for f in ('open', 'high', 'low', 'close')):
            continue
        vol = last_rows.at[t, 'Volume'] if 'Volume' in last_rows.columns else np.nan
        w = float(prev_close) * float(vol) if (pd.notna(vol) and float(vol) > 0) else 1.0
        ticker_data.append((float(prev_close), bar, w))

    if not ticker_data:
        return None

    weights = np.asarray([d[2] for d in ticker_data], dtype=float)
    total_w = np.nansum(weights)

    result = {}
    for field in ('open', 'high', 'low', 'close'):
        rets = np.asarray(
            [(d[1][field] / d[0]) - 1.0 for d in ticker_data],
            dtype=float,
        )
        if total_w > 0:
            weighted_ret = float(np.nansum(weights * rets) / total_w)
        else:
            weighted_ret = float(np.nanmean(rets))
        result[field] = float(basket_prev_close) * (1.0 + weighted_ret)

    return result


# ---------------------------------------------------------------------------
# Basket parquet lookup helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Live update context
# ---------------------------------------------------------------------------

def _get_live_update_context(quarter_universe, all_signals_df):
    """Return live-update context only when Databento SPY date is newer than Norgate.

    Parameters
    ----------
    quarter_universe : dict
        Mapping of quarter key -> set of tickers (e.g. QUARTER_UNIVERSE).
    all_signals_df : DataFrame
        Full signals dataframe used for fallback date detection and last-row lookup.
    """
    global _LIVE_UPDATE_CONTEXT_CACHE
    gate = _get_live_update_gate(all_signals_df)
    if not gate.get('should_live_update', False):
        return None

    if isinstance(_LIVE_UPDATE_CONTEXT_CACHE, dict):
        cached_today = _LIVE_UPDATE_CONTEXT_CACHE.get('today')
        if (
            pd.notna(cached_today)
            and pd.Timestamp(cached_today).normalize() == pd.Timestamp(gate['spy_trade_date']).normalize()
        ):
            return _LIVE_UPDATE_CONTEXT_CACHE

    # Keep gate date logic from Databento, but use current ET wall-clock for intraday file timestamps.
    now = datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
    current_key = f"{gate['spy_trade_date'].year} Q{(gate['spy_trade_date'].month - 1) // 3 + 1}"
    current_universe = sorted(t for t in quarter_universe.get(current_key, set()) if '-' not in t)
    if not current_universe:
        return None

    live_ohlc_map = get_live_ohlc_bars(current_universe)
    if not live_ohlc_map:
        return None

    live_price_map = {t: v['Close'] for t, v in live_ohlc_map.items() if v.get('Close') is not None}
    # Build lowercase ohlcv_map for existing basket consumers
    ohlcv_map = {t: {k.lower(): v for k, v in bar.items()} for t, bar in live_ohlc_map.items()}

    last_rows = _get_latest_norgate_rows_by_ticker(all_signals_df, before_date=gate['spy_trade_date'])
    common = [t for t in current_universe if t in live_price_map and t in last_rows.index]
    if not common:
        return None

    ctx = {
        'today': pd.Timestamp(gate['spy_trade_date']),
        'current_key': current_key,
        'current_universe': current_universe,
        'live_dt': now,
        'live_price_map': live_price_map,
        'live_ohlc_map': live_ohlc_map,
        'ohlcv_map': ohlcv_map,
        'last_rows': last_rows,
    }
    _LIVE_UPDATE_CONTEXT_CACHE = ctx
    return ctx


# ---------------------------------------------------------------------------
# Signal export functions
# ---------------------------------------------------------------------------

def _get_ticker_theme(ticker, thematic_universes):
    """Return comma-separated theme labels for a ticker.

    Parameters
    ----------
    ticker : str
    thematic_universes : list of (name, set) pairs for the current quarter.
    """
    labels = [name for name, u in thematic_universes if ticker in u]
    return ', '.join(labels)


def export_today_signals(quarter_universe, all_signals_df,
                         ticker_sector=None, ticker_subindustry=None,
                         thematic_universes=None,
                         verbose=False, live_ctx=None):
    """Export live intraday signals to Excel.

    Parameters
    ----------
    quarter_universe : dict
        Mapping of quarter key -> set of tickers.
    all_signals_df : DataFrame
        Full signals dataframe.
    ticker_sector : dict, optional
        Ticker -> sector mapping.
    ticker_subindustry : dict, optional
        Ticker -> sub-industry mapping.
    thematic_universes : list, optional
        List of (name, set) for theme lookups.
    verbose : bool
    live_ctx : dict, optional
        Pre-built live context. If None, one is computed.
    """
    global _LIVE_UPDATE_CONTEXT_CACHE
    if ticker_sector is None:
        ticker_sector = {}
    if ticker_subindustry is None:
        ticker_subindustry = {}
    if thematic_universes is None:
        thematic_universes = []

    if live_ctx is None:
        gate = _get_live_update_gate(all_signals_df)
        if not gate.get('should_live_update', False):
            if gate.get('reason') == 'norgate_up_to_date':
                print(
                    f"Skipping live signals export: Databento SPY date "
                    f"{pd.Timestamp(gate['spy_trade_date']).strftime('%Y-%m-%d')} equals Norgate "
                    f"{pd.Timestamp(gate['latest_norgate_date']).strftime('%Y-%m-%d')}."
                )
            else:
                print(
                    "Skipping live signals export: unable to fetch Databento SPY last trade date "
                    f"newer than Norgate {pd.Timestamp(gate['latest_norgate_date']).strftime('%Y-%m-%d')}."
                    if gate.get('latest_norgate_date') is not None else
                    "Skipping live signals export: unable to determine Databento/Norgate date gate."
                )
            return None

        et_now = datetime.now(ZoneInfo("America/New_York"))
        now = et_now.replace(tzinfo=None)
        current_key = f"{gate['spy_trade_date'].year} Q{(gate['spy_trade_date'].month - 1) // 3 + 1}"
        current_universe = quarter_universe.get(current_key, set())
        if not current_universe:
            print(f"No universe found for {current_key}")
            return None
        tickers = sorted(t for t in current_universe if '-' not in t)
        live_ohlc_map = get_live_ohlc_bars(tickers)
        if live_ohlc_map:
            _today_str = pd.Timestamp(gate['spy_trade_date']).strftime('%Y-%m-%d')
            _rows = [{'Date': _today_str, 'Ticker': t,
                       'Open': v['Open'], 'High': v['High'],
                       'Low': v['Low'], 'Close': v['Close']}
                      for t, v in live_ohlc_map.items()]
            _live_ohlc_df = pd.DataFrame(_rows)
            _live_ohlc_path = paths.data / f'live_signals_{SIZE}.parquet'
            _live_ohlc_df.to_parquet(_live_ohlc_path, index=False)
            print(f"Saved live OHLC ({len(_live_ohlc_df)} tickers): {_live_ohlc_path}")
        price_map = {t: v['Close'] for t, v in live_ohlc_map.items() if v.get('Close') is not None}
        last_rows = _get_latest_norgate_rows_by_ticker(all_signals_df, before_date=gate['spy_trade_date'])
        live_ctx = {
            'today': pd.Timestamp(gate['spy_trade_date']),
            'current_key': current_key,
            'current_universe': tickers,
            'live_dt': now,
            'live_price_map': price_map,
            'live_ohlc_map': live_ohlc_map,
            'last_rows': last_rows,
            'spy_trade_date': pd.Timestamp(gate['spy_trade_date']),
            'latest_norgate_date': pd.Timestamp(gate['latest_norgate_date']),
        }
    else:
        # Always use fresh wall-clock ET time for intraday output timestamping.
        now = datetime.now(ZoneInfo("America/New_York")).replace(tzinfo=None)
        current_key = live_ctx['current_key']
        current_universe = quarter_universe.get(current_key, set())
        if not current_universe:
            print(f"No universe found for {current_key}")
            return None
        tickers = sorted(t for t in current_universe if '-' not in t)
        price_map = live_ctx['live_price_map']
        live_ohlc_map = live_ctx.get('live_ohlc_map', {})
        if live_ohlc_map:
            _today_str = pd.Timestamp(live_ctx['today']).strftime('%Y-%m-%d')
            _rows = [{'Date': _today_str, 'Ticker': t,
                       'Open': v['Open'], 'High': v['High'],
                       'Low': v['Low'], 'Close': v['Close']}
                      for t, v in live_ohlc_map.items()]
            _live_ohlc_df = pd.DataFrame(_rows)
            _live_ohlc_path = paths.data / f'live_signals_{SIZE}.parquet'
            _live_ohlc_df.to_parquet(_live_ohlc_path, index=False)
            print(f"Saved live OHLC ({len(_live_ohlc_df)} tickers): {_live_ohlc_path}")
        last_rows = live_ctx['last_rows']
        # Ensure we're using pre-today Norgate baseline (in case context was cached post-append)
        _today_cutoff = pd.Timestamp(live_ctx['today']).normalize()
        if not last_rows.empty and (pd.to_datetime(last_rows['Date']).dt.normalize() >= _today_cutoff).any():
            last_rows = _get_latest_norgate_rows_by_ticker(all_signals_df, before_date=live_ctx['today'])

    _LIVE_UPDATE_CONTEXT_CACHE = live_ctx

    if not current_universe:
        print(f"No universe found for {current_key}")
        return None

    current_universe = quarter_universe.get(current_key, set())

    signal_flags = {
        'Up_Rot': 'Is_Up_Rotation',
        'Down_Rot': 'Is_Down_Rotation',
        'Breakout': 'Is_Breakout',
        'Breakdown': 'Is_Breakdown',
        'BTFD': 'Is_BTFD',
        'STFR': 'Is_STFR',
    }

    rows = []
    total = len(tickers)
    last_milestone = 0

    for i, ticker in enumerate(tickers, start=1):
        try:
            live_price = price_map.get(ticker)
            if live_price is None:
                if verbose:
                    print(f"[{i}/{len(tickers)}] {ticker} failed: no Databento price")
                continue
            if ticker not in last_rows.index:
                if verbose:
                    print(f"[{i}/{len(tickers)}] {ticker} failed: no cached history")
                continue
            last_row = last_rows.loc[ticker]
            ohlc = live_ohlc_map.get(ticker, {})
            new_row = _build_signals_next_row(
                last_row, live_price, now,
                live_high=ohlc.get('High'),
                live_low=ohlc.get('Low'),
                live_open=ohlc.get('Open'),
            )
            if new_row is None:
                continue
            last_row = new_row
            for sig_name, flag_col in signal_flags.items():
                if bool(last_row.get(flag_col, False)):
                    rows.append({
                        'Date': now.date(),
                        'Ticker': ticker,
                        'Close': live_price,
                        'Signal_Type': sig_name,
                        'Theme': _get_ticker_theme(ticker, thematic_universes),
                        'Sector': ticker_sector.get(ticker, ''),
                        'Industry': ticker_subindustry.get(ticker, ''),
                        'Entry_Price': last_row.get(f'{sig_name}_Entry_Price', np.nan),
                    })
        except Exception as exc:
            if verbose:
                print(f"[{i}/{len(current_universe)}] {ticker} failed: {exc}")

        if total > 0:
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    out_df = pd.DataFrame(rows)
    if not out_df.empty:
        col_order = [
            'Date', 'Ticker', 'Close', 'Signal_Type',
            'Theme', 'Sector', 'Industry', 'Entry_Price',
        ]
        out_df = out_df[[c for c in col_order if c in out_df.columns]]
        for col in ['Close', 'Entry_Price']:
            if col in out_df.columns:
                out_df[col] = out_df[col].apply(_fmt_price)
        out_df = _sort_signals_df(out_df)
    today_str = now.strftime('%Y_%m_%d')
    time_str = now.strftime('%H%M')
    out_file = LIVE_ROTATIONS_FOLDER / f'{today_str}_{time_str}_Live_Signals_for_top_{SIZE}.xlsx'
    _n_live = len(out_df)
    try:
        out_df.to_excel(out_file, index=False, engine='openpyxl')
        WriteThroughPath(out_file).sync()
        print(f"Saved live signals ({_n_live} rows): {out_file}")
    except PermissionError:
        # Common case: file already open/locked (e.g., Excel). Fallback to a unique name.
        time_str_fallback = now.strftime('%H%M%S')
        out_file = LIVE_ROTATIONS_FOLDER / f'{today_str}_{time_str_fallback}_Live_Signals_for_top_{SIZE}.xlsx'
        suffix = 1
        while out_file.exists():
            out_file = LIVE_ROTATIONS_FOLDER / f'{today_str}_{time_str_fallback}_{suffix}_Live_Signals_for_top_{SIZE}.xlsx'
            suffix += 1
        out_df.to_excel(out_file, index=False, engine='openpyxl')
        WriteThroughPath(out_file).sync()
        print(f"Primary xlsx path was locked; saved fallback ({_n_live} rows): {out_file}")
    return None



def export_today_etf_signals(etf_universe, live_ctx=None, all_signals_df=None):
    """Export live OHLC for ETF universe to a separate parquet file.

    Mirrors the live OHLC export portion of export_today_signals but for ETFs.
    Reuses the same live update gate and Databento OHLC fetcher.

    Parameters
    ----------
    etf_universe : dict
        Mapping of quarter key -> set of ETF tickers.
    live_ctx : dict, optional
        Pre-built live context.
    all_signals_df : DataFrame, optional
        Passed through to gate for fallback date.
    """
    if live_ctx is None:
        gate = _get_live_update_gate(all_signals_df)
        if not gate.get('should_live_update', False):
            return None
        current_key = f"{gate['spy_trade_date'].year} Q{(gate['spy_trade_date'].month - 1) // 3 + 1}"
    else:
        current_key = live_ctx.get('current_key')
        gate = None

    etf_tickers_set = etf_universe.get(current_key, set())
    if not etf_tickers_set:
        print(f"[ETF live] No ETF universe found for {current_key}")
        return None

    etf_tickers = sorted(t for t in etf_tickers_set if '-' not in t)
    live_ohlc_map = get_live_ohlc_bars(etf_tickers)
    if not live_ohlc_map:
        print("[ETF live] No live OHLC data for ETFs")
        return None

    _today_str = pd.Timestamp(gate['spy_trade_date'] if gate else live_ctx['today']).strftime('%Y-%m-%d')
    _rows = [{'Date': _today_str, 'Ticker': t,
              'Open': v['Open'], 'High': v['High'],
              'Low': v['Low'], 'Close': v['Close']}
             for t, v in live_ohlc_map.items()]
    _live_ohlc_df = pd.DataFrame(_rows)
    _etf_live_ohlc_path = paths.data / 'live_signals_etf_50.parquet'
    _live_ohlc_df.to_parquet(_etf_live_ohlc_path, index=False)
    print(f"[ETF live] Saved live OHLC ({len(_live_ohlc_df)} ETFs): {_etf_live_ohlc_path}")
    return None


def append_live_today_to_etf_signals_parquet(etf_universe, etf_signals_df,
                                              quarter_universe, all_signals_df):
    """Build today's ETF signal rows from live OHLC and append to ETF signals parquet.

    Mirrors append_live_today_to_signals_parquet but for ETFs.

    Parameters
    ----------
    etf_universe : dict
        Mapping of quarter key -> set of ETF tickers.
    etf_signals_df : DataFrame
        ETF signals dataframe.
    quarter_universe : dict
        Passed through to _get_live_update_context.
    all_signals_df : DataFrame
        Passed through to gate/context.
    """
    gate = _get_live_update_gate(all_signals_df)
    if not gate.get('should_live_update', False):
        return

    ctx = _get_live_update_context(quarter_universe, all_signals_df)
    if ctx is None:
        return

    today = ctx['today']
    current_key = ctx['current_key']
    live_dt = ctx['live_dt']

    etf_tickers_set = etf_universe.get(current_key, set())
    if not etf_tickers_set:
        return

    etf_tickers = sorted(t for t in etf_tickers_set if '-' not in t)
    live_ohlc_map = get_live_ohlc_bars(etf_tickers)
    if not live_ohlc_map:
        return

    # Build last rows from ETF signals (not global all_signals_df)
    _etf_df = etf_signals_df
    _before = pd.Timestamp(ctx.get('spy_trade_date', today)).normalize()
    _etf_df_filtered = _etf_df[pd.to_datetime(_etf_df['Date']).dt.normalize() < _before]
    last_rows = (
        _etf_df_filtered.sort_values('Date')
        .groupby('Ticker', as_index=False)
        .tail(1)
        .set_index('Ticker')
    )

    new_rows = []
    for ticker in etf_tickers:
        if ticker not in live_ohlc_map or ticker not in last_rows.index:
            continue
        ohlc = live_ohlc_map[ticker]
        close = ohlc.get('Close')
        if close is None:
            continue
        prev_row = last_rows.loc[ticker]
        new_row = _build_signals_next_row(
            prev_row, close, live_dt,
            live_high=ohlc.get('High'),
            live_low=ohlc.get('Low'),
            live_open=ohlc.get('Open'),
        )
        if new_row is None:
            continue
        new_row['Ticker'] = ticker
        new_row['Date'] = today
        new_row['Source'] = 'live'
        new_rows.append(new_row)

    if not new_rows:
        return

    today_df = pd.DataFrame(new_rows)
    if ETF_SIGNALS_CACHE_FILE.exists():
        try:
            existing = pd.read_parquet(ETF_SIGNALS_CACHE_FILE)
            combined = pd.concat([existing, today_df], ignore_index=True)
        except Exception:
            combined = today_df
    else:
        combined = today_df
    combined = combined.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    combined = combined.sort_values(['Ticker', 'Date']).reset_index(drop=True)
    pq.write_table(pa.Table.from_pandas(combined, preserve_index=False),
                    ETF_SIGNALS_CACHE_FILE, compression='snappy', use_dictionary=False)
    print(f"[ETF live] Appended {len(new_rows)} live rows for {pd.Timestamp(today).date()} to {ETF_SIGNALS_CACHE_FILE.name}")


# ---------------------------------------------------------------------------
# Basket report helpers
# ---------------------------------------------------------------------------


def _get_all_basket_specs_for_reports(
    beta_universe, low_beta_universe,
    momentum_universe, momentum_losers_universe,
    high_yield_universe, div_growth_universe, div_with_growth_universe,
    risk_adj_mom_universe, size_universe, volume_growth_universe,
    sector_universes, industry_universes,
):
    """Build the full list of (label, universe_dict, cache_key) specs for reports.

    All universe dicts are passed as parameters instead of read from module globals.
    """
    specs = [
        ('Theme: High Beta',           beta_universe,            'High Beta'),
        ('Theme: Low Beta',            low_beta_universe,        'Low Beta'),
        ('Theme: Momentum Leaders',    momentum_universe,        'Momentum Leaders'),
        ('Theme: Momentum Losers',     momentum_losers_universe, 'Momentum Losers'),
        ('Theme: High Dividend Yield', high_yield_universe,      'High Dividend Yield'),
        ('Theme: Dividend Growth',     div_growth_universe,      'Dividend Growth'),
        ('Theme: Dividend with Growth', div_with_growth_universe, 'Dividend with Growth'),
        ('Theme: Risk Adj Momentum',   risk_adj_mom_universe,    'Risk Adj Momentum'),
        ('Theme: Size',                size_universe,            'Size'),
        ('Theme: Volume Growth',       volume_growth_universe,   'Volume Growth'),
    ]
    specs += [(f"Sector: {name}", sector_universes[name], name) for name in sorted(sector_universes.keys())]
    specs += [(f"Industry: {name}", industry_universes[name], name) for name in sorted(industry_universes.keys())]
    return specs


def _write_live_basket_ohlc(live_ctx, all_basket_specs):
    """Write live_basket_signals_{SIZE}.parquet with today's basket OHLC bars.

    Parameters
    ----------
    live_ctx : dict or None
        Live update context.
    all_basket_specs : list
        Output of _get_all_basket_specs_for_reports.
    """
    if live_ctx is None:
        return
    live_ohlc_map = live_ctx.get('live_ohlc_map', {})
    if not live_ohlc_map:
        return

    live_basket_rows = []
    for spec in all_basket_specs:
        if len(spec) == 3:
            group_name, universe_by_qtr, cache_key_name = spec
        else:
            group_name, universe_by_qtr = spec
            cache_key_name = group_name
        _cache_slug = cache_key_name.replace(' ', '_').replace('&', 'and')
        _basket_pq = _find_basket_parquet(_cache_slug)
        _prev_basket_close = None
        if _basket_pq:
            try:
                _eq_df = pd.read_parquet(str(_basket_pq), columns=['Date', 'Close'])
                _eq_df['Date'] = pd.to_datetime(_eq_df['Date']).dt.normalize()
                _prev_basket_close = float(_eq_df.sort_values('Date').iloc[-1]['Close'])
            except Exception:
                pass
        bar = _compute_live_basket_ohlc(
            universe_by_qtr, live_ohlc_map,
            live_ctx['last_rows'], live_ctx['current_key'], _prev_basket_close
        )
        if bar:
            bar['Date'] = live_ctx['today'].strftime('%Y-%m-%d')
            bar['BasketName'] = group_name
            live_basket_rows.append(bar)

    if live_basket_rows:
        _basket_df = pd.DataFrame(live_basket_rows)
        _basket_path = paths.data / f'live_basket_signals_{SIZE}.parquet'
        _basket_df.to_parquet(_basket_path, index=False)
        print(f"Saved live basket OHLC ({len(_basket_df)} baskets): {_basket_path}")


def _build_basket_annual_grid(all_basket_specs, all_signals_df, quarter_universe, live_ctx=None):
    """Return basket_year_grid DataFrame (baskets x years) for _render_return_bar_charts.

    Parameters
    ----------
    all_basket_specs : list
        Output of _get_all_basket_specs_for_reports.
    all_signals_df : DataFrame
        Full signals dataframe (for fallback Norgate date).
    quarter_universe : dict
        Passed through to _get_live_update_context if live_ctx is None.
    live_ctx : dict, optional
    """
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and all_signals_df is not None and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        latest_norgate = pd.to_datetime(all_signals_df['Date']).max().normalize()
    if latest_norgate is None:
        return pd.DataFrame()
    if live_ctx is None:
        live_ctx = _get_live_update_context(quarter_universe, all_signals_df)
    annual_grid = _build_group_annual_return_grid(all_basket_specs, live_ctx=live_ctx)
    if annual_grid.empty:
        return pd.DataFrame()
    return annual_grid.T.sort_index(axis=1)


def _build_group_daily_return_grid(group_specs, quarter_universe, all_signals_df, live_ctx=None):
    """Build daily return grid for all basket specs.

    Parameters
    ----------
    group_specs : list
        Output of _get_all_basket_specs_for_reports.
    quarter_universe : dict
    all_signals_df : DataFrame
    live_ctx : dict, optional
    """
    if live_ctx is None:
        live_ctx = _get_live_update_context(quarter_universe, all_signals_df)
    if live_ctx is not None:
        print("Live update enabled (Databento SPY date newer than Norgate); updating final daily return.")

    by_group = {}
    used_live_today = False
    live_basket_rows = []
    for spec in group_specs:
        if len(spec) == 3:
            group_name, universe_by_qtr, cache_key_name = spec
        else:
            group_name, universe_by_qtr = spec
            cache_key_name = group_name
        daily_series = _compute_daily_returns_for_basket(group_name, universe_by_qtr, cache_key_name)
        if daily_series is not None and not daily_series.empty:
            if live_ctx is not None:
                live_ret = _compute_live_basket_return(
                    universe_by_qtr,
                    live_ctx['live_price_map'],
                    live_ctx['last_rows'],
                    live_ctx['current_key'],
                )
                if pd.notna(live_ret):
                    daily_series.loc[live_ctx['today']] = float(live_ret)
                    used_live_today = True
                live_ohlc_map = live_ctx.get('live_ohlc_map', {})
                if live_ohlc_map:
                    _cache_slug = cache_key_name.replace(' ', '_').replace('&', 'and')
                    _basket_pq = _find_basket_parquet(_cache_slug)
                    _prev_basket_close = None
                    if _basket_pq:
                        try:
                            _eq_df = pd.read_parquet(str(_basket_pq), columns=['Date', 'Close'])
                            _eq_df['Date'] = pd.to_datetime(_eq_df['Date']).dt.normalize()
                            _prev_basket_close = float(_eq_df.sort_values('Date').iloc[-1]['Close'])
                        except Exception:
                            pass
                    bar = _compute_live_basket_ohlc(
                        universe_by_qtr, live_ohlc_map,
                        live_ctx['last_rows'], live_ctx['current_key'], _prev_basket_close
                    )
                    if bar:
                        bar['Date'] = live_ctx['today'].strftime('%Y-%m-%d')
                        bar['BasketName'] = group_name
                        live_basket_rows.append(bar)
            by_group[group_name] = daily_series
    if live_basket_rows:
        _basket_df = pd.DataFrame(live_basket_rows)
        _basket_path = paths.data / f'live_basket_signals_{SIZE}.parquet'
        _basket_df.to_parquet(_basket_path, index=False)
        print(f"Saved live basket OHLC ({len(_basket_df)} baskets): {_basket_path}")
    if not by_group:
        return pd.DataFrame()
    grid = pd.concat(by_group, axis=1).sort_index()
    grid.index.name = 'Date'
    grid.attrs['used_live_today'] = bool(used_live_today)
    if live_ctx is not None:
        grid.attrs['live_today_date'] = live_ctx['today']
    return grid


def _build_basket_daily_grid_last20(all_basket_specs, quarter_universe, all_signals_df, live_ctx=None):
    """Return basket_date_grid (baskets x last-20 dates) for _render_return_bar_charts.

    Parameters
    ----------
    all_basket_specs : list
    quarter_universe : dict
    all_signals_df : DataFrame
    live_ctx : dict, optional
    """
    if live_ctx is None:
        live_ctx = _get_live_update_context(quarter_universe, all_signals_df)
    daily_grid = _build_group_daily_return_grid(all_basket_specs, quarter_universe, all_signals_df, live_ctx=live_ctx)
    if daily_grid.empty:
        return pd.DataFrame()
    daily_grid = daily_grid.sort_index()
    if live_ctx is not None:
        live_today = live_ctx['today']
        for spec in all_basket_specs:
            group_name = spec[0]
            universe_by_qtr = spec[1]
            live_ret = _compute_live_basket_return(
                universe_by_qtr, live_ctx['live_price_map'],
                live_ctx['last_rows'], live_ctx['current_key'])
            if pd.notna(live_ret):
                daily_grid.loc[live_today, group_name] = float(live_ret)
        if live_today in daily_grid.index:
            hist_19 = daily_grid[daily_grid.index < live_today].tail(19)
            live_row = daily_grid.loc[[live_today]]
            daily_grid = pd.concat([hist_19, live_row]).sort_index()
        else:
            daily_grid = daily_grid.tail(20)
    else:
        daily_grid = daily_grid.tail(20)
    return daily_grid.T.sort_index(axis=1)


# ---------------------------------------------------------------------------
# Basket parquet live updates
# ---------------------------------------------------------------------------

def update_basket_parquets_with_live_ohlcv(live_ctx, all_basket_specs):
    """Append (or replace) a live intraday row in each basket's consolidated parquet.

    Reads the consolidated basket parquet, computes live OHLCV bar, builds a new
    signal row via _build_signals_next_row, and appends it.
    Idempotent: any existing row with today's date is dropped before the new row is appended.
    Skips silently if live_ctx is None or a basket's parquet is missing.

    Parameters
    ----------
    live_ctx : dict or None
        Live update context.
    all_basket_specs : list
        Output of _get_all_basket_specs_for_reports. Used instead of BASKET_RESULTS global.
        Each element is (group_name, universe_by_qtr, cache_key_name).
    """
    if live_ctx is None:
        return

    ohlcv_map = live_ctx.get('ohlcv_map', {})
    if not ohlcv_map:
        print("update_basket_parquets_with_live_ohlcv: no ohlcv_map in live_ctx, skipping.")
        return

    today = pd.Timestamp(live_ctx['today']).normalize()
    current_key = live_ctx['current_key']
    last_rows = live_ctx['last_rows']
    updated = 0

    for spec in all_basket_specs:
        if len(spec) == 3:
            group_name, universe_by_qtr, cache_key_name = spec
        else:
            group_name, universe_by_qtr = spec
            cache_key_name = group_name

        slug = _slugify_label(cache_key_name)

        # Find the consolidated basket parquet
        basket_path = _find_basket_parquet(slug)
        if not basket_path:
            continue
        try:
            basket_df = pq.read_table(str(basket_path)).to_pandas()
        except Exception:
            continue

        if basket_df.empty or 'Close' not in basket_df.columns:
            continue

        basket_df['Date'] = pd.to_datetime(basket_df['Date'], errors='coerce').dt.normalize()
        basket_prev_close = float(basket_df.sort_values('Date').iloc[-1]['Close'])

        bar = _compute_live_basket_ohlcv(
            universe_by_qtr, ohlcv_map, last_rows, current_key, basket_prev_close
        )
        if bar is None:
            continue

        # Build signal row from the previous day's row
        prev_row = basket_df.sort_values('Date').iloc[-1]
        new_sig_row = _build_signals_next_row(
            prev_row,
            live_price=bar['close'],
            live_dt=today,
            live_high=bar['high'],
            live_low=bar['low'],
            live_open=bar['open'],
        )

        # Drop any existing today row then append
        basket_df = basket_df[basket_df['Date'] != today].copy()

        if new_sig_row is not None:
            # Use full signal row (includes OHLC + all derived columns)
            new_row_df = pd.DataFrame([new_sig_row])
        else:
            # Fallback: append OHLC-only row
            new_row_df = pd.DataFrame([{
                'Date':  today,
                'Open':  bar['open'],
                'High':  bar['high'],
                'Low':   bar['low'],
                'Close': bar['close'],
            }])

        new_row_df['Source'] = 'live'
        basket_df = pd.concat([basket_df, new_row_df], ignore_index=True)

        try:
            table = pa.Table.from_pandas(basket_df, preserve_index=False)
            existing_meta = table.schema.metadata or {}
            new_meta = {**existing_meta,
                        b'chart_schema_version': str(CHART_SCHEMA_VERSION).encode()}
            pq.write_table(table.replace_schema_metadata(new_meta),
                           str(basket_path), compression='snappy')
            updated += 1
        except Exception as exc:
            print(f"  Failed to save basket parquet for {slug}: {exc}")

    print(f"update_basket_parquets_with_live_ohlcv: Updated {updated} basket parquets")



def main():
    """Run live update cycle: fetch Databento prices, compute signals, export."""
    _install_timed_print()
    reset_cell_timer("Live Intraday Signal Exports")

    print(f"Databento config: API_KEY={'SET' if DATABENTO_API_KEY else 'MISSING'}, DATASET={DATABENTO_DATASET or 'MISSING'}")

    # --- Load universe and signals from disk caches ---
    quarter_universe = load_universe_from_disk()
    if quarter_universe is None:
        print("QUARTER_UNIVERSE not found on disk. Run rotations.py first.")
        return
    etf_universe = load_etf_universe_from_disk()
    if etf_universe is None:
        etf_universe = {}

    # Load all_signals_df from parquet cache
    if not SIGNALS_CACHE_FILE.exists():
        print(f"Signals cache not found: {SIGNALS_CACHE_FILE}. Run rotations.py first.")
        return
    all_signals_df = pd.read_parquet(SIGNALS_CACHE_FILE)
    all_signals_df['Date'] = pd.to_datetime(all_signals_df['Date'])

    # Load ETF signals
    etf_signals_df = pd.DataFrame()
    if ETF_SIGNALS_CACHE_FILE.exists():
        try:
            etf_signals_df = pd.read_parquet(ETF_SIGNALS_CACHE_FILE)
            etf_signals_df['Date'] = pd.to_datetime(etf_signals_df['Date'])
        except Exception:
            pass

    # Load GICS mappings
    gics_result = load_gics_from_disk()
    if gics_result is not None:
        ticker_sector, ticker_subindustry, sector_universes, industry_universes = gics_result
    else:
        ticker_sector, ticker_subindustry = {}, {}
        sector_universes, industry_universes = {}, {}

    # Load thematic universes (using subkey-aware loader)
    beta_universe = load_thematic_universe_from_disk(BETA_CACHE_FILE, 'high') or {}
    low_beta_universe = load_thematic_universe_from_disk(BETA_CACHE_FILE, 'low') or {}
    momentum_universe = load_thematic_universe_from_disk(MOMENTUM_CACHE_FILE, 'winners') or {}
    momentum_losers_universe = load_thematic_universe_from_disk(MOMENTUM_CACHE_FILE, 'losers') or {}
    high_yield_universe = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'high_yield') or {}
    div_growth_universe = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'growth') or {}
    div_with_growth_universe = load_thematic_universe_from_disk(DIVIDEND_CACHE_FILE, 'with_growth') or {}
    risk_adj_mom_universe = load_thematic_universe_from_disk(RISK_ADJ_MOM_CACHE_FILE) or {}
    size_universe = load_thematic_universe_from_disk(SIZE_CACHE_FILE) or {}
    volume_growth_universe = load_thematic_universe_from_disk(VOLUME_GROWTH_CACHE_FILE) or {}

    current_key = get_current_quarter_key()
    thematic_universes = [
        ('High Beta',           beta_universe.get(current_key, set())),
        ('Low Beta',            low_beta_universe.get(current_key, set())),
        ('Momentum Leaders',    momentum_universe.get(current_key, set())),
        ('Momentum Losers',     momentum_losers_universe.get(current_key, set())),
        ('High Dividend Yield', high_yield_universe.get(current_key, set())),
        ('Dividend Growth',     div_growth_universe.get(current_key, set())),
        ('Risk Adj Momentum',   risk_adj_mom_universe.get(current_key, set())),
    ]

    # Force reset caches
    global _LIVE_GATE_CACHE, _LIVE_UPDATE_CONTEXT_CACHE
    _LIVE_GATE_CACHE = None
    _LIVE_UPDATE_CONTEXT_CACHE = None

    # --- Get live context ---
    _live_ctx_for_reports = _get_live_update_context(quarter_universe, all_signals_df)

    # --- Run live exports ---
    export_today_signals(
        quarter_universe, all_signals_df,
        ticker_sector=ticker_sector,
        ticker_subindustry=ticker_subindustry,
        thematic_universes=thematic_universes,
        live_ctx=_live_ctx_for_reports,
    )
    export_today_etf_signals(etf_universe, live_ctx=_live_ctx_for_reports, all_signals_df=all_signals_df)
    append_live_today_to_etf_signals_parquet(etf_universe, etf_signals_df, quarter_universe, all_signals_df)

    # Build all basket specs for report functions
    all_basket_specs = _get_all_basket_specs_for_reports(
        beta_universe, low_beta_universe,
        momentum_universe, momentum_losers_universe,
        high_yield_universe, div_growth_universe, div_with_growth_universe,
        risk_adj_mom_universe, size_universe, volume_growth_universe,
        sector_universes, industry_universes,
    )
    _write_live_basket_ohlc(_live_ctx_for_reports, all_basket_specs)

    print("Live update cycle complete.")


if __name__ == '__main__':
    main()
