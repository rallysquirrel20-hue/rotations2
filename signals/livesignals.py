"""Live Intraday Signals — Phase 4
Fetches live OHLC from Databento, computes signals, appends to consolidated parquets.
Run: python livesignals.py  (loops every ~75s during market hours)
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import time
import os
import json
from datetime import datetime
from pathlib import Path
import traceback

from foundation import (
    SIZE,
    ETF_SIZE,
    SIGNALS,
    CHART_SCHEMA_VERSION,
    paths,
    DATA_FOLDER,
    SIGNALS_CACHE_FILE,
    ETF_SIGNALS_CACHE_FILE,
    _build_signals_from_df,
    _build_signals_next_row,
    _find_basket_parquet,
    _cache_slugify_label,
    build_all_basket_specs,
    load_universe_from_cache,
    load_etf_universe_from_cache,
    load_all_universes,
    get_current_quarter_key,
    WriteThroughPath,
    reset_cell_timer,
    _get_latest_norgate_date,
)

import databento as db
from dotenv import load_dotenv
from zoneinfo import ZoneInfo


# ---------------------------------------------------------------------------
# Databento config + env loading
# ---------------------------------------------------------------------------

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
# Gate / context functions
# ---------------------------------------------------------------------------

def _get_latest_norgate_date_fallback():
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None:
        # Fallback: read from signals parquet
        if SIGNALS_CACHE_FILE.exists():
            try:
                _df = pd.read_parquet(SIGNALS_CACHE_FILE, columns=['Date'])
                _df['Date'] = pd.to_datetime(_df['Date'])
                if 'Source' in _df.columns:
                    _df = _df[_df['Source'] != 'live']
                latest_norgate = _df['Date'].max().normalize() if not _df.empty else None
            except Exception:
                pass
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


def _get_live_update_gate():
    global _LIVE_GATE_CACHE
    # Fast market-hours guard — skip Databento entirely outside trading hours
    _now_et = datetime.now(ZoneInfo("America/New_York"))
    if _now_et.weekday() >= 5:                                    # Sat=5, Sun=6
        return {'should_live_update': False, 'reason': 'weekend'}
    _mkt_open  = _now_et.replace(hour=9,  minute=25, second=0, microsecond=0)
    _mkt_close = _now_et.replace(hour=16, minute=15, second=0, microsecond=0)
    if not (_mkt_open <= _now_et <= _mkt_close):
        return {'should_live_update': False, 'reason': 'outside_market_hours'}

    latest_norgate = _get_latest_norgate_date_fallback()
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


def _is_market_open_via_spy_volume():
    """Compatibility wrapper: live update only when Databento SPY date > Norgate date."""
    gate = _get_live_update_gate()
    return bool(gate.get('should_live_update', False))


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


# ---------------------------------------------------------------------------
# Ticker live signal generation
# ---------------------------------------------------------------------------

def build_signals_for_ticker_live(ticker, live_price, live_dt):
    """Build signals for a single ticker using live price against parquet history."""
    if not SIGNALS_CACHE_FILE.exists():
        return None
    try:
        df = pd.read_parquet(SIGNALS_CACHE_FILE)
    except Exception:
        return None
    df = df[df['Ticker'] == ticker].copy()
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
    """Sort by Signal_Type (custom order), then Industry, then Historical_EV descending."""
    df = df.copy()
    df['_sig_rank'] = df['Signal_Type'].map(_SIGNAL_RANK).fillna(len(_SIGNAL_ORDER))
    def _parse_ev(x):
        try:
            return float(str(x).replace('%', ''))
        except (ValueError, TypeError):
            return float('nan')
    df['_ev_num'] = df['Historical_EV'].apply(_parse_ev) if 'Historical_EV' in df.columns else 0
    sort_cols = ['_sig_rank']
    if 'Industry' in df.columns:
        sort_cols.append('Industry')
    sort_cols.append('_ev_num')
    asc = [True] * (len(sort_cols) - 1) + [False]
    df = df.sort_values(sort_cols, ascending=asc, na_position='last')
    df = df.drop(columns=['_sig_rank', '_ev_num']).reset_index(drop=True)
    return df


def export_today_signals(verbose=False, live_ctx=None):
    global _LIVE_UPDATE_CONTEXT_CACHE
    if live_ctx is None:
        gate = _get_live_update_gate()
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
        current_universe = QUARTER_UNIVERSE.get(current_key, set())
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
        last_rows = _get_latest_norgate_rows_by_ticker(before_date=gate['spy_trade_date'])
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
        current_universe = QUARTER_UNIVERSE.get(current_key, set())
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
            last_rows = _get_latest_norgate_rows_by_ticker(before_date=live_ctx['today'])

    _LIVE_UPDATE_CONTEXT_CACHE = live_ctx
    return None


def append_live_today_to_signals_parquet():
    """Build today's signal rows from live OHLC and append them to signals_cache parquet.

    Uses the live update gate and context to ensure we only run when Databento
    has data newer than Norgate. Drops any existing row for today before appending
    so the function is idempotent.
    """
    gate = _get_live_update_gate()
    if not gate.get('should_live_update', False):
        print(f"[live] Gate closed ({gate.get('reason', 'unknown')}), skipping parquet update.")
        return

    ctx = _get_live_update_context()
    if ctx is None:
        print("[live] No live context available, skipping parquet update.")
        return

    today = ctx['today']
    live_ohlc_map = ctx.get('live_ohlc_map', {})
    last_rows = ctx['last_rows']
    live_dt = ctx['live_dt']

    new_rows = []
    for ticker in ctx['current_universe']:
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
        print("[live] No rows built, skipping parquet update.")
        return

    today_df = pd.DataFrame(new_rows)
    # Read existing parquet directly (without touching in-memory all_signals_df)
    if SIGNALS_CACHE_FILE.exists():
        try:
            existing = pd.read_parquet(SIGNALS_CACHE_FILE)
            combined = pd.concat([existing, today_df], ignore_index=True)
        except Exception:
            combined = today_df
    else:
        combined = today_df
    combined = combined.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
    combined = combined.sort_values(['Ticker', 'Date']).reset_index(drop=True)
    combined.to_parquet(SIGNALS_CACHE_FILE, index=False, compression='snappy')
    print(f"[live] Appended {len(new_rows)} live rows for {pd.Timestamp(today).date()} to {SIGNALS_CACHE_FILE.name}")


# ---------------------------------------------------------------------------
# ETF live signals
# ---------------------------------------------------------------------------

def export_today_etf_signals(live_ctx=None):
    """Export live OHLC for ETF universe to a separate parquet file.

    Mirrors the live OHLC export portion of export_today_signals but for ETFs.
    Reuses the same live update gate and Databento OHLC fetcher.
    """
    if live_ctx is None:
        gate = _get_live_update_gate()
        if not gate.get('should_live_update', False):
            return None
        current_key = f"{gate['spy_trade_date'].year} Q{(gate['spy_trade_date'].month - 1) // 3 + 1}"
    else:
        current_key = live_ctx.get('current_key')
        gate = None

    etf_universe = ETF_UNIVERSE.get(current_key, set())
    if not etf_universe:
        print(f"[ETF live] No ETF universe found for {current_key}")
        return None

    etf_tickers = sorted(t for t in etf_universe if '-' not in t)
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


def append_live_today_to_etf_signals_parquet():
    """Build today's ETF signal rows from live OHLC and append to ETF signals parquet.

    Mirrors append_live_today_to_signals_parquet but for ETFs.
    """
    gate = _get_live_update_gate()
    if not gate.get('should_live_update', False):
        return

    ctx = _get_live_update_context()
    if ctx is None:
        return

    today = ctx['today']
    current_key = ctx['current_key']
    live_dt = ctx['live_dt']

    etf_universe = ETF_UNIVERSE.get(current_key, set())
    if not etf_universe:
        return

    etf_tickers = sorted(t for t in etf_universe if '-' not in t)
    live_ohlc_map = get_live_ohlc_bars(etf_tickers)
    if not live_ohlc_map:
        return

    # Build last rows from ETF signals parquet (not global etf_signals_df)
    if not ETF_SIGNALS_CACHE_FILE.exists():
        return
    try:
        _etf_df = pd.read_parquet(ETF_SIGNALS_CACHE_FILE)
    except Exception:
        return
    _etf_df['Date'] = pd.to_datetime(_etf_df['Date'])
    _before = pd.Timestamp(ctx.get('spy_trade_date', today)).normalize()
    if 'Source' in _etf_df.columns:
        _etf_df = _etf_df[_etf_df['Source'] != 'live']
    _etf_df_filtered = _etf_df[_etf_df['Date'].dt.normalize() < _before]
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
# Live context + basket helpers
# ---------------------------------------------------------------------------

def _get_latest_norgate_rows_by_ticker(before_date=None):
    """Load last row per ticker from signals parquet (excluding live rows)."""
    df = pd.read_parquet(SIGNALS_CACHE_FILE)
    df['Date'] = pd.to_datetime(df['Date'])
    if 'Source' in df.columns:
        df = df[df['Source'] != 'live']
    if before_date is not None:
        cutoff = pd.Timestamp(before_date).normalize()
        df = df[df['Date'] < cutoff]
    return (
        df.sort_values('Date')
        .groupby('Ticker', as_index=False)
        .tail(1)
        .set_index('Ticker')
    )


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


def _get_live_update_context():
    """Return live-update context only when Databento SPY date is newer than Norgate."""
    global _LIVE_UPDATE_CONTEXT_CACHE
    gate = _get_live_update_gate()
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
    current_universe = sorted(t for t in QUARTER_UNIVERSE.get(current_key, set()) if '-' not in t)
    if not current_universe:
        return None

    live_ohlc_map = get_live_ohlc_bars(current_universe)
    if not live_ohlc_map:
        return None

    live_price_map = {t: v['Close'] for t, v in live_ohlc_map.items() if v.get('Close') is not None}
    # Build lowercase ohlcv_map for existing basket consumers
    ohlcv_map = {t: {k.lower(): v for k, v in bar.items()} for t, bar in live_ohlc_map.items()}

    last_rows = _get_latest_norgate_rows_by_ticker(before_date=gate['spy_trade_date'])
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


def export_live_basket_signals(live_ctx=None):
    """Compute live intraday OHLC for every basket and write live_basket_signals parquet."""
    if live_ctx is None:
        live_ctx = _get_live_update_context()
    if live_ctx is None:
        return

    live_ohlc_map = live_ctx.get('live_ohlc_map', {})
    if not live_ohlc_map:
        return

    live_basket_rows = []
    for basket_name, (_, slug, _, universe_by_qtr) in BASKET_RESULTS.items():
        basket_pq = _find_basket_parquet(slug)
        if not basket_pq:
            continue
        try:
            eq_df = pd.read_parquet(str(basket_pq), columns=['Date', 'Close'])
            eq_df['Date'] = pd.to_datetime(eq_df['Date']).dt.normalize()
            prev_basket_close = float(eq_df.sort_values('Date').iloc[-1]['Close'])
        except Exception:
            continue

        bar = _compute_live_basket_ohlc(
            universe_by_qtr, live_ohlc_map,
            live_ctx['last_rows'], live_ctx['current_key'], prev_basket_close
        )
        if bar:
            bar['Date'] = live_ctx['today'].strftime('%Y-%m-%d')
            bar['BasketName'] = basket_name
            live_basket_rows.append(bar)

    if live_basket_rows:
        basket_df = pd.DataFrame(live_basket_rows)
        basket_path = paths.data / f'live_basket_signals_{SIZE}.parquet'
        basket_df.to_parquet(basket_path, index=False)
        print(f"Saved live basket OHLC ({len(basket_df)} baskets): {basket_path}")


def update_basket_parquets_with_live_ohlcv(live_ctx):
    """Append (or replace) a live intraday row in each basket's consolidated parquet.

    Reads the consolidated basket parquet, computes live OHLCV bar, builds a new
    signal row via _build_signals_next_row, and appends it.
    Idempotent: any existing row with today's date is dropped before the new row is appended.
    Skips silently if live_ctx is None or a basket's parquet is missing.
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

    for basket_name, (_, slug, _, universe_by_qtr) in BASKET_RESULTS.items():
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


# ---------------------------------------------------------------------------
# Module-level universe + basket loading (deferred until actually needed)
# ---------------------------------------------------------------------------

QUARTER_UNIVERSE = {}
ETF_UNIVERSE = {}
BASKET_RESULTS = {}

def _load_universes_and_baskets():
    """Load universes from cache and build BASKET_RESULTS for live basket functions."""
    global QUARTER_UNIVERSE, ETF_UNIVERSE, BASKET_RESULTS

    universes = load_all_universes()
    QUARTER_UNIVERSE = universes['QUARTER_UNIVERSE']
    ETF_UNIVERSE = universes['ETF_UNIVERSE']

    # Build basket specs and populate BASKET_RESULTS from cached parquets
    all_baskets = build_all_basket_specs(universes)
    for basket_name, universe_by_qtr, charts_folder, basket_type in all_baskets:
        slug = _cache_slugify_label(basket_name)
        basket_pq = _find_basket_parquet(slug)
        if basket_pq:
            # Mimic the (merged_all, slug, hist_folder, universe_by_qtr) tuple structure
            BASKET_RESULTS[basket_name] = (None, slug, None, universe_by_qtr)

    print(f"Loaded {len(QUARTER_UNIVERSE)} quarter universes, "
          f"{len(ETF_UNIVERSE)} ETF universes, "
          f"{len(BASKET_RESULTS)} basket specs")


# ---------------------------------------------------------------------------
# __main__ — continuous loop
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    import sys

    _load_env_file()
    _load_universes_and_baskets()

    INTERVAL = 75  # seconds
    cycle = 0

    print(f"Live signal loop starting. Interval: {INTERVAL}s")
    print(f"Databento: API_KEY={'SET' if DATABENTO_API_KEY else 'MISSING'}, DATASET={DATABENTO_DATASET or 'MISSING'}")

    while True:
        cycle += 1
        print(f"\n{'='*60}")
        print(f"[livesignals] Cycle {cycle} at {datetime.now():%Y-%m-%d %H:%M:%S}")
        print(f"{'='*60}")

        try:
            # Reset caches each cycle
            _LIVE_GATE_CACHE = None
            _LIVE_UPDATE_CONTEXT_CACHE = None

            gate = _get_live_update_gate()
            if not gate.get('should_live_update', False):
                reason = gate.get('reason', 'unknown')
                print(f"Gate closed: {reason}")
                time.sleep(INTERVAL)
                continue

            # Get live context (fetches Databento OHLC)
            live_ctx = _get_live_update_context()
            if live_ctx is None:
                print("No live context available.")
                time.sleep(INTERVAL)
                continue

            # 1. Write live OHLC snapshot + cache context
            export_today_signals(live_ctx=live_ctx)

            # 2. Append live rows to ticker signals parquet
            append_live_today_to_signals_parquet()

            # 3. Write ETF live OHLC snapshot
            export_today_etf_signals(live_ctx=live_ctx)

            # 4. Append live rows to ETF signals parquet
            append_live_today_to_etf_signals_parquet()

            # 5. Write live basket OHLC file (backward compat)
            export_live_basket_signals(live_ctx=live_ctx)

            # 6. Append live rows to individual basket parquets
            update_basket_parquets_with_live_ohlcv(live_ctx)

            print(f"[livesignals] Cycle {cycle} complete.")

        except KeyboardInterrupt:
            print("\n[livesignals] Stopping.")
            break
        except Exception:
            import traceback
            traceback.print_exc()

        time.sleep(INTERVAL)
