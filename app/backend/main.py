from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
import os
from pathlib import Path
import json
import databento as db
from dotenv import load_dotenv
from datetime import datetime, timedelta
import asyncio
import logging
import signals_engine
import re
from zoneinfo import ZoneInfo
from pydantic import BaseModel
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load .env: check local backend dir first, then repo root, then shared ~/Documents/Repositories/.env
_local_env = Path(__file__).parent / ".env"
_repo_root_env = Path(__file__).parent.parent.parent / ".env"
_shared_env = Path.home() / "Documents" / "Repositories" / ".env"
env_path = next((p for p in [_local_env, _repo_root_env, _shared_env] if p.exists()), _shared_env)
load_dotenv(dotenv_path=env_path, override=True)

app = FastAPI()

# Databento Configuration
DB_API_KEY = os.getenv("DATABENTO_API_KEY")
DB_DATASET = os.getenv("DATABENTO_DATASET", "EQUS.MINI")
DB_STYPE_IN = os.getenv("DATABENTO_STYPE_IN", "raw_symbol")

logger.info(f"--- STARTING BACKEND ---")
logger.info(f"DATASET: {DB_DATASET}")
logger.info(f"STYPE_IN: {DB_STYPE_IN}")
masked_key = DB_API_KEY[:5] + "..." + DB_API_KEY[-5:] if DB_API_KEY and len(DB_API_KEY) > 10 else "NOT SET"
logger.info(f"API_KEY: {masked_key}")
logger.info(f"------------------------")

db_client = db.Historical(DB_API_KEY) if DB_API_KEY and "YOUR_API_KEY" not in DB_API_KEY else None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# PORTABILITY FIX:
# Look for 'DATA_PATH' in environment variables,
# otherwise default to the relative path where your data usually is.
DEFAULT_PATH = Path.home() / "Documents" / "Python_Outputs"
BASE_DIR = Path(os.getenv("PYTHON_OUTPUTS_DIR", str(DEFAULT_PATH))).expanduser()

DATA_STORAGE = BASE_DIR / "Data_Storage"
THEMATIC_BASKET_CACHE = DATA_STORAGE / "thematic_basket_cache"
SECTOR_BASKET_CACHE = DATA_STORAGE / "sector_basket_cache"
INDUSTRY_BASKET_CACHE = DATA_STORAGE / "industry_basket_cache"
BASKET_CACHE_FOLDERS = [THEMATIC_BASKET_CACHE, SECTOR_BASKET_CACHE, INDUSTRY_BASKET_CACHE, DATA_STORAGE]
INDIVIDUAL_SIGNALS_FILE = DATA_STORAGE / "signals_500.parquet"
LIVE_SIGNALS_FILE = DATA_STORAGE / "live_signals_500.parquet"
LIVE_BASKET_SIGNALS_FILE = DATA_STORAGE / "live_basket_signals_500.parquet"
TOP_500_FILE = DATA_STORAGE / "top500stocks.json"
GICS_MAPPINGS_FILE = DATA_STORAGE / "gics_mappings_500.json"
ETF_UNIVERSES_FILE = DATA_STORAGE / "etf_universes_50.json"
ETF_SIGNALS_FILE = DATA_STORAGE / "signals_etf_50.parquet"
ETF_LIVE_SIGNALS_FILE = DATA_STORAGE / "live_signals_etf_50.parquet"
TICKER_NAMES_FILE = DATA_STORAGE / "ticker_names.json"

THEMATIC_CONFIG = {
    "High_Beta": ("beta_universes_500.json", "high"),
    "Low_Beta": ("beta_universes_500.json", "low"),
    "Momentum_Leaders": ("momentum_universes_500.json", "winners"),
    "Momentum_Losers": ("momentum_universes_500.json", "losers"),
    "High_Dividend_Yield": ("dividend_universes_500.json", "high_yield"),
    "Dividend_Growth": ("dividend_universes_500.json", "div_growth"),
    "Dividend_with_Growth": ("dividend_universes_500.json", "div_with_growth"),
    "Risk_Adj_Momentum": ("risk_adj_momentum_500.json", None),
    "Size": ("size_universes_500.json", None),
    "Volume_Growth": ("volume_growth_universes_500.json", None),
}

def _read_live_parquet(path):
    """Read a live parquet file. Returns None if missing, empty, or contains empty dict."""
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return None
        return df
    except Exception:
        return None


def _live_is_current(live_df, norgate_max_date):
    """Return True if the live parquet date is strictly newer than Norgate.

    When Norgate updates after market close, it will have the same date as the
    live file from that trading day.  In that case Norgate's end-of-day data is
    authoritative, so the live overlay should be skipped.  The live file is only
    useful when its date is ahead of Norgate (i.e. market is open, Norgate
    hasn't updated yet).
    """
    if live_df is None or live_df.empty:
        return False
    try:
        live_date = pd.to_datetime(live_df['Date'].iloc[0])
        return live_date > pd.to_datetime(norgate_max_date)
    except Exception:
        return False

def _find_basket_parquet(slug):
    """Glob for a basket parquet by slug prefix across basket cache folders. Returns path or None."""
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        matches = list(folder.glob(f'{slug}_*_of_*_signals.parquet'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_*_signals.parquet'))
        if matches:
            return matches[0]
    return None

def _find_basket_meta(slug):
    """Glob for a basket meta JSON by slug prefix across basket cache folders. Returns path or None."""
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        matches = list(folder.glob(f'{slug}_*_of_*_signals_meta.json'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_*_signals_meta.json'))
        if matches:
            return matches[0]
    return None


def _resolve_ticker_signals_file(ticker):
    """Return (signals_file, live_file) for a ticker — ETF parquet if not found in stocks."""
    if INDIVIDUAL_SIGNALS_FILE.exists():
        try:
            df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker'],
                                 filters=[('Ticker', '==', ticker)])
            if not df.empty:
                return INDIVIDUAL_SIGNALS_FILE, LIVE_SIGNALS_FILE
        except Exception:
            pass
    if ETF_SIGNALS_FILE.exists():
        return ETF_SIGNALS_FILE, ETF_LIVE_SIGNALS_FILE
    return INDIVIDUAL_SIGNALS_FILE, LIVE_SIGNALS_FILE


def _read_ticker_parquet(ticker, columns=None, filters=None):
    """Read signal data for a ticker, trying stock parquet first then ETF."""
    for sig_file in [INDIVIDUAL_SIGNALS_FILE, ETF_SIGNALS_FILE]:
        if not sig_file.exists():
            continue
        try:
            filt = [('Ticker', '==', ticker)]
            if filters:
                filt.extend(filters)
            if columns:
                df = pd.read_parquet(sig_file, columns=columns, filters=filt)
            else:
                df = pd.read_parquet(sig_file, filters=filt)
            if not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()


def clean_data_for_json(df):
    return json.loads(df.to_json(orient="records", date_format="iso"))

def get_latest_universe_tickers(basket_name):
    if GICS_MAPPINGS_FILE.exists():
        with open(GICS_MAPPINGS_FILE, 'r') as f:
            gics = json.load(f)
            search_name = basket_name.replace("_", " ")
            # Search in sector_u and industry_u sub-dicts
            for group_key in ('sector_u', 'industry_u'):
                group = gics.get(group_key, {})
                if search_name in group:
                    d = group[search_name]
                    qs = sorted(d.keys())
                    if qs: return list(d[qs[-1]])
    if basket_name in THEMATIC_CONFIG:
        fn, key = THEMATIC_CONFIG[basket_name]
        p_path = THEMATIC_BASKET_CACHE / fn
        if p_path.exists():
            with open(p_path, 'r') as f:
                data = json.load(f)
                ud = data[key] if key is not None else data
                qs = sorted(ud.keys())
                if qs: return list(ud[qs[-1]])
    return []


def get_meta_file_tickers(basket_name):
    meta_file = _find_basket_meta(basket_name)
    if not meta_file:
        return []
    try:
        with open(meta_file, 'r') as f:
            meta = json.load(f)
        weights = meta.get('state', {}).get('weights', {})
        return list(weights.keys())
    except Exception:
        return []




def _get_universe_history(basket_name):
    """Return the quarterly universe dict for a basket: {'2025 Q4': ['AAPL', ...], ...}"""
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


def _quarter_str_to_date(q_str):
    """Convert '2025 Q4' to pd.Timestamp('2025-10-01')."""
    parts = q_str.split()
    year = int(parts[0])
    qn = int(parts[1][1])
    month = (qn - 1) * 3 + 1
    return pd.Timestamp(year=year, month=month, day=1)


def _get_universe_tickers_for_range(basket_name, start_date, end_date):
    """Return the union of tickers across all quarters overlapping [start_date, end_date]."""
    history = _get_universe_history(basket_name)
    if not history:
        return []
    tickers = set()
    for q_str, q_tickers in history.items():
        q_start = _quarter_str_to_date(q_str)
        qn = int(q_str.split()[1][1])
        q_end_month = qn * 3
        q_end = pd.Timestamp(year=q_start.year, month=q_end_month, day=1) + pd.offsets.MonthEnd(0)
        # Quarter overlaps with range if q_end >= start_date and q_start <= end_date
        if q_end >= start_date and q_start <= end_date:
            tickers.update(q_tickers)
    return list(tickers)


def _get_ticker_join_dates(basket_name, tickers):
    """Return dict of ticker -> pd.Timestamp for when each ticker first appeared in the basket."""
    quarter_data = _get_universe_history(basket_name)
    if not quarter_data:
        return {}
    ticker_set = set(tickers)
    join_dates = {}
    for q in sorted(quarter_data.keys()):
        q_tickers = set(quarter_data[q])
        for t in ticker_set:
            if t in q_tickers and t not in join_dates:
                join_dates[t] = _quarter_str_to_date(q)
    return join_dates


def _get_tickers_for_date(basket_name, target_date):
    """Return the list of tickers that were in the basket at a given date."""
    quarter_data = _get_universe_history(basket_name)
    if not quarter_data:
        return []
    target_ts = pd.Timestamp(target_date)
    # Find the quarter that contains this date (latest quarter start <= target_date)
    best_q = None
    best_ts = None
    for q in sorted(quarter_data.keys()):
        q_ts = _quarter_str_to_date(q)
        if q_ts <= target_ts:
            best_q = q
            best_ts = q_ts
    if best_q is None:
        # Target is before any quarter — use earliest
        qs = sorted(quarter_data.keys())
        best_q = qs[0] if qs else None
    return list(quarter_data[best_q]) if best_q else []



def get_basket_weights_from_contributions(basket_name):
    """Read the latest Weight_BOD per ticker from the contributions parquet."""
    contrib_file = _find_basket_contributions(basket_name)
    if not contrib_file:
        return {}
    try:
        df = pd.read_parquet(contrib_file, columns=['Date', 'Ticker', 'Weight_BOD'])
        if df.empty:
            return {}
        df['Date'] = pd.to_datetime(df['Date'])
        latest = df[df['Date'] == df['Date'].max()]
        return {
            str(row['Ticker']): float(row['Weight_BOD'])
            for _, row in latest.iterrows()
            if pd.notna(row['Weight_BOD'])
        }
    except Exception:
        return {}

def _tally_breadth(tickers, live_close, last_hist):
    """Count uptrend and breakout tickers given live prices and last historical signals."""
    uptrend = bo_seq = total = 0
    for t in tickers:
        if t not in live_close or t not in last_hist.index:
            continue
        total += 1
        lc = live_close[t]
        r = last_hist.loc[t]

        prev_res = r['Resistance_Pivot']
        prev_sup = r['Support_Pivot']
        prev_trend = r['Trend']
        prev_upper = r['Upper_Target']
        prev_lower = r['Lower_Target']
        prev_bo = r['Is_Breakout_Sequence']

        is_up_rot = pd.notna(prev_res) and lc > prev_res
        is_down_rot = pd.notna(prev_sup) and lc < prev_sup

        if is_up_rot:
            trend = True
        elif is_down_rot:
            trend = False
        else:
            trend = bool(prev_trend) if pd.notna(prev_trend) else False

        if trend:
            uptrend += 1

        is_bo = is_up_rot and pd.notna(prev_upper) and lc > prev_upper
        is_bd = is_down_rot and pd.notna(prev_lower) and lc < prev_lower

        if is_bo:
            live_bo = True
        elif is_bd:
            live_bo = False
        else:
            live_bo = bool(prev_bo) if pd.notna(prev_bo) else False

        if live_bo:
            bo_seq += 1

    if total == 0:
        return None
    return {'Uptrend_Pct': round(uptrend / total * 100, 1), 'Breakout_Pct': round(bo_seq / total * 100, 1)}


def _compute_live_breadth_batch(slugs):
    """Compute live Uptrend_Pct, Breakout_Pct, Correlation_Pct for multiple baskets in one pass.
    Returns {slug: {'Uptrend_Pct': ..., 'Breakout_Pct': ..., 'Correlation_Pct': ...}}."""
    live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
    if live_df is None:
        return {}

    # Gather all tickers needed across all baskets
    basket_tickers = {}
    all_tickers = set()
    for slug in slugs:
        tickers = get_latest_universe_tickers(slug)
        if tickers:
            basket_tickers[slug] = tickers
            all_tickers.update(tickers)
    if not all_tickers:
        return {}

    # Read historical signals once for all tickers
    needed_cols = ['Ticker', 'Date', 'Close', 'Trend', 'Resistance_Pivot', 'Support_Pivot',
                   'Upper_Target', 'Lower_Target', 'Is_Breakout_Sequence']
    try:
        hist = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=needed_cols,
                               filters=[('Ticker', 'in', list(all_tickers))])
    except Exception:
        return {}

    if not _live_is_current(live_df, hist['Date'].max()):
        return {}

    live_prices = live_df[live_df['Ticker'].isin(all_tickers)].set_index('Ticker')
    if live_prices.empty:
        return {}
    live_close_all = live_prices['Close'].to_dict()
    last_hist_all = hist.sort_values('Date').groupby('Ticker').tail(1).set_index('Ticker')

    # Read close pivot for correlation (once for all tickers)
    try:
        close_df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                    filters=[('Ticker', 'in', list(all_tickers))])
        close_pivot = close_df.pivot_table(index='Date', columns='Ticker', values='Close').sort_index()
        live_date = pd.to_datetime(live_df['Date'].iloc[0])
        live_series = pd.Series(live_close_all, name=live_date)
        close_pivot = pd.concat([close_pivot, live_series.to_frame().T]).sort_index()
        returns_pivot = close_pivot.pct_change()
    except Exception:
        returns_pivot = None

    result = {}
    for slug, tickers in basket_tickers.items():
        # Breadth
        breadth = _tally_breadth(tickers, live_close_all, last_hist_all)
        if breadth is None:
            continue
        entry = dict(breadth)

        # Correlation from pre-computed returns pivot
        if returns_pivot is not None:
            try:
                basket_cols = [t for t in tickers if t in returns_pivot.columns]
                if len(basket_cols) >= 2:
                    recent = returns_pivot[basket_cols].tail(21)
                    valid = [c for c in basket_cols if recent[c].notna().sum() >= 10]
                    if len(valid) >= 2:
                        corr = recent[valid].corr()
                        mask = np.triu(np.ones(corr.shape, dtype=bool), k=1)
                        vals = corr.values[mask]
                        vals = vals[~np.isnan(vals)]
                        if len(vals) > 0:
                            entry['Correlation_Pct'] = round(float(np.mean(vals) * 100), 2)
            except Exception:
                pass

        result[slug] = entry
    return result


def _compute_live_breadth(basket_name):
    """Compute live-bar Uptrend_Pct, Breakout_Pct, Correlation_Pct from constituent ticker data."""
    tickers = get_latest_universe_tickers(basket_name)
    if not tickers:
        return {}

    live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
    if live_df is None:
        return {}

    # Skip live overlay if Norgate already has newer data (market closed)
    needed_cols = ['Ticker', 'Date', 'Close', 'Trend', 'Resistance_Pivot', 'Support_Pivot',
                   'Upper_Target', 'Lower_Target', 'Is_Breakout_Sequence']
    hist = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=needed_cols, filters=[('Ticker', 'in', tickers)])
    if not _live_is_current(live_df, hist['Date'].max()):
        return {}

    live_prices = live_df[live_df['Ticker'].isin(tickers)].set_index('Ticker')
    if live_prices.empty:
        return {}
    live_close = live_prices['Close'].to_dict()

    last = hist.sort_values('Date').groupby('Ticker').tail(1).set_index('Ticker')

    breadth = _tally_breadth(tickers, live_close, last)
    if breadth is None:
        return {}

    result = dict(breadth)

    # Correlation_Pct: avg pairwise correlation of last 21 days of returns including live
    try:
        close_df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                   filters=[('Ticker', 'in', tickers)])
        pivot = close_df.pivot_table(index='Date', columns='Ticker', values='Close').sort_index()

        # Add live prices as new row
        live_date = pd.to_datetime(live_df['Date'].iloc[0])
        live_series = pd.Series(live_close, name=live_date)
        pivot = pd.concat([pivot, live_series.to_frame().T]).sort_index()

        returns = pivot.pct_change()
        recent = returns.tail(21)
        valid = [c for c in recent.columns if recent[c].notna().sum() >= 10]
        if len(valid) >= 2:
            corr = recent[valid].corr()
            mask = np.triu(np.ones(corr.shape, dtype=bool), k=1)
            vals = corr.values[mask]
            vals = vals[~np.isnan(vals)]
            if len(vals) > 0:
                result['Correlation_Pct'] = round(float(np.mean(vals) * 100), 2)
    except Exception:
        pass

    return result


@app.get("/")
def read_root(): return {"status": "ok", "data_path": str(BASE_DIR)}

@app.get("/api/baskets")
def list_baskets():
    if not DATA_STORAGE.exists(): return {"Themes": [], "Sectors": [], "Industries": []}
    t_names = list(THEMATIC_CONFIG.keys())
    s_names = ["Communication_Services", "Consumer_Discretionary", "Consumer_Staples", "Energy", "Financials", "Health_Care", "Industrials", "Information_Technology", "Materials", "Real_Estate", "Utilities"]
    cats = {"Themes": [], "Sectors": [], "Industries": []}
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        for f in folder.glob("*_of_*_signals.parquet"):
            name = f.stem.rsplit("_signals", 1)[0]
            slug = re.sub(r'(_\d+)?_of_\d+$', '', name)
            if slug in t_names: cats["Themes"].append(slug)
            elif slug in s_names: cats["Sectors"].append(slug)
            else: cats["Industries"].append(slug)
    for k in cats: cats[k] = sorted(set(cats[k]))
    return cats

@app.get("/api/baskets/compositions")
def get_basket_compositions():
    """Return per-quarter ticker lists for every basket (sectors, industries, themes)."""
    result = {}
    # Sectors and Industries from GICS mappings
    if GICS_MAPPINGS_FILE.exists():
        with open(GICS_MAPPINGS_FILE, 'r') as f:
            gics = json.load(f)
        for group_key in ('sector_u', 'industry_u'):
            group = gics.get(group_key, {})
            for name, quarter_dict in group.items():
                slug = name.replace(" ", "_")
                result[slug] = {q: sorted(tickers) for q, tickers in quarter_dict.items()}
    # Themes from thematic config JSON files
    for basket_name, (fn, key) in THEMATIC_CONFIG.items():
        p_path = THEMATIC_BASKET_CACHE / fn
        if p_path.exists():
            try:
                with open(p_path, 'r') as f:
                    data = json.load(f)
                ud = data[key] if key is not None else data
                result[basket_name] = {q: sorted(tickers) for q, tickers in ud.items()}
            except Exception:
                pass
    return result

@app.get("/api/baskets/breadth")
def get_basket_breadth():
    """Return latest Uptrend_Pct and Breakout_Pct for every basket."""
    result = {}
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        for f in folder.glob("*_of_*_signals.parquet"):
            slug = re.sub(r'(_\d+)?_of_\d+_signals$', '', f.stem)
            if slug in result:
                continue
            try:
                sig_cols = ['Date', 'Close', 'Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct',
                            'Trend', 'Is_Breakout_Sequence',
                            'Resistance_Pivot', 'Support_Pivot', 'Upper_Target', 'Lower_Target',
                            'BTFD_Entry_Price', 'BTFD_Exit_Date', 'STFR_Entry_Price', 'STFR_Exit_Date']
                df = pd.read_parquet(f, columns=sig_cols)
                if df.empty:
                    continue
                df = df.sort_values('Date')
                last = df.iloc[-1]
                entry = {}
                if pd.notna(last.get('Uptrend_Pct')):
                    entry['uptrend_pct'] = round(float(last['Uptrend_Pct']), 1)
                if pd.notna(last.get('Breakout_Pct')):
                    entry['breakout_pct'] = round(float(last['Breakout_Pct']), 1)
                if pd.notna(last.get('Correlation_Pct')):
                    entry['corr_pct'] = round(float(last['Correlation_Pct']), 1)
                entry['st_trend'] = 'UP' if last.get('Trend') else 'DN'
                entry['lt_trend'] = 'BO' if last.get('Is_Breakout_Sequence') else 'BD'
                # Mean reversion
                btfd_open = pd.notna(last.get('BTFD_Entry_Price')) and pd.isna(last.get('BTFD_Exit_Date'))
                stfr_open = pd.notna(last.get('STFR_Entry_Price')) and pd.isna(last.get('STFR_Exit_Date'))
                if btfd_open and stfr_open:
                    entry['mean_rev'] = 'BTFD'  # prefer BTFD for baskets
                elif btfd_open:
                    entry['mean_rev'] = 'BTFD'
                elif stfr_open:
                    entry['mean_rev'] = 'STFR'
                # Last price
                if pd.notna(last.get('Close')):
                    entry['last_price'] = round(float(last['Close']), 2)
                # Pct change from last 2 closes
                if len(df) >= 2:
                    prev_close = df.iloc[-2]['Close']
                    curr_close = last['Close']
                    if pd.notna(prev_close) and pd.notna(curr_close) and prev_close != 0:
                        entry['pct_change'] = round(float(curr_close / prev_close - 1) * 100, 2)
                # Stash pivots and prev close for live overlay
                entry['_pivots'] = {
                    'Trend': last.get('Trend'),
                    'Is_Breakout_Sequence': last.get('Is_Breakout_Sequence'),
                    'Resistance_Pivot': last.get('Resistance_Pivot'),
                    'Support_Pivot': last.get('Support_Pivot'),
                    'Upper_Target': last.get('Upper_Target'),
                    'Lower_Target': last.get('Lower_Target'),
                }
                entry['_prev_close'] = float(last['Close']) if pd.notna(last.get('Close')) else None
                result[slug] = entry
            except Exception:
                continue

    # Overlay live breadth values (single batch read of signals parquet)
    try:
        live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
        if live_df is not None:
            needed_cols = ['Ticker', 'Date', 'Trend', 'Resistance_Pivot', 'Support_Pivot',
                           'Upper_Target', 'Lower_Target', 'Is_Breakout_Sequence']
            hist = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=needed_cols,
                                   filters=[('Ticker', 'in', list(live_df['Ticker'].unique()))])
            norgate_max_date = hist['Date'].max()

            if _live_is_current(live_df, norgate_max_date):
                live_close = live_df.set_index('Ticker')['Close'].to_dict()
                last_hist = hist.sort_values('Date').groupby('Ticker').tail(1).set_index('Ticker')

                for slug in list(result.keys()):
                    tickers = get_latest_universe_tickers(slug)
                    if not tickers:
                        continue
                    breadth = _tally_breadth(tickers, live_close, last_hist)
                    if breadth:
                        result[slug]['uptrend_pct'] = breadth['Uptrend_Pct']
                        result[slug]['breakout_pct'] = breadth['Breakout_Pct']
    except Exception:
        pass

    # Overlay live basket equity curve signals using live basket OHLC + cached pivots
    try:
        live_basket_df = _read_live_parquet(LIVE_BASKET_SIGNALS_FILE)
        if live_basket_df is not None and _live_is_current(live_basket_df, norgate_max_date if 'norgate_max_date' in dir() else pd.Timestamp.min):
            name_col = 'BasketName' if 'BasketName' in live_basket_df.columns else 'Basket'
            for slug, entry in result.items():
                pivots = entry.get('_pivots')
                if not pivots:
                    continue
                basket_name_spaced = slug.replace('_', ' ')
                live_row = live_basket_df[live_basket_df[name_col].str.endswith(basket_name_spaced)]
                if live_row.empty:
                    continue
                lc = float(live_row.iloc[0]['Close'])

                prev_res = pivots['Resistance_Pivot']
                prev_sup = pivots['Support_Pivot']
                is_up = pd.notna(prev_res) and lc > prev_res
                is_dn = pd.notna(prev_sup) and lc < prev_sup

                if is_up:
                    entry['st_trend'] = 'UP'
                elif is_dn:
                    entry['st_trend'] = 'DN'

                prev_upper = pivots['Upper_Target']
                prev_lower = pivots['Lower_Target']
                is_bo = is_up and pd.notna(prev_upper) and lc > prev_upper
                is_bd = is_dn and pd.notna(prev_lower) and lc < prev_lower

                if is_bo:
                    entry['lt_trend'] = 'BO'
                elif is_bd:
                    entry['lt_trend'] = 'BD'

                # Live last_price and pct_change from cached prev close
                entry['last_price'] = round(lc, 2)
                prev_close = entry.get('_prev_close')
                if prev_close and prev_close != 0:
                    entry['pct_change'] = round(float(lc / prev_close - 1) * 100, 2)
    except Exception:
        pass

    # Strip internal fields before returning
    for entry in result.values():
        entry.pop('_pivots', None)
        entry.pop('_prev_close', None)

    return result

logger.info(f"BASE_DIR: {BASE_DIR} (exists={BASE_DIR.exists()})")
logger.info(f"DATA_STORAGE: {DATA_STORAGE} (exists={DATA_STORAGE.exists()})")
logger.info(f"INDIVIDUAL_SIGNALS_FILE: {INDIVIDUAL_SIGNALS_FILE} (exists={INDIVIDUAL_SIGNALS_FILE.exists()})")

@app.get("/api/baskets/returns")
def get_basket_returns(start: str = None, end: str = None, mode: str = "period", basket: str = None, group: str = "all", top_n: int = 10, threshold: float = 0.0, conditions: str = None, bar_period: str = "1D", metric: str = "returns"):
    """Cross-basket period returns or single-basket daily returns time series."""
    t_names = list(THEMATIC_CONFIG.keys())
    s_names = ["Communication_Services", "Consumer_Discretionary", "Consumer_Staples", "Energy", "Financials", "Health_Care", "Industrials", "Information_Technology", "Materials", "Real_Estate", "Utilities"]

    # Discover all basket slugs
    all_slugs = set()
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        for f in folder.glob("*_of_*_signals.parquet"):
            slug = re.sub(r'(_\d+)?_of_\d+_signals$', '', f.stem)
            all_slugs.add(slug)

    def _categorize(slug):
        if slug in t_names:
            return "theme"
        elif slug in s_names:
            return "sector"
        else:
            return "industry"

    # Find global date range from any basket
    global_min, global_max = None, None
    for slug in all_slugs:
        pf = _find_basket_parquet(slug)
        if not pf:
            continue
        try:
            df = pd.read_parquet(pf, columns=['Date'])
            if df.empty:
                continue
            df['Date'] = pd.to_datetime(df['Date'])
            dmin, dmax = df['Date'].min(), df['Date'].max()
            if global_min is None or dmin < global_min:
                global_min = dmin
            if global_max is None or dmax > global_max:
                global_max = dmax
        except Exception:
            continue

    # Read live basket closes for today overlay (skip if stale)
    live_closes = {}  # slug -> (date, close)
    try:
        live_basket_df = _read_live_parquet(LIVE_BASKET_SIGNALS_FILE)
        if live_basket_df is not None and _live_is_current(live_basket_df, global_max if global_max else pd.Timestamp.min):
            name_col = 'BasketName' if 'BasketName' in live_basket_df.columns else 'Basket'
            for _, row in live_basket_df.iterrows():
                bname = row[name_col]
                # "Theme: High Beta" -> "High_Beta", "Sector: Financials" -> "Financials"
                slug_candidate = bname.split(': ', 1)[-1].replace(' ', '_') if ': ' in bname else bname.replace(' ', '_')
                live_closes[slug_candidate] = (pd.to_datetime(row['Date']), float(row['Close']))
    except Exception:
        pass

    # Update global_max to include live date if present
    if live_closes:
        live_max = max(d for d, _ in live_closes.values())
        if global_max is None or live_max > global_max:
            global_max = live_max

    # Collect trading dates from the first available basket (for 1D scroll)
    _trading_dates = []
    if not start and not end:
        for slug in sorted(all_slugs):
            pf = _find_basket_parquet(slug)
            if not pf: continue
            try:
                tdf = pd.read_parquet(pf, columns=['Date'])
                tdf['Date'] = pd.to_datetime(tdf['Date'])
                _trading_dates = sorted(tdf['Date'].dropna().unique())
                break
            except Exception:
                continue

    date_range = {
        "min": global_min.strftime('%Y-%m-%d') if global_min else None,
        "max": global_max.strftime('%Y-%m-%d') if global_max else None,
    }

    if mode == "daily":
        # Single basket returns (daily or aggregated by bar_period)
        if not basket:
            raise HTTPException(status_code=400, detail="basket param required for mode=daily")
        if not re.fullmatch(r'[A-Za-z0-9_\-]+', basket):
            raise HTTPException(status_code=400, detail="Invalid basket name")
        pf = _find_basket_parquet(basket)
        if not pf:
            raise HTTPException(status_code=404, detail=f"Basket not found: {basket}")
        df = pd.read_parquet(pf, columns=['Date', 'Close'])
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').dropna(subset=['Close'])
        # Append live row if available and within range
        if basket in live_closes:
            live_date, live_close = live_closes[basket]
            if live_date not in df['Date'].values:
                live_row = pd.DataFrame({'Date': [live_date], 'Close': [live_close]})
                df = pd.concat([df, live_row], ignore_index=True).sort_values('Date')
        # Filter end date first
        if end:
            df = df[df['Date'] <= pd.Timestamp(end)]
        # For start: keep one anchor row before start for pct_change, then filter output
        start_ts = pd.Timestamp(start) if start else None
        if start_ts is not None:
            before_start = df[df['Date'] < start_ts]
            in_range = df[df['Date'] >= start_ts]
            if not before_start.empty:
                df = pd.concat([before_start.iloc[[-1]], in_range])
            else:
                df = in_range

        if bar_period == "1D":
            df['return'] = df['Close'].pct_change()
            df = df.dropna(subset=['return'])
            if start_ts is not None:
                df = df[df['Date'] >= start_ts]
            dates_out = [d.strftime('%Y-%m-%d') for d in df['Date']]
            returns_out = [round(float(r), 6) for r in df['return']]
        else:
            # Aggregate into period buckets using last close per period
            df = df.set_index('Date').sort_index()
            period_map = {'1W': 'W-FRI', '1M': 'ME', '1Q': 'QE', '1Y': 'YE'}
            rule = period_map.get(bar_period, 'ME')
            period_close = df['Close'].resample(rule).last().dropna()
            period_ret = period_close.pct_change().dropna()
            if start_ts is not None:
                period_ret = period_ret[period_ret.index >= start_ts]
            # Label: use period end date for W, last day of month/quarter/year for others
            dates_out = [d.strftime('%Y-%m-%d') for d in period_ret.index]
            returns_out = [round(float(r), 6) for r in period_ret]

        return {
            "basket": basket,
            "dates": dates_out,
            "returns": returns_out,
            "date_range": date_range,
        }

    # ── mode=analogs: regime analogs (cross-basket historical similarity) ──
    if mode == "analogs":
        top_n = max(1, min(50, top_n))
        # Filter baskets by group
        slugs = []
        for slug in sorted(all_slugs):
            cat = _categorize(slug)
            if group != "all":
                if group == "themes" and cat != "theme":
                    continue
                if group == "sectors" and cat != "sector":
                    continue
                if group == "industries" and cat != "industry":
                    continue
            slugs.append(slug)

        # Pre-compute live metrics for all baskets in one pass
        live_breadth_batch = _compute_live_breadth_batch(slugs) if live_closes else {}

        # Read aligned data for all baskets
        COLS = ['Date', 'Close', 'Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA']
        basket_frames = {}
        for slug in slugs:
            pf = _find_basket_parquet(slug)
            if not pf:
                continue
            try:
                df = pd.read_parquet(pf, columns=COLS)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date').drop_duplicates(subset=['Date'])
                # Append live close with real-time metrics computed from constituents
                if slug in live_closes:
                    live_date, live_close = live_closes[slug]
                    if live_date not in df['Date'].values:
                        lb = live_breadth_batch.get(slug, {})
                        # Compute live RV_EMA: RV = |ret|, EMA(RV, span=10)
                        prev_close = df['Close'].iloc[-1]
                        prev_rv_ema = df['RV_EMA'].iloc[-1]
                        if pd.notna(prev_close) and prev_close != 0 and pd.notna(prev_rv_ema):
                            live_rv_ema = 2.0/11.0 * abs(live_close / prev_close - 1) + (1 - 2.0/11.0) * prev_rv_ema
                        else:
                            live_rv_ema = np.nan
                        live_row = pd.DataFrame({'Date': [live_date], 'Close': [live_close],
                                                 'Uptrend_Pct': [lb.get('Uptrend_Pct', np.nan)],
                                                 'Breakout_Pct': [lb.get('Breakout_Pct', np.nan)],
                                                 'Correlation_Pct': [lb.get('Correlation_Pct', np.nan)],
                                                 'RV_EMA': [live_rv_ema]})
                        df = pd.concat([df, live_row], ignore_index=True).sort_values('Date')
                basket_frames[slug] = df.set_index('Date')
            except Exception:
                continue

        if len(basket_frames) < 3:
            return {"current": None, "analogs": [], "date_range": date_range, "message": "Not enough baskets"}

        # Outer-join all baskets by date, forward-fill
        ordered_slugs = sorted(basket_frames.keys())
        B = len(ordered_slugs)
        all_dates = sorted(set().union(*(df.index for df in basket_frames.values())))
        date_idx = pd.DatetimeIndex(all_dates)
        T = len(date_idx)

        close_mat = np.full((T, B), np.nan)
        uptrend_mat = np.full((T, B), np.nan)
        breakout_mat = np.full((T, B), np.nan)
        corr_mat = np.full((T, B), np.nan)
        rv_mat = np.full((T, B), np.nan)

        for j, slug in enumerate(ordered_slugs):
            df = basket_frames[slug]
            idxs = date_idx.searchsorted(df.index)
            valid = idxs < T
            idxs = idxs[valid]
            close_mat[idxs, j] = df['Close'].values[valid]
            uptrend_mat[idxs, j] = df['Uptrend_Pct'].values[valid]
            breakout_mat[idxs, j] = df['Breakout_Pct'].values[valid]
            corr_mat[idxs, j] = df['Correlation_Pct'].values[valid]
            rv_mat[idxs, j] = df['RV_EMA'].values[valid]

        # Forward-fill each column
        for mat in [close_mat, uptrend_mat, breakout_mat, corr_mat, rv_mat]:
            for j in range(B):
                col = mat[:, j]
                mask = np.isnan(col)
                if mask.all():
                    continue
                idx_arr = np.where(~mask, np.arange(T), 0)
                np.maximum.accumulate(idx_arr, out=idx_arr)
                mat[:, j] = col[idx_arr]
                # Keep leading NaN
                first_valid = np.argmax(~mask)
                mat[:first_valid, j] = np.nan

        # Cross-basket rolling correlation (21-day mean pairwise correlation)
        daily_ret = close_mat[1:] / close_mat[:-1] - 1  # (T-1, B)
        corr_window = 21
        cross_corr_series = np.full(T, np.nan)
        if T > corr_window + 1 and B >= 2:
            for i in range(corr_window - 1, daily_ret.shape[0]):
                window_slice = daily_ret[i - corr_window + 1:i + 1]  # (21, B)
                valid_cols = ~np.isnan(window_slice).any(axis=0)
                if valid_cols.sum() < 2:
                    continue
                ws = window_slice[:, valid_cols]
                mu = ws.mean(axis=0, keepdims=True)
                std = ws.std(axis=0, keepdims=True)
                std[std == 0] = 1.0
                z = (ws - mu) / std
                n_valid = valid_cols.sum()
                corr_mat_local = (z.T @ z) / (corr_window - 1)
                # Average upper triangle
                upper_sum = 0.0
                count = 0
                for ci in range(n_valid):
                    for cj in range(ci + 1, n_valid):
                        upper_sum += corr_mat_local[ci, cj]
                        count += 1
                if count > 0:
                    cross_corr_series[i + 1] = upper_sum / count  # +1 because daily_ret is shifted by 1

        # Find current period indices
        if not start or not end:
            return {"current": None, "analogs": [], "date_range": date_range, "message": "start and end required"}
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        start_idx = int(date_idx.searchsorted(start_ts, side='left'))
        end_idx = int(date_idx.searchsorted(end_ts, side='right')) - 1
        if end_idx < 0 or start_idx >= T:
            return {"current": None, "analogs": [], "date_range": date_range, "message": "Date range out of bounds"}
        end_idx = min(end_idx, T - 1)
        start_idx = min(start_idx, end_idx)
        W = end_idx - start_idx
        if W < 5:
            return {"current": None, "analogs": [], "date_range": date_range, "message": "Window too short (< 5 days)"}

        # Helper: rank across baskets (handles NaN by giving worst rank)
        def rank_vec(v, descending=False):
            """Rank values 1..B. descending=True: 1=highest. NaN gets rank B."""
            out = np.full(len(v), float(len(v)))
            valid = ~np.isnan(v)
            nv = valid.sum()
            if nv > 0:
                order = np.argsort(v[valid])
                ranks = np.empty(nv, dtype=float)
                if descending:
                    ranks[order] = np.arange(nv, 0, -1, dtype=float)
                else:
                    ranks[order] = np.arange(1, nv + 1, dtype=float)
                out[valid] = ranks
            return out

        # Current fingerprint
        # Returns, breadth, breakout: 1 = highest (descending)
        # Correlation, volatility: 1 = lowest (ascending)
        cur_ret = close_mat[end_idx] / close_mat[start_idx] - 1
        cur_ret_ranks = rank_vec(cur_ret, descending=True)
        cur_upt_ranks = rank_vec(uptrend_mat[end_idx], descending=True)
        cur_bkt_ranks = rank_vec(breakout_mat[end_idx], descending=True)
        cur_cor_ranks = rank_vec(corr_mat[end_idx])
        cur_rv_ranks = rank_vec(rv_mat[end_idx])

        # Rolling fingerprints: vectorized
        # Return: close_mat[W:] / close_mat[:-W] - 1, shape (T-W, B)
        roll_ret = close_mat[W:] / close_mat[:-W] - 1  # (T-W, B)
        roll_upt = uptrend_mat[W:]   # end-of-window snapshots
        roll_bkt = breakout_mat[W:]
        roll_cor = corr_mat[W:]
        roll_rv = rv_mat[W:]
        N_windows = roll_ret.shape[0]

        # Rank each metric for all windows: vectorized argsort
        def rank_matrix(mat, descending=False):
            """Rank across axis=1 for each row. NaN → rank B."""
            R = np.full_like(mat, float(B))
            for i in range(mat.shape[0]):
                valid = ~np.isnan(mat[i])
                nv = valid.sum()
                if nv == 0:
                    continue
                order = np.argsort(mat[i][valid])
                ranks = np.empty(nv, dtype=float)
                if descending:
                    ranks[order] = np.arange(nv, 0, -1, dtype=float)
                else:
                    ranks[order] = np.arange(1, nv + 1, dtype=float)
                R[i][valid] = ranks
            return R

        rr_ret = rank_matrix(roll_ret, descending=True)
        rr_upt = rank_matrix(roll_upt, descending=True)
        rr_bkt = rank_matrix(roll_bkt, descending=True)
        rr_cor = rank_matrix(roll_cor)
        rr_rv = rank_matrix(roll_rv)

        # Spearman: rho = 1 - 6*sum(d^2) / (B*(B^2-1))
        denom = B * (B * B - 1)
        def spearman_vec(cur_ranks, roll_ranks):
            d2 = (roll_ranks - cur_ranks[np.newaxis, :]) ** 2
            return 1.0 - 6.0 * d2.sum(axis=1) / denom

        rho_ret = spearman_vec(cur_ret_ranks, rr_ret)
        rho_upt = spearman_vec(cur_upt_ranks, rr_upt)
        rho_bkt = spearman_vec(cur_bkt_ranks, rr_bkt)
        rho_cor = spearman_vec(cur_cor_ranks, rr_cor)
        rho_rv = spearman_vec(cur_rv_ranks, rr_rv)

        # Multi-timeframe fingerprints
        MULTI_TF = {"1D": 1, "1W": 5, "1M": 21, "1Q": 63, "1Y": 252, "3Y": 756}
        rho_tf_list = []
        cur_tf_returns = {}  # for current.metrics
        for tf_label, W_t in MULTI_TF.items():
            if end_idx < W_t:
                continue  # not enough history for this timeframe
            cur_tf_ret = close_mat[end_idx] / close_mat[max(0, end_idx - W_t)] - 1
            cur_tf_ranks = rank_vec(cur_tf_ret, descending=True)
            cur_tf_returns[tf_label] = cur_tf_ret
            # Vectorized: compute roll_tf as matrix (N_windows, B) then rank
            roll_tf_mat = np.full((N_windows, B), np.nan)
            for i in range(N_windows):
                e = i + W
                s_tf = e - W_t
                if s_tf >= 0 and e < T:
                    roll_tf_mat[i] = close_mat[e] / close_mat[s_tf] - 1
            rr_tf = rank_matrix(roll_tf_mat, descending=True)
            rho_tf = spearman_vec(cur_tf_ranks, rr_tf)
            rho_tf_list.append(rho_tf)

        # Multi-timeframe absolute change for non-return metrics (vol, corr, breadth, breakout)
        METRIC_MATS = {
            'rv_ema': (rv_mat, False),        # ascending: 1 = lowest vol
            'correlation_pct': (corr_mat, False),  # ascending: 1 = lowest corr
            'uptrend_pct': (uptrend_mat, True),    # descending: 1 = highest breadth
            'breakout_pct': (breakout_mat, True),  # descending: 1 = highest breakout
        }
        cur_tf_metrics = {}   # metric_name -> {tf_label -> vec of per-basket change}
        for metric_name, (mat, desc) in METRIC_MATS.items():
            cur_tf_metrics[metric_name] = {}
            for tf_label, W_t in MULTI_TF.items():
                if end_idx < W_t:
                    continue
                prev_val = mat[max(0, end_idx - W_t)]
                cur_val = mat[end_idx]
                chg = cur_val - prev_val
                cur_tf_metrics[metric_name][tf_label] = chg
                cur_chg_ranks = rank_vec(chg, descending=desc)
                # Rolling version for similarity
                roll_chg_mat = np.full((N_windows, B), np.nan)
                for i in range(N_windows):
                    e = i + W
                    s_tf = e - W_t
                    if s_tf >= 0 and e < T:
                        roll_chg_mat[i] = mat[e] - mat[s_tf]
                rr_chg = rank_matrix(roll_chg_mat, descending=desc)
                rho_chg = spearman_vec(cur_chg_ranks, rr_chg)
                rho_tf_list.append(rho_chg)

        # Cross-basket correlation similarity
        rho_xc = np.zeros(N_windows)
        cur_xc = cross_corr_series[end_idx] if not np.isnan(cross_corr_series[end_idx]) else None
        if cur_xc is not None:
            valid_xc = cross_corr_series[~np.isnan(cross_corr_series)]
            xc_range = float(valid_xc.max() - valid_xc.min()) if len(valid_xc) > 1 else 1.0
            if xc_range == 0:
                xc_range = 1.0
            for i in range(N_windows):
                e = i + W
                if e < T and not np.isnan(cross_corr_series[e]):
                    rho_xc[i] = 1.0 - abs(cur_xc - cross_corr_series[e]) / xc_range
                else:
                    rho_xc[i] = 0.0
            has_xc = True
        else:
            has_xc = False

        # Overall similarity: average of all metrics
        n_metrics = 5 + len(rho_tf_list) + (1 if has_xc else 0)
        rho_avg = (rho_ret + rho_upt + rho_bkt + rho_cor + rho_rv)
        for rho_tf in rho_tf_list:
            rho_avg = rho_avg + rho_tf
        if has_xc:
            rho_avg = rho_avg + rho_xc
        rho_avg = rho_avg / n_metrics

        # Exclude current window ± W/2 days
        half_w = W // 2
        # Each window i corresponds to historical period [i, i+W], where end = i+W
        # The current window has start_idx at position (start_idx) in original, which maps to
        # roll index = start_idx (since roll_ret[i] = close[i+W]/close[i] - 1, so i=start_idx means period [start_idx, start_idx+W=end_idx])
        cur_roll_idx = start_idx
        excl_lo = max(0, cur_roll_idx - half_w)
        excl_hi = min(N_windows, cur_roll_idx + half_w + 1)
        rho_avg[excl_lo:excl_hi] = -999.0

        # Greedy top-N with overlap exclusion
        selected = []
        used = rho_avg.copy()
        for _ in range(top_n):
            best = int(np.argmax(used))
            if used[best] <= -999.0:
                break
            selected.append(best)
            # Exclude neighbors within W/2
            lo = max(0, best - half_w)
            hi = min(N_windows, best + half_w + 1)
            used[lo:hi] = -999.0

        # Build response
        # Current period data
        current_returns = {}
        current_metrics = {"uptrend_pct": {}, "breakout_pct": {}, "correlation_pct": {}, "rv_ema": {}}
        current_ranks = {"returns": {}, "uptrend_pct": {}, "breakout_pct": {}, "correlation_pct": {}, "rv_ema": {}}
        # Build multi-TF rank vectors for returns
        cur_tf_rank_vecs = {}
        for tf_label in MULTI_TF:
            if tf_label in cur_tf_returns:
                cur_tf_rank_vecs[tf_label] = rank_vec(cur_tf_returns[tf_label], descending=True)
        # Init multi-TF metric keys for returns
        for tf_label in MULTI_TF:
            current_metrics[f"returns_{tf_label}"] = {}
            current_ranks[f"returns_{tf_label}"] = {}
        # Init multi-TF metric keys for non-return metrics
        for metric_name, (_, desc) in METRIC_MATS.items():
            for tf_label in MULTI_TF:
                current_metrics[f"{metric_name}_{tf_label}"] = {}
                current_ranks[f"{metric_name}_{tf_label}"] = {}
        # Build rank vectors for non-return multi-TF
        cur_tf_metric_rank_vecs = {}
        for metric_name, (_, desc) in METRIC_MATS.items():
            cur_tf_metric_rank_vecs[metric_name] = {}
            for tf_label in MULTI_TF:
                if metric_name in cur_tf_metrics and tf_label in cur_tf_metrics[metric_name]:
                    cur_tf_metric_rank_vecs[metric_name][tf_label] = rank_vec(
                        cur_tf_metrics[metric_name][tf_label], descending=desc)

        current_metrics["cross_basket_corr"] = round(float(cur_xc), 4) if cur_xc is not None else None
        for j, slug in enumerate(ordered_slugs):
            r = cur_ret[j]
            current_returns[slug] = round(float(r), 6) if not np.isnan(r) else None
            current_metrics["uptrend_pct"][slug] = round(float(uptrend_mat[end_idx, j]), 2) if not np.isnan(uptrend_mat[end_idx, j]) else None
            current_metrics["breakout_pct"][slug] = round(float(breakout_mat[end_idx, j]), 2) if not np.isnan(breakout_mat[end_idx, j]) else None
            current_metrics["correlation_pct"][slug] = round(float(corr_mat[end_idx, j]), 2) if not np.isnan(corr_mat[end_idx, j]) else None
            current_metrics["rv_ema"][slug] = round(float(rv_mat[end_idx, j]), 6) if not np.isnan(rv_mat[end_idx, j]) else None
            current_ranks["returns"][slug] = int(cur_ret_ranks[j])
            current_ranks["uptrend_pct"][slug] = int(cur_upt_ranks[j])
            current_ranks["breakout_pct"][slug] = int(cur_bkt_ranks[j])
            current_ranks["correlation_pct"][slug] = int(cur_cor_ranks[j])
            current_ranks["rv_ema"][slug] = int(cur_rv_ranks[j])
            # Multi-TF returns
            for tf_label in MULTI_TF:
                if tf_label in cur_tf_returns:
                    v = cur_tf_returns[tf_label][j]
                    current_metrics[f"returns_{tf_label}"][slug] = round(float(v), 6) if not np.isnan(v) else None
                    current_ranks[f"returns_{tf_label}"][slug] = int(cur_tf_rank_vecs[tf_label][j])
                else:
                    current_metrics[f"returns_{tf_label}"][slug] = None
                    current_ranks[f"returns_{tf_label}"][slug] = None
            # Multi-TF non-return metrics (% change)
            for metric_name in METRIC_MATS:
                for tf_label in MULTI_TF:
                    if metric_name in cur_tf_metrics and tf_label in cur_tf_metrics[metric_name]:
                        v = cur_tf_metrics[metric_name][tf_label][j]
                        current_metrics[f"{metric_name}_{tf_label}"][slug] = round(float(v), 6) if not np.isnan(v) else None
                        current_ranks[f"{metric_name}_{tf_label}"][slug] = int(cur_tf_metric_rank_vecs[metric_name][tf_label][j])
                    else:
                        current_metrics[f"{metric_name}_{tf_label}"][slug] = None
                        current_ranks[f"{metric_name}_{tf_label}"][slug] = None

        analogs = []
        HORIZONS = {"1M": 21, "1Q": 63, "6M": 126, "1Y": 252}
        for sel_idx in selected:
            a_start = sel_idx
            a_end = sel_idx + W
            a_start_date = date_idx[a_start].strftime('%Y-%m-%d')
            a_end_date = date_idx[a_end].strftime('%Y-%m-%d')

            # Per-basket returns for this analog
            a_returns = {}
            for j, slug in enumerate(ordered_slugs):
                r = roll_ret[sel_idx, j]
                a_returns[slug] = round(float(r), 6) if not np.isnan(r) else None

            # Similarity breakdown
            breakdown = {
                "returns": round(float(rho_ret[sel_idx]), 4),
                "breadth": round(float(rho_upt[sel_idx]), 4),
                "breakout": round(float(rho_bkt[sel_idx]), 4),
                "correlation": round(float(rho_cor[sel_idx]), 4),
                "volatility": round(float(rho_rv[sel_idx]), 4),
            }
            tf_active = [tfl for tfl in MULTI_TF if tfl in cur_tf_returns]
            for ti, tfl in enumerate(tf_active):
                if ti < len(rho_tf_list):
                    breakdown[f"ret_{tfl}"] = round(float(rho_tf_list[ti][sel_idx]), 4)
            if has_xc:
                breakdown["cross_corr"] = round(float(rho_xc[sel_idx]), 4)

            # Forward returns (point-in-time)
            forward = {}
            for hz_label, hz_days in HORIZONS.items():
                fwd_idx = a_end + hz_days
                if fwd_idx >= T:
                    forward[hz_label] = None
                else:
                    fwd = {}
                    for j, slug in enumerate(ordered_slugs):
                        c_end = close_mat[a_end, j]
                        c_fwd = close_mat[fwd_idx, j]
                        if np.isnan(c_end) or np.isnan(c_fwd) or c_end == 0:
                            fwd[slug] = None
                        else:
                            fwd[slug] = round(float(c_fwd / c_end - 1), 6)
                    forward[hz_label] = fwd

            # Forward series: daily cumulative returns per basket (up to 252 days)
            max_fwd = min(252, T - a_end - 1)
            fwd_dates = []
            fwd_baskets = {slug: [] for slug in ordered_slugs}
            for d in range(1, max_fwd + 1):
                fwd_dates.append(date_idx[a_end + d].strftime('%Y-%m-%d'))
                for j, slug in enumerate(ordered_slugs):
                    c_base = close_mat[a_end, j]
                    c_fwd = close_mat[a_end + d, j]
                    if np.isnan(c_base) or np.isnan(c_fwd) or c_base == 0:
                        fwd_baskets[slug].append(None)
                    else:
                        fwd_baskets[slug].append(round(float(c_fwd / c_base - 1), 6))

            analogs.append({
                "start": a_start_date,
                "end": a_end_date,
                "similarity": round(float(rho_avg[sel_idx]) if rho_avg[sel_idx] > -999 else float((rho_ret[sel_idx] + rho_upt[sel_idx] + rho_bkt[sel_idx] + rho_cor[sel_idx] + rho_rv[sel_idx]) / 5), 4),
                "similarity_breakdown": breakdown,
                "returns": a_returns,
                "forward": forward,
                "forward_series": {"dates": fwd_dates, "baskets": fwd_baskets},
            })

        # Filter by threshold
        if threshold > 0:
            analogs = [a for a in analogs if a["similarity"] >= threshold]

        # Aggregate stats across analogs
        AGG_HORIZONS = {"1M": 21, "1Q": 63, "6M": 126, "1Y": 252}
        aggregate = {}
        for hz_label, hz_days in AGG_HORIZONS.items():
            # Collect per-basket forward returns at this horizon
            per_basket = {slug: [] for slug in ordered_slugs}
            all_vals = []
            for a in analogs:
                fwd = a["forward"].get(hz_label)
                if fwd is None:
                    continue
                for slug in ordered_slugs:
                    v = fwd.get(slug)
                    if v is not None:
                        per_basket[slug].append(v)
                        all_vals.append(v)
            if all_vals:
                arr = np.array(all_vals)
                aggregate[hz_label] = {
                    "mean": round(float(np.mean(arr)), 6),
                    "median": round(float(np.median(arr)), 6),
                    "min": round(float(np.min(arr)), 6),
                    "max": round(float(np.max(arr)), 6),
                    "std": round(float(np.std(arr)), 6),
                    "count": len(all_vals),
                    "per_basket": {},
                }
                for slug in ordered_slugs:
                    vals = per_basket[slug]
                    if vals:
                        ba = np.array(vals)
                        aggregate[hz_label]["per_basket"][slug] = {
                            "mean": round(float(np.mean(ba)), 6),
                            "median": round(float(np.median(ba)), 6),
                            "min": round(float(np.min(ba)), 6),
                            "max": round(float(np.max(ba)), 6),
                            "std": round(float(np.std(ba)), 6),
                            "count": len(vals),
                        }
            else:
                aggregate[hz_label] = None

        return {
            "current": {
                "start": start,
                "end": end,
                "returns": current_returns,
                "metrics": current_metrics,
                "ranks": current_ranks,
                "basket_count": B,
            },
            "analogs": analogs,
            "aggregate": aggregate,
            "date_range": date_range,
        }

    # ── mode=query: condition-based historical search ──
    if mode == "query":
        try:
            conds = json.loads(conditions) if conditions else []
        except Exception:
            conds = []

        # Load all baskets (respect group filter for ranking context)
        slugs = []
        for slug in sorted(all_slugs):
            cat = _categorize(slug)
            if group != "all":
                if group == "themes" and cat != "theme": continue
                if group == "sectors" and cat != "sector": continue
                if group == "industries" and cat != "industry": continue
            slugs.append(slug)

        q_live_breadth = _compute_live_breadth_batch(slugs) if live_closes else {}

        COLS = ['Date', 'Close', 'Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA']
        basket_frames = {}
        for slug in slugs:
            pf = _find_basket_parquet(slug)
            if not pf: continue
            try:
                df = pd.read_parquet(pf, columns=COLS)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date').drop_duplicates(subset=['Date'])
                if slug in live_closes:
                    ld, lc = live_closes[slug]
                    if ld not in df['Date'].values:
                        lb = q_live_breadth.get(slug, {})
                        prev_c = df['Close'].iloc[-1]
                        prev_rv = df['RV_EMA'].iloc[-1]
                        if pd.notna(prev_c) and prev_c != 0 and pd.notna(prev_rv):
                            l_rv = 2.0/11.0 * abs(lc/prev_c - 1) + (1 - 2.0/11.0) * prev_rv
                        else:
                            l_rv = np.nan
                        df = pd.concat([df, pd.DataFrame({'Date': [ld], 'Close': [lc],
                            'Uptrend_Pct': [lb.get('Uptrend_Pct', np.nan)],
                            'Breakout_Pct': [lb.get('Breakout_Pct', np.nan)],
                            'Correlation_Pct': [lb.get('Correlation_Pct', np.nan)],
                            'RV_EMA': [l_rv]})],
                            ignore_index=True).sort_values('Date')
                basket_frames[slug] = df.set_index('Date')
            except Exception:
                continue

        if len(basket_frames) < 3:
            return {"matches": [], "aggregate": {}, "match_count": 0, "date_range": date_range}

        ordered_slugs = sorted(basket_frames.keys())
        B = len(ordered_slugs)
        all_dates = sorted(set().union(*(df.index for df in basket_frames.values())))
        date_idx = pd.DatetimeIndex(all_dates)
        T = len(date_idx)
        slug_to_j = {s: j for j, s in enumerate(ordered_slugs)}

        close_mat = np.full((T, B), np.nan)
        q_uptrend_mat = np.full((T, B), np.nan)
        q_breakout_mat = np.full((T, B), np.nan)
        q_corr_mat = np.full((T, B), np.nan)
        q_rv_mat = np.full((T, B), np.nan)

        for j, slug in enumerate(ordered_slugs):
            df = basket_frames[slug]
            idxs = date_idx.searchsorted(df.index)
            valid = idxs < T
            idxs = idxs[valid]
            close_mat[idxs, j] = df['Close'].values[valid]
            q_uptrend_mat[idxs, j] = df['Uptrend_Pct'].values[valid]
            q_breakout_mat[idxs, j] = df['Breakout_Pct'].values[valid]
            q_corr_mat[idxs, j] = df['Correlation_Pct'].values[valid]
            q_rv_mat[idxs, j] = df['RV_EMA'].values[valid]

        for mat in [close_mat, q_uptrend_mat, q_breakout_mat, q_corr_mat, q_rv_mat]:
            for j in range(B):
                col = mat[:, j]
                m = np.isnan(col)
                if m.all(): continue
                idx_arr = np.where(~m, np.arange(T), 0)
                np.maximum.accumulate(idx_arr, out=idx_arr)
                mat[:, j] = col[idx_arr]
                first_valid = np.argmax(~m)
                mat[:first_valid, j] = np.nan

        # Lookback return matrices
        LOOKBACKS = {"return_1D": 1, "return_1W": 5, "return_1M": 21, "return_1Q": 63, "return_1Y": 252}
        ret_mats = {}
        for key, lb in LOOKBACKS.items():
            mat = np.full((T, B), np.nan)
            if lb < T:
                mat[lb:] = close_mat[lb:] / close_mat[:-lb] - 1
            ret_mats[key] = mat

        # Change matrices for non-return metrics (absolute difference over lookback)
        NR_MATS = {
            "uptrend_pct": q_uptrend_mat,
            "breakout_pct": q_breakout_mat,
            "correlation_pct": q_corr_mat,
            "rv_ema": q_rv_mat,
        }
        NR_LOOKBACKS = {"1D": 1, "1W": 5, "1M": 21, "1Q": 63, "1Y": 252}
        nr_change_mats = {}
        for nr_name, nr_mat in NR_MATS.items():
            for tf, lb in NR_LOOKBACKS.items():
                key = f"{nr_name}_{tf}"
                mat = np.full((T, B), np.nan)
                if lb < T:
                    mat[lb:] = nr_mat[lb:] - nr_mat[:-lb]
                nr_change_mats[key] = mat

        metric_mats = {
            **ret_mats,
            **nr_change_mats,
            # Keep raw level versions for backward compat
            "uptrend_pct": q_uptrend_mat,
            "breakout_pct": q_breakout_mat,
            "correlation_pct": q_corr_mat,
            "rv_ema": q_rv_mat,
        }

        cat_map = {"sectors": "sector", "themes": "theme", "industries": "industry"}

        # Pre-compute rank matrices (rank 1 = lowest value, rank B = highest)
        rank_mats = {}
        for mkey, mat in metric_mats.items():
            R = np.full((T, B), np.nan)
            for t in range(T):
                row = mat[t]
                v = ~np.isnan(row)
                nv = v.sum()
                if nv == 0: continue
                order = np.argsort(row[v])
                ranks = np.empty(nv, dtype=float)
                ranks[order] = np.arange(1, nv + 1, dtype=float)
                R[t, v] = ranks
            rank_mats[mkey] = R

        # Evaluate conditions
        mask = np.ones(T, dtype=bool)
        mask[:max(252, 5)] = False  # need lookback history

        for cond in conds:
            cbasket = cond.get("basket", "")
            cmetric = cond.get("metric", "return_1D")
            cop = cond.get("operator", "negative")
            cval = float(cond.get("value", 0))

            mat = metric_mats.get(cmetric)
            rmat = rank_mats.get(cmetric)
            if mat is None:
                continue

            if cbasket.startswith("*"):
                # Group condition: ALL baskets in group must satisfy
                gkey = cbasket[1:]
                cat_val = cat_map.get(gkey, gkey)
                group_js = [slug_to_j[s] for s in ordered_slugs if _categorize(s) == cat_val]

                for j in group_js:
                    col = mat[:, j]
                    nan_mask = np.isnan(col)
                    if cop == "positive":
                        mask &= (col > 0) & ~nan_mask
                    elif cop == "negative":
                        mask &= (col < 0) & ~nan_mask
                    elif cop == "above":
                        mask &= (col > cval) & ~nan_mask
                    elif cop == "below":
                        mask &= (col < cval) & ~nan_mask
                    elif cop == "top_n" and rmat is not None:
                        nv_per_date = (~np.isnan(mat)).sum(axis=1)
                        mask &= (rmat[:, j] > nv_per_date - int(cval)) & ~np.isnan(rmat[:, j])
                    elif cop == "bottom_n" and rmat is not None:
                        mask &= (rmat[:, j] <= int(cval)) & ~np.isnan(rmat[:, j])
            else:
                j = slug_to_j.get(cbasket)
                if j is None:
                    continue
                col = mat[:, j]
                nan_mask = np.isnan(col)

                if cop == "positive":
                    mask &= (col > 0) & ~nan_mask
                elif cop == "negative":
                    mask &= (col < 0) & ~nan_mask
                elif cop == "above":
                    mask &= (col > cval) & ~nan_mask
                elif cop == "below":
                    mask &= (col < cval) & ~nan_mask
                elif cop == "top_n" and rmat is not None:
                    nv_per_date = (~np.isnan(mat)).sum(axis=1)
                    mask &= (rmat[:, j] > nv_per_date - int(cval)) & ~np.isnan(rmat[:, j])
                elif cop == "bottom_n" and rmat is not None:
                    mask &= (rmat[:, j] <= int(cval)) & ~np.isnan(rmat[:, j])

        mask[-5:] = False  # need forward data

        # Deduplicate: skip matches within 5 days of each other
        match_indices_raw = np.where(mask)[0]
        deduped = []
        last_idx = -10
        for idx in match_indices_raw:
            if idx - last_idx >= 5:
                deduped.append(idx)
                last_idx = idx
        match_indices = deduped[:100]

        # Build matches with forward returns
        Q_HORIZONS = {"1W": 5, "1M": 21, "1Q": 63, "6M": 126, "1Y": 252}
        matches = []
        for idx in match_indices:
            forward = {}
            for hz_label, hz_days in Q_HORIZONS.items():
                fwd_idx = idx + hz_days
                if fwd_idx >= T:
                    forward[hz_label] = None
                else:
                    fwd = {}
                    for j, slug in enumerate(ordered_slugs):
                        c_now = close_mat[idx, j]
                        c_fwd = close_mat[fwd_idx, j]
                        if np.isnan(c_now) or np.isnan(c_fwd) or c_now == 0:
                            fwd[slug] = None
                        else:
                            fwd[slug] = round(float(c_fwd / c_now - 1), 6)
                    forward[hz_label] = fwd

            # Forward series (up to 252 days)
            max_fwd = min(252, T - idx - 1)
            fwd_dates = []
            fwd_baskets = {slug: [] for slug in ordered_slugs}
            for d in range(1, max_fwd + 1):
                fwd_dates.append(date_idx[idx + d].strftime('%Y-%m-%d'))
                for j, slug in enumerate(ordered_slugs):
                    c_base = close_mat[idx, j]
                    c_fwd = close_mat[idx + d, j]
                    if np.isnan(c_base) or np.isnan(c_fwd) or c_base == 0:
                        fwd_baskets[slug].append(None)
                    else:
                        fwd_baskets[slug].append(round(float(c_fwd / c_base - 1), 6))

            matches.append({
                "date": date_idx[idx].strftime('%Y-%m-%d'),
                "forward": forward,
                "forward_series": {"dates": fwd_dates, "baskets": fwd_baskets},
            })

        # Aggregate
        aggregate = {}
        for hz_label in Q_HORIZONS:
            per_basket = {slug: [] for slug in ordered_slugs}
            all_vals = []
            for m in matches:
                fwd = m["forward"].get(hz_label)
                if not fwd: continue
                for slug in ordered_slugs:
                    v = fwd.get(slug)
                    if v is not None:
                        per_basket[slug].append(v)
                        all_vals.append(v)
            if all_vals:
                arr = np.array(all_vals)
                aggregate[hz_label] = {
                    "mean": round(float(np.mean(arr)), 6),
                    "median": round(float(np.median(arr)), 6),
                    "min": round(float(np.min(arr)), 6),
                    "max": round(float(np.max(arr)), 6),
                    "std": round(float(np.std(arr)), 6),
                    "count": len(all_vals),
                    "per_basket": {},
                }
                for slug in ordered_slugs:
                    vals = per_basket[slug]
                    if vals:
                        ba = np.array(vals)
                        aggregate[hz_label]["per_basket"][slug] = {
                            "mean": round(float(np.mean(ba)), 6),
                            "median": round(float(np.median(ba)), 6),
                            "min": round(float(np.min(ba)), 6),
                            "max": round(float(np.max(ba)), 6),
                            "std": round(float(np.std(ba)), 6),
                            "count": len(vals),
                        }
            else:
                aggregate[hz_label] = None

        return {
            "matches": matches,
            "match_count": len(matches),
            "total_searched": int(mask.sum() + (~mask).sum()),
            "aggregate": aggregate,
            "baskets": ordered_slugs,
            "date_range": date_range,
        }

    # mode=cumulative: daily cumulative return series for all baskets
    if mode == "cumulative":
        if not start and not end and global_max:
            end_dt = global_max
            start_dt = end_dt - pd.DateOffset(years=1)
            start = start_dt.strftime('%Y-%m-%d')
            end = end_dt.strftime('%Y-%m-%d')

        # Determine which column to read based on metric
        metric_col_map = {"returns": "Close", "volatility": "RV_EMA", "correlation": "Correlation_Pct"}
        data_col = metric_col_map.get(metric, "Close")
        read_cols = ['Date', data_col] if data_col != 'Close' else ['Date', 'Close']

        # Collect series per basket
        basket_series = {}
        for slug in sorted(all_slugs):
            cat = _categorize(slug)
            if group != "all":
                if group == "themes" and cat != "theme": continue
                if group == "sectors" and cat != "sector": continue
                if group == "industries" and cat != "industry": continue
            pf = _find_basket_parquet(slug)
            if not pf: continue
            try:
                df = pd.read_parquet(pf, columns=read_cols)
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.sort_values('Date').dropna(subset=[data_col])
                if data_col == 'Close' and slug in live_closes:
                    live_date, live_close = live_closes[slug]
                    if live_date not in df['Date'].values:
                        live_row = pd.DataFrame({'Date': [live_date], 'Close': [live_close]})
                        df = pd.concat([df, live_row], ignore_index=True).sort_values('Date')
                if end:
                    df = df[df['Date'] <= pd.Timestamp(end)]
                if start:
                    start_ts = pd.Timestamp(start)
                    before = df[df['Date'] < start_ts]
                    in_range = df[df['Date'] >= start_ts]
                    if not before.empty:
                        df = pd.concat([before.iloc[[-1]], in_range])
                    else:
                        df = in_range
                if len(df) < 2: continue
                base_val = float(df.iloc[0][data_col])
                if base_val == 0: continue
                df = df.iloc[1:]  # drop anchor row
                vals = [(float(v) / base_val) - 1 for v in df[data_col]]
                dates = [d.strftime('%Y-%m-%d') for d in df['Date']]
                basket_series[slug] = {"name": slug, "group": cat, "dates": dates, "values": vals}
            except Exception:
                continue

        # Build unified date index
        all_dates_set = set()
        for bs in basket_series.values():
            all_dates_set.update(bs["dates"])
        unified_dates = sorted(all_dates_set)

        series = []
        for bs in basket_series.values():
            date_to_val = dict(zip(bs["dates"], bs["values"]))
            vals = [date_to_val.get(d, None) for d in unified_dates]
            series.append({"name": bs["name"], "group": bs["group"], "values": vals})

        return {"dates": unified_dates, "series": series, "date_range": date_range}

    # mode=period: one return per basket
    # Default to 1Y range if no dates specified
    if not start and not end and global_max:
        end_dt = global_max
        start_dt = end_dt - pd.DateOffset(years=1)
        start = start_dt.strftime('%Y-%m-%d')
        end = end_dt.strftime('%Y-%m-%d')

    metric_col_map_p = {"returns": "Close", "volatility": "RV_EMA", "correlation": "Correlation_Pct"}
    data_col_p = metric_col_map_p.get(metric, "Close")
    read_cols_p = ['Date', data_col_p] if data_col_p != 'Close' else ['Date', 'Close']

    baskets = []
    for slug in sorted(all_slugs):
        cat = _categorize(slug)
        if group != "all":
            if group == "themes" and cat != "theme":
                continue
            if group == "sectors" and cat != "sector":
                continue
            if group == "industries" and cat != "industry":
                continue
        pf = _find_basket_parquet(slug)
        if not pf:
            continue
        try:
            df = pd.read_parquet(pf, columns=read_cols_p)
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date').dropna(subset=[data_col_p])
            if data_col_p == 'Close' and slug in live_closes:
                live_date, live_close = live_closes[slug]
                if live_date not in df['Date'].values:
                    live_row = pd.DataFrame({'Date': [live_date], 'Close': [live_close]})
                    df = pd.concat([df, live_row], ignore_index=True).sort_values('Date')
            if end:
                df = df[df['Date'] <= pd.Timestamp(end)]
            # Grab anchor row before start for % change calculation
            if start:
                start_ts = pd.Timestamp(start)
                before = df[df['Date'] < start_ts]
                in_range = df[df['Date'] >= start_ts]
                if not before.empty:
                    df = pd.concat([before.iloc[[-1]], in_range])
                else:
                    df = in_range
            if len(df) < 2:
                continue
            first_val = float(df.iloc[0][data_col_p])
            last_val = float(df.iloc[-1][data_col_p])
            if first_val == 0:
                continue
            ret = (last_val / first_val) - 1
            baskets.append({"name": slug, "group": cat, "return": round(ret, 6)})
        except Exception:
            continue

    actual_range = {"start": start, "end": end}
    resp = {"baskets": baskets, "date_range": date_range, "actual_range": actual_range}
    if _trading_dates:
        resp["trading_dates"] = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else pd.Timestamp(d).strftime('%Y-%m-%d') for d in _trading_dates]
    return resp

@app.get("/api/baskets/{basket_name}")
def get_basket_data(basket_name: str):
    basket_file = _find_basket_parquet(basket_name)
    if not basket_file:
        raise HTTPException(status_code=404, detail=f"Basket file not found for {basket_name}")
    try:
        df = pd.read_parquet(basket_file)
        df['Date'] = pd.to_datetime(df['Date'])

        # Merge live basket data for today's candle with recomputed signals (skip if stale)
        live_basket_df = _read_live_parquet(LIVE_BASKET_SIGNALS_FILE)
        if live_basket_df is not None and _live_is_current(live_basket_df, df['Date'].max()):
            name_col = 'BasketName' if 'BasketName' in live_basket_df.columns else 'Basket'
            basket_name_spaced = basket_name.replace('_', ' ')
            live_row = live_basket_df[live_basket_df[name_col].str.endswith(basket_name_spaced)]
            if not live_row.empty:
                live_row = live_row.copy()
                live_row['Date'] = pd.to_datetime(live_row['Date'])
                live_row = live_row.drop(columns=[name_col])

                # Recompute basket-level signals (pivots, targets) on combined OHLC
                ohlc_cols = [c for c in ['Date', 'Open', 'High', 'Low', 'Close', 'Volume'] if c in df.columns]
                live_ohlc = live_row[[c for c in ohlc_cols if c in live_row.columns]].copy()
                if 'Volume' not in live_ohlc.columns:
                    live_ohlc['Volume'] = 0
                combined_ohlc = pd.concat([df[ohlc_cols], live_ohlc], ignore_index=True)
                combined_ohlc = combined_ohlc.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')

                ticker_label = df['Ticker'].iloc[0] if 'Ticker' in df.columns and not df['Ticker'].isna().all() else basket_name.upper()
                recomputed = signals_engine._build_signals_from_df(combined_ohlc.set_index('Date'), ticker_label)

                if recomputed is not None and not recomputed.empty:
                    # Take only the live bar's recomputed signals
                    live_computed = recomputed.iloc[[-1]].copy()

                    # Compute breadth metrics for the live bar
                    breadth = _compute_live_breadth(basket_name)
                    for col, val in breadth.items():
                        live_computed[col] = val

                    df = pd.concat([df, live_computed], ignore_index=True)
                    df = df.drop_duplicates(subset=['Date'], keep='last')
                else:
                    # Fallback: just append OHLC
                    df = pd.concat([df, live_row], ignore_index=True)
                    df = df.drop_duplicates(subset=['Date'], keep='last')

        current_weights = get_basket_weights_from_contributions(basket_name)
        if current_weights:
            tickers = sorted([{"symbol": s, "weight": float(w)} for s, w in current_weights.items()], key=lambda x: x['weight'], reverse=True)
        else:
            latest_universe = get_latest_universe_tickers(basket_name)
            tickers = [{"symbol": symbol, "weight": 0.0} for symbol in latest_universe]

        return {"chart_data": clean_data_for_json(df), "tickers": tickers}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tickers")
def list_tickers():
    if TOP_500_FILE.exists():
        try:
            with open(TOP_500_FILE, 'r') as f:
                data = json.load(f)
                qs = sorted(data.keys())
                if qs: return sorted(list(data[qs[-1]]))
        except: pass
    if not INDIVIDUAL_SIGNALS_FILE.exists(): return []
    try:
        df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker'])
        return sorted(df['Ticker'].dropna().unique().tolist())
    except: raise HTTPException(status_code=500)

@app.get("/api/tickers/quarters")
def list_tickers_by_quarter():
    """Return all quarters and their ticker universes from top500stocks.json."""
    if not TOP_500_FILE.exists():
        return {"quarters": [], "tickers_by_quarter": {}}
    try:
        with open(TOP_500_FILE, 'r') as f:
            data = json.load(f)
        quarters = sorted(data.keys(), reverse=True)
        tickers_by_quarter = {q: sorted(data[q]) for q in quarters}
        return {"quarters": quarters, "tickers_by_quarter": tickers_by_quarter}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/etfs")
def list_etfs():
    """Return sorted ETF ticker list from the latest quarter in the ETF universe cache."""
    if not ETF_UNIVERSES_FILE.exists():
        return []
    try:
        with open(ETF_UNIVERSES_FILE, 'r') as f:
            data = json.load(f)
        qs = sorted(data.keys())
        if qs:
            return sorted(data[qs[-1]])
        return []
    except Exception:
        return []

@app.get("/api/etfs/quarters")
def list_etfs_by_quarter():
    """Return all quarters and their ETF universes from etf_universes_50.json."""
    if not ETF_UNIVERSES_FILE.exists():
        return {"quarters": [], "etfs_by_quarter": {}}
    try:
        with open(ETF_UNIVERSES_FILE, 'r') as f:
            data = json.load(f)
        quarters = sorted(data.keys(), reverse=True)
        etfs_by_quarter = {q: sorted(data[q]) for q in quarters}
        return {"quarters": quarters, "etfs_by_quarter": etfs_by_quarter}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ticker-names")
def get_ticker_names():
    """Return ticker → security name mapping."""
    if not TICKER_NAMES_FILE.exists():
        return {}
    try:
        with open(TICKER_NAMES_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}

@app.get("/api/live-signals")
def list_live_signal_tickers():
    """Return sorted list of tickers where a signal fires TODAY (recomputed with live prices)."""
    if not INDIVIDUAL_SIGNALS_FILE.exists():
        return []
    try:
        # Universe filter
        universe = None
        if TOP_500_FILE.exists():
            try:
                with open(TOP_500_FILE, 'r') as f:
                    data = json.load(f)
                    qs = sorted(data.keys())
                    if qs:
                        universe = set(data[qs[-1]])
            except:
                pass

        # Only read columns needed by _build_signals_next_row (avoids loading 50+ unused columns)
        _SIGNAL_COLS = [
            'Ticker', 'Date', 'Close',
            'RV_EMA', 'Trend', 'Resistance_Pivot', 'Support_Pivot',
            'Rotation_ID', 'Up_Range_EMA', 'Down_Range_EMA', 'Up_Range', 'Down_Range',
            'Rotation_Open', 'Upper_Target', 'Lower_Target',
            'BTFD_Triggered', 'STFR_Triggered',
            'Is_Breakout', 'Is_Breakdown', 'Is_Breakout_Sequence',
        ]
        cutoff = pd.Timestamp(datetime.now() - timedelta(days=14))
        df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=_SIGNAL_COLS,
                             filters=[('Date', '>=', cutoff)])
        df = df.sort_values('Date')
        latest = df.groupby('Ticker').tail(1)

        # Exclude delisted tickers
        max_date = latest['Date'].max()
        latest = latest[latest['Date'] >= max_date]
        if universe is not None:
            latest = latest[latest['Ticker'].isin(universe)]

        # Read live OHLC (skip if stale — market closed, Norgate already updated)
        live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
        if live_df is None or live_df.empty or not _live_is_current(live_df, max_date):
            return []  # No live data or stale → no live signals

        live_ohlc = {}
        for _, lr in live_df.iterrows():
            t = lr.get('Ticker')
            if t and pd.notna(lr.get('Close')):
                live_ohlc[t] = {
                    'Close': float(lr['Close']),
                    'Open': float(lr['Open']) if pd.notna(lr.get('Open')) else None,
                    'High': float(lr['High']) if pd.notna(lr.get('High')) else None,
                    'Low': float(lr['Low']) if pd.notna(lr.get('Low')) else None,
                }

        now = datetime.now()

        signal_flag_to_name = {
            'Is_Up_Rotation': 'Up_Rot', 'Is_Down_Rotation': 'Down_Rot',
            'Is_Breakout': 'Breakout', 'Is_Breakdown': 'Breakdown',
            'Is_BTFD': 'BTFD', 'Is_STFR': 'STFR',
        }

        results = []
        for _, row in latest.iterrows():
            ticker = row['Ticker']
            if ticker not in live_ohlc:
                continue
            ohlc = live_ohlc[ticker]
            new_row = signals_engine._build_signals_next_row(
                row, ohlc['Close'], now,
                live_high=ohlc.get('High'),
                live_low=ohlc.get('Low'),
                live_open=ohlc.get('Open'),
            )
            if new_row is None:
                continue
            fired = [name for flag_col, name in signal_flag_to_name.items()
                     if bool(new_row.get(flag_col, False))]
            if fired:
                results.append({"symbol": ticker, "signals": fired})

        # Also process ETF signals from separate parquet
        if ETF_SIGNALS_FILE.exists():
            try:
                etf_universe = None
                if ETF_UNIVERSES_FILE.exists():
                    with open(ETF_UNIVERSES_FILE, 'r') as f:
                        etf_data = json.load(f)
                        etf_qs = sorted(etf_data.keys())
                        if etf_qs:
                            etf_universe = set(etf_data[etf_qs[-1]])
                etf_df = pd.read_parquet(ETF_SIGNALS_FILE, columns=_SIGNAL_COLS,
                                         filters=[('Date', '>=', cutoff)])
                etf_df = etf_df.sort_values('Date')
                etf_latest = etf_df.groupby('Ticker').tail(1)
                etf_max_date = etf_latest['Date'].max()
                etf_latest = etf_latest[etf_latest['Date'] >= etf_max_date]
                if etf_universe is not None:
                    etf_latest = etf_latest[etf_latest['Ticker'].isin(etf_universe)]

                etf_live_df = _read_live_parquet(ETF_LIVE_SIGNALS_FILE)
                if etf_live_df is not None and not etf_live_df.empty and _live_is_current(etf_live_df, etf_max_date):
                    etf_live_ohlc = {}
                    for _, lr in etf_live_df.iterrows():
                        t = lr.get('Ticker')
                        if t and pd.notna(lr.get('Close')):
                            etf_live_ohlc[t] = {
                                'Close': float(lr['Close']),
                                'Open': float(lr['Open']) if pd.notna(lr.get('Open')) else None,
                                'High': float(lr['High']) if pd.notna(lr.get('High')) else None,
                                'Low': float(lr['Low']) if pd.notna(lr.get('Low')) else None,
                            }
                    for _, row in etf_latest.iterrows():
                        ticker = row['Ticker']
                        if ticker not in etf_live_ohlc:
                            continue
                        ohlc = etf_live_ohlc[ticker]
                        new_row = signals_engine._build_signals_next_row(
                            row, ohlc['Close'], now,
                            live_high=ohlc.get('High'),
                            live_low=ohlc.get('Low'),
                            live_open=ohlc.get('Open'),
                        )
                        if new_row is None:
                            continue
                        fired = [name for flag_col, name in signal_flag_to_name.items()
                                 if bool(new_row.get(flag_col, False))]
                        if fired:
                            results.append({"symbol": ticker, "signals": fired})
            except Exception:
                logger.exception("live-signals ETF processing failed")

        results.sort(key=lambda x: x["symbol"])
        return results
    except Exception as e:
        logger.exception("live-signals failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ticker-signals")
def get_ticker_signals():
    """Return per-ticker signal summary: LT trend, ST trend, mean reversion, and daily % change."""
    if not INDIVIDUAL_SIGNALS_FILE.exists():
        return {}
    try:
        cols = ['Ticker', 'Date', 'Close', 'Volume', 'Trend', 'Is_Breakout_Sequence',
                'Is_BTFD', 'Is_STFR', 'BTFD_Entry_Price', 'BTFD_Exit_Date',
                'STFR_Entry_Price', 'STFR_Exit_Date']
        cutoff = pd.Timestamp(datetime.now() - timedelta(days=14))
        df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=cols,
                             filters=[('Date', '>=', cutoff)])
        df = df.sort_values(['Ticker', 'Date'])

        # Track last BTFD/STFR entry dates per ticker
        btfd_last_entry = {}
        stfr_last_entry = {}
        for _, r in df.iterrows():
            t = r['Ticker']
            if r.get('Is_BTFD', False):
                btfd_last_entry[t] = r['Date']
            if r.get('Is_STFR', False):
                stfr_last_entry[t] = r['Date']

        # Get last 2 rows per ticker for pct_change calculation
        last2 = df.groupby('Ticker').tail(2)

        result = {}
        for ticker, group in last2.groupby('Ticker'):
            rows = group.sort_values('Date')
            final = rows.iloc[-1]

            # LT Trend from Is_Breakout_Sequence
            lt = None
            val = final.get('Is_Breakout_Sequence')
            if pd.notna(val):
                lt = 'BO' if bool(val) else 'BD'

            # ST Trend from Trend
            st = None
            trend_val = final.get('Trend')
            if pd.notna(trend_val):
                st = 'Up' if int(trend_val) == 1 else 'Dn'

            # Mean Reversion (open trade state from Entry_Price/Exit_Date)
            mr = None
            btfd_open = pd.notna(final.get('BTFD_Entry_Price')) and pd.isna(final.get('BTFD_Exit_Date'))
            stfr_open = pd.notna(final.get('STFR_Entry_Price')) and pd.isna(final.get('STFR_Exit_Date'))
            if btfd_open and stfr_open:
                bd = btfd_last_entry.get(ticker)
                sd = stfr_last_entry.get(ticker)
                mr = 'STFR' if sd and (not bd or sd > bd) else 'BTFD'
            elif btfd_open:
                mr = 'BTFD'
            elif stfr_open:
                mr = 'STFR'

            # Pct change from last 2 closes
            pct = None
            if len(rows) >= 2:
                prev_close = rows.iloc[-2]['Close']
                curr_close = final['Close']
                if pd.notna(prev_close) and pd.notna(curr_close) and prev_close != 0:
                    pct = round(float(curr_close / prev_close - 1) * 100, 2)

            # Dollar volume — use prior row's Volume if latest (live) row has Volume=0
            dv = None
            vol = final.get('Volume')
            if (not vol or vol == 0) and len(rows) >= 2:
                vol = rows.iloc[-2].get('Volume')
                close_for_dv = rows.iloc[-2].get('Close')
            else:
                close_for_dv = final.get('Close')
            if pd.notna(close_for_dv) and pd.notna(vol) and vol > 0:
                dv = round(float(close_for_dv) * float(vol))

            # Last price
            last_price = None
            if pd.notna(final.get('Close')):
                last_price = round(float(final['Close']), 2)

            result[ticker] = {
                'lt_trend': lt,
                'st_trend': st,
                'mean_rev': mr,
                'pct_change': float(pct) if pct is not None else None,
                'dollar_vol': int(dv) if dv is not None else None,
                'last_price': last_price,
            }

        # Override pct_change and last_price with live data if available and newer than Norgate
        live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
        if live_df is not None and _live_is_current(live_df, pd.to_datetime(df['Date']).max()):
            for _, lr in live_df.iterrows():
                t = lr.get('Ticker')
                if t and pd.notna(lr.get('Close')) and t in result:
                    live_close = float(lr['Close'])
                    result[t]['last_price'] = round(live_close, 2)
                    ticker_rows = last2[last2['Ticker'] == t].sort_values('Date')
                    if len(ticker_rows) >= 1:
                        prev_close = ticker_rows.iloc[-1]['Close']
                        if pd.notna(prev_close) and prev_close != 0:
                            result[t]['pct_change'] = round(float(live_close / prev_close - 1) * 100, 2)

        # Merge ETF signals from separate parquet
        if ETF_SIGNALS_FILE.exists():
            try:
                etf_df = pd.read_parquet(ETF_SIGNALS_FILE, columns=cols,
                                         filters=[('Date', '>=', cutoff)])
                etf_df = etf_df.sort_values(['Ticker', 'Date'])
                etf_last2 = etf_df.groupby('Ticker').tail(2)
                for ticker, group in etf_last2.groupby('Ticker'):
                    rows_e = group.sort_values('Date')
                    final = rows_e.iloc[-1]
                    lt = None
                    val = final.get('Is_Breakout_Sequence')
                    if pd.notna(val):
                        lt = 'BO' if bool(val) else 'BD'
                    st = None
                    trend_val = final.get('Trend')
                    if pd.notna(trend_val):
                        st = 'Up' if int(trend_val) == 1 else 'Dn'
                    mr = None
                    btfd_open = pd.notna(final.get('BTFD_Entry_Price')) and pd.isna(final.get('BTFD_Exit_Date'))
                    stfr_open = pd.notna(final.get('STFR_Entry_Price')) and pd.isna(final.get('STFR_Exit_Date'))
                    if btfd_open and stfr_open:
                        mr = 'STFR'
                    elif btfd_open:
                        mr = 'BTFD'
                    elif stfr_open:
                        mr = 'STFR'
                    pct = None
                    if len(rows_e) >= 2:
                        prev_close = rows_e.iloc[-2]['Close']
                        curr_close = final['Close']
                        if pd.notna(prev_close) and pd.notna(curr_close) and prev_close != 0:
                            pct = round(float(curr_close / prev_close - 1) * 100, 2)
                    # Use prior row's Volume if latest (live) row has Volume=0
                    dv = None
                    vol = final.get('Volume')
                    if (not vol or vol == 0) and len(rows_e) >= 2:
                        vol = rows_e.iloc[-2].get('Volume')
                        close_for_dv = rows_e.iloc[-2].get('Close')
                    else:
                        close_for_dv = final.get('Close')
                    if pd.notna(close_for_dv) and pd.notna(vol) and vol > 0:
                        dv = round(float(close_for_dv) * float(vol))
                    last_price = round(float(final['Close']), 2) if pd.notna(final.get('Close')) else None
                    result[ticker] = {
                        'lt_trend': lt, 'st_trend': st, 'mean_rev': mr,
                        'pct_change': float(pct) if pct is not None else None,
                        'dollar_vol': int(dv) if dv is not None else None,
                        'last_price': last_price,
                    }
                # Override with ETF live data
                etf_live_df = _read_live_parquet(ETF_LIVE_SIGNALS_FILE)
                if etf_live_df is not None and _live_is_current(etf_live_df, pd.to_datetime(etf_df['Date']).max()):
                    for _, lr in etf_live_df.iterrows():
                        t = lr.get('Ticker')
                        if t and pd.notna(lr.get('Close')) and t in result:
                            live_close = float(lr['Close'])
                            result[t]['last_price'] = round(live_close, 2)
                            etf_rows = etf_last2[etf_last2['Ticker'] == t].sort_values('Date')
                            if len(etf_rows) >= 1:
                                prev_close = etf_rows.iloc[-1]['Close']
                                if pd.notna(prev_close) and prev_close != 0:
                                    result[t]['pct_change'] = round(float(live_close / prev_close - 1) * 100, 2)
            except Exception:
                logger.exception("ticker-signals ETF merge failed")

        return result
    except Exception as e:
        logger.exception("ticker-signals failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/tickers/{ticker}")
def get_ticker_data(ticker: str):
    # Try stock signals first, fall back to ETF signals
    signals_file = INDIVIDUAL_SIGNALS_FILE
    live_file = LIVE_SIGNALS_FILE
    df = pd.DataFrame()
    if INDIVIDUAL_SIGNALS_FILE.exists():
        df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, filters=[('Ticker', '==', ticker)])
    if df.empty and ETF_SIGNALS_FILE.exists():
        df = pd.read_parquet(ETF_SIGNALS_FILE, filters=[('Ticker', '==', ticker)])
        signals_file = ETF_SIGNALS_FILE
        live_file = ETF_LIVE_SIGNALS_FILE
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data for ticker {ticker}")
    try:
        df['Date'] = pd.to_datetime(df['Date'])

        # Merge live bar using incremental signal computation (matches live-signals endpoint)
        # Only use the live bar if it's at least as recent as the latest Norgate date;
        # otherwise Norgate already has newer/equal data and the live file is stale.
        live_df = _read_live_parquet(live_file)
        if live_df is not None:
            live_row = live_df[live_df['Ticker'] == ticker]
            if not live_row.empty:
                lr = live_row.iloc[0]
                live_date = pd.to_datetime(lr['Date'])
                latest_norgate_date = df['Date'].max()

                if live_date > latest_norgate_date:
                    # Drop any existing row for the live date (live bar replaces it)
                    df = df[df['Date'] < live_date]

                    # Use last cached row + _build_signals_next_row (same path as /api/live-signals)
                    prev = df.sort_values('Date').iloc[-1]
                    ohlc = {
                        'Close': float(lr['Close']) if pd.notna(lr.get('Close')) else None,
                        'Open':  float(lr['Open'])  if pd.notna(lr.get('Open'))  else None,
                        'High':  float(lr['High'])  if pd.notna(lr.get('High'))  else None,
                        'Low':   float(lr['Low'])   if pd.notna(lr.get('Low'))   else None,
                    }
                    if ohlc['Close'] is not None:
                        new_row = signals_engine._build_signals_next_row(
                            prev, ohlc['Close'], live_date,
                            live_high=ohlc.get('High'),
                            live_low=ohlc.get('Low'),
                            live_open=ohlc.get('Open'),
                        )
                        if new_row is not None:
                            live_bar = pd.DataFrame([new_row])
                            live_bar['Source'] = 'live'
                            df = pd.concat([df, live_bar], ignore_index=True)

        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        return {"chart_data": clean_data_for_json(df.sort_values('Date')), "tickers": []}
    except Exception: raise HTTPException(status_code=500)


SIGNAL_TYPES = ['Breakout', 'Breakdown', 'Up_Rot', 'Down_Rot', 'BTFD', 'STFR', 'Long', 'Short']
SIGNAL_PAIRS = [('Breakout', 'Breakdown'), ('Up_Rot', 'Down_Rot'), ('BTFD', 'STFR')]
# The Is_ columns in the parquet use different names for rotations
SIGNAL_IS_COL = {
    'Breakout': 'Is_Breakout', 'Breakdown': 'Is_Breakdown',
    'Up_Rot': 'Is_Up_Rotation', 'Down_Rot': 'Is_Down_Rotation',
    'BTFD': 'Is_BTFD', 'STFR': 'Is_STFR',
}
# Long/Short are not in SIGNAL_IS_COL — they don't use Is_ columns
BACKTEST_DIRECTION = {
    "Up_Rot": "long", "Down_Rot": "short",
    "Breakout": "long", "Breakdown": "short",
    "BTFD": "long", "STFR": "short",
    "Long": "long", "Short": "short",
}
DEFAULT_EXIT_MAP = {
    'Breakout': 'Breakdown', 'Breakdown': 'Breakout',
    'Up_Rot': 'Down_Rot', 'Down_Rot': 'Up_Rot',
    'BTFD': 'Breakdown', 'STFR': 'Breakout',
}
EXIT_IS_COL = {
    'Breakout': 'Is_Breakout', 'Breakdown': 'Is_Breakdown',
    'Up_Rot': 'Is_Up_Rotation', 'Down_Rot': 'Is_Down_Rotation',
    'BTFD': 'Is_BTFD', 'STFR': 'Is_STFR',
}


def safe_float(value, digits=4):
    if value is None or pd.isna(value):
        return None
    return round(float(value), digits)


def safe_int(value):
    if value is None or pd.isna(value):
        return 0
    return int(value)

@app.get("/api/baskets/{basket_name}/summary")
def get_basket_summary(basket_name: str, start: str = None, end: str = None):
    if not INDIVIDUAL_SIGNALS_FILE.exists():
        raise HTTPException(status_code=404, detail="Signals file not found")
    try:
        range_start = pd.Timestamp(start) if start else None
        range_end = pd.Timestamp(end) if end else None
        is_range_mode = range_start is not None and range_end is not None

        # Build per-quarter membership lookup for range mode
        quarter_membership = {}  # quarter_str -> set of tickers
        last_quarter_tickers = set()
        if is_range_mode:
            history = _get_universe_history(basket_name)
            for q_str, q_tickers in history.items():
                q_start = _quarter_str_to_date(q_str)
                qn = int(q_str.split()[1][1])
                q_end = pd.Timestamp(year=q_start.year, month=qn * 3, day=1) + pd.offsets.MonthEnd(0)
                if q_end >= range_start and q_start <= range_end:
                    quarter_membership[q_str] = set(q_tickers)
            # Last quarter in range = the one with the latest start date
            if quarter_membership:
                last_q = sorted(quarter_membership.keys())[-1]
                last_quarter_tickers = quarter_membership[last_q]
            # Union of all tickers for data loading
            tickers = list(set().union(*quarter_membership.values())) if quarter_membership else []
        else:
            tickers = get_latest_universe_tickers(basket_name)
        if not tickers:
            tickers = get_meta_file_tickers(basket_name)
        if not tickers:
            raise HTTPException(status_code=404, detail="No tickers found for basket")

        # --- Open Signals ---
        STAT_SUFFIXES = [
            'Entry_Price', 'Exit_Date',
            'Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
            'Avg_MFE', 'Avg_MAE',
            'Std_Dev', 'Historical_EV', 'EV_Last_3',
            'Risk_Adj_EV', 'Risk_Adj_EV_Last_3', 'Count',
        ]
        cols_needed = ['Ticker', 'Date', 'Close', 'Trend', 'Is_Breakout_Sequence',
                       'Resistance_Pivot', 'Support_Pivot', 'Upper_Target', 'Lower_Target',
                       'BTFD_Triggered', 'STFR_Triggered']
        for st in SIGNAL_TYPES:
            if st not in SIGNAL_IS_COL:
                continue
            cols_needed.append(SIGNAL_IS_COL[st])
            for suf in STAT_SUFFIXES:
                cols_needed.append(f'{st}_{suf}')
        df = pd.read_parquet(
            INDIVIDUAL_SIGNALS_FILE,
            columns=cols_needed,
            filters=[('Ticker', 'in', tickers)],
        )
        df = df.sort_values('Date')

        # Filter by end date if range mode
        if range_end is not None:
            df = df[df['Date'] <= range_end]

        # For each ticker and signal pair, find which signal fired most recently
        # so we only report one open signal per pair per ticker.
        # Also track closed trades when in range mode.
        SHORT_SIGNALS = {'Down_Rot', 'Breakdown', 'STFR'}
        last_fired = {}  # (ticker, pair_index) -> (signal_type, entry_date, entry_price)
        closed_signals = []
        btfd_last_entry = {}  # ticker -> (entry_date, entry_price)
        stfr_last_entry = {}  # ticker -> (entry_date, entry_price)
        btfd_prev_exit_date = {}  # ticker -> previous BTFD_Exit_Date
        stfr_prev_exit_date = {}  # ticker -> previous STFR_Exit_Date
        for _, row in df.iterrows():
            ticker = row['Ticker']
            row_date = row['Date']
            for pi, (s1, s2) in enumerate(SIGNAL_PAIRS[:2]):
                # Determine which signal fires on this row (s2 wins if both fire)
                new_sig = None
                if row.get(SIGNAL_IS_COL[s1], False):
                    new_sig = s1
                if row.get(SIGNAL_IS_COL[s2], False):
                    new_sig = s2

                if new_sig is not None:
                    key = (ticker, pi)
                    prev = last_fired.get(key)
                    # If signal changed, the previous trade is closed
                    if is_range_mode and prev is not None and prev[0] != new_sig and row_date >= range_start:
                        prev_sig, prev_entry_date, prev_entry_price = prev
                        exit_price = row['Close']
                        perf = None
                        if pd.notna(prev_entry_price) and prev_entry_price:
                            ep = float(prev_entry_price)
                            xp = float(exit_price)
                            perf = (ep - xp) / ep if prev_sig in SHORT_SIGNALS else (xp - ep) / ep
                        entry_date_str = pd.Timestamp(prev_entry_date).strftime('%Y-%m-%d') if pd.notna(prev_entry_date) else None
                        exit_date_str = pd.Timestamp(row_date).strftime('%Y-%m-%d') if pd.notna(row_date) else None
                        closed_signals.append({
                            'Ticker': ticker, 'Signal_Type': prev_sig,
                            'Entry_Date': entry_date_str, 'Exit_Date': exit_date_str,
                            'Close': safe_float(exit_price, 2),
                            'Entry_Price': safe_float(prev_entry_price, 2),
                            'Current_Performance': safe_float(perf, 4),
                            'Win_Rate': safe_float(row.get(f'{prev_sig}_Win_Rate')),
                            'Avg_Winner': safe_float(row.get(f'{prev_sig}_Avg_Winner')),
                            'Avg_Loser': safe_float(row.get(f'{prev_sig}_Avg_Loser')),
                            'Avg_Winner_Bars': safe_float(row.get(f'{prev_sig}_Avg_Winner_Bars'), 1),
                            'Avg_Loser_Bars': safe_float(row.get(f'{prev_sig}_Avg_Loser_Bars'), 1),
                            'Avg_MFE': safe_float(row.get(f'{prev_sig}_Avg_MFE')),
                            'Avg_MAE': safe_float(row.get(f'{prev_sig}_Avg_MAE')),
                            'Std_Dev': safe_float(row.get(f'{prev_sig}_Std_Dev')),
                            'Historical_EV': safe_float(row.get(f'{prev_sig}_Historical_EV')),
                            'EV_Last_3': safe_float(row.get(f'{prev_sig}_EV_Last_3')),
                            'Risk_Adj_EV': safe_float(row.get(f'{prev_sig}_Risk_Adj_EV')),
                            'Risk_Adj_EV_Last_3': safe_float(row.get(f'{prev_sig}_Risk_Adj_EV_Last_3')),
                            'Count': safe_int(row.get(f'{prev_sig}_Count')),
                            'Is_Live': False,
                        })
                    # Store entry price at fire time so it's available when the trade closes
                    new_entry_price = row.get(f'{new_sig}_Entry_Price')
                    last_fired[key] = (new_sig, row_date, new_entry_price)

            # Track BTFD/STFR independently (not paired)
            if row.get(SIGNAL_IS_COL['BTFD'], False):
                btfd_last_entry[ticker] = (row_date, row.get('BTFD_Entry_Price'))
            if row.get(SIGNAL_IS_COL['STFR'], False):
                stfr_last_entry[ticker] = (row_date, row.get('STFR_Entry_Price'))

            # Detect BTFD/STFR closes via Exit_Date transition (for range mode)
            if is_range_mode and row_date >= range_start:
                for mr_sig, mr_entry_dict, mr_prev_exit_dict, mr_exit_col in [
                    ('BTFD', btfd_last_entry, btfd_prev_exit_date, 'BTFD_Exit_Date'),
                    ('STFR', stfr_last_entry, stfr_prev_exit_date, 'STFR_Exit_Date'),
                ]:
                    cur_exit = row.get(mr_exit_col)
                    prev_exit = mr_prev_exit_dict.get(ticker)
                    if pd.notna(cur_exit) and (prev_exit is None or pd.isna(prev_exit)):
                        prev_info = mr_entry_dict.get(ticker)
                        if prev_info is not None:
                            prev_entry_date, prev_entry_price = prev_info
                            exit_price = row['Close']
                            perf = None
                            if pd.notna(prev_entry_price) and prev_entry_price:
                                ep = float(prev_entry_price)
                                xp = float(exit_price)
                                perf = (ep - xp) / ep if mr_sig in SHORT_SIGNALS else (xp - ep) / ep
                            entry_date_str = pd.Timestamp(prev_entry_date).strftime('%Y-%m-%d') if pd.notna(prev_entry_date) else None
                            exit_date_str = pd.Timestamp(row_date).strftime('%Y-%m-%d') if pd.notna(row_date) else None
                            closed_signals.append({
                                'Ticker': ticker, 'Signal_Type': mr_sig,
                                'Entry_Date': entry_date_str, 'Exit_Date': exit_date_str,
                                'Close': safe_float(exit_price, 2),
                                'Entry_Price': safe_float(prev_entry_price, 2),
                                'Current_Performance': safe_float(perf, 4),
                                'Win_Rate': safe_float(row.get(f'{mr_sig}_Win_Rate')),
                                'Avg_Winner': safe_float(row.get(f'{mr_sig}_Avg_Winner')),
                                'Avg_Loser': safe_float(row.get(f'{mr_sig}_Avg_Loser')),
                                'Avg_Winner_Bars': safe_float(row.get(f'{mr_sig}_Avg_Winner_Bars'), 1),
                                'Avg_Loser_Bars': safe_float(row.get(f'{mr_sig}_Avg_Loser_Bars'), 1),
                                'Avg_MFE': safe_float(row.get(f'{mr_sig}_Avg_MFE')),
                                'Avg_MAE': safe_float(row.get(f'{mr_sig}_Avg_MAE')),
                                'Std_Dev': safe_float(row.get(f'{mr_sig}_Std_Dev')),
                                'Historical_EV': safe_float(row.get(f'{mr_sig}_Historical_EV')),
                                'EV_Last_3': safe_float(row.get(f'{mr_sig}_EV_Last_3')),
                                'Risk_Adj_EV': safe_float(row.get(f'{mr_sig}_Risk_Adj_EV')),
                                'Risk_Adj_EV_Last_3': safe_float(row.get(f'{mr_sig}_Risk_Adj_EV_Last_3')),
                                'Count': safe_int(row.get(f'{mr_sig}_Count')),
                                'Is_Live': False,
                            })
            btfd_prev_exit_date[ticker] = row.get('BTFD_Exit_Date')
            stfr_prev_exit_date[ticker] = row.get('STFR_Exit_Date')

        latest = df.groupby('Ticker').tail(1)

        if is_range_mode:
            # In range mode, don't exclude delisted tickers
            pass
        else:
            # Exclude delisted tickers whose data ends before the most recent date
            max_date = latest['Date'].max()
            latest = latest[latest['Date'] >= max_date]

        # Read live closes for intraday price updates (skip in range mode or stale live file)
        live_closes = {}
        if not is_range_mode:
            live_df = _read_live_parquet(LIVE_SIGNALS_FILE)
            if live_df is not None and _live_is_current(live_df, max_date):
                for _, lr in live_df.iterrows():
                    t = lr.get('Ticker')
                    c = lr.get('Close')
                    if t and pd.notna(c):
                        live_closes[t] = float(c)

        open_signals = []
        for _, row in latest.iterrows():
            ticker = row['Ticker']
            hist_close = row['Close']

            # --- Live state recomputation (same pivot logic as _compute_live_breadth) ---
            if ticker in live_closes:
                close = live_closes[ticker]
                prev_res = row.get('Resistance_Pivot')
                prev_sup = row.get('Support_Pivot')
                prev_upper = row.get('Upper_Target')
                prev_lower = row.get('Lower_Target')
                is_up_rot = pd.notna(prev_res) and close > prev_res
                is_down_rot = pd.notna(prev_sup) and close < prev_sup

                if is_up_rot:
                    live_trend = 1.0
                elif is_down_rot:
                    live_trend = 0.0
                else:
                    live_trend = row.get('Trend')

                is_bo = is_up_rot and pd.notna(prev_upper) and close > prev_upper
                is_bd = is_down_rot and pd.notna(prev_lower) and close < prev_lower
                if is_bo:
                    live_bos = True
                elif is_bd:
                    live_bos = False
                else:
                    live_bos = row.get('Is_Breakout_Sequence', False)
            else:
                close = hist_close
                live_trend = row.get('Trend')
                live_bos = row.get('Is_Breakout_Sequence', False)

            # --- LT Trend (Breakout/Breakdown): always present for every ticker ---
            bos = live_bos
            lt_active = 'Breakout' if bos else 'Breakdown'
            lt_is_live = bool(bos != row.get('Is_Breakout_Sequence', False)) and ticker in live_closes
            lt_fired = last_fired.get((ticker, 0))
            lt_entry_date = lt_fired[1] if lt_fired and lt_fired[0] == lt_active else None
            lt_entry_price = row.get(f'{lt_active}_Entry_Price')
            if pd.notna(lt_entry_price) and lt_entry_price:
                lt_perf = ((lt_entry_price - close) / lt_entry_price if lt_active in SHORT_SIGNALS
                           else (close - lt_entry_price) / lt_entry_price)
            else:
                lt_perf = None
            lt_date_str = pd.Timestamp(lt_entry_date).strftime('%Y-%m-%d') if pd.notna(lt_entry_date) else None
            open_signals.append({
                'Ticker': ticker, 'Signal_Type': lt_active,
                'Entry_Date': lt_date_str,
                'Close': safe_float(close, 2),
                'Entry_Price': safe_float(lt_entry_price, 2),
                'Current_Performance': safe_float(lt_perf, 4),
                'Win_Rate': safe_float(row.get(f'{lt_active}_Win_Rate')),
                'Avg_Winner': safe_float(row.get(f'{lt_active}_Avg_Winner')),
                'Avg_Loser': safe_float(row.get(f'{lt_active}_Avg_Loser')),
                'Avg_Winner_Bars': safe_float(row.get(f'{lt_active}_Avg_Winner_Bars'), 1),
                'Avg_Loser_Bars': safe_float(row.get(f'{lt_active}_Avg_Loser_Bars'), 1),
                'Avg_MFE': safe_float(row.get(f'{lt_active}_Avg_MFE')),
                'Avg_MAE': safe_float(row.get(f'{lt_active}_Avg_MAE')),
                'Std_Dev': safe_float(row.get(f'{lt_active}_Std_Dev')),
                'Historical_EV': safe_float(row.get(f'{lt_active}_Historical_EV')),
                'EV_Last_3': safe_float(row.get(f'{lt_active}_EV_Last_3')),
                'Risk_Adj_EV': safe_float(row.get(f'{lt_active}_Risk_Adj_EV')),
                'Risk_Adj_EV_Last_3': safe_float(row.get(f'{lt_active}_Risk_Adj_EV_Last_3')),
                'Count': safe_int(row.get(f'{lt_active}_Count')),
                'Is_Live': lt_is_live,
            })

            # --- ST Trend (Up_Rot/Down_Rot): always present for every ticker ---
            trend_val = live_trend
            if pd.notna(trend_val):
                st_active = 'Up_Rot' if trend_val == 1.0 else 'Down_Rot'
            else:
                st_active = 'Down_Rot'  # default to downtrend if unknown
            hist_trend = row.get('Trend')
            st_is_live = bool(
                ticker in live_closes
                and pd.notna(live_trend) and pd.notna(hist_trend)
                and live_trend != hist_trend
            )
            st_fired = last_fired.get((ticker, 1))
            st_entry_date = st_fired[1] if st_fired and st_fired[0] == st_active else None
            st_entry_price = row.get(f'{st_active}_Entry_Price')
            if pd.notna(st_entry_price) and st_entry_price:
                st_perf = ((st_entry_price - close) / st_entry_price if st_active in SHORT_SIGNALS
                           else (close - st_entry_price) / st_entry_price)
            else:
                st_perf = None
            st_date_str = pd.Timestamp(st_entry_date).strftime('%Y-%m-%d') if pd.notna(st_entry_date) else None
            open_signals.append({
                'Ticker': ticker, 'Signal_Type': st_active,
                'Entry_Date': st_date_str,
                'Close': safe_float(close, 2),
                'Entry_Price': safe_float(st_entry_price, 2),
                'Current_Performance': safe_float(st_perf, 4),
                'Win_Rate': safe_float(row.get(f'{st_active}_Win_Rate')),
                'Avg_Winner': safe_float(row.get(f'{st_active}_Avg_Winner')),
                'Avg_Loser': safe_float(row.get(f'{st_active}_Avg_Loser')),
                'Avg_Winner_Bars': safe_float(row.get(f'{st_active}_Avg_Winner_Bars'), 1),
                'Avg_Loser_Bars': safe_float(row.get(f'{st_active}_Avg_Loser_Bars'), 1),
                'Avg_MFE': safe_float(row.get(f'{st_active}_Avg_MFE')),
                'Avg_MAE': safe_float(row.get(f'{st_active}_Avg_MAE')),
                'Std_Dev': safe_float(row.get(f'{st_active}_Std_Dev')),
                'Historical_EV': safe_float(row.get(f'{st_active}_Historical_EV')),
                'EV_Last_3': safe_float(row.get(f'{st_active}_EV_Last_3')),
                'Risk_Adj_EV': safe_float(row.get(f'{st_active}_Risk_Adj_EV')),
                'Risk_Adj_EV_Last_3': safe_float(row.get(f'{st_active}_Risk_Adj_EV_Last_3')),
                'Count': safe_int(row.get(f'{st_active}_Count')),
                'Is_Live': st_is_live,
            })

            # --- BTFD/STFR: check independently, both can be open ---
            btfd_is_live = False
            stfr_is_live = False
            if ticker in live_closes:
                prev_lower = row.get('Lower_Target')
                prev_upper = row.get('Upper_Target')
                hist_btfd_triggered = bool(row.get('BTFD_Triggered', False))
                hist_stfr_triggered = bool(row.get('STFR_Triggered', False))
                # BTFD: prev candle was downtrend, still in downtrend, close <= lower target, not triggered
                if (pd.notna(prev_lower) and close <= prev_lower
                        and hist_trend == 0.0 and live_trend == 0.0
                        and not hist_btfd_triggered):
                    btfd_is_live = True
                # STFR: prev candle was uptrend, still in uptrend, close >= upper target, not triggered
                if (pd.notna(prev_upper) and close >= prev_upper
                        and hist_trend == 1.0 and live_trend == 1.0
                        and not hist_stfr_triggered):
                    stfr_is_live = True

            for mr_sig, mr_entry_dict, mr_is_live in [
                ('BTFD', btfd_last_entry, btfd_is_live),
                ('STFR', stfr_last_entry, stfr_is_live),
            ]:
                entry_col = f'{mr_sig}_Entry_Price'
                exit_col = f'{mr_sig}_Exit_Date'
                entry_price = row.get(entry_col) if entry_col in row.index else None
                exit_date_val = row.get(exit_col) if exit_col in row.index else None
                if pd.notna(entry_price) and pd.isna(exit_date_val):
                    entry_info = mr_entry_dict.get(ticker)
                    entry_date = entry_info[0] if entry_info else None
                    if mr_sig in SHORT_SIGNALS:
                        perf = (entry_price - close) / entry_price if entry_price else 0
                    else:
                        perf = (close - entry_price) / entry_price if entry_price else 0
                    entry_date_str = pd.Timestamp(entry_date).strftime('%Y-%m-%d') if pd.notna(entry_date) else None
                    open_signals.append({
                        'Ticker': ticker, 'Signal_Type': mr_sig,
                        'Entry_Date': entry_date_str,
                        'Close': safe_float(close, 2),
                        'Entry_Price': safe_float(entry_price, 2),
                        'Current_Performance': safe_float(perf, 4),
                        'Win_Rate': safe_float(row.get(f'{mr_sig}_Win_Rate')),
                        'Avg_Winner': safe_float(row.get(f'{mr_sig}_Avg_Winner')),
                        'Avg_Loser': safe_float(row.get(f'{mr_sig}_Avg_Loser')),
                        'Avg_Winner_Bars': safe_float(row.get(f'{mr_sig}_Avg_Winner_Bars'), 1),
                        'Avg_Loser_Bars': safe_float(row.get(f'{mr_sig}_Avg_Loser_Bars'), 1),
                        'Avg_MFE': safe_float(row.get(f'{mr_sig}_Avg_MFE')),
                        'Avg_MAE': safe_float(row.get(f'{mr_sig}_Avg_MAE')),
                        'Std_Dev': safe_float(row.get(f'{mr_sig}_Std_Dev')),
                        'Historical_EV': safe_float(row.get(f'{mr_sig}_Historical_EV')),
                        'EV_Last_3': safe_float(row.get(f'{mr_sig}_EV_Last_3')),
                        'Risk_Adj_EV': safe_float(row.get(f'{mr_sig}_Risk_Adj_EV')),
                        'Risk_Adj_EV_Last_3': safe_float(row.get(f'{mr_sig}_Risk_Adj_EV_Last_3')),
                        'Count': safe_int(row.get(f'{mr_sig}_Count')),
                        'Is_Live': mr_is_live,
                    })
        open_signals.sort(key=lambda x: x['Ticker'])
        closed_signals.sort(key=lambda x: x['Ticker'])

        # In range mode, filter signals by basket membership
        if is_range_mode and quarter_membership:
            # Open signals: only tickers in the LAST quarter of the range
            open_signals = [s for s in open_signals if s['Ticker'] in last_quarter_tickers]

            # Closed signals: only trades where ticker was in the basket at exit time
            def _ticker_in_basket_at_date(ticker, date_str):
                if not date_str:
                    return False
                dt = pd.Timestamp(date_str)
                for q_str, q_tickers in quarter_membership.items():
                    q_start = _quarter_str_to_date(q_str)
                    qn = int(q_str.split()[1][1])
                    q_end = pd.Timestamp(year=q_start.year, month=qn * 3, day=1) + pd.offsets.MonthEnd(0)
                    if q_start <= dt <= q_end and ticker in q_tickers:
                        return True
                return False
            closed_signals = [s for s in closed_signals if _ticker_in_basket_at_date(s['Ticker'], s.get('Exit_Date'))]

        # --- 21-Day Correlation ---
        close_df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                   filters=[('Ticker', 'in', tickers)])
        close_pivot = close_df.pivot_table(index='Date', columns='Ticker', values='Close')
        returns = close_pivot.pct_change()
        recent_returns = returns.sort_index().tail(21)
        valid_cols = [c for c in recent_returns.columns if recent_returns[c].notna().sum() >= 10]
        corr_labels = sorted(valid_cols)
        corr_matrix = recent_returns[corr_labels].corr()
        corr_values = corr_matrix.values.tolist()
        # Replace NaN with null for JSON
        corr_values = [[None if (v != v) else round(v, 3) for v in row] for row in corr_values]

        # --- Cumulative Returns (respects active basket membership via contributions) ---
        contrib_file = _find_basket_contributions(basket_name)
        if contrib_file:
            cdf = pd.read_parquet(contrib_file)
            cdf['Date'] = pd.to_datetime(cdf['Date']).dt.normalize()
            cdf = cdf.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
            ret_pivot = cdf.pivot_table(index='Date', columns='Ticker', values='Daily_Return')
            ret_pivot = ret_pivot.sort_index()
            active_mask = ret_pivot.notna()
            # Fill inactive days with 0% return (factor=1) so cumprod passes through
            factors = ret_pivot.fillna(0) + 1
            equity = factors.cumprod()
            cum_ret = equity - 1
            # Mask inactive days back to NaN
            cum_ret[~active_mask] = float('nan')
            dates = [d.strftime('%Y-%m-%d') for d in ret_pivot.index]
            cum_series = []
            for t in sorted(ret_pivot.columns):
                vals = [None if pd.isna(v) else round(float(v), 4) for v in cum_ret[t].tolist()]
                cum_series.append({'ticker': t, 'values': vals, 'join_date': None})
        else:
            # Fallback: use close prices and join dates (no contributions file)
            join_dates = _get_ticker_join_dates(basket_name, tickers)
            close_sorted = close_pivot.sort_index()
            if close_sorted.empty:
                dates = []
                cum_series = []
            else:
                dates = [d.strftime('%Y-%m-%d') for d in close_sorted.index]
                cum_series = []
                for t in sorted(close_sorted.columns):
                    col = close_sorted[t]
                    jd = join_dates.get(t)
                    if jd:
                        valid = col[col.index >= jd].dropna()
                    else:
                        valid = col.dropna()
                    if valid.empty:
                        vals = [None] * len(dates)
                    else:
                        base_price = valid.iloc[0]
                        rebased = col / base_price - 1
                        if jd:
                            rebased[rebased.index < jd] = float('nan')
                        vals = [None if pd.isna(v) else round(float(v), 4) for v in rebased.tolist()]
                    jd_str = jd.strftime('%Y-%m-%d') if jd else None
                    cum_series.append({'ticker': t, 'values': vals, 'join_date': jd_str})

        return {
            'open_signals': open_signals,
            'closed_signals': closed_signals,
            'correlation': {'labels': corr_labels, 'matrix': corr_values},
            'cumulative_returns': {'dates': dates, 'series': cum_series},
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_basket_summary for {basket_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/baskets/{basket_name}/correlation")
def get_basket_correlation(basket_name: str, date: str = None):
    """Return 21-day trailing correlation matrix for tickers in the basket at a given date."""
    try:
        if date:
            target_date = pd.Timestamp(date)
        else:
            target_date = None

        # Get tickers for the target date's quarter (or latest)
        if target_date:
            corr_tickers = _get_tickers_for_date(basket_name, target_date)
        else:
            corr_tickers = get_latest_universe_tickers(basket_name)
            if not corr_tickers:
                corr_tickers = get_meta_file_tickers(basket_name)
        if not corr_tickers:
            raise HTTPException(status_code=404, detail="No tickers found for basket")

        close_df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                   filters=[('Ticker', 'in', corr_tickers)])
        close_pivot = close_df.pivot_table(index='Date', columns='Ticker', values='Close').sort_index()

        if target_date:
            close_pivot = close_pivot[close_pivot.index <= target_date]

        returns = close_pivot.pct_change()
        recent_returns = returns.tail(21)
        valid_cols = [c for c in recent_returns.columns if recent_returns[c].notna().sum() >= 10]
        corr_labels = sorted(valid_cols)
        corr_matrix = recent_returns[corr_labels].corr()
        corr_values = corr_matrix.values.tolist()
        corr_values = [[None if (v != v) else round(v, 3) for v in row] for row in corr_values]

        # Return available date range for the date picker
        all_dates = [d.strftime('%Y-%m-%d') for d in close_pivot.index]
        return {
            'labels': corr_labels,
            'matrix': corr_values,
            'min_date': all_dates[0] if all_dates else None,
            'max_date': all_dates[-1] if all_dates else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_basket_correlation for {basket_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _find_basket_contributions(slug):
    """Glob for a basket contributions parquet by slug prefix across basket cache folders."""
    for folder in BASKET_CACHE_FOLDERS:
        if not folder.exists():
            continue
        matches = list(folder.glob(f'{slug}_*_of_*_contributions.parquet'))
        if not matches:
            matches = list(folder.glob(f'{slug}_of_*_contributions.parquet'))
        if matches:
            return matches[0]
    return None


@app.get("/api/baskets/{basket_name}/contributions")
def get_basket_contributions(basket_name: str, start: str = None, end: str = None):
    """Return per-constituent contribution data for a date range."""
    try:
        contrib_file = _find_basket_contributions(basket_name)
        if not contrib_file:
            raise HTTPException(status_code=404, detail=f"Contributions file not found for {basket_name}")

        df = pd.read_parquet(contrib_file)
        df['Date'] = pd.to_datetime(df['Date']).dt.normalize()

        # Per-ticker metadata from full dataset (before date filtering)
        full_max_date = df['Date'].max()
        ticker_meta = df.groupby('Ticker').agg(
            first_date=('Date', 'min'),
            last_date=('Date', 'max'),
        ).reset_index()
        # Current weight: Weight_BOD on the dataset max date (null if ticker exited)
        max_day = df[df['Date'] == full_max_date][['Ticker', 'Weight_BOD']].rename(
            columns={'Weight_BOD': 'current_weight'}
        )
        ticker_meta = ticker_meta.merge(max_day, on='Ticker', how='left')

        # Full date range (for the date picker)
        full_min_str = df['Date'].min().strftime('%Y-%m-%d')
        full_max_str = full_max_date.strftime('%Y-%m-%d')

        # Apply date filtering
        if start:
            df = df[df['Date'] >= pd.Timestamp(start)]
        if end:
            df = df[df['Date'] <= pd.Timestamp(end)]

        if df.empty:
            return {
                "tickers": [], "dates": [], "total_contributions": [],
                "initial_weights": [], "final_weights": [],
                "first_dates": [], "last_dates": [], "current_weights": [],
                "equity_dates": [], "equity_values": [],
                "date_range": {"min": full_min_str, "max": full_max_str},
            }

        # Equity curve: daily basket return then cumulative product
        daily_return = df.groupby('Date')['Contribution'].sum().sort_index()
        equity = (1 + daily_return).cumprod()
        equity_dates = [d.strftime('%Y-%m-%d') for d in equity.index]
        equity_values = equity.tolist()

        # Aggregate per-ticker over the period
        agg = df.groupby('Ticker').agg(
            total_contribution=('Contribution', 'sum'),
            initial_weight=('Weight_BOD', 'first'),
            final_weight=('Weight_BOD', 'last'),
        ).reset_index()

        # Sort worst to best
        agg = agg.sort_values('total_contribution').reset_index(drop=True)

        # Merge ticker metadata so arrays align with tickers[]
        agg = agg.merge(ticker_meta, on='Ticker', how='left')

        # Date range info
        all_dates = sorted(df['Date'].unique())
        date_strs = [d.strftime('%Y-%m-%d') for d in all_dates]

        return {
            "tickers": agg['Ticker'].tolist(),
            "total_contributions": agg['total_contribution'].tolist(),
            "initial_weights": agg['initial_weight'].tolist(),
            "final_weights": agg['final_weight'].tolist(),
            "first_dates": [d.strftime('%Y-%m-%d') for d in agg['first_date']],
            "last_dates": [d.strftime('%Y-%m-%d') for d in agg['last_date']],
            "current_weights": [None if pd.isna(w) else float(w) for w in agg['current_weight']],
            "equity_dates": equity_dates,
            "equity_values": equity_values,
            "dates": date_strs,
            "date_range": {
                "min": full_min_str,
                "max": full_max_str,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_basket_contributions for {basket_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/baskets/{basket_name}/candle-detail")
def get_basket_candle_detail(basket_name: str, date: str = None):
    """Return per-constituent weights, returns, and contributions for a single day."""
    try:
        contrib_file = _find_basket_contributions(basket_name)
        if not contrib_file:
            raise HTTPException(status_code=404, detail=f"Contributions file not found for {basket_name}")

        df = pd.read_parquet(contrib_file)
        df['Date'] = pd.to_datetime(df['Date']).dt.normalize()

        if date:
            target = pd.Timestamp(date).normalize()
        else:
            target = df['Date'].max()

        day = df[df['Date'] == target]
        if day.empty:
            return {"date": target.strftime('%Y-%m-%d'), "constituents": []}

        # Sort by contribution descending
        day = day.sort_values('Contribution', ascending=False)

        # Compute current stint entry date per ticker and end-of-day drifted weight
        # For each ticker present on `target`, walk backwards through its sorted dates
        # to find the start of the current continuous run (gap > 5 business days = new stint)
        all_dates_sorted = sorted(df['Date'].unique())
        date_to_idx = {d: i for i, d in enumerate(all_dates_sorted)}
        target_idx = date_to_idx.get(target)
        stint_entry = {}
        for tkr, grp in df.groupby('Ticker'):
            tkr_dates = sorted(grp['Date'].unique())
            # Only care about dates up to and including target
            tkr_dates = [d for d in tkr_dates if d <= target]
            if not tkr_dates or tkr_dates[-1] != target:
                continue
            # Walk backwards from end to find where the current stint started
            entry = tkr_dates[-1]
            for i in range(len(tkr_dates) - 1, 0, -1):
                gap = date_to_idx.get(tkr_dates[i], 0) - date_to_idx.get(tkr_dates[i - 1], 0)
                if gap > 5:  # more than 5 trading days gap = new stint
                    break
                entry = tkr_dates[i - 1]
            stint_entry[tkr] = entry

        total_eod = (day['Weight_BOD'] * (1 + day['Daily_Return'])).sum()

        constituents = []
        for _, row in day.iterrows():
            tkr = row['Ticker']
            w_bod = float(row['Weight_BOD'])
            d_ret = float(row['Daily_Return'])
            eod_weight = (w_bod * (1 + d_ret)) / total_eod if total_eod > 0 else 0
            se = stint_entry.get(tkr)
            constituents.append({
                "ticker": tkr,
                "first_date": se.strftime('%Y-%m-%d') if se is not None and pd.notna(se) else None,
                "weight": round(w_bod, 6),
                "eod_weight": round(eod_weight, 6),
                "daily_return": round(d_ret, 6),
                "contribution": round(float(row['Contribution']), 6),
            })

        basket_return = float(day['Contribution'].sum())

        return {
            "date": target.strftime('%Y-%m-%d'),
            "constituents": constituents,
            "basket_return": round(basket_return, 6),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_basket_candle_detail for {basket_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/ticker-baskets/{ticker}")
def get_ticker_baskets(ticker: str):
    """Return list of basket names containing this ticker that also have parquet data."""
    candidates = []
    if GICS_MAPPINGS_FILE.exists():
        with open(GICS_MAPPINGS_FILE, 'r') as f:
            gics = json.load(f)
        for group_key in ('sector_u', 'industry_u'):
            group = gics.get(group_key, {})
            for name, quarter_dict in group.items():
                for q_tickers in quarter_dict.values():
                    if ticker in q_tickers:
                        candidates.append(name.replace(" ", "_"))
                        break
    for basket_name, (fn, key) in THEMATIC_CONFIG.items():
        p_path = THEMATIC_BASKET_CACHE / fn
        if p_path.exists():
            try:
                with open(p_path, 'r') as f:
                    data = json.load(f)
                ud = data[key] if key is not None else data
                for q_tickers in ud.values():
                    if ticker in q_tickers:
                        candidates.append(basket_name)
                        break
            except Exception:
                pass
    # Only return baskets that have a signals parquet file
    return sorted(set(b for b in candidates if _find_basket_parquet(b)))


class BacktestFilter(BaseModel):
    metric: str
    condition: str
    value: Optional[float] = None
    source: str = "self"
    lookback: Optional[int] = 21

class BacktestRequest(BaseModel):
    target: str
    target_type: str
    entry_signal: str
    exit_signal: Optional[str] = None
    stop_signal: Optional[str] = None
    exit_rv_multiple: Optional[float] = None
    stop_rv_multiple: Optional[float] = None
    trailing_stop_rv_multiple: Optional[float] = None
    no_exit_target: Optional[bool] = None
    filters: List[BacktestFilter] = []
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    position_size: float = 1.0
    max_leverage: float = 2.5

class MultiBacktestLeg(BaseModel):
    target: str
    target_type: str
    entry_signal: str
    exit_signal: Optional[str] = None
    stop_signal: Optional[str] = None
    exit_rv_multiple: Optional[float] = None
    stop_rv_multiple: Optional[float] = None
    trailing_stop_rv_multiple: Optional[float] = None
    no_exit_target: Optional[bool] = None
    allocation_pct: float  # 0-1 fraction
    position_size: float = 1.0
    filters: List[BacktestFilter] = []

class MultiBacktestRequest(BaseModel):
    legs: List[MultiBacktestLeg]
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    max_leverage: float = 2.5
    equity_only: bool = False  # Return only equity curve (for benchmarks)


def _build_leg_trades(target, target_type, entry_signal, filters, start_date, end_date,
                      exit_signal=None, stop_signal=None, exit_rv_multiple=None, stop_rv_multiple=None,
                      trailing_stop_rv_multiple=None, no_exit_target=False):
    """Extract trade-building logic for a single backtest leg.
    Returns (trades, df, ticker_closes, direction).
    """
    sig = entry_signal

    # Long/Short/Buy_Hold: single trade spanning the full date range
    if sig in ('Buy_Hold', 'Long', 'Short'):
        direction = 'short' if sig == 'Short' else 'long'
        if target_type == 'basket':
            basket_file = _find_basket_parquet(target)
            if not basket_file:
                raise HTTPException(status_code=404, detail=f"Basket file not found for {target}")
            df = pd.read_parquet(basket_file, columns=['Date', 'Close'])
        elif target_type == 'basket_tickers':
            raise HTTPException(status_code=400, detail=f"{sig} not supported for basket_tickers — use basket mode")
        elif target_type == 'etf':
            if not ETF_SIGNALS_FILE.exists():
                raise HTTPException(status_code=404, detail="ETF signals file not found")
            df = pd.read_parquet(ETF_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                 filters=[('Ticker', '==', target)])
        else:
            if not INDIVIDUAL_SIGNALS_FILE.exists():
                raise HTTPException(status_code=404, detail="Signals file not found")
            df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                                 filters=[('Ticker', '==', target)])
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {target}")
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values('Date').reset_index(drop=True)
        if start_date:
            df = df[df['Date'] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df['Date'] <= pd.Timestamp(end_date)]
        closes = df.drop_duplicates('Date').set_index('Date')['Close'].sort_index()
        ticker_closes = {'': closes}
        if closes.empty:
            return ([], df, ticker_closes, direction)
        first_close = float(closes.iloc[0])
        last_close = float(closes.iloc[-1])
        if direction == 'long':
            total_return = last_close / first_close - 1 if first_close else 0
            mfe_val = float(closes.max()) / first_close - 1 if first_close else 0
            mae_val = float(closes.min()) / first_close - 1 if first_close else 0
        else:
            total_return = (first_close - last_close) / first_close if first_close else 0
            mfe_val = (first_close - float(closes.min())) / first_close if first_close else 0
            mae_val = (first_close - float(closes.max())) / first_close if first_close else 0
        entry_date = closes.index[0]
        exit_date = closes.index[-1]
        bars = max(1, int(np.busday_count(entry_date.date(), exit_date.date())))
        trade = {
            'entry_date': entry_date.strftime('%Y-%m-%d'),
            'exit_date': exit_date.strftime('%Y-%m-%d'),
            'entry_price': round(first_close, 2),
            'exit_price': round(last_close, 2),
            'change': round(total_return, 6),
            'mfe': round(mfe_val, 6),
            'mae': round(mae_val, 6),
            'bars_held': bars,
            'regime_pass': True,
            'entry_weight': None,
            'exit_weight': None,
            'contribution': None,
        }
        return ([trade], df, ticker_closes, direction)

    is_col = SIGNAL_IS_COL.get(sig)
    if not is_col:
        raise HTTPException(status_code=400, detail=f"Unknown signal: {sig}")
    direction = BACKTEST_DIRECTION[sig]

    # Determine if custom exit is needed
    default_exit = DEFAULT_EXIT_MAP.get(sig)
    use_custom_exit = (
        no_exit_target or
        (exit_signal is not None and exit_signal != default_exit) or
        stop_signal is not None or
        exit_rv_multiple is not None or stop_rv_multiple is not None or
        trailing_stop_rv_multiple is not None
    )

    # Trade data columns for entry signal (only needed for default exit path)
    if not use_custom_exit:
        trade_cols = [f'{sig}_Entry_Price', f'{sig}_Exit_Date', f'{sig}_Exit_Price',
                      f'{sig}_Final_Change', f'{sig}_MFE', f'{sig}_MAE']
    else:
        trade_cols = [f'{sig}_Entry_Price']
    # Resolve effective exit signal for custom path (None if no_exit_target)
    effective_exit = None if no_exit_target else (exit_signal if exit_signal is not None else default_exit)

    is_multi_ticker = target_type == 'basket_tickers'

    # 1. Load target data
    _quarter_bounds = None
    if target_type == 'basket':
        basket_file = _find_basket_parquet(target)
        if not basket_file:
            raise HTTPException(status_code=404, detail=f"Basket file not found for {target}")
        base_cols = ['Date', 'Close', is_col] + trade_cols
        # Add custom exit/stop Is_ columns if needed
        if use_custom_exit:
            for _sig in [effective_exit, stop_signal]:
                if _sig:
                    _is = EXIT_IS_COL.get(_sig)
                    if _is and _is not in base_cols:
                        base_cols.append(_is)
            base_cols.extend(['High', 'Low'])
            if exit_rv_multiple is not None or stop_rv_multiple is not None or trailing_stop_rv_multiple is not None:
                base_cols.append('RV_EMA')
        for flt in filters:
            if flt.source == 'self' and flt.metric not in base_cols:
                base_cols.append(flt.metric)
        base_cols = list(dict.fromkeys(base_cols))  # dedupe
        try:
            df = pd.read_parquet(basket_file, columns=[c for c in base_cols if c])
        except Exception:
            df = pd.read_parquet(basket_file)
            df = df[[c for c in base_cols if c in df.columns]]
    elif target_type == 'basket_tickers':
        if not INDIVIDUAL_SIGNALS_FILE.exists():
            raise HTTPException(status_code=404, detail="Signals file not found")
        _universe_history = _get_universe_history(target)
        if _universe_history:
            if start_date and end_date:
                basket_tickers = _get_universe_tickers_for_range(target, pd.Timestamp(start_date), pd.Timestamp(end_date))
            elif start_date:
                basket_tickers = _get_universe_tickers_for_range(target, pd.Timestamp(start_date), pd.Timestamp('2099-12-31'))
            else:
                basket_tickers = list({t for tks in _universe_history.values() for t in tks})
            _quarter_bounds = sorted(
                [(_quarter_str_to_date(q), set(tks)) for q, tks in _universe_history.items()],
                key=lambda x: x[0]
            )
        else:
            basket_tickers = get_latest_universe_tickers(target)
            if not basket_tickers:
                basket_tickers = get_meta_file_tickers(target)
            _quarter_bounds = None
        if not basket_tickers:
            raise HTTPException(status_code=404, detail=f"No tickers found for basket {target}")
        base_cols = ['Ticker', 'Date', 'Close', is_col] + trade_cols
        if use_custom_exit:
            exit_is = EXIT_IS_COL.get(exit_signal)
            if exit_is and exit_is not in base_cols:
                base_cols.append(exit_is)
            base_cols.extend(['High', 'Low'])
            if exit_rv_multiple is not None or stop_rv_multiple is not None or trailing_stop_rv_multiple is not None:
                base_cols.append('RV_EMA')
        for flt in filters:
            if flt.source == 'self' and flt.metric not in base_cols:
                base_cols.append(flt.metric)
        base_cols = list(dict.fromkeys(base_cols))
        try:
            df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE,
                                 columns=[c for c in base_cols if c],
                                 filters=[('Ticker', 'in', basket_tickers)])
        except Exception:
            df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE,
                                 filters=[('Ticker', 'in', basket_tickers)])
            df = df[[c for c in base_cols if c in df.columns]]
    else:
        _sig_file = ETF_SIGNALS_FILE if target_type == 'etf' else INDIVIDUAL_SIGNALS_FILE
        if not _sig_file.exists():
            raise HTTPException(status_code=404, detail="Signals file not found")
        base_cols = ['Ticker', 'Date', 'Close', is_col] + trade_cols
        if use_custom_exit:
            exit_is = EXIT_IS_COL.get(exit_signal)
            if exit_is and exit_is not in base_cols:
                base_cols.append(exit_is)
            base_cols.extend(['High', 'Low'])
            if exit_rv_multiple is not None or stop_rv_multiple is not None or trailing_stop_rv_multiple is not None:
                base_cols.append('RV_EMA')
        for flt in filters:
            if flt.source == 'self' and flt.metric not in base_cols:
                base_cols.append(flt.metric)
        base_cols = list(dict.fromkeys(base_cols))
        try:
            df = pd.read_parquet(_sig_file,
                                 columns=[c for c in base_cols if c],
                                 filters=[('Ticker', '==', target)])
        except Exception:
            df = pd.read_parquet(_sig_file,
                                 filters=[('Ticker', '==', target)])
            df = df[[c for c in base_cols if c in df.columns]]

    df['Date'] = pd.to_datetime(df['Date'])
    if is_multi_ticker:
        df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)
    else:
        df = df.sort_values('Date').reset_index(drop=True)

    # Date range filter
    if start_date:
        df = df[df['Date'] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df['Date'] <= pd.Timestamp(end_date)]

    if df.empty:
        return [], df, {}, direction

    # Compute Return filter column if needed
    for flt in filters:
        if flt.metric == 'Return':
            lookback = flt.lookback or 252
            return_col = f'__return_{lookback}'
            if return_col not in df.columns:
                if is_multi_ticker:
                    df[return_col] = df.groupby('Ticker')['Close'].transform(
                        lambda x: x / x.shift(lookback) - 1)
                else:
                    df[return_col] = df['Close'] / df['Close'].shift(lookback) - 1

    # Load external filter sources and merge
    external_sources = {}
    failed_sources = set()
    needs_ext_merge = any(flt.source != 'self' for flt in filters)
    if needs_ext_merge and is_multi_ticker:
        df = df.sort_values('Date').reset_index(drop=True)
    for flt in filters:
        if flt.source != 'self' and flt.source not in external_sources and flt.source not in failed_sources:
            ext_file = _find_basket_parquet(flt.source)
            if not ext_file:
                failed_sources.add(flt.source)
                continue
            ext_cols = ['Date']
            for f2 in filters:
                if f2.source == flt.source and f2.metric not in ext_cols:
                    ext_cols.append(f2.metric)
            try:
                ext_df = pd.read_parquet(ext_file, columns=ext_cols)
            except Exception:
                ext_df = pd.read_parquet(ext_file)
                ext_df = ext_df[[c for c in ext_cols if c in ext_df.columns]]
            ext_df['Date'] = pd.to_datetime(ext_df['Date'])
            ext_df = ext_df.sort_values('Date')
            suffix = f'__{flt.source}'
            rename_map = {c: f'{c}{suffix}' for c in ext_df.columns if c != 'Date'}
            ext_df = ext_df.rename(columns=rename_map)
            df = pd.merge_asof(df, ext_df, on='Date', direction='backward')
            external_sources[flt.source] = suffix
    if needs_ext_merge and is_multi_ticker:
        df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    # Add shift columns for increasing/decreasing conditions (with lookback and EMA smoothing)
    for flt in filters:
        col_name = flt.metric
        if flt.source != 'self':
            col_name = f'{flt.metric}__{flt.source}'
        # For Return metric, use the computed column
        if flt.metric == 'Return':
            col_name = f'__return_{flt.lookback or 252}'
        if flt.condition in ('increasing', 'decreasing') and col_name in df.columns:
            lookback = flt.lookback or 1
            # EMA smoothing for noisy metrics
            col_for_shift = col_name
            if flt.metric in ('Volume', 'RV_EMA') and flt.condition in ('increasing', 'decreasing'):
                smooth_col = f'{col_name}__ema10'
                if smooth_col not in df.columns:
                    if is_multi_ticker:
                        df[smooth_col] = df.groupby('Ticker')[col_name].transform(lambda x: x.ewm(span=10).mean())
                    else:
                        df[smooth_col] = df[col_name].ewm(span=10).mean()
                col_for_shift = smooth_col
            shift_col = f'{col_for_shift}__prev_{lookback}'
            if shift_col not in df.columns:
                if is_multi_ticker:
                    df[shift_col] = df.groupby('Ticker')[col_for_shift].shift(lookback)
                else:
                    df[shift_col] = df[col_for_shift].shift(lookback)

    # Build custom exit/stop date indexes if needed
    exit_date_index = None
    stop_date_index = None

    def _build_signal_date_index(signal_name):
        sig_is = EXIT_IS_COL.get(signal_name)
        if not sig_is or sig_is not in df.columns:
            return None
        if is_multi_ticker:
            idx = {}
            for tkr, grp in df.groupby('Ticker'):
                idx[tkr] = grp[grp[sig_is] == True]['Date'].values
            return idx
        else:
            return df[df[sig_is] == True]['Date'].values

    if use_custom_exit:
        exit_date_index = _build_signal_date_index(effective_exit)
        if stop_signal:
            stop_date_index = _build_signal_date_index(stop_signal)

    # Find entry rows
    entries = df[df[is_col] == True].copy()

    # Build trades
    trades = []
    for _, row in entries.iterrows():
        entry_date = row['Date']

        # For basket_tickers: skip trades where ticker wasn't in the basket at entry date
        if is_multi_ticker and _quarter_bounds:
            ticker = row.get('Ticker', '')
            active_tickers = None
            for q_start, q_tickers in _quarter_bounds:
                if q_start <= entry_date:
                    active_tickers = q_tickers
                else:
                    break
            if active_tickers is None:
                active_tickers = _quarter_bounds[0][1]
            if ticker not in active_tickers:
                continue

        entry_price = row.get(f'{sig}_Entry_Price')
        if pd.isna(entry_price) or entry_price is None or entry_price == 0:
            entry_price = row.get('Close', 0)

        if use_custom_exit:
            entry_np = np.datetime64(entry_date)
            tkr = row.get('Ticker', '') if is_multi_ticker else ''
            ep = float(entry_price)
            if ep == 0:
                continue

            # Collect exit candidates: (date, exit_price_override or None)
            candidates = []

            # Signal-based exit target
            if exit_date_index is not None:
                earr = exit_date_index.get(tkr) if is_multi_ticker else exit_date_index
                if earr is not None and len(earr) > 0:
                    ei = np.searchsorted(earr, entry_np, side='right')
                    if ei < len(earr):
                        candidates.append((pd.Timestamp(earr[ei]), None))

            # Signal-based stop
            if stop_date_index is not None:
                sarr = stop_date_index.get(tkr) if is_multi_ticker else stop_date_index
                if sarr is not None and len(sarr) > 0:
                    si = np.searchsorted(sarr, entry_np, side='right')
                    if si < len(sarr):
                        candidates.append((pd.Timestamp(sarr[si]), None))

            # RV-based price targets (forward scan through High/Low)
            if (exit_rv_multiple is not None or stop_rv_multiple is not None):
                rv_ema = row.get('RV_EMA')
                if pd.notna(rv_ema) and float(rv_ema) > 0:
                    rv = float(rv_ema)
                    if is_multi_ticker:
                        fwd = df[(df['Ticker'] == tkr) & (df['Date'] > entry_date)]
                    else:
                        fwd = df[df['Date'] > entry_date]
                    if not fwd.empty and 'High' in fwd.columns and 'Low' in fwd.columns:
                        if exit_rv_multiple is not None:
                            if direction == 'long':
                                target_px = ep * (1 + exit_rv_multiple * rv)
                                hits = fwd[fwd['High'] >= target_px]
                            else:
                                target_px = ep * (1 - exit_rv_multiple * rv)
                                hits = fwd[fwd['Low'] <= target_px]
                            if not hits.empty:
                                candidates.append((hits.iloc[0]['Date'], target_px))
                        if stop_rv_multiple is not None:
                            if direction == 'long':
                                stop_px = ep * (1 - stop_rv_multiple * rv)
                                hits = fwd[fwd['Low'] <= stop_px]
                            else:
                                stop_px = ep * (1 + stop_rv_multiple * rv)
                                hits = fwd[fwd['High'] >= stop_px]
                            if not hits.empty:
                                candidates.append((hits.iloc[0]['Date'], stop_px))

            # Trailing RVol stop (day-by-day scan — stop ratchets with new extremes)
            if trailing_stop_rv_multiple is not None:
                rv_ema = row.get('RV_EMA')
                if pd.notna(rv_ema) and float(rv_ema) > 0:
                    rv = float(rv_ema)
                    trail_offset = trailing_stop_rv_multiple * rv * ep
                    if is_multi_ticker:
                        fwd_t = df[(df['Ticker'] == tkr) & (df['Date'] > entry_date)]
                    else:
                        fwd_t = df[df['Date'] > entry_date]
                    # Limit scan horizon to earliest other candidate (optimization)
                    if candidates:
                        horizon = min(c[0] for c in candidates)
                        fwd_t = fwd_t[fwd_t['Date'] <= horizon]
                    if not fwd_t.empty and 'High' in fwd_t.columns and 'Low' in fwd_t.columns:
                        if direction == 'long':
                            best = ep  # best high seen
                            stop_lvl = ep - trail_offset
                            for _, frow in fwd_t.iterrows():
                                fh = float(frow['High']) if pd.notna(frow.get('High')) else best
                                fl = float(frow['Low']) if pd.notna(frow.get('Low')) else fh
                                if fh > best:
                                    best = fh
                                    stop_lvl = best - trail_offset
                                if fl <= stop_lvl:
                                    candidates.append((frow['Date'], stop_lvl))
                                    break
                        else:
                            best = ep  # best low seen
                            stop_lvl = ep + trail_offset
                            for _, frow in fwd_t.iterrows():
                                fl = float(frow['Low']) if pd.notna(frow.get('Low')) else best
                                fh = float(frow['High']) if pd.notna(frow.get('High')) else fl
                                if fl < best:
                                    best = fl
                                    stop_lvl = best + trail_offset
                                if fh >= stop_lvl:
                                    candidates.append((frow['Date'], stop_lvl))
                                    break

            if not candidates:
                continue

            # Take earliest; on tie prefer stop (conservative)
            candidates.sort(key=lambda c: c[0])
            exit_dt = candidates[0][0]
            exit_price_override = candidates[0][1]

            # Get exit price and compute MFE/MAE from High/Low
            if is_multi_ticker:
                tkr_df = df[(df['Ticker'] == tkr) & (df['Date'] >= entry_date) & (df['Date'] <= exit_dt)]
            else:
                tkr_df = df[(df['Date'] >= entry_date) & (df['Date'] <= exit_dt)]
            if tkr_df.empty:
                continue
            if exit_price_override is not None:
                exit_price = exit_price_override
            else:
                exit_row = tkr_df[tkr_df['Date'] == exit_dt]
                exit_price = float(exit_row['Close'].iloc[0]) if not exit_row.empty else float(tkr_df['Close'].iloc[-1])
            if direction == 'long':
                final_change = (exit_price - ep) / ep
                max_high = float(tkr_df['High'].max()) if 'High' in tkr_df.columns else exit_price
                min_low = float(tkr_df['Low'].min()) if 'Low' in tkr_df.columns else exit_price
                mfe = (max_high - ep) / ep
                mae = (min_low - ep) / ep
            else:
                final_change = (ep - exit_price) / ep
                max_high = float(tkr_df['High'].max()) if 'High' in tkr_df.columns else exit_price
                min_low = float(tkr_df['Low'].min()) if 'Low' in tkr_df.columns else exit_price
                mfe = (ep - min_low) / ep
                mae = (ep - max_high) / ep
        else:
            # Default exit: use pre-computed columns
            exit_date_val = row.get(f'{sig}_Exit_Date')
            exit_price = row.get(f'{sig}_Exit_Price')
            final_change = row.get(f'{sig}_Final_Change')
            mfe = row.get(f'{sig}_MFE')
            mae = row.get(f'{sig}_MAE')
            if pd.isna(exit_date_val) or pd.isna(exit_price):
                continue
            exit_dt = pd.Timestamp(exit_date_val)

        bars_held = max(1, int(np.busday_count(entry_date.date(), exit_dt.date())))
        trade_return = float(final_change) if pd.notna(final_change) else 0.0

        # Apply regime filters
        regime_pass = True
        for flt in filters:
            if flt.source != 'self' and flt.source in failed_sources:
                continue
            col_name = flt.metric
            if flt.source != 'self':
                col_name = f'{flt.metric}__{flt.source}'
            # For Return metric, use computed column
            if flt.metric == 'Return':
                col_name = f'__return_{flt.lookback or 252}'
            val = row.get(col_name)
            if val is None and col_name not in row.index:
                continue  # skip filter if column missing
            if flt.condition == 'above':
                regime_pass = regime_pass and (pd.notna(val) and float(val) > flt.value)
            elif flt.condition == 'below':
                regime_pass = regime_pass and (pd.notna(val) and float(val) < flt.value)
            elif flt.condition == 'increasing':
                lookback = flt.lookback or 1
                actual_col = col_name
                if flt.metric in ('Volume', 'RV_EMA'):
                    actual_col = f'{col_name}__ema10'
                prev_col = f'{actual_col}__prev_{lookback}'
                cur = row.get(actual_col if flt.metric in ('Volume', 'RV_EMA') else col_name)
                prev_val = row.get(prev_col)
                regime_pass = regime_pass and (pd.notna(cur) and pd.notna(prev_val) and float(cur) > float(prev_val))
            elif flt.condition == 'decreasing':
                lookback = flt.lookback or 1
                actual_col = col_name
                if flt.metric in ('Volume', 'RV_EMA'):
                    actual_col = f'{col_name}__ema10'
                prev_col = f'{actual_col}__prev_{lookback}'
                cur = row.get(actual_col if flt.metric in ('Volume', 'RV_EMA') else col_name)
                prev_val = row.get(prev_col)
                regime_pass = regime_pass and (pd.notna(cur) and pd.notna(prev_val) and float(cur) < float(prev_val))
            elif flt.condition == 'equals_true':
                regime_pass = regime_pass and (pd.notna(val) and bool(val))
            elif flt.condition == 'equals_false':
                regime_pass = regime_pass and (pd.notna(val) and not bool(val))

        trade_dict = {
            'entry_date': entry_date.strftime('%Y-%m-%d'),
            'exit_date': exit_dt.strftime('%Y-%m-%d'),
            'entry_price': safe_float(entry_price, 2),
            'exit_price': safe_float(exit_price, 2),
            'change': safe_float(trade_return, 4),
            'mfe': safe_float(mfe, 4),
            'mae': safe_float(mae, 4),
            'bars_held': bars_held,
            'regime_pass': regime_pass,
            'entry_weight': None,
            'exit_weight': None,
            'contribution': None,
        }
        if is_multi_ticker:
            trade_dict['ticker'] = row.get('Ticker', '')
        trades.append(trade_dict)

    # Build ticker_closes for path computation
    if is_multi_ticker:
        ticker_closes = {}
        for tkr, grp in df.groupby('Ticker'):
            ticker_closes[tkr] = grp.set_index('Date')['Close'].sort_index()
    else:
        closes = df.set_index('Date')['Close'].sort_index()
        ticker_closes = {'': closes}

    return trades, df, ticker_closes, direction


@app.get("/api/date-range/{target_type}/{target}")
def get_date_range(target_type: str, target: str):
    """Return min/max date for a basket or ticker so the frontend can constrain date pickers."""
    if target_type in ('basket', 'basket_tickers'):
        basket_file = _find_basket_parquet(target)
        if not basket_file:
            raise HTTPException(status_code=404, detail=f"Basket file not found for {target}")
        dates = pd.read_parquet(basket_file, columns=['Date'])['Date']
    elif target_type == 'etf':
        if not ETF_SIGNALS_FILE.exists():
            raise HTTPException(status_code=404, detail="ETF signals file not found")
        dates = pd.read_parquet(ETF_SIGNALS_FILE, columns=['Ticker', 'Date'],
                                filters=[('Ticker', '==', target)])['Date']
    else:
        df = _read_ticker_parquet(target, columns=['Ticker', 'Date'])
        if df.empty:
            raise HTTPException(status_code=404, detail="Signals file not found")
        dates = df['Date']
    dates = pd.to_datetime(dates)
    if dates.empty:
        raise HTTPException(status_code=404, detail="No data found")
    return {"min": dates.min().strftime('%Y-%m-%d'), "max": dates.max().strftime('%Y-%m-%d')}

def _build_buy_hold(target, target_type, start_date, end_date, direction='long'):
    """Build a buy-and-hold backtest result from the Close series."""
    if target_type == 'basket':
        basket_file = _find_basket_parquet(target)
        if not basket_file:
            raise HTTPException(status_code=404, detail=f"Basket file not found for {target}")
        df = pd.read_parquet(basket_file, columns=['Date', 'Close'])
    elif target_type == 'basket_tickers':
        raise HTTPException(status_code=400, detail="Long/Short not supported for basket_tickers mode — use basket mode")
    elif target_type == 'etf':
        if not ETF_SIGNALS_FILE.exists():
            raise HTTPException(status_code=404, detail="ETF signals file not found")
        df = pd.read_parquet(ETF_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                             filters=[('Ticker', '==', target)])
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No ETF data for {target}")
    else:
        if not INDIVIDUAL_SIGNALS_FILE.exists():
            raise HTTPException(status_code=404, detail="Signals file not found")
        df = pd.read_parquet(INDIVIDUAL_SIGNALS_FILE, columns=['Ticker', 'Date', 'Close'],
                             filters=[('Ticker', '==', target)])
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data for {target}")
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').reset_index(drop=True)
    date_range = {"min": df['Date'].min().strftime('%Y-%m-%d'), "max": df['Date'].max().strftime('%Y-%m-%d')}
    if start_date:
        df = df[df['Date'] >= pd.Timestamp(start_date)]
    if end_date:
        df = df[df['Date'] <= pd.Timestamp(end_date)]
    if df.empty:
        return {"trades": [], "trade_paths": [], "equity_curve": {"dates": [], "filtered": [], "unfiltered": [], "buy_hold": []},
                "stats": {"filtered": {}, "unfiltered": {}}, "date_range": date_range}
    closes = df.drop_duplicates('Date').set_index('Date')['Close'].sort_index()
    first_close = float(closes.iloc[0])
    last_close = float(closes.iloc[-1])
    if direction == 'long':
        total_return = last_close / first_close - 1 if first_close else 0
        mfe_val = float(closes.max()) / first_close - 1 if first_close else 0
        mae_val = float(closes.min()) / first_close - 1 if first_close else 0
    else:
        total_return = (first_close - last_close) / first_close if first_close else 0
        mfe_val = (first_close - float(closes.min())) / first_close if first_close else 0
        mae_val = (first_close - float(closes.max())) / first_close if first_close else 0
    entry_date = closes.index[0]
    exit_date = closes.index[-1]
    bars = max(1, int(np.busday_count(entry_date.date(), exit_date.date())))
    trade = {
        'entry_date': entry_date.strftime('%Y-%m-%d'),
        'exit_date': exit_date.strftime('%Y-%m-%d'),
        'entry_price': round(first_close, 2),
        'exit_price': round(last_close, 2),
        'change': round(total_return, 6),
        'mfe': round(mfe_val, 6),
        'mae': round(mae_val, 6),
        'bars_held': bars,
        'regime_pass': True,
        'entry_weight': 1.0,
        'exit_weight': 1.0,
        'contribution': round(total_return, 6),
    }
    trade_path = [round(float(c) / first_close - 1, 6) for c in closes.values]
    eq_values = [round(float(c) / first_close, 6) for c in closes.values]
    eq_dates = [d.strftime('%Y-%m-%d') for d in closes.index]
    stats = {
        'trades': 1, 'win_rate': 1.0 if total_return > 0 else 0.0,
        'avg_winner': total_return if total_return > 0 else 0,
        'avg_loser': total_return if total_return <= 0 else 0,
        'ev': total_return, 'profit_factor': float('inf') if total_return > 0 else 0,
        'max_dd': round(float((closes / closes.cummax() - 1).min()), 6),
        'avg_bars': bars,
    }
    return {
        "trades": [trade], "trade_paths": [trade_path],
        "equity_curve": {"dates": eq_dates, "filtered": eq_values, "unfiltered": eq_values, "buy_hold": eq_values},
        "stats": {"filtered": stats, "unfiltered": stats},
        "date_range": date_range,
    }


@app.post("/api/backtest")
def run_backtest(req: BacktestRequest):
    sig = req.entry_signal
    # Handle Long/Short/Buy_Hold via _build_buy_hold
    if sig in ('Buy_Hold', 'Long', 'Short'):
        direction = 'short' if sig == 'Short' else 'long'
        return _build_buy_hold(req.target, req.target_type, req.start_date, req.end_date, direction)

    # Delegate trade building to _build_leg_trades
    trades, df, ticker_closes, direction = _build_leg_trades(
        req.target, req.target_type, sig, req.filters,
        req.start_date, req.end_date, exit_signal=req.exit_signal,
        stop_signal=req.stop_signal, exit_rv_multiple=req.exit_rv_multiple,
        stop_rv_multiple=req.stop_rv_multiple, trailing_stop_rv_multiple=req.trailing_stop_rv_multiple,
        no_exit_target=bool(req.no_exit_target),
    )

    is_multi_ticker = req.target_type == 'basket_tickers'

    # Capture date range from df
    if df.empty:
        return {"trades": [], "trade_paths": [], "equity_curve": {"dates": [], "filtered": [], "unfiltered": []},
                "stats": {"filtered": {}, "unfiltered": {}}, "date_range": {"min": "", "max": ""}}

    date_range = {
        "min": df['Date'].min().strftime('%Y-%m-%d'),
        "max": df['Date'].max().strftime('%Y-%m-%d'),
    }

    # Compute trade paths
    trade_paths = []
    for t in trades:
        ep = t['entry_price']
        tkr = t.get('ticker', '')
        if ep is None or ep == 0:
            trade_paths.append([])
            continue
        ed = pd.Timestamp(t['entry_date'])
        xd = pd.Timestamp(t['exit_date'])
        cs = ticker_closes.get(tkr)
        if cs is None:
            trade_paths.append([])
            continue
        segment = cs[(cs.index >= ed) & (cs.index <= xd)]
        path = [round(float(c) / ep - 1, 6) for c in segment.values]
        trade_paths.append(path)

    # Build daily mark-to-market equity curves with position sizing
    paired = sorted(zip(trades, trade_paths), key=lambda p: p[0]['exit_date'])
    sorted_trades = [p[0] for p in paired]
    sorted_paths = [p[1] for p in paired]
    initial = 1.0
    pos_size = req.position_size
    max_lev = req.max_leverage
    is_long = direction == 'long'

    all_dates = sorted(df['Date'].unique())

    entry_map = {}
    exit_map = {}
    trade_map = {i: t for i, t in enumerate(sorted_trades)}
    for i, t in enumerate(sorted_trades):
        ed = pd.Timestamp(t['entry_date'])
        xd = pd.Timestamp(t['exit_date'])
        entry_map.setdefault(ed, []).append(i)
        exit_map.setdefault(xd, []).append(i)

    def mtm_equity(open_pos, cash, close_date):
        total = cash
        for info in open_pos.values():
            tkr = info.get('ticker', '')
            ep = info['entry_price']
            cs = ticker_closes.get(tkr)
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

    cash_all = initial
    cash_filt = initial
    open_all = {}
    open_filt = {}
    eq_dates = []
    eq_all_vals = []
    eq_filt_vals = []
    daily_positions = {}
    blew_up = None

    for date in all_dates:
        filt_exits_today = [idx for idx in exit_map.get(date, []) if idx in open_filt]
        pre_exit_eq_filt = mtm_equity(open_filt, cash_filt, date) if filt_exits_today else 0
        for idx in exit_map.get(date, []):
            t = trade_map[idx]
            ret = t['change'] or 0.0
            if idx in open_all:
                a = open_all.pop(idx)
                cash_all += a['alloc'] * (1 + ret)
            if idx in open_filt:
                a = open_filt.pop(idx)
                exit_val = a['alloc'] * (1 + ret)
                cash_filt += exit_val
                ew = a.get('entry_weight', 0)
                trade_map[idx]['entry_weight'] = ew
                trade_map[idx]['exit_weight'] = round(exit_val / pre_exit_eq_filt, 4) if pre_exit_eq_filt > 0 else 0
                trade_map[idx]['contribution'] = round(ew * ret, 4)

        for idx in entry_map.get(date, []):
            t = trade_map[idx]
            eq_est = mtm_equity(open_all, cash_all, date)
            if eq_est <= 0:
                continue
            wanted = eq_est * pos_size * max_lev
            exposure = eq_est - cash_all
            room = max(0, eq_est * max_lev - exposure)
            alloc = min(wanted, room)
            if alloc > 0:
                open_all[idx] = {'alloc': alloc, 'entry_price': t['entry_price'] or 0,
                                  'ticker': t.get('ticker', '')}
                cash_all -= alloc
            if t['regime_pass']:
                eq_est_f = mtm_equity(open_filt, cash_filt, date)
                if eq_est_f > 0:
                    wanted_f = eq_est_f * pos_size * max_lev
                    exposure_f = eq_est_f - cash_filt
                    room_f = max(0, eq_est_f * max_lev - exposure_f)
                    alloc_f = min(wanted_f, room_f)
                    if alloc_f > 0:
                        entry_wt = round(alloc_f / eq_est_f, 4)
                        open_filt[idx] = {'alloc': alloc_f, 'entry_price': t['entry_price'] or 0,
                                           'ticker': t.get('ticker', ''),
                                           'entry_weight': entry_wt}
                        cash_filt -= alloc_f
                        trade_map[idx]['entry_weight'] = entry_wt
                        trade_map[idx]['_was_taken'] = True

        equity_all = mtm_equity(open_all, cash_all, date)
        equity_filt = mtm_equity(open_filt, cash_filt, date)
        di = len(eq_dates)
        eq_dates.append(pd.Timestamp(date).strftime('%Y-%m-%d'))
        eq_all_vals.append(round(equity_all, 2))
        eq_filt_vals.append(round(equity_filt, 2))
        if equity_all <= 0 and blew_up is None:
            blew_up = {"date": eq_dates[-1], "trade_index": -1, "equity": "unfiltered"}
        if equity_filt <= 0 and blew_up is None:
            blew_up = {"date": eq_dates[-1], "trade_index": -1, "equity": "filtered"}

        if open_filt:
            positions = []
            for tidx, info in open_filt.items():
                t = trade_map.get(tidx, {})
                tkr = info.get('ticker', '')
                ep = info['entry_price']
                cs = ticker_closes.get(tkr)
                if cs is not None and ep and ep > 0:
                    cv = cs.asof(date)
                    if pd.notna(cv):
                        current_val = info['alloc'] * (float(cv) / ep) if is_long else info['alloc'] * (2 - float(cv) / ep)
                        daily_ret = float(cv) / ep - 1 if is_long else 1 - float(cv) / ep
                    else:
                        current_val = info['alloc']
                        daily_ret = 0.0
                else:
                    current_val = info['alloc']
                    daily_ret = 0.0
                weight = current_val / equity_filt if equity_filt > 0 else 0
                entry_wt = info.get('entry_weight', 0)
                contribution = entry_wt * daily_ret if equity_filt > 0 else 0
                positions.append({
                    'trade_idx': tidx,
                    'ticker': tkr or None,
                    'entry_date': t.get('entry_date', ''),
                    'alloc': info['alloc'],
                    'entry_weight': info.get('entry_weight', 0),
                    'weight': round(weight, 4),
                    'daily_return': round(daily_ret, 4),
                    'contribution': round(contribution, 6),
                })
            exposure = sum(info['alloc'] for info in open_filt.values())
            daily_positions[di] = {
                'exposure_pct': round(exposure / equity_filt, 4) if equity_filt > 0 else 0,
                'equity': round(equity_filt, 2),
                'positions': positions,
            }

    # Build buy-and-hold curve
    if is_multi_ticker:
        basket_file_bh = _find_basket_parquet(req.target)
        if basket_file_bh:
            bh_raw = pd.read_parquet(basket_file_bh, columns=['Date', 'Close'])
            bh_raw['Date'] = pd.to_datetime(bh_raw['Date'])
            bh_raw = bh_raw.sort_values('Date')
            if req.start_date:
                bh_raw = bh_raw[bh_raw['Date'] >= pd.Timestamp(req.start_date)]
            if req.end_date:
                bh_raw = bh_raw[bh_raw['Date'] <= pd.Timestamp(req.end_date)]
            bh_series = bh_raw.set_index('Date')['Close'].sort_index()
        else:
            bh_series = pd.Series(dtype=float)
    else:
        bh_series = df.drop_duplicates('Date').set_index('Date')['Close'].sort_index()

    bh_vals = []
    if not bh_series.empty:
        first_bh = float(bh_series.iloc[0])
        for d in all_dates:
            v = bh_series.asof(pd.Timestamp(d))
            if pd.notna(v) and first_bh > 0:
                bh_vals.append(round(initial * float(v) / first_bh, 2))
            else:
                bh_vals.append(round(initial, 2))
    else:
        bh_vals = [round(initial, 2)] * len(all_dates)

    # Compute stats
    def compute_stats(trade_list, equity_vals):
        if not trade_list:
            return {'trades': 0, 'trades_met_criteria': 0, 'trades_taken': 0, 'trades_skipped': 0,
                    'win_rate': 0, 'avg_winner': 0, 'avg_loser': 0,
                    'ev': 0, 'profit_factor': 0, 'max_dd': 0, 'avg_bars': 0}
        met_criteria = len(trade_list)
        taken = [t for t in trade_list if t.get('_was_taken')]
        returns = [t['change'] for t in taken if t['change'] is not None]
        winners = [r for r in returns if r > 0]
        losers = [r for r in returns if r <= 0]
        total = len(returns)
        win_rate = len(winners) / total if total > 0 else 0
        avg_winner = sum(winners) / len(winners) if winners else 0
        avg_loser = sum(losers) / len(losers) if losers else 0
        ev = sum(returns) / total if total > 0 else 0
        gross_profit = sum(winners)
        gross_loss = abs(sum(losers))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999 if gross_profit > 0 else 0)
        max_dd = 0.0
        if equity_vals:
            peak = equity_vals[0]
            for v in equity_vals:
                peak = max(peak, v)
                if peak > 0:
                    dd = (peak - v) / peak
                    max_dd = max(max_dd, dd)
        avg_bars = sum(t['bars_held'] for t in taken) / len(taken) if taken else 0
        return {
            'trades': total, 'trades_met_criteria': met_criteria,
            'trades_taken': total, 'trades_skipped': met_criteria - total,
            'win_rate': round(win_rate, 4), 'avg_winner': round(avg_winner, 4),
            'avg_loser': round(avg_loser, 4), 'ev': round(ev, 4),
            'profit_factor': round(profit_factor, 2), 'max_dd': round(max_dd, 4),
            'avg_bars': round(avg_bars, 1),
        }

    filtered_trades = [t for t in trades if t['regime_pass']]
    stats_filtered = compute_stats(filtered_trades, eq_filt_vals)
    stats_unfiltered = compute_stats(trades, eq_all_vals)

    for t in trades:
        t.pop('_was_taken', None)

    resp = {
        "trades": sorted_trades,
        "trade_paths": sorted_paths,
        "equity_curve": {"dates": eq_dates, "filtered": eq_filt_vals, "unfiltered": eq_all_vals, "buy_hold": bh_vals},
        "stats": {"filtered": stats_filtered, "unfiltered": stats_unfiltered},
        "date_range": date_range,
        "daily_positions": daily_positions if daily_positions else None,
    }
    if blew_up:
        resp["blew_up"] = blew_up
    return resp


@app.post("/api/backtest/multi")
def run_multi_backtest(req: MultiBacktestRequest):
    """Run a multi-leg backtest and return combined equity curves + per-leg stats."""
    initial = 1.0
    max_lev = req.max_leverage

    # ---- helpers ----
    def compute_stats(trade_list, all_trades_count, equity_vals):
        """Compute portfolio + trade stats from a trade list and equity curve.
        all_trades_count: total trades that met signal criteria (before leverage/cash filtering).
        trade_list: trades that were actually taken (regime_pass=True and allocated capital).
        """
        empty = {
            'portfolio': {
                'strategy_return': 0, 'cagr': 0, 'volatility': 0, 'max_dd': 0,
                'sharpe': 0, 'sortino': 0,
            },
            'trade': {
                'trades_met_criteria': all_trades_count,
                'trades_taken': 0, 'trades_skipped': 0,
                'win_rate': 0, 'avg_winner': 0, 'avg_loser': 0,
                'ev': 0, 'profit_factor': 0,
                'avg_time_winner': 0, 'avg_time_loser': 0,
            },
        }
        if not equity_vals or len(equity_vals) < 2:
            return empty

        # ---- Portfolio stats from equity curve ----
        eq = np.array(equity_vals, dtype=float)
        daily_rets = np.diff(eq) / eq[:-1]
        daily_rets = daily_rets[np.isfinite(daily_rets)]

        strategy_return = eq[-1] / eq[0] - 1 if eq[0] > 0 else 0
        years = len(daily_rets) / 252 if len(daily_rets) > 0 else 0
        cagr = (eq[-1] / eq[0]) ** (1 / years) - 1 if years > 0 and eq[0] > 0 and eq[-1] > 0 else 0
        volatility = float(np.std(daily_rets) * np.sqrt(252)) if len(daily_rets) > 1 else 0
        mean_daily = float(np.mean(daily_rets)) if len(daily_rets) > 0 else 0
        std_daily = float(np.std(daily_rets)) if len(daily_rets) > 1 else 0
        sharpe = (mean_daily / std_daily * np.sqrt(252)) if std_daily > 0 else 0
        downside = daily_rets[daily_rets < 0]
        downside_std = float(np.std(downside)) if len(downside) > 1 else 0
        sortino = (mean_daily / downside_std * np.sqrt(252)) if downside_std > 0 else 0

        max_dd = 0.0
        peak = eq[0]
        for v in eq:
            peak = max(peak, v)
            if peak > 0:
                dd = (peak - v) / peak
                max_dd = max(max_dd, dd)

        # ---- Trade stats ----
        taken = [t for t in trade_list if t.get('_was_taken')]
        skipped = all_trades_count - len(taken)
        returns = [t['change'] for t in taken if t['change'] is not None]
        winners = [r for r in returns if r > 0]
        losers = [r for r in returns if r <= 0]
        total = len(returns)
        win_rate = len(winners) / total if total > 0 else 0
        avg_winner = sum(winners) / len(winners) if winners else 0
        avg_loser = sum(losers) / len(losers) if losers else 0
        ev = sum(returns) / total if total > 0 else 0
        gross_profit = sum(winners)
        gross_loss = abs(sum(losers))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999 if gross_profit > 0 else 0)

        winner_trades = [t for t in taken if t['change'] is not None and t['change'] > 0]
        loser_trades = [t for t in taken if t['change'] is not None and t['change'] <= 0]
        avg_time_winner = sum(t['bars_held'] for t in winner_trades) / len(winner_trades) if winner_trades else 0
        avg_time_loser = sum(t['bars_held'] for t in loser_trades) / len(loser_trades) if loser_trades else 0

        return {
            'portfolio': {
                'strategy_return': round(strategy_return, 4),
                'cagr': round(float(cagr), 4),
                'volatility': round(volatility, 4),
                'max_dd': round(max_dd, 4),
                'sharpe': round(float(sharpe), 2),
                'sortino': round(float(sortino), 2),
            },
            'trade': {
                'trades_met_criteria': all_trades_count,
                'trades_taken': total,
                'trades_skipped': max(0, skipped),
                'win_rate': round(win_rate, 4),
                'avg_winner': round(avg_winner, 4),
                'avg_loser': round(avg_loser, 4),
                'ev': round(ev, 4),
                'profit_factor': round(profit_factor, 2),
                'avg_time_winner': round(avg_time_winner, 1),
                'avg_time_loser': round(avg_time_loser, 1),
            },
        }

    # ---- build trades for each leg ----
    leg_results = []   # list of dicts per leg
    all_leg_dates = set()
    global_date_min = None
    global_date_max = None
    for leg in req.legs:
        trades, df, ticker_closes, direction = _build_leg_trades(
            leg.target, leg.target_type, leg.entry_signal,
            leg.filters, req.start_date, req.end_date,
            exit_signal=leg.exit_signal, stop_signal=leg.stop_signal,
            exit_rv_multiple=leg.exit_rv_multiple,
            stop_rv_multiple=leg.stop_rv_multiple,
            trailing_stop_rv_multiple=leg.trailing_stop_rv_multiple,
            no_exit_target=bool(leg.no_exit_target),
        )
        if not df.empty:
            dates_in_leg = sorted(df['Date'].unique())
            all_leg_dates.update(dates_in_leg)
            leg_min = df['Date'].min()
            leg_max = df['Date'].max()
            if global_date_min is None or leg_min < global_date_min:
                global_date_min = leg_min
            if global_date_max is None or leg_max > global_date_max:
                global_date_max = leg_max
        else:
            dates_in_leg = []

        leg_results.append({
            'leg': leg,
            'trades': trades,
            'df': df,
            'ticker_closes': ticker_closes,
            'direction': direction,
            'dates': dates_in_leg,
        })

    # Unified date axis across all legs
    all_dates = sorted(all_leg_dates)
    if not all_dates:
        return {
            "legs": [],
            "combined": {
                "equity_curve": {"dates": [], "combined": [], "per_leg": [], "buy_hold": []},
                "stats": {},
            },
            "date_range": {"min": "", "max": ""},
        }

    date_range = {
        "min": global_date_min.strftime('%Y-%m-%d') if global_date_min else "",
        "max": global_date_max.strftime('%Y-%m-%d') if global_date_max else "",
    }

    # ---- per-leg equity curves (allocated capital, mark-to-market) ----
    per_leg_equity = []      # allocated (for combined sum)
    per_leg_standalone = []  # standalone full-portfolio (for per-leg chart display)
    per_leg_stats = []
    leg_responses = []       # final leg response dicts
    skipped_entries = []
    daily_positions = {}     # date_str -> list of position info

    for li, lr in enumerate(leg_results):
        leg = lr['leg']
        trades = lr['trades']
        direction = lr['direction']
        ticker_closes = lr['ticker_closes']
        is_long = direction == 'long'
        alloc_frac = leg.allocation_pct
        pos_size = leg.position_size
        # Leverage multiplies position sizes: entry = equity * pos_size * max_lev
        # and sets max exposure per leg: alloc * max_lev (combined), full * max_lev (standalone)
        leg_initial = initial * alloc_frac       # starting capital for combined
        leg_standalone = initial                  # starting capital for standalone

        # Sort trades by entry date
        sorted_trades = sorted(trades, key=lambda t: t['entry_date'])

        # Build entry/exit maps
        entry_map = {}
        exit_map = {}
        trade_map = {i: t for i, t in enumerate(sorted_trades)}
        for i, t in enumerate(sorted_trades):
            ed = pd.Timestamp(t['entry_date'])
            xd = pd.Timestamp(t['exit_date'])
            entry_map.setdefault(ed, []).append(i)
            exit_map.setdefault(xd, []).append(i)

        def mtm_equity(open_pos, cash, close_date):
            total = cash
            for info in open_pos.values():
                tkr = info.get('ticker', '')
                ep = info['entry_price']
                cs = ticker_closes.get(tkr)
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

        # Two parallel simulations: allocated (for combined) and standalone (for per-leg display)
        cash_alloc = leg_initial
        cash_solo = leg_standalone
        open_alloc = {}
        open_solo = {}
        eq_vals = []        # allocated (for combined sum)
        eq_solo_vals = []   # standalone (for per-leg chart)

        for di, date in enumerate(all_dates):
            # Process exits
            alloc_exits_today = [idx for idx in exit_map.get(date, []) if idx in open_alloc]
            pre_exit_eq_alloc = mtm_equity(open_alloc, cash_alloc, date) if alloc_exits_today else 0
            for idx in exit_map.get(date, []):
                t = trade_map[idx]
                ret = t['change'] or 0.0
                if idx in open_alloc:
                    a = open_alloc.pop(idx)
                    exit_val = a['alloc'] * (1 + ret)
                    cash_alloc += exit_val
                    # Store alloc for post-processing (recompute vs combined equity)
                    trade_map[idx]['_alloc'] = a['alloc']
                if idx in open_solo:
                    a = open_solo.pop(idx)
                    cash_solo += a['alloc'] * (1 + ret)

            # Process entries
            for idx in entry_map.get(date, []):
                t = trade_map[idx]
                if not t['regime_pass']:
                    continue
                # Allocated path: position = equity * pos_size * max_lev, capped by leg allocation * max_lev
                eq_est = mtm_equity(open_alloc, cash_alloc, date)
                if eq_est > 0:
                    wanted = eq_est * pos_size * max_lev
                    exposure = eq_est - cash_alloc  # MTM exposure
                    cap = eq_est * max_lev
                    room = max(0, cap - exposure)
                    alloc = min(wanted, room)
                    if alloc > 0:
                        entry_wt = round(alloc / eq_est, 4)
                        open_alloc[idx] = {'alloc': alloc, 'entry_price': t['entry_price'] or 0, 'ticker': t.get('ticker', ''),
                                           'entry_weight': entry_wt}
                        cash_alloc -= alloc
                        trade_map[idx]['_was_taken'] = True
                # Standalone path: same leverage multiplier on position sizing
                eq_est_s = mtm_equity(open_solo, cash_solo, date)
                if eq_est_s > 0:
                    wanted_s = eq_est_s * pos_size * max_lev
                    exposure_s = eq_est_s - cash_solo  # MTM exposure
                    cap_s = eq_est_s * max_lev
                    room_s = max(0, cap_s - exposure_s)
                    alloc_s = min(wanted_s, room_s)
                    if alloc_s > 0:
                        open_solo[idx] = {'alloc': alloc_s, 'entry_price': t['entry_price'] or 0, 'ticker': t.get('ticker', '')}
                        cash_solo -= alloc_s
                    else:
                        skipped_entries.append({'leg': li, 'date': pd.Timestamp(date).strftime('%Y-%m-%d'), 'reason': 'leverage_limit'})
                else:
                    skipped_entries.append({'leg': li, 'date': pd.Timestamp(date).strftime('%Y-%m-%d'), 'reason': 'equity_zero'})

            # Record daily equity (both paths)
            equity = mtm_equity(open_alloc, cash_alloc, date)
            eq_vals.append(round(equity, 2))
            equity_solo = mtm_equity(open_solo, cash_solo, date)
            eq_solo_vals.append(round(equity_solo, 2))

            # Track daily positions for constituents overlay (keyed by date index)
            if open_alloc:
                eq_now = equity
                positions = daily_positions.get(di, {'exposure_pct': 0, 'equity': 0, 'positions': []})
                for idx_p, info in open_alloc.items():
                    t_p = trade_map.get(idx_p, {})
                    tkr = info.get('ticker', '')
                    ep = info['entry_price']
                    cs = ticker_closes.get(tkr)
                    if cs is not None and ep and ep > 0:
                        cv = cs.asof(date)
                        if pd.notna(cv):
                            current_val = info['alloc'] * (float(cv) / ep) if is_long else info['alloc'] * (2 - float(cv) / ep)
                            daily_ret = float(cv) / ep - 1 if is_long else 1 - float(cv) / ep
                        else:
                            current_val = info['alloc']
                            daily_ret = 0.0
                    else:
                        current_val = info['alloc']
                        daily_ret = 0.0
                    weight = current_val / eq_now if eq_now > 0 else 0
                    entry_wt = info.get('entry_weight', 0)
                    contribution = entry_wt * daily_ret if eq_now > 0 else 0
                    positions['positions'].append({
                        'trade_idx': idx_p,
                        'ticker': tkr or leg.target,
                        'entry_date': t_p.get('entry_date', ''),
                        'leg_target': leg.target,
                        'alloc': info['alloc'],
                        'entry_weight': info.get('entry_weight', 0),
                        'weight': round(weight, 4),
                        'daily_return': round(daily_ret, 4),
                        'contribution': round(contribution, 6),
                    })
                positions['equity'] = round(eq_now, 2)
                exposure = sum(info['alloc'] for info in open_alloc.values())
                positions['exposure_pct'] = round(exposure / eq_now, 4) if eq_now > 0 else 0
                daily_positions[di] = positions

        per_leg_equity.append(eq_vals)        # allocated (for combined sum)
        per_leg_standalone.append(eq_solo_vals)  # standalone (for per-leg chart)

        # Per-leg stats (from standalone curve — what the user sees)
        filtered_trades = [t for t in sorted_trades if t['regime_pass']]
        stats = compute_stats(filtered_trades, len(sorted_trades), eq_solo_vals)
        per_leg_stats.append(stats)

        # Build trade paths for this leg
        leg_trade_paths = []
        for t in sorted_trades:
            ep = t['entry_price']
            tkr = t.get('ticker', '')
            if ep is None or ep == 0:
                leg_trade_paths.append([])
                continue
            ed = pd.Timestamp(t['entry_date'])
            xd = pd.Timestamp(t['exit_date'])
            cs = ticker_closes.get(tkr)
            if cs is None:
                leg_trade_paths.append([])
                continue
            segment = cs[(cs.index >= ed) & (cs.index <= xd)]
            path = [round(float(c) / ep - 1, 6) for c in segment.values]
            leg_trade_paths.append(path)

        leg_responses.append({
            'target': leg.target,
            'target_type': leg.target_type,
            'entry_signal': leg.entry_signal,
            'allocation_pct': leg.allocation_pct,
            'direction': direction,
            'trades': sorted_trades,
            'trade_paths': leg_trade_paths,
            'stats': stats,
        })

    # ---- combined equity curve (sum of per-leg curves) ----
    combined_equity = []
    for i in range(len(all_dates)):
        total = sum(per_leg_equity[li][i] for li in range(len(leg_results))
                    if i < len(per_leg_equity[li]))
        combined_equity.append(round(total, 2))

    # Combined stats (must run BEFORE _was_taken cleanup)
    all_filtered_trades = []
    all_total_trades = 0
    for lr in leg_responses:
        all_filtered_trades.extend([t for t in lr['trades'] if t['regime_pass']])
        all_total_trades += len(lr['trades'])
    combined_stats = compute_stats(all_filtered_trades, all_total_trades, combined_equity)

    # Fix trade entry/exit weights: recompute relative to combined equity (not per-leg)
    ceq_by_date = dict(zip([pd.Timestamp(d).strftime('%Y-%m-%d') for d in all_dates], combined_equity))
    for lr in leg_responses:
        for t in lr['trades']:
            t.pop('_was_taken', None)
            alloc = t.pop('_alloc', None)
            if alloc is not None:
                entry_eq = ceq_by_date.get(t['entry_date'], 0)
                exit_eq = ceq_by_date.get(t['exit_date'], 0)
                ret = t['change'] or 0.0
                exit_val = alloc * (1 + ret)
                t['entry_weight'] = round(alloc / entry_eq, 4) if entry_eq > 0 else 0
                t['exit_weight'] = round(exit_val / exit_eq, 4) if exit_eq > 0 else 0
                t['contribution'] = round(t['entry_weight'] * ret, 4)

    # Fix daily_positions: recompute ALL weights relative to combined equity
    for di, dp in daily_positions.items():
        if di < len(combined_equity):
            ceq = combined_equity[di]
            dp['equity'] = ceq
            total_alloc = sum(p['alloc'] for p in dp['positions'])
            dp['exposure_pct'] = round(total_alloc / ceq, 4) if ceq > 0 else 0
            if ceq > 0:
                for p in dp['positions']:
                    alloc = p['alloc']
                    dr = p.get('daily_return', 0)
                    current_val = alloc * (1 + dr)
                    p['weight'] = round(current_val / ceq, 4)
                    entry_eq = ceq_by_date.get(p.get('entry_date', ''), ceq)
                    p['entry_weight'] = round(alloc / entry_eq, 4) if entry_eq > 0 else 0
                    p['contribution'] = round(p['entry_weight'] * dr, 6) if dr else 0

    # ---- buy-hold curve from first basket leg (or first leg overall) ----
    bh_vals = []
    bh_leg = None
    for lr in leg_results:
        if lr['leg'].target_type in ('basket', 'basket_tickers'):
            bh_leg = lr
            break
    if bh_leg is None and leg_results:
        bh_leg = leg_results[0]

    if bh_leg is not None:
        is_multi_ticker_bh = bh_leg['leg'].target_type == 'basket_tickers'
        if is_multi_ticker_bh:
            basket_file_bh = _find_basket_parquet(bh_leg['leg'].target)
            if basket_file_bh:
                bh_raw = pd.read_parquet(basket_file_bh, columns=['Date', 'Close'])
                bh_raw['Date'] = pd.to_datetime(bh_raw['Date'])
                bh_raw = bh_raw.sort_values('Date')
                if req.start_date:
                    bh_raw = bh_raw[bh_raw['Date'] >= pd.Timestamp(req.start_date)]
                if req.end_date:
                    bh_raw = bh_raw[bh_raw['Date'] <= pd.Timestamp(req.end_date)]
                bh_series = bh_raw.set_index('Date')['Close'].sort_index()
            else:
                bh_series = pd.Series(dtype=float)
        elif bh_leg['leg'].target_type == 'basket':
            basket_file_bh = _find_basket_parquet(bh_leg['leg'].target)
            if basket_file_bh:
                bh_raw = pd.read_parquet(basket_file_bh, columns=['Date', 'Close'])
                bh_raw['Date'] = pd.to_datetime(bh_raw['Date'])
                bh_raw = bh_raw.sort_values('Date')
                if req.start_date:
                    bh_raw = bh_raw[bh_raw['Date'] >= pd.Timestamp(req.start_date)]
                if req.end_date:
                    bh_raw = bh_raw[bh_raw['Date'] <= pd.Timestamp(req.end_date)]
                bh_series = bh_raw.set_index('Date')['Close'].sort_index()
            else:
                bh_series = pd.Series(dtype=float)
        else:
            bh_df = bh_leg['df']
            if not bh_df.empty:
                bh_series = bh_df.drop_duplicates('Date').set_index('Date')['Close'].sort_index()
            else:
                bh_series = pd.Series(dtype=float)

        if not bh_series.empty:
            first_bh = float(bh_series.iloc[0])
            for d in all_dates:
                v = bh_series.asof(pd.Timestamp(d))
                if pd.notna(v) and first_bh > 0:
                    bh_vals.append(round(initial * float(v) / first_bh, 2))
                else:
                    bh_vals.append(round(initial, 2))
        else:
            bh_vals = [round(initial, 2)] * len(all_dates)
    else:
        bh_vals = [round(initial, 2)] * len(all_dates)

    # Inter-leg correlation (daily returns of standalone equity curves)
    leg_correlations = {}
    if len(per_leg_standalone) > 1:
        # Compute daily returns for each leg's standalone curve
        leg_daily_rets = []
        for eq_vals in per_leg_standalone:
            eq_arr = np.array(eq_vals, dtype=float)
            dr = np.diff(eq_arr) / eq_arr[:-1]
            dr[~np.isfinite(dr)] = 0.0
            leg_daily_rets.append(dr)
        # Pairwise correlation matrix
        n_legs = len(leg_daily_rets)
        min_len = min(len(dr) for dr in leg_daily_rets)
        if min_len > 10:
            ret_matrix = np.column_stack([dr[:min_len] for dr in leg_daily_rets])
            corr_matrix = np.corrcoef(ret_matrix, rowvar=False)
            for i in range(n_legs):
                target_i = leg_responses[i]['target']
                leg_correlations[target_i] = {}
                for j in range(n_legs):
                    if i != j:
                        target_j = leg_responses[j]['target']
                        leg_correlations[target_i][target_j] = round(float(corr_matrix[i, j]), 4)

    # Per-leg contribution to combined portfolio
    if len(per_leg_equity) > 1 and combined_equity and combined_equity[-1] > 0:
        for i, lr in enumerate(leg_responses):
            leg_end = per_leg_equity[i][-1] if per_leg_equity[i] else 0
            alloc_frac = req.legs[i].allocation_pct
            leg_return = leg_end / (initial * alloc_frac) - 1 if (initial * alloc_frac) > 0 else 0
            lr['stats']['portfolio']['contribution'] = round(alloc_frac * leg_return, 4)
            lr['stats']['portfolio']['allocation'] = round(alloc_frac, 4)
    else:
        for lr in leg_responses:
            lr['stats']['portfolio']['contribution'] = lr['stats']['portfolio']['strategy_return']
            lr['stats']['portfolio']['allocation'] = 1.0

    eq_date_strs = [pd.Timestamp(d).strftime('%Y-%m-%d') for d in all_dates]

    result = {
        "legs": leg_responses,
        "combined": {
            "equity_curve": {
                "dates": eq_date_strs,
                "combined": combined_equity,
                "per_leg": per_leg_standalone,
                "buy_hold": bh_vals,
            },
            "stats": combined_stats,
        },
        "date_range": date_range,
        "daily_positions": daily_positions,
        "skipped_entries": skipped_entries,
        "leg_correlations": leg_correlations if leg_correlations else None,
    }
    if req.equity_only:
        return {"combined": {"equity_curve": result["combined"]["equity_curve"]}}
    return result


@app.websocket("/ws/live/{ticker}")
async def websocket_endpoint(websocket: WebSocket, ticker: str):
    await websocket.accept()
    if not DB_API_KEY:
        await websocket.send_text(json.dumps({"error": "Databento API key missing"}))
        await websocket.close()
        return

    try:
        # Initialize Databento Live client
        live_client = db.Live(key=DB_API_KEY)
        live_client.subscribe(
            dataset=DB_DATASET,
            schema="ohlcv-1m",
            symbols=[ticker],
            stype_in=DB_STYPE_IN
        )

        queue = asyncio.Queue()

        def handle_record(record):
            if not hasattr(record, 'open'):
                return

            # Format record for frontend - Convert UTC to NY
            dt_utc = datetime.fromtimestamp(record.ts_event / 1e9, tz=ZoneInfo("UTC"))
            dt_ny = dt_utc.astimezone(ZoneInfo("America/New_York"))

            # FILTER RTH: Drop anything outside 09:30 - 16:00
            if dt_ny.hour < 9 or (dt_ny.hour == 9 and dt_ny.minute < 30) or dt_ny.hour >= 16:
                return

            data = {
                "time": dt_ny.strftime('%Y-%m-%dT%H:%M:%S'),
                "open": record.open,
                "high": record.high,
                "low": record.low,
                "close": record.close,
                "volume": record.volume
            }
            asyncio.run_coroutine_threadsafe(queue.put(data), asyncio.get_event_loop())

        live_client.add_callback(handle_record)
        thread = asyncio.create_task(asyncio.to_thread(live_client.start))

        try:
            while True:
                data = await queue.get()
                await websocket.send_json(data)
        except WebSocketDisconnect:
            pass
        finally:
            live_client.stop()
            thread.cancel()
    except Exception as e:
        logger.error(f"Error in websocket for {ticker}: {e}")
        await websocket.send_json({"error": str(e)})
    finally:
        try:
            await websocket.close()
        except:
            pass

if __name__ == "__main__":
    import uvicorn
    # Use 0.0.0.0 to allow access from other devices on the network
    uvicorn.run(app, host="0.0.0.0", port=8000)
