"""Universe Construction — Phase 1
Builds quarterly ticker universes from Norgatedata and caches as JSON.
Run: python universe.py
"""
from foundation import (
    # constants
    SIZE, ETF_SIZE, START_YEAR, THEME_SIZE, DIV_THEME_SIZE,
    LOOKBACK_DAYS, MOMENTUM_LOOKBACK_DAYS,
    MARKET_SYMBOL, SECTOR_LIST, INDUSTRY_MIN_STOCKS,
    # paths
    paths, DATA_FOLDER, CACHE_FILE,
    BETA_CACHE_FILE, MOMENTUM_CACHE_FILE, RISK_ADJ_MOM_CACHE_FILE,
    DIVIDEND_CACHE_FILE, SIZE_CACHE_FILE, VOLUME_GROWTH_CACHE_FILE,
    GICS_CACHE_FILE, TICKER_NAMES_FILE, ETF_CACHE_FILE,
    # utilities
    WriteThroughPath, reset_cell_timer,
    # universe helpers
    _universe_to_json, _json_to_universe, is_universe_current,
    _beta_universes_to_json, _json_to_beta_universes,
    _gics_to_json, _json_to_gics,
    _quarter_end_from_key, _quarter_start_from_key,
    get_current_quarter_key,
)
import norgatedata
import pandas as pd
import numpy as np
import json
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# ---------------------------------------------------------------------------
# Module-level universe globals (populated by load_or_build_* functions)
# ---------------------------------------------------------------------------
QUARTER_UNIVERSE = {}
ETF_UNIVERSE = {}
BETA_UNIVERSE = {}
LOW_BETA_UNIVERSE = {}
MOMENTUM_UNIVERSE = {}
MOMENTUM_LOSERS_UNIVERSE = {}
RISK_ADJ_MOM_UNIVERSE = {}
HIGH_YIELD_UNIVERSE = {}
DIV_GROWTH_UNIVERSE = {}
DIV_WITH_GROWTH_UNIVERSE = {}
SIZE_UNIVERSE = {}
VOLUME_GROWTH_UNIVERSE = {}
TICKER_SECTOR = {}
TICKER_SUBINDUSTRY = {}
SECTOR_UNIVERSES = {}
INDUSTRY_UNIVERSES = {}
TICKER_NAMES = {}
INDUSTRY_LIST = []


# ---------------------------------------------------------------------------
# Universe Construction Functions
# ---------------------------------------------------------------------------

def get_quarterly_vol(ticker):
    try:
        df = norgatedata.price_timeseries(
            ticker,
            stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
            padding_setting=norgatedata.PaddingType.NONE,
            timeseriesformat='pandas-dataframe',
        )
        if df is None or df.empty:
            return []
        quarterly = (df['Close'] * df['Volume']).resample('QE-DEC').mean()
        return [(d, ticker, v) for d, v in quarterly.items() if d.year >= START_YEAR and v > 0]
    except Exception:
        return []


def build_quarter_universe():
    us_equities = []
    us_delisted = []

    try:
        us_equities = [s for s in norgatedata.database_symbols('US Equities')
                       if norgatedata.subtype1(s) == 'Equity']
    except Exception:
        pass

    try:
        us_delisted = [s for s in norgatedata.database_symbols('US Equities Delisted')
                       if norgatedata.subtype1(s) == 'Equity']
    except Exception:
        pass

    print(f"US Equities: {len(us_equities)} stocks")
    print(f"US Equities Delisted: {len(us_delisted)} stocks")

    symbols = list(set(us_equities + us_delisted))
    print(f"Total unique equities to analyze: {len(symbols)}")

    all_data = []
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor() as ex:
        for i, rows in enumerate(ex.map(get_quarterly_vol, symbols), start=1):
            all_data.extend(rows)

            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'Vol'])
    universe = {}
    for date, grp in df.groupby('Date'):
        universe[f"{date.year} Q{date.quarter}"] = set(grp.nlargest(SIZE, 'Vol')['Ticker'])

    return universe


def load_or_build_universe():
    if CACHE_FILE.exists():
        try:
            universe = _json_to_universe(CACHE_FILE.read_text(encoding='utf-8'))
            if is_universe_current(universe):
                print("Universe loaded from cache (up to date)")
                return universe
            print("Universe outdated, rebuilding...")
        except Exception:
            print("Universe cache invalid, rebuilding...")

    universe = build_quarter_universe()
    WriteThroughPath(CACHE_FILE).write_text(_universe_to_json(universe))
    latest_key = max(universe.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', ''))))
    print(f"Saved: {CACHE_FILE} ({len(universe.get(latest_key, set()))} tickers, {len(universe)} quarters)")
    return universe


def get_universe(date):
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    # Use previous quarter's ranking to avoid look-ahead bias
    if quarter == 1:
        return QUARTER_UNIVERSE.get(f"{year - 1} Q4", set())
    return QUARTER_UNIVERSE.get(f"{year} Q{quarter - 1}", set())


# --- ETF Universe ---

def build_quarter_etf_universe():
    etf_symbols = []

    for db_name in ('US Equities', 'US Equities Delisted'):
        try:
            symbols = norgatedata.database_symbols(db_name)
            subtypes = {norgatedata.subtype1(s) for s in symbols[:20]}
            print(f"[ETF] {db_name}: sample subtype1 values = {subtypes}")
            etf_symbols.extend(s for s in symbols if norgatedata.subtype1(s) == 'Exchange Traded Product')
        except Exception:
            pass

    etf_symbols = list(set(etf_symbols))
    print(f"[ETF] Total unique ETFs found: {len(etf_symbols)}")

    all_data = []
    total = len(etf_symbols)
    last_milestone = 0

    with ThreadPoolExecutor() as ex:
        for i, rows in enumerate(ex.map(get_quarterly_vol, etf_symbols), start=1):
            all_data.extend(rows)
            percent = int((i / total) * 100) if total else 100
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  [ETF] {milestone}% complete ({i} / {total})")
                last_milestone = milestone

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'Vol'])
    universe = {}
    for date, grp in df.groupby('Date'):
        universe[f"{date.year} Q{date.quarter}"] = set(grp.nlargest(ETF_SIZE, 'Vol')['Ticker'])

    return universe


def load_or_build_etf_universe():
    if ETF_CACHE_FILE.exists():
        try:
            universe = _json_to_universe(ETF_CACHE_FILE.read_text(encoding='utf-8'))
            if is_universe_current(universe):
                print("[ETF] Universe loaded from cache (up to date)")
                return universe
            print("[ETF] Universe outdated, rebuilding...")
        except Exception:
            print("[ETF] Universe cache invalid, rebuilding...")

    universe = build_quarter_etf_universe()
    WriteThroughPath(ETF_CACHE_FILE).write_text(_universe_to_json(universe))
    latest_key = max(universe.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if universe else None
    if latest_key:
        print(f"[ETF] Saved: {ETF_CACHE_FILE} ({len(universe.get(latest_key, set()))} ETFs, {len(universe)} quarters)")
    return universe


# --- Ticker Names Cache ---

def _build_ticker_names():
    """Build ticker -> security name mapping for all stocks + ETFs."""
    all_tickers = sorted(
        {t for tickers in QUARTER_UNIVERSE.values() for t in tickers}
        | {t for tickers in ETF_UNIVERSE.values() for t in tickers}
    )
    # Load existing cache
    existing = {}
    if TICKER_NAMES_FILE.exists():
        try:
            existing = json.loads(TICKER_NAMES_FILE.read_text(encoding='utf-8'))
        except Exception:
            pass

    # Only fetch names for tickers not already cached
    missing = [t for t in all_tickers if t not in existing]
    if not missing:
        print(f"Ticker names loaded from cache ({len(existing)} names)")
        return existing

    print(f"Fetching {len(missing)} ticker names from Norgate...")
    names = dict(existing)
    for t in missing:
        try:
            name = norgatedata.security_name(t)
            if name:
                names[t] = name
        except Exception:
            pass
    WriteThroughPath(TICKER_NAMES_FILE).write_text(json.dumps(names, sort_keys=True))
    print(f"Saved: {TICKER_NAMES_FILE} ({len(names)} names)")
    return names


# --- Beta Universe ---

def _calc_beta_quarterly(ticker, mkt_rets, mkt_var):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or len(df) < LOOKBACK_DAYS:
        return None

    df = df.reset_index()
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df.set_index(date_col, inplace=True)
    stock_rets = df['Close'].pct_change()

    aligned = pd.concat([stock_rets, mkt_rets], axis=1, join='inner')
    aligned.columns = ['Stock', 'Market']
    rolling_cov = aligned['Stock'].rolling(window=LOOKBACK_DAYS).cov(aligned['Market'])
    beta = rolling_cov / mkt_var

    quarterly_beta = beta.resample('QE-DEC').last()
    return quarterly_beta


def _safe_calc_beta(ticker, mkt_rets, mkt_var):
    try:
        result = _calc_beta_quarterly(ticker, mkt_rets, mkt_var)
        return ticker, result
    except Exception:
        return ticker, None


def build_quarter_beta_universes():
    """Build both high-beta and low-beta universes in a single pass."""
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Unique stocks in beta universe: {len(symbols)}")

    mkt_df = norgatedata.price_timeseries(
        MARKET_SYMBOL,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if mkt_df is None or mkt_df.empty:
        raise ValueError(f"Could not load {MARKET_SYMBOL}")

    mkt_df = mkt_df.reset_index()
    date_col = mkt_df.columns[0]
    mkt_df[date_col] = pd.to_datetime(mkt_df[date_col]).dt.normalize()
    mkt_df.set_index(date_col, inplace=True)
    mkt_rets = mkt_df['Close'].pct_change()
    mkt_var = mkt_rets.rolling(window=LOOKBACK_DAYS).var()

    # Single threaded pass to build beta cache for all tickers
    beta_cache = {}
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_beta, t, mkt_rets, mkt_var) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            ticker, beta_series = fut.result()
            if beta_series is not None:
                beta_cache[ticker] = beta_series

            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    # Derive both high-beta and low-beta from the same cache
    high_beta = {}
    low_beta = {}
    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        prev_universe = QUARTER_UNIVERSE[prev_key]
        ranking_date = _quarter_end_from_key(prev_key)

        beta_vals = []
        for t in prev_universe:
            if t in beta_cache and ranking_date in beta_cache[t].index:
                b = beta_cache[t].at[ranking_date]
                if pd.notna(b):
                    beta_vals.append((t, b))
        beta_vals.sort(key=lambda x: x[1])
        low_beta[key] = set(t for t, _ in beta_vals[:THEME_SIZE])
        high_beta[key] = set(t for t, _ in beta_vals[-THEME_SIZE:])

    return high_beta, low_beta


def is_beta_universes_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    high, low = cached
    return current_key in high and current_key in low


def load_or_build_beta_universes():
    if BETA_CACHE_FILE.exists():
        try:
            cached = _json_to_beta_universes(BETA_CACHE_FILE.read_text(encoding='utf-8'))
            if is_beta_universes_current(cached):
                print("Beta universes loaded from cache (up to date)")
                return cached
            print("Beta universes outdated, rebuilding...")
        except Exception:
            print("Beta universe cache invalid, rebuilding...")

    result = build_quarter_beta_universes()
    WriteThroughPath(BETA_CACHE_FILE).write_text(_beta_universes_to_json(result))
    high_beta, low_beta = result
    _latest = max(high_beta.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', ''))))
    print(f"Saved: {BETA_CACHE_FILE} (High Beta: {len(high_beta.get(_latest, set()))}, Low Beta: {len(low_beta.get(_latest, set()))})")
    return result


# --- Momentum Universe ---

def _calc_momentum_quarterly(ticker):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or len(df) < MOMENTUM_LOOKBACK_DAYS:
        return None

    df = df.reset_index()
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df.set_index(date_col, inplace=True)
    mom = df['Close'].pct_change(periods=MOMENTUM_LOOKBACK_DAYS)
    quarterly_mom = mom.resample('QE-DEC').last()
    return quarterly_mom


def _safe_calc_momentum(ticker):
    try:
        result = _calc_momentum_quarterly(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v)]
        return ticker, rows
    except Exception:
        return ticker, []


def build_quarter_momentum_universes():
    """Build both momentum winners and losers universes in a single pass."""
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Unique stocks in momentum universe: {len(symbols)}")

    all_data = []
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_momentum, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            all_data.extend(rows)

            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    if not all_data:
        return {}, {}

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'Momentum'])

    # Derive both winners (top N) and losers (bottom N) from same data
    winners = {}
    losers = {}
    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        ranking_date = _quarter_end_from_key(prev_key)
        prev_universe = QUARTER_UNIVERSE[prev_key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(prev_universe))]
        if grp.empty:
            continue
        sorted_desc = grp.sort_values('Momentum', ascending=False)
        winners[key] = set(sorted_desc.head(THEME_SIZE)['Ticker'])
        losers[key] = set(sorted_desc.tail(THEME_SIZE)['Ticker'])

    return winners, losers


def is_momentum_universes_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    winners, losers = cached
    return current_key in winners and current_key in losers


def load_or_build_momentum_universes():
    if MOMENTUM_CACHE_FILE.exists():
        try:
            d = json.loads(MOMENTUM_CACHE_FILE.read_text(encoding='utf-8'))
            cached = ({k: set(v) for k, v in d['winners'].items()},
                      {k: set(v) for k, v in d['losers'].items()})
            if is_momentum_universes_current(cached):
                print("Momentum universes loaded from cache (up to date)")
                return cached
            print("Momentum universes outdated, rebuilding...")
        except Exception:
            print("Momentum universe cache invalid, rebuilding...")

    result = build_quarter_momentum_universes()
    winners, losers = result
    WriteThroughPath(MOMENTUM_CACHE_FILE).write_text(
        json.dumps({'winners': {k: sorted(v) for k, v in winners.items()},
                    'losers':  {k: sorted(v) for k, v in losers.items()}})
    )
    _latest = max(winners.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if winners else None
    _w_count = len(winners.get(_latest, set())) if _latest else 0
    _l_count = len(losers.get(_latest, set())) if _latest else 0
    print(f"Saved: {MOMENTUM_CACHE_FILE} (Leaders: {_w_count}, Losers: {_l_count})")
    return result


# --- Risk-Adjusted Momentum Universe ---

def _calc_risk_adj_momentum_quarterly(ticker):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or len(df) < MOMENTUM_LOOKBACK_DAYS:
        return None

    df = df.reset_index()
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df.set_index(date_col, inplace=True)
    daily_ret = df['Close'].pct_change()
    trailing_return = df['Close'].pct_change(periods=MOMENTUM_LOOKBACK_DAYS)
    trailing_vol = daily_ret.rolling(MOMENTUM_LOOKBACK_DAYS).std()
    risk_adj = trailing_return / trailing_vol
    quarterly = risk_adj.resample('QE-DEC').last()
    return quarterly


def _safe_calc_risk_adj_momentum(ticker):
    try:
        result = _calc_risk_adj_momentum_quarterly(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v) and np.isfinite(v)]
        return ticker, rows
    except Exception:
        return ticker, []


def build_quarter_risk_adj_momentum():
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Unique stocks in risk-adj momentum universe: {len(symbols)}")

    all_data = []
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_risk_adj_momentum, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            all_data.extend(rows)
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    if not all_data:
        return {}

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'RiskAdjMom'])

    universe = {}
    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        ranking_date = _quarter_end_from_key(prev_key)
        prev_universe = QUARTER_UNIVERSE[prev_key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(prev_universe))]
        if grp.empty:
            continue
        sorted_desc = grp.sort_values('RiskAdjMom', ascending=False)
        universe[key] = set(sorted_desc.head(THEME_SIZE)['Ticker'])

    return universe


def is_risk_adj_momentum_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    return current_key in cached


def load_or_build_risk_adj_momentum():
    if RISK_ADJ_MOM_CACHE_FILE.exists():
        try:
            cached = {k: set(v) for k, v in
                      json.loads(RISK_ADJ_MOM_CACHE_FILE.read_text(encoding='utf-8')).items()}
            if is_risk_adj_momentum_current(cached):
                print("Risk-adj momentum universe loaded from cache (up to date)")
                return cached
            print("Risk-adj momentum universe outdated, rebuilding...")
        except Exception:
            print("Risk-adj momentum cache invalid, rebuilding...")

    result = build_quarter_risk_adj_momentum()
    WriteThroughPath(RISK_ADJ_MOM_CACHE_FILE).write_text(
        json.dumps({k: sorted(v) for k, v in result.items()})
    )
    _latest = max(result.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if result else None
    print(f"Saved: {RISK_ADJ_MOM_CACHE_FILE} ({len(result.get(_latest, set())) if _latest else 0} tickers)")
    return result


# --- Dividend Universe ---

def _calc_dividend_yield_quarterly(ticker):
    """Return a pd.Series of trailing 12M dividend yield % at each quarter-end date."""
    try:
        # Fetch dividend yield timeseries (trailing 12M dividends / price).
        # Try pandas format first; fall back to the default recarray format if needed.
        dy_raw = None
        try:
            dy_raw = norgatedata.dividend_yield_timeseries(
                ticker,
                timeseriesformat='pandas-dataframe',
            )
        except Exception:
            pass
        if dy_raw is None or (hasattr(dy_raw, 'empty') and dy_raw.empty):
            try:
                dy_raw = norgatedata.dividend_yield_timeseries(ticker)
            except Exception:
                return None
        if dy_raw is None:
            return None

        # Normalise to a date-indexed Series regardless of return type.
        # pd.DataFrame() handles both pandas DataFrames and numpy recarrays.
        try:
            dy_df = pd.DataFrame(dy_raw).reset_index()
            dy_date_col = dy_df.columns[0]
            dy_df[dy_date_col] = pd.to_datetime(dy_df[dy_date_col]).dt.normalize()
            dy_df = dy_df.set_index(dy_date_col).sort_index()
            # Pick the first numeric column (Norgate names it differently by version)
            yield_cols = [c for c in dy_df.columns if pd.api.types.is_numeric_dtype(dy_df[c])]
            if not yield_cols:
                return None
            yield_series = dy_df[yield_cols[0]].dropna()
        except Exception:
            return None

        if yield_series.empty:
            return None
        quarterly_yield = yield_series.resample('QE-DEC').last()
        return quarterly_yield
    except Exception:
        return None


def _safe_calc_dividend_yield(ticker):
    try:
        result = _calc_dividend_yield_quarterly(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v) and float(v) > 0]
        return ticker, rows
    except Exception:
        return ticker, []


def _calc_trailing_dividends_quarterly(ticker):
    """Return trailing 12M ordinary dividends per share at each quarter-end date.
    Uses CAPITALSPECIAL adjustment so the Dividend column contains only ordinary
    dividends (no special payments / spin-offs).
    """
    try:
        df = norgatedata.price_timeseries(
            ticker,
            stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.CAPITALSPECIAL,
            padding_setting=norgatedata.PaddingType.NONE,
            timeseriesformat='pandas-dataframe',
        )
        if df is None or (hasattr(df, 'empty') and df.empty):
            return None
        df = df.reset_index()
        date_col = df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
        df = df.set_index(date_col).sort_index()
        if 'Dividend' not in df.columns:
            return None
        div_series = df['Dividend'].fillna(0.0)
        # Rolling 252-trading-day sum ~ trailing 12 months of dividends per share
        trailing_12m = div_series.rolling(252, min_periods=1).sum()
        quarterly = trailing_12m.resample('QE-DEC').last()
        quarterly = quarterly[quarterly > 0]
        return quarterly if not quarterly.empty else None
    except Exception:
        return None


def _safe_calc_trailing_divs(ticker):
    try:
        result = _calc_trailing_dividends_quarterly(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v) and float(v) > 0]
        return ticker, rows
    except Exception:
        return ticker, []


def build_quarter_dividend_universes():
    """Build High Yield (top 25 by trailing 12M yield %) and Dividend Growth
    (top 25 by YoY growth in trailing 12M ordinary dividends per share) universes."""
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    total = len(symbols)
    print(f"Unique stocks in dividend universe: {total}")

    # --- Pass 1: dividend yield % (for High Yield basket) ---
    print("  Pass 1/2: fetching dividend yield timeseries...")
    yield_data = []
    last_milestone = 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_dividend_yield, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            yield_data.extend(rows)
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"    {milestone}% ({i}/{total})")
                last_milestone = milestone

    # --- Pass 2: trailing 12M ordinary dividends per share (for Dividend Growth basket) ---
    print("  Pass 2/2: fetching per-share dividend timeseries...")
    divs_data = []
    last_milestone = 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_trailing_divs, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            divs_data.extend(rows)
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"    {milestone}% ({i}/{total})")
                last_milestone = milestone

    if not yield_data:
        return {}, {}

    df_yield = pd.DataFrame(yield_data, columns=['Date', 'Ticker', 'Yield'])
    df_divs = pd.DataFrame(divs_data, columns=['Date', 'Ticker', 'TrailingDivs']) if divs_data else pd.DataFrame(columns=['Date', 'Ticker', 'TrailingDivs'])

    high_yield = {}
    div_growth = {}
    div_with_growth = {}

    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        ranking_date = _quarter_end_from_key(prev_key)
        prev_universe = QUARTER_UNIVERSE[prev_key]

        # Basket 1: High Dividend Yield -- top DIV_THEME_SIZE by trailing 12M yield %
        grp_yield = df_yield[(df_yield['Date'] == ranking_date) & (df_yield['Ticker'].isin(prev_universe))]
        if not grp_yield.empty:
            high_yield[key] = set(grp_yield.sort_values('Yield', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

        # Basket 2 & 3: need dividend growth data
        if df_divs.empty:
            continue
        current_divs = df_divs[(df_divs['Date'] == ranking_date) & (df_divs['Ticker'].isin(prev_universe))]
        if current_divs.empty:
            continue
        prev_year_date = ranking_date - pd.DateOffset(years=1)
        prior_divs = df_divs[
            df_divs['Date'].between(prev_year_date - pd.Timedelta(days=45),
                                     prev_year_date + pd.Timedelta(days=45))
            & df_divs['Ticker'].isin(prev_universe)
        ].copy()
        if prior_divs.empty:
            continue
        prior_divs['_dist'] = (prior_divs['Date'] - prev_year_date).abs()
        prior_divs = prior_divs.sort_values('_dist').drop_duplicates('Ticker')
        prior_divs = prior_divs.rename(columns={'TrailingDivs': 'PriorDivs'})[['Ticker', 'PriorDivs']]
        merged = current_divs.merge(prior_divs, on='Ticker', how='inner')
        merged = merged[(merged['TrailingDivs'] > 0) & (merged['PriorDivs'] > 0)].copy()
        if merged.empty:
            continue
        merged['Growth'] = (merged['TrailingDivs'] / merged['PriorDivs']) - 1.0

        # Basket 2: Dividend Growth -- top DIV_THEME_SIZE by YoY growth
        div_growth[key] = set(merged.sort_values('Growth', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

        # Basket 3: Dividend with Growth -- top DIV_THEME_SIZE by yield, filtered to >0% growth
        growing_tickers = set(merged.loc[merged['Growth'] > 0, 'Ticker'])
        if growing_tickers and not grp_yield.empty:
            filtered = grp_yield[grp_yield['Ticker'].isin(growing_tickers)]
            if not filtered.empty:
                div_with_growth[key] = set(filtered.sort_values('Yield', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

    return high_yield, div_growth, div_with_growth


def is_dividend_universes_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    high, growth, with_growth = cached
    return current_key in high and current_key in growth and current_key in with_growth


def load_or_build_dividend_universes():
    if DIVIDEND_CACHE_FILE.exists():
        try:
            d = json.loads(DIVIDEND_CACHE_FILE.read_text(encoding='utf-8'))
            cached = ({k: set(v) for k, v in d['high_yield'].items()},
                      {k: set(v) for k, v in d['div_growth'].items()},
                      {k: set(v) for k, v in d.get('div_with_growth', {}).items()})
            if is_dividend_universes_current(cached):
                print("Dividend universes loaded from cache (up to date)")
                return cached
            print("Dividend universes outdated, rebuilding...")
        except Exception:
            print("Dividend universe cache invalid, rebuilding...")

    result = build_quarter_dividend_universes()
    high_yield, div_growth, div_with_growth = result
    WriteThroughPath(DIVIDEND_CACHE_FILE).write_text(
        json.dumps({'high_yield': {k: sorted(v) for k, v in high_yield.items()},
                    'div_growth': {k: sorted(v) for k, v in div_growth.items()},
                    'div_with_growth': {k: sorted(v) for k, v in div_with_growth.items()}})
    )
    _latest = max(high_yield.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if high_yield else None
    _hy_count = len(high_yield.get(_latest, set())) if _latest else 0
    _dg_count = len(div_growth.get(_latest, set())) if _latest else 0
    _dwg_count = len(div_with_growth.get(_latest, set())) if _latest else 0
    print(f"Saved: {DIVIDEND_CACHE_FILE} (High Yield: {_hy_count}, Dividend Growth: {_dg_count}, Div with Growth: {_dwg_count})")
    return result


# --- Size (Dollar Volume) Universe ---

def _calc_avg_dollar_volume_quarterly(ticker):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or df.empty:
        return None
    df = df.reset_index()
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df.set_index(date_col, inplace=True)
    dollar_vol = df['Close'] * df['Volume']
    quarterly = dollar_vol.resample('QE-DEC').mean()
    return quarterly


def _safe_calc_dollar_volume(ticker):
    try:
        result = _calc_avg_dollar_volume_quarterly(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v) and v > 0]
        return ticker, rows
    except Exception:
        return ticker, []


def build_quarter_size_universes():
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Unique stocks in size universe: {len(symbols)}")

    all_data = []
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_dollar_volume, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            all_data.extend(rows)
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    if not all_data:
        return {}

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'DollarVol'])

    universe = {}
    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        ranking_date = _quarter_end_from_key(prev_key)
        prev_universe = QUARTER_UNIVERSE[prev_key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(prev_universe))]
        if grp.empty:
            continue
        sorted_desc = grp.sort_values('DollarVol', ascending=False)
        universe[key] = set(sorted_desc.head(THEME_SIZE)['Ticker'])

    return universe


def is_size_universe_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    return current_key in cached


def load_or_build_size_universes():
    if SIZE_CACHE_FILE.exists():
        try:
            cached = {k: set(v) for k, v in
                      json.loads(SIZE_CACHE_FILE.read_text(encoding='utf-8')).items()}
            if is_size_universe_current(cached):
                print("Size universe loaded from cache (up to date)")
                return cached
            print("Size universe outdated, rebuilding...")
        except Exception:
            print("Size universe cache invalid, rebuilding...")

    result = build_quarter_size_universes()
    WriteThroughPath(SIZE_CACHE_FILE).write_text(
        json.dumps({k: sorted(v) for k, v in result.items()})
    )
    _latest = max(result.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if result else None
    print(f"Saved: {SIZE_CACHE_FILE} ({len(result.get(_latest, set())) if _latest else 0} tickers)")
    return result


# --- Volume Growth Universe ---

def _calc_quarterly_dollar_volume(ticker):
    df = norgatedata.price_timeseries(
        ticker,
        stock_price_adjustment_setting=norgatedata.StockPriceAdjustmentType.TOTALRETURN,
        padding_setting=norgatedata.PaddingType.NONE,
        timeseriesformat='pandas-dataframe'
    )
    if df is None or df.empty:
        return None
    df = df.reset_index()
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    df.set_index(date_col, inplace=True)
    dollar_vol = df['Close'] * df['Volume']
    quarterly = dollar_vol.resample('QE-DEC').mean()
    return quarterly


def _safe_calc_quarterly_dollar_volume(ticker):
    try:
        result = _calc_quarterly_dollar_volume(ticker)
        if result is None:
            return ticker, []
        rows = [(d, ticker, v) for d, v in result.items()
                if d.year >= START_YEAR and pd.notna(v) and v > 0]
        return ticker, rows
    except Exception:
        return ticker, []


def build_quarter_volume_growth_universes():
    symbols = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Unique stocks in volume growth universe: {len(symbols)}")

    all_data = []
    total = len(symbols)
    last_milestone = 0

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(_safe_calc_quarterly_dollar_volume, t) for t in symbols]
        for i, fut in enumerate(futures, start=1):
            _, rows = fut.result()
            all_data.extend(rows)
            percent = int((i / total) * 100)
            milestone = percent // 10 * 10
            if milestone > last_milestone and milestone % 10 == 0:
                print(f"  {milestone}% complete ({i} / {total} stocks)")
                last_milestone = milestone

    if not all_data:
        return {}

    df = pd.DataFrame(all_data, columns=['Date', 'Ticker', 'DollarVol'])

    universe = {}
    for key in sorted(QUARTER_UNIVERSE.keys()):
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        # Need two prior quarters to compute growth
        prev_year = int(prev_key.split()[0])
        prev_q = int(prev_key.split()[1].replace("Q", ""))
        if prev_q == 1:
            prev_prev_key = f"{prev_year - 1} Q4"
        else:
            prev_prev_key = f"{prev_year} Q{prev_q - 1}"

        ranking_date = _quarter_end_from_key(prev_key)
        prev_ranking_date = _quarter_end_from_key(prev_prev_key)
        prev_universe = QUARTER_UNIVERSE[prev_key]

        cur_grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(prev_universe))].set_index('Ticker')
        prev_grp = df[(df['Date'] == prev_ranking_date) & (df['Ticker'].isin(prev_universe))].set_index('Ticker')

        if cur_grp.empty or prev_grp.empty:
            continue

        merged = cur_grp[['DollarVol']].join(prev_grp[['DollarVol']], lsuffix='_cur', rsuffix='_prev', how='inner')
        merged = merged[merged['DollarVol_prev'] > 0]
        merged['Growth'] = (merged['DollarVol_cur'] - merged['DollarVol_prev']) / merged['DollarVol_prev']

        sorted_desc = merged.sort_values('Growth', ascending=False)
        universe[key] = set(sorted_desc.head(THEME_SIZE).index)

    return universe


def is_volume_growth_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    return current_key in cached


def load_or_build_volume_growth_universes():
    if VOLUME_GROWTH_CACHE_FILE.exists():
        try:
            cached = {k: set(v) for k, v in
                      json.loads(VOLUME_GROWTH_CACHE_FILE.read_text(encoding='utf-8')).items()}
            if is_volume_growth_current(cached):
                print("Volume growth universe loaded from cache (up to date)")
                return cached
            print("Volume growth universe outdated, rebuilding...")
        except Exception:
            print("Volume growth universe cache invalid, rebuilding...")

    result = build_quarter_volume_growth_universes()
    WriteThroughPath(VOLUME_GROWTH_CACHE_FILE).write_text(
        json.dumps({k: sorted(v) for k, v in result.items()})
    )
    _latest = max(result.keys(), key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))) if result else None
    print(f"Saved: {VOLUME_GROWTH_CACHE_FILE} ({len(result.get(_latest, set())) if _latest else 0} tickers)")
    return result


# --- GICS Sector / Industry Mappings ---

def _build_gics_mappings():
    """Map all tickers to sector (GICS level 1) and sub-industry (GICS level 4) in one pass."""
    all_tickers = sorted({t for tickers in QUARTER_UNIVERSE.values() for t in tickers})
    print(f"Mapping GICS classifications for {len(all_tickers)} stocks")

    ticker_sector = {}
    ticker_subindustry = {}
    total = len(all_tickers)
    last_milestone = 0

    for i, t in enumerate(all_tickers, start=1):
        try:
            sec = norgatedata.classification_at_level(t, "GICS", "Name", 1)
            if sec:
                ticker_sector[t] = sec
        except Exception:
            pass
        try:
            subind = norgatedata.classification_at_level(t, "GICS", "Name", 4)
            if subind:
                ticker_subindustry[t] = subind
        except Exception:
            pass

        percent = int((i / total) * 100)
        milestone = percent // 10 * 10
        if milestone > last_milestone and milestone % 10 == 0:
            print(f"  {milestone}% complete ({i} / {total} stocks)")
            last_milestone = milestone

    return ticker_sector, ticker_subindustry


def _build_sector_universes(ticker_sector):
    """Build quarter -> sector -> tickers using prior quarter volume universe."""
    sector_universes = {s: {} for s in SECTOR_LIST}
    for key in QUARTER_UNIVERSE:
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        prev_universe = QUARTER_UNIVERSE[prev_key]
        for t in prev_universe:
            sec = ticker_sector.get(t)
            if sec in sector_universes:
                sector_universes[sec].setdefault(key, set()).add(t)
    return sector_universes


def _build_industry_universes(ticker_subindustry):
    """Build quarter -> industry -> tickers, filtering to industries with enough stocks."""
    global INDUSTRY_LIST

    latest_key = max(
        QUARTER_UNIVERSE.keys(),
        key=lambda k: (int(k.split()[0]), int(k.split()[1].replace("Q", "")))
    ) if QUARTER_UNIVERSE else None

    latest_universe = QUARTER_UNIVERSE.get(latest_key, set()) if latest_key else set()
    latest_counts = {}
    for t in latest_universe:
        subind = ticker_subindustry.get(t)
        if subind:
            latest_counts[subind] = latest_counts.get(subind, 0) + 1
    selected_industries = sorted([name for name, cnt in latest_counts.items() if cnt >= INDUSTRY_MIN_STOCKS])
    INDUSTRY_LIST = selected_industries

    if latest_key:
        print(f"Tracking {len(INDUSTRY_LIST)} industries with >= {INDUSTRY_MIN_STOCKS} stocks in {latest_key}")

    industry_universes = {ind: {} for ind in selected_industries}
    for key in QUARTER_UNIVERSE:
        year_str, q_str = key.split()
        year = int(year_str)
        quarter = int(q_str.replace("Q", ""))
        if quarter == 1:
            prev_key = f"{year - 1} Q4"
        else:
            prev_key = f"{year} Q{quarter - 1}"
        if prev_key not in QUARTER_UNIVERSE:
            continue
        prev_universe = QUARTER_UNIVERSE[prev_key]
        for t in prev_universe:
            group = ticker_subindustry.get(t)
            if group in industry_universes:
                industry_universes[group].setdefault(key, set()).add(t)
    return industry_universes


def _is_gics_current(cached):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    _, _, sector_u, industry_u = cached
    has_sector = any(current_key in u for u in sector_u.values())
    has_industry = any(current_key in u for u in industry_u.values())
    return has_sector and has_industry


def load_or_build_gics_mappings():
    global INDUSTRY_LIST
    if GICS_CACHE_FILE.exists():
        try:
            cached = _json_to_gics(GICS_CACHE_FILE.read_text(encoding='utf-8'))
            if len(cached) == 4 and _is_gics_current(cached):
                print("GICS mappings loaded from cache (up to date)")
                _, _, sector_u, industry_u = cached
                INDUSTRY_LIST = sorted(industry_u.keys())
                return cached
            print("GICS mappings outdated, rebuilding...")
        except Exception:
            print("GICS cache invalid, rebuilding...")

    ticker_sector, ticker_subindustry = _build_gics_mappings()
    sector_u = _build_sector_universes(ticker_sector)
    industry_u = _build_industry_universes(ticker_subindustry)
    result = (ticker_sector, ticker_subindustry, sector_u, industry_u)
    WriteThroughPath(GICS_CACHE_FILE).write_text(_gics_to_json(result))
    print(f"Saved: {GICS_CACHE_FILE} ({len(sector_u)} sectors, {len(industry_u)} industries)")
    return result


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    reset_cell_timer("Universe Construction")

    QUARTER_UNIVERSE = load_or_build_universe()
    print(f"Universe: {len(QUARTER_UNIVERSE)} quarters")

    ETF_UNIVERSE = load_or_build_etf_universe()
    print(f"ETF Universe: {len(ETF_UNIVERSE)} quarters")

    TICKER_NAMES = _build_ticker_names()

    BETA_UNIVERSE, LOW_BETA_UNIVERSE = load_or_build_beta_universes()
    MOMENTUM_UNIVERSE, MOMENTUM_LOSERS_UNIVERSE = load_or_build_momentum_universes()
    RISK_ADJ_MOM_UNIVERSE = load_or_build_risk_adj_momentum()
    HIGH_YIELD_UNIVERSE, DIV_GROWTH_UNIVERSE, DIV_WITH_GROWTH_UNIVERSE = load_or_build_dividend_universes()
    SIZE_UNIVERSE = load_or_build_size_universes()
    VOLUME_GROWTH_UNIVERSE = load_or_build_volume_growth_universes()
    TICKER_SECTOR, TICKER_SUBINDUSTRY, SECTOR_UNIVERSES, INDUSTRY_UNIVERSES = load_or_build_gics_mappings()

    print("All universes built and cached.")
