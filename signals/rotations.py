# %% [markdown]
## Imports & Dependencies
# %%

import norgatedata
import pandas as pd
import json
import hashlib
import builtins
import os
import shutil
import glob as globmod
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import numba
from dataclasses import dataclass, field
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import time
import numpy as np
import openpyxl  # noqa: F401 – required engine for pandas .to_excel() / .read_excel()

# %% [markdown]
## Configuration & Constants
# %%

START_YEAR = 2000
SIZE = 500
# Get output folder from environment variable or use default in Documents folder
BASE_OUTPUT_FOLDER = Path(os.getenv('PYTHON_OUTPUTS_DIR', Path.home() / 'Documents' / 'Python_Outputs')).expanduser()
BASE_OUTPUT_FOLDER.mkdir(parents=True, exist_ok=True)


def _resolve_onedrive_output_folder():
    candidates = []
    explicit_output_dir = os.getenv('PYTHON_OUTPUTS_ONEDRIVE_DIR')
    if explicit_output_dir:
        candidates.append(Path(explicit_output_dir))
    env_root = (
        os.getenv('OneDriveCommercial')
        or os.getenv('OneDriveConsumer')
        or os.getenv('OneDrive')
    )
    if env_root:
        candidates.append(Path(env_root) / 'Documents' / 'Python_Outputs')
    candidates.append(Path.home() / 'OneDrive' / 'Documents' / 'Python_Outputs')

    for candidate in candidates:
        if candidate == BASE_OUTPUT_FOLDER:
            continue
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            return candidate
        except OSError:
            continue
    return None


ONEDRIVE_OUTPUT_FOLDER = _resolve_onedrive_output_folder()


def _mirror_to_onedrive(local_path):
    """Copy a local output file to the mirrored OneDrive path. Non-blocking."""
    if ONEDRIVE_OUTPUT_FOLDER is None:
        return
    try:
        rel = Path(local_path).relative_to(BASE_OUTPUT_FOLDER)
        od_path = ONEDRIVE_OUTPUT_FOLDER / rel
        od_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, od_path)
    except Exception:
        pass


def _needs_write_and_mirror(local_path):
    """Returns (need_write, need_mirror).
    Skip entirely only if both local and OneDrive copies already exist.
    If local exists but OneDrive doesn't, mirror only (no rewrite).
    If local doesn't exist, write and mirror.
    """
    local_path = Path(local_path)
    local_exists = local_path.exists()
    if ONEDRIVE_OUTPUT_FOLDER is None:
        return not local_exists, False
    try:
        rel = local_path.relative_to(BASE_OUTPUT_FOLDER)
        od_exists = (ONEDRIVE_OUTPUT_FOLDER / rel).exists()
    except ValueError:
        od_exists = local_exists
    return not local_exists, not od_exists


EQUITY_CACHE_SCHEMA_VERSION = 1
EQUITY_SIGNAL_LOGIC_VERSION = '2026-03-13-btfd-stfr-prev-trend'
EQUITY_UNIVERSE_LOGIC_VERSION = '2026-04-01-target-quarter-keying'
FORCE_REBUILD_EQUITY_CACHE = False
BASKET_SIGNALS_CACHE_SCHEMA_VERSION = 1
FORCE_REBUILD_BASKET_SIGNALS = True
CHART_SCHEMA_VERSION = 2  # Bump to force rebuild of basket chart PNGs (added 21d corr panel)
BENCHMARK_BASKETS = 0      # If > 0, only process this many baskets then stop
BENCHMARK_TIMING = True    # If True, print per-step timing breakdown for each basket

_basket_timing = {
    'cache_check': [], 'equity_ohlc': [], 'build_signals': [],
    'breadth_trend': [], 'breadth_breakout': [], 'breadth_merge': [],
    'breadth_pivots': [], 'correlation': [], 'save_parquet': [],
    'contributions': [], 'total': [],
}
_basket_timing_names = []

# %% [markdown]
## Output Path Layout
# %%


@dataclass
class OutputPaths:
    """Central registry for all output folder paths.

    Instantiate once from BASE_OUTPUT_FOLDER.  All sub-folders are derived
    automatically and created on first access via ``_mkdirs()``.
    """

    base: Path

    def __post_init__(self):
        self.data                 = self.base / 'Data_Storage'
        self.corr_cache           = self.data / 'correlation_cache'
        self.thematic_basket_cache = self.data / 'thematic_basket_cache'
        self.sector_basket_cache  = self.data / 'sector_basket_cache'
        self.industry_basket_cache = self.data / 'industry_basket_cache'
        self.baskets              = self.base / 'Baskets'
        self.thematic_charts      = self.baskets / 'Thematic_Baskets'
        self.sector_charts        = self.baskets / 'Sector_Baskets'
        self.industry_charts      = self.baskets / 'Industry_Baskets'
        self.holdings             = self.base / 'Trading_View_Lists'
        self.previous_day         = self.base / 'Previous_Day_Rotations'
        self.correlations         = self.base / 'Correlations'
        self.live                 = self.base / 'Live_Rotations'
        self.summary              = self.base / 'Summary'
        self._mkdirs()

    def _mkdirs(self):
        for attr in [
            'data', 'corr_cache',
            'thematic_basket_cache', 'sector_basket_cache', 'industry_basket_cache',
            'baskets', 'thematic_charts', 'sector_charts', 'industry_charts',
            'holdings', 'previous_day', 'correlations', 'live', 'summary',
        ]:
            getattr(self, attr).mkdir(parents=True, exist_ok=True)


paths = OutputPaths(BASE_OUTPUT_FOLDER)

# Folder-constant aliases — all folder definitions are owned by OutputPaths above
DATA_FOLDER                   = paths.data
CORR_CACHE_FOLDER             = paths.corr_cache
BASKETS_FOLDER                = paths.baskets
THEMATIC_CHARTS_FOLDER        = paths.thematic_charts
SECTOR_CHARTS_FOLDER          = paths.sector_charts
INDUSTRY_CHARTS_FOLDER        = paths.industry_charts
HOLDINGS_FOLDER               = paths.holdings
PREVIOUS_DAY_ROTATIONS_FOLDER = paths.previous_day
CORR_FOLDER                   = paths.correlations
LIVE_ROTATIONS_FOLDER         = paths.live
SUMMARY_FOLDER                = paths.summary

# Cache file paths
CACHE_FILE = DATA_FOLDER / f'top{SIZE}stocks.json'


# %% [markdown]
## Utility Functions
# %%


# ---------------------------------------------------------------------------
# WriteThroughPath — automatic OneDrive mirroring on every write
# ---------------------------------------------------------------------------
class WriteThroughPath:
    """Wraps a local output Path and its OneDrive mirror.

    Every write operation (``write_bytes``, ``write_text``, ``open``) writes
    locally first then copies to the mirror automatically.  Call ``sync()``
    after any external write (e.g. pandas to_parquet / to_excel) to trigger
    the same mirror copy.
    """

    def __init__(self, local: Path):
        self.local = Path(local)
        if ONEDRIVE_OUTPUT_FOLDER is None:
            self._mirror = None
        else:
            try:
                rel = self.local.relative_to(BASE_OUTPUT_FOLDER)
                self._mirror = ONEDRIVE_OUTPUT_FOLDER / rel
            except ValueError:
                self._mirror = None

    def _copy(self) -> None:
        if self._mirror is not None:
            try:
                self._mirror.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(self.local, self._mirror)
            except Exception:
                pass

    def write_bytes(self, data: bytes) -> None:
        self.local.parent.mkdir(parents=True, exist_ok=True)
        self.local.write_bytes(data)
        self._copy()

    def write_text(self, text: str, encoding: str = 'utf-8') -> None:
        self.local.parent.mkdir(parents=True, exist_ok=True)
        self.local.write_text(text, encoding=encoding)
        self._copy()

    @contextmanager
    def open(self, mode: str = 'wb', **kwargs):
        self.local.parent.mkdir(parents=True, exist_ok=True)
        with builtins.open(self.local, mode, **kwargs) as f:
            yield f
        self._copy()

    def sync(self) -> None:
        """Mirror an already-written local file to OneDrive."""
        self._copy()

    def __fspath__(self) -> str:
        return str(self.local)

    def __str__(self) -> str:
        return str(self.local)


# ---------------------------------------------------------------------------
# build_pdf — centralised PDF generation helper
# ---------------------------------------------------------------------------
def build_pdf(figures: list, path) -> None:
    """Save a list of matplotlib Figures to a single PDF, then close each."""
    from matplotlib.backends.backend_pdf import PdfPages
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with PdfPages(path) as pdf:
        for fig in figures:
            pdf.savefig(fig, dpi=150)
            plt.close(fig)
    WriteThroughPath(path).sync()


# ---------------------------------------------------------------------------
# Timer / print utilities
# ---------------------------------------------------------------------------
if hasattr(builtins, "_codex_original_print"):
    _ORIGINAL_PRINT = builtins._codex_original_print
else:
    _ORIGINAL_PRINT = builtins.print
    builtins._codex_original_print = _ORIGINAL_PRINT
_CELL_TIMER_START = None


def _timed_print(*args, **kwargs):
    if _CELL_TIMER_START is None:
        _ORIGINAL_PRINT(*args, **kwargs)
        return
    elapsed = time.perf_counter() - _CELL_TIMER_START
    _ORIGINAL_PRINT(f"[+{elapsed:7.2f}s]", *args, **kwargs)


def _install_timed_print():
    if builtins.print is not _timed_print:
        builtins.print = _timed_print


def reset_cell_timer(cell_name=None):
    global _CELL_TIMER_START
    _CELL_TIMER_START = time.perf_counter()
    if cell_name:
        _ORIGINAL_PRINT(f"[+{0.00:7.2f}s] Starting {cell_name}")


_install_timed_print()
reset_cell_timer("Utility Functions")


def _get_current_quarter_key():
    """Get the most recent quarter key from the universe."""
    keys = sorted(QUARTER_UNIVERSE.keys())
    return keys[-1] if keys else None


# %% [markdown]
## Universe Construction
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Universe Construction")


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
        # Key is the NEXT quarter — Q1 volume determines Q2 universe
        next_q = date.quarter % 4 + 1
        next_y = date.year + (1 if date.quarter == 4 else 0)
        universe[f"{next_y} Q{next_q}"] = set(grp.nlargest(SIZE, 'Vol')['Ticker'])

    return universe


def is_universe_current(universe):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    return current_key in universe


def _universe_to_json(universe: dict) -> str:
    """Serialize universe dict (str → set) to JSON (str → sorted list)."""
    return json.dumps({k: sorted(v) for k, v in universe.items()})


def _json_to_universe(text: str) -> dict:
    """Deserialize universe dict from JSON (str → list → set)."""
    return {k: set(v) for k, v in json.loads(text).items()}


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


QUARTER_UNIVERSE = load_or_build_universe()

# --- ETF Universe ---

ETF_SIZE = 50
ETF_CACHE_FILE = DATA_FOLDER / 'etf_universes_50.json'


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
        next_q = date.quarter % 4 + 1
        next_y = date.year + (1 if date.quarter == 4 else 0)
        universe[f"{next_y} Q{next_q}"] = set(grp.nlargest(ETF_SIZE, 'Vol')['Ticker'])

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


ETF_UNIVERSE = load_or_build_etf_universe()

# --- Ticker Names Cache ---

TICKER_NAMES_FILE = DATA_FOLDER / 'ticker_names.json'


def _build_ticker_names():
    """Build ticker → security name mapping for all stocks + ETFs."""
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


TICKER_NAMES = _build_ticker_names()

# --- Beta Universe ---

THEME_SIZE = 25
BETA_CACHE_FILE = paths.thematic_basket_cache / f'beta_universes_{SIZE}.json'
MARKET_SYMBOL = 'SPY'
LOOKBACK_DAYS = 252
DIVIDEND_CACHE_FILE = paths.thematic_basket_cache / f'dividend_universes_{SIZE}.json'
DIV_THEME_SIZE = 25


def _quarter_end_from_key(key: str) -> pd.Timestamp:
    # key format: "YYYY QN"
    year_str, q_str = key.split()
    year = int(year_str)
    quarter = int(q_str.replace("Q", ""))
    return pd.Period(f"{year}Q{quarter}").end_time.normalize()


def _quarter_start_from_key(key: str) -> pd.Timestamp:
    # key format: "YYYY QN"
    year_str, q_str = key.split()
    year = int(year_str)
    quarter = int(q_str.replace("Q", ""))
    return pd.Period(f"{year}Q{quarter}").start_time.normalize()


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
        ranking_date = _quarter_end_from_key(prev_key)
        current_universe = QUARTER_UNIVERSE[key]

        beta_vals = []
        for t in current_universe:
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


def _beta_universes_to_json(result: tuple) -> str:
    high, low = result
    return json.dumps({'high': {k: sorted(v) for k, v in high.items()},
                       'low':  {k: sorted(v) for k, v in low.items()}})


def _json_to_beta_universes(text: str) -> tuple:
    d = json.loads(text)
    return ({k: set(v) for k, v in d['high'].items()},
            {k: set(v) for k, v in d['low'].items()})


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


print("Building high & low beta groups...")
BETA_UNIVERSE, LOW_BETA_UNIVERSE = load_or_build_beta_universes()

# --- Momentum Universe ---

MOMENTUM_CACHE_FILE = paths.thematic_basket_cache / f'momentum_universes_{SIZE}.json'
MOMENTUM_LOOKBACK_DAYS = 252


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
        ranking_date = _quarter_end_from_key(prev_key)
        current_universe = QUARTER_UNIVERSE[key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(current_universe))]
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


print("Building high & low momentum groups...")
MOMENTUM_UNIVERSE, MOMENTUM_LOSERS_UNIVERSE = load_or_build_momentum_universes()

# --- Risk-Adjusted Momentum Universe ---

# ---------------------------------------------------------------------------
# Risk-Adjusted Momentum: 1-year return / 1-year volatility
# ---------------------------------------------------------------------------
RISK_ADJ_MOM_CACHE_FILE = paths.thematic_basket_cache / f'risk_adj_momentum_{SIZE}.json'


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
        ranking_date = _quarter_end_from_key(prev_key)
        current_universe = QUARTER_UNIVERSE[key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(current_universe))]
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


print("Building risk-adjusted momentum group...")
RISK_ADJ_MOM_UNIVERSE = load_or_build_risk_adj_momentum()

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
        # Rolling 252-trading-day sum â‰ˆ trailing 12 months of dividends per share
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
        ranking_date = _quarter_end_from_key(prev_key)
        current_universe = QUARTER_UNIVERSE[key]

        # Basket 1: High Dividend Yield — top DIV_THEME_SIZE by trailing 12M yield %
        grp_yield = df_yield[(df_yield['Date'] == ranking_date) & (df_yield['Ticker'].isin(current_universe))]
        if not grp_yield.empty:
            high_yield[key] = set(grp_yield.sort_values('Yield', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

        # Basket 2 & 3: need dividend growth data
        if df_divs.empty:
            continue
        current_divs = df_divs[(df_divs['Date'] == ranking_date) & (df_divs['Ticker'].isin(current_universe))]
        if current_divs.empty:
            continue
        prev_year_date = ranking_date - pd.DateOffset(years=1)
        prior_divs = df_divs[
            df_divs['Date'].between(prev_year_date - pd.Timedelta(days=45),
                                     prev_year_date + pd.Timedelta(days=45))
            & df_divs['Ticker'].isin(current_universe)
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

        # Basket 2: Dividend Growth — top DIV_THEME_SIZE by YoY growth
        div_growth[key] = set(merged.sort_values('Growth', ascending=False).head(DIV_THEME_SIZE)['Ticker'])

        # Basket 3: Dividend with Growth — top DIV_THEME_SIZE by yield, filtered to >0% growth
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


print("Building high dividend yield & dividend growth groups...")
HIGH_YIELD_UNIVERSE, DIV_GROWTH_UNIVERSE, DIV_WITH_GROWTH_UNIVERSE = load_or_build_dividend_universes()

# --- Size (Dollar Volume) Universe ---

SIZE_CACHE_FILE = paths.thematic_basket_cache / f'size_universes_{SIZE}.json'


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
        ranking_date = _quarter_end_from_key(prev_key)
        current_universe = QUARTER_UNIVERSE[key]
        grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(current_universe))]
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


print("Building size (dollar volume) group...")
SIZE_UNIVERSE = load_or_build_size_universes()

# --- Volume Growth Universe ---

VOLUME_GROWTH_CACHE_FILE = paths.thematic_basket_cache / f'volume_growth_universes_{SIZE}.json'


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
        # Need two prior quarters to compute growth
        prev_year = int(prev_key.split()[0])
        prev_q = int(prev_key.split()[1].replace("Q", ""))
        if prev_q == 1:
            prev_prev_key = f"{prev_year - 1} Q4"
        else:
            prev_prev_key = f"{prev_year} Q{prev_q - 1}"

        ranking_date = _quarter_end_from_key(prev_key)
        prev_ranking_date = _quarter_end_from_key(prev_prev_key)
        current_universe = QUARTER_UNIVERSE[key]

        cur_grp = df[(df['Date'] == ranking_date) & (df['Ticker'].isin(current_universe))].set_index('Ticker')
        prev_grp = df[(df['Date'] == prev_ranking_date) & (df['Ticker'].isin(current_universe))].set_index('Ticker')

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


print("Building volume growth group...")
VOLUME_GROWTH_UNIVERSE = load_or_build_volume_growth_universes()

# --- GICS Sector / Industry Mappings ---

SECTOR_LIST = [
    "Communication Services",
    "Consumer Discretionary",
    "Consumer Staples",
    "Energy",
    "Financials",
    "Health Care",
    "Industrials",
    "Information Technology",
    "Materials",
    "Real Estate",
    "Utilities",
]
INDUSTRY_MIN_STOCKS = 10
INDUSTRY_LIST = []

GICS_CACHE_FILE = DATA_FOLDER / f'gics_mappings_{SIZE}.json'


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
        current_universe = QUARTER_UNIVERSE[key]
        for t in current_universe:
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
        current_universe = QUARTER_UNIVERSE[key]
        for t in current_universe:
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


def _gics_to_json(result: tuple) -> str:
    ticker_sector, ticker_subindustry, sector_u, industry_u = result
    return json.dumps({
        'ticker_sector': ticker_sector,
        'ticker_subindustry': ticker_subindustry,
        'sector_u': {s: {k: sorted(v) for k, v in qmap.items()} for s, qmap in sector_u.items()},
        'industry_u': {g: {k: sorted(v) for k, v in qmap.items()} for g, qmap in industry_u.items()},
    })


def _json_to_gics(text: str) -> tuple:
    d = json.loads(text)
    sector_u = {s: {k: set(v) for k, v in qmap.items()} for s, qmap in d['sector_u'].items()}
    industry_u = {g: {k: set(v) for k, v in qmap.items()} for g, qmap in d['industry_u'].items()}
    return d['ticker_sector'], d['ticker_subindustry'], sector_u, industry_u


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


print("Building sector & industry mappings...")
TICKER_SECTOR, TICKER_SUBINDUSTRY, SECTOR_UNIVERSES, INDUSTRY_UNIVERSES = load_or_build_gics_mappings()

print("All thematic, sector, and industry groups ready.")

# %% [markdown]
## Signal Cache
# %%

import numpy as np
if 'reset_cell_timer' in globals():
    reset_cell_timer("Signal Cache")

# Signal types: Up_Rot, Down_Rot, Breakout, Breakdown, BTFD, STFR
SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']
RV_MULT = np.sqrt(252) / np.sqrt(21)
EMA_MULT = 2.0 / 11.0  # For range EMAs
RV_EMA_ALPHA = 2.0 / 11.0  # span=10 EMA for RV


def calc_rolling_stats(changes_list, mfe_list=None, mae_list=None, bars_list=None):
    """Calculate rolling stats from lists of closed trade changes and MFE/MAE.
    Used for breadth signal CSVs (small trade counts). For the hot signal loop,
    use RollingStatsAccumulator instead."""
    if not changes_list:
        return {}
    winners = [c for c in changes_list if c > 0]
    losers = [c for c in changes_list if c <= 0]
    total = len(changes_list)
    win_rate = len(winners) / total if total > 0 else np.nan
    avg_winner = np.mean(winners) if winners else 0
    avg_loser = np.mean(losers) if losers else 0
    hist_ev = (win_rate * avg_winner) + ((1 - win_rate) * avg_loser) if total > 0 else np.nan
    ev_last_3 = np.mean(changes_list[-3:]) if len(changes_list) >= 3 else np.nan
    std_dev = np.std(changes_list) if len(changes_list) >= 2 else np.nan
    if pd.notna(std_dev) and np.isfinite(std_dev) and std_dev > 0:
        risk_adj_ev = hist_ev / std_dev
        risk_adj_ev_last_3 = (ev_last_3 / std_dev) if not np.isnan(ev_last_3) else np.nan
    else:
        risk_adj_ev = np.nan
        risk_adj_ev_last_3 = np.nan
    avg_mfe = np.mean(mfe_list) if mfe_list else np.nan
    avg_mae = np.mean(mae_list) if mae_list else np.nan
    if bars_list is not None and len(bars_list) == total:
        winner_bars = [b for c, b in zip(changes_list, bars_list) if c > 0]
        loser_bars  = [b for c, b in zip(changes_list, bars_list) if c <= 0]
        avg_winner_bars = np.mean(winner_bars) if winner_bars else np.nan
        avg_loser_bars  = np.mean(loser_bars)  if loser_bars  else np.nan
    else:
        avg_winner_bars = np.nan
        avg_loser_bars  = np.nan
    return {
        'Win_Rate': win_rate, 'Avg_Winner': avg_winner, 'Avg_Loser': avg_loser,
        'Avg_Winner_Bars': avg_winner_bars, 'Avg_Loser_Bars': avg_loser_bars,
        'Avg_MFE': avg_mfe, 'Avg_MAE': avg_mae,
        'Historical_EV': hist_ev, 'Std_Dev': std_dev, 'Risk_Adj_EV': risk_adj_ev,
        'EV_Last_3': ev_last_3, 'Risk_Adj_EV_Last_3': risk_adj_ev_last_3, 'Count': total
    }


from collections import deque


class RollingStatsAccumulator:
    """O(1) incremental rolling stats tracker for Pass 5 signal loop."""
    __slots__ = ('count', 'n_winners', 'sum_winners', 'n_losers', 'sum_losers',
                 'sum_all', 'sum_sq', 'sum_mfe', 'sum_mae', 'last_3',
                 'sum_winner_bars', 'sum_loser_bars')

    def __init__(self):
        self.count = 0
        self.n_winners = 0
        self.sum_winners = 0.0
        self.n_losers = 0
        self.sum_losers = 0.0
        self.sum_all = 0.0
        self.sum_sq = 0.0
        self.sum_mfe = 0.0
        self.sum_mae = 0.0
        self.last_3 = deque(maxlen=3)
        self.sum_winner_bars = 0.0
        self.sum_loser_bars = 0.0

    def add(self, change, mfe, mae, bars=None):
        self.count += 1
        self.sum_all += change
        self.sum_sq += change * change
        self.last_3.append(change)
        if change > 0:
            self.n_winners += 1
            self.sum_winners += change
            if bars is not None:
                self.sum_winner_bars += bars
        else:
            self.n_losers += 1
            self.sum_losers += change
            if bars is not None:
                self.sum_loser_bars += bars
        self.sum_mfe += mfe
        self.sum_mae += mae

    def get_stats(self):
        if self.count == 0:
            return {}
        n = self.count
        win_rate = self.n_winners / n
        avg_winner = (self.sum_winners / self.n_winners) if self.n_winners > 0 else 0.0
        avg_loser = (self.sum_losers / self.n_losers) if self.n_losers > 0 else 0.0
        hist_ev = (win_rate * avg_winner) + ((1 - win_rate) * avg_loser)
        ev_last_3 = (sum(self.last_3) / len(self.last_3)) if len(self.last_3) >= 3 else np.nan
        # Population std dev (matches np.std default)
        mean = self.sum_all / n
        variance = (self.sum_sq / n) - (mean * mean)
        std_dev = np.sqrt(max(variance, 0.0)) if n >= 2 else np.nan
        if pd.notna(std_dev) and np.isfinite(std_dev) and std_dev > 0:
            risk_adj_ev = hist_ev / std_dev
            risk_adj_ev_last_3 = (ev_last_3 / std_dev) if not np.isnan(ev_last_3) else np.nan
        else:
            risk_adj_ev = np.nan
            risk_adj_ev_last_3 = np.nan
        avg_mfe = self.sum_mfe / n
        avg_mae = self.sum_mae / n
        avg_winner_bars = (self.sum_winner_bars / self.n_winners) if self.n_winners > 0 else np.nan
        avg_loser_bars = (self.sum_loser_bars / self.n_losers) if self.n_losers > 0 else np.nan
        return {
            'Win_Rate': win_rate, 'Avg_Winner': avg_winner, 'Avg_Loser': avg_loser,
            'Avg_Winner_Bars': avg_winner_bars, 'Avg_Loser_Bars': avg_loser_bars,
            'Avg_MFE': avg_mfe, 'Avg_MAE': avg_mae,
            'Historical_EV': hist_ev, 'Std_Dev': std_dev, 'Risk_Adj_EV': risk_adj_ev,
            'EV_Last_3': ev_last_3, 'Risk_Adj_EV_Last_3': risk_adj_ev_last_3, 'Count': n
        }


# Global timing tracker for passes
pass_times = {'data_load': 0, 'pass1': 0, 'pass2': 0, 'pass3': 0, 'pass4': 0, 'pass5': 0}
pass_counts = {'tickers': 0}


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
    """Pass 5 for a single signal type: trade tracking + rolling stats.
    Returns 20 arrays: entry_price, change, exit_idx, exit_price, final_change,
    mfe, mae, + 13 stats arrays."""
    MAX_STACK = 32

    entry_price_col = np.full(n, np.nan)
    change_col = np.full(n, np.nan)
    exit_idx_col = np.full(n, np.int64(-1))
    exit_price_col = np.full(n, np.nan)
    final_change_col = np.full(n, np.nan)
    mfe_col = np.full(n, np.nan)
    mae_col = np.full(n, np.nan)

    # 13 stats arrays
    s_win_rate = np.full(n, np.nan)
    s_avg_winner = np.full(n, np.nan)
    s_avg_loser = np.full(n, np.nan)
    s_avg_winner_bars = np.full(n, np.nan)
    s_avg_loser_bars = np.full(n, np.nan)
    s_avg_mfe = np.full(n, np.nan)
    s_avg_mae = np.full(n, np.nan)
    s_hist_ev = np.full(n, np.nan)
    s_std_dev = np.full(n, np.nan)
    s_risk_adj_ev = np.full(n, np.nan)
    s_ev_last_3 = np.full(n, np.nan)
    s_risk_adj_ev_last_3 = np.full(n, np.nan)
    s_count = np.full(n, np.nan)

    # Position stack
    p_idx = np.zeros(MAX_STACK, dtype=np.int64)
    p_price = np.zeros(MAX_STACK)
    p_hi = np.zeros(MAX_STACK)
    p_lo = np.zeros(MAX_STACK)
    n_open = 0

    # Accumulator state
    acc_count = 0
    acc_nw = 0
    acc_sw = 0.0
    acc_nl = 0
    acc_sl = 0.0
    acc_sa = 0.0
    acc_ssq = 0.0
    acc_smfe = 0.0
    acc_smae = 0.0
    acc_swb = 0.0
    acc_slb = 0.0
    l3 = np.zeros(3)
    l3_pos = 0
    l3_cnt = 0

    # Current stats (forward-filled)
    has_stats = False
    c_wr = 0.0
    c_aw = 0.0
    c_al = 0.0
    c_awb = np.nan
    c_alb = np.nan
    c_amfe = 0.0
    c_amae = 0.0
    c_hev = 0.0
    c_sd = np.nan
    c_raev = np.nan
    c_el3 = np.nan
    c_rael3 = np.nan
    c_cnt = 0.0

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
                bars = i - p_idx[p]

                # Update accumulator
                acc_count += 1
                acc_sa += fc
                acc_ssq += fc * fc
                l3[l3_pos % 3] = fc
                l3_pos += 1
                if l3_cnt < 3:
                    l3_cnt += 1
                if fc > 0:
                    acc_nw += 1
                    acc_sw += fc
                    acc_swb += bars
                else:
                    acc_nl += 1
                    acc_sl += fc
                    acc_slb += bars
                acc_smfe += mfe_v
                acc_smae += mae_v

                # Backfill
                eidx = p_idx[p]
                exit_idx_col[eidx] = i
                exit_price_col[eidx] = closes[i]
                final_change_col[eidx] = fc
                mfe_col[eidx] = mfe_v
                mae_col[eidx] = mae_v

            # Recompute current stats
            nn = acc_count
            c_wr = acc_nw / nn
            c_aw = (acc_sw / acc_nw) if acc_nw > 0 else 0.0
            c_al = (acc_sl / acc_nl) if acc_nl > 0 else 0.0
            c_awb = (acc_swb / acc_nw) if acc_nw > 0 else np.nan
            c_alb = (acc_slb / acc_nl) if acc_nl > 0 else np.nan
            c_hev = c_wr * c_aw + (1.0 - c_wr) * c_al
            if l3_cnt >= 3:
                c_el3 = (l3[0] + l3[1] + l3[2]) / 3.0
            else:
                c_el3 = np.nan
            mean = acc_sa / nn
            var = (acc_ssq / nn) - mean * mean
            if nn >= 2 and var > 0:
                c_sd = np.sqrt(var)
            else:
                c_sd = np.nan
            if not np.isnan(c_sd) and c_sd > 0:
                c_raev = c_hev / c_sd
                c_rael3 = c_el3 / c_sd if not np.isnan(c_el3) else np.nan
            else:
                c_raev = np.nan
                c_rael3 = np.nan
            c_amfe = acc_smfe / nn
            c_amae = acc_smae / nn
            c_cnt = float(nn)
            has_stats = True
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

        # Forward fill stats
        if has_stats:
            s_win_rate[i] = c_wr
            s_avg_winner[i] = c_aw
            s_avg_loser[i] = c_al
            s_avg_winner_bars[i] = c_awb
            s_avg_loser_bars[i] = c_alb
            s_avg_mfe[i] = c_amfe
            s_avg_mae[i] = c_amae
            s_hist_ev[i] = c_hev
            s_std_dev[i] = c_sd
            s_risk_adj_ev[i] = c_raev
            s_ev_last_3[i] = c_el3
            s_risk_adj_ev_last_3[i] = c_rael3
            s_count[i] = c_cnt

    # Force-close remaining open positions
    if n_open > 0:
        # Find last valid close
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
                bars = exit_i - p_idx[p]

                acc_count += 1
                acc_sa += fc
                acc_ssq += fc * fc
                l3[l3_pos % 3] = fc
                l3_pos += 1
                if l3_cnt < 3:
                    l3_cnt += 1
                if fc > 0:
                    acc_nw += 1
                    acc_sw += fc
                    acc_swb += bars
                else:
                    acc_nl += 1
                    acc_sl += fc
                    acc_slb += bars
                acc_smfe += mfe_v
                acc_smae += mae_v

                eidx = p_idx[p]
                exit_idx_col[eidx] = exit_i
                exit_price_col[eidx] = closes[exit_i]
                final_change_col[eidx] = fc
                mfe_col[eidx] = mfe_v
                mae_col[eidx] = mae_v

            # Recompute stats and backfill to end
            nn = acc_count
            c_wr = acc_nw / nn
            c_aw = (acc_sw / acc_nw) if acc_nw > 0 else 0.0
            c_al = (acc_sl / acc_nl) if acc_nl > 0 else 0.0
            c_awb = (acc_swb / acc_nw) if acc_nw > 0 else np.nan
            c_alb = (acc_slb / acc_nl) if acc_nl > 0 else np.nan
            c_hev = c_wr * c_aw + (1.0 - c_wr) * c_al
            if l3_cnt >= 3:
                c_el3 = (l3[0] + l3[1] + l3[2]) / 3.0
            else:
                c_el3 = np.nan
            mean = acc_sa / nn
            var = (acc_ssq / nn) - mean * mean
            if nn >= 2 and var > 0:
                c_sd = np.sqrt(var)
            else:
                c_sd = np.nan
            if not np.isnan(c_sd) and c_sd > 0:
                c_raev = c_hev / c_sd
                c_rael3 = c_el3 / c_sd if not np.isnan(c_el3) else np.nan
            else:
                c_raev = np.nan
                c_rael3 = np.nan
            c_amfe = acc_smfe / nn
            c_amae = acc_smae / nn
            c_cnt = float(nn)
            for j in range(exit_i, n):
                s_win_rate[j] = c_wr
                s_avg_winner[j] = c_aw
                s_avg_loser[j] = c_al
                s_avg_winner_bars[j] = c_awb
                s_avg_loser_bars[j] = c_alb
                s_avg_mfe[j] = c_amfe
                s_avg_mae[j] = c_amae
                s_hist_ev[j] = c_hev
                s_std_dev[j] = c_sd
                s_risk_adj_ev[j] = c_raev
                s_ev_last_3[j] = c_el3
                s_risk_adj_ev_last_3[j] = c_rael3
                s_count[j] = c_cnt

    return (
        entry_price_col, change_col, exit_idx_col, exit_price_col,
        final_change_col, mfe_col, mae_col,
        s_win_rate, s_avg_winner, s_avg_loser, s_avg_winner_bars, s_avg_loser_bars,
        s_avg_mfe, s_avg_mae, s_hist_ev, s_std_dev, s_risk_adj_ev,
        s_ev_last_3, s_risk_adj_ev_last_3, s_count,
    )


def _build_signals_from_df(df, ticker):
    """Core signal builder that expects OHLCV with a Date index or column.
    Uses numba-accelerated passes for ~50-100x speedup on the inner loops."""
    global pass_times, pass_counts

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
            s_wr, s_aw, s_al, s_awb, s_alb,
            s_amfe, s_amae, s_hev, s_sd, s_raev,
            s_el3, s_rael3, s_cnt,
        ) = _numba_pass5_signal(entry_arr, exit_arr, cp, has_custom,
                                closes, highs, lows, n, is_short)

        # Convert exit_idx back to dates
        exit_date_col = np.empty(n, dtype=object)
        for i in range(n):
            idx = exit_idx_col[i]
            if idx >= 0:
                exit_date_col[i] = dates[idx]
            else:
                exit_date_col[i] = np.nan

        new_cols[f'{sig_name}_Entry_Price'] = entry_price_col
        new_cols[f'{sig_name}_Change'] = change_col
        new_cols[f'{sig_name}_Exit_Date'] = exit_date_col
        new_cols[f'{sig_name}_Exit_Price'] = exit_price_col
        new_cols[f'{sig_name}_Final_Change'] = final_change_col
        new_cols[f'{sig_name}_MFE'] = mfe_col
        new_cols[f'{sig_name}_MAE'] = mae_col
        new_cols[f'{sig_name}_Win_Rate'] = s_wr
        new_cols[f'{sig_name}_Avg_Winner'] = s_aw
        new_cols[f'{sig_name}_Avg_Loser'] = s_al
        new_cols[f'{sig_name}_Avg_Winner_Bars'] = s_awb
        new_cols[f'{sig_name}_Avg_Loser_Bars'] = s_alb
        new_cols[f'{sig_name}_Avg_MFE'] = s_amfe
        new_cols[f'{sig_name}_Avg_MAE'] = s_amae
        new_cols[f'{sig_name}_Historical_EV'] = s_hev
        new_cols[f'{sig_name}_Std_Dev'] = s_sd
        new_cols[f'{sig_name}_Risk_Adj_EV'] = s_raev
        new_cols[f'{sig_name}_EV_Last_3'] = s_el3
        new_cols[f'{sig_name}_Risk_Adj_EV_Last_3'] = s_rael3
        new_cols[f'{sig_name}_Count'] = s_cnt

    pass_times['pass5'] += time.time() - t5

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
    append_rows    = {}   # ticker â†’ list of pd.Series
    rebuild_tickers = list(new_tickers)
    rebuild_info = {}     # ticker â†’ reason string (for diagnostics)

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
            # 'no_new_data' â†’ ticker halted or cache already current; skip

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


cell3_start_time = time.time()
SIGNALS_CACHE_FILE = DATA_FOLDER / f'signals_{SIZE}.parquet'
INCREMENTAL_MAX_DAYS = 5  # calendar days of staleness before full rebuild


def _get_latest_norgate_date():
    """Get the most recent date available in Norgate database using SPY."""
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


all_signals_df = load_or_build_signals()


# --- ETF Signal Pipeline (parallel to stock signals, separate parquet) ---

ETF_SIGNALS_CACHE_FILE = DATA_FOLDER / 'signals_etf_50.parquet'


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


etf_signals_df = load_or_build_etf_signals()
print(f"[ETF] {len(etf_signals_df)} rows, {etf_signals_df['Ticker'].nunique()} ETFs ready")


# %% [markdown]
## Basket Processing
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Basket Processing")


def _cache_slugify_label(label):
    return str(label).replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')


def _cache_build_quarter_lookup(universe_by_date):
    if not universe_by_date:
        return [], []
    sample_key = next(iter(universe_by_date.keys()))
    if isinstance(sample_key, str):
        q_pairs = [(k, _quarter_start_from_key(k)) for k in universe_by_date.keys()]
    else:
        q_pairs = [(k, pd.Timestamp(k).normalize()) for k in universe_by_date.keys()]
    q_pairs.sort(key=lambda x: x[1])
    return [k for k, _ in q_pairs], [d for _, d in q_pairs]


def _cache_find_active_quarter(d, quarter_labels, quarter_ends):
    import bisect
    idx = bisect.bisect_right(quarter_ends, d) - 1
    if idx < 0:
        return None
    return quarter_labels[idx]


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

    q_labels, q_ends = _cache_build_quarter_lookup(universe_by_date)
    if not q_labels:
        return pd.DataFrame()

    equity = 1.0
    rows = []
    for d, g in df.groupby('Date'):
        q_key = _cache_find_active_quarter(d, q_labels, q_ends)
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


_DATA_SIGNATURE_CACHE = None


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


def _prebuild_equity_cache_from_signals(all_df):
    data_sig = _get_data_signature(all_df)
    latest_source = data_sig.get('latest_date')
    if pd.isna(latest_source):
        return
    latest_source = pd.Timestamp(latest_source).normalize()

    specs = [
        ('High Beta', BETA_UNIVERSE, 'thematic'),
        ('Low Beta', LOW_BETA_UNIVERSE, 'thematic'),
        ('Momentum Leaders', MOMENTUM_UNIVERSE, 'thematic'),
        ('Momentum Losers', MOMENTUM_LOSERS_UNIVERSE, 'thematic'),
        ('High Dividend Yield', HIGH_YIELD_UNIVERSE, 'thematic'),
        ('Dividend Growth', DIV_GROWTH_UNIVERSE, 'thematic'),
        ('Dividend with Growth', DIV_WITH_GROWTH_UNIVERSE, 'thematic'),
        ('Risk Adj Momentum', RISK_ADJ_MOM_UNIVERSE, 'thematic'),
        ('Size', SIZE_UNIVERSE, 'thematic'),
        ('Volume Growth', VOLUME_GROWTH_UNIVERSE, 'thematic'),
    ]
    specs += [(name, uni, 'sector') for name, uni in SECTOR_UNIVERSES.items()]
    specs += [(name, uni, 'industry') for name, uni in INDUSTRY_UNIVERSES.items()]

    built = 0
    skipped = 0
    total = len(specs)
    last_milestone = 0
    for i, (name, universe, btype) in enumerate(specs, start=1):
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
    rot_high = np.full(n, np.nan)   # running max during up rotations
    rot_low = np.full(n, np.nan)    # running min during down rotations
    is_bull_div = np.zeros(n, dtype=bool)
    is_bear_div = np.zeros(n, dtype=bool)

    prev_down_low = np.nan   # low of the second-to-last completed down rotation
    last_down_low = np.nan   # low of the most recently completed down rotation
    prev_up_high = np.nan    # high of the second-to-last completed up rotation
    last_up_high = np.nan    # high of the most recently completed up rotation
    cur_high = np.nan
    cur_low = np.nan

    for i in range(start_idx, n):
        t = trends[i]
        if t is None:
            continue

        if is_up_rot[i]:
            # A down rotation just completed — record its low
            if not np.isnan(cur_low):
                prev_down_low = last_down_low
                last_down_low = cur_low
            # Check bullish divergence: higher low than previous down rotation
            if not np.isnan(prev_down_low) and not np.isnan(last_down_low):
                if last_down_low > prev_down_low:
                    is_bull_div[i] = True
            cur_high = ema[i]
            cur_low = np.nan

        elif is_down_rot[i]:
            # An up rotation just completed — record its high
            if not np.isnan(cur_high):
                prev_up_high = last_up_high
                last_up_high = cur_high
            # Check bearish divergence: lower high than previous up rotation
            if not np.isnan(prev_up_high) and not np.isnan(last_up_high):
                if last_up_high < prev_up_high:
                    is_bear_div[i] = True
            cur_low = ema[i]
            cur_high = np.nan

        # Update running extremes
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


def compute_signal_trades(df, entry_col, exit_col, direction='long',
                          price_col='Close', high_col='High', low_col='Low', date_col='Date'):
    """Compute simple entry/exit trades based on entry/exit signals."""
    trades = []
    in_pos = False
    entry_idx = None
    entry_price = None
    max_high = None
    min_low = None

    for i in range(len(df)):
        if in_pos:
            hi = df.at[i, high_col]
            lo = df.at[i, low_col]
            if pd.notna(hi):
                max_high = hi if max_high is None else max(max_high, hi)
            if pd.notna(lo):
                min_low = lo if min_low is None else min(min_low, lo)

            if bool(df.at[i, exit_col]):
                exit_price = df.at[i, price_col]
                if pd.isna(exit_price) or entry_price is None or entry_price == 0:
                    in_pos = False
                    entry_idx = None
                    continue

                if direction == 'short':
                    change = (entry_price - exit_price) / entry_price
                    mfe = (entry_price - min_low) / entry_price if min_low is not None else np.nan
                    mae = (entry_price - max_high) / entry_price if max_high is not None else np.nan
                else:
                    change = (exit_price - entry_price) / entry_price
                    mfe = (max_high - entry_price) / entry_price if max_high is not None else np.nan
                    mae = (min_low - entry_price) / entry_price if min_low is not None else np.nan

                trades.append({
                    'entry_date': df.at[entry_idx, date_col],
                    'exit_date': df.at[i, date_col],
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'change': change,
                    'mfe': mfe,
                    'mae': mae,
                    'bars': i - entry_idx,
                })
                in_pos = False
                entry_idx = None
                entry_price = None
                max_high = None
                min_low = None

        if not in_pos and bool(df.at[i, entry_col]):
            price = df.at[i, price_col]
            if pd.isna(price) or price == 0:
                continue
            in_pos = True
            entry_idx = i
            entry_price = price
            max_high = df.at[i, high_col] if pd.notna(df.at[i, high_col]) else None
            min_low = df.at[i, low_col] if pd.notna(df.at[i, low_col]) else None

    return trades


import bisect


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
    # Assign quarter key to each row via searchsorted
    quarter_ends_ts = pd.DatetimeIndex(quarter_ends)
    date_vals = df['Date'].values
    idx = np.searchsorted(quarter_ends_ts.values, date_vals, side='right') - 1

    # Exclude dates before the first quarter (idx == -1)
    valid_mask = idx >= 0
    df = df[valid_mask].copy()
    idx = idx[valid_mask]

    label_arr = np.array(quarter_labels)
    df['_q_key'] = label_arr[idx]

    # Build membership table if not provided (reuse across callers for performance)
    if membership_df is None:
        membership_df = _build_membership_df(universe_by_date)
    return df.merge(membership_df, on=['_q_key', 'Ticker'], how='inner')


def compute_breadth_from_trend(all_df, universe_by_date, membership_df=None):
    """Count uptrend vs downtrend stocks per day (vectorized)."""
    df = all_df[['Date', 'Ticker', 'Trend']].copy()
    df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
    df = df.dropna(subset=['Trend'])

    quarter_labels, quarter_ends = _build_quarter_lookup(universe_by_date)
    df = _vectorized_quarter_filter(df, universe_by_date, quarter_labels, quarter_ends, membership_df=membership_df)
    if df.empty:
        return pd.DataFrame(columns=['Date', 'Uptrend_Count', 'Downtrend_Count', 'Total_Stocks', 'Breadth_Ratio'])

    # Trend is float32: 1.0=up, 0.0=down
    is_up = (df['Trend'] == 1.0) if df['Trend'].dtype != object else (df['Trend'] == True)
    is_down = (df['Trend'] == 0.0) if df['Trend'].dtype != object else (df['Trend'] == False)

    grouped = df.groupby('Date')
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

            # Date range (exclude next quarter's start)
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

            # Initial weights
            w0 = pd.Series({t: w_dict[t] for t in tickers})
            w0 = w0 / w0.sum()

            # Cumulative growth and portfolio value (buy-and-hold within quarter)
            cum_growth = (1 + close_ret).cumprod()
            portfolio_value = (cum_growth * w0).sum(axis=1)

            # Equity close = previous quarter's last close * portfolio growth
            eq_close = equity_prev_close * portfolio_value

            # Previous close for OHLC computation
            eq_prev_close_series = eq_close.shift(1).fillna(equity_prev_close)

            # BOD weights (drifted)
            cum_growth_prev = cum_growth.shift(1).fillna(1.0)
            portfolio_value_prev = portfolio_value.shift(1).fillna(1.0)
            bod_weights = cum_growth_prev.multiply(w0, axis=1).div(portfolio_value_prev, axis=0)

            # Weighted portfolio OHLC returns
            port_open_ret = (bod_weights * open_ret).sum(axis=1)
            port_high_ret = (bod_weights * high_ret).sum(axis=1)
            port_low_ret = (bod_weights * low_ret).sum(axis=1)

            eq_open = eq_prev_close_series * (1 + port_open_ret)
            eq_high = eq_prev_close_series * (1 + port_high_ret)
            eq_low = eq_prev_close_series * (1 + port_low_ret)

            # OHLC constraint
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

            # Contributions as byproduct
            if return_contributions:
                contributions = bod_weights * close_ret
                bod_long = bod_weights.stack().rename('Weight_BOD')
                ret_long = close_ret.stack().rename('Daily_Return')
                contrib_long = contributions.stack().rename('Contribution')
                q_contrib = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
                q_contrib.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
                contrib_parts.append(q_contrib)

            # Carry forward equity_prev_close for next quarter
            equity_prev_close = float(eq_close.iloc[-1])
            # Build state for caching
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
                # Append rebalance rows to contributions: Q2 initial BOD weights
                # on the quarter start date so the contributions reflect the new basket
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


def _build_universe_signature(universe_by_date):
    h = hashlib.sha256()
    for q in sorted(universe_by_date.keys()):
        h.update(str(q).encode('utf-8'))
        for t in sorted(universe_by_date.get(q, set())):
            h.update(b'|')
            h.update(str(t).encode('utf-8'))
        h.update(b';')
    return h.hexdigest()


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
    # If path has wildcard (glob fallback), resolve to actual file
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
    # Embed CHART_SCHEMA_VERSION in parquet metadata (improvement G)
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
        return False  # data rolled back — rebuild
    if current_source_date.normalize() == cached_source_date.normalize():
        return meta.get('data_fingerprint') == data_sig.get('fingerprint')
    return True  # source date advanced — incremental append will handle the delta


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
        current_key = _get_current_quarter_key()
        if current_key is None:
            keys = sorted(universe_by_qtr.keys()) if universe_by_qtr else []
            current_key = keys[-1] if keys else None
        basket_size = len(universe_by_qtr.get(current_key, set())) if current_key else 0
        return f'{slug}_{basket_size}_of_{SIZE}_{suffix}'
    else:
        return f'{slug}_of_{SIZE}_{suffix}'


def _basket_cache_paths(slug, basket_type='sector', universe_by_qtr=None):
    """Return (parquet_path, meta_path) for the consolidated basket file."""
    folder = _basket_cache_folder(basket_type)
    if universe_by_qtr is not None:
        stem = _cache_file_stem(slug, basket_type, universe_by_qtr, 'signals')
    else:
        # Fallback: try to glob for existing file
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

# --- Basket Signals ---


def _get_chart_schema_version_from_parquet(slug: str) -> int | None:
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


def compute_equity_ohlc_cached(all_df, universe_by_date, basket_name, slug, basket_type='sector',
                               returns_matrix=None, ohlc_ret_matrices=None):
    data_sig = _get_data_signature(all_df)
    universe_sig = _build_universe_signature(universe_by_date)
    cached_df, meta = _load_equity_cache(slug, basket_type, universe_by_date)

    if FORCE_REBUILD_EQUITY_CACHE or FORCE_REBUILD_BASKET_SIGNALS:
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
        return cached_df, None  # contributions not recomputed when fully cached

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
    """Compatibility helper for Cells 11-12 (returns Date + Equity)."""
    ohlc_df = compute_equity_ohlc(all_df, universe_by_date)
    if ohlc_df.empty:
        return pd.DataFrame()
    eq_df = ohlc_df[['Date', 'Close']].copy()
    eq_df.rename(columns={'Close': 'Equity'}, inplace=True)
    return eq_df


def _fmt_price(x):
    return f"${x:,.2f}" if pd.notna(x) else ""


def _fmt_bars(x):
    return str(int(round(x))) if pd.notna(x) else ""


def _fmt_pct(x):
    return f"{x * 100:.2f}%" if pd.notna(x) else ""


def _append_trade_rows(rows, signal_name, direction, trades, bull_div_dates, bear_div_dates):
    closed_changes, closed_mfe, closed_mae, closed_bars = [], [], [], []
    for t in trades:
        entry_dt = pd.to_datetime(t['entry_date']).normalize()
        if entry_dt in bull_div_dates:
            div_type = 'Bullish'
        elif entry_dt in bear_div_dates:
            div_type = 'Bearish'
        else:
            div_type = ''
        closed_changes.append(t['change'])
        closed_mfe.append(t['mfe'])
        closed_mae.append(t['mae'])
        closed_bars.append(t['bars'])
        rolling_stats = calc_rolling_stats(closed_changes, closed_mfe, closed_mae, bars_list=closed_bars)
        rows.append({
            'Signal': signal_name,
            'Divergence_Type': div_type,
            'Direction': direction,
            'Entry_Date': t['entry_date'],
            'Exit_Date': t['exit_date'],
            'Entry_Price': t['entry_price'],
            'Exit_Price': t['exit_price'],
            'Final_Change': t['change'],
            'MFE': t['mfe'],
            'MAE': t['mae'],
            'Bars': t['bars'],
            'Win_Rate': rolling_stats.get('Win_Rate', np.nan),
            'Avg_Winner': rolling_stats.get('Avg_Winner', np.nan),
            'Avg_Loser': rolling_stats.get('Avg_Loser', np.nan),
            'Avg_Winner_Bars': rolling_stats.get('Avg_Winner_Bars', np.nan),
            'Avg_Loser_Bars': rolling_stats.get('Avg_Loser_Bars', np.nan),
            'Avg_MFE': rolling_stats.get('Avg_MFE', np.nan),
            'Avg_MAE': rolling_stats.get('Avg_MAE', np.nan),
            'Historical_EV': rolling_stats.get('Historical_EV', np.nan),
            'Std_Dev': rolling_stats.get('Std_Dev', np.nan),
            'Risk_Adj_EV': rolling_stats.get('Risk_Adj_EV', np.nan),
            'EV_Last_3': rolling_stats.get('EV_Last_3', np.nan),
            'Risk_Adj_EV_Last_3': rolling_stats.get('Risk_Adj_EV_Last_3', np.nan),
            'Count': rolling_stats.get('Count', 0),
        })


def _compute_within_basket_correlation(universe_by_qtr, returns_matrix, window=21):
    """Compute rolling within-basket average pairwise correlation via variance decomposition.

    For each rolling window, z-scores returns WITHIN that window, then applies:
        avg_pairwise_corr = (n * Var(EW z-portfolio) - 1) / (n - 1)
    This gives the exact simple average of pairwise correlations.

    Uses numpy for the inner loop (O(n*w) per date vs O(n²*w) for full .corr()).

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

        # Date range for this quarter
        q_start = quarter_ends[q_idx]
        q_end = quarter_ends[q_idx + 1] if q_idx + 1 < len(quarter_ends) else returns_matrix.index[-1]
        # Include window days before q_start for rolling warm-up
        warmup_start_idx = returns_matrix.index.searchsorted(q_start) - window
        warmup_start = returns_matrix.index[max(0, warmup_start_idx)]
        sub_ret = returns_matrix.loc[warmup_start:q_end, tickers]

        if len(sub_ret) < window:
            continue

        # Filter to tickers with sufficient data in the quarter period
        q_data = sub_ret.loc[q_start:]
        valid_tickers = [t for t in tickers if q_data[t].notna().sum() >= min_obs]
        if len(valid_tickers) < 2:
            continue
        sub_ret = sub_ret[valid_tickers]

        # Convert to numpy for fast inner loop
        ret_arr = sub_ret.values  # (T, n)
        dates_arr = sub_ret.index
        q_start_idx = sub_ret.index.searchsorted(q_start)
        # Exclusive upper bound for output: exclude next quarter's start date
        if q_idx + 1 < len(quarter_ends):
            q_output_end = q_end - pd.Timedelta(days=1)
        else:
            q_output_end = q_end

        for d_idx in range(window, len(dates_arr)):
            d = dates_arr[d_idx]
            if d < q_start or d > q_output_end:
                continue
            w_slice = ret_arr[d_idx - window + 1:d_idx + 1, :]  # (window, n)
            # Valid columns: enough non-NaN in this window
            col_counts = np.sum(~np.isnan(w_slice), axis=0)
            col_valid = col_counts >= min_obs
            nv = col_valid.sum()
            if nv < 2:
                all_dates.append(d)
                all_corrs.append(np.nan)
                continue
            w = w_slice[:, col_valid]
            # Z-score within this window
            means = np.nanmean(w, axis=0)
            stds = np.nanstd(w, axis=0, ddof=1)
            stds[stds == 0] = np.nan
            z = (w - means) / stds
            # EW portfolio of z-scores
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


def _augment_basket_signals_with_breadth(signals_df, universe_by_qtr):
    _do_timing = BENCHMARK_TIMING or BENCHMARK_BASKETS > 0

    # Build membership table once, shared by both breadth calls
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
    # Drop any breadth columns already present (e.g. carried forward from prev_row
    # via _build_signals_next_row when running the incremental-append path).
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


def _finalize_basket_signals_output(name, slug, hist_folder, merged_all, data_sig, universe_sig, universe_by_qtr, basket_type='sector', returns_matrix=None, contrib_df=None):
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

    # Build combined meta (merges old equity_meta + basket_signals_meta)
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
        _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=returns_matrix)
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


def _compute_and_save_contributions(slug, basket_type, universe_by_qtr, returns_matrix=None):
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

        # Date range for this quarter (exclude next quarter's start)
        q_start = quarter_ends[q_idx]
        if q_idx + 1 < len(quarter_ends):
            q_end = quarter_ends[q_idx + 1] - pd.Timedelta(days=1)
        else:
            q_end = returns_matrix.index[-1]
        rets_q = returns_matrix.loc[q_start:q_end, tickers].copy()
        if rets_q.empty:
            continue
        rets_q = rets_q.fillna(0.0)

        # Initial weights vector
        w0 = pd.Series({t: w_dict[t] for t in tickers})
        # Normalize in case some tickers are missing from the matrix
        w0 = w0 / w0.sum()

        # Drift weights via cumprod: w_t = w0 * cumprod(1 + ret) / sum(...)
        cum_growth = (1 + rets_q).cumprod()
        # Weighted cum_growth for each ticker
        weighted_cum = cum_growth.multiply(w0, axis=1)
        # Normalize each row to get drifted weights (beginning-of-day for NEXT day)
        row_sums = weighted_cum.sum(axis=1)
        row_sums = row_sums.replace(0, np.nan)
        drifted_weights = weighted_cum.div(row_sums, axis=0)

        # BOD weights: first day uses w0, subsequent days use previous day's drifted weights
        bod_weights = drifted_weights.shift(1)
        bod_weights.iloc[0] = w0

        # Contributions = BOD weight * daily return
        contributions = bod_weights * rets_q

        # Melt to long form
        for col_name, matrix, val_name in [
            ('Weight_BOD', bod_weights, 'Weight_BOD'),
            ('Daily_Return', rets_q, 'Daily_Return'),
            ('Contribution', contributions, 'Contribution'),
        ]:
            pass  # handled below via stack

        # Stack all three matrices at once
        bod_long = bod_weights.stack().rename('Weight_BOD')
        ret_long = rets_q.stack().rename('Daily_Return')
        contrib_long = contributions.stack().rename('Contribution')
        q_df = pd.concat([bod_long, ret_long, contrib_long], axis=1).reset_index()
        q_df.columns = ['Date', 'Ticker', 'Weight_BOD', 'Daily_Return', 'Contribution']
        quarter_dfs.append(q_df)

    if not quarter_dfs:
        return

    contrib_df = pd.concat(quarter_dfs, ignore_index=True)
    # Remove duplicate dates at quarter boundaries (keep last quarter's values)
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


def _record_basket_timing(name, **timings):
    """Record per-step timings for a basket into the global accumulator."""
    _basket_timing_names.append(name)
    for k in _basket_timing:
        _basket_timing[k].append(timings.get(k, 0.0))
    parts = " | ".join(f"{k}={timings.get(k, 0.0):.1f}s" for k in _basket_timing if k != 'total')
    print(f"  [TIMING] {name}: total={timings.get('total', 0.0):.1f}s | {parts}")


def process_basket_signals(name, universe_by_qtr, charts_folder, basket_type='sector', returns_matrix=None, ohlc_ret_matrices=None):
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
        not FORCE_REBUILD_BASKET_SIGNALS
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
                appended_merged = _augment_basket_signals_with_breadth(appended_signals_df, universe_by_qtr)
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
                    name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig, universe_by_qtr, basket_type,
                    returns_matrix=returns_matrix, contrib_df=_contrib_df,
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
    merged_all = _augment_basket_signals_with_breadth(signals_df, universe_by_qtr)
    if _do_timing:
        _t_breadth_trend = getattr(_augment_basket_signals_with_breadth, '_last_trend_time', 0.0)
        _t_breadth_breakout = getattr(_augment_basket_signals_with_breadth, '_last_breakout_time', 0.0)
        _t_breadth_merge = getattr(_augment_basket_signals_with_breadth, '_last_merge_time', 0.0)
    result = _finalize_basket_signals_output(
        name, slug, hist_folder, merged_all, _bsig_data_sig, _bsig_universe_sig, universe_by_qtr, basket_type,
        returns_matrix=returns_matrix, contrib_df=_contrib_df,
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

# --- Basket Processing Loop ---

print("Building basket signals (consolidated: OHLC + signals + breadth + correlation)...")

# Pre-compute shared returns matrix (Date × Ticker) for basket correlation
_ret_matrix_path = DATA_FOLDER / 'returns_matrix_500.parquet'
_ret_fingerprint = f"{all_signals_df.shape}_{all_signals_df['Date'].max()}_{sorted(all_signals_df['Ticker'].unique())}"
_ret_fp_hash = hashlib.md5(_ret_fingerprint.encode()).hexdigest()
_ret_fp_path = DATA_FOLDER / 'returns_matrix_500.fingerprint'
_rebuild_ret_matrix = True
if _ret_matrix_path.exists() and _ret_fp_path.exists():
    try:
        if _ret_fp_path.read_text().strip() == _ret_fp_hash:
            returns_matrix = pd.read_parquet(_ret_matrix_path)
            _rebuild_ret_matrix = False
            print(f"  Returns matrix loaded from cache ({returns_matrix.shape})")
            # Build OHLC returns matrices (not cached — fast pivot from all_signals_df)
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

BASKET_RESULTS = {}
all_baskets = [
    ('High Beta',           BETA_UNIVERSE,           THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Low Beta',            LOW_BETA_UNIVERSE,        THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Momentum Leaders',    MOMENTUM_UNIVERSE,        THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Momentum Losers',     MOMENTUM_LOSERS_UNIVERSE, THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('High Dividend Yield', HIGH_YIELD_UNIVERSE,      THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Dividend Growth',     DIV_GROWTH_UNIVERSE,      THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Dividend with Growth', DIV_WITH_GROWTH_UNIVERSE, THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Risk Adj Momentum',   RISK_ADJ_MOM_UNIVERSE,    THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Size',                SIZE_UNIVERSE,            THEMATIC_CHARTS_FOLDER, 'thematic'),
    ('Volume Growth',       VOLUME_GROWTH_UNIVERSE,   THEMATIC_CHARTS_FOLDER, 'thematic'),
]
all_baskets += [(s, u, SECTOR_CHARTS_FOLDER, 'sector') for s, u in SECTOR_UNIVERSES.items()]
all_baskets += [(ind, u, INDUSTRY_CHARTS_FOLDER, 'industry') for ind, u in INDUSTRY_UNIVERSES.items()]

total_baskets = len(all_baskets)
last_milestone = 0
for i, (basket_name, basket_universe, basket_folder, basket_type) in enumerate(all_baskets, start=1):
    result = process_basket_signals(basket_name, basket_universe, basket_folder, basket_type, returns_matrix=returns_matrix, ohlc_ret_matrices=ohlc_ret_matrices)
    if result is not None:
        BASKET_RESULTS[basket_name] = result
    percent = int((i / total_baskets) * 100)
    milestone = percent // 10 * 10
    if milestone > last_milestone and milestone % 10 == 0:
        print(f"  {milestone}% complete ({i} / {total_baskets} baskets)")
        last_milestone = milestone
    if BENCHMARK_BASKETS > 0 and i >= BENCHMARK_BASKETS:
        print(f"  BENCHMARK_BASKETS={BENCHMARK_BASKETS} limit reached, stopping early.")
        break
print(f"Basket signals complete: {len(BASKET_RESULTS)} / {total_baskets} baskets processed.")

# --- Timing summary ---
if (BENCHMARK_TIMING or BENCHMARK_BASKETS > 0) and _basket_timing_names:
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

# %% [markdown]
## Live Intraday Data

if 'reset_cell_timer' in globals():
    reset_cell_timer("Live Intraday Signal Exports")

import os
import requests
from dotenv import load_dotenv
import databento as db
from zoneinfo import ZoneInfo
from pathlib import Path
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.patches import Rectangle

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
# LIVE_ROTATIONS_FOLDER is defined via OutputPaths aliases near the top of this file


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


def _get_latest_norgate_date_fallback():
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and 'all_signals_df' in globals() and not all_signals_df.empty and 'Date' in all_signals_df.columns:
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


def build_signals_for_ticker_live(ticker, live_price, live_dt):
    if 'all_signals_df' not in globals():
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

    if not current_universe:
        print(f"No universe found for {current_key}")
        return None

    current_universe = QUARTER_UNIVERSE.get(current_key, set())

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
                    hist_ev = last_row.get(f'{sig_name}_Historical_EV', np.nan)
                    ev_last_3 = last_row.get(f'{sig_name}_EV_Last_3', np.nan)
                    risk_adj_ev = last_row.get(f'{sig_name}_Risk_Adj_EV', np.nan)
                    risk_adj_ev_last_3 = last_row.get(f'{sig_name}_Risk_Adj_EV_Last_3', np.nan)
                    rows.append({
                        'Date': now.date(),
                        'Ticker': ticker,
                        'Close': live_price,
                        'Signal_Type': sig_name,
                        'Theme': _get_ticker_theme(ticker) if '_get_ticker_theme' in globals() else '',
                        'Sector': TICKER_SECTOR.get(ticker, '') if 'TICKER_SECTOR' in globals() else '',
                        'Industry': TICKER_SUBINDUSTRY.get(ticker, '') if 'TICKER_SUBINDUSTRY' in globals() else '',
                        'Entry_Price': last_row.get(f'{sig_name}_Entry_Price', np.nan),
                        'Win_Rate': last_row.get(f'{sig_name}_Win_Rate', np.nan),
                        'Avg_Winner': last_row.get(f'{sig_name}_Avg_Winner', np.nan),
                        'Avg_Loser': last_row.get(f'{sig_name}_Avg_Loser', np.nan),
                        'Avg_Winner_Bars': last_row.get(f'{sig_name}_Avg_Winner_Bars', np.nan),
                        'Avg_Loser_Bars': last_row.get(f'{sig_name}_Avg_Loser_Bars', np.nan),
                        'Avg_MFE': last_row.get(f'{sig_name}_Avg_MFE', np.nan),
                        'Avg_MAE': last_row.get(f'{sig_name}_Avg_MAE', np.nan),
                        'Std_Dev': last_row.get(f'{sig_name}_Std_Dev', np.nan),
                        'Historical_EV': hist_ev,
                        'EV_Last_3': ev_last_3,
                        'Risk_Adj_EV': risk_adj_ev,
                        'Risk_Adj_EV_Last_3': risk_adj_ev_last_3,
                        'Count': last_row.get(f'{sig_name}_Count', np.nan),
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
            'Theme', 'Sector', 'Industry',
            'Entry_Price',
            'Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
            'Avg_MFE', 'Avg_MAE',
            'Std_Dev', 'Historical_EV', 'EV_Last_3',
            'Risk_Adj_EV', 'Risk_Adj_EV_Last_3', 'Count',
        ]
        out_df = out_df[[c for c in col_order if c in out_df.columns]]
        pct_cols = [
            'Win_Rate', 'Avg_Winner', 'Avg_Loser',
            'Avg_MFE', 'Avg_MAE', 'Std_Dev',
            'Historical_EV', 'EV_Last_3',
            'Risk_Adj_EV', 'Risk_Adj_EV_Last_3',
        ]
        for col in pct_cols:
            if col in out_df.columns:
                out_df[col] = out_df[col].apply(
                    lambda x: f"{x * 100:.2f}%" if pd.notna(x) else ""
                )
        for col in ['Close', 'Entry_Price']:
            if col in out_df.columns:
                out_df[col] = out_df[col].apply(_fmt_price)
        for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
            if col in out_df.columns:
                out_df[col] = out_df[col].apply(_fmt_bars)
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


def _get_basket_ohlc_for_reports(group_name, universe_by_qtr, cache_key_name=None):
    cache_name = cache_key_name if cache_key_name else group_name
    if '_slugify_label' in globals():
        slug = _slugify_label(cache_name)
    else:
        slug = str(cache_name).replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')
    # Try consolidated basket parquet first
    basket_pq = _find_basket_parquet(slug)
    if basket_pq:
        try:
            cached = pd.read_parquet(basket_pq)
            if isinstance(cached, pd.DataFrame) and not cached.empty:
                return cached
        except Exception:
            pass
    # Fallback to equity cache (search all basket cache folders)
    for _eq_folder in [paths.thematic_basket_cache, paths.sector_basket_cache, paths.industry_basket_cache, DATA_FOLDER]:
        # New naming: *_ohlc.parquet
        _ohlc_matches = list(_eq_folder.glob(f'{slug}_*_of_{SIZE}_ohlc.parquet'))
        if not _ohlc_matches:
            _ohlc_matches = list(_eq_folder.glob(f'{slug}_of_{SIZE}_ohlc.parquet'))
        # Legacy fallback: *_equity_ohlc.parquet
        if not _ohlc_matches:
            _legacy = _eq_folder / f'{slug}_equity_ohlc.parquet'
            if _legacy.exists():
                _ohlc_matches = [_legacy]
        for cache_path in _ohlc_matches[:1]:
            try:
                cached = pd.read_parquet(cache_path)
                if isinstance(cached, pd.DataFrame) and not cached.empty:
                    return cached
            except Exception:
                pass
    print(f"[{cache_name}] basket parquet missing. Run basket processing cell first.")
    return pd.DataFrame()


def _compute_annual_returns_for_basket(group_name, universe_by_qtr, cache_key_name=None, live_ctx=None):
    ohlc_df = _get_basket_ohlc_for_reports(group_name, universe_by_qtr, cache_key_name)
    if ohlc_df.empty:
        return None
    eq = ohlc_df[['Date', 'Close']].copy()
    eq['Date'] = pd.to_datetime(eq['Date']).dt.normalize()
    eq = eq.dropna(subset=['Close']).sort_values('Date')
    if eq.empty:
        return None
    eq['Year'] = eq['Date'].dt.year
    yearly = eq.groupby('Year')['Close'].agg(['first', 'last'])
    out = np.where(yearly['first'] > 0, (yearly['last'] / yearly['first']) - 1.0, np.nan)
    out_series = pd.Series(out, index=yearly.index, name=group_name)

    if live_ctx is not None:
        live_year = int(pd.Timestamp(live_ctx['today']).year)
        if live_year in yearly.index:
            first_close = yearly.at[live_year, 'first']
            last_close = yearly.at[live_year, 'last']
            if pd.notna(first_close) and pd.notna(last_close) and float(first_close) > 0 and float(last_close) > 0:
                live_ret = _compute_live_basket_return(
                    universe_by_qtr,
                    live_ctx['live_price_map'],
                    live_ctx['last_rows'],
                    live_ctx['current_key'],
                )
                if pd.notna(live_ret):
                    live_close = float(last_close) * (1.0 + float(live_ret))
                    out_series.loc[live_year] = (live_close / float(first_close)) - 1.0
    return out_series


def _build_group_annual_return_grid(group_specs, live_ctx=None):
    by_group = {}
    for spec in group_specs:
        if len(spec) == 3:
            group_name, universe_by_qtr, cache_key_name = spec
        else:
            group_name, universe_by_qtr = spec
            cache_key_name = group_name
        annual_series = _compute_annual_returns_for_basket(
            group_name,
            universe_by_qtr,
            cache_key_name,
            live_ctx=live_ctx,
        )
        if annual_series is not None and not annual_series.empty:
            by_group[group_name] = annual_series
    if not by_group:
        return pd.DataFrame()
    grid = pd.concat(by_group, axis=1).sort_index()
    grid.index.name = 'Year'
    return grid


def _compute_daily_returns_for_basket(group_name, universe_by_qtr, cache_key_name=None):
    ohlc_df = _get_basket_ohlc_for_reports(group_name, universe_by_qtr, cache_key_name)
    if ohlc_df.empty:
        return None
    eq = ohlc_df[['Date', 'Close']].copy()
    eq['Date'] = pd.to_datetime(eq['Date']).dt.normalize()
    eq = eq.dropna(subset=['Close']).sort_values('Date')
    if eq.empty:
        return None
    eq['Daily_Return'] = eq['Close'].pct_change()
    out = eq.set_index('Date')['Daily_Return'].dropna()
    out.name = group_name
    return out


def _get_latest_norgate_rows_by_ticker(before_date=None):
    df = all_signals_df
    if before_date is not None:
        cutoff = pd.Timestamp(before_date).normalize()
        # all_signals_df['Date'] is already datetime64[ns]
        df = df[df['Date'] < cutoff]
    return (
        df.sort_values('Date')
        .groupby('Ticker', as_index=False)
        .tail(1)
        .set_index('Ticker')
    )


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


def _build_group_daily_return_grid(group_specs, live_ctx=None):
    if live_ctx is None:
        live_ctx = _get_live_update_context()
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


def _render_return_table_pages(pdf, title, basket_col_grid, columns_per_page=10, fixed_range=0.50):
    if basket_col_grid.empty:
        return

    basket_col_grid = basket_col_grid.sort_index(axis=1)
    col_labels = basket_col_grid.columns.tolist()
    for start in range(0, len(col_labels), columns_per_page):
        sub_cols = col_labels[start:start + columns_per_page]
        sub = basket_col_grid[sub_cols].copy()
        text_rows = []
        color_rows = []
        bar_specs = []

        range_abs = abs(float(fixed_range))
        if range_abs <= 0:
            range_abs = 0.50
        col_norm = {col: (-range_abs, range_abs) for col in sub_cols}
        col_axis = {col: 0.5 for col in sub_cols}

        for basket_name, row in sub.iterrows():
            text_row = [basket_name]
            color_row = [(1.0, 1.0, 1.0, 1.0)]
            for col_idx, v in enumerate(row.values):
                col = sub_cols[col_idx]
                if pd.isna(v):
                    text_row.append("")
                    color_row.append((1.0, 1.0, 1.0, 1.0))
                else:
                    text_row.append(f"{float(v) * 100:.2f}%")
                    bounds = col_norm.get(col)
                    if bounds is None:
                        color_row.append((1.0, 1.0, 1.0, 1.0))
                    else:
                        vmin, vmax = bounds
                        v_clipped = min(max(float(v), vmin), vmax)
                        t = (v_clipped - vmin) / (vmax - vmin)
                        t = max(0.0, min(1.0, t))
                        axis_t = max(0.0, min(1.0, float(col_axis.get(col, 0.0))))
                        bar_specs.append((len(text_rows), col_idx, t, axis_t))
                        color_row.append((1.0, 1.0, 1.0, 1.0))
            text_rows.append(text_row)
            color_rows.append(color_row)

        fig = plt.figure(figsize=(11, 8.5))
        ax = fig.add_axes([0.03, 0.06, 0.94, 0.88])
        ax.axis('off')
        first_col = sub_cols[0] if sub_cols else None
        last_col = sub_cols[-1] if sub_cols else None
        if first_col is not None and last_col is not None:
            if isinstance(first_col, (pd.Timestamp, datetime)):
                first_label = pd.Timestamp(first_col).strftime('%Y-%m-%d')
                last_label = pd.Timestamp(last_col).strftime('%Y-%m-%d')
            else:
                first_label = str(first_col)
                last_label = str(last_col)
            page_suffix = f" ({first_label} to {last_label})"
        else:
            page_suffix = ""
        ax.set_title(f"{title}{page_suffix}", fontsize=12, fontweight='bold', pad=8)

        headers = ['Basket']
        for col in sub_cols:
            if isinstance(col, (pd.Timestamp, datetime)):
                headers.append(pd.Timestamp(col).strftime('%Y-%m-%d'))
            else:
                headers.append(str(col))
        left_col_w = 0.36
        col_w = (1.0 - left_col_w) / max(1, len(sub_cols))
        col_widths = [left_col_w] + [col_w] * len(sub_cols)
        table = ax.table(
            cellText=text_rows,
            colLabels=headers,
            cellColours=color_rows,
            colLoc='center',
            cellLoc='center',
            colWidths=col_widths,
            bbox=[0.0, 0.0, 1.0, 0.95],
        )
        table.auto_set_font_size(False)
        n_rows = max(1, len(text_rows))
        n_cols = max(1, len(headers))
        font_size = max(3.2, min(8.0, 10.0 - 0.14 * n_cols - 0.06 * n_rows))
        table.set_fontsize(font_size)
        table.scale(1.0, max(0.33, min(1.00, 1.06 - 0.0048 * n_rows)))
        for (r, c), cell in table.get_celld().items():
            if r == 0:
                cell.set_facecolor((0.93, 0.93, 0.93))
                cell.set_text_props(weight='bold')
            if c == 0 and r > 0:
                cell.get_text().set_ha('left')
                cell.PAD = 0.02
            cell.set_edgecolor((0.75, 0.75, 0.75))
            cell.set_linewidth(0.55)

        fig.canvas.draw()
        renderer = fig.canvas.get_renderer()
        inv = ax.transAxes.inverted()

        for row_idx, col_idx, t, axis_t in bar_specs:
            cell = table.get_celld().get((row_idx + 1, col_idx + 1))
            if cell is None:
                continue
            bbox = cell.get_window_extent(renderer=renderer)
            (x0, y0) = inv.transform((bbox.x0, bbox.y0))
            (x1, y1) = inv.transform((bbox.x1, bbox.y1))
            w = x1 - x0
            h = y1 - y0
            if w <= 0 or h <= 0:
                continue

            left_pad = w * 0.03
            right_pad = w * 0.03
            bar_max_w = max(0.0, w - left_pad - right_pad)
            bar_h = h * 0.62
            bar_y = y0 + (h - bar_h) / 2.0
            bar_x = x0 + left_pad
            axis_x = bar_x + bar_max_w * axis_t
            t = max(0.0, min(1.0, float(t)))
            if t >= axis_t:
                frac = 0.0 if axis_t >= 1.0 else (t - axis_t) / (1.0 - axis_t)
                bar_w = bar_max_w * frac * (1.0 - axis_t)
                if bar_w > 0:
                    ax.add_patch(Rectangle((axis_x, bar_y), bar_w, bar_h, facecolor='#0066ff', edgecolor='none', alpha=0.42, zorder=1.5))
            else:
                frac = 0.0 if axis_t <= 0.0 else (axis_t - t) / axis_t
                bar_w = bar_max_w * frac * axis_t
                if bar_w > 0:
                    ax.add_patch(Rectangle((axis_x - bar_w, bar_y), bar_w, bar_h, facecolor='#ff3296', edgecolor='none', alpha=0.42, zorder=1.5))

        pdf.savefig(fig)
        plt.close(fig)


def _render_return_bar_charts(
    pdf_or_list,
    title,
    basket_col_grid,
    y_min=-0.50,
    y_max=0.50,
    n_cols=6,
    figsize=(11, 8.5),
    n_rows_fixed=None,
):
    """Render one column-bar chart per basket on a grid page.
    If n_rows_fixed is provided, every page uses that exact grid size.

    ``pdf_or_list``: pass a ``PdfPages`` object to write directly (legacy),
    or pass a list to collect figures for use with ``build_pdf()``.
    """
    if basket_col_grid.empty:
        return

    basket_col_grid = basket_col_grid.sort_index(axis=1)
    cols = basket_col_grid.columns.tolist()
    baskets = basket_col_grid.index.tolist()

    # Determine x-axis label format from column type
    def _col_label(col):
        if isinstance(col, (pd.Timestamp, datetime)):
            return pd.Timestamp(col).strftime('%b\n%d')
        try:
            return str(int(col))
        except (TypeError, ValueError):
            return str(col)

    x_labels = [_col_label(c) for c in cols]

    # Strip group-type prefix from basket name to save subplot title space
    def _short_name(name):
        for prefix in ('Theme: ', 'Sector: ', 'Industry: '):
            if name.startswith(prefix):
                return name[len(prefix):]
        return name

    default_rows = 4
    rows_per_page = int(n_rows_fixed) if n_rows_fixed is not None else default_rows
    if rows_per_page < 1:
        rows_per_page = default_rows
    charts_per_page = n_cols * rows_per_page

    for page_start in range(0, len(baskets), charts_per_page):
        page_baskets = baskets[page_start:page_start + charts_per_page]
        n_rows = rows_per_page if n_rows_fixed is not None else max(1, -(-len(page_baskets) // n_cols))

        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        fig.patch.set_facecolor('white')
        fig.suptitle(title, fontsize=11, fontweight='bold', y=0.99)

        # Normalise axes to always be 2-D array
        if n_rows == 1 and n_cols == 1:
            axes = [[axes]]
        elif n_rows == 1:
            axes = [list(axes)]
        elif n_cols == 1:
            axes = [[ax] for ax in axes]
        else:
            axes = [list(row) for row in axes]

        for idx, basket_name in enumerate(page_baskets):
            r, c = divmod(idx, n_cols)
            ax = axes[r][c]
            row_series = basket_col_grid.loc[basket_name]
            values = [float(row_series[col]) if pd.notna(row_series[col]) else 0.0 for col in cols]
            bar_colors = ['#0066ff' if v >= 0 else '#ff3296' for v in values]

            ax.bar(range(len(cols)), values, color=bar_colors, width=0.7, zorder=2)
            ax.axhline(0, color='#333333', linewidth=0.6, zorder=3)
            ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x * 100:.0f}%"))
            ax.yaxis.set_tick_params(labelsize=5)
            ax.set_xticks(range(len(cols)))
            single_chart = (n_cols == 1 and rows_per_page == 1)
            x_fs = (6.5 if isinstance(cols[0], (pd.Timestamp, datetime)) else 7.5) if single_chart else \
                   (4.5 if isinstance(cols[0], (pd.Timestamp, datetime)) else 5.5)
            n_x = len(cols)
            sparse_threshold = 30 if single_chart else 8
            if n_x > sparse_threshold:
                mid = n_x // 2
                sparse = [x_labels[i] if i in (0, mid, n_x - 1) else '' for i in range(n_x)]
            else:
                sparse = x_labels
            ax.set_xticklabels(sparse, fontsize=x_fs, rotation=0, linespacing=0.9)
            ax.tick_params(axis='x', length=2, pad=1)
            ax.tick_params(axis='y', length=2, pad=1)
            _title_fs = 11 if (n_cols == 1 and rows_per_page == 1) else 6.5
            ax.set_title(_short_name(basket_name), fontsize=_title_fs, pad=2, fontweight='bold')
            ax.grid(axis='y', linewidth=0.3, alpha=0.4, zorder=0)
            ax.set_facecolor('white')
            for spine in ax.spines.values():
                spine.set_linewidth(0.4)
                spine.set_edgecolor('#aaaaaa')

        # Hide any unused subplot slots
        total_slots = n_rows * n_cols
        for idx in range(len(page_baskets), total_slots):
            r, c = divmod(idx, n_cols)
            axes[r][c].set_visible(False)

        plt.tight_layout(rect=[0, 0, 1, 0.97])
        if isinstance(pdf_or_list, list):
            pdf_or_list.append(fig)
        else:
            pdf_or_list.savefig(fig, dpi=150)
            plt.close(fig)


def _get_all_basket_specs_for_reports():
    specs = [
        ('Theme: High Beta',           BETA_UNIVERSE,           'High Beta'),
        ('Theme: Low Beta',            LOW_BETA_UNIVERSE,        'Low Beta'),
        ('Theme: Momentum Leaders',    MOMENTUM_UNIVERSE,        'Momentum Leaders'),
        ('Theme: Momentum Losers',     MOMENTUM_LOSERS_UNIVERSE, 'Momentum Losers'),
        ('Theme: High Dividend Yield', HIGH_YIELD_UNIVERSE,      'High Dividend Yield'),
        ('Theme: Dividend Growth',     DIV_GROWTH_UNIVERSE,      'Dividend Growth'),
        ('Theme: Dividend with Growth', DIV_WITH_GROWTH_UNIVERSE, 'Dividend with Growth'),
        ('Theme: Risk Adj Momentum',   RISK_ADJ_MOM_UNIVERSE,    'Risk Adj Momentum'),
        ('Theme: Size',                SIZE_UNIVERSE,            'Size'),
        ('Theme: Volume Growth',       VOLUME_GROWTH_UNIVERSE,   'Volume Growth'),
    ]
    specs += [(f"Sector: {name}", SECTOR_UNIVERSES[name], name) for name in sorted(SECTOR_UNIVERSES.keys())]
    specs += [(f"Industry: {name}", INDUSTRY_UNIVERSES[name], name) for name in sorted(INDUSTRY_UNIVERSES.keys())]
    return specs


def _write_live_basket_ohlc(live_ctx):
    """Write live_basket_signals_{SIZE}.parquet with today's basket OHLC bars."""
    if live_ctx is None:
        return
    live_ohlc_map = live_ctx.get('live_ohlc_map', {})
    if not live_ohlc_map:
        return

    all_specs = _get_all_basket_specs_for_reports()
    live_basket_rows = []
    for spec in all_specs:
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


def _build_basket_annual_grid(live_ctx=None):
    """Return basket_year_grid DataFrame (baskets Ã— years) for _render_return_bar_charts."""
    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and 'all_signals_df' in globals() and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        latest_norgate = pd.to_datetime(all_signals_df['Date']).max().normalize()
    if latest_norgate is None:
        return pd.DataFrame()
    if live_ctx is None:
        live_ctx = _get_live_update_context()
    all_specs = _get_all_basket_specs_for_reports()
    annual_grid = _build_group_annual_return_grid(all_specs, live_ctx=live_ctx)
    if annual_grid.empty:
        return pd.DataFrame()
    return annual_grid.T.sort_index(axis=1)


def _build_basket_daily_grid_last20(live_ctx=None):
    """Return basket_date_grid (baskets Ã— last-20 dates) for _render_return_bar_charts."""
    all_specs = _get_all_basket_specs_for_reports()
    if live_ctx is None:
        live_ctx = _get_live_update_context()
    daily_grid = _build_group_daily_return_grid(all_specs, live_ctx=live_ctx)
    if daily_grid.empty:
        return pd.DataFrame()
    daily_grid = daily_grid.sort_index()
    if live_ctx is not None:
        live_today = live_ctx['today']
        for spec in all_specs:
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


def export_annual_returns(live_ctx=None):
    summary_folder = BASE_OUTPUT_FOLDER / 'Baskets' / 'Basket_Reports'
    annual_reports_folder = summary_folder / 'annual_reports'
    annual_reports_folder.mkdir(parents=True, exist_ok=True)

    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and 'all_signals_df' in globals() and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        latest_norgate = pd.to_datetime(all_signals_df['Date']).max().normalize()
    if latest_norgate is None:
        print("Unable to determine latest Norgate date for annual returns export.")
        return

    report_asof_date = pd.Timestamp(live_ctx['today']).normalize() if live_ctx is not None else pd.Timestamp(latest_norgate).normalize()
    date_str = report_asof_date.strftime('%Y_%m_%d')
    out_path = annual_reports_folder / f'{date_str}_annual_returns.pdf'
    _need_write, _need_mirror = _needs_write_and_mirror(out_path)
    if not _need_write and not _need_mirror:
        print(f"Annual returns report already exists for {date_str}, skipping export: {out_path}")
        return
    if not _need_write and _need_mirror:
        WriteThroughPath(out_path).sync()
        return

    basket_year_grid = _build_basket_annual_grid(live_ctx=live_ctx)
    if basket_year_grid.empty:
        print("No annual return data generated.")
        return

    _yr_min = min(-0.10, round(float(basket_year_grid.min().min()) * 1.05 - 0.01, 2))
    _yr_max = max( 0.10, round(float(basket_year_grid.max().max()) * 1.05 + 0.01, 2))
    _annual_figs = []
    _render_return_bar_charts(
        _annual_figs,
        'Annual Returns - All Baskets',
        basket_year_grid,
        y_min=_yr_min, y_max=_yr_max,
        figsize=(11.0, 8.5), n_cols=1, n_rows_fixed=1,
    )
    build_pdf(_annual_figs, out_path)
    print(f"Saved annual returns PDF: {out_path}")


def export_last_20_days_returns(live_ctx=None):
    summary_folder = BASE_OUTPUT_FOLDER / 'Baskets' / 'Basket_Reports'
    summary_folder.mkdir(parents=True, exist_ok=True)
    _now = datetime.now()
    _stamp = _now.strftime('%Y_%m_%d_%H%M')
    out_path = summary_folder / f'{_stamp}_last_20_days_returns.pdf'

    basket_date_grid = _build_basket_daily_grid_last20(live_ctx=live_ctx)
    if basket_date_grid.empty:
        print("No daily return data generated.")
        return

    _d20_min = min(-0.03, round(float(basket_date_grid.min().min()) * 1.05 - 0.005, 3))
    _d20_max = max( 0.03, round(float(basket_date_grid.max().max()) * 1.05 + 0.005, 3))
    _d20_figs = []
    _render_return_bar_charts(
        _d20_figs,
        'Daily Returns - Last 20 Trading Days (All Baskets)',
        basket_date_grid,
        y_min=_d20_min, y_max=_d20_max,
        figsize=(11.0, 8.5), n_cols=1, n_rows_fixed=1,
    )
    build_pdf(_d20_figs, out_path)
    print(f"Saved 20-day returns PDF: {out_path}")


def _render_year_basket_bar_charts(fig_list, year_basket_grid, y_min, y_max, n_cols=4, n_rows_fixed=3, x_fontsize=4):
    """One subplot per year; x-axis = baskets; bars = that year's annual return per basket."""
    if year_basket_grid.empty:
        return

    years = sorted(year_basket_grid.index.tolist())
    baskets = year_basket_grid.columns.tolist()

    def _short_name(name):
        for prefix in ('Theme: ', 'Sector: ', 'Industry: '):
            if name.startswith(prefix):
                return name[len(prefix):]
        return name

    charts_per_page = n_cols * n_rows_fixed

    for page_start in range(0, len(years), charts_per_page):
        page_years = years[page_start:page_start + charts_per_page]

        fig, axes = plt.subplots(n_rows_fixed, n_cols, figsize=(11, 8.5))
        fig.patch.set_facecolor('white')
        fig.suptitle('Annual Returns by Year — All Baskets', fontsize=11, fontweight='bold', y=0.99)

        if n_rows_fixed == 1 and n_cols == 1:
            axes = [[axes]]
        elif n_rows_fixed == 1:
            axes = [list(axes)]
        elif n_cols == 1:
            axes = [[ax] for ax in axes]
        else:
            axes = [list(row) for row in axes]

        for idx, year in enumerate(page_years):
            r, c = divmod(idx, n_cols)
            ax = axes[r][c]
            row = year_basket_grid.loc[year]
            # Sort baskets by return ascending (lowest left, highest right)
            sorted_baskets = sorted(baskets, key=lambda b: float(row[b]) if pd.notna(row[b]) else float('-inf'))
            values = [float(row[b]) if pd.notna(row[b]) else 0.0 for b in sorted_baskets]
            sorted_labels = [_short_name(b) for b in sorted_baskets]
            bar_colors = ['#0066ff' if v >= 0 else '#ff3296' for v in values]

            ax.bar(range(len(sorted_baskets)), values, color=bar_colors, width=0.7, zorder=2)
            ax.axhline(0, color='#333333', linewidth=0.6, zorder=3)
            ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x * 100:.0f}%"))
            ax.yaxis.set_tick_params(labelsize=5)
            ax.set_xticks(range(len(sorted_baskets)))
            ax.set_xticklabels(sorted_labels, fontsize=x_fontsize, rotation=45, ha='right')
            ax.tick_params(axis='x', length=2, pad=1)
            ax.tick_params(axis='y', length=2, pad=1)
            ax.set_title(str(int(year)), fontsize=7, pad=2, fontweight='bold')
            ax.grid(axis='y', linewidth=0.3, alpha=0.4, zorder=0)
            ax.set_facecolor('white')
            for spine in ax.spines.values():
                spine.set_linewidth(0.4)
                spine.set_edgecolor('#aaaaaa')

        total_slots = n_rows_fixed * n_cols
        for idx in range(len(page_years), total_slots):
            r, c = divmod(idx, n_cols)
            axes[r][c].set_visible(False)

        plt.tight_layout(rect=[0, 0, 1, 0.97])
        fig_list.append(fig)


def _render_day_basket_bar_charts(fig_list, date_basket_grid, y_min, y_max, n_cols=2, n_rows_fixed=2, x_fontsize=5):
    """One subplot per trading day; x-axis = baskets sorted by return; bars = that day's return per basket."""
    if date_basket_grid.empty:
        return

    dates = sorted(date_basket_grid.index.tolist())
    baskets = date_basket_grid.columns.tolist()

    def _short_name(name):
        for prefix in ('Theme: ', 'Sector: ', 'Industry: '):
            if name.startswith(prefix):
                return name[len(prefix):]
        return name

    charts_per_page = n_cols * n_rows_fixed

    for page_start in range(0, len(dates), charts_per_page):
        page_dates = dates[page_start:page_start + charts_per_page]

        fig, axes = plt.subplots(n_rows_fixed, n_cols, figsize=(11, 8.5))
        fig.patch.set_facecolor('white')
        fig.suptitle('Daily Returns by Day — All Baskets', fontsize=11, fontweight='bold', y=0.99)

        if n_rows_fixed == 1 and n_cols == 1:
            axes = [[axes]]
        elif n_rows_fixed == 1:
            axes = [list(axes)]
        elif n_cols == 1:
            axes = [[ax] for ax in axes]
        else:
            axes = [list(row) for row in axes]

        for idx, date in enumerate(page_dates):
            r, c = divmod(idx, n_cols)
            ax = axes[r][c]
            row = date_basket_grid.loc[date]
            sorted_baskets = sorted(baskets, key=lambda b: float(row[b]) if pd.notna(row[b]) else float('-inf'))
            values = [float(row[b]) if pd.notna(row[b]) else 0.0 for b in sorted_baskets]
            sorted_labels = [_short_name(b) for b in sorted_baskets]
            bar_colors = ['#0066ff' if v >= 0 else '#ff3296' for v in values]

            ax.bar(range(len(sorted_baskets)), values, color=bar_colors, width=0.7, zorder=2)
            ax.axhline(0, color='#333333', linewidth=0.6, zorder=3)
            ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x * 100:.1f}%"))
            ax.yaxis.set_tick_params(labelsize=5)
            ax.set_xticks(range(len(sorted_baskets)))
            ax.set_xticklabels(sorted_labels, fontsize=x_fontsize, rotation=45, ha='right')
            ax.tick_params(axis='x', length=2, pad=1)
            ax.tick_params(axis='y', length=2, pad=1)
            date_label = pd.Timestamp(date).strftime('%a  %b %d, %Y')
            ax.set_title(date_label, fontsize=7, pad=2, fontweight='bold')
            ax.grid(axis='y', linewidth=0.3, alpha=0.4, zorder=0)
            ax.set_facecolor('white')
            for spine in ax.spines.values():
                spine.set_linewidth(0.4)
                spine.set_edgecolor('#aaaaaa')

        total_slots = n_rows_fixed * n_cols
        for idx in range(len(page_dates), total_slots):
            r, c = divmod(idx, n_cols)
            axes[r][c].set_visible(False)

        plt.tight_layout(rect=[0, 0, 1, 0.97])
        fig_list.append(fig)


def export_annual_returns_by_year(live_ctx=None):
    """Per-year bar charts: each subplot is one year, bars = annual return per basket."""
    summary_folder = BASE_OUTPUT_FOLDER / 'Baskets' / 'Basket_Reports' / 'annual_reports'
    summary_folder.mkdir(parents=True, exist_ok=True)

    latest_norgate = _get_latest_norgate_date()
    if latest_norgate is None and 'all_signals_df' in globals() and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        latest_norgate = pd.to_datetime(all_signals_df['Date']).max().normalize()
    if latest_norgate is None:
        print("Unable to determine latest Norgate date for annual-by-year export.")
        return

    report_asof_date = pd.Timestamp(live_ctx['today']).normalize() if live_ctx is not None else pd.Timestamp(latest_norgate).normalize()
    date_str = report_asof_date.strftime('%Y_%m_%d')
    out_path = summary_folder / f'{date_str}_annual_returns_by_year.pdf'

    _need_write, _need_mirror = _needs_write_and_mirror(out_path)
    if not _need_write and not _need_mirror:
        print(f"Annual-by-year report already exists for {date_str}, skipping: {out_path}")
        return
    if not _need_write and _need_mirror:
        WriteThroughPath(out_path).sync()
        return

    # basket_year_grid: index=baskets, columns=years — transpose to get years × baskets
    basket_year_grid = _build_basket_annual_grid(live_ctx=live_ctx)
    if basket_year_grid.empty:
        print("No annual return data generated.")
        return

    year_basket_grid = basket_year_grid.T  # index=years, columns=baskets

    _yr_min = min(-0.10, round(float(year_basket_grid.min().min()) * 1.05 - 0.01, 2))
    _yr_max = max( 0.10, round(float(year_basket_grid.max().max()) * 1.05 + 0.01, 2))
    _figs = []
    _render_year_basket_bar_charts(_figs, year_basket_grid, _yr_min, _yr_max, n_cols=1, n_rows_fixed=1, x_fontsize=7)
    build_pdf(_figs, out_path)
    print(f"Saved annual-by-year returns PDF: {out_path}")


def export_last_20_days_returns_by_day(live_ctx=None):
    """Per-day bar charts: each subplot is one trading day, bars = that day's return per basket (sorted)."""
    summary_folder = BASE_OUTPUT_FOLDER / 'Baskets' / 'Basket_Reports'
    summary_folder.mkdir(parents=True, exist_ok=True)
    _stamp = datetime.now().strftime('%Y_%m_%d_%H%M')
    out_path = summary_folder / f'{_stamp}_last_20_days_returns_by_day.pdf'

    basket_date_grid = _build_basket_daily_grid_last20(live_ctx=live_ctx)
    if basket_date_grid.empty:
        print("No daily return data generated.")
        return

    # basket_date_grid: index=baskets, columns=dates — transpose to get dates × baskets
    date_basket_grid = basket_date_grid.T  # index=dates, columns=baskets

    _d_min = min(-0.03, round(float(date_basket_grid.min().min()) * 1.05 - 0.005, 3))
    _d_max = max( 0.03, round(float(date_basket_grid.max().max()) * 1.05 + 0.005, 3))
    _figs = []
    _render_day_basket_bar_charts(_figs, date_basket_grid, _d_min, _d_max, n_cols=2, n_rows_fixed=2, x_fontsize=5)
    build_pdf(_figs, out_path)
    print(f"Saved 20-day by-day returns PDF: {out_path}")


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


# Run when desired:
# Force reset caches in case prior run cached a failure
_LIVE_GATE_CACHE = None
_LIVE_UPDATE_CONTEXT_CACHE = None
print(f"Databento config: API_KEY={'SET' if DATABENTO_API_KEY else 'MISSING'}, DATASET={DATABENTO_DATASET or 'MISSING'}")
_live_ctx_for_reports = _get_live_update_context()
export_today_signals(live_ctx=_live_ctx_for_reports)
export_today_etf_signals(live_ctx=_live_ctx_for_reports)
append_live_today_to_etf_signals_parquet()
_write_live_basket_ohlc(_live_ctx_for_reports)
# PDF Generation disabled per user request
# export_annual_returns(live_ctx=_live_ctx_for_reports)
# export_annual_returns_by_year(live_ctx=_live_ctx_for_reports)
# export_last_20_days_returns(live_ctx=_live_ctx_for_reports)
# export_last_20_days_returns_by_day(live_ctx=_live_ctx_for_reports)
# update_basket_parquets_with_live_ohlcv: disabled — live data is written to
# live_signals_500.parquet and live_basket_signals_500.parquet instead
# (the latter via _write_live_basket_ohlc above).

# %% [markdown]
## Holdings Exports (TradingView lists) [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Holdings Exports (TradingView lists)")


def export_group_holdings():
    current_qtr = _get_current_quarter_key()
    if not current_qtr:
        print("No quarter data available.")
        return

    theme_file    = HOLDINGS_FOLDER / f"Theme of Top {SIZE} {current_qtr}.txt"
    sector_file   = HOLDINGS_FOLDER / f"Sector of Top {SIZE} {current_qtr}.txt"
    industry_file = HOLDINGS_FOLDER / f"Industry of Top {SIZE} {current_qtr}.txt"

    # Thematic groups
    theme_lines = []
    thematic_groups = [
        ("High Beta",           BETA_UNIVERSE),
        ("Low Beta",            LOW_BETA_UNIVERSE),
        ("Momentum Leaders",    MOMENTUM_UNIVERSE),
        ("Momentum Losers",     MOMENTUM_LOSERS_UNIVERSE),
        ("High Dividend Yield", HIGH_YIELD_UNIVERSE),
        ("Dividend Growth",     DIV_GROWTH_UNIVERSE),
        ("Dividend with Growth", DIV_WITH_GROWTH_UNIVERSE),
        ("Risk Adj Momentum",   RISK_ADJ_MOM_UNIVERSE),
        ("Size",                SIZE_UNIVERSE),
        ("Volume Growth",       VOLUME_GROWTH_UNIVERSE),
    ]
    for name, universe in thematic_groups:
        tickers = sorted(universe.get(current_qtr, set()))
        theme_lines.append(f"###{name} of top {SIZE}")
        for t in tickers:
            theme_lines.append(t)
        theme_lines.append("")

    # Sector groups
    sector_lines = []
    for sector in SECTOR_LIST:
        universe = SECTOR_UNIVERSES.get(sector, {})
        tickers = sorted(universe.get(current_qtr, set()))
        sector_lines.append(f"###{sector} of top {SIZE}")
        for t in tickers:
            sector_lines.append(t)
        sector_lines.append("")

    # Industry groups
    industry_lines = []
    for industry in sorted(INDUSTRY_UNIVERSES.keys()):
        universe = INDUSTRY_UNIVERSES.get(industry, {})
        tickers = sorted(universe.get(current_qtr, set()))
        industry_lines.append(f"###{industry} of top {SIZE}")
        for t in tickers:
            industry_lines.append(t)
        industry_lines.append("")

    for path, lines in [
        (theme_file, theme_lines),
        (sector_file, sector_lines),
        (industry_file, industry_lines),
    ]:
        WriteThroughPath(path).write_text('\n'.join(lines))
        print(f"Saved: {path}")


def export_current_quarter_universe():
    current_qtr = _get_current_quarter_key()
    if not current_qtr:
        print("No quarter data available.")
        return

    tickers = sorted(QUARTER_UNIVERSE.get(current_qtr, set()))
    if not tickers:
        print(f"No universe tickers found for {current_qtr}.")
        return

    universe_file = HOLDINGS_FOLDER / f"Universe Top {SIZE} {current_qtr}.txt"
    WriteThroughPath(universe_file).write_text('\n'.join(tickers))
    print(f"Saved {len(tickers)} tickers to: {universe_file}")

