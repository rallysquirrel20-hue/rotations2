"""foundation.py — shared constants, utility classes, serialisation helpers,
signal computation engine, and basket cache helpers.

This module is imported by universe.py, tickersignals.py, basketsignals.py,
and livesignals.py.  It must NEVER be run directly (no top-level side-effects
that hit Norgate or build caches).
"""

# ---------------------------------------------------------------------------
# Section 1: Imports
# ---------------------------------------------------------------------------

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
from bisect import bisect_right
from collections import deque
import bisect

# ---------------------------------------------------------------------------
# Section 2: Constants
# ---------------------------------------------------------------------------

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
EQUITY_UNIVERSE_LOGIC_VERSION = '2026-02-10-codex-1'
FORCE_REBUILD_EQUITY_CACHE = False
BASKET_SIGNALS_CACHE_SCHEMA_VERSION = 1
FORCE_REBUILD_BASKET_SIGNALS = False
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

# Constants from later sections of rotations.py
ETF_SIZE = 50
THEME_SIZE = 25
DIV_THEME_SIZE = 25
MARKET_SYMBOL = 'SPY'
LOOKBACK_DAYS = 252
MOMENTUM_LOOKBACK_DAYS = 252

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

SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']
RV_MULT = np.sqrt(252) / np.sqrt(21)
EMA_MULT = 2.0 / 11.0  # For range EMAs
RV_EMA_ALPHA = 2.0 / 11.0  # span=10 EMA for RV

INCREMENTAL_MAX_DAYS = 5  # calendar days of staleness before full rebuild

# ---------------------------------------------------------------------------
# Section 3: OutputPaths + Folder Aliases
# ---------------------------------------------------------------------------


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

# Universe cache file paths
ETF_CACHE_FILE = DATA_FOLDER / 'etf_universes_50.json'
BETA_CACHE_FILE = paths.thematic_basket_cache / f'beta_universes_{SIZE}.json'
MOMENTUM_CACHE_FILE = paths.thematic_basket_cache / f'momentum_universes_{SIZE}.json'
RISK_ADJ_MOM_CACHE_FILE = paths.thematic_basket_cache / f'risk_adj_momentum_{SIZE}.json'
DIVIDEND_CACHE_FILE = paths.thematic_basket_cache / f'dividend_universes_{SIZE}.json'
SIZE_CACHE_FILE = paths.thematic_basket_cache / f'size_universes_{SIZE}.json'
VOLUME_GROWTH_CACHE_FILE = paths.thematic_basket_cache / f'volume_growth_universes_{SIZE}.json'
GICS_CACHE_FILE = DATA_FOLDER / f'gics_mappings_{SIZE}.json'
TICKER_NAMES_FILE = DATA_FOLDER / 'ticker_names.json'

# Signal cache file paths
SIGNALS_CACHE_FILE = DATA_FOLDER / f'signals_{SIZE}.parquet'
ETF_SIGNALS_CACHE_FILE = DATA_FOLDER / 'signals_etf_50.parquet'

# ---------------------------------------------------------------------------
# Section 4: Utility Classes
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


def get_current_quarter_key(quarter_universe):
    """Get the most recent quarter key from the given universe dict."""
    keys = sorted(quarter_universe.keys())
    return keys[-1] if keys else None


# ---------------------------------------------------------------------------
# Section 5: Universe JSON serialisation helpers
# ---------------------------------------------------------------------------


def _universe_to_json(universe: dict) -> str:
    """Serialize universe dict (str -> set) to JSON (str -> sorted list)."""
    return json.dumps({k: sorted(v) for k, v in universe.items()})


def _json_to_universe(text: str) -> dict:
    """Deserialize universe dict from JSON (str -> list -> set)."""
    return {k: set(v) for k, v in json.loads(text).items()}


def is_universe_current(universe):
    today = datetime.today()
    current_key = f"{today.year} Q{(today.month - 1) // 3 + 1}"
    return current_key in universe


def _beta_universes_to_json(result: tuple) -> str:
    high, low = result
    return json.dumps({'high': {k: sorted(v) for k, v in high.items()},
                       'low':  {k: sorted(v) for k, v in low.items()}})


def _json_to_beta_universes(text: str) -> tuple:
    d = json.loads(text)
    return ({k: set(v) for k, v in d['high'].items()},
            {k: set(v) for k, v in d['low'].items()})


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


def get_universe(date, quarter_universe):
    """Look up the active universe for a given date, using prior quarter to avoid look-ahead bias."""
    year = date.year
    quarter = (date.month - 1) // 3 + 1
    # Use previous quarter's ranking to avoid look-ahead bias
    if quarter == 1:
        return quarter_universe.get(f"{year - 1} Q4", set())
    return quarter_universe.get(f"{year} Q{quarter - 1}", set())


# ---------------------------------------------------------------------------
# Section 6: Universe Cache Loaders
# ---------------------------------------------------------------------------


def load_universe_from_cache():
    """Load QUARTER_UNIVERSE from JSON cache."""
    if not CACHE_FILE.exists():
        raise FileNotFoundError(f"Universe cache not found: {CACHE_FILE}. Run universe.py first.")
    return _json_to_universe(CACHE_FILE.read_text(encoding='utf-8'))


def load_etf_universe_from_cache():
    """Load ETF_UNIVERSE from JSON cache."""
    if not ETF_CACHE_FILE.exists():
        raise FileNotFoundError(f"ETF universe cache not found: {ETF_CACHE_FILE}. Run universe.py first.")
    return _json_to_universe(ETF_CACHE_FILE.read_text(encoding='utf-8'))


def load_beta_universes_from_cache():
    """Load (HIGH_BETA, LOW_BETA) universe tuple from JSON cache."""
    if not BETA_CACHE_FILE.exists():
        raise FileNotFoundError(f"Beta universe cache not found: {BETA_CACHE_FILE}. Run universe.py first.")
    return _json_to_beta_universes(BETA_CACHE_FILE.read_text(encoding='utf-8'))


def load_momentum_universes_from_cache():
    """Load (MOMENTUM_WINNERS, MOMENTUM_LOSERS) universe tuple from JSON cache."""
    if not MOMENTUM_CACHE_FILE.exists():
        raise FileNotFoundError(f"Momentum universe cache not found: {MOMENTUM_CACHE_FILE}. Run universe.py first.")
    d = json.loads(MOMENTUM_CACHE_FILE.read_text(encoding='utf-8'))
    return ({k: set(v) for k, v in d['winners'].items()},
            {k: set(v) for k, v in d['losers'].items()})


def load_risk_adj_momentum_from_cache():
    """Load risk-adjusted momentum universe from JSON cache."""
    if not RISK_ADJ_MOM_CACHE_FILE.exists():
        raise FileNotFoundError(f"Risk-adj momentum cache not found: {RISK_ADJ_MOM_CACHE_FILE}. Run universe.py first.")
    return {k: set(v) for k, v in
            json.loads(RISK_ADJ_MOM_CACHE_FILE.read_text(encoding='utf-8')).items()}


def load_dividend_universes_from_cache():
    """Load (HIGH_YIELD, DIV_GROWTH, DIV_WITH_GROWTH) universe tuple from JSON cache."""
    if not DIVIDEND_CACHE_FILE.exists():
        raise FileNotFoundError(f"Dividend universe cache not found: {DIVIDEND_CACHE_FILE}. Run universe.py first.")
    d = json.loads(DIVIDEND_CACHE_FILE.read_text(encoding='utf-8'))
    return ({k: set(v) for k, v in d['high_yield'].items()},
            {k: set(v) for k, v in d['div_growth'].items()},
            {k: set(v) for k, v in d.get('div_with_growth', {}).items()})


def load_size_universes_from_cache():
    """Load size (dollar volume) universe from JSON cache."""
    if not SIZE_CACHE_FILE.exists():
        raise FileNotFoundError(f"Size universe cache not found: {SIZE_CACHE_FILE}. Run universe.py first.")
    return {k: set(v) for k, v in
            json.loads(SIZE_CACHE_FILE.read_text(encoding='utf-8')).items()}


def load_volume_growth_universes_from_cache():
    """Load volume growth universe from JSON cache."""
    if not VOLUME_GROWTH_CACHE_FILE.exists():
        raise FileNotFoundError(f"Volume growth cache not found: {VOLUME_GROWTH_CACHE_FILE}. Run universe.py first.")
    return {k: set(v) for k, v in
            json.loads(VOLUME_GROWTH_CACHE_FILE.read_text(encoding='utf-8')).items()}


def load_gics_mappings_from_cache():
    """Load GICS mappings (ticker_sector, ticker_subindustry, sector_universes, industry_universes) from JSON cache."""
    if not GICS_CACHE_FILE.exists():
        raise FileNotFoundError(f"GICS cache not found: {GICS_CACHE_FILE}. Run universe.py first.")
    return _json_to_gics(GICS_CACHE_FILE.read_text(encoding='utf-8'))


def load_ticker_names_from_cache():
    """Load ticker -> security name mapping from JSON cache."""
    if not TICKER_NAMES_FILE.exists():
        raise FileNotFoundError(f"Ticker names cache not found: {TICKER_NAMES_FILE}. Run universe.py first.")
    return json.loads(TICKER_NAMES_FILE.read_text(encoding='utf-8'))


def load_all_universes():
    """Load all universe dicts from cache. Returns dict of name -> universe data."""
    quarter_universe = load_universe_from_cache()
    etf_universe = load_etf_universe_from_cache()
    high_beta, low_beta = load_beta_universes_from_cache()
    momentum_winners, momentum_losers = load_momentum_universes_from_cache()
    risk_adj_momentum = load_risk_adj_momentum_from_cache()
    high_yield, div_growth, div_with_growth = load_dividend_universes_from_cache()
    size_universe = load_size_universes_from_cache()
    volume_growth = load_volume_growth_universes_from_cache()
    ticker_sector, ticker_subindustry, sector_universes, industry_universes = load_gics_mappings_from_cache()
    ticker_names = load_ticker_names_from_cache()

    return {
        'QUARTER_UNIVERSE': quarter_universe,
        'ETF_UNIVERSE': etf_universe,
        'BETA_UNIVERSE': high_beta,
        'LOW_BETA_UNIVERSE': low_beta,
        'MOMENTUM_UNIVERSE': momentum_winners,
        'MOMENTUM_LOSERS_UNIVERSE': momentum_losers,
        'RISK_ADJ_MOM_UNIVERSE': risk_adj_momentum,
        'HIGH_YIELD_UNIVERSE': high_yield,
        'DIV_GROWTH_UNIVERSE': div_growth,
        'DIV_WITH_GROWTH_UNIVERSE': div_with_growth,
        'SIZE_UNIVERSE': size_universe,
        'VOLUME_GROWTH_UNIVERSE': volume_growth,
        'TICKER_SECTOR': ticker_sector,
        'TICKER_SUBINDUSTRY': ticker_subindustry,
        'SECTOR_UNIVERSES': sector_universes,
        'INDUSTRY_UNIVERSES': industry_universes,
        'TICKER_NAMES': ticker_names,
    }


def build_all_basket_specs(universes):
    """Return list of (name, universe_dict, charts_folder, basket_type) tuples."""
    all_baskets = [
        ('High Beta',           universes['BETA_UNIVERSE'],           THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Low Beta',            universes['LOW_BETA_UNIVERSE'],        THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Momentum Leaders',    universes['MOMENTUM_UNIVERSE'],        THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Momentum Losers',     universes['MOMENTUM_LOSERS_UNIVERSE'], THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('High Dividend Yield', universes['HIGH_YIELD_UNIVERSE'],      THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Dividend Growth',     universes['DIV_GROWTH_UNIVERSE'],      THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Dividend with Growth', universes['DIV_WITH_GROWTH_UNIVERSE'], THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Risk Adj Momentum',   universes['RISK_ADJ_MOM_UNIVERSE'],    THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Size',                universes['SIZE_UNIVERSE'],            THEMATIC_CHARTS_FOLDER, 'thematic'),
        ('Volume Growth',       universes['VOLUME_GROWTH_UNIVERSE'],   THEMATIC_CHARTS_FOLDER, 'thematic'),
    ]
    all_baskets += [(s, u, SECTOR_CHARTS_FOLDER, 'sector') for s, u in universes['SECTOR_UNIVERSES'].items()]
    all_baskets += [(ind, u, INDUSTRY_CHARTS_FOLDER, 'industry') for ind, u in universes['INDUSTRY_UNIVERSES'].items()]
    return all_baskets


# ---------------------------------------------------------------------------
# Section 7: Signal Computation Engine
# ---------------------------------------------------------------------------

# Global timing tracker for passes
pass_times = {'data_load': 0, 'pass1': 0, 'pass2': 0, 'pass3': 0, 'pass4': 0, 'pass5': 0}
pass_counts = {'tickers': 0}


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


# ---------------------------------------------------------------------------
# Section 8: Basket Cache Helpers
# ---------------------------------------------------------------------------

_DATA_SIGNATURE_CACHE = None


def _cache_slugify_label(label):
    return str(label).replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')


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


def reset_data_signature_cache():
    """Reset the data signature cache so it will be recomputed on next call."""
    global _DATA_SIGNATURE_CACHE
    _DATA_SIGNATURE_CACHE = None


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
        with builtins.open(meta_path, 'r', encoding='utf-8') as f:
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
    with builtins.open(meta_path, 'w', encoding='utf-8') as f:
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
        current_key = get_current_quarter_key(universe_by_qtr)
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


# ---------------------------------------------------------------------------
# NOTE: The following functions from rotations.py reference `all_signals_df`
# as a module-level global and therefore CANNOT live in foundation.py:
#   - process_basket_signals()
#   - compute_equity_ohlc_cached() [the version that calls all_signals_df
#     is actually the process_basket_signals wrapper; the standalone version
#     takes all_df as a parameter and IS included above]
#   - _prebuild_equity_cache_from_signals()
#   - _compute_within_basket_correlation() (at ~line 4784)
#   - _compute_within_basket_correlation_incremental() (at ~line 4948)
#   - build_signals_for_ticker_live()
#   - _get_latest_norgate_rows_by_ticker()
# These must stay in the consuming files (basketsignals.py, livesignals.py).
# ---------------------------------------------------------------------------
