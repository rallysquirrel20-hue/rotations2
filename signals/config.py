"""Shared configuration, constants, paths, and utility functions.

This module is the single source of truth for all constants and cache paths
used by both ``rotations.py`` and ``build_universes.py``.

ZERO side effects at import -- no ``_install_timed_print()`` calls,
no ``reset_cell_timer()`` calls at module level.  Those are exported for
consumers to call explicitly.
"""

import os
import json
import hashlib
import shutil
import builtins
import time
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from contextlib import contextmanager

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Core constants
# ---------------------------------------------------------------------------
SIZE = 500
ETF_SIZE = 50
THEME_SIZE = 25
DIV_THEME_SIZE = 25
START_YEAR = 2000
LOOKBACK_DAYS = 252
MOMENTUM_LOOKBACK_DAYS = 252
INDUSTRY_MIN_STOCKS = 3
INDUSTRY_TOP_PCT = 0.25  # keep top 25% of industries by dollar volume each quarter
INCREMENTAL_MAX_DAYS = 5  # calendar days of staleness before full rebuild
MARKET_SYMBOL = 'SPY'

SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']
RV_MULT = np.sqrt(252) / np.sqrt(21)
EMA_MULT = 2.0 / 11.0   # For range EMAs
RV_EMA_ALPHA = 2.0 / 11.0  # span=10 EMA for RV

# ---------------------------------------------------------------------------
# Version stamps -- bump to invalidate caches
# ---------------------------------------------------------------------------
EQUITY_CACHE_SCHEMA_VERSION = 1
EQUITY_SIGNAL_LOGIC_VERSION = '2026-03-13-btfd-stfr-prev-trend'
EQUITY_UNIVERSE_LOGIC_VERSION = '2026-04-01-target-quarter-keying'
BASKET_SIGNALS_CACHE_SCHEMA_VERSION = 1
DIVIDEND_METRICS_SCHEMA_VERSION = 2  # bumped 2026-04-13: switched TTM/YoY from trading days to calendar days
DIVIDEND_TTM_WINDOW = 365  # trailing CALENDAR days for TTM dividend sum (time-based rolling)
DIVIDEND_YOY_LAG = 365     # CALENDAR-day lag for YoY growth comparison (asof-based)
CHART_SCHEMA_VERSION = 2  # Bump to force rebuild of basket chart PNGs (added 21d corr panel)
BENCHMARK_BASKETS = 0      # If > 0, only process this many baskets then stop
BENCHMARK_TIMING = True    # If True, print per-step timing breakdown for each basket

# ---------------------------------------------------------------------------
# Sector list
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Force flags -- env-var-driven so notebooks / CI can override
# ---------------------------------------------------------------------------
FORCE_REBUILD_EQUITY_CACHE = os.getenv('FORCE_REBUILD_EQUITY_CACHE', '').lower() in ('1', 'true', 'yes')
FORCE_REBUILD_BASKET_SIGNALS = os.getenv('FORCE_REBUILD_BASKET_SIGNALS', '').lower() in ('1', 'true', 'yes')

# ---------------------------------------------------------------------------
# Output folder resolution
# ---------------------------------------------------------------------------
BASE_OUTPUT_FOLDER = Path(
    os.getenv('PYTHON_OUTPUTS_DIR', Path.home() / 'Documents' / 'Python_Outputs')
).expanduser()
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


# ---------------------------------------------------------------------------
# OutputPaths dataclass
# ---------------------------------------------------------------------------
@dataclass
class OutputPaths:
    """Central registry for all output folder paths.

    Instantiate once from BASE_OUTPUT_FOLDER.  All sub-folders are derived
    automatically and created on first access via ``_mkdirs()``.
    """

    base: Path

    def __post_init__(self):
        self.data                  = self.base / os.getenv('DATA_STORAGE_NAME', 'Data_Storage')
        self.corr_cache            = self.data / 'correlation_cache'
        self.thematic_basket_cache = self.data / 'thematic_basket_cache'
        self.sector_basket_cache   = self.data / 'sector_basket_cache'
        self.industry_basket_cache = self.data / 'industry_basket_cache'
        self.baskets               = self.base / 'Baskets'
        self.thematic_charts       = self.baskets / 'Thematic_Baskets'
        self.sector_charts         = self.baskets / 'Sector_Baskets'
        self.industry_charts       = self.baskets / 'Industry_Baskets'
        self.holdings              = self.base / 'Trading_View_Lists'
        self.previous_day          = self.base / 'Previous_Day_Rotations'
        self.correlations          = self.base / 'Correlations'
        self.live                  = self.base / 'Live_Rotations'
        self.summary               = self.base / 'Summary'
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

# ---------------------------------------------------------------------------
# Folder-constant aliases -- all definitions owned by OutputPaths above
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Cache file paths
# ---------------------------------------------------------------------------
CACHE_FILE               = DATA_FOLDER / f'top{SIZE}stocks.json'
ETF_CACHE_FILE           = DATA_FOLDER / 'etf_universes_50.json'
TICKER_NAMES_FILE        = DATA_FOLDER / 'ticker_names.json'
BETA_CACHE_FILE          = paths.thematic_basket_cache / f'beta_universes_{SIZE}.json'
MOMENTUM_CACHE_FILE      = paths.thematic_basket_cache / f'momentum_universes_{SIZE}.json'
RISK_ADJ_MOM_CACHE_FILE  = paths.thematic_basket_cache / f'risk_adj_momentum_{SIZE}.json'
DIVIDEND_CACHE_FILE      = paths.thematic_basket_cache / f'dividend_universes_{SIZE}.json'
SIZE_CACHE_FILE          = paths.thematic_basket_cache / f'size_universes_{SIZE}.json'
VOLUME_CACHE_FILE        = paths.thematic_basket_cache / f'volume_universes_{SIZE}.json'
GICS_CACHE_FILE          = DATA_FOLDER / f'gics_mappings_{SIZE}.json'
SIGNALS_CACHE_FILE       = DATA_FOLDER / f'signals_{SIZE}.parquet'
ETF_SIGNALS_CACHE_FILE   = DATA_FOLDER / 'signals_etf_50.parquet'
DIVIDEND_METRICS_CACHE_FILE     = DATA_FOLDER / f'dividend_metrics_{SIZE}.parquet'
DIVIDEND_METRICS_ETF_CACHE_FILE = DATA_FOLDER / 'dividend_metrics_etf_50.parquet'


# ---------------------------------------------------------------------------
# WriteThroughPath -- automatic OneDrive mirroring on every write
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
# build_pdf -- centralised PDF generation helper
# ---------------------------------------------------------------------------
def build_pdf(figures: list, path) -> None:
    """Save a list of matplotlib Figures to a single PDF, then close each."""
    from matplotlib.backends.backend_pdf import PdfPages
    import matplotlib.pyplot as plt

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


# ---------------------------------------------------------------------------
# Quarter key helpers
# ---------------------------------------------------------------------------
def _quarter_end_from_key(key: str) -> pd.Timestamp:
    """Return the last calendar day of the quarter denoted by *key* ('YYYY QN')."""
    year_str, q_str = key.split()
    year = int(year_str)
    quarter = int(q_str.replace("Q", ""))
    return pd.Period(f"{year}Q{quarter}").end_time.normalize()


def _quarter_start_from_key(key: str) -> pd.Timestamp:
    """Return the first calendar day of the quarter denoted by *key* ('YYYY QN')."""
    year_str, q_str = key.split()
    year = int(year_str)
    quarter = int(q_str.replace("Q", ""))
    return pd.Period(f"{year}Q{quarter}").start_time.normalize()


def get_current_quarter_key() -> str:
    """Return the key for today's calendar quarter, e.g. '2026 Q2'."""
    today = datetime.today()
    return f"{today.year} Q{(today.month - 1) // 3 + 1}"


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------
def _universe_to_json(universe: dict) -> str:
    """Serialize universe dict (str -> set) to JSON (str -> sorted list)."""
    return json.dumps({k: sorted(v) for k, v in universe.items()})


def _json_to_universe(text: str) -> dict:
    """Deserialize universe dict from JSON (str -> list -> set)."""
    return {k: set(v) for k, v in json.loads(text).items()}


def _beta_universes_to_json(result: tuple) -> str:
    """Serialize (high_beta, low_beta) tuple to JSON."""
    high, low = result
    return json.dumps({'high': {k: sorted(v) for k, v in high.items()},
                       'low':  {k: sorted(v) for k, v in low.items()}})


def _json_to_beta_universes(text: str) -> tuple:
    """Deserialize (high_beta, low_beta) tuple from JSON."""
    d = json.loads(text)
    return ({k: set(v) for k, v in d['high'].items()},
            {k: set(v) for k, v in d['low'].items()})


def _gics_to_json(result: tuple) -> str:
    """Serialize GICS tuple (ticker_sector, ticker_subindustry, sector_u, industry_u) to JSON."""
    ticker_sector, ticker_subindustry, sector_u, industry_u = result
    return json.dumps({
        'ticker_sector': ticker_sector,
        'ticker_subindustry': ticker_subindustry,
        'sector_u': {s: {k: sorted(v) for k, v in qmap.items()} for s, qmap in sector_u.items()},
        'industry_u': {g: {k: sorted(v) for k, v in qmap.items()} for g, qmap in industry_u.items()},
    })


def _json_to_gics(text: str) -> tuple:
    """Deserialize GICS tuple from JSON."""
    d = json.loads(text)
    sector_u = {s: {k: set(v) for k, v in qmap.items()} for s, qmap in d['sector_u'].items()}
    industry_u = {g: {k: set(v) for k, v in qmap.items()} for g, qmap in d['industry_u'].items()}
    return d['ticker_sector'], d['ticker_subindustry'], sector_u, industry_u


# ---------------------------------------------------------------------------
# New helpers -- atomic I/O and disk loaders
# ---------------------------------------------------------------------------
def atomic_write_parquet(df: pd.DataFrame, path: Path, **kwargs) -> None:
    """Write a DataFrame to *path* atomically (tmp + rename) then mirror to OneDrive.

    Accepts any extra keyword arguments supported by ``DataFrame.to_parquet``
    (e.g. ``compression='snappy'``, ``use_dictionary=False``).
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix('.parquet.tmp')
    kwargs.setdefault('index', False)
    kwargs.setdefault('compression', 'snappy')
    df.to_parquet(tmp_path, **kwargs)
    tmp_path.replace(path)
    WriteThroughPath(path).sync()


def atomic_write_json(path: Path, data: str) -> None:
    """Write JSON string to *path* atomically via WriteThroughPath."""
    WriteThroughPath(path).write_text(data)


def load_universe_from_disk() -> dict | None:
    """Load the equity universe from *CACHE_FILE* without triggering Norgate.

    Returns ``{quarter_key: set_of_tickers}`` or ``None`` if the file is
    missing or corrupt.
    """
    if not CACHE_FILE.exists():
        return None
    try:
        return _json_to_universe(CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return None


def load_etf_universe_from_disk() -> dict | None:
    """Load the ETF universe from *ETF_CACHE_FILE* without triggering Norgate.

    Returns ``{quarter_key: set_of_tickers}`` or ``None`` if the file is
    missing or corrupt.
    """
    if not ETF_CACHE_FILE.exists():
        return None
    try:
        return _json_to_universe(ETF_CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return None


def load_gics_from_disk() -> tuple | None:
    """Load GICS mappings from *GICS_CACHE_FILE* without triggering Norgate.

    Returns ``(ticker_sector, ticker_subindustry, sector_universes,
    industry_universes)`` or ``None`` if the file is missing or corrupt.
    """
    if not GICS_CACHE_FILE.exists():
        return None
    try:
        return _json_to_gics(GICS_CACHE_FILE.read_text(encoding='utf-8'))
    except Exception:
        return None


def load_thematic_universe_from_disk(cache_file: Path, subkey=None) -> dict | None:
    """Load a thematic universe from JSON cache.

    If *subkey* is provided (e.g. ``'high'``, ``'winners'``), extracts that
    sub-dict first.  Returns ``{quarter_key: set_of_tickers}`` or ``None``.
    """
    if not cache_file.exists():
        return None
    try:
        raw = json.loads(cache_file.read_text(encoding='utf-8'))
        if subkey is not None:
            raw = raw[subkey]
        return {k: set(v) for k, v in raw.items()}
    except Exception:
        return None
