# rotations_old_outputs.py — Extracted from rotations.py (Cells 9-15)
# Group B — Report Only: Signal filtering, Excel/CSV exports, correlation cache,
# charts, and PDF reports.
#
# These cells depend on all globals, constants, universes, utility functions,
# and data objects built by Cells 0-8 of rotations.py.

# --- Standard library ---
import gc
import hashlib
import json
import re
import textwrap
from datetime import datetime
from pathlib import Path

# --- Third-party ---
import matplotlib.dates as mdates
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import openpyxl  # noqa: F401
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap
from pandas.errors import EmptyDataError  # noqa: F401

# --- rotations.py: public names (constants, universes, data, utilities) ---
from rotations import *  # noqa: F403

# --- rotations.py: private helpers that cells 9-15 reference ---
from rotations import (  # noqa: F811
    _append_trade_rows,
    _build_basket_annual_grid,
    _build_basket_daily_grid_last20,
    _build_group_daily_return_grid,
    _build_universe_signature,
    _compute_live_basket_return,
    _fmt_bars,
    _fmt_pct,
    _fmt_price,
    _get_all_basket_specs_for_reports,
    _get_chart_schema_version_from_parquet,
    _get_latest_norgate_date,
    _get_latest_norgate_date_fallback,
    _get_live_update_context,
    _live_ctx_for_reports,
    _needs_write_and_mirror,
    _render_return_bar_charts,
    _SIGNAL_ORDER,
    _SIGNAL_RANK,
    _sort_signals_df,
)


# %% [markdown]
## Signal Universe Filtering [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Signal Universe Filtering")

all_signals_df['Date'] = pd.to_datetime(all_signals_df['Date'])

# Filter to universe-eligible signals only (vectorized by quarter)
dates = all_signals_df['Date']
quarters = ((dates.dt.month - 1) // 3 + 1).astype(int)
years = dates.dt.year.astype(int)
prev_quarters = (quarters - 2) % 4 + 1
prev_years = years.where(quarters > 1, years - 1)
prev_keys = prev_years.astype(str) + ' Q' + prev_quarters.astype(str)

in_universe = pd.Series(False, index=all_signals_df.index)
for key, idx in prev_keys.groupby(prev_keys).groups.items():
    universe = QUARTER_UNIVERSE.get(key)
    if not universe:
        continue
    in_universe.loc[idx] = all_signals_df.loc[idx, 'Ticker'].isin(universe)

universe_df = all_signals_df[in_universe].copy()

signal_flags = {
    'Up_Rot': 'Is_Up_Rotation',
    'Down_Rot': 'Is_Down_Rotation',
    'Breakout': 'Is_Breakout',
    'Breakdown': 'Is_Breakdown',
    'BTFD': 'Is_BTFD',
    'STFR': 'Is_STFR',
}




# %% [markdown]
## Daily Signal Exports (Excel) [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Daily Signal Exports")

# Use Norgate's latest date for filename
latest_norgate = _get_latest_norgate_date()
date_str = latest_norgate.strftime('%Y_%m_%d')

# Export signals for the last date only, combined into one CSV
last_date = universe_df['Date'].max()
last_day_df = universe_df[universe_df['Date'] == last_date]

# Build theme lookup for the current quarter
cur_q = (last_date.month - 1) // 3 + 1
cur_key = f"{last_date.year} Q{cur_q}"
_thematic_universes = [
    ('High Beta',           BETA_UNIVERSE.get(cur_key, set())),
    ('Low Beta',            LOW_BETA_UNIVERSE.get(cur_key, set())),
    ('Momentum Leaders',    MOMENTUM_UNIVERSE.get(cur_key, set())),
    ('Momentum Losers',     MOMENTUM_LOSERS_UNIVERSE.get(cur_key, set())),
    ('High Dividend Yield', HIGH_YIELD_UNIVERSE.get(cur_key, set())),
    ('Dividend Growth',     DIV_GROWTH_UNIVERSE.get(cur_key, set())),
    ('Risk Adj Momentum',   RISK_ADJ_MOM_UNIVERSE.get(cur_key, set())),
]
def _get_ticker_theme(ticker):
    labels = [name for name, u in _thematic_universes if ticker in u]
    return ', '.join(labels)

common_cols = ['Date', 'Ticker', 'Close']
stat_suffixes = [
    'Entry_Price',
    'Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
    'Avg_MFE', 'Avg_MAE',
    'Std_Dev', 'Historical_EV', 'EV_Last_3',
    'Risk_Adj_EV', 'Risk_Adj_EV_Last_3', 'Count',
]
pct_suffixes = [
    'Win_Rate', 'Avg_Winner', 'Avg_Loser',
    'Avg_MFE', 'Avg_MAE', 'Std_Dev',
    'Historical_EV', 'EV_Last_3',
    'Risk_Adj_EV', 'Risk_Adj_EV_Last_3',
]

combined_parts = []
for sig_name, flag_col in signal_flags.items():
    sig_df = last_day_df[last_day_df[flag_col] == True].copy()
    if sig_df.empty:
        continue

    prefixed_cols = [f'{sig_name}_{s}' for s in stat_suffixes]
    # Some stat columns may be absent depending on available signal history; keep schema stable.
    sig_df = sig_df.reindex(columns=common_cols + prefixed_cols).copy()

    rename_map = {f'{sig_name}_{s}': s for s in stat_suffixes}
    sig_df.rename(columns=rename_map, inplace=True)

    sig_df.insert(3, 'Signal_Type', sig_name)
    sig_df.insert(4, 'Theme',    sig_df['Ticker'].map(_get_ticker_theme))
    sig_df.insert(5, 'Sector',   sig_df['Ticker'].map(lambda t: TICKER_SECTOR.get(t, '')))
    sig_df.insert(6, 'Industry', sig_df['Ticker'].map(lambda t: TICKER_SUBINDUSTRY.get(t, '')))

    for s in pct_suffixes:
        if s in sig_df.columns:
            sig_df[s] = sig_df[s].apply(
                lambda x: f"{x * 100:.2f}%" if pd.notna(x) else ""
            )
    for col in ['Close', 'Entry_Price']:
        if col in sig_df.columns:
            sig_df[col] = sig_df[col].apply(_fmt_price)
    for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
        if col in sig_df.columns:
            sig_df[col] = sig_df[col].apply(_fmt_bars)

    combined_parts.append(sig_df)

if combined_parts:
    combined_df = pd.concat(combined_parts, ignore_index=True)
    combined_df = _sort_signals_df(combined_df)
    print(f"Prepared {len(combined_df)} signals for {last_date.strftime('%Y-%m-%d')}:")
    for sig_name in signal_flags:
        count = len(combined_df[combined_df['Signal_Type'] == sig_name])
        if count > 0:
            print(f"  {sig_name}: {count}")
else:
    combined_df = pd.DataFrame()
    print(f"No signals found for {last_date.strftime('%Y-%m-%d')}")

# --- Basket signals CSV ---
_thematic_names = {'High Beta', 'Low Beta', 'Momentum Leaders', 'Momentum Losers',
                   'High Dividend Yield', 'Dividend Growth', 'Risk Adj Momentum'}
_sector_names   = set(SECTOR_UNIVERSES.keys())

def _basket_tsi(name):
    if name in _thematic_names:
        return name, '', ''
    elif name in _sector_names:
        return '', name, ''
    else:
        return '', '', name

basket_parts = []
for basket_name, (merged_all, slug, hist_folder, universe_by_qtr) in BASKET_RESULTS.items():
    basket_last_date = merged_all['Date'].max()
    basket_last_df = merged_all[merged_all['Date'] == basket_last_date]
    theme, sector, industry = _basket_tsi(basket_name)

    for sig_name, flag_col in signal_flags.items():
        if flag_col not in basket_last_df.columns:
            continue
        sig_df = basket_last_df[basket_last_df[flag_col] == True]
        if sig_df.empty:
            continue

        signal_row = sig_df.iloc[0]
        out_row = {
            'Date':        signal_row['Date'],
            'Basket':      basket_name,
            'Close':       signal_row.get('Close', np.nan),
            'Signal_Type': sig_name,
            'Theme':       theme,
            'Sector':      sector,
            'Industry':    industry,
        }
        for s in stat_suffixes:
            out_row[s] = signal_row.get(f'{sig_name}_{s}', np.nan)
        basket_parts.append(out_row)

if basket_parts:
    basket_df = pd.DataFrame(basket_parts)
    for s in pct_suffixes:
        if s in basket_df.columns:
            basket_df[s] = basket_df[s].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "")
    for col in ['Close', 'Entry_Price']:
        if col in basket_df.columns:
            basket_df[col] = basket_df[col].apply(_fmt_price)
    for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
        if col in basket_df.columns:
            basket_df[col] = basket_df[col].apply(_fmt_bars)
    basket_df = _sort_signals_df(basket_df)
    print(f"Prepared {len(basket_df)} basket signals for {basket_last_date.strftime('%Y-%m-%d')}:")
    for sig_name in signal_flags:
        count = len(basket_df[basket_df['Signal_Type'] == sig_name])
        if count > 0:
            print(f"  {sig_name}: {count}")
else:
    basket_df = pd.DataFrame()
    print("No basket signals found for last date")


# Export open signal for every stock in the universe
print("Exporting open signals for all universe stocks...")

# 1. Get tickers in the current quarter's universe only
current_universe = QUARTER_UNIVERSE.get(cur_key, set())
all_universe_tickers = sorted(t for t in universe_df['Ticker'].unique() if t in current_universe)

# 2. Get latest close per ticker
latest_rows_per_ticker = universe_df.loc[universe_df.groupby('Ticker')['Date'].idxmax()].set_index('Ticker')

# 3. Pre-compute last entry row per ticker per signal type
_SIGNAL_PAIRS = [('Up_Rot', 'Down_Rot'), ('Breakout', 'Breakdown'), ('BTFD', 'STFR')]

last_entry_by_sig = {}
for sig_name, flag_col in signal_flags.items():
    sig_df = universe_df[universe_df[flag_col] == True]
    if not sig_df.empty:
        last_entry_by_sig[sig_name] = sig_df.loc[sig_df.groupby('Ticker')['Date'].idxmax()].set_index('Ticker')

# 4. For each pair, keep only the open signal (the one that fired last)
all_open_signals = []
for sig_a, sig_b in _SIGNAL_PAIRS:
    for tkr in all_universe_tickers:
        if tkr not in latest_rows_per_ticker.index:
            continue
        date_a = pd.to_datetime(last_entry_by_sig[sig_a].loc[tkr]['Date']) if sig_a in last_entry_by_sig and tkr in last_entry_by_sig[sig_a].index else None
        date_b = pd.to_datetime(last_entry_by_sig[sig_b].loc[tkr]['Date']) if sig_b in last_entry_by_sig and tkr in last_entry_by_sig[sig_b].index else None

        if date_a is None and date_b is None:
            continue
        open_sig = sig_a if (date_b is None or (date_a is not None and date_a >= date_b)) else sig_b

        signal_row = last_entry_by_sig[open_sig].loc[tkr]
        entry_price = signal_row.get(f'{open_sig}_Entry_Price', np.nan)
        if pd.isna(entry_price) or float(entry_price) <= 0:
            continue
        latest_close = float(latest_rows_per_ticker.loc[tkr]['Close'])
        if open_sig in ('Down_Rot', 'Breakdown', 'STFR'):
            current_perf = (float(entry_price) - latest_close) / float(entry_price)
        else:
            current_perf = (latest_close - float(entry_price)) / float(entry_price)

        out_row = {
            'Date': signal_row['Date'],
            'Ticker': tkr,
            'Close': latest_close,
            'Signal_Type': open_sig,
            'Current_Performance': current_perf,
        }
        for s in stat_suffixes:
            out_row[s] = signal_row.get(f'{open_sig}_{s}', np.nan)
        all_open_signals.append(out_row)

if all_open_signals:
    open_signals_df = pd.DataFrame(all_open_signals)
    open_signals_df.insert(4, 'Theme',    open_signals_df['Ticker'].map(_get_ticker_theme))
    open_signals_df.insert(5, 'Sector',   open_signals_df['Ticker'].map(lambda t: TICKER_SECTOR.get(t, '')))
    open_signals_df.insert(6, 'Industry', open_signals_df['Ticker'].map(lambda t: TICKER_SUBINDUSTRY.get(t, '')))

    pct_suffixes_plus_perf = pct_suffixes + ['Current_Performance']
    for s in pct_suffixes_plus_perf:
        if s in open_signals_df.columns:
            open_signals_df[s] = open_signals_df[s].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "")
    for col in ['Close', 'Entry_Price']:
        if col in open_signals_df.columns:
            open_signals_df[col] = open_signals_df[col].apply(_fmt_price)
    for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
        if col in open_signals_df.columns:
            open_signals_df[col] = open_signals_df[col].apply(_fmt_bars)

    open_signals_df = _sort_signals_df(open_signals_df)
    OPEN_SIGNALS_DF = open_signals_df.copy()
    print(f"Prepared {len(open_signals_df)} open signals.")
else:
    open_signals_df = pd.DataFrame()
    OPEN_SIGNALS_DF = pd.DataFrame()
    print("No open signals found for any stock in the universe.")

# Export open signal for every basket
print("Exporting open signals for all baskets...")

all_open_basket_signals = []
for basket_name, (merged_all, slug, hist_folder, universe_by_qtr) in BASKET_RESULTS.items():
    if merged_all is None or merged_all.empty:
        continue

    theme, sector, industry = _basket_tsi(basket_name)
    _bdf = merged_all.copy()
    _bdf['Date'] = pd.to_datetime(_bdf['Date'], errors='coerce')
    _bdf = _bdf.dropna(subset=['Date']).sort_values('Date')
    if _bdf.empty:
        continue

    latest_row = _bdf.iloc[-1]
    latest_close = latest_row.get('Close', np.nan)
    if pd.isna(latest_close) or float(latest_close) <= 0:
        continue

    basket_last_entry_by_sig = {}
    for sig_name, flag_col in signal_flags.items():
        if flag_col not in _bdf.columns:
            continue
        sig_hits = _bdf[_bdf[flag_col] == True]
        if not sig_hits.empty:
            basket_last_entry_by_sig[sig_name] = sig_hits.iloc[-1]

    for sig_a, sig_b in _SIGNAL_PAIRS:
        row_a = basket_last_entry_by_sig.get(sig_a)
        row_b = basket_last_entry_by_sig.get(sig_b)
        date_a = pd.to_datetime(row_a['Date']) if row_a is not None else None
        date_b = pd.to_datetime(row_b['Date']) if row_b is not None else None

        if date_a is None and date_b is None:
            continue

        open_sig = sig_a if (date_b is None or (date_a is not None and date_a >= date_b)) else sig_b
        signal_row = basket_last_entry_by_sig.get(open_sig)
        if signal_row is None:
            continue

        entry_price = signal_row.get(f'{open_sig}_Entry_Price', np.nan)
        if pd.isna(entry_price) or float(entry_price) <= 0:
            continue

        if open_sig in ('Down_Rot', 'Breakdown', 'STFR'):
            current_perf = (float(entry_price) - float(latest_close)) / float(entry_price)
        else:
            current_perf = (float(latest_close) - float(entry_price)) / float(entry_price)

        out_row = {
            'Date': signal_row['Date'],
            'Basket': basket_name,
            'Close': float(latest_close),
            'Signal_Type': open_sig,
            'Theme': theme,
            'Sector': sector,
            'Industry': industry,
            'Current_Performance': current_perf,
        }
        for s in stat_suffixes:
            out_row[s] = signal_row.get(f'{open_sig}_{s}', np.nan)
        all_open_basket_signals.append(out_row)

if all_open_basket_signals:
    open_basket_signals_df = pd.DataFrame(all_open_basket_signals)
    pct_suffixes_plus_perf = pct_suffixes + ['Current_Performance']
    for s in pct_suffixes_plus_perf:
        if s in open_basket_signals_df.columns:
            open_basket_signals_df[s] = open_basket_signals_df[s].apply(lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "")
    for col in ['Close', 'Entry_Price']:
        if col in open_basket_signals_df.columns:
            open_basket_signals_df[col] = open_basket_signals_df[col].apply(_fmt_price)
    for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
        if col in open_basket_signals_df.columns:
            open_basket_signals_df[col] = open_basket_signals_df[col].apply(_fmt_bars)

    open_basket_signals_df = _sort_signals_df(open_basket_signals_df)
    print(f"Prepared {len(open_basket_signals_df)} open basket signals.")
else:
    open_basket_signals_df = pd.DataFrame()
    print("No open signals found for any basket.")

# --- Improvement C: write all 4 signal DataFrames into one 4-sheet workbook ---
_daily_path = PREVIOUS_DAY_ROTATIONS_FOLDER / f'{date_str}_Daily_Signals_for_top_{SIZE}.xlsx'
with pd.ExcelWriter(_daily_path, engine='openpyxl') as _writer:
    combined_df.to_excel(_writer, sheet_name='Signals', index=False)
    basket_df.to_excel(_writer, sheet_name='Basket Signals', index=False)
    open_signals_df.to_excel(_writer, sheet_name='Open Signals', index=False)
    open_basket_signals_df.to_excel(_writer, sheet_name='Open Basket Signals', index=False)
WriteThroughPath(_daily_path).sync()
print(f"Daily signals workbook: {_daily_path}")


# %% [markdown]
## Correlation Cache [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Correlation Cache")

from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.dates as mdates

CORR_WINDOWS = [21]
CORR_MIN_OBS_RATIO = 0.70
CORR_BLUE = '#0066ff'
CORR_PINK = '#ff3296'
CORR_CMAP = LinearSegmentedColormap.from_list(
    'corr_pink_white_blue',
    [CORR_PINK, '#ffffff', CORR_BLUE],
    N=256
)
# CORR_FOLDER, CORR_CACHE_FOLDER already defined via OutputPaths aliases above
# Correlation cache — two parquet files (basket_osc + within_osc) + signature in metadata
CORR_CACHE_BASKET_OSC_FILE  = CORR_CACHE_FOLDER / f'basket_correlations_of_{SIZE}.parquet'
CORR_CACHE_SIG_FILE         = CORR_CACHE_FOLDER / f'correlation_meta_{SIZE}.json'


def _corr_asof_date():
    asof = _get_latest_norgate_date_fallback() if '_get_latest_norgate_date_fallback' in globals() else None
    if asof is None and 'all_signals_df' in globals() and not all_signals_df.empty and 'Date' in all_signals_df.columns:
        asof = pd.to_datetime(all_signals_df['Date'], errors='coerce').max()
    if asof is None or pd.isna(asof):
        return pd.Timestamp(datetime.now()).normalize()
    return pd.Timestamp(asof).normalize()


def _corr_cache_signature():
    basket_meta = []
    if 'BASKET_RESULTS' in globals() and isinstance(BASKET_RESULTS, dict):
        for bname in sorted(BASKET_RESULTS.keys()):
            try:
                _, _, _, universe_by_qtr = BASKET_RESULTS[bname]
                u_sig = _build_universe_signature(universe_by_qtr) if '_build_universe_signature' in globals() else str(len(universe_by_qtr))
            except Exception:
                u_sig = 'na'
            basket_meta.append((bname, u_sig))

    payload = {
        'basket_meta': basket_meta,
        'windows': CORR_WINDOWS,
        'size': SIZE,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode('utf-8')).hexdigest()


def _load_corr_cache():
    """Load correlation cache from parquet files.  Returns dict or None."""
    if not (CORR_CACHE_BASKET_OSC_FILE.exists() and CORR_CACHE_SIG_FILE.exists()):
        return None
    try:
        sig_data = json.loads(CORR_CACHE_SIG_FILE.read_text(encoding='utf-8'))
        basket_osc_df = pd.read_parquet(CORR_CACHE_BASKET_OSC_FILE)
        basket_osc = {int(col): basket_osc_df[col].dropna() for col in basket_osc_df.columns}
        # within_osc_map is no longer cached to a standalone file — it's folded
        # into each basket's Correlation_Pct column. Reconstruct from baskets.
        within_osc_map = {}
        return {'signature': sig_data.get('signature'),
                'basket_osc': basket_osc,
                'within_osc_map': within_osc_map}
    except Exception:
        return None


def _save_corr_cache(payload):
    """Save correlation cache to parquet files."""
    try:
        # Signature
        WriteThroughPath(CORR_CACHE_SIG_FILE).write_text(
            json.dumps({'signature': payload.get('signature')})
        )
        print(f"Saved: {CORR_CACHE_SIG_FILE}")
        # basket_osc: dict(int → pd.Series) → DataFrame
        basket_osc = payload.get('basket_osc', {})
        if basket_osc:
            basket_osc_df = pd.DataFrame({str(w): s for w, s in basket_osc.items()})
            basket_osc_df.to_parquet(CORR_CACHE_BASKET_OSC_FILE, compression='snappy')
            WriteThroughPath(CORR_CACHE_BASKET_OSC_FILE).sync()
            print(f"Saved: {CORR_CACHE_BASKET_OSC_FILE}")
        # within_osc_map is no longer saved as a standalone file —
        # Correlation_Pct is embedded in each basket's parquet (Cell 6)
    except Exception:
        pass


def _quarter_key_from_date(d):
    d = pd.Timestamp(d)
    q = (d.month - 1) // 3 + 1
    return f"{d.year} Q{q}"


def _fallback_latest_quarter_key(universe_by_qtr):
    if not universe_by_qtr:
        return None
    keys = list(universe_by_qtr.keys())
    if not keys:
        return None
    if isinstance(keys[0], str):
        return max(keys, key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', ''))))
    return max(keys)


def _window_corr_matrix(ret_df, window, min_obs_ratio=CORR_MIN_OBS_RATIO):
    if ret_df is None or ret_df.empty:
        return pd.DataFrame()
    recent = ret_df.sort_index().tail(window)
    min_obs = max(10, int(window * min_obs_ratio))
    valid_cols = [c for c in recent.columns if recent[c].notna().sum() >= min_obs]
    if len(valid_cols) < 2:
        return pd.DataFrame()
    return recent[valid_cols].corr()


def _corr_pairs(corr_df, top_n=10):
    if corr_df is None or corr_df.empty or corr_df.shape[1] < 2:
        empty = pd.DataFrame(columns=['Pair', 'Corr'])
        return empty, empty
    cols = corr_df.columns.tolist()
    rows = []
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            v = corr_df.iat[i, j]
            if pd.notna(v):
                rows.append((f"{cols[i]} vs {cols[j]}", float(v)))
    if not rows:
        empty = pd.DataFrame(columns=['Pair', 'Corr'])
        return empty, empty
    pair_df = pd.DataFrame(rows, columns=['Pair', 'Corr']).sort_values('Corr', ascending=False).reset_index(drop=True)
    top = pair_df.head(top_n).copy()
    bottom = pair_df.tail(top_n).sort_values('Corr', ascending=True).reset_index(drop=True)
    return top, bottom


def _render_corr_heatmap(ax, corr_df, title):
    ax.set_title(title, fontsize=10, fontweight='bold', pad=6)
    if corr_df is None or corr_df.empty:
        ax.axis('off')
        ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', fontsize=10, transform=ax.transAxes)
        return None
    im = ax.imshow(corr_df.values, vmin=-1, vmax=1, cmap=CORR_CMAP, aspect='equal')
    labels = [str(c) for c in corr_df.columns]
    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=90, fontsize=6)
    ax.set_yticklabels(labels, fontsize=6)
    ax.tick_params(length=0)
    return im


def _render_pairs_table(ax, df, title):
    ax.axis('off')
    ax.set_title(title, fontsize=10, fontweight='bold', pad=6)
    if df is None or df.empty:
        ax.text(0.5, 0.5, 'No pairs', ha='center', va='center', fontsize=9, transform=ax.transAxes)
        return
    show = df.copy()
    show['Corr'] = show['Corr'].map(lambda x: f"{x:.2f}")
    tbl = ax.table(
        cellText=show[['Pair', 'Corr']].values.tolist(),
        colLabels=['Pair', 'Corr'],
        loc='center',
        cellLoc='left',
        colLoc='left',
        colWidths=[0.78, 0.22],
        bbox=[0.0, 0.0, 1.0, 0.95],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(7)
    for (r, c), cell in tbl.get_celld().items():
        if r == 0:
            cell.set_text_props(weight='bold')
            cell.set_facecolor((0.93, 0.93, 0.93))
        cell.set_edgecolor((0.75, 0.75, 0.75))
        cell.set_linewidth(0.5)


def _build_stock_returns_matrix():
    if 'all_signals_df' not in globals() or all_signals_df.empty:
        return pd.DataFrame()
    df = all_signals_df[['Date', 'Ticker', 'Close']].copy()
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.normalize()
    df = df.dropna(subset=['Date', 'Ticker', 'Close']).sort_values(['Ticker', 'Date'])
    if df.empty:
        return pd.DataFrame()
    df['Ret'] = df.groupby('Ticker')['Close'].pct_change()
    ret = df.pivot(index='Date', columns='Ticker', values='Ret').sort_index()
    return ret


def _build_basket_returns_matrix():
    if 'BASKET_RESULTS' not in globals() or not BASKET_RESULTS:
        return pd.DataFrame()
    series_map = {}
    for basket_name, (merged_all, _, _, _) in BASKET_RESULTS.items():
        if merged_all is None or merged_all.empty:
            continue
        b = merged_all[['Date', 'Close']].copy()
        b['Date'] = pd.to_datetime(b['Date'], errors='coerce').dt.normalize()
        b = b.dropna(subset=['Date', 'Close']).sort_values('Date')
        if b.empty:
            continue
        s = b.set_index('Date')['Close'].pct_change()
        series_map[basket_name] = s
    if not series_map:
        return pd.DataFrame()
    return pd.concat(series_map, axis=1).sort_index()


def _mean_offdiag(corr_df):
    if corr_df is None or corr_df.empty or corr_df.shape[1] < 2:
        return np.nan, np.nan, np.nan, 0
    vals = corr_df.values
    iu = np.triu_indices_from(vals, k=1)
    pairs = vals[iu]
    pairs = pairs[np.isfinite(pairs)]
    if pairs.size == 0:
        return np.nan, np.nan, np.nan, 0
    return float(np.mean(pairs)), float(np.median(pairs)), float(np.min(pairs)), int(pairs.size)


def _rolling_avg_pairwise_corr_series(ret_df, window, min_obs_ratio=CORR_MIN_OBS_RATIO):
    """Daily oscillator: rolling mean off-diagonal correlation in [-1, 1]."""
    if ret_df is None or ret_df.empty:
        return pd.Series(dtype=float)
    ret_df = ret_df.sort_index()
    min_obs = max(10, int(window * min_obs_ratio))
    cols = ret_df.columns.tolist()
    n = len(cols)
    if n < 2:
        return pd.Series(dtype=float)
    # Compute rolling correlation for each unique pair in Cython (avoids Python loop over dates)
    pair_series = [
        ret_df[cols[i]].rolling(window, min_periods=min_obs).corr(ret_df[cols[j]])
        for i in range(n) for j in range(i + 1, n)
    ]
    pair_df = pd.concat(pair_series, axis=1)
    avg = pair_df.mean(axis=1, skipna=True)
    avg[pair_df.count(axis=1) == 0] = np.nan
    avg.index = pd.to_datetime(avg.index, errors='coerce')
    return avg.clip(-1, 1).dropna()


def _series_last_date(s):
    if s is None or not isinstance(s, pd.Series) or s.empty:
        return pd.NaT
    idx = pd.to_datetime(s.index, errors='coerce')
    if len(idx) == 0:
        return pd.NaT
    return pd.Timestamp(idx.max()).normalize()


def _update_rolling_osc_incremental(existing_s, ret_df, window):
    """Incrementally extend a cached oscillator series when new dates arrive."""
    if ret_df is None or ret_df.empty:
        return existing_s if isinstance(existing_s, pd.Series) else pd.Series(dtype=float)
    ret_df = ret_df.sort_index()
    if not isinstance(existing_s, pd.Series) or existing_s.empty:
        return _rolling_avg_pairwise_corr_series(ret_df, window)

    src_last = pd.Timestamp(ret_df.index.max()).normalize()
    last_cached = _series_last_date(existing_s)
    if pd.isna(last_cached):
        return _rolling_avg_pairwise_corr_series(ret_df, window)
    if src_last <= last_cached:
        out = existing_s.copy()
        out.index = pd.to_datetime(out.index, errors='coerce')
        return out[out.index <= src_last].sort_index().clip(-1, 1)

    idx = pd.to_datetime(ret_df.index, errors='coerce')
    pos = idx.searchsorted(last_cached, side='right') - 1
    if pos < 0:
        return _rolling_avg_pairwise_corr_series(ret_df, window)
    start_pos = max(0, pos - window + 1)
    new_s = _rolling_avg_pairwise_corr_series(ret_df.iloc[start_pos:], window)
    if new_s.empty:
        out = existing_s.copy()
        out.index = pd.to_datetime(out.index, errors='coerce')
        return out.sort_index().clip(-1, 1)

    base = existing_s.copy()
    base.index = pd.to_datetime(base.index, errors='coerce')
    cutoff = pd.Timestamp(new_s.index.min()).normalize()
    merged = pd.concat([base[base.index < cutoff], new_s]).sort_index()
    merged = merged[~merged.index.duplicated(keep='last')]
    return merged.clip(-1, 1)


def _update_within_osc_map_incremental(stock_ret, basket_constituents, cached_map=None, progress_label=None):
    """Return per-window, per-basket rolling oscillator series with incremental updates."""
    if cached_map is None or not isinstance(cached_map, dict):
        cached_map = {}
    out = {w: {} for w in CORR_WINDOWS}
    total_jobs = max(1, len(CORR_WINDOWS) * len(basket_constituents))
    done = 0
    last_milestone = 0
    for w in CORR_WINDOWS:
        prev_w = cached_map.get(w, {}) if isinstance(cached_map.get(w, {}), dict) else {}
        for bname, tickers in basket_constituents.items():
            done += 1
            if len(tickers) < 2:
                continue
            cols = [t for t in tickers if t in stock_ret.columns]
            if len(cols) < 2:
                continue
            prev_s = prev_w.get(bname)
            out[w][bname] = _update_rolling_osc_incremental(prev_s, stock_ret[cols], w)
            if progress_label:
                pct = int((done / total_jobs) * 100)
                ms = pct // 10 * 10
                if ms > last_milestone and ms % 10 == 0:
                    print(f"  {progress_label}: {ms}% complete ({done} / {total_jobs} basket-window jobs)")
                    last_milestone = ms
    return out


def _plot_corr_oscillator(ax, series_by_window, title):
    ax.set_title(title, fontsize=11, fontweight='bold', pad=8)
    style = {
        21:  {'color': CORR_BLUE, 'linestyle': '-',  'linewidth': 1.3},
        63:  {'color': CORR_BLUE, 'linestyle': '--', 'linewidth': 1.3},
        252: {'color': CORR_PINK, 'linestyle': '-',  'linewidth': 1.3},
    }
    has_data = False
    for w in CORR_WINDOWS:
        s = series_by_window.get(w, pd.Series(dtype=float))
        if s is None or s.empty:
            continue
        s = s.sort_index()
        last_dt = pd.to_datetime(s.index, errors='coerce').max()
        if pd.notna(last_dt):
            s = s[s.index >= (pd.Timestamp(last_dt).normalize() - pd.DateOffset(years=1))]
        if s.empty:
            continue
        has_data = True
        ax.plot(s.index, s.values, label=f'{w}d', **style.get(w, {'color': '#555555'}))
    if not has_data:
        ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', fontsize=10, transform=ax.transAxes)
        ax.set_axis_off()
        return
    ax.axhline(0, color='#555555', linewidth=0.8)
    ax.set_ylim(-1, 1)
    ax.set_ylabel('Correlation')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.1f}"))
    ax.grid(axis='y', alpha=0.25, linewidth=0.4)
    locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', fontsize=7)
    ax.legend(loc='upper left', fontsize=8)


def _plot_single_corr_oscillator(ax, s, window, title):
    ax.set_title(title, fontsize=11, fontweight='bold', pad=8)
    if s is None or s.empty:
        ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', fontsize=10, transform=ax.transAxes)
        ax.set_axis_off()
        return
    s = s.sort_index()
    last_dt = pd.to_datetime(s.index, errors='coerce').max()
    if pd.notna(last_dt):
        s = s[s.index >= (pd.Timestamp(last_dt).normalize() - pd.DateOffset(years=1))]
    if s.empty:
        ax.text(0.5, 0.5, 'Insufficient data', ha='center', va='center', fontsize=10, transform=ax.transAxes)
        ax.set_axis_off()
        return
    color = CORR_BLUE if window in (21, 63) else CORR_PINK
    linestyle = '--' if window == 63 else '-'
    ax.plot(s.index, s.values, color=color, linestyle=linestyle, linewidth=1.4, label=f'{window}d')
    ax.axhline(0, color='#555555', linewidth=0.8)
    ax.set_ylim(-1, 1)
    ax.set_ylabel('Correlation')
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.1f}"))
    ax.grid(axis='y', alpha=0.25, linewidth=0.4)
    locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right', fontsize=7)
    ax.legend(loc='upper left', fontsize=8)


def build_correlation_reports():
    asof = _corr_asof_date()
    asof_str = asof.strftime('%Y_%m_%d')
    corr_sig = _corr_cache_signature()
    corr_cache = _load_corr_cache()
    cache_ok = isinstance(corr_cache, dict) and corr_cache.get('signature') == corr_sig

    basket_ret = _build_basket_returns_matrix()
    stock_ret = _build_stock_returns_matrix()

    # 1) Basket-to-basket correlation PDF — build_pdf()
    basket_pdf = CORR_FOLDER / f'{asof_str}_Basket_to_Basket_Correlations.pdf'
    _basket_figs = []
    print("Building basket correlation snapshots...")
    for w in CORR_WINDOWS:
        print(f"  Window {w}d...")
        corr = _window_corr_matrix(basket_ret, w)
        top, bottom = _corr_pairs(corr, top_n=10)

        # Page A: Full-page heatmap, colorbar to the right of the axes
        fig, ax_hm = plt.subplots(figsize=(11, 8.5))
        fig.patch.set_facecolor('white')
        im = _render_corr_heatmap(ax_hm, corr, f'Basket-to-Basket Correlation ({w}d)')
        if im is not None:
            plt.colorbar(im, ax=ax_hm, fraction=0.02, pad=0.04, aspect=30)
        fig.suptitle(f'Basket Correlation Snapshot — {w} Trading Days', fontsize=12, fontweight='bold', y=0.98)
        fig.tight_layout()
        _basket_figs.append(fig)

        # Page B: Highest and lowest correlation pairs tables
        fig2, (ax_top, ax_bot) = plt.subplots(2, 1, figsize=(8.5, 11), gridspec_kw={'hspace': 0.5})
        fig2.patch.set_facecolor('white')
        _render_pairs_table(ax_top, top,    f'Highest Correlation Pairs ({w}d)')
        _render_pairs_table(ax_bot, bottom, f'Lowest Correlation Pairs ({w}d)')
        fig2.suptitle(f'Correlation Pairs — {w} Trading Days', fontsize=12, fontweight='bold', y=0.98)
        fig2.tight_layout()
        _basket_figs.append(fig2)

    # Compute basket_osc for the cache (incremental); no longer rendered as a standalone PDF page
    cached_basket_osc = corr_cache.get('basket_osc', {}) if cache_ok and isinstance(corr_cache, dict) else {}
    basket_osc = {}
    print("Building basket correlation oscillators...")
    for w in CORR_WINDOWS:
        print(f"  Basket oscillator {w}d...")
        basket_osc[w] = _update_rolling_osc_incremental(cached_basket_osc.get(w), basket_ret, w)
    if cache_ok and cached_basket_osc:
        print("Correlation cache: basket oscillators incrementally updated")

    # Per-pair rolling correlation charts, sorted highest to lowest by most-recent primary-window correlation
    _WIN_STYLES = {
        21:  {'color': CORR_BLUE, 'linestyle': '-',  'linewidth': 1.0},
        63:  {'color': CORR_BLUE, 'linestyle': '--', 'linewidth': 1.0},
        252: {'color': CORR_PINK, 'linestyle': '-',  'linewidth': 1.0},
    }
    if not basket_ret.empty and basket_ret.shape[1] >= 2:
        _bcols = basket_ret.columns.tolist()
        _all_pairs = [(a, b) for i, a in enumerate(_bcols) for b in _bcols[i + 1:]]

        _primary_w = CORR_WINDOWS[0]
        _primary_corr = _window_corr_matrix(basket_ret, _primary_w)

        def _pair_val(pair):
            a, b = pair
            if _primary_corr.empty or a not in _primary_corr.index or b not in _primary_corr.columns:
                return float('-inf')
            return float(_primary_corr.loc[a, b])

        _all_pairs_sorted = sorted(_all_pairs, key=_pair_val, reverse=True)
        _total_pairs = len(_all_pairs_sorted)

        _NC, _NR = 3, 4          # 12 mini-charts per page
        _pp = _NC * _NR
        _n_pair_pages = max(1, (_total_pairs + _pp - 1) // _pp)
        print(f"  Building {_total_pairs} per-pair rolling correlation charts ({_n_pair_pages} pages)...")

        for _pi in range(_n_pair_pages):
            _page_pairs = _all_pairs_sorted[_pi * _pp : (_pi + 1) * _pp]
            fig_p, axes_p = plt.subplots(_NR, _NC, figsize=(11, 8.5))
            fig_p.patch.set_facecolor('white')
            _axf = axes_p.flatten()
            for _k, (a, b) in enumerate(_page_pairs):
                _ax = _axf[_k]
                for _w in CORR_WINDOWS:
                    if a in basket_ret.columns and b in basket_ret.columns:
                        _rc = basket_ret[a].rolling(_w).corr(basket_ret[b]).dropna()
                        if not _rc.empty:
                            _st = _WIN_STYLES.get(_w, {'color': CORR_BLUE, 'linestyle': '-', 'linewidth': 1.0})
                            _ax.plot(_rc.index, _rc.values, label=f'{_w}d', **_st)
                _ax.axhline(0, color='#888888', linewidth=0.5)
                _ax.set_ylim(-1, 1)
                _cv = _pair_val((a, b))
                _cv_str = f'  ({_cv:.2f})' if _cv != float('-inf') else ''
                _ax.set_title(f'{a}\nvs {b}{_cv_str}', fontsize=5.5, fontweight='bold', pad=2)
                _ax.tick_params(axis='both', labelsize=5)
                _ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f'{y:.1f}'))
                _loc = mdates.AutoDateLocator(minticks=3, maxticks=6)
                _ax.xaxis.set_major_locator(_loc)
                _ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(_loc))
                plt.setp(_ax.get_xticklabels(), rotation=30, ha='right', fontsize=4)
                if len(CORR_WINDOWS) > 1 and _k == 0:
                    _ax.legend(fontsize=4, loc='upper left')
            for _k in range(len(_page_pairs), _NR * _NC):
                _axf[_k].set_visible(False)
            _rng_lo = _pi * _pp + 1
            _rng_hi = min((_pi + 1) * _pp, _total_pairs)
            fig_p.suptitle(
                f'Basket-to-Basket Rolling Correlations — sorted high→low  '
                f'({_rng_lo}–{_rng_hi} of {_total_pairs})',
                fontsize=8, fontweight='bold',
            )
            fig_p.tight_layout()
            _basket_figs.append(fig_p)

    build_pdf(_basket_figs, basket_pdf)
    print(f"Saved basket correlation PDF: {basket_pdf}")

    # 2) Stock-within-basket correlation PDF
    stock_pdf = CORR_FOLDER / f'{asof_str}_Stocks_within_Baskets_Correlations.pdf'
    rows = []
    snapshot_corr = {}
    basket_constituents = {}

    current_key = _quarter_key_from_date(asof)
    print("Building within-basket stock correlation snapshots...")
    _snap_total = len(BASKET_RESULTS) * len(CORR_WINDOWS)
    _snap_done = 0
    _snap_last_ms = 0
    for basket_name, (_, _, _, universe_by_qtr) in BASKET_RESULTS.items():
        active_key = current_key if current_key in universe_by_qtr else _fallback_latest_quarter_key(universe_by_qtr)
        tickers = sorted([t for t in universe_by_qtr.get(active_key, set()) if t in stock_ret.columns]) if active_key else []
        basket_constituents[basket_name] = tickers
        for w in CORR_WINDOWS:
            _snap_done += 1
            if _snap_total > 0:
                _pct = int((_snap_done / _snap_total) * 100)
                _ms = _pct // 10 * 10
                if _ms > _snap_last_ms and _ms % 10 == 0:
                    print(f"  Snapshot: {_ms}% complete ({_snap_done} / {_snap_total})")
                    _snap_last_ms = _ms
            if len(tickers) < 2:
                rows.append({
                    'Window': w, 'Basket': basket_name, 'Quarter_Key': active_key or '',
                    'Stock_Count': len(tickers), 'Pair_Count': 0,
                    'Avg_Corr': np.nan, 'Median_Corr': np.nan, 'Min_Corr': np.nan
                })
                continue

            recent = stock_ret[tickers].sort_index().tail(w)
            min_obs = max(10, int(w * CORR_MIN_OBS_RATIO))
            valid = [c for c in recent.columns if recent[c].notna().sum() >= min_obs]
            if len(valid) < 2:
                rows.append({
                    'Window': w, 'Basket': basket_name, 'Quarter_Key': active_key or '',
                    'Stock_Count': len(valid), 'Pair_Count': 0,
                    'Avg_Corr': np.nan, 'Median_Corr': np.nan, 'Min_Corr': np.nan
                })
                continue

            corr = recent[valid].corr()
            avg_c, med_c, min_c, n_pairs = _mean_offdiag(corr)
            rows.append({
                'Window': w, 'Basket': basket_name, 'Quarter_Key': active_key or '',
                'Stock_Count': len(valid), 'Pair_Count': n_pairs,
                'Avg_Corr': avg_c, 'Median_Corr': med_c, 'Min_Corr': min_c
            })
            snapshot_corr[(basket_name, w)] = corr

    within_df = pd.DataFrame(rows)

    # Daily rolling oscillator over time (within-basket stock correlations), per basket
    print("Building within-basket stock correlation oscillators...")
    cached_within_osc_map = corr_cache.get('within_osc_map', {}) if cache_ok and isinstance(corr_cache, dict) else {}
    within_osc_map = _update_within_osc_map_incremental(
        stock_ret,
        basket_constituents,
        cached_map=cached_within_osc_map,
        progress_label="Within-osc",
    )
    if cache_ok and cached_within_osc_map:
        print("Correlation cache: within-basket oscillators incrementally updated")

    # 2) Stocks-within-basket correlation PDF — build_pdf()
    _stock_figs = []
    for basket_name in sorted(basket_constituents.keys()):
        n_win = len(CORR_WINDOWS)
        fig = plt.figure(figsize=(11, 8.5))
        gs = fig.add_gridspec(3, n_win, height_ratios=[1.15, 1.0, 0.55], wspace=0.22, hspace=0.30)

        heat_axes = [fig.add_subplot(gs[0, j]) for j in range(n_win)]
        osc_axes = [fig.add_subplot(gs[1, j]) for j in range(n_win)]
        ax_tbl = fig.add_subplot(gs[2, :])
        ax_tbl.axis('off')

        last_im = None
        for j, w in enumerate(CORR_WINDOWS):
            cdf = snapshot_corr.get((basket_name, w), pd.DataFrame())
            last_im = _render_corr_heatmap(heat_axes[j], cdf, f'{w}d Snapshot')
            _plot_single_corr_oscillator(
                osc_axes[j],
                within_osc_map.get(w, {}).get(basket_name, pd.Series(dtype=float)),
                w,
                f'{w}d Oscillator',
            )

        bstats = within_df[within_df['Basket'] == basket_name].copy().sort_values('Window')
        if bstats.empty:
            ax_tbl.text(0.5, 0.5, 'No basket stats available', ha='center', va='center', fontsize=9, transform=ax_tbl.transAxes)
        else:
            show = bstats[['Window', 'Stock_Count', 'Pair_Count', 'Avg_Corr', 'Median_Corr', 'Min_Corr']].copy()
            for c in ['Avg_Corr', 'Median_Corr', 'Min_Corr']:
                show[c] = show[c].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
            show['Window'] = show['Window'].map(lambda x: f"{int(x)}d")
            table = ax_tbl.table(
                cellText=show.values.tolist(),
                colLabels=['Window', 'Stocks', 'Pairs', 'Avg Corr', 'Median Corr', 'Min Corr'],
                loc='center',
                cellLoc='center',
                colLoc='center',
                colWidths=[0.12, 0.12, 0.12, 0.18, 0.18, 0.18],
                bbox=[0.08, 0.02, 0.84, 0.90],
            )
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            for (r, c), cell in table.get_celld().items():
                if r == 0:
                    cell.set_text_props(weight='bold')
                    cell.set_facecolor((0.93, 0.93, 0.93))
                cell.set_edgecolor((0.75, 0.75, 0.75))
                cell.set_linewidth(0.5)

        fig.suptitle(f'{basket_name} - Stocks Within Basket Correlations', fontsize=12, fontweight='bold', y=0.98)
        if last_im is not None:
            cbar_ax = fig.add_axes([0.92, 0.55, 0.01, 0.35])
            cbar = fig.colorbar(last_im, cax=cbar_ax)
            cbar.ax.tick_params(labelsize=7)
        fig.subplots_adjust(left=0.05, right=0.90, bottom=0.05, top=0.92, wspace=0.22, hspace=0.30)
        _stock_figs.append(fig)
    build_pdf(_stock_figs, stock_pdf)
    print(f"Saved within-basket stock correlation PDF: {stock_pdf}")
    _save_corr_cache({
        'signature': corr_sig,
        'basket_osc': basket_osc,
        'within_osc_map': within_osc_map,
    })

    return basket_pdf, stock_pdf, within_osc_map, snapshot_corr


_corr_result = build_correlation_reports()
_correlation_report_paths = _corr_result[:2]
WITHIN_OSC_MAP = _corr_result[2]
SNAPSHOT_CORR = _corr_result[3]

# %% [markdown]
## Per-Basket Excel Reports [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Per-Basket Excel Reports")


def export_basket_excel_reports():
    """Export breadth and equity signal trade history to Excel for each basket.
    Moved from _finalize_basket_signals_output to Group B (report-only).
    """
    if not BASKET_RESULTS:
        print("No basket results available, skipping Excel exports.")
        return

    total = len(BASKET_RESULTS)
    last_milestone = 0
    exported = 0

    for idx, (basket_name, (merged_all, slug, hist_folder, universe_by_qtr)) in enumerate(BASKET_RESULTS.items(), start=1):
        if merged_all is None or merged_all.empty:
            continue

        hist_folder.mkdir(parents=True, exist_ok=True)

        # Breadth signal trades
        trend_up_trades = compute_signal_trades(merged_all, 'B_Up_Rot', 'B_Down_Rot', direction='long')
        trend_down_trades = compute_signal_trades(merged_all, 'B_Down_Rot', 'B_Up_Rot', direction='short')
        regime_up_trades = compute_signal_trades(merged_all, 'BO_B_Up_Rot', 'BO_B_Down_Rot', direction='long')
        regime_down_trades = compute_signal_trades(merged_all, 'BO_B_Down_Rot', 'BO_B_Up_Rot', direction='short')

        div_trade_rows = []
        trend_bull_div_dates = set(merged_all.loc[merged_all['B_Bull_Div'].astype(bool), 'Date'])
        trend_bear_div_dates = set(merged_all.loc[merged_all['B_Bear_Div'].astype(bool), 'Date'])
        regime_bull_div_dates = set(merged_all.loc[merged_all['BO_B_Bull_Div'].astype(bool), 'Date'])
        regime_bear_div_dates = set(merged_all.loc[merged_all['BO_B_Bear_Div'].astype(bool), 'Date'])
        _append_trade_rows(div_trade_rows, 'Trend Up Rotation', 'long', trend_up_trades, trend_bull_div_dates, trend_bear_div_dates)
        _append_trade_rows(div_trade_rows, 'Trend Down Rotation', 'short', trend_down_trades, trend_bull_div_dates, trend_bear_div_dates)
        _append_trade_rows(div_trade_rows, 'Regime Up Rotation', 'long', regime_up_trades, regime_bull_div_dates, regime_bear_div_dates)
        _append_trade_rows(div_trade_rows, 'Regime Down Rotation', 'short', regime_down_trades, regime_bull_div_dates, regime_bear_div_dates)

        if div_trade_rows:
            div_df = pd.DataFrame(div_trade_rows).sort_values('Entry_Date')
            div_df['Entry_Date'] = pd.to_datetime(div_df['Entry_Date']).dt.strftime('%Y-%m-%d')
            div_df['Exit_Date'] = pd.to_datetime(div_df['Exit_Date']).dt.strftime('%Y-%m-%d')
            for col in ['Entry_Price', 'Exit_Price']:
                div_df[col] = div_df[col].apply(_fmt_price)
            for col in ['Final_Change', 'MFE', 'MAE', 'Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_MFE', 'Avg_MAE', 'Historical_EV', 'Std_Dev', 'Risk_Adj_EV', 'EV_Last_3', 'Risk_Adj_EV_Last_3']:
                if col in div_df.columns:
                    div_df[col] = div_df[col].apply(_fmt_pct)
            for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
                if col in div_df.columns:
                    div_df[col] = div_df[col].apply(_fmt_bars)
            _breadth_xlsx = hist_folder / f'{slug}_Breadth_Signals.xlsx'
            div_df.to_excel(_breadth_xlsx, index=False, engine='openpyxl')
            WriteThroughPath(_breadth_xlsx).sync()
            print(f"Saved: {_breadth_xlsx}")

        # Equity signal trades
        basket_rows = []
        signal_specs = [('Up Rot', 'Is_Up_Rotation', 'Up_Rot', 'long'), ('Down Rot', 'Is_Down_Rotation', 'Down_Rot', 'short'), ('Breakout', 'Is_Breakout', 'Breakout', 'long'), ('Breakdown', 'Is_Breakdown', 'Breakdown', 'short'), ('BTFD', 'Is_BTFD', 'BTFD', 'long'), ('STFR', 'Is_STFR', 'STFR', 'short')]
        merged_all_dates = pd.to_datetime(merged_all['Date']).dt.normalize()
        date_to_idx = {d: i for i, d in enumerate(merged_all_dates)}
        for signal_name, flag_col, sig_prefix, direction in signal_specs:
            if flag_col not in merged_all.columns:
                continue
            signal_entries = merged_all.loc[merged_all[flag_col].astype(bool)].copy()
            for _, r in signal_entries.iterrows():
                entry_dt = pd.to_datetime(r['Date']).normalize()
                exit_dt_raw = r.get(f'{sig_prefix}_Exit_Date', np.nan)
                exit_dt = pd.to_datetime(exit_dt_raw).normalize() if pd.notna(exit_dt_raw) else pd.NaT
                entry_idx = date_to_idx.get(entry_dt)
                exit_idx = date_to_idx.get(exit_dt) if pd.notna(exit_dt) else None
                bars = (exit_idx - entry_idx) if (entry_idx is not None and exit_idx is not None) else np.nan
                basket_rows.append({'Signal': signal_name, 'Direction': direction, 'Entry_Date': r['Date'], 'Exit_Date': exit_dt_raw, 'Entry_Price': r.get(f'{sig_prefix}_Entry_Price', np.nan), 'Exit_Price': r.get(f'{sig_prefix}_Exit_Price', np.nan), 'Final_Change': r.get(f'{sig_prefix}_Final_Change', np.nan), 'MFE': r.get(f'{sig_prefix}_MFE', np.nan), 'MAE': r.get(f'{sig_prefix}_MAE', np.nan), 'Bars': bars, 'Win_Rate': r.get(f'{sig_prefix}_Win_Rate', np.nan), 'Avg_Winner': r.get(f'{sig_prefix}_Avg_Winner', np.nan), 'Avg_Loser': r.get(f'{sig_prefix}_Avg_Loser', np.nan), 'Avg_Winner_Bars': r.get(f'{sig_prefix}_Avg_Winner_Bars', np.nan), 'Avg_Loser_Bars': r.get(f'{sig_prefix}_Avg_Loser_Bars', np.nan), 'Avg_MFE': r.get(f'{sig_prefix}_Avg_MFE', np.nan), 'Avg_MAE': r.get(f'{sig_prefix}_Avg_MAE', np.nan), 'Historical_EV': r.get(f'{sig_prefix}_Historical_EV', np.nan), 'Std_Dev': r.get(f'{sig_prefix}_Std_Dev', np.nan), 'Risk_Adj_EV': r.get(f'{sig_prefix}_Risk_Adj_EV', np.nan), 'EV_Last_3': r.get(f'{sig_prefix}_EV_Last_3', np.nan), 'Risk_Adj_EV_Last_3': r.get(f'{sig_prefix}_Risk_Adj_EV_Last_3', np.nan), 'Count': r.get(f'{sig_prefix}_Count', np.nan)})

        if basket_rows:
            basket_df = pd.DataFrame(basket_rows).sort_values('Entry_Date')
            basket_df['Entry_Date'] = pd.to_datetime(basket_df['Entry_Date']).dt.strftime('%Y-%m-%d')
            basket_df['Exit_Date'] = pd.to_datetime(basket_df['Exit_Date']).dt.strftime('%Y-%m-%d')
            for col in ['Entry_Price', 'Exit_Price']:
                basket_df[col] = basket_df[col].apply(_fmt_price)
            for col in ['Final_Change', 'MFE', 'MAE', 'Win_Rate', 'Avg_Winner', 'Avg_Loser', 'Avg_MFE', 'Avg_MAE', 'Historical_EV', 'Std_Dev', 'Risk_Adj_EV', 'EV_Last_3', 'Risk_Adj_EV_Last_3']:
                if col in basket_df.columns:
                    basket_df[col] = basket_df[col].apply(_fmt_pct)
            for col in ['Avg_Winner_Bars', 'Avg_Loser_Bars', 'Count']:
                if col in basket_df.columns:
                    basket_df[col] = basket_df[col].apply(_fmt_bars)
            _equity_xlsx = hist_folder / f'{slug}_Equity_Signals.xlsx'
            basket_df.to_excel(_equity_xlsx, index=False, engine='openpyxl')
            WriteThroughPath(_equity_xlsx).sync()
            print(f"Saved: {_equity_xlsx}")

        exported += 1

        percent = int((idx / total) * 100)
        ms = percent // 10 * 10
        if ms > last_milestone and ms % 10 == 0:
            print(f"  {ms}% complete ({idx} / {total} baskets)")
            last_milestone = ms

    print(f"Per-basket Excel reports complete: {exported} baskets exported.")


export_basket_excel_reports()

# %% [markdown]
## PNG Chart Generation [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("PNG Chart Generation")


def plot_one_year_breadth_and_equity(breadth_df, equity_df, title, filename, output_folder=None):
    """Compatibility plotting helper for Cells 11-12."""
    if output_folder is None:
        output_folder = THEMATIC_CHARTS_FOLDER
    if breadth_df.empty or equity_df.empty:
        print("No data to plot.")
        return

    last_date = breadth_df['Date'].max()
    start_date = last_date - pd.DateOffset(years=1)
    b = breadth_df[breadth_df['Date'] >= start_date].copy()
    e = equity_df[equity_df['Date'] >= start_date].copy()
    if b.empty or e.empty:
        print("No data in the last 1 year.")
        return

    b = b.sort_values('Date')
    e = e.sort_values('Date')
    b['Breadth_EMA'] = b['Breadth_Ratio'].ewm(span=10, adjust=False).mean()
    b['Uptrend_Pct'] = (b['Uptrend_Count'] / b['Total_Stocks']) * 100.0
    b['Downtrend_Pct'] = (b['Downtrend_Count'] / b['Total_Stocks']) * 100.0

    Path(output_folder).mkdir(exist_ok=True)
    merged = pd.merge(
        e[['Date', 'Equity']],
        b[['Date', 'Uptrend_Pct', 'Downtrend_Pct', 'Breadth_EMA']],
        on='Date', how='inner'
    ).sort_values('Date').reset_index(drop=True)
    if merged.empty:
        print("No overlapping dates to plot.")
        return

    x_axis = merged.index
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(11, 10), sharex=True, gridspec_kw={'height_ratios': [2, 1]}
    )
    ax1.plot(x_axis, merged['Equity'], color='blue', linewidth=2.0, label='Equity Curve')
    ax1.set_ylabel('Equity', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper left', fontsize=9)
    ax1.set_title(title, fontsize=14, fontweight='bold')

    ax2.bar(x_axis, merged['Uptrend_Pct'], color=(186/255, 216/255, 240/255, 1.0), width=1.0, label='Uptrend %')
    ax2.bar(x_axis, -merged['Downtrend_Pct'], color=(255/255, 175/255, 205/255, 1.0), width=1.0, label='Downtrend %')
    ax2.plot(x_axis, merged['Breadth_EMA'] * 100.0, color='black', linewidth=1.5, label='Breadth EMA %')
    ax2.set_ylabel('Breadth %', fontsize=12, fontweight='bold')
    ax2.grid(True, axis='y', alpha=0.15)
    ax2.axhline(0, color='gray', linewidth=0.8)
    ax2.xaxis.set_major_locator(ticker.MaxNLocator(12))

    def format_date(x, pos=None):
        thisind = int(x)
        if 0 <= thisind < len(merged):
            return merged['Date'].iloc[thisind].strftime('%b %d')
        return ""

    ax2.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
    plt.xticks(rotation=45)
    plt.tight_layout()

    out_path = output_folder / filename
    plt.savefig(out_path, dpi=150)
    plt.close()
    WriteThroughPath(out_path).sync()
    print(f"Saved: {out_path}")
# Consolidated execution starts here.

def _slugify_label(label):
    return label.replace('/', ' ').replace('&', 'and').replace('-', ' ').replace(' ', '_')


def _make_fmt(df):
    def fmt(x, pos=None):
        idx = int(x)
        if 0 <= idx < len(df):
            return df['Date'].iloc[idx].strftime('%b %d')
        return ""
    return fmt



def plot_basket_charts(name, merged_all, slug, hist_folder, universe_by_qtr):
    charts_folder = hist_folder.parent
    first_date = merged_all['Date'].min()
    last_date = merged_all['Date'].max()
    year_start = pd.Timestamp(first_date.year, 1, 1)
    hist_created = 0
    hist_skipped = 0
    last_year_created = False
    last_year_skipped = False

    # Chart schema version check — force rebuild when layout changes (version embedded in parquet)
    _charts_stale = (_get_chart_schema_version_from_parquet(slug) != CHART_SCHEMA_VERSION)
    if _charts_stale:
        # Delete old chart PNGs so they get regenerated with new layout
        for _old_png in hist_folder.glob('*.png'):
            _old_png.unlink(missing_ok=True)

    while year_start <= last_date:
        year_end = year_start + pd.DateOffset(years=1)
        if year_end > last_date and year_start.year == last_date.year:
            trailing_start = last_date - pd.DateOffset(years=1)
            merged = merged_all[(merged_all['Date'] > trailing_start)].reset_index(drop=True)
            year_label = 'Last 1 Year'
        else:
            merged = merged_all[(merged_all['Date'] >= year_start) & (merged_all['Date'] < year_end)].reset_index(drop=True)
            year_label = year_start.year

        if merged.empty:
            year_start = year_end
            continue

        is_last_year_chart = (year_label == 'Last 1 Year')
        current_data_date = merged['Date'].max().strftime('%Y-%m-%d')
        chart_date_str = current_data_date.replace('-', '_') if is_last_year_chart else str(year_label)
        chart_file = f"{chart_date_str}_Top_{THEME_SIZE}_{slug}_of_{SIZE}.png"
        if is_last_year_chart:
            ltm_folder = charts_folder / chart_date_str
            ltm_folder.mkdir(parents=True, exist_ok=True)
            chart_path = ltm_folder / chart_file
        else:
            chart_path = hist_folder / chart_file

        if not is_last_year_chart and chart_path.exists():
            hist_skipped += 1
            WriteThroughPath(chart_path).sync()
            year_start = year_end
            continue
        if is_last_year_chart and chart_path.exists():
            # Check schema version for LTM charts too (version embedded in parquet)
            _ltm_stale = (_get_chart_schema_version_from_parquet(slug) != CHART_SCHEMA_VERSION)
            if _ltm_stale:
                for _old in ltm_folder.glob('*.png'):
                    _old.unlink(missing_ok=True)
            else:
                last_year_skipped = True
                WriteThroughPath(chart_path).sync()
                year_start = year_end
                continue

        x_axis = merged.index
        fig = plt.figure(figsize=(20, 16), constrained_layout=True)
        gs = fig.add_gridspec(
            4, 2,
            width_ratios=[1.25, 4.75],
            height_ratios=[2.5, 1, 1, 1],
            wspace=0.06
        )
        ax_tbl = fig.add_subplot(gs[:, 0])
        ax1 = fig.add_subplot(gs[0, 1])
        ax2 = fig.add_subplot(gs[1, 1], sharex=ax1)
        ax3 = fig.add_subplot(gs[2, 1], sharex=ax1)
        ax4 = fig.add_subplot(gs[3, 1], sharex=ax1)

        # Left panel: compact holdings table (quarters as columns, sorted tickers down rows)
        window_end = pd.to_datetime(merged['Date']).max()
        quarter_holdings = {}
        if isinstance(year_label, int):
            preferred_keys = [f"{year_label} Q1", f"{year_label} Q2", f"{year_label} Q3", f"{year_label} Q4"]
            for q_key in preferred_keys:
                ticks = sorted(universe_by_qtr.get(q_key, set()))
                if ticks:
                    quarter_holdings[q_key] = ticks
        else:
            cur_q = (window_end.month - 1) // 3 + 1
            keys_sorted = sorted(
                universe_by_qtr.keys(),
                key=lambda k: (int(k.split()[0]), int(k.split()[1].replace('Q', '')))
            )
            eligible = [
                k for k in keys_sorted
                if (int(k.split()[0]), int(k.split()[1].replace('Q', ''))) <= (window_end.year, cur_q)
            ]
            for q_key in eligible[-4:]:
                ticks = sorted(universe_by_qtr.get(q_key, set()))
                if ticks:
                    quarter_holdings[q_key] = ticks

        if not quarter_holdings:
            fallback_key = sorted(universe_by_qtr.keys())[-1]
            quarter_holdings[fallback_key] = sorted(universe_by_qtr.get(fallback_key, set()))

        ax_tbl.axis('off')
        ax_tbl.set_title(f'{name} Holdings by Quarter', fontsize=10, fontweight='bold', loc='center')
        quarter_cols = list(quarter_holdings.keys())[-4:]
        quarter_holdings = {k: quarter_holdings[k] for k in quarter_cols}
        max_len = max((len(v) for v in quarter_holdings.values()), default=0)
        table_rows = []
        for i_row in range(max_len):
            table_rows.append([
                quarter_holdings[q][i_row] if i_row < len(quarter_holdings[q]) else ''
                for q in quarter_cols
            ])
        holdings_table = ax_tbl.table(
            cellText=table_rows,
            colLabels=quarter_cols,
            colLoc='center',
            cellLoc='center',
            colWidths=[1.0 / max(1, len(quarter_cols))] * max(1, len(quarter_cols)),
            bbox=[0.0, 0.0, 1.0, 0.98]
        )
        holdings_table.auto_set_font_size(False)
        holdings_table.set_fontsize(8)
        for (r, c), cell in holdings_table.get_celld().items():
            if r == 0:
                cell.set_text_props(weight='bold')
                cell.set_facecolor((0.92, 0.92, 0.92))
                cell.set_edgecolor((0.75, 0.75, 0.75))
                cell.set_linewidth(0.6)
            else:
                cell.set_facecolor((0.985, 0.985, 0.985))
                cell.set_edgecolor((0.985, 0.985, 0.985))
                cell.set_linewidth(0.0)
                cell.PAD = 0.03

        blue = (50/255, 50/255, 255/255)
        pink = (255/255, 50/255, 150/255)
        light_blue = (186/255, 216/255, 240/255, 1.0)
        light_pink = (255/255, 175/255, 205/255, 1.0)
        breadth_size = 68
        breakout_size = breadth_size * 2

        # Top panel as OHLC candlesticks
        o_vals = merged['Open'].to_numpy(dtype=float)
        h_vals = merged['High'].to_numpy(dtype=float)
        l_vals = merged['Low'].to_numpy(dtype=float)
        c_vals = merged['Close'].to_numpy(dtype=float)
        for xi, o, h, l, c in zip(x_axis, o_vals, h_vals, l_vals, c_vals):
            if np.isnan(o) or np.isnan(h) or np.isnan(l) or np.isnan(c):
                continue
            is_up = c >= o
            body_bottom = min(o, c)
            body_height = abs(c - o)
            if body_height == 0:
                body_height = max(abs(o) * 0.0002, 1e-6)
            edge_color = light_blue if is_up else light_pink
            ax1.vlines(xi, l, h, color=edge_color, linewidth=0.8, zorder=2)
            ax1.bar(
                xi, body_height, bottom=body_bottom, width=0.6,
                color=edge_color, edgecolor=edge_color, linewidth=1.0,
                align='center', zorder=3
            )
        ax1.scatter(x_axis, merged['Resistance_Pivot'], color=(255/255, 50/255, 150/255), s=1)
        ax1.scatter(x_axis, merged['Support_Pivot'], color=(50/255, 50/255, 255/255), s=1)
        ax1.plot(x_axis, merged['Upper_Target'], color=(50/255, 50/255, 255/255), linewidth=2)
        lower_target_plot = merged['Lower_Target'].where(pd.to_numeric(merged['Lower_Target'], errors='coerce') > 0, np.nan)
        ax1.plot(x_axis, lower_target_plot, color=(255/255, 50/255, 150/255), linewidth=2)
        ax1.set_ylabel('Equity', fontsize=12, fontweight='bold')
        ax1.set_title(f'{name} Basket - {year_label}', fontsize=14, fontweight='bold')

        ax2.bar(x_axis, merged['Uptrend_Pct'], color=(186/255, 216/255, 240/255, 1.0), width=1.0)
        ax2.bar(x_axis, -merged['Downtrend_Pct'], color=(255/255, 175/255, 205/255, 1.0), width=1.0)
        ax2.plot(x_axis, merged['Breadth_EMA'] * 100.0, color='black', linewidth=1.5)
        if 'B_Resistance' in merged.columns:
            ax2.scatter(x_axis, merged['B_Resistance'] * 100.0, color=pink, s=1)
        if 'B_Support' in merged.columns:
            ax2.scatter(x_axis, merged['B_Support'] * 100.0, color=blue, s=1)

        b_up = merged['B_Up_Rot'].astype(bool) if 'B_Up_Rot' in merged.columns else pd.Series(False, index=merged.index)
        b_dn = merged['B_Down_Rot'].astype(bool) if 'B_Down_Rot' in merged.columns else pd.Series(False, index=merged.index)
        b_bull = merged['B_Bull_Div'].astype(bool) if 'B_Bull_Div' in merged.columns else pd.Series(False, index=merged.index)
        b_bear = merged['B_Bear_Div'].astype(bool) if 'B_Bear_Div' in merged.columns else pd.Series(False, index=merged.index)
        b_up_div = b_up & b_bull
        b_dn_div = b_dn & b_bear
        b_up_norm = b_up & ~b_bull
        b_dn_norm = b_dn & ~b_bear

        # Breadth panel arrows: normal rotations are white fill, divergences are filled.
        if b_up_norm.any():
            ax2.scatter(x_axis[b_up_norm], merged.loc[b_up_norm, 'Breadth_EMA'] * 100.0,
                        marker='^', s=breadth_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=6)
        if b_dn_norm.any():
            ax2.scatter(x_axis[b_dn_norm], merged.loc[b_dn_norm, 'Breadth_EMA'] * 100.0,
                        marker='v', s=breadth_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=6)
        if b_up_div.any():
            ax2.scatter(x_axis[b_up_div], merged.loc[b_up_div, 'Breadth_EMA'] * 100.0,
                        marker='^', s=breadth_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=7)
        if b_dn_div.any():
            ax2.scatter(x_axis[b_dn_div], merged.loc[b_dn_div, 'Breadth_EMA'] * 100.0,
                        marker='v', s=breadth_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=7)

        # Price panel arrows: place relative to candle wicks.
        low_vals = merged['Low']
        high_vals = merged['High']
        wick_span = (high_vals - low_vals).abs()
        min_off = merged['Close'].abs() * 0.002
        wick_off = np.maximum(wick_span * 0.08, min_off)
        up_price_y = low_vals - wick_off
        dn_price_y = high_vals + wick_off
        if b_up_norm.any():
            ax1.scatter(x_axis[b_up_norm], up_price_y[b_up_norm],
                        marker='^', s=breadth_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=8)
        if b_dn_norm.any():
            ax1.scatter(x_axis[b_dn_norm], dn_price_y[b_dn_norm],
                        marker='v', s=breadth_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=8)
        if b_up_div.any():
            ax1.scatter(x_axis[b_up_div], up_price_y[b_up_div],
                        marker='^', s=breadth_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=9)
        if b_dn_div.any():
            ax1.scatter(x_axis[b_dn_div], dn_price_y[b_dn_div],
                        marker='v', s=breadth_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=9)
        ax2.set_ylabel('Breadth %', fontsize=12, fontweight='bold')
        ax2.grid(True, axis='y', alpha=0.15)
        ax2.axhline(0, color='gray', linewidth=0.8)
        ax2.set_ylim(-100, 100)
        ax1.tick_params(axis='x', labelbottom=False)
        ax2.tick_params(axis='x', labelbottom=False)

        ax3.bar(x_axis, merged['Breakout_Pct'], color=(186/255, 216/255, 240/255, 1.0), width=1.0)
        ax3.bar(x_axis, -merged['Breakdown_Pct'], color=(255/255, 175/255, 205/255, 1.0), width=1.0)
        ax3.plot(x_axis, merged['BO_Breadth_EMA'] * 100.0, color='black', linewidth=1.5)
        if 'BO_B_Resistance' in merged.columns:
            ax3.scatter(x_axis, merged['BO_B_Resistance'] * 100.0, color=pink, s=1)
        if 'BO_B_Support' in merged.columns:
            ax3.scatter(x_axis, merged['BO_B_Support'] * 100.0, color=blue, s=1)

        bo_up = merged['BO_B_Up_Rot'].astype(bool) if 'BO_B_Up_Rot' in merged.columns else pd.Series(False, index=merged.index)
        bo_dn = merged['BO_B_Down_Rot'].astype(bool) if 'BO_B_Down_Rot' in merged.columns else pd.Series(False, index=merged.index)
        bo_bull = merged['BO_B_Bull_Div'].astype(bool) if 'BO_B_Bull_Div' in merged.columns else pd.Series(False, index=merged.index)
        bo_bear = merged['BO_B_Bear_Div'].astype(bool) if 'BO_B_Bear_Div' in merged.columns else pd.Series(False, index=merged.index)
        bo_up_div = bo_up & bo_bull
        bo_dn_div = bo_dn & bo_bear
        bo_up_norm = bo_up & ~bo_bull
        bo_dn_norm = bo_dn & ~bo_bear

        # Breakout panel arrows: same format, 2x breadth size.
        if bo_up_norm.any():
            ax3.scatter(x_axis[bo_up_norm], merged.loc[bo_up_norm, 'BO_Breadth_EMA'] * 100.0,
                        marker='^', s=breakout_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=6)
        if bo_dn_norm.any():
            ax3.scatter(x_axis[bo_dn_norm], merged.loc[bo_dn_norm, 'BO_Breadth_EMA'] * 100.0,
                        marker='v', s=breakout_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=6)
        if bo_up_div.any():
            ax3.scatter(x_axis[bo_up_div], merged.loc[bo_up_div, 'BO_Breadth_EMA'] * 100.0,
                        marker='^', s=breakout_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=7)
        if bo_dn_div.any():
            ax3.scatter(x_axis[bo_dn_div], merged.loc[bo_dn_div, 'BO_Breadth_EMA'] * 100.0,
                        marker='v', s=breakout_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=7)

        # Price panel arrows for breakout rotations (2x size vs breadth).
        if bo_up_norm.any():
            ax1.scatter(x_axis[bo_up_norm], up_price_y[bo_up_norm],
                        marker='^', s=breakout_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=8)
        if bo_dn_norm.any():
            ax1.scatter(x_axis[bo_dn_norm], dn_price_y[bo_dn_norm],
                        marker='v', s=breakout_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=8)
        if bo_up_div.any():
            ax1.scatter(x_axis[bo_up_div], up_price_y[bo_up_div],
                        marker='^', s=breakout_size, facecolors=blue, edgecolors=blue, linewidths=1.2, zorder=9)
        if bo_dn_div.any():
            ax1.scatter(x_axis[bo_dn_div], dn_price_y[bo_dn_div],
                        marker='v', s=breakout_size, facecolors=pink, edgecolors=pink, linewidths=1.2, zorder=9)
        ax3.set_ylabel('Breakout %', fontsize=12, fontweight='bold')
        ax3.grid(True, axis='y', alpha=0.15)
        ax3.axhline(0, color='gray', linewidth=0.8)
        ax3.set_ylim(-100, 100)
        ax3.tick_params(axis='x', labelbottom=False)

        # 4th panel: 21-day intra-basket correlation oscillator
        corr_osc = WITHIN_OSC_MAP.get(21, {}).get(name, pd.Series(dtype=float)) if 'WITHIN_OSC_MAP' in globals() else pd.Series(dtype=float)
        if not corr_osc.empty:
            corr_osc = corr_osc.sort_index()
            merged_dates = pd.to_datetime(merged['Date'])
            corr_start = merged_dates.min()
            corr_end = merged_dates.max()
            corr_window = corr_osc[(corr_osc.index >= corr_start) & (corr_osc.index <= corr_end)]
            if not corr_window.empty:
                # Map correlation dates to x_axis integer positions
                date_to_x = dict(zip(merged_dates.dt.normalize(), x_axis))
                corr_x = [date_to_x[d] for d in corr_window.index.normalize() if d in date_to_x]
                corr_y = [corr_window.loc[d] for d in corr_window.index if d.normalize() in date_to_x]
                if corr_x:
                    ax4.plot(corr_x, corr_y, color=(50/255, 50/255, 255/255), linewidth=1.4)
        ax4.axhline(0, color='#555555', linewidth=0.8)
        ax4.set_ylim(-1, 1)
        ax4.set_ylabel('21d Corr', fontsize=12, fontweight='bold')
        ax4.grid(True, axis='y', alpha=0.15)
        ax4.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.1f}"))

        # One x-axis date label per month (on bottom panel now).
        date_series = pd.to_datetime(merged['Date'])
        month_tick_idx = date_series.groupby(date_series.dt.to_period('M')).head(1).index.tolist()
        month_tick_lbls = [date_series.iloc[idx].strftime('%b %Y') for idx in month_tick_idx]
        ax4.set_xticks(month_tick_idx)
        ax4.set_xticklabels(month_tick_lbls, rotation=45, ha='right')

        fig.savefig(chart_path, dpi=150, bbox_inches='tight')
        plt.close(fig)
        WriteThroughPath(chart_path).sync()
        if is_last_year_chart:
            last_year_created = True
        else:
            hist_created += 1
        year_start = year_end

    return hist_created


_chart_total = len(BASKET_RESULTS)
_chart_built = 0
_chart_last_milestone = 0
for _ci, (basket_name, (merged_all, slug, hist_folder, universe_by_qtr)) in enumerate(BASKET_RESULTS.items(), start=1):
    _chart_built += plot_basket_charts(basket_name, merged_all, slug, hist_folder, universe_by_qtr)
    _pct = int((_ci / _chart_total) * 100) if _chart_total else 100
    _ms = _pct // 10 * 10
    if _ms > _chart_last_milestone and _ms % 10 == 0:
        print(f"  {_ms}% complete ({_ci} / {_chart_total} baskets)")
        _chart_last_milestone = _ms
print(f"All basket charts complete (created={_chart_built}, up_to_date={_chart_total - _chart_built}).")


# %% [markdown]
## Comprehensive Summary Report (PDF) [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Comprehensive Summary Report")

import matplotlib.dates as mdates
import matplotlib.image as mpimg
import textwrap
from pandas.errors import EmptyDataError

_PAGE_W = 8.5
_PAGE_H = 11.0
_MARGIN = 1.0  # inches


# ---------------------------------------------------------------------------
# Helper: find most-recent file matching a glob in a folder
# ---------------------------------------------------------------------------
def _find_latest_file(folder, pattern):
    matches = sorted(folder.glob(pattern))
    return matches[-1] if matches else None


def _date_label_from_file(path):
    """Extract a readable date/time stamp from a signals filename.
    2026_02_24_Signals_for_top_500.xlsx            -> '2026-02-24'
    2026_02_24_1430_Live_Signals_for_top_500.xlsx  -> '2026-02-24 14:30'
    Returns '' if the filename doesn't match the expected pattern.
    """
    import re
    stem = Path(path).stem
    m = re.match(r'^(\d{4})_(\d{2})_(\d{2})_(\d{2})(\d{2})_', stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}:{m.group(5)}"
    m = re.match(r'^(\d{4})_(\d{2})_(\d{2})_', stem)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ''


# ---------------------------------------------------------------------------
# Helper: locate the LTM basket chart PNG for a given basket name
# ---------------------------------------------------------------------------
def _find_basket_chart_path(basket_name):
    if basket_name not in BASKET_RESULTS:
        return None
    merged_all, slug, hist_folder, _ = BASKET_RESULTS[basket_name]
    charts_folder = hist_folder.parent
    date_str = merged_all['Date'].max().strftime('%Y_%m_%d')
    chart_path = charts_folder / date_str / f"{date_str}_Top_{THEME_SIZE}_{slug}_of_{SIZE}.png"
    return chart_path if chart_path.exists() else None


# ---------------------------------------------------------------------------
# Helper: embed an existing PNG as a full portrait page
# ---------------------------------------------------------------------------
def _embed_image_page(pdf, img_path, title=None, landscape=False):
    img = mpimg.imread(str(img_path))
    img_h_px, img_w_px = img.shape[:2]
    img_ratio = img_w_px / img_h_px  # e.g. ~1.6 for landscape charts

    page_w = _PAGE_H if landscape else _PAGE_W   # 11.0" or 8.5"
    page_h = _PAGE_W if landscape else _PAGE_H   # 8.5"  or 11.0"

    title_space = 0.25 if title else 0.0
    avail_w = page_w - 2 * _MARGIN
    avail_h = page_h - 2 * _MARGIN - title_space

    # Fit within available area while preserving the original aspect ratio
    if img_ratio >= avail_w / avail_h:
        display_w = avail_w
        display_h = avail_w / img_ratio
    else:
        display_h = avail_h
        display_w = avail_h * img_ratio

    # Centre the image in the available area
    left = _MARGIN + (avail_w - display_w) / 2
    bottom = _MARGIN + (avail_h - display_h) / 2

    fig = plt.figure(figsize=(page_w, page_h))
    fig.patch.set_facecolor('white')
    ax = fig.add_axes([left / page_w, bottom / page_h,
                       display_w / page_w, display_h / page_h])
    ax.imshow(img)
    ax.axis('off')
    if title:
        fig.suptitle(title, fontsize=9, fontweight='bold', y=0.99)
    pdf.savefig(fig, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Helper: render a DataFrame as paginated portrait tables
# ---------------------------------------------------------------------------
# These constants are shared across every table in the PDF so all tables
# have identical row heights and column proportions regardless of row count.
_TABLE_ROW_H_IN   = 0.21   # inches per row (data rows AND header row)
_TABLE_TOP_FRAC   = 0.93   # axes y-fraction where the top of the table sits
_TABLE_AXES_H_IN  = _PAGE_H - 2 * _MARGIN   # 9.0"
_TABLE_ROW_H_FRAC = _TABLE_ROW_H_IN / _TABLE_AXES_H_IN   # row height in axes coords
# Max data rows per page: rows that fit below the table-top boundary
_TABLE_ROWS_PER_PAGE = max(1, int(_TABLE_TOP_FRAC / _TABLE_ROW_H_FRAC) - 1)

def _render_df_table_pages(pdf, df, title, keep_cols=None):
    if keep_cols:
        df = df[[c for c in keep_cols if c in df.columns]].copy()
    if df.empty:
        return

    cols = df.columns.tolist()
    str_df = df.fillna('').astype(str)

    # Dynamic column widths: proportional to max content character length
    header_labels = [c.replace('_', ' ') for c in cols]
    max_chars = [
        max(len(h), int(str_df[c].str.len().max()) if len(str_df) > 0 else 0)
        for h, c in zip(header_labels, cols)
    ]
    total_chars = max(1, sum(max_chars))
    col_widths = [mc / total_chars for mc in max_chars]

    # Dynamic font size: ensure all columns fit within the axes width
    # axes_width_pts = 6.5" Ã— 72 pt/in; each char â‰ˆ 0.58 Ã— font_size pts wide
    axes_w_pts = (_PAGE_W - 2 * _MARGIN) * 72
    font_size = min(7.5, max(5.0, axes_w_pts / (total_chars * 0.58)))

    # Wrap headers proportionally to each column's character budget
    wrapped_headers = [
        textwrap.fill(h, width=max(4, mc))
        for h, mc in zip(header_labels, max_chars)
    ]
    # Header row may need extra height if any header wraps to multiple lines
    max_header_lines = max(1, max(h.count('\n') + 1 for h in wrapped_headers))
    header_cell_h_frac = _TABLE_ROW_H_FRAC * max_header_lines

    n_total = len(df)
    n_pages = max(1, -(-n_total // _TABLE_ROWS_PER_PAGE))

    for page_i in range(n_pages):
        chunk = df.iloc[page_i * _TABLE_ROWS_PER_PAGE:(page_i + 1) * _TABLE_ROWS_PER_PAGE]
        n_rows = len(chunk)

        # bbox height = taller header row + fixed data row height Ã— data rows
        table_h = header_cell_h_frac + _TABLE_ROW_H_FRAC * n_rows
        table_bbox = [0.0, _TABLE_TOP_FRAC - table_h, 1.0, table_h]

        fig = plt.figure(figsize=(_PAGE_W, _PAGE_H))
        fig.patch.set_facecolor('white')
        ax = fig.add_axes([
            _MARGIN / _PAGE_W,
            _MARGIN / _PAGE_H,
            (_PAGE_W - 2 * _MARGIN) / _PAGE_W,
            _TABLE_AXES_H_IN / _PAGE_H,
        ])
        ax.axis('off')
        page_label = f"  (page {page_i + 1}/{n_pages})" if n_pages > 1 else ""
        ax.set_title(f"{title}{page_label}", fontsize=11, fontweight='bold', pad=8)

        table = ax.table(
            cellText=chunk.fillna('').astype(str).values.tolist(),
            colLabels=wrapped_headers,
            loc='upper center',
            cellLoc='center',
            colLoc='center',
            colWidths=col_widths,
            bbox=table_bbox,
        )
        table.auto_set_font_size(False)
        table.set_fontsize(font_size)
        table.scale(1.0, 1.0)

        # Override cell heights: header cells expand to show all wrapped lines;
        # data cells stay at the standard single-line height.
        for (r, _), cell in table.get_celld().items():
            if r == 0:
                cell.set_height(header_cell_h_frac)
            else:
                cell.set_height(_TABLE_ROW_H_FRAC)

        left_align_cols = {'Basket', 'Ticker', 'Signal_Type', 'Theme', 'Sector', 'Industry'}
        for (r, c), cell in table.get_celld().items():
            if r == 0:
                cell.set_text_props(weight='bold')
                cell.set_facecolor((0.93, 0.93, 0.93))
            if r > 0 and cols[c] in left_align_cols:
                cell.get_text().set_ha('left')
                cell.PAD = 0.02
            cell.set_edgecolor((0.75, 0.75, 0.75))
            cell.set_linewidth(0.5)

        pdf.savefig(fig, dpi=150)
        plt.close(fig)


# ---------------------------------------------------------------------------
# Helper: YTD rebase line chart (one page per basket group type)
# ---------------------------------------------------------------------------
def _render_ytd_rebase_page(pdf, daily_grid_full, group_prefix, title):
    """Cumulative % return from Jan 1 of current year for baskets in group_prefix."""
    ytd_start = pd.Timestamp(f'{datetime.now().year}-01-01')
    cols = [c for c in daily_grid_full.columns if c.startswith(group_prefix)]
    if not cols:
        return
    ytd = daily_grid_full.loc[daily_grid_full.index >= ytd_start, cols].copy().fillna(0)
    if ytd.empty:
        return
    rebased = (1 + ytd).cumprod() - 1
    short_names = [c[len(group_prefix):] for c in rebased.columns]

    fig, ax = plt.subplots(figsize=(_PAGE_W, _PAGE_H))
    fig.patch.set_facecolor('white')
    ax.set_position([
        _MARGIN / _PAGE_W,
        1.5 * _MARGIN / _PAGE_H,
        (_PAGE_W - 2 * _MARGIN) / _PAGE_W,
        (_PAGE_H - 2.5 * _MARGIN) / _PAGE_H,
    ])

    n_lines = len(rebased.columns)
    cmap = plt.get_cmap('tab20', max(n_lines, 1))
    for i, (col, label) in enumerate(zip(rebased.columns, short_names)):
        ax.plot(rebased.index, rebased[col] * 100, label=label,
                linewidth=1.2, color=cmap(i))

    ax.axhline(0, color='#555555', linewidth=0.6, zorder=3)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.0f}%"))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=6)
    ax.tick_params(axis='y', labelsize=7)
    ax.grid(axis='y', linewidth=0.3, alpha=0.4)
    ax.set_facecolor('white')
    for spine in ax.spines.values():
        spine.set_linewidth(0.4)
        spine.set_edgecolor('#aaaaaa')

    n_legend_cols = 2 if n_lines > 6 else 1
    ax.legend(fontsize=6, ncol=n_legend_cols, loc='upper left',
              framealpha=0.85, edgecolor='#cccccc')
    fig.suptitle(title, fontsize=12, fontweight='bold', y=0.98)
    pdf.savefig(fig, dpi=150)
    plt.close(fig)


# ---------------------------------------------------------------------------
# Main: generate_summary_pdf
# ---------------------------------------------------------------------------
def generate_summary_pdf(live_ctx=None):
    now = datetime.now()
    stamp = now.strftime('%Y_%m_%d_%H%M')
    summary_path = SUMMARY_FOLDER / f'{stamp}_Summary.pdf'
    report_asof = _get_latest_norgate_date_fallback()
    report_date_prefix = report_asof.strftime('%Y_%m_%d') if report_asof is not None else None

    if live_ctx is None:
        live_ctx = _get_live_update_context()

    all_specs = _get_all_basket_specs_for_reports()

    print("Building Summary PDF...")

    with PdfPages(summary_path) as pdf:
        # ------------------------------------------------------------------
        # 1-3. Returns by group: Annual â†’ Last 20 Days â†’ YTD Rebase
        #      Order: Themes, then Sectors, then Industries
        # ------------------------------------------------------------------
        print("  [1-3/6] Building return grids...")
        basket_year_grid = _build_basket_annual_grid(live_ctx=live_ctx)
        basket_date_grid = _build_basket_daily_grid_last20(live_ctx=live_ctx)
        daily_grid_full = _build_group_daily_return_grid(all_specs, live_ctx=live_ctx)
        if not daily_grid_full.empty:
            daily_grid_full = daily_grid_full.sort_index()
            if live_ctx is not None:
                live_today = live_ctx['today']
                for spec in all_specs:
                    group_name = spec[0]
                    universe_by_qtr = spec[1]
                    live_ret = _compute_live_basket_return(
                        universe_by_qtr, live_ctx['live_price_map'],
                        live_ctx['last_rows'], live_ctx['current_key'])
                    if pd.notna(live_ret):
                        daily_grid_full.loc[live_today, group_name] = float(live_ret)

        for _grp_prefix, _grp_label in [
            ('Theme: ',    'Themes'),
            ('Sector: ',   'Sectors'),
            ('Industry: ', 'Industries'),
        ]:
            def _grp_rows(grid, prefix=_grp_prefix):
                if grid.empty:
                    return grid
                rows = [i for i in grid.index if i.startswith(prefix)]
                return grid.loc[rows] if rows else grid.iloc[:0]

            grp_year = _grp_rows(basket_year_grid)
            grp_date = _grp_rows(basket_date_grid)

            # Annual returns — asymmetric y-axis based on this group's actual min/max
            if not grp_year.empty:
                _yr_min = min(-0.10, round(float(grp_year.min().min()) * 1.05 - 0.01, 2))
                _yr_max = max( 0.10, round(float(grp_year.max().max()) * 1.05 + 0.01, 2))
                _render_return_bar_charts(
                    pdf, f'Annual Returns — {_grp_label}',
                    grp_year, y_min=_yr_min, y_max=_yr_max,
                    figsize=(_PAGE_W, _PAGE_H), n_cols=4, n_rows_fixed=3,
                )

            # Last 20 days — asymmetric y-axis based on this group's actual min/max
            if not grp_date.empty:
                _d20_min = min(-0.03, round(float(grp_date.min().min()) * 1.05 - 0.005, 3))
                _d20_max = max( 0.03, round(float(grp_date.max().max()) * 1.05 + 0.005, 3))
                _render_return_bar_charts(
                    pdf, f'Daily Returns — Last 20 Days ({_grp_label})',
                    grp_date, y_min=_d20_min, y_max=_d20_max,
                    figsize=(_PAGE_W, _PAGE_H), n_cols=4, n_rows_fixed=3,
                )

            # YTD rebase
            if not daily_grid_full.empty:
                _render_ytd_rebase_page(
                    pdf, daily_grid_full, _grp_prefix,
                    f'YTD Returns — {_grp_label}',
                )

        # ------------------------------------------------------------------
        # 4. Basket Signals Table + basket charts for signalling baskets
        # ------------------------------------------------------------------
        print("  [4/6] Basket signals table...")
        _daily_xlsx = None
        if report_date_prefix:
            _daily_xlsx = _find_latest_file(
                PREVIOUS_DAY_ROTATIONS_FOLDER,
                f'{report_date_prefix}_Daily_Signals_for_top_{SIZE}.xlsx',
            )
        basket_cols = ['Basket', 'Signal_Type', 'Theme', 'Sector', 'Industry',
                       'Entry_Price', 'Win_Rate', 'Historical_EV', 'Risk_Adj_EV',
                       'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
                       'Count']
        signalling_baskets = []
        if _daily_xlsx and _daily_xlsx.exists():
            basket_df = pd.read_excel(_daily_xlsx, sheet_name='Basket Signals', engine='openpyxl')
            _basket_date = _date_label_from_file(_daily_xlsx)
            _basket_title = f'Basket Signals — {_basket_date}' if _basket_date else 'Basket Signals — Previous Day'
            _render_df_table_pages(pdf, basket_df, _basket_title, basket_cols)
            signalling_baskets = basket_df['Basket'].dropna().unique().tolist() \
                if 'Basket' in basket_df.columns else []
        else:
            if report_date_prefix:
                print(f"    No daily signals workbook for {report_date_prefix} — skipping basket table.")
            else:
                print("    No daily signals workbook found — skipping basket table.")

        for basket_name in signalling_baskets:
            chart_path = _find_basket_chart_path(basket_name)
            if chart_path:
                _embed_image_page(pdf, chart_path, basket_name, landscape=True)

        # ------------------------------------------------------------------
        # 5. Individual Stock Signals Table
        # ------------------------------------------------------------------
        print("  [5/6] Individual stock signals table...")
        stock_cols = ['Ticker', 'Signal_Type', 'Theme', 'Sector',
                      'Entry_Price', 'Win_Rate', 'Historical_EV', 'Risk_Adj_EV',
                      'Avg_Winner', 'Avg_Loser', 'Avg_Winner_Bars', 'Avg_Loser_Bars',
                      'Count']
        if _daily_xlsx and _daily_xlsx.exists():
            stock_df = pd.read_excel(_daily_xlsx, sheet_name='Signals', engine='openpyxl')
            _stock_date = _date_label_from_file(_daily_xlsx)
            _stock_title = f'Individual Stock Signals — {_stock_date}' if _stock_date else 'Individual Stock Signals — Previous Day'
            _render_df_table_pages(pdf, stock_df, _stock_title, stock_cols)
        else:
            print("    No daily signals workbook found — skipping stock signals table.")

        # ------------------------------------------------------------------
        # 6. Live Signals Table (only if today's live file exists with rows)
        # ------------------------------------------------------------------
        print("  [6/6] Live signals (if available)...")
        today_prefix = datetime.now().strftime('%Y_%m_%d')
        live_xlsx = _find_latest_file(LIVE_ROTATIONS_FOLDER,
                                      f'{today_prefix}*_Live_Signals_for_top_{SIZE}.xlsx')
        if live_xlsx and live_xlsx.exists():
            try:
                live_df = _sort_signals_df(pd.read_excel(live_xlsx, engine='openpyxl'))
            except Exception:
                live_df = pd.DataFrame()
            if not live_df.empty:
                _live_stamp = _date_label_from_file(live_xlsx)
                _live_title = f'Live Stock Signals — {_live_stamp}' if _live_stamp else 'Live Stock Signals'
                _render_df_table_pages(pdf, live_df, _live_title, stock_cols)
            else:
                print("    Live signals file is empty — skipping.")
        else:
            print("    No live signals for today — Norgate is current, skipping.")

    WriteThroughPath(summary_path).sync()
    print(f"Summary PDF saved: {summary_path}")
    return summary_path


_summary_pdf_path = generate_summary_pdf(
    live_ctx=_live_ctx_for_reports if '_live_ctx_for_reports' in globals() else None
)

# %% [markdown]
## Per-Basket Report (PDF) [Group B — Report Only]
# %%

if 'reset_cell_timer' in globals():
    reset_cell_timer("Per-Basket Report PDFs")

import gc


def generate_basket_report_pdfs():
    """Generate a consolidated PDF report for each basket.

    Page 1: LTM basket chart (OHLC + breadth + breakout + 21d correlation)
    Page 2: Open signals table filtered to the basket's current tickers
    Page 3: 21-day correlation heatmap + 1-year cumulative return rebase chart
    """
    if 'BASKET_RESULTS' not in globals() or not BASKET_RESULTS:
        print("No basket results available, skipping basket report PDFs.")
        return

    open_sig_df = OPEN_SIGNALS_DF if 'OPEN_SIGNALS_DF' in globals() else pd.DataFrame()
    within_osc = WITHIN_OSC_MAP if 'WITHIN_OSC_MAP' in globals() else {}
    snap_corr = SNAPSHOT_CORR if 'SNAPSHOT_CORR' in globals() else {}

    # Determine the latest data date for the report filename
    _sample_merged = next(iter(BASKET_RESULTS.values()))[0]
    report_date = pd.to_datetime(_sample_merged['Date']).max()
    report_date_str = report_date.strftime('%Y_%m_%d')

    built = 0
    skipped = 0
    _pdf_total = len(BASKET_RESULTS)
    _pdf_last_milestone = 0
    for _pdf_i, (basket_name, (merged_all, slug, hist_folder, universe_by_qtr)) in enumerate(BASKET_RESULTS.items(), start=1):
        if merged_all is None or merged_all.empty:
            continue

        charts_folder = hist_folder.parent
        ltm_date_str = pd.to_datetime(merged_all['Date']).max().strftime('%Y_%m_%d')
        ltm_folder = charts_folder / ltm_date_str
        summary_folder = BASE_OUTPUT_FOLDER / 'Baskets' / 'Basket_Summary' / ltm_date_str
        summary_folder.mkdir(parents=True, exist_ok=True)
        report_path = summary_folder / f'{ltm_date_str}_{slug}_Report.pdf'

        # Skip if report already exists (only mirror to OneDrive if mirror is missing)
        _need_write, _need_mirror = _needs_write_and_mirror(report_path)
        if not _need_write:
            if _need_mirror:
                WriteThroughPath(report_path).sync()
            skipped += 1
            continue

        ltm_folder.mkdir(parents=True, exist_ok=True)

        with PdfPages(report_path) as pdf:
            # ==============================================================
            # PAGE 1: Embed the LTM basket chart PNG
            # ==============================================================
            chart_path = _find_basket_chart_path(basket_name)
            if chart_path and chart_path.exists():
                _embed_image_page(pdf, chart_path, basket_name, landscape=True)
            else:
                # Fallback: blank page with message
                fig, ax = plt.subplots(figsize=(11, 8.5))
                ax.text(0.5, 0.5, f'{basket_name}\nChart not available',
                        ha='center', va='center', fontsize=16, transform=ax.transAxes)
                ax.axis('off')
                pdf.savefig(fig, dpi=150)
                plt.close(fig)

            # ==============================================================
            # PAGE 2: Open signals table (filtered to basket tickers)
            # ==============================================================
            # Determine current basket tickers
            asof = pd.to_datetime(merged_all['Date']).max()
            current_key = _quarter_key_from_date(asof)
            if current_key not in universe_by_qtr:
                current_key = _fallback_latest_quarter_key(universe_by_qtr)
            basket_tickers = set(universe_by_qtr.get(current_key, set())) if current_key else set()

            if not open_sig_df.empty and 'Ticker' in open_sig_df.columns and basket_tickers:
                filtered_signals = open_sig_df[open_sig_df['Ticker'].isin(basket_tickers)].copy()
                filtered_signals = filtered_signals.sort_values('Ticker').reset_index(drop=True)
            else:
                filtered_signals = pd.DataFrame()

            if not filtered_signals.empty:
                _render_df_table_pages(
                    pdf, filtered_signals,
                    f'{basket_name} — Open Stock Signals',
                    keep_cols=['Ticker', 'Signal_Type', 'Close', 'Current_Performance',
                               'Entry_Price', 'Win_Rate', 'Historical_EV', 'Risk_Adj_EV',
                               'Avg_Winner', 'Avg_Loser', 'Count'],
                )
            else:
                fig, ax = plt.subplots(figsize=(8.5, 11))
                ax.text(0.5, 0.5, f'{basket_name}\nNo open signals for basket tickers',
                        ha='center', va='center', fontsize=14, transform=ax.transAxes)
                ax.axis('off')
                fig.suptitle(f'{basket_name} — Open Stock Signals', fontsize=12, fontweight='bold', y=0.96)
                pdf.savefig(fig, dpi=150)
                plt.close(fig)

            # ==============================================================
            # PAGE 3: Correlation heatmap + 1-year rebase chart
            # ==============================================================
            fig = plt.figure(figsize=(11, 8.5))
            gs_p3 = fig.add_gridspec(2, 1, height_ratios=[1, 1.2], hspace=0.35)

            # Top: 21-day correlation heatmap
            ax_heat = fig.add_subplot(gs_p3[0])
            corr_df = snap_corr.get((basket_name, 21), pd.DataFrame())
            _render_corr_heatmap(ax_heat, corr_df, f'{basket_name} — 21d Stock Correlation')
            if not corr_df.empty:
                im = ax_heat.images[0] if ax_heat.images else None
                if im is not None:
                    cbar_ax = fig.add_axes([0.92, 0.55, 0.01, 0.35])
                    cbar = fig.colorbar(im, cax=cbar_ax)
                    cbar.ax.tick_params(labelsize=7)
                    fig.subplots_adjust(right=0.90)

            # Bottom: 1-year cumulative return rebase chart
            ax_rebase = fig.add_subplot(gs_p3[1])
            tickers_for_rebase = sorted(basket_tickers & set(all_signals_df['Ticker'].unique())) if basket_tickers else []
            if tickers_for_rebase:
                rebase_start = asof - pd.DateOffset(years=1)
                sig_subset = all_signals_df[
                    (all_signals_df['Ticker'].isin(tickers_for_rebase)) &
                    (pd.to_datetime(all_signals_df['Date']) >= rebase_start)
                ][['Date', 'Ticker', 'Close']].copy()
                sig_subset['Date'] = pd.to_datetime(sig_subset['Date']).dt.normalize()
                sig_subset = sig_subset.sort_values(['Ticker', 'Date'])

                price_pivot = sig_subset.pivot_table(index='Date', columns='Ticker', values='Close', aggfunc='last')
                daily_ret = price_pivot.pct_change()
                cum_ret = (1 + daily_ret).cumprod() - 1

                cmap = plt.colormaps.get_cmap('tab20').resampled(max(len(tickers_for_rebase), 1))
                for i, tkr in enumerate(cum_ret.columns):
                    s = cum_ret[tkr].dropna()
                    if not s.empty:
                        c = cmap(i % 20)
                        ax_rebase.plot(s.index, s.values, linewidth=1.0, color=c)
                        ax_rebase.text(s.index[-1], s.values[-1], f'  {tkr}',
                                       fontsize=5, color=c, va='center', clip_on=True)

                ax_rebase.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: f"{y:.0%}"))
                ax_rebase.axhline(0, color='#555555', linewidth=0.6)
                ax_rebase.grid(axis='y', alpha=0.2)
                locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
                ax_rebase.xaxis.set_major_locator(locator)
                ax_rebase.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
                plt.setp(ax_rebase.get_xticklabels(), rotation=45, ha='right', fontsize=7)
            else:
                ax_rebase.text(0.5, 0.5, 'No ticker data available',
                               ha='center', va='center', fontsize=12, transform=ax_rebase.transAxes)
                ax_rebase.axis('off')

            ax_rebase.set_title(f'{basket_name} — 1Y Cumulative Returns', fontsize=11, fontweight='bold')
            fig.suptitle(f'{basket_name} — Correlation & Performance', fontsize=12, fontweight='bold', y=0.98)
            fig.subplots_adjust(left=0.08, right=0.95, bottom=0.08, top=0.92)
            pdf.savefig(fig, dpi=150)
            plt.close(fig)

        plt.close('all')
        gc.collect()
        WriteThroughPath(report_path).sync()
        built += 1

        _pdf_pct = int((_pdf_i / _pdf_total) * 100) if _pdf_total else 100
        _pdf_ms = _pdf_pct // 10 * 10
        if _pdf_ms > _pdf_last_milestone and _pdf_ms % 10 == 0:
            print(f"  {_pdf_ms}% complete ({_pdf_i} / {_pdf_total} basket PDFs)")
            _pdf_last_milestone = _pdf_ms

    print(f"Basket report PDFs: built={built}, up_to_date={skipped}")


if __name__ == "__main__":
    load_or_build_signals()
    generate_summary_pdf()
    generate_basket_report_pdfs()

# %%
