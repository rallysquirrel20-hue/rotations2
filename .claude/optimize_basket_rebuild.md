# Optimize Basket Incremental Rebuild

**Created:** 2026-03-25
**Branch:** `optimize-incremental-rebuild`
**Status:** Implementation complete, pending user verification before merging to main

## Problem

When Norgate updates with 1-2 new trading days, the "incremental" basket path still recomputes correlation, contributions, and breadth pivots over the **full history** for all 27 baskets. This takes 10+ minutes when it should take under a minute.

The bottleneck is in `_finalize_basket_signals_output` (rotations.py), called from the incremental append path. Three operations recompute full history even for 1 new day:

| Step | Function | What it does now | Cost per basket |
|---|---|---|---|
| Correlation | `_compute_within_basket_correlation` | Rolling 21-day corr for ALL dates, ALL quarters | ~1.2s |
| Contributions | `_compute_and_save_contributions` | Full weight-drift + contributions for ALL quarters | ~0.6s |
| Breadth pivots | `compute_breadth_pivots` x2 | Stateful loop over full EMA arrays | ~0.1s |

## What Was Built

### New functions in `rotations.py` (on branch)

1. **`_compute_within_basket_correlation_incremental`** (after line ~4484)
   - Only computes rolling correlation for the specified new dates
   - Groups new dates by active quarter, extracts 21-day window slice, applies same z-score variance decomposition
   - **171x speedup** (1.2s → 0.007s per basket)

2. **`_compute_and_save_contributions_incremental`** (after line ~4810)
   - Loads existing contributions parquet, appends only new days
   - Chains weight drift across consecutive new dates (fixed multi-day bug)
   - Falls back to full recompute if no existing file
   - **6x speedup** (0.6s → 0.1s per basket)

3. **`_finalize_basket_signals_output`** — added `incremental_dates=None` parameter
   - When set: routes to incremental correlation and contributions functions
   - When None: existing full-recompute behavior (no regression)

4. **Incremental append path** (line ~5199) — computes `_new_dates` from `appended_ohlc` and passes to finalize

### Test files in `signals/`

| File | Purpose |
|---|---|
| `prep_test_data.py` | Creates `test_data/` with 3/24 stripped from all 30 basket signals, contributions, and meta files |
| `test_incremental_run.py` | Runs incremental rebuild on test_data for all 30 baskets, writes results, validates against production |
| `test_incremental_benchmark.py` | Benchmarks old (full) vs new (incremental) per-function timing with correctness validation |

### Test data in `signals/test_data/`

Pre-generated clones of all basket signal/contribution/meta files with 2026-03-24 stripped out. Ready for `test_incremental_run.py`.

## Benchmark Results

### Per-function (Information Technology basket, 1-day append)

| Step | Full recompute | Incremental | Speedup | Match? |
|---|---|---|---|---|
| Correlation | 1.12s | 0.007s | **171x** | Exact (diff=0) |
| Contributions | 0.59s | 0.09s | **6.6x** | Exact (diff~5e-17) |

### Full 30-basket test run (test_incremental_run.py)

| Metric | Value |
|---|---|
| Total wall time | 115s (includes data loading) |
| Total correlation time | **0.28s** (vs ~33s full) |
| Total contributions time | **2.07s** (vs ~12s full) |
| Total breadth pivots | 2.28s (still full recompute) |
| Validation | **ALL 30 BASKETS MATCH PRODUCTION** |

## Phases Not Yet Implemented

### Phase 2: Breadth Pivot State Caching (lower priority)

Refactor `compute_breadth_pivots` to accept/return a state dict (trends, resistance, support, rotation tracking variables). Cache state in meta JSON. Resume from cached state for new rows — reduces O(N) to O(1-2) per call. Currently ~0.1s per basket so not urgent.

### Phase 3: Basket-Level Parallelism (only if needed)

Wrap the sequential basket loop (lines ~5051-5062) in `ThreadPoolExecutor(max_workers=4)`. Each basket writes to separate files; shared data (`returns_matrix`, `all_signals_df`) is read-only. Would give ~2-3x on top of Phase 1. Not needed if Phase 1 meets the <1 min target.

## How to Verify and Merge

```bash
# 1. Switch to the branch
cd ~/Documents/Rotations
git checkout optimize-incremental-rebuild

# 2. Prep test data (strips 3/24 from all basket files into test_data/)
cd signals
python prep_test_data.py

# 3. Run the incremental rebuild on test data and validate
python test_incremental_run.py

# 4. (Optional) Run the per-function benchmark
python test_incremental_benchmark.py --days 1 --basket Information_Technology

# 5. If satisfied, merge to main
git checkout main
git merge optimize-incremental-rebuild
```

## Edge Cases to Watch

- **Quarter boundary:** If a new date is the first day of a new quarter, contributions must reset to new quarter weights (handled by the `_carried_q_key` check)
- **Missing contributions file:** Falls back to full `_compute_and_save_contributions`
- **Corrupted parquet:** Try/except with full-recompute fallback
- **Multi-day append (2+ days):** Contributions chain BOD weights across consecutive days (the `_carried_bod` fix)

## Key File Locations

- `signals/rotations.py` — Main pipeline with incremental functions (branch only)
- `signals/test_incremental_run.py` — Full 30-basket test runner
- `signals/test_incremental_benchmark.py` — Per-function timing benchmark
- `signals/prep_test_data.py` — Test data generator
- `signals/test_data/` — Pre-stripped basket files for testing
