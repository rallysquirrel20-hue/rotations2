"""
Benchmark: measure actual backtest endpoint latency.
Run while the backend is serving on localhost:8000.

Usage:  python bench_backtest.py
"""

import time, requests, concurrent.futures, json, sys

API = "http://localhost:8000/api"

# ── Config: mirrors what the frontend sends for a single-leg basket_tickers backtest ──
TARGET = "High_Beta"
TARGET_TYPE = "basket_tickers"
POSITION_SIZE = 0.25
MAX_LEVERAGE = 2.5
SIGNALS = ["Breakout", "Breakdown", "Up_Rot", "Down_Rot", "BTFD", "STFR"]

def build_body(signal, equity_only=False):
    return {
        "legs": [{
            "target": TARGET,
            "target_type": TARGET_TYPE,
            "entry_signal": signal,
            "allocation_pct": 1.0,
            "position_size": POSITION_SIZE,
            "filters": [],
        }],
        "start_date": None,
        "end_date": None,
        "max_leverage": MAX_LEVERAGE,
        "equity_only": equity_only,
    }

def run_one(signal, equity_only=False, label=""):
    body = build_body(signal, equity_only)
    t0 = time.perf_counter()
    r = requests.post(f"{API}/backtest/multi", json=body, timeout=300)
    elapsed = time.perf_counter() - t0
    ok = r.status_code == 200
    size = len(r.content)
    if not ok:
        detail = r.text[:200]
        print(f"  {label}{signal:12s}  FAILED ({r.status_code}) {elapsed:6.1f}s  {detail}")
    else:
        print(f"  {label}{signal:12s}  OK     {elapsed:6.1f}s  {size/1024:.0f} KB")
    return signal, elapsed, ok

# ── Fetch date range to confirm backend is up ──
print("=" * 65)
print("BACKTEST BENCHMARK TIMING TEST")
print("=" * 65)

try:
    r = requests.get(f"{API}/date-range/basket_tickers/{TARGET}", timeout=10)
    r.raise_for_status()
    dr = r.json()
    print(f"Target: {TARGET} ({TARGET_TYPE})")
    print(f"Date range: {dr['min']} to {dr['max']}")
except Exception as e:
    print(f"ERROR: Backend not reachable or target invalid: {e}")
    sys.exit(1)

print()

# ── Test 1: Single main backtest (Breakout, full response) ──
print("-" * 65)
print("TEST 1: Single main backtest (Breakout, full response)")
print("-" * 65)
_, t_main, _ = run_one("Breakout", equity_only=False, label="[main] ")
print(f"  Main backtest time: {t_main:.1f}s")
print()

# ── Test 2: Single benchmark request (Breakout, equity_only) ──
print("-" * 65)
print("TEST 2: Single benchmark request (Breakout, equity_only=true)")
print("-" * 65)
_, t_eq, _ = run_one("Breakout", equity_only=True, label="[bench] ")
print(f"  equity_only savings: {t_main - t_eq:.1f}s ({(1 - t_eq/t_main)*100:.0f}% faster)")
print()

# ── Test 3: All 6 benchmarks SEQUENTIAL ──
print("-" * 65)
print("TEST 3: 6 benchmark signals SEQUENTIAL (equity_only=true)")
print("-" * 65)
t0 = time.perf_counter()
seq_times = []
for sig in SIGNALS:
    _, t, _ = run_one(sig, equity_only=True, label="[seq]  ")
    seq_times.append(t)
t_seq_total = time.perf_counter() - t0
print(f"  Sequential total: {t_seq_total:.1f}s  (sum of parts: {sum(seq_times):.1f}s)")
print(f"  Avg per signal:   {sum(seq_times)/len(seq_times):.1f}s")
print()

# ── Test 4: All 6 benchmarks PARALLEL ──
print("-" * 65)
print("TEST 4: 6 benchmark signals PARALLEL (equity_only=true)")
print("-" * 65)
t0 = time.perf_counter()
par_times = []
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
    futures = {pool.submit(run_one, sig, True, "[par]  "): sig for sig in SIGNALS}
    for f in concurrent.futures.as_completed(futures):
        sig, t, ok = f.result()
        par_times.append(t)
t_par_total = time.perf_counter() - t0
print(f"  Parallel wall time: {t_par_total:.1f}s  (slowest request: {max(par_times):.1f}s)")
print()

# ── Test 5: Main + 6 benchmarks PARALLEL (what the frontend does) ──
print("-" * 65)
print("TEST 5: 1 main + 6 benchmarks ALL PARALLEL (frontend pattern)")
print("-" * 65)
t0 = time.perf_counter()
all_times = []
with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
    futures = {}
    futures[pool.submit(run_one, "Breakout", False, "[MAIN] ")] = "main"
    for sig in SIGNALS:
        futures[pool.submit(run_one, sig, True, "[bench] ")] = sig
    for f in concurrent.futures.as_completed(futures):
        sig, t, ok = f.result()
        all_times.append(t)
t_all_total = time.perf_counter() - t0
print(f"  Total wall time (7 parallel): {t_all_total:.1f}s")
print(f"  Slowest single request:       {max(all_times):.1f}s")
print()

# ── Test 6: NEW batch endpoint ──
print("-" * 65)
print("TEST 6: /api/backtest/benchmarks (single batch request)")
print("-" * 65)
t0 = time.perf_counter()
bench_body = {
    "target": TARGET,
    "target_type": TARGET_TYPE,
    "position_size": POSITION_SIZE,
    "max_leverage": MAX_LEVERAGE,
    "start_date": None,
    "end_date": None,
}
r = requests.post(f"{API}/backtest/benchmarks", json=bench_body, timeout=300)
t_batch = time.perf_counter() - t0
if r.status_code == 200:
    data = r.json()
    sigs = list(data.get("benchmarks", {}).keys())
    timings = data.get("timings", {})
    print(f"  Status: OK  {t_batch:.1f}s  {len(r.content)/1024:.0f} KB")
    print(f"  Signals returned: {sigs}")
    print(f"  Backend timings: load={timings.get('load', '?')}s  per_signal={timings.get('per_signal', {})}  total={timings.get('total', '?')}s")
else:
    print(f"  FAILED ({r.status_code}): {r.text[:300]}")
    t_batch = float('inf')
print()

# ── Summary ──
print("=" * 65)
print("SUMMARY")
print("=" * 65)
print(f"  Single main backtest:            {t_main:6.1f}s")
print(f"  6 benchmarks sequential:         {t_seq_total:6.1f}s")
print(f"  6 benchmarks parallel:           {t_par_total:6.1f}s")
print(f"  7 requests parallel (old frontend):{t_all_total:5.1f}s")
print(f"  Batch endpoint (new):            {t_batch:6.1f}s")
print()
print(f"  Batch speedup vs sequential: {t_seq_total/t_batch:.1f}x")
print(f"  Batch speedup vs parallel:   {t_par_total/t_batch:.1f}x")
print(f"  New total (main + batch):    {t_main + t_batch:.1f}s  (was {t_all_total:.1f}s)")
