"""
Strategy Scanner — sweep backtest combinations and collect results.

Usage:
    1. Make sure the backend is running (python main.py)
    2. Run:  python strategy_scanner.py
    3. Results saved to  strategy_scan_results.csv  and  _full.json

Customize by editing the SCAN_CONFIG section below.
"""

import itertools, json, time, csv, sys, argparse
from datetime import datetime
from pathlib import Path

import requests

# ── connection ──────────────────────────────────────────────────────────────
API_BASE = "http://localhost:8000"

# ── SCAN CONFIG — edit these to control what gets swept ─────────────────────

# Date range for all backtests
START_DATE = "2015-01-01"
END_DATE   = None            # None = use all available data

# Signals to test (pick from: Breakout, Breakdown, Up_Rot, Down_Rot, BTFD, STFR, Buy_Hold)
SIGNALS = ["Breakout"]

# Target types: "basket" (aggregate) or "basket_tickers" (per-constituent)
TARGET_TYPES = ["ticker"]

# Which basket groups to include — set to False to skip
SCAN_SECTORS    = False
SCAN_THEMES     = False
SCAN_INDUSTRIES = False

# Individual tickers to test (used when TARGET_TYPES includes "ticker")
TICKERS = ["NVDA"]

# Position sizing / leverage combos to try
POSITION_SIZES = [i / 100 for i in range(1, 101)]  # 0.01 to 1.0 (1% to 100%)
MAX_LEVERAGES  = [1.0]

# ── Filter templates ────────────────────────────────────────────────────────
# Each entry is a list of filter dicts applied together.
# "NO_FILTER" = no regime filter (baseline).
FILTER_PRESETS = {
    "no_filter": [],

    "uptrend_above_50": [
        {"metric": "Uptrend_Pct", "condition": "above", "value": 50, "source": "self"}
    ],
    "uptrend_above_60": [
        {"metric": "Uptrend_Pct", "condition": "above", "value": 60, "source": "self"}
    ],
    "uptrend_above_70": [
        {"metric": "Uptrend_Pct", "condition": "above", "value": 70, "source": "self"}
    ],

    "uptrend_below_50": [
        {"metric": "Uptrend_Pct", "condition": "below", "value": 50, "source": "self"}
    ],
    "uptrend_below_30": [
        {"metric": "Uptrend_Pct", "condition": "below", "value": 30, "source": "self"}
    ],

    "breakout_above_20": [
        {"metric": "Breakout_Pct", "condition": "above", "value": 20, "source": "self"}
    ],
    "breakout_above_40": [
        {"metric": "Breakout_Pct", "condition": "above", "value": 40, "source": "self"}
    ],

    "trend_true": [
        {"metric": "Trend", "condition": "equals_true", "value": 0, "source": "self"}
    ],
    "trend_false": [
        {"metric": "Trend", "condition": "equals_false", "value": 0, "source": "self"}
    ],

    "low_vol": [
        {"metric": "RV_EMA", "condition": "below", "value": 0.03, "source": "self"}
    ],
    "high_vol": [
        {"metric": "RV_EMA", "condition": "above", "value": 0.03, "source": "self"}
    ],

    "uptrend_50_and_low_vol": [
        {"metric": "Uptrend_Pct", "condition": "above", "value": 50, "source": "self"},
        {"metric": "RV_EMA", "condition": "below", "value": 0.03, "source": "self"},
    ],
    "uptrend_increasing": [
        {"metric": "Uptrend_Pct", "condition": "increasing", "value": 0, "source": "self"}
    ],
    "uptrend_decreasing": [
        {"metric": "Uptrend_Pct", "condition": "decreasing", "value": 0, "source": "self"}
    ],
}

# Which filter presets to actually run (use list(FILTER_PRESETS.keys()) for all)
ACTIVE_FILTERS = ["no_filter"]


# ── Multi-leg portfolio templates (optional) ────────────────────────────────
# Each template is a list of partial leg dicts. The scanner fills in defaults.
# Set to [] to skip multi-leg scanning.
MULTI_LEG_TEMPLATES = [
    # Example: 50/50 long rotation on two sectors
    # {
    #     "name": "tech_fin_uprot",
    #     "legs": [
    #         {"target": "Information_Technology", "target_type": "basket", "entry_signal": "Up_Rot", "allocation_pct": 0.5},
    #         {"target": "Financials",             "target_type": "basket", "entry_signal": "Up_Rot", "allocation_pct": 0.5},
    #     ],
    #     "filters_per_leg": [[], []],  # per-leg filter lists
    # },
]

# ── Rate limiting ───────────────────────────────────────────────────────────
DELAY_BETWEEN_CALLS = 0.05   # seconds; increase if backend is slow


# ═══════════════════════════════════════════════════════════════════════════
#  No need to edit below here
# ═══════════════════════════════════════════════════════════════════════════

def fetch_baskets():
    """Get available baskets from the running backend."""
    r = requests.get(f"{API_BASE}/api/baskets", timeout=10)
    r.raise_for_status()
    data = r.json()
    baskets = []
    if SCAN_THEMES:
        baskets += [("theme", b) for b in data.get("Themes", [])]
    if SCAN_SECTORS:
        baskets += [("sector", b) for b in data.get("Sectors", [])]
    if SCAN_INDUSTRIES:
        baskets += [("industry", b) for b in data.get("Industries", [])]
    return baskets


def run_single_backtest(target, target_type, signal, filters, position_size, max_leverage):
    """POST to /api/backtest and return parsed response or None on error."""
    payload = {
        "target": target,
        "target_type": target_type,
        "entry_signal": signal,
        "filters": filters,
        "position_size": position_size,
        "max_leverage": max_leverage,
    }
    if START_DATE:
        payload["start_date"] = START_DATE
    if END_DATE:
        payload["end_date"] = END_DATE

    try:
        r = requests.post(f"{API_BASE}/api/backtest", json=payload, timeout=120)
        if r.status_code != 200:
            return None, r.text[:200]
        return r.json(), None
    except Exception as e:
        return None, str(e)


def run_multi_backtest(legs, start_date=None, end_date=None, max_leverage=2.5):
    """POST to /api/backtest/multi."""
    payload = {"legs": legs, "max_leverage": max_leverage}
    if start_date:
        payload["start_date"] = start_date
    if end_date:
        payload["end_date"] = end_date
    try:
        r = requests.post(f"{API_BASE}/api/backtest/multi", json=payload, timeout=120)
        if r.status_code != 200:
            return None, r.text[:200]
        return r.json(), None
    except Exception as e:
        return None, str(e)


def extract_stats(result, which="filtered"):
    """Pull flat stats dict from a single-backtest response."""
    stats = result.get("stats", {}).get(which, {})
    ec = result.get("equity_curve", {})
    curve = ec.get(which, [])

    # Compute CAGR from equity curve
    cagr = None
    if curve and len(curve) >= 2:
        final_eq = curve[-1]
        n_days = len(curve)
        years = n_days / 252
        if years > 0 and final_eq > 0:
            cagr = (final_eq ** (1 / years)) - 1

    return {
        "trades_met_criteria": stats.get("trades_met_criteria", stats.get("trades", 0)),
        "trades_taken":       stats.get("trades_taken", stats.get("trades", 0)),
        "trades_skipped":     stats.get("trades_skipped", 0),
        "trades":             stats.get("trades", 0),
        "win_rate":           stats.get("win_rate"),
        "avg_winner":         stats.get("avg_winner"),
        "avg_loser":          stats.get("avg_loser"),
        "ev":                 stats.get("ev"),
        "profit_factor":      stats.get("profit_factor"),
        "max_dd":             stats.get("max_dd"),
        "avg_bars":           stats.get("avg_bars"),
        "final_equity":       curve[-1] if curve else None,
        "cagr":               cagr,
    }


def extract_multi_stats(result):
    """Pull combined stats from a multi-backtest response."""
    combined = result.get("combined", {})
    cstats = combined.get("stats", {})
    pstats = cstats.get("portfolio", {})
    tstats = cstats.get("trade", {})
    ec = combined.get("equity_curve", {})
    curve = ec.get("combined", [])

    return {
        "strategy_return": pstats.get("strategy_return"),
        "cagr":            pstats.get("cagr"),
        "volatility":      pstats.get("volatility"),
        "max_dd":          pstats.get("max_dd"),
        "sharpe":          pstats.get("sharpe"),
        "sortino":         pstats.get("sortino"),
        "trades_taken":    tstats.get("trades_taken"),
        "win_rate":        tstats.get("win_rate"),
        "ev":              tstats.get("ev"),
        "profit_factor":   tstats.get("profit_factor"),
        "final_equity":    curve[-1] if curve else None,
    }


def build_combinations(baskets):
    """Generate all (target, signal, filter_name, pos_size, leverage) combos."""
    combos = []

    # Basket-based combos
    for (group, basket_name) in baskets:
        for signal in SIGNALS:
            for filter_name in ACTIVE_FILTERS:
                for ps in POSITION_SIZES:
                    for ml in MAX_LEVERAGES:
                        for tt in TARGET_TYPES:
                            if tt == "ticker":
                                continue  # tickers handled below
                            combos.append({
                                "group": group,
                                "target": basket_name,
                                "target_type": tt,
                                "signal": signal,
                                "filter_name": filter_name,
                                "filters": FILTER_PRESETS[filter_name],
                                "position_size": ps,
                                "max_leverage": ml,
                            })

    # Individual ticker combos
    if "ticker" in TARGET_TYPES:
        for ticker in TICKERS:
            for signal in SIGNALS:
                for filter_name in ACTIVE_FILTERS:
                    for ps in POSITION_SIZES:
                        for ml in MAX_LEVERAGES:
                            combos.append({
                                "group": "ticker",
                                "target": ticker,
                                "target_type": "ticker",
                                "signal": signal,
                                "filter_name": filter_name,
                                "filters": FILTER_PRESETS[filter_name],
                                "position_size": ps,
                                "max_leverage": ml,
                            })

    return combos


def main():
    parser = argparse.ArgumentParser(description="Backtest strategy scanner")
    parser.add_argument("--dry-run", action="store_true", help="Print combos without running")
    parser.add_argument("--limit", type=int, default=0, help="Max backtests to run (0=all)")
    parser.add_argument("--output", type=str, default="strategy_scan_results", help="Output filename prefix")
    args = parser.parse_args()

    print("Connecting to backend...")
    try:
        r = requests.get(f"{API_BASE}/", timeout=5)
        r.raise_for_status()
        print(f"  Backend OK: {r.json()}")
    except Exception as e:
        print(f"  ERROR: Cannot reach backend at {API_BASE}. Is it running?\n  {e}")
        sys.exit(1)

    print("Fetching available baskets...")
    baskets = fetch_baskets()
    print(f"  Found {len(baskets)} baskets")
    for g, names in itertools.groupby(sorted(baskets), key=lambda x: x[0]):
        names_list = list(names)
        print(f"    {g}: {len(names_list)}  ({', '.join(n[1] for n in names_list[:5])}{'...' if len(names_list) > 5 else ''})")

    # ── Single-leg sweep ────────────────────────────────────────────────────
    combos = build_combinations(baskets)
    total = len(combos) + len(MULTI_LEG_TEMPLATES)
    if args.limit:
        combos = combos[:args.limit]
    print(f"\nSingle-leg combinations: {len(combos)}  (of {total} total)")

    if args.dry_run:
        for c in combos[:20]:
            print(f"  {c['target']:30s}  {c['signal']:12s}  {c['filter_name']}")
        if len(combos) > 20:
            print(f"  ... and {len(combos) - 20} more")
        return

    # Prepare output files
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = Path(args.output).with_suffix(".csv")
    json_path = Path(f"{args.output}_full.json")

    csv_fields = [
        "group", "target", "target_type", "signal", "filter_name",
        "position_size", "max_leverage",
        "trades_met_criteria", "trades_taken", "trades_skipped", "trades",
        "unfiltered_trades", "filter_reduced_pct",
        "win_rate", "avg_winner", "avg_loser",
        "ev", "profit_factor", "max_dd", "avg_bars",
        "final_equity", "cagr", "error",
    ]

    filter_warnings = []  # track filters that had no effect

    full_results = []

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=csv_fields)
        writer.writeheader()

        for i, combo in enumerate(combos, 1):
            label = f"[{i}/{len(combos)}] {combo['target']} / {combo['signal']} / {combo['filter_name']}"
            print(f"  {label}", end="", flush=True)

            result, err = run_single_backtest(
                target=combo["target"],
                target_type=combo["target_type"],
                signal=combo["signal"],
                filters=combo["filters"],
                position_size=combo["position_size"],
                max_leverage=combo["max_leverage"],
            )

            row = {
                "group":         combo["group"],
                "target":        combo["target"],
                "target_type":   combo["target_type"],
                "signal":        combo["signal"],
                "filter_name":   combo["filter_name"],
                "position_size": combo["position_size"],
                "max_leverage":  combo["max_leverage"],
                "error":         err,
            }

            if result:
                stats_f = extract_stats(result, "filtered")
                stats_u = extract_stats(result, "unfiltered")
                row.update(stats_f)

                # Filter validation: compare filtered vs unfiltered trade counts
                f_trades = stats_f.get("trades", 0)
                u_trades = stats_u.get("trades", 0)
                row["unfiltered_trades"] = u_trades
                if u_trades > 0:
                    row["filter_reduced_pct"] = round((1 - f_trades / u_trades) * 100, 1)
                else:
                    row["filter_reduced_pct"] = 0

                # Flag filters that had zero effect
                if combo["filter_name"] != "no_filter" and f_trades == u_trades and u_trades > 0:
                    filter_warnings.append(f"  WARNING: filter '{combo['filter_name']}' had NO effect on {combo['target']}/{combo['signal']} ({f_trades}={u_trades} trades)")

                taken = stats_f.get("trades_taken", f_trades)
                skipped = stats_f.get("trades_skipped", 0)
                ev = stats_f.get("ev")
                ev_str = f"{ev:+.4f}" if ev is not None else "n/a"
                filt_str = f" [filter: {row['filter_reduced_pct']}% removed]" if combo["filter_name"] != "no_filter" else ""
                cagr_val = stats_f.get("cagr")
                cagr_str = f", CAGR={cagr_val:.1%}" if cagr_val is not None else ""
                dd_val = stats_f.get("max_dd")
                dd_str = f", MaxDD={dd_val:.1%}" if dd_val is not None else ""
                eq_val = stats_f.get("final_equity")
                eq_str = f", Eq={eq_val:.2f}" if eq_val is not None else ""
                print(f"  ->  signals={f_trades}, taken={taken}, skipped={skipped}, EV={ev_str}, PosSize={combo['position_size']:.0%}{cagr_str}{dd_str}{eq_str}{filt_str}")

                # Save full result for deep analysis
                full_results.append({
                    "config": combo,
                    "stats_filtered": stats_f,
                    "stats_unfiltered": stats_u,
                    "blew_up": result.get("blew_up"),
                })
            else:
                print(f"  ->  ERROR: {err[:80]}")

            writer.writerow(row)
            f.flush()
            time.sleep(DELAY_BETWEEN_CALLS)

    # ── Multi-leg sweep ─────────────────────────────────────────────────────
    multi_results = []
    for tmpl in MULTI_LEG_TEMPLATES:
        name = tmpl["name"]
        print(f"\n  Multi-leg: {name}", end="", flush=True)

        legs = []
        for j, leg_def in enumerate(tmpl["legs"]):
            leg = {
                "target":         leg_def["target"],
                "target_type":    leg_def.get("target_type", "basket"),
                "entry_signal":   leg_def["entry_signal"],
                "allocation_pct": leg_def.get("allocation_pct", 1.0 / len(tmpl["legs"])),
                "position_size":  leg_def.get("position_size", 1.0),
                "filters":        tmpl.get("filters_per_leg", [[]])[j] if j < len(tmpl.get("filters_per_leg", [])) else [],
            }
            legs.append(leg)

        result, err = run_multi_backtest(legs, start_date=START_DATE, end_date=END_DATE)
        if result:
            ms = extract_multi_stats(result)
            print(f"  ->  CAGR={ms.get('cagr', 'n/a')}, Sharpe={ms.get('sharpe', 'n/a')}")
            multi_results.append({"name": name, "legs": legs, "stats": ms})
        else:
            print(f"  ->  ERROR: {err[:80]}")
            multi_results.append({"name": name, "legs": legs, "error": err})

    # ── Save full JSON ──────────────────────────────────────────────────────
    with open(json_path, "w") as f:
        json.dump({
            "scan_time": ts,
            "config": {
                "start_date": START_DATE,
                "end_date": END_DATE,
                "signals": SIGNALS,
                "target_types": TARGET_TYPES,
                "active_filters": ACTIVE_FILTERS,
            },
            "single_leg": full_results,
            "multi_leg": multi_results,
        }, f, indent=2, default=str)

    # ── Summary ─────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Done!  {len(full_results)} successful / {len(combos)} attempted")
    print(f"  CSV:  {csv_path.resolve()}")
    print(f"  JSON: {json_path.resolve()}")

    # ── Filter validation report ────────────────────────────────────────────
    if filter_warnings:
        print(f"\n  FILTER VALIDATION — {len(filter_warnings)} filters had NO effect:")
        for w in filter_warnings[:30]:
            print(w)
        if len(filter_warnings) > 30:
            print(f"  ... and {len(filter_warnings) - 30} more (see JSON for full list)")
    else:
        print(f"\n  FILTER VALIDATION — All filters reduced trade counts as expected.")

    # Quick top-10 by EV
    ranked = sorted(
        [r for r in full_results if r["stats_filtered"].get("ev") is not None and r["stats_filtered"].get("trades", 0) >= 5],
        key=lambda r: r["stats_filtered"]["ev"],
        reverse=True,
    )
    if ranked:
        print(f"\n  Top 10 strategies by EV (min 5 trades):")
        print(f"  {'Target':<28} {'Signal':<12} {'Filter':<24} {'Trades':>6} {'WinRate':>8} {'EV':>8} {'PF':>6} {'MaxDD':>8} {'CAGR':>8}")
        print(f"  {'-'*28} {'-'*12} {'-'*24} {'-'*6} {'-'*8} {'-'*8} {'-'*6} {'-'*8} {'-'*8}")
        for r in ranked[:10]:
            c = r["config"]
            s = r["stats_filtered"]
            wr = f"{s['win_rate']:.1%}" if s.get("win_rate") is not None else "n/a"
            ev = f"{s['ev']:+.4f}" if s.get("ev") is not None else "n/a"
            pf = f"{s['profit_factor']:.2f}" if s.get("profit_factor") is not None else "n/a"
            dd = f"{s['max_dd']:.1%}" if s.get("max_dd") is not None else "n/a"
            ca = f"{s['cagr']:.1%}" if s.get("cagr") is not None else "n/a"
            print(f"  {c['target']:<28} {c['signal']:<12} {c['filter_name']:<24} {s['trades']:>6} {wr:>8} {ev:>8} {pf:>6} {dd:>8} {ca:>8}")

    # Bottom 10 (worst)
    if ranked and len(ranked) > 10:
        print(f"\n  Bottom 5 strategies by EV:")
        for r in ranked[-5:]:
            c = r["config"]
            s = r["stats_filtered"]
            wr = f"{s['win_rate']:.1%}" if s.get("win_rate") is not None else "n/a"
            ev = f"{s['ev']:+.4f}" if s.get("ev") is not None else "n/a"
            print(f"  {c['target']:<28} {c['signal']:<12} {c['filter_name']:<24} {s['trades']:>6} {wr:>8} {ev:>8}")


if __name__ == "__main__":
    main()
