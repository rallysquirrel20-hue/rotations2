"""
databento_test.py — Standalone diagnostic script for Databento API access.

Tests:
  1. Historical ohlcv-1d via EQUS.SUMMARY (previous day EOD bar for SPY)
  2. Historical ohlcv-1m via EQUS.MINI aggregated to running OHLC for today
  3. Live ohlcv-1d via EQUS.MINI (partial intraday bar — confirmed supported)
  4. Live mbp-1 via EQUS.MINI with start=0 replay (derive mid from bid/ask)
  5. Live mbp-1 via EQUS.MINI with snapshot=True (instant current book, no replay)

Run: python databento_test.py

Key DBN facts (databento/dbn repo):
  - All prices are int64 nanodollars; divide by FIXED_PRICE_SCALE (1e9) for dollars
  - OHLCVMsg.pretty_open/high/low/close return pre-scaled floats directly
  - UNDEF_PRICE = 2^63-1 (i64::MAX) is the null sentinel for prices
  - MBP1Msg.levels[0].bid_px / .ask_px are int64 nanodollars
  - stype_out_symbol, err, msg fields are bytes — must decode + rstrip null bytes
  - SymbolMappingMsg arrives first on any Live subscription (instrument_id mapping)
  - start=0 on Live subscribe = intraday replay from Unix epoch (treated as "today")
  - snapshot=True = deliver current book state immediately without replay
"""

import os
import sys
import time
import threading
from datetime import timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# Env loading — mirrors rotations_parquet.py pattern
# ---------------------------------------------------------------------------

def _load_env():
    base = Path(__file__).resolve().parent
    candidates = [
        base / ".env",
        base.parent / "Python_Files" / ".env",
        base.parent / ".env",
    ]
    for p in candidates:
        if p.exists():
            load_dotenv(p, override=False)
            print(f"[env] Loaded: {p}")
            return
    print("[env] WARNING: No .env file found — relying on existing environment variables")


_load_env()

DATABENTO_API_KEY = os.getenv("DATABENTO_API_KEY")
DATABENTO_DATASET = os.getenv("DATABENTO_DATASET", "EQUS.MINI")

if not DATABENTO_API_KEY:
    print("ERROR: DATABENTO_API_KEY not set. Cannot continue.")
    sys.exit(1)

print(f"[env] DATABENTO_API_KEY = {'*' * 8}{DATABENTO_API_KEY[-4:]}")
print(f"[env] DATABENTO_DATASET = {DATABENTO_DATASET}")
print()

import databento as db  # noqa: E402  (import after env load)


# ---------------------------------------------------------------------------
# Constants — use canonical databento_dbn values where available
# ---------------------------------------------------------------------------

try:
    from databento_dbn import UNDEF_PRICE, FIXED_PRICE_SCALE
    PX_NULL = UNDEF_PRICE           # 2^63-1 (i64::MAX), null sentinel
    PX_SCALE = 1.0 / FIXED_PRICE_SCALE  # 1e-9, nanodollars → dollars
    print(f"[constants] Imported UNDEF_PRICE={PX_NULL}, FIXED_PRICE_SCALE={FIXED_PRICE_SCALE}")
except ImportError:
    PX_NULL = 2**63 - 1
    PX_SCALE = 1e-9
    print("[constants] databento_dbn constants not importable — using hardcoded fallbacks")

PASS = "PASS"
FAIL = "FAIL"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _result_line(label, status, detail=""):
    marker = "✓" if status == PASS else "✗"
    print(f"  [{marker}] {label}: {status}" + (f" — {detail}" if detail else ""))


def _decode(val) -> str:
    """Safely decode bytes fields from DBN records (stype_out_symbol, err, msg)."""
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", errors="replace").rstrip("\x00").strip()
    return str(val) if val is not None else "?"


def _instrument_id(msg) -> int:
    """Get instrument_id — accessible directly or via .hd on some message types."""
    direct = getattr(msg, "instrument_id", None)
    if direct is not None:
        return direct
    hd = getattr(msg, "hd", None)
    return getattr(hd, "instrument_id", -1) if hd else -1


def _ts_event_ns(msg) -> int:
    """Get ts_event nanoseconds — accessible directly or via .hd."""
    direct = getattr(msg, "ts_event", None)
    if direct is not None:
        return direct
    hd = getattr(msg, "hd", None)
    return getattr(hd, "ts_event", 0) if hd else 0


def _ts_et(msg) -> pd.Timestamp:
    """Convert msg ts_event to Eastern time. Uses pretty_ts_event if available."""
    pretty = getattr(msg, "pretty_ts_event", None)
    if pretty is not None:
        ts = pd.Timestamp(pretty)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.tz_convert("America/New_York")
    return pd.Timestamp(_ts_event_ns(msg), unit="ns", tz="UTC").tz_convert("America/New_York")


def _ohlcv_prices(msg) -> tuple:
    """Extract (open, high, low, close, volume) from an OHLCVMsg as floats."""
    # pretty_* properties are pre-scaled floats — use them if available
    if hasattr(msg, "pretty_open") and msg.pretty_open is not None:
        return (
            float(msg.pretty_open),
            float(msg.pretty_high),
            float(msg.pretty_low),
            float(msg.pretty_close),
            getattr(msg, "volume", None),
        )
    # Fall back to raw int64 nanodollar fields
    o = getattr(msg, "open", None)
    h = getattr(msg, "high", None)
    l = getattr(msg, "low", None)
    c = getattr(msg, "close", None)
    v = getattr(msg, "volume", None)
    if o is not None and isinstance(o, int) and o > 1_000_000:
        o, h, l, c = o * PX_SCALE, h * PX_SCALE, l * PX_SCALE, c * PX_SCALE
    return o, h, l, c, v


def _scale_df_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Scale OHLC columns in a DataFrame from nanodollars to dollars if needed."""
    for col in ("open", "high", "low", "close"):
        if col in df.columns and len(df) > 0 and df[col].iloc[0] > 1_000_000:
            df = df.copy()
            df[col] = df[col] * PX_SCALE
            break  # all columns scale together
    for col in ("open", "high", "low", "close"):
        if col in df.columns and len(df) > 0 and df[col].iloc[0] > 1_000_000:
            df[col] = df[col] * PX_SCALE
    return df


def _live_run(subscribe_kwargs: dict, on_msg_fn, timeout: int) -> tuple[list, list, list]:
    """
    Run a Live subscription in a daemon thread.
    Returns (error_msgs, all_msgs, []) — on_msg_fn handles result side-effects.
    """
    errors = []
    all_msgs = []

    def _run():
        try:
            live = db.Live()
            live.subscribe(**subscribe_kwargs)

            def _cb(msg):
                all_msgs.append(type(msg).__name__)
                on_msg_fn(msg)

            def _exc_cb(exc):
                # Retrieve the exception so asyncio doesn't warn "Future exception never retrieved"
                errors.append(str(exc))

            live.add_callback(_cb, _exc_cb)
            live.start()
            deadline = time.time() + timeout
            while time.time() < deadline:
                time.sleep(0.05)
            try:
                live.stop()
            except Exception:
                pass
        except Exception as e:
            errors.append(str(e))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout + 8)
    return errors, all_msgs


# ---------------------------------------------------------------------------
# Schema listing
# ---------------------------------------------------------------------------

def print_available_schemas():
    print("=" * 60)
    print("Available schemas for EQUS.MINI")
    print("=" * 60)
    try:
        client = db.Historical(DATABENTO_API_KEY)
        schemas = client.metadata.list_schemas(dataset=DATABENTO_DATASET)
        for s in schemas:
            print(f"  {s}")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()

    print("=" * 60)
    print("Available schemas for EQUS.SUMMARY")
    print("=" * 60)
    try:
        client = db.Historical(DATABENTO_API_KEY)
        schemas = client.metadata.list_schemas(dataset="EQUS.SUMMARY")
        for s in schemas:
            print(f"  {s}")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()


# ---------------------------------------------------------------------------
# Test 1 — Historical ohlcv-1d via EQUS.SUMMARY (previous EOD bar)
# ---------------------------------------------------------------------------

def test1_historical_ohlcv_1d():
    print("=" * 60)
    print("TEST 1 — Historical ohlcv-1d via EQUS.SUMMARY")
    print("=" * 60)
    try:
        client = db.Historical(DATABENTO_API_KEY)
        today = pd.Timestamp.now(tz="America/New_York").normalize()
        yesterday = today - timedelta(days=1)
        while yesterday.dayofweek >= 5:
            yesterday -= timedelta(days=1)

        print(f"  Requesting SPY ohlcv-1d from {yesterday.date()} to {today.date()}")

        data = client.timeseries.get_range(
            dataset="EQUS.SUMMARY",
            schema="ohlcv-1d",
            symbols=["SPY"],
            start=yesterday.date().isoformat(),
            end=today.date().isoformat(),
        )
        df = data.to_df()
        print(f"  Rows returned: {len(df)}")
        print(f"  Columns: {list(df.columns)}")

        if len(df) == 0:
            _result_line("Test 1", FAIL, "No rows returned")
            return

        df = _scale_df_prices(df)
        row = df.iloc[-1]
        o, h, l, c = row["open"], row["high"], row["low"], row["close"]
        v = row.get("volume", "N/A")
        print(f"  SPY OHLC: O={o:.4f}  H={h:.4f}  L={l:.4f}  C={c:.4f}  V={v}")
        _result_line("Test 1", PASS, f"close={c:.4f}")
    except Exception as e:
        _result_line("Test 1", FAIL, str(e))
    print()


# ---------------------------------------------------------------------------
# Test 2 — Historical ohlcv-1m via EQUS.MINI aggregated to running OHLC
# ---------------------------------------------------------------------------

def test2_historical_ohlcv_1m_aggregate():
    print("=" * 60)
    print("TEST 2 — Historical ohlcv-1m via EQUS.MINI (aggregated to OHLC)")
    print("=" * 60)
    try:
        client = db.Historical(DATABENTO_API_KEY)
        now_et = pd.Timestamp.now(tz="America/New_York")
        today_open_et = now_et.normalize().replace(hour=9, minute=30)
        today_open_utc = today_open_et.tz_convert("UTC")
        now_utc = now_et.tz_convert("UTC")

        # If before market open today, use yesterday's full session
        if now_et < today_open_et:
            yesterday = now_et.normalize() - timedelta(days=1)
            while yesterday.dayofweek >= 5:
                yesterday -= timedelta(days=1)
            today_open_utc = yesterday.replace(hour=9, minute=30).tz_convert("UTC")
            now_utc = yesterday.replace(hour=16, minute=0).tz_convert("UTC")
            print(f"  Market not yet open — using {yesterday.date()} session")
        else:
            # Historical API lags real-time by ~10-15 min; cap end at available range.
            # get_dataset_range() returns a dict, not an object — use key access.
            try:
                dataset_range = client.metadata.get_dataset_range(dataset=DATABENTO_DATASET)
                end_val = dataset_range["end"] if isinstance(dataset_range, dict) else dataset_range.end
                available_end = pd.Timestamp(end_val, tz="UTC")
                if available_end < now_utc:
                    print(f"  Historical data available to: {available_end} (capping end)")
                    now_utc = available_end
            except Exception as meta_err:
                now_utc = now_utc - timedelta(minutes=15)
                print(f"  metadata.get_dataset_range failed ({meta_err}); capping end at now-15min: {now_utc}")

        print(f"  Requesting SPY ohlcv-1m from {today_open_utc} to {now_utc}")

        data = client.timeseries.get_range(
            dataset=DATABENTO_DATASET,
            schema="ohlcv-1m",
            symbols=["SPY"],
            start=today_open_utc.isoformat(),
            end=now_utc.isoformat(),
        )
        df = data.to_df()
        print(f"  Rows (1m bars) returned: {len(df)}")

        if len(df) == 0:
            _result_line("Test 2", FAIL, "No rows returned")
            return

        df = _scale_df_prices(df)
        agg_o = df["open"].iloc[0]
        agg_h = df["high"].max()
        agg_l = df["low"].min()
        agg_c = df["close"].iloc[-1]
        agg_v = df["volume"].sum() if "volume" in df.columns else "N/A"

        print(f"  Aggregated OHLC: O={agg_o:.4f}  H={agg_h:.4f}  L={agg_l:.4f}  C={agg_c:.4f}  V={agg_v}")
        _result_line("Test 2", PASS, f"from {len(df)} 1m bars, close={agg_c:.4f}")
    except Exception as e:
        _result_line("Test 2", FAIL, str(e))
    print()


# ---------------------------------------------------------------------------
# Test 3 — Live ohlcv-1d via EQUS.MINI (partial intraday bar via replay)
# ---------------------------------------------------------------------------

def test3_live_ohlcv_1d():
    print("=" * 60)
    print("TEST 3 — Live ohlcv-1d via EQUS.MINI (intraday partial bar)")
    print("=" * 60)
    timeout = 15
    by_type: dict = {}
    connect_error = []

    def on_msg(msg):
        name = type(msg).__name__
        by_type.setdefault(name, []).append(msg)
        if isinstance(msg, db.ErrorMsg):
            print(f"  [ErrorMsg] {_decode(getattr(msg, 'err', b''))}")
        elif isinstance(msg, db.SystemMsg):
            print(f"  [SystemMsg] {_decode(getattr(msg, 'msg', b''))}")
        elif isinstance(msg, db.SymbolMappingMsg):
            sym = _decode(getattr(msg, "stype_out_symbol", b""))
            iid = _instrument_id(msg)
            print(f"  SymbolMappingMsg: instrument_id={iid} → '{sym}'")

    def _run():
        try:
            live = db.Live()
            live.subscribe(
                dataset=DATABENTO_DATASET,
                schema="ohlcv-1d",
                symbols=["SPY"],
                # start=0 = Unix nanosecond epoch; delivers the most recent completed
                # daily bar first, then the current partial bar once it aggregates.
                # NOTE: the first OHLCVMsg may be from the previous session, not today.
                start=0,
            )
            live.add_callback(on_msg)
            live.start()
            deadline = time.time() + timeout
            while time.time() < deadline and not by_type:
                time.sleep(0.05)
            try:
                live.stop()
            except Exception:
                pass
        except Exception as e:
            connect_error.append(str(e))

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout + 8)

    if connect_error:
        _result_line("Test 3", FAIL, connect_error[0])
        print()
        return

    print(f"  Message types received: {list(by_type.keys())}")
    ohlcv_msgs = by_type.get("OHLCVMsg", [])
    if ohlcv_msgs:
        msg = ohlcv_msgs[0]
        o, h, l, c, v = _ohlcv_prices(msg)
        ts = _ts_et(msg)
        print(f"  OHLCVMsg timestamp (ET): {ts}")
        if o is not None:
            print(f"  SPY OHLC: O={o:.4f}  H={h:.4f}  L={l:.4f}  C={c:.4f}  V={v}")
        else:
            print(f"  WARNING: OHLC fields returned None — check field names on OHLCVMsg")
        print(f"  Total OHLCV msgs received: {len(ohlcv_msgs)}")
        # Check whether this bar is today's session or a stale previous-day bar
        today_et = pd.Timestamp.now(tz="America/New_York").normalize()
        bar_date = ts.normalize() if ts.tzinfo else pd.Timestamp(ts).tz_localize("America/New_York").normalize()
        is_today = bar_date >= today_et
        if not is_today:
            print(f"  WARNING: Bar date {bar_date.date()} != today {today_et.date()}")
            print(f"           start=0 delivered previous session's EOD bar, not today's partial bar.")
            print(f"           This OHLC is stale — not suitable for live intraday price.")
            _result_line("Test 3", FAIL, f"stale bar ({bar_date.date()}) — ohlcv-1d live gives prev EOD, not today")
        else:
            _result_line("Test 3", PASS, f"today's bar — close={c:.4f}")
    elif "SymbolMappingMsg" in by_type and "ErrorMsg" not in by_type:
        _result_line("Test 3", FAIL, "Subscription connected + symbol mapped, but no OHLCV bars delivered")
    elif "ErrorMsg" in by_type:
        _result_line("Test 3", FAIL, "API ErrorMsg received (see above)")
    else:
        _result_line("Test 3", FAIL, f"No OHLCV data in {timeout}s")
    print()


# ---------------------------------------------------------------------------
# Test 4 — Live mbp-1, start=0 replay (derive mid from bid/ask)
# ---------------------------------------------------------------------------

def test4_live_mbp1_replay():
    print("=" * 60)
    print("TEST 4 — Live mbp-1 via EQUS.MINI, start=today open (intraday replay)")
    print("=" * 60)
    # Use today's market open (9:30 ET) as start rather than 0.
    # start=0 (Unix epoch) causes replay of the entire day from midnight UTC —
    # 461k+ messages on EQUS.MINI, too slow to reach live within any reasonable timeout.
    # Starting from market open dramatically reduces replay volume.
    now_et = pd.Timestamp.now(tz="America/New_York")
    today_open_et = now_et.normalize().replace(hour=9, minute=30)
    if now_et < today_open_et:
        # Before market open — use yesterday's open
        yesterday = now_et.normalize() - timedelta(days=1)
        while yesterday.dayofweek >= 5:
            yesterday -= timedelta(days=1)
        today_open_et = yesterday.replace(hour=9, minute=30, tzinfo=today_open_et.tzinfo)
    start_ns = int(today_open_et.timestamp() * 1e9)
    print(f"  Replay start: {today_open_et} ({start_ns} ns)")

    timeout = 30
    result = {}
    errors = []

    def on_msg(msg):
        if isinstance(msg, db.SymbolMappingMsg):
            sym = _decode(getattr(msg, "stype_out_symbol", b""))
            iid = _instrument_id(msg)
            result.setdefault("symbol_maps", []).append(sym)
            print(f"  SymbolMappingMsg: instrument_id={iid} → '{sym}'")
        elif isinstance(msg, db.ErrorMsg):
            err = _decode(getattr(msg, "err", b""))
            errors.append(f"ErrorMsg: {err}")
            print(f"  [ErrorMsg] {err}")
        elif isinstance(msg, db.SystemMsg):
            print(f"  [SystemMsg] {_decode(getattr(msg, 'msg', b''))}")
        elif isinstance(msg, db.MBP1Msg):
            bid = msg.levels[0].bid_px
            ask = msg.levels[0].ask_px
            if bid != PX_NULL and ask != PX_NULL and bid > 0 and ask > 0:
                # Keep overwriting — we want the most recent quote reached by timeout,
                # not just the first one (which would be the replay start price)
                result["mid"] = (bid + ask) * PX_SCALE * 0.5
                result["bid"] = bid * PX_SCALE
                result["ask"] = ask * PX_SCALE
                result["ts"] = _ts_et(msg)

    errors_conn, all_type_names = _live_run(
        subscribe_kwargs=dict(
            dataset=DATABENTO_DATASET,
            schema="mbp-1",
            symbols=["SPY"],
            start=start_ns,
        ),
        on_msg_fn=on_msg,
        timeout=timeout,
    )

    errors = errors_conn + errors
    unique_types = list(dict.fromkeys(all_type_names))
    print(f"  Message types received: {unique_types}")
    print(f"  Total messages: {len(all_type_names)}")

    if errors and "mid" not in result:
        _result_line("Test 4", FAIL, errors[0])
    elif "mid" in result:
        r = result
        now_et = pd.Timestamp.now(tz="America/New_York")
        lag_s = (now_et - r["ts"]).total_seconds()
        stale = lag_s > 120  # stale if price is more than 2 minutes behind wall clock
        print(f"  SPY mid: {r['mid']:.4f}  (bid={r['bid']:.4f}, ask={r['ask']:.4f})")
        print(f"  Timestamp (ET): {r['ts']}  (lag: {lag_s:.0f}s)" + (" ← STALE" if stale else " ← live"))
        status = PASS if not stale else FAIL
        _result_line("Test 4", status, f"mid={r['mid']:.4f}" + (f" (lag={lag_s:.0f}s — increase timeout)" if stale else f" (lag={lag_s:.0f}s)"))
    elif len(all_type_names) == 0:
        _result_line("Test 4", FAIL, "No messages at all — connection or subscription issue")
    elif result.get("symbol_maps"):
        _result_line("Test 4", FAIL, f"Symbol mapped but no valid bid/ask in {timeout}s — market closed or one-sided book")
    else:
        _result_line("Test 4", FAIL, f"{len(all_type_names)} msgs received but no usable MBP1Msg")
    print()


# ---------------------------------------------------------------------------
# Test 5 — Live mbp-1, snapshot=True (instant current book, no replay lag)
# ---------------------------------------------------------------------------

def test5_live_mbp1_snapshot():
    print("=" * 60)
    print("TEST 5 — Live mbp-1 via EQUS.MINI, snapshot=True")
    print("         (KNOWN UNSUPPORTED on EQUS.MINI — kept to confirm error message)")
    print("=" * 60)
    timeout = 10  # fails fast with ErrorMsg
    result = {}
    errors = []

    def on_msg(msg):
        if isinstance(msg, db.SymbolMappingMsg):
            sym = _decode(getattr(msg, "stype_out_symbol", b""))
            iid = _instrument_id(msg)
            result["mapped_symbol"] = sym
            print(f"  SymbolMappingMsg: instrument_id={iid} → '{sym}'")
        elif isinstance(msg, db.ErrorMsg):
            err = _decode(getattr(msg, "err", b""))
            errors.append(f"ErrorMsg: {err}")
            print(f"  [ErrorMsg] {err}")
        elif isinstance(msg, db.SystemMsg):
            print(f"  [SystemMsg] {_decode(getattr(msg, 'msg', b''))}")
        elif isinstance(msg, db.MBP1Msg):
            bid = msg.levels[0].bid_px
            ask = msg.levels[0].ask_px
            if bid != PX_NULL and ask != PX_NULL and bid > 0 and ask > 0:
                result["mid"] = (bid + ask) * PX_SCALE * 0.5
                result["bid"] = bid * PX_SCALE
                result["ask"] = ask * PX_SCALE
                result["ts"] = _ts_et(msg)

    errors_conn, all_type_names = _live_run(
        subscribe_kwargs=dict(
            dataset=DATABENTO_DATASET,
            schema="mbp-1",
            symbols=["SPY"],
            snapshot=True,  # current state only, no replay; cannot combine with start=
        ),
        on_msg_fn=on_msg,
        timeout=timeout,
    )

    errors = errors_conn + errors
    unique_types = list(dict.fromkeys(all_type_names))
    print(f"  Message types received: {unique_types}")
    print(f"  Total messages: {len(all_type_names)}")

    if errors and "mid" not in result:
        _result_line("Test 5", FAIL, errors[0])
    elif "mid" in result:
        r = result
        print(f"  SPY mid: {r['mid']:.4f}  (bid={r['bid']:.4f}, ask={r['ask']:.4f})")
        print(f"  Timestamp (ET): {r['ts']}")
        _result_line("Test 5", PASS, f"mid={r['mid']:.4f}")
    elif len(all_type_names) == 0:
        _result_line("Test 5", FAIL, "No messages — connection or subscription issue")
    elif result.get("mapped_symbol"):
        _result_line("Test 5", FAIL, f"Symbol mapped but no bid/ask — market closed or one-sided book")
    else:
        _result_line("Test 5", FAIL, f"{len(all_type_names)} msgs but no usable MBP1Msg")
    print()


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def main():
    print()
    print("=" * 60)
    print("DATABENTO API DIAGNOSTIC")
    print(f"Dataset under test: {DATABENTO_DATASET}")
    print("=" * 60)
    print()

    print_available_schemas()
    test1_historical_ohlcv_1d()
    test2_historical_ohlcv_1m_aggregate()
    test3_live_ohlcv_1d()
    test4_live_mbp1_replay()
    test5_live_mbp1_snapshot()

    print("=" * 60)
    print("Done. See PASS/FAIL markers above.")
    print()
    print("Interpretation:")
    print("  T1 PASS  → Historical EOD close works (EQUS.SUMMARY ohlcv-1d)")
    print("  T2 PASS  → Historical 1m bars work; aggregate for intraday OHLC")
    print("  T2 note  → metadata.get_dataset_range() returns dict, use ['end'] not .end")
    print("  T3 PASS (today's date) → Live ohlcv-1d has today's partial bar")
    print("  T3 FAIL (stale date)   → start=0 delivers prev EOD bar, not today's data")
    print("  T4 PASS (live ts)   → mbp-1 replay from market open caught up; mid is current")
    print("  T4 FAIL (stale ts)  → Replay still behind; increase timeout or narrow start")
    print("  T4 FAIL (no bid/ask)→ Market closed; one-sided book")
    print("  T5 FAIL  → snapshot=True not supported on EQUS.MINI (confirmed)")
    print("=" * 60)


if __name__ == "__main__":
    main()
