"""Live updates loop — runs live_updates.py every 5 minutes during market hours on trading days."""

import sys
import time
import subprocess
import traceback
from datetime import datetime, time as dt_time
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

ET = ZoneInfo("America/New_York")
SCRIPT = str(Path(__file__).with_name("live_updates.py"))
INTERVAL = 300  # 5 minutes
MARKET_OPEN = dt_time(9, 30)
MARKET_CLOSE = dt_time(16, 0)


def _build_trading_days():
    """Return set of all NYSE trading dates for the current year."""
    nyse = xcals.get_calendar("XNYS")
    year = datetime.now(ET).year
    sessions = nyse.sessions_in_range(f"{year}-01-01", f"{year}-12-31")
    return {s.date() for s in sessions}


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def main():
    trading_days = _build_trading_days()
    log(f"[loop_live] {len(trading_days)} trading days loaded for {datetime.now(ET).year}")
    cycle = 0

    while True:
        now = datetime.now(ET)
        today = now.date()
        current_time = now.time()

        if today in trading_days and MARKET_OPEN <= current_time <= MARKET_CLOSE:
            cycle += 1
            log(f"\n{'='*60}")
            log(f"[loop_live] Cycle {cycle} — {now:%Y-%m-%d %H:%M:%S} ET")
            log(f"{'='*60}")
            try:
                subprocess.run([sys.executable, SCRIPT], check=True)
            except subprocess.CalledProcessError as exc:
                log(f"[loop_live] Error: exit code {exc.returncode}")
            except BaseException:
                traceback.print_exc()
            log(f"[loop_live] Cycle {cycle} done at {datetime.now(ET):%H:%M:%S} ET")
            time.sleep(INTERVAL)
        else:
            # Outside market hours — sleep longer, check every 60s
            if today in trading_days and current_time < MARKET_OPEN:
                log(f"[loop_live] Pre-market, waiting for 9:30 ET... ({now:%H:%M})")
            time.sleep(60)

        # Rebuild schedule on Jan 1
        if now.month == 1 and now.day == 1 and now.hour == 0:
            trading_days = _build_trading_days()
            log(f"[loop_live] Refreshed: {len(trading_days)} trading days")


if __name__ == "__main__":
    main()
