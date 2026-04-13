"""Daily signals + baskets loop — runs build_signals.py, build_dividend_metrics.py,
then build_baskets.py at 5pm ET on trading days."""

import sys
import time
import subprocess
import traceback
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

ET = ZoneInfo("America/New_York")
SIGNALS_SCRIPT = str(Path(__file__).with_name("build_signals.py"))
DIVIDEND_METRICS_SCRIPT = str(Path(__file__).with_name("build_dividend_metrics.py"))
BASKETS_SCRIPT = str(Path(__file__).with_name("build_baskets.py"))
CHECK_INTERVAL = 300  # check every 5 minutes
RUN_HOUR = 17  # 5pm ET


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
    log(f"[loop_signals] {len(trading_days)} trading days loaded for {datetime.now(ET).year}")
    already_ran_today = None

    while True:
        now = datetime.now(ET)
        today = now.date()

        if today in trading_days and now.hour >= RUN_HOUR and already_ran_today != today:
            log(f"\n{'='*60}")
            log(f"[loop_signals] Starting daily signal + basket build — {now:%Y-%m-%d %H:%M:%S} ET")
            log(f"{'='*60}")
            try:
                log("[loop_signals] Running build_signals.py ...")
                subprocess.run([sys.executable, SIGNALS_SCRIPT], check=True)
                log("[loop_signals] Running build_dividend_metrics.py ...")
                subprocess.run([sys.executable, DIVIDEND_METRICS_SCRIPT], check=True)
                log("[loop_signals] Running build_baskets.py ...")
                subprocess.run([sys.executable, BASKETS_SCRIPT], check=True)
            except subprocess.CalledProcessError as exc:
                log(f"[loop_signals] Error: exit code {exc.returncode}")
            except BaseException:
                traceback.print_exc()
            already_ran_today = today
            log(f"[loop_signals] Finished at {datetime.now(ET):%H:%M:%S} ET")

        # Rebuild schedule on Jan 1
        if now.month == 1 and now.day == 1 and now.hour == 0:
            trading_days = _build_trading_days()
            log(f"[loop_signals] Refreshed: {len(trading_days)} trading days")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
