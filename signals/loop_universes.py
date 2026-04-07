"""Universe builder loop — runs build_universes.py on the last NYSE trading day of each quarter."""

import sys
import time
import subprocess
import traceback
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import exchange_calendars as xcals

ET = ZoneInfo("America/New_York")
SCRIPT = str(Path(__file__).with_name("build_universes.py"))
CHECK_INTERVAL = 3600  # check every hour
RUN_HOUR = 17  # run at 5pm ET


def _build_schedule():
    """Return the set of dates (last trading day of each quarter) for the current year."""
    nyse = xcals.get_calendar("XNYS")
    year = datetime.now(ET).year
    quarter_ends = [
        date(year, 3, 31),
        date(year, 6, 30),
        date(year, 9, 30),
        date(year, 12, 31),
    ]
    run_dates = set()
    for qe in quarter_ends:
        sessions = nyse.sessions_in_range(
            date(year, qe.month - 2, 1).isoformat(),
            qe.isoformat(),
        )
        if len(sessions):
            run_dates.add(sessions[-1].date())
    return run_dates


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def main():
    run_dates = _build_schedule()
    log(f"[loop_universes] Quarter-end trading dates for {datetime.now(ET).year}: {sorted(run_dates)}")
    already_ran_today = None

    while True:
        now = datetime.now(ET)
        today = now.date()

        if today in run_dates and now.hour >= RUN_HOUR and already_ran_today != today:
            log(f"\n{'='*60}")
            log(f"[loop_universes] Running build_universes.py — {now:%Y-%m-%d %H:%M:%S} ET")
            log(f"{'='*60}")
            try:
                subprocess.run([sys.executable, SCRIPT], check=True)
            except subprocess.CalledProcessError as exc:
                log(f"[loop_universes] Error: exit code {exc.returncode}")
            except BaseException:
                traceback.print_exc()
            already_ran_today = today
            log(f"[loop_universes] Finished at {datetime.now(ET):%H:%M:%S} ET")

        # Rebuild schedule on Jan 1
        if now.month == 1 and now.day == 1 and now.hour == 0:
            run_dates = _build_schedule()
            log(f"[loop_universes] Refreshed schedule: {sorted(run_dates)}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
