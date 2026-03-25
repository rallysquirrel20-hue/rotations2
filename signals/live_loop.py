"""
Live signal auto-refresh loop.

Runs rotations.py every 15 minutes via runpy.run_path() so each
iteration gets a fresh namespace.  Cache guards in rotations.py
(is_*_current()) skip stale-free data; Cell 6's market-hours gate
(_get_live_update_gate()) no-ops outside Mon-Fri 9:25-16:15 ET.
"""

import sys
import time
import subprocess
import traceback
from datetime import datetime
from pathlib import Path

INTERVAL = 900  # 15 minutes

script = str(Path(__file__).with_name("rotations.py"))
cycle = 0


def log(msg):
    """Write to stderr so banners aren't swallowed by rotations.py's stdout timing wrapper."""
    print(msg, file=sys.stderr, flush=True)


while True:
    cycle += 1
    log(f"\n{'='*60}")
    log(f"[live_loop] Cycle {cycle} starting at {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"{'='*60}")
    try:
        # Use subprocess instead of runpy to force complete memory reclamation
        subprocess.run([sys.executable, script], check=True)
    except subprocess.CalledProcessError as exc:
        log(f"[live_loop] Error: Script exited with code {exc.returncode}")
    except BaseException as exc:
        traceback.print_exc()
        if isinstance(exc, KeyboardInterrupt):
            log("[live_loop] Stopping loop.")
            break
    log(f"[live_loop] Cycle {cycle} finished at {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"[live_loop] Sleeping {INTERVAL // 60} minutes...")
    time.sleep(INTERVAL)
