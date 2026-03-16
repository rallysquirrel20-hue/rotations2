"""
Live signal auto-refresh loop.

Runs rotations.py every 15 minutes via runpy.run_path() so each
iteration gets a fresh namespace.  Cache guards in rotations.py
(is_*_current()) skip stale-free data; Cell 6's market-hours gate
(_get_live_update_gate()) no-ops outside Mon-Fri 9:25-16:15 ET.
"""

import time
import runpy
import traceback
from pathlib import Path

INTERVAL = 900  # 15 minutes

script = str(Path(__file__).with_name("rotations.py"))

while True:
    try:
        runpy.run_path(script, run_name="__main__")
    except Exception:
        traceback.print_exc()
    time.sleep(INTERVAL)
