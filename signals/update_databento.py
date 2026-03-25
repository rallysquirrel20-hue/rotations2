import os
import sys
import subprocess
from pathlib import Path

# Force the core rotations file to only run Databento (live intraday) updates
os.environ['ROTATIONS_RUN_MODE'] = 'DATABENTO'

script = str(Path(__file__).parent / "rotations.py")

if __name__ == "__main__":
    print("Starting Databento Live Update...")
    try:
        subprocess.run([sys.executable, script], check=True)
        print("Databento Live Update Complete.")
    except subprocess.CalledProcessError as exc:
        print(f"Error: Databento live update failed with code {exc.returncode}")
        sys.exit(exc.returncode)
