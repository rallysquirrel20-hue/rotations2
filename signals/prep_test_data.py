"""
Prepare test data for incremental rebuild verification.

Copies all basket signals, contributions, and meta files to a test directory,
stripping the last trading day (2026-03-24) from signals and contributions
and rewinding the meta JSON dates to 2026-03-23.

Usage:
    python prep_test_data.py
"""

import json
import shutil
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

STRIP_DATE = pd.Timestamp("2026-03-24").normalize()
REWIND_DATE = "2026-03-23"

DATA_FOLDER = Path.home() / "Documents" / "Python_Outputs" / "Data_Storage"
TEST_ROOT = Path(__file__).parent / "test_data"

BASKET_CACHE_DIRS = {
    "sector": DATA_FOLDER / "sector_basket_cache",
    "thematic": DATA_FOLDER / "thematic_basket_cache",
    "industry": DATA_FOLDER / "industry_basket_cache",
}


def strip_parquet(src: Path, dst: Path, date_col="Date"):
    """Copy parquet, removing rows where date_col == STRIP_DATE."""
    df = pd.read_parquet(src)
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    before = len(df)
    df = df[df[date_col] < STRIP_DATE].copy()
    after = len(df)
    pq.write_table(
        pa.Table.from_pandas(df, preserve_index=False),
        dst, compression="snappy",
    )
    return before - after


def rewind_meta(src: Path, dst: Path):
    """Copy meta JSON, rewinding last_cached_date to REWIND_DATE."""
    with open(src, "r", encoding="utf-8") as f:
        meta = json.load(f)
    if meta.get("last_cached_date"):
        meta["last_cached_date"] = REWIND_DATE
    with open(dst, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def main():
    # Clean previous test data
    if TEST_ROOT.exists():
        shutil.rmtree(TEST_ROOT)

    total_signals = 0
    total_contrib = 0
    total_meta = 0
    total_ohlc_meta = 0

    for basket_type, src_dir in BASKET_CACHE_DIRS.items():
        dst_dir = TEST_ROOT / f"{basket_type}_basket_cache"
        dst_dir.mkdir(parents=True, exist_ok=True)

        if not src_dir.exists():
            print(f"  SKIP {src_dir} (not found)")
            continue

        for f in sorted(src_dir.iterdir()):
            dst = dst_dir / f.name

            if f.name.endswith("_signals.parquet"):
                stripped = strip_parquet(f, dst)
                total_signals += 1
                print(f"  [signals]  {f.name}  ({stripped} rows stripped)")

            elif f.name.endswith("_contributions.parquet"):
                stripped = strip_parquet(f, dst, date_col="Date")
                total_contrib += 1
                print(f"  [contrib]  {f.name}  ({stripped} rows stripped)")

            elif f.name.endswith("_signals_meta.json"):
                rewind_meta(f, dst)
                total_meta += 1
                print(f"  [meta]     {f.name}  (rewound to {REWIND_DATE})")

            elif f.name.endswith("_ohlc.parquet"):
                # Copy OHLC as-is (equity OHLC has its own cache logic)
                shutil.copy2(f, dst)
                print(f"  [ohlc]     {f.name}  (copied)")

            elif f.name.endswith("_ohlc_meta.json"):
                # Copy OHLC meta as-is
                shutil.copy2(f, dst)
                total_ohlc_meta += 1
                print(f"  [ohlc-meta]{f.name}  (copied)")

    print()
    print(f"Test data created at: {TEST_ROOT}")
    print(f"  {total_signals} signal files stripped")
    print(f"  {total_contrib} contribution files stripped")
    print(f"  {total_meta} meta files rewound to {REWIND_DATE}")
    print(f"  {total_ohlc_meta} ohlc meta files copied")
    print()
    print("Run test_incremental_run.py to verify the incremental rebuild.")


if __name__ == "__main__":
    main()
