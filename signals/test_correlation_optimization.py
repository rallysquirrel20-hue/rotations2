"""
Test script for correlation optimization.
1. Backs up old Correlation_Pct from first 3 baskets
2. Runs pipeline with BENCHMARK_BASKETS=3
3. Compares old vs new Correlation_Pct
4. Tests backend endpoints
"""
import pandas as pd
import numpy as np
from pathlib import Path
import shutil
import sys
import json

DATA_FOLDER = Path.home() / 'Documents' / 'Python_Outputs' / 'Data_Storage'
THEMATIC_CACHE = DATA_FOLDER / 'thematic_basket_cache'

# The first 3 baskets in rotations.py are: High Beta, Low Beta, Momentum Leaders
BASKET_SLUGS = ['High_Beta', 'Low_Beta', 'Momentum_Leaders']
BASKET_FILES = {
    'High_Beta': THEMATIC_CACHE / 'High_Beta_25_of_500_signals.parquet',
    'Low_Beta': THEMATIC_CACHE / 'Low_Beta_25_of_500_signals.parquet',
    'Momentum_Leaders': THEMATIC_CACHE / 'Momentum_Leaders_25_of_500_signals.parquet',
}

BACKUP_DIR = DATA_FOLDER / '_correlation_test_backup'


def step1_backup():
    """Back up old basket parquets and extract Correlation_Pct."""
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    old_corr = {}
    for slug, path in BASKET_FILES.items():
        if not path.exists():
            print(f"  SKIP {slug}: {path} not found")
            continue
        # Back up the file
        backup_path = BACKUP_DIR / path.name
        shutil.copy2(path, backup_path)
        print(f"  Backed up {path.name}")
        # Extract Correlation_Pct
        df = pd.read_parquet(path, columns=['Date', 'Correlation_Pct'])
        df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
        old_corr[slug] = df.set_index('Date')['Correlation_Pct']
        print(f"  {slug}: {len(df)} rows, non-null Correlation_Pct: {df['Correlation_Pct'].notna().sum()}")
        print(f"    Range: [{df['Correlation_Pct'].min():.2f}, {df['Correlation_Pct'].max():.2f}]")
        print(f"    Mean: {df['Correlation_Pct'].mean():.2f}")
    # Save old correlation for comparison
    old_df = pd.DataFrame(old_corr)
    old_df.to_parquet(BACKUP_DIR / 'old_correlation_pct.parquet')
    print(f"\n  Old correlation values saved to {BACKUP_DIR / 'old_correlation_pct.parquet'}")
    return old_corr


def step2_compare():
    """Compare old vs new Correlation_Pct values."""
    old_df = pd.read_parquet(BACKUP_DIR / 'old_correlation_pct.parquet')
    print("\n=== Correlation_Pct Comparison ===\n")

    for slug, path in BASKET_FILES.items():
        if not path.exists():
            print(f"  SKIP {slug}: new file not found")
            continue
        new = pd.read_parquet(path, columns=['Date', 'Correlation_Pct'])
        new['Date'] = pd.to_datetime(new['Date']).dt.normalize()
        new_series = new.set_index('Date')['Correlation_Pct']

        if slug not in old_df.columns:
            print(f"  SKIP {slug}: no old data")
            continue
        old_series = old_df[slug]

        # Align on common dates
        common = old_series.index.intersection(new_series.index)
        if len(common) == 0:
            print(f"  {slug}: NO overlapping dates!")
            continue

        old_vals = old_series.loc[common]
        new_vals = new_series.loc[common]

        # Only compare where both are non-null
        both_valid = old_vals.notna() & new_vals.notna()
        n_both = both_valid.sum()
        if n_both == 0:
            print(f"  {slug}: no rows where both old & new are non-null")
            continue

        diff = (new_vals[both_valid] - old_vals[both_valid]).abs()
        print(f"  {slug}:")
        print(f"    Common dates: {len(common)}, both non-null: {n_both}")
        print(f"    Old mean: {old_vals[both_valid].mean():.2f}, New mean: {new_vals[both_valid].mean():.2f}")
        print(f"    Abs diff — mean: {diff.mean():.2f}, median: {diff.median():.2f}, max: {diff.max():.2f}, p95: {diff.quantile(0.95):.2f}")
        print(f"    Correlation (old vs new): {old_vals[both_valid].corr(new_vals[both_valid]):.4f}")

        # Check within ±2% tolerance
        within_2 = (diff <= 2.0).mean() * 100
        within_5 = (diff <= 5.0).mean() * 100
        print(f"    Within ±2: {within_2:.1f}%, Within ±5: {within_5:.1f}%")

        # Show worst mismatches
        worst = diff.nlargest(5)
        if not worst.empty:
            print(f"    Worst 5 mismatches:")
            for d in worst.index:
                print(f"      {d.date()}: old={old_vals.loc[d]:.2f}, new={new_vals.loc[d]:.2f}, diff={diff.loc[d]:.2f}")
        print()


def step3_check_returns_matrix():
    """Verify returns_matrix_500.parquet was created."""
    rm_path = DATA_FOLDER / 'returns_matrix_500.parquet'
    fp_path = DATA_FOLDER / 'returns_matrix_500.fingerprint'
    if rm_path.exists():
        rm = pd.read_parquet(rm_path)
        print(f"  returns_matrix_500.parquet: {rm.shape} ({rm_path.stat().st_size / 1024 / 1024:.1f} MB)")
        print(f"  Date range: {rm.index.min()} to {rm.index.max()}")
        print(f"  Tickers: {len(rm.columns)}")
    else:
        print(f"  returns_matrix_500.parquet: NOT FOUND")
    if fp_path.exists():
        print(f"  Fingerprint: {fp_path.read_text().strip()[:20]}...")
    else:
        print(f"  Fingerprint: NOT FOUND")


def step4_test_backend():
    """Test backend correlation endpoints (requires running backend)."""
    import urllib.request
    base = 'http://localhost:8000'
    tests = [
        ('/api/baskets/breadth', 'Basket breadth (uses Correlation_Pct from parquet)'),
        ('/api/baskets/High%20Beta/correlation', 'Basket correlation matrix (live-computed)'),
        ('/api/baskets/High%20Beta/summary', 'Basket summary with correlation (live-computed)'),
    ]
    for endpoint, desc in tests:
        url = f"{base}{endpoint}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode())
            print(f"  OK {desc}")
            if 'breadth' in endpoint:
                # Check that Correlation_Pct shows up
                for key in ['High Beta', 'Low Beta', 'Momentum Leaders']:
                    slug_key = key.lower().replace(' ', '_')
                    # breadth returns basket slugs
                    found = False
                    for k, v in data.items():
                        if key.lower().replace(' ', '') in k.lower().replace(' ', '').replace('_', ''):
                            corr_val = v.get('corr_pct')
                            print(f"    {k}: corr_pct={corr_val}")
                            found = True
                            break
                    if not found:
                        print(f"    {key}: not found in breadth response")
            elif 'correlation' in endpoint and 'summary' not in endpoint:
                labels = data.get('labels', [])
                matrix = data.get('matrix', [])
                print(f"    Labels: {len(labels)} tickers, Matrix: {len(matrix)}x{len(matrix[0]) if matrix else 0}")
            elif 'summary' in endpoint:
                corr = data.get('correlation', {})
                labels = corr.get('labels', [])
                print(f"    Correlation in summary: {len(labels)} tickers")
        except Exception as e:
            print(f"  FAIL {desc}: {e}")


def step5_restore():
    """Restore backed-up parquets."""
    for slug, path in BASKET_FILES.items():
        backup = BACKUP_DIR / path.name
        if backup.exists():
            shutil.copy2(backup, path)
            print(f"  Restored {path.name}")
    print("  Backup files left in place for reference. Delete manually when done:")
    print(f"    {BACKUP_DIR}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python test_correlation_optimization.py <step>")
        print("  step1  - Back up old Correlation_Pct values")
        print("  step2  - Compare old vs new (run AFTER pipeline)")
        print("  step3  - Check returns_matrix_500.parquet was created")
        print("  step4  - Test backend endpoints (backend must be running)")
        print("  step5  - Restore old parquets from backup")
        sys.exit(0)

    step = sys.argv[1]
    if step == 'step1':
        print("Step 1: Backing up old Correlation_Pct...")
        step1_backup()
    elif step == 'step2':
        print("Step 2: Comparing old vs new Correlation_Pct...")
        step2_compare()
    elif step == 'step3':
        print("Step 3: Checking returns_matrix_500.parquet...")
        step3_check_returns_matrix()
    elif step == 'step4':
        print("Step 4: Testing backend endpoints...")
        step4_test_backend()
    elif step == 'step5':
        print("Step 5: Restoring old parquets...")
        step5_restore()
    else:
        print(f"Unknown step: {step}")
