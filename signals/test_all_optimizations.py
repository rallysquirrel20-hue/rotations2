"""Compare old vs new breadth + contributions + correlation values."""
import pandas as pd
import numpy as np
from pathlib import Path

DATA = Path.home() / 'Documents' / 'Python_Outputs' / 'Data_Storage'
BACKUP = DATA / '_correlation_test_backup'
THEMATIC = DATA / 'thematic_basket_cache'

SLUGS = {
    'High_Beta': THEMATIC / 'High_Beta_25_of_500_signals.parquet',
    'Low_Beta': THEMATIC / 'Low_Beta_25_of_500_signals.parquet',
    'Momentum_Leaders': THEMATIC / 'Momentum_Leaders_25_of_500_signals.parquet',
}


def compare_column(old_df, new_df, col, label):
    """Compare a column between old and new DataFrames."""
    common = old_df.index.intersection(new_df.index)
    if len(common) == 0:
        print(f"    {col}: NO overlapping dates")
        return
    old_s = old_df.loc[common, col] if col in old_df.columns else pd.Series(np.nan, index=common)
    new_s = new_df.loc[common, col] if col in new_df.columns else pd.Series(np.nan, index=common)
    both_valid = old_s.notna() & new_s.notna()
    n = both_valid.sum()
    if n == 0:
        print(f"    {col}: no rows where both non-null")
        return
    diff = (new_s[both_valid] - old_s[both_valid]).abs()
    corr = old_s[both_valid].corr(new_s[both_valid])
    print(f"    {col}: n={n}, mean_diff={diff.mean():.4f}, max_diff={diff.max():.4f}, corr={corr:.6f}")


# Compare signals parquets
for slug, path in SLUGS.items():
    old_path = BACKUP / path.name
    if not old_path.exists() or not path.exists():
        print(f"SKIP {slug}: missing old or new")
        continue

    old = pd.read_parquet(old_path)
    new = pd.read_parquet(path)
    old['Date'] = pd.to_datetime(old['Date']).dt.normalize()
    new['Date'] = pd.to_datetime(new['Date']).dt.normalize()
    old = old.set_index('Date')
    new = new.set_index('Date')

    print(f"\n=== {slug} (signals) ===")
    print(f"  Old: {len(old)} rows, New: {len(new)} rows")
    for col in ['Uptrend_Pct', 'Downtrend_Pct', 'Breadth_EMA',
                'Breakout_Pct', 'Breakdown_Pct', 'BO_Breadth_EMA',
                'Correlation_Pct']:
        compare_column(old, new, col, slug)

# Compare contributions parquets
print("\n\n=== CONTRIBUTIONS ===")
for slug in ['High_Beta', 'Low_Beta', 'Momentum_Leaders']:
    old_path = THEMATIC / f'{slug}_25_of_500_contributions.parquet'
    # No backup of contributions — compare against what's there now
    # We'll just check the new file exists and has reasonable data
    if old_path.exists():
        df = pd.read_parquet(old_path)
        print(f"\n  {slug}: {len(df)} rows, {df['Ticker'].nunique()} tickers, "
              f"{df['Date'].nunique()} dates")
        print(f"    Weight_BOD: [{df['Weight_BOD'].min():.4f}, {df['Weight_BOD'].max():.4f}], "
              f"mean={df['Weight_BOD'].mean():.4f}")
        print(f"    Contribution: [{df['Contribution'].min():.6f}, {df['Contribution'].max():.6f}], "
              f"mean={df['Contribution'].mean():.6f}")
        # Weights should sum to ~1.0 per day
        daily_w = df.groupby('Date')['Weight_BOD'].sum()
        print(f"    Daily weight sum: mean={daily_w.mean():.4f}, "
              f"min={daily_w.min():.4f}, max={daily_w.max():.4f}")
    else:
        print(f"  {slug}: NOT FOUND")
