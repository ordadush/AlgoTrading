from __future__ import annotations

import pandas as pd

from simulation.data_loaders import load_sp500, load_betas

def validate_data_integrity(force_reload: bool = False) -> None:
    """
    Performs full consistency check between SP500 index and beta tables.

    This function:
    - Ensures both datasets cover the same date range.
    - Reports any dates where beta data is missing but SP500 data exists.

    Args:
        force_reload (bool): If True, reload fresh data from database ignoring cache.
    """
    # Load full datasets (from cache unless forced)
    sp500_df = load_sp500(force_reload=force_reload)
    betas_df = load_betas(force_reload=force_reload)

    # Extract unique dates from both tables
    sp_dates = set(sp500_df["date"])
    beta_dates = set(betas_df["date"])

    # Identify missing dates
    missing_dates = sp_dates - beta_dates

    if missing_dates:
        print(f"❌ Missing beta data for {len(missing_dates)} days:")
        for d in sorted(missing_dates):
            print(f" - {pd.to_datetime(d).date()}")
    else:
        print("✅ All dates have complete SP500 and beta data.")
