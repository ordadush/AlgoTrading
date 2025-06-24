from __future__ import annotations

from typing import List, Optional
import pandas as pd

from simulation.or_data_bundle import build_daily_bundle, DailyBundle

# ---------------------------------------------------------------------------
# Main universe builder function
# ---------------------------------------------------------------------------

def get_eligible_universe(
    date: str | pd.Timestamp,
    beta_window: int,
    beta_up_threshold: Optional[float] = None,
    beta_down_threshold: Optional[float] = None
) -> List[str]:
    """
    Select eligible trading universe for a given date based on beta thresholds.

    Args:
        date (str | pd.Timestamp): Target trading date.
        beta_window (int): Rolling window (e.g., 30, 60, 90, 180, 360).
        beta_up_threshold (Optional[float]): Minimum beta_up threshold (long filter).
        beta_down_threshold (Optional[float]): Maximum beta_down threshold (short filter).

    Returns:
        List[str]: List of symbols passing the filtering criteria for this date.
    """

    # Build full bundle for target date
    bundle = build_daily_bundle(date)
    betas = bundle.betas

    # Build column names dynamically based on selected window
    beta_up_col = f'beta_up_{beta_window}'
    beta_down_col = f'beta_down_{beta_window}'

    # Filter out any rows missing beta values for this window
    betas_filtered = betas.dropna(subset=[beta_up_col, beta_down_col])

    # Apply beta thresholds
    condition = pd.Series(True, index=betas_filtered.index)

    if beta_up_threshold is not None:
        condition &= (betas_filtered[beta_up_col] >= beta_up_threshold)

    if beta_down_threshold is not None:
        condition &= (betas_filtered[beta_down_col] <= beta_down_threshold)

    eligible_symbols = betas_filtered.loc[condition, "symbol"].unique().tolist()
    eligible_symbols.sort()

    return eligible_symbols
