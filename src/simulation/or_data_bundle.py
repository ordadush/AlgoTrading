from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import List

import pandas as pd

from simulation.data_loaders import load_sp500, load_stocks, load_betas

# ---------------------------------------------------------------------------
# Load full data into memory once to avoid repeated DB queries
# ---------------------------------------------------------------------------
_sp500_df = load_sp500()
_stocks_df = load_stocks()
_betas_df = load_betas()

_sp500_df = _sp500_df.sort_values("date")
_stocks_df = _stocks_df.sort_values(["date", "symbol"])
_betas_df = _betas_df.sort_values(["date", "symbol"])

# ---------------------------------------------------------------------------
# DailyBundle: unified data object for a single trading day
# ---------------------------------------------------------------------------

@dataclass
class DailyBundle:
    """
    Encapsulates all relevant data for one specific trading day.
    
    Attributes:
        date (datetime): The target date.
        regime_score (int): Market regime score for that day (-2 to +2).
        eligible_symbols (List[str]): List of symbols with valid beta data.
        betas (pd.DataFrame): DataFrame of beta values for all eligible stocks.
    """
    date: datetime
    regime_score: int
    eligible_symbols: List[str]
    betas: pd.DataFrame

# ---------------------------------------------------------------------------
# Core builder function to construct DailyBundle
# ---------------------------------------------------------------------------

def build_daily_bundle(target_date: str | datetime) -> DailyBundle:
    """
    Assemble a complete data bundle for a specific trading day.

    Args:
        target_date (str | datetime): Date to retrieve.

    Returns:
        DailyBundle: Fully populated data object for the day.

    Raises:
        ValueError: If data for the requested date is missing.
    """

    if isinstance(target_date, str):
        target_date = pd.to_datetime(target_date).normalize()

    # Retrieve market regime score for the date
    sp_row = _sp500_df[_sp500_df["date"] == target_date]
    if sp_row.empty:
        raise ValueError(f"No SP500 data for {target_date.date()}")

    regime_score = int(sp_row.iloc[0]["score"])

    # Retrieve beta data for the date
    betas_today = _betas_df[_betas_df["date"] == target_date].copy()
    if betas_today.empty:
        raise ValueError(f"No beta data for {target_date.date()}")

    eligible_symbols = sorted(betas_today["symbol"].unique().tolist())

    return DailyBundle(
        date=target_date,
        regime_score=regime_score,
        eligible_symbols=eligible_symbols,
        betas=betas_today
    )
