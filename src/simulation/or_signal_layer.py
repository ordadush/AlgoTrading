from __future__ import annotations

from typing import Optional
import pandas as pd

from simulation.or_data_bundle import build_daily_bundle


# ---------------------------------------------------------------------------
# Mapping the regime score into directional signal
# ---------------------------------------------------------------------------

def get_regime_signal(date: str | pd.Timestamp) -> int:
    """
    Convert market regime score to trading direction.

    Args:
        date (str | pd.Timestamp): Trading day.

    Returns:
        int: Market signal direction:
             +1 → Long bias (score >= 1)
             -1 → Short bias (score <= -1)
              0 → No trade (neutral)
    """
    bundle = build_daily_bundle(date)
    score = bundle.regime_score

    if score >= 1:
        return 1  # Long bias
    elif score <= -1:
        return -1  # Short bias
    else:
        return 0  # Neutral zone


# ---------------------------------------------------------------------------
# Extract beta values for a specific stock and window
# ---------------------------------------------------------------------------

def get_beta_values(
    date: str | pd.Timestamp,
    symbol: str,
    beta_window: int
) -> tuple[Optional[float], Optional[float]]:
    """
    Retrieve beta_up and beta_down values for a stock on given date.

    Args:
        date (str | pd.Timestamp): Trading date.
        symbol (str): Ticker symbol.
        beta_window (int): Rolling window length.

    Returns:
        Tuple: (beta_up, beta_down) or (None, None) if missing.
    """
    bundle = build_daily_bundle(date)
    betas = bundle.betas

    row = betas[(betas["symbol"] == symbol)]
    if row.empty:
        return None, None

    beta_up_col = f"beta_up_{beta_window}"
    beta_down_col = f"beta_down_{beta_window}"

    beta_up = row.iloc[0].get(beta_up_col)
    beta_down = row.iloc[0].get(beta_down_col)

    if pd.isna(beta_up) or pd.isna(beta_down):
        return None, None

    return float(beta_up), float(beta_down)


# ---------------------------------------------------------------------------
# Entry signal generator
# ---------------------------------------------------------------------------

def is_entry_long(
    date: str | pd.Timestamp,
    symbol: str,
    beta_window: int,
    beta_up_threshold: float
) -> bool:
    """
    Determine if long entry is allowed for given stock on given date.

    Logic:
      - Market must be bullish (score >= 1)
      - Stock beta_up must exceed threshold

    Args:
        date: Trading date.
        symbol: Ticker symbol.
        beta_window: Rolling beta window size.
        beta_up_threshold: Minimum required beta_up.

    Returns:
        bool: True if long entry conditions are satisfied.
    """
    regime_signal = get_regime_signal(date)
    if regime_signal != 1:
        return False

    beta_up, _ = get_beta_values(date, symbol, beta_window)
    if beta_up is None:
        return False

    return beta_up >= beta_up_threshold


def is_entry_short(
    date: str | pd.Timestamp,
    symbol: str,
    beta_window: int,
    beta_down_threshold: float
) -> bool:
    """
    Determine if short entry is allowed for given stock on given date.

    Logic:
      - Market must be bearish (score <= -1)
      - Stock beta_down must exceed threshold

    Args:
        date: Trading date.
        symbol: Ticker symbol.
        beta_window: Rolling beta window size.
        beta_down_threshold: Minimum required beta_down.

    Returns:
        bool: True if short entry conditions are satisfied.
    """
    regime_signal = get_regime_signal(date)
    if regime_signal != -1:
        return False

    _, beta_down = get_beta_values(date, symbol, beta_window)
    if beta_down is None:
        return False

    return beta_down >= beta_down_threshold
