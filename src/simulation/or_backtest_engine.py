from __future__ import annotations

import pandas as pd

from simulation.data_loaders import load_sp500, load_betas


def generate_signal_calendar(
    start_date: str,
    end_date: str,
    beta_window: int,
    beta_up_threshold: float,
    beta_down_threshold: float
) -> pd.DataFrame:
    """
    Fast vectorized version of full signal calendar generation.
    For each day, classify which stocks have long and short entry signals.

    Args:
        start_date (str): Backtest start date (YYYY-MM-DD).
        end_date (str): Backtest end date (YYYY-MM-DD).
        beta_window (int): Rolling beta window to use.
        beta_up_threshold (float): Threshold for long entry.
        beta_down_threshold (float): Threshold for short entry.

    Returns:
        pd.DataFrame: Signal calendar with columns:
            - date
            - long_symbols: List[str]
            - short_symbols: List[str]
    """

    # Load data once
    sp500 = load_sp500()
    betas = load_betas()

    sp500["date"] = pd.to_datetime(sp500["date"])
    betas["date"] = pd.to_datetime(betas["date"])

    # Filter date range
    sp500 = sp500[(sp500["date"] >= start_date) & (sp500["date"] <= end_date)]
    sp500 = sp500.dropna(subset=["score"])

    # Merge betas into SP500 table (expand to cross symbol-date)
    merged = pd.merge(
        sp500[["date", "score"]],
        betas,
        on="date",
        how="inner"
    )

    # Build regime direction column
    merged["regime_signal"] = 0
    merged.loc[merged["score"] >= 1, "regime_signal"] = 1
    merged.loc[merged["score"] <= -1, "regime_signal"] = -1

    # Extract beta columns dynamically
    beta_up_col = f"beta_up_{beta_window}"
    beta_down_col = f"beta_down_{beta_window}"

    if beta_up_col not in merged.columns or beta_down_col not in merged.columns:
        raise ValueError(f"Missing beta columns for window {beta_window}")

    # Create long and short signal masks
    merged["long_signal"] = (
        (merged["regime_signal"] == 1) &
        (merged[beta_up_col] >= beta_up_threshold)
    )

    merged["short_signal"] = (
        (merged["regime_signal"] == -1) &
        (merged[beta_down_col] >= beta_down_threshold)
    )

    # Aggregate signals per day
    grouped = merged.groupby("date").agg({
        "symbol": list,
        "long_signal": list,
        "short_signal": list
    }).reset_index()

    # Build final lists per day
    output_rows = []
    for _, row in grouped.iterrows():
        symbols = row["symbol"]
        long_flags = row["long_signal"]
        short_flags = row["short_signal"]

        long_syms = [sym for sym, flag in zip(symbols, long_flags) if flag]
        short_syms = [sym for sym, flag in zip(symbols, short_flags) if flag]

        output_rows.append({
            "date": row["date"],
            "long_symbols": long_syms,
            "short_symbols": short_syms
        })

    return pd.DataFrame(output_rows)