from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

import numpy as np
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def low_pass_fft(series: pd.Series, freq_cutoff: float) -> pd.Series:
    """
    Apply a low-pass FFT filter to extract the long-term trend.

    Parameters:
        series (pd.Series): Time series to filter.
        freq_cutoff (float): Frequency threshold (in cycles/day).

    Returns:
        pd.Series: Filtered trend component.
    """
    n = len(series)
    fft_vals = np.fft.fft(series.values)
    freqs = np.fft.fftfreq(n, d=1.0)
    fft_vals[np.abs(freqs) > freq_cutoff] = 0
    trend = np.fft.ifft(fft_vals).real
    return pd.Series(trend, index=series.index, name="trend")


def markov_signal(returns: pd.Series, threshold: float) -> pd.DataFrame:
    """
    Fit a 2-regime Markov regression model and return binary regime signal.

    Parameters:
        returns (pd.Series): Target return series.
        threshold (float): Minimum regime probability for confidence.

    Returns:
        pd.DataFrame: DataFrame with regime_signal column.
    """
    model = MarkovRegression(returns, k_regimes=2, switching_variance=True)
    res = model.fit()

    df = pd.DataFrame(index=returns.index)
    df["regime_0_prob"] = res.filtered_marginal_probabilities[0]
    df["regime_1_prob"] = res.filtered_marginal_probabilities[1]

    bull_regime = 0
    if returns[res.filtered_marginal_probabilities[1] > 0.5].mean() > \
       returns[res.filtered_marginal_probabilities[0] > 0.5].mean():
        bull_regime = 1
    bear_regime = 1 - bull_regime

    df["dom_prob"] = df[["regime_0_prob", "regime_1_prob"]].max(axis=1)
    df["dom_regime"] = np.where(df["regime_0_prob"] > df["regime_1_prob"], 0, 1)

    signal = np.zeros(len(df), dtype=int)
    signal[(df["dom_regime"] == bull_regime) & (df["dom_prob"] >= threshold)] = 1
    signal[(df["dom_regime"] == bear_regime) & (df["dom_prob"] >= threshold)] = -1

    df["regime_signal"] = signal
    return df[["regime_signal"]]

# ---------------------------------------------------------------------------
# Main routine
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Decision‑fusion of Markov + FFT-based regime models.")
    parser.add_argument("--cutoff", type=float, default=0.05, help="FFT frequency cutoff (cycles/day)")
    parser.add_argument("--threshold", type=float, default=0.80, help="Confidence threshold for regime classification")
    parser.add_argument("--out", type=str, default="markov_fft_side_trend_old_data.csv", help="Output CSV file path")
    args = parser.parse_args()

    # Load environment and database
    env_path = Path(__file__).resolve().parents[2] / "Algo_env" / ".env"
    load_dotenv(env_path)
    engine = sa.create_engine(sa.engine.URL.create("postgresql+psycopg2", query={"sslmode": "require"}))
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        engine = sa.create_engine(db_url)

    query = """
    SELECT date, close
    FROM sp500_index
    WHERE date BETWEEN '2014-01-01' AND '2024-12-31'
    ORDER BY date ASC;
    """
    df = pd.read_sql(query, engine, parse_dates=["date"]).set_index("date")
    df["returns"] = df["close"].pct_change()
    df = df.dropna(subset=["returns"])

    # Model A: Raw returns
    raw_sig = markov_signal(df["returns"], args.threshold)
    raw_sig.rename(columns={"regime_signal": "regime_signal_raw"}, inplace=True)

    # Model B: Trend returns (via FFT)
    trend = low_pass_fft(df["close"], args.cutoff)
    df["trend_returns"] = trend.pct_change()
    fft_df = df.dropna(subset=["trend_returns"])
    fft_sig = markov_signal(fft_df["trend_returns"], args.threshold)
    fft_sig = fft_sig.reindex(df.index)
    fft_sig.rename(columns={"regime_signal": "regime_signal_fft"}, inplace=True)

    # Decision fusion
    fused = pd.DataFrame(index=df.index)
    fused["close"] = df["close"]
    fused = fused.join([raw_sig, fft_sig])

    agreement = (fused["regime_signal_raw"] == fused["regime_signal_fft"]) & (fused["regime_signal_raw"].abs() == 1)
    fused["regime_signal_fused"] = 0
    fused.loc[agreement, "regime_signal_fused"] = fused["regime_signal_raw"]

    # Save result
    out_path = Path(args.out)
    fused.to_csv(out_path)
    print(f"CSV written to {out_path.resolve()}")

    # Preview last rows
    print("\nTail preview:")
    print(fused.tail(10))


if __name__ == "__main__":
    main()
