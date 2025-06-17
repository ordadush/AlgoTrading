#display_marcov_fft_side_trend.py
"""
Displays the S&P 500 close price with decision‑fusion regime signals (Approach 3).

This visualization highlights market regime classification obtained by combining
two separate Markov-switching models:
    - Model A: Raw returns
    - Model B: FFT-smoothed trend returns

Signal values and their meanings:
    +1 → green   (bullish / sustained uptrend)
     0 → yellow  (transition / no consensus)
    -1 → red     (bearish / sustained downtrend)

Usage:
------
>>> python -m Or.display_market_fused
>>> python -m Or.display_market_fused --save fused_output.png
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CSV = SCRIPT_DIR.parent / "markov_fft_side_trend.csv"

COLOUR_MAP = {
    1: "green",
    0: "yellow",
   -1: "red",
}

LEGEND_LINES = [
    plt.Line2D([0], [0], marker="o", color="w", label="Bull (+1)", markerfacecolor="green", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="Bear (-1)", markerfacecolor="red", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="Transition (0)", markerfacecolor="yellow", markersize=6),
]

# ---------------------------------------------------------------------------
# Main visualization logic
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Load a CSV file with decision-fused regime signals and plot the S&P 500 close price
    using color-coded markers to reflect the regime state each day.
    """
    parser = argparse.ArgumentParser(
        prog="display_market_fused",
        description="Plot S&P 500 close price with decision-fused regime signals.",
    )
    parser.add_argument("--csv", type=str, help="Path to input CSV file.")
    parser.add_argument("--save", type=str, metavar="FILE", help="Save plot to FILE instead of showing it.")
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else DEFAULT_CSV
    if not csv_path.exists():
        alt = Path.cwd() / csv_path.name
        if alt.exists():
            csv_path = alt
        else:
            sys.exit(f"CSV not found. Tried {csv_path} and {alt}. Provide a valid path with --csv.")

    df = pd.read_csv(csv_path, parse_dates=["date"])
    if "date" in df.columns:
        df = df.set_index("date")

    required_cols = {"close", "regime_signal_fused"}
    missing = required_cols - set(df.columns)
    if missing:
        sys.exit(f"Missing columns in CSV: {', '.join(sorted(missing))}")

    df["regime_signal_fused"] = df["regime_signal_fused"].fillna(0).astype(int)
    colours = df["regime_signal_fused"].map(COLOUR_MAP).fillna("gray")

    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df["close"], linewidth=1, color="black", label="Close Price")
    plt.scatter(df.index, df["close"], c=colours, s=12, label="Fused Signal")

    plt.title("S&P 500 – Close Price with Decision-Fused Regime Signal")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.legend(handles=LEGEND_LINES + [plt.Line2D([], [], color="black", label="Close")])
    plt.tight_layout()

    if args.save:
        plt.savefig(args.save, dpi=150)
        print(f"Plot saved to {args.save}")
    else:
        plt.show()


if __name__ == "__main__":
    main()
