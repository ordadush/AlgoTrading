"""
Displays the S&P 500 close price with a 5-level combined regime signal.

The regime signal is a result of fusing directional and strength signals.
Direction is determined by a primary FFT-based Markov model.
Strength is determined by support from a secondary fused model.

Signal values and their corresponding meanings:
    +2 → strong long     (strong green)
    +1 → weak long       (light green)
     0 → no trade        (yellow)
    -1 → weak short      (light red)
    -2 → strong short    (dark red)

Usage examples:
---------------
>>> python -m Or.display_marcov_fft_com
>>> python -m Or.display_marcov_fft_com --save output.png
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
DEFAULT_CSV = SCRIPT_DIR.parent / "markov_fft_com.csv"

COLOUR_MAP = {
    2: "darkgreen",
    1: "lightgreen",
    0: "yellow",
   -1: "lightcoral",
   -2: "darkred",
}

LEGEND_LINES = [
    plt.Line2D([0], [0], marker="o", color="w", label="Strong Long (+2)", markerfacecolor="darkgreen", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="Weak Long (+1)", markerfacecolor="lightgreen", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="No Trade (0)", markerfacecolor="yellow", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="Weak Short (-1)", markerfacecolor="lightcoral", markersize=6),
    plt.Line2D([0], [0], marker="o", color="w", label="Strong Short (-2)", markerfacecolor="darkred", markersize=6),
]

# ---------------------------------------------------------------------------
# Main script
# ---------------------------------------------------------------------------

def main() -> None:
    """
    Parse CLI arguments, load data, and plot S&P 500 close price with regime signal colors.
    """
    parser = argparse.ArgumentParser(description="Display combined regime signal (5 levels)")
    parser.add_argument("--csv", type=str, help="Path to CSV with regime_signal_combined column")
    parser.add_argument("--save", type=str, metavar="FILE", help="Save plot instead of showing")
    args = parser.parse_args()

    csv_path = Path(args.csv) if args.csv else DEFAULT_CSV
    if not csv_path.exists():
        alt = Path.cwd() / csv_path.name
        if alt.exists():
            csv_path = alt
        else:
            sys.exit(f"CSV not found. Tried {csv_path} and {alt}.")

    df = pd.read_csv(csv_path, parse_dates=["date"])
    if "date" in df.columns:
        df = df.set_index("date")

    required_cols = {"close", "regime_signal_combined"}
    missing = required_cols - set(df.columns)
    if missing:
        sys.exit(f"Missing columns in CSV: {', '.join(sorted(missing))}")

    # Replace NaNs in signal with 0 to ensure continuity
    df["regime_signal_combined"] = df["regime_signal_combined"].fillna(0).astype(int)
    colours = df["regime_signal_combined"].map(COLOUR_MAP).fillna("gray")

    plt.figure(figsize=(13, 6))
    plt.plot(df.index, df["close"], color="black", linewidth=1, label="Close")
    plt.scatter(df.index, df["close"], c=colours, s=12, label="Signal")

    plt.title("S&P 500 – Close Price with Combined Regime Signal")
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
