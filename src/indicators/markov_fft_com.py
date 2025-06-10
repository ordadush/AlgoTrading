"""
Generates a 5-level market regime signal by fusing direction and strength inputs.

Overview:
---------
- Direction comes from a primary model based on FFT‑FEAT signal processing.
- Strength comes from a secondary model providing support (fused signal).
- Fusion logic outputs a combined signal with 5 discrete values:
    +2 → strong long (full agreement)
    +1 → weak long   (partial or missing support)
     0 → no trade    (no direction)
    -1 → weak short  (partial or missing support)
    -2 → strong short (full agreement)

Fusion Rules:
-------------
1. If `regime_signal_fftfeat` is NaN ⇒ no trade (0).
2. If `regime_signal_fftfeat` is 0 ⇒ no trade (0).
3. If `regime_signal_fused` equals main direction ⇒ strong signal (±2).
4. Else (including if missing) ⇒ weak signal (±1).

Outputs:
--------
Creates a CSV file `sp500_markov_regimes_combined.csv` with:
    - close price
    - both input signals
    - fused signal in `regime_signal_combined`
"""

from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Signal fusion logic
# ---------------------------------------------------------------------------

def fuse(a: float | int | None, b: float | int | None) -> int:
    """
    Fuse direction and support signals into a 5-level regime indicator.

    Parameters:
        a (float|int|None): Main direction signal from FFT‑FEAT model.
        b (float|int|None): Support signal from fused model.

    Returns:
        int: Combined signal in {+2, +1, 0, -1, -2}
    """
    if pd.isna(a):
        return 0  # No direction → no trade

    a = int(a)
    b_valid = not pd.isna(b)
    same_sign = b_valid and int(b) == a

    if a == 0:
        return 0
    if a == 1:
        return 2 if same_sign else 1
    if a == -1:
        return -2 if same_sign else -1

    return 0  # fallback (should not occur)

# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        "markov_regime_fuse",
        description="Fuse directional and support regime signals to produce 5‑level output"
    )
    parser.add_argument("--fft", default="markov_fft_main_trend.csv", help="Main signal CSV (FFT‑FEAT)")
    parser.add_argument("--fused", default="markov_fft_side_trend.csv", help="Support signal CSV (fused)")
    parser.add_argument("--out", default="markov_fft_com.csv", help="Path to output CSV file")
    args = parser.parse_args()

    fft_df = pd.read_csv(args.fft, parse_dates=["date"]).set_index("date")
    fused_df = pd.read_csv(args.fused, parse_dates=["date"]).set_index("date")

    required_fft = "regime_signal_fftfeat"
    required_fused = "regime_signal_fused"
    if required_fft not in fft_df.columns:
        raise KeyError(f"Missing {required_fft} in {args.fft}")
    if required_fused not in fused_df.columns:
        raise KeyError(f"Missing {required_fused} in {args.fused}")

    # Outer join to ensure full date coverage
    df = fft_df[[required_fft, "close"]].join(fused_df[[required_fused]], how="outer")

    # Apply fusion logic
    df["regime_signal_combined"] = [fuse(a, b) for a, b in zip(df[required_fft], df[required_fused])]

    # Save output
    out_path = Path(args.out).resolve()
    df.to_csv(out_path, index=True)
    print("✅ written", out_path)

    # Summary
    print(df["regime_signal_combined"].value_counts().sort_index())


if __name__ == "__main__":
    main()
