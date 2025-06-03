# clean_nasdaq_symbols.py
# -------------------------------------------------------------
# Build a list of NASDAQ symbols that:
#   • are not ETFs / warrants / units / test issues
#   • have ≥ 11 years of trading history
#   • show ≥ $500k average dollar-volume
# Result is saved to 'filtered_nasdaq_symbols.csv'
# -------------------------------------------------------------

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf

# ---- parameters you can tweak ---------------------------------
MIN_YEARS_HISTORY   = 11
MIN_DOLLAR_VOLUME   = 500_000      # USD
SOURCE_CSV          = Path(__file__).resolve().parent.parent / "nasdaqlisted.csv"
OUTPUT_CSV          = Path(__file__).resolve().parent.parent / "filtered_nasdaq_symbols.csv"
# ---------------------------------------------------------------


def load_raw_nasdaq() -> pd.DataFrame:
    """Read nasdaqlisted.csv and return DataFrame."""
    if not SOURCE_CSV.exists():
        raise FileNotFoundError(
            f"nasdaqlisted.csv not found at {SOURCE_CSV}\n"
            "Download it from https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt\n"
            "and rename it to nasdaqlisted.csv (remove the last footer line)."
        )
    df = pd.read_csv(SOURCE_CSV, sep="|", on_bad_lines="skip")
    return df


def basic_symbol_filter(df: pd.DataFrame) -> pd.Series:
    """Boolean mask for symbols that are NOT ETFs / units / warrants / etc."""
    mask = (
        (df["ETF"] != "Y")
        & (df["Test Issue"] != "Y")
        & (~df["Symbol"].str.contains(r"[.\-/]", na=False))
        & (~df["Security Name"].str.contains(r"Warrant|Right|Unit", case=False, na=False))
    )
    return mask


def has_sufficient_history_and_volume(symbol: str) -> bool:
    """Check 11-year history and dollar-volume threshold."""
    try:
        if not symbol:
            return False

        hist = yf.Ticker(symbol).history(period="max", auto_adjust=False, actions=False)
        if hist.empty:
            return False

        # history length
        first_date = hist.index[0].to_pydatetime().replace(tzinfo=None)
        years = (datetime.utcnow() - first_date).days / 365
        if years < MIN_YEARS_HISTORY:
            return False

        # avg dollar volume
        hist = hist.dropna(subset=["Close", "Volume"])
        hist["dollar_vol"] = hist["Close"] * hist["Volume"]
        if hist["dollar_vol"].mean() < MIN_DOLLAR_VOLUME:
            return False

        return True

    except Exception as e:
        print(f"⚠️  {symbol}: {e}")
        return False


def main():
    raw = load_raw_nasdaq()
    candidates = raw[basic_symbol_filter(raw)]["Symbol"].tolist()
    print(f"Found {len(candidates)} clean symbols to check…")

    valid = []
    for i, sym in enumerate(candidates, 1):
        print(f"[{i}/{len(candidates)}] Checking {sym}", end="\r")
        if has_sufficient_history_and_volume(sym):
            valid.append(sym)

    # save result
    pd.Series(valid, name="Symbol").to_csv(OUTPUT_CSV, index=False)
    print(f"\n✅ Saved {len(valid)} symbols to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
