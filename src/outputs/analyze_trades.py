### analyze_trades.py – updated version ###

import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def load_trades(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=["entry_date", "exit_date"])
    df = df.sort_values("entry_date").reset_index(drop=True)
    return df

def basic_stats(df: pd.DataFrame):
    # נשמור רק עסקאות סגורות
    closed_trades = df.dropna(subset=["exit_date"]).copy()

    print("\n=== Basic statistics ===")
    total_trades = len(closed_trades)
    winners = (closed_trades["pnl"] > 0).sum()
    losers = (closed_trades["pnl"] <= 0).sum()
    open_trades = df["exit_date"].isna().sum()

    print(f"Total closed trades: {total_trades}")
    print(f"Winners            : {winners}  ({winners/total_trades:.1%})")
    print(f"Losers             : {losers}  ({losers/total_trades:.1%})")
    print(f"Open positions     : {open_trades}  ({open_trades/len(df):.1%} of all records)")

    print(f"Average PnL        : {closed_trades['pnl'].mean():.2f}")
    print(f"Median PnL         : {closed_trades['pnl'].median():.2f}")
    print(f"Max win            : {closed_trades['pnl'].max():.2f}")
    print(f"Max loss           : {closed_trades['pnl'].min():.2f}")

    return closed_trades

def plot_histogram(df: pd.DataFrame):
    plt.figure()
    df["pnl"].hist(bins=40)
    plt.title("Distribution of trade PnL")
    plt.xlabel("PnL")
    plt.ylabel("Count")
    plt.grid(True, linestyle="--", alpha=0.3)
    plt.show()

def plot_cumulative(df: pd.DataFrame):
    df = df.dropna(subset=["exit_date", "pnl"]).copy()
    df["cumulative_pnl"] = df["pnl"].cumsum()

    plt.figure()
    plt.plot(df["exit_date"], df["cumulative_pnl"])
    plt.title("Cumulative PnL over time")
    plt.xlabel("Exit Date")
    plt.ylabel("Cumulative PnL")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.show()

def main():
    parser = argparse.ArgumentParser(description="Analyze trades.csv")
    parser.add_argument("csv", type=Path, help="Path to trades.csv")
    args = parser.parse_args()

    df = load_trades(args.csv)
    closed_df = basic_stats(df)
    plot_histogram(closed_df)
    plot_cumulative(closed_df)

if __name__ == "__main__":
    main()
