#!/usr/bin/env python
"""
analyze_equity.py  –  Quick view of equity.csv

Usage
-----
$ python analyze_equity.py /path/to/equity.csv
"""

import argparse
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


def load_equity(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path, parse_dates=[0])  # העמודה הראשונה מכילה את התאריכים
    df.rename(columns={df.columns[0]: "date"}, inplace=True)
    return df

def show_stats(df: pd.DataFrame):
    total_ret = df["equity"].iloc[-1] / df["equity"].iloc[0] - 1
    sharpe = (
        df["daily_return"].mean() / df["daily_return"].std()
        * (252 ** 0.5)
        if df["daily_return"].std() > 0 else 0
    )
    max_dd = (df["equity"] / df["equity"].cummax() - 1).min()
    print("\n=== Equity summary ===")
    print(f"Total return : {total_ret:.2%}")
    print(f"Annual Sharpe: {sharpe:.2f}")
    print(f"Max drawdown : {max_dd:.2%}")


def plot_equity(df: pd.DataFrame):
    plt.figure()
    plt.plot(df["date"], df["equity"])
    plt.title("Equity curve")
    plt.xlabel("Date")
    plt.ylabel("Equity")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.show()


def plot_drawdown(df: pd.DataFrame):
    dd = df["equity"] / df["equity"].cummax() - 1
    plt.figure()
    plt.fill_between(df["date"], dd, color="red", step="pre")
    plt.title("Drawdown (%)")
    plt.xlabel("Date")
    plt.ylabel("Drawdown")
    plt.gca().yaxis.set_major_formatter(lambda x, pos: f"{x:.0%}")
    plt.show()


def main():
    parser = argparse.ArgumentParser(description="Analyze equity.csv")
    parser.add_argument("csv", type=Path, help="Path to equity.csv")
    args = parser.parse_args()

    df = load_equity(args.csv)
    show_stats(df)
    plot_equity(df)
    plot_drawdown(df)


if __name__ == "__main__":
    main()
