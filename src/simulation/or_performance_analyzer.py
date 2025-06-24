from __future__ import annotations

import pandas as pd
import numpy as np


class PerformanceAnalyzer:
    """
    Analyze backtest results and compute performance statistics.
    """

    def __init__(self, equity_curve: pd.DataFrame):
        """
        Initialize with equity curve DataFrame.
        Args:
            equity_curve (pd.DataFrame): Output from BacktestSimulator.results()
        """
        self.df = equity_curve.copy()
        self.df = self.df.sort_index()
        self.df["returns"] = self.df["equity"].pct_change().fillna(0)

    def total_return(self) -> float:
        start = self.df["equity"].iloc[0]
        end = self.df["equity"].iloc[-1]
        return (end - start) / start

    def cagr(self) -> float:
        start = self.df.index[0]
        end = self.df.index[-1]
        n_years = (end - start).days / 365.25
        return (self.df["equity"].iloc[-1] / self.df["equity"].iloc[0]) ** (1/n_years) - 1

    def volatility(self) -> float:
        return self.df["returns"].std() * np.sqrt(252)

    def sharpe_ratio(self, risk_free_rate=0.0) -> float:
        daily_returns = self.df["daily_return"]
        excess_returns = daily_returns  
        std = np.std(excess_returns)
        if std == 0 or np.isnan(std):
            return np.nan
        return np.mean(excess_returns) / std * np.sqrt(252)

    def max_drawdown(self) -> float:
        cum_max = self.df["equity"].cummax()
        drawdown = (self.df["equity"] - cum_max) / cum_max
        return drawdown.min()

    def hit_ratio(self) -> float:
        positive_days = (self.df["returns"] > 0).sum()
        total_days = len(self.df)
        return positive_days / total_days

    def yearly_returns(self) -> pd.Series:
        yearly = self.df.resample("YE").last()
        yearly["year_return"] = yearly["equity"].pct_change()
        return yearly["year_return"].dropna()

    def average_yearly_return(self) -> float:
        returns_by_year = self.yearly_returns()
        return returns_by_year.mean()

    def median_yearly_return(self) -> float:
        returns_by_year = self.yearly_returns()
        return returns_by_year.median()

    def summarize(self) -> None:
        """
        Print full summary of key metrics.
        """
        print("=== Backtest Performance Summary ===")
        print(f"Total Return        : {self.total_return()*100:.2f}%")
        print(f"CAGR               : {self.cagr()*100:.2f}%")
        print(f"Volatility         : {self.volatility()*100:.2f}%")
        print(f"Sharpe Ratio       : {self.sharpe_ratio():.2f}")
        print(f"Max Drawdown       : {self.max_drawdown()*100:.2f}%")
        print(f"Hit Ratio (Days Up): {self.hit_ratio()*100:.2f}%")
        print(f"Avg Yearly Return  : {self.average_yearly_return()*100:.2f}%")
        print(f"Median Yearly Ret  : {self.median_yearly_return()*100:.2f}%")

        print("\nYearly Returns:")
        for year, ret in self.yearly_returns().items():
            print(f"  {year.year}: {ret*100:.2f}%")

        print("====================================")
