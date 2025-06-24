from __future__ import annotations

import pandas as pd
import matplotlib.pyplot as plt

from simulation.or_backtest_engine import generate_signal_calendar
from simulation.or_backtest_simulator import BacktestSimulator
from simulation.or_performance_analyzer import PerformanceAnalyzer



def run_full_backtest():
    """
    Full end-to-end backtest example:
    1. Generate signals.
    2. Simulate trading.
    3. Plot equity curve.
    """

    print("=== Step 1: Generating signals ===")
    signals = generate_signal_calendar(
        start_date="2014-01-01",
        end_date="2024-12-31",
        beta_window=60,
        beta_up_threshold=1.1,
        beta_down_threshold=1.05
    )

    print("✅ Signals generated.")

    print("\n=== Step 2: Running backtest simulation ===")
    sim = BacktestSimulator(
        signal_calendar=signals,
        initial_cash=1_000_000,
        max_positions=10,
        fixed_size=10_000
    )
    sim.run()
    results = sim.results()

    print("✅ Backtest completed.")
    analyzer = PerformanceAnalyzer(results)
    analyzer.summarize()

    print("\nFinal Equity: ${:,.2f}".format(results['equity'].iloc[-1]))

    print("\n=== Step 3: Plotting results ===")
    plt.figure(figsize=(12, 6))
    plt.plot(results.index, results["equity"], label="Equity Curve", color="blue")
    plt.title("Backtest Equity Curve")
    plt.xlabel("Date")
    plt.ylabel("Equity ($)")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    run_full_backtest()
