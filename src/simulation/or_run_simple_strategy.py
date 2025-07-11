"""
run_simple_strategy.py
======================
Back-test for a *very* specific scenario:

• 30-day beta window
• stock must be “stronger/weaker” for 5 straight sessions before entry
• hard-stop 3 % + trailing (ממומש כבר ב-SL_Simulator)
• long only when market-score > 0  → strong stocks
  short only when market-score < 0 → weak  stocks
• period: 2016-01-01 – 2018-01-01 (שנתיים בדיוק)

Outputs
-------
1. Trade-log (head)
2. Equity summary & Δ-equity ≈ Σ PnL check
3. Performance metrics (Sharpe, CAGR, DD, …)
"""

from datetime import date
import pandas as pd
from pathlib import Path


# --- project imports --------------------------------------------------------
from simulation.or_param_optimizer_v3 import (
    ParamSet,
    build_signals,
    SL_Simulator,
    INITIAL_CASH,
    MAX_POS,
    FIXED_SIZE,
)
from simulation.or_performance_analyzer import PerformanceAnalyzer

# ----------------------------------------------------------------------------
# 1) define the single parameter-set we want to test
#    th_up / th_dn_lo chosen very close to 1 so the logical conditions match
# ----------------------------------------------------------------------------
PARAMS = ParamSet(
    window=30,        # 30-day rolling betas
    th_up=1.01,       # beta_up > 1 → strong
    th_dn_lo=0.99,    # beta_down < 1 → strong   (and vice-versa for weakness)
    min_days=5,       # need 5 consecutive strong / weak days before entry
)

START, END = pd.Timestamp("2016-01-01"), pd.Timestamp("2018-01-01")

# ----------------------------------------------------------------------------
# 2) build the daily signal calendar
# ----------------------------------------------------------------------------
print("⏳ building signal calendar …")
signals_df = build_signals(START, END, PARAMS)
print("✅ calendar built – rows:", len(signals_df))

# ----------------------------------------------------------------------------
# 3) run the back-test (stop-loss simulator inherits hard-/trailing logic)
# ----------------------------------------------------------------------------
sim = SL_Simulator(
    signals_df,
    initial_cash=INITIAL_CASH,
    max_positions=MAX_POS,
    fixed_size=FIXED_SIZE,   # equal-weight position sizing
)
sim.run()                    # ← heavy work happens here
print("\nOpen positions at end:")
print(pd.DataFrame(sim.positions).head(10))

# ----------------------------------------------------------------------------
# 4) inspect the trade-log
# ----------------------------------------------------------------------------
trades = pd.DataFrame(sim.trades)
print("\n──── Trades (first 10) ────")
print(trades.head(10))
print("… total trades:", len(trades))

# ----------------------------------------------------------------------------
# 5) equity-curve & internal consistency check
# ----------------------------------------------------------------------------
results = sim.results()
delta_equity = results["equity"].iloc[-1] - results["equity"].iloc[0]
cum_pnl = trades["pnl"].dropna().sum()
print(
    f"\nΔ Equity = {delta_equity:,.0f}   "
    f"Σ PnL = {cum_pnl:,.0f}   "
    f"{'✔️' if abs(delta_equity - cum_pnl) < 1e-6 else '❌'}"
)

# ----------------------------------------------------------------------------
# 6) performance metrics
# ----------------------------------------------------------------------------
print("\n──── Performance summary ────")
pa = PerformanceAnalyzer(results)
print(pa.summarize())

# ----------------------------------------------------------------------------
# 7) (optional) save equity curve & trades to disk for further analysis
# ----------------------------------------------------------------------------
RESULTS_DIR = "outputs"
Path(RESULTS_DIR).mkdir(exist_ok=True)
results.to_csv(f"{RESULTS_DIR}/equity_curve_30d_5days.csv", index=False)
trades.to_csv(f"{RESULTS_DIR}/trades_30d_5days.csv", index=False)
print(f"\n📄 CSVs saved to: {RESULTS_DIR}/")
