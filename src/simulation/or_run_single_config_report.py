"""
Run best-config back-test on TRAIN+VALID (2014-2022) and give
detailed yearly trade statistics.

Usage
-----
> python -m simulation.or_run_single_config_report
"""
from __future__ import annotations

import pandas as pd
from collections import defaultdict

# ---- imports מהקוד הקיים שלך ---------------------------------------------
from simulation.or_param_optimizer_v3 import (
    ParamSet,                     # מחלקת הפרמטרים
    build_signals,                # פונקציית יצירת הסיגנלים עם look-back
    SL_Simulator,                 # הסימולטור עם Hard-SL + Trailing-SL
    INITIAL_CASH, MAX_POS, FIXED_SIZE,
)

from simulation.or_performance_analyzer import PerformanceAnalyzer

# ---- הקונפיגורציה שנבחרה --------------------------------------------------
BEST_PARAMS = ParamSet(
    window    = 90,
    th_up     = 1.05,
    th_dn_lo  = 0.90,
    min_days  = 3,
)

SPAN = ("2014-01-01", "2022-12-31")   # TRAIN + VALIDATION

# ---------------------------------------------------------------------------
def main() -> None:
    print("► Generating signals …")
    sigs = build_signals(SPAN[0], SPAN[1], BEST_PARAMS)
    print(f"  Loaded {len(sigs):,} trading days.")

    print("► Running simulation …")
    sim = SL_Simulator(
        signal_calendar=sigs,
        initial_cash=INITIAL_CASH,
        max_positions=MAX_POS,
        fixed_size=FIXED_SIZE,
    )
    sim.run()
    equity = sim.results()

    # -------- Performance summary -----------------------------------------
    print("\n=== Portfolio Performance ===")
    PerformanceAnalyzer(equity).summarize()

    # -------- Yearly trade analysis ---------------------------------------
    yearly = defaultdict(lambda: dict(trades=0, winners=0, pnl=0.0, sl=0))
    for tr in sim.trades:
        if tr["exit_date"] is None:           # עסקה עדיין פתוחה
            continue
        yr = tr["exit_date"].year
        yearly[yr]["trades"] += 1
        yearly[yr]["pnl"]    += tr["pnl"]
        if tr["pnl"] > 0:
            yearly[yr]["winners"] += 1
        if tr.get("sl_hit"):
            yearly[yr]["sl"] += 1

    rows = []
    for yr in sorted(yearly):
        st = yearly[yr]
        win_rate = st["winners"] / st["trades"] if st["trades"] else 0
        rows.append(dict(
            Year        = yr,
            Trades      = st["trades"],
            Winners     = st["winners"],
            WinRate     = f"{win_rate*100:.1f}%",
            SL_Hits     = st["sl"],
            Total_PnL   = f"${st['pnl']:,.0f}",
            Avg_PnL     = f"${st['pnl']/st['trades'] if st['trades'] else 0:,.2f}",
        ))

    df_yearly = pd.DataFrame(rows)
    print("\n=== Year-by-Year Trade Stats ===")
    print(df_yearly.to_string(index=False))

if __name__ == "__main__":
    main()
