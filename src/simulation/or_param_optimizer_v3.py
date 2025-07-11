"""
Param Optimizer v3  ––  *Coarse grid + Quick-Reject, parallel*
=============================================================

• סורק רק 60 קומבינציות (5×2×1×6) ← רזולוציה גסה  
• מדלג מוקדם על קומבינציות עם <50 עסקאות אימון  
• רץ במקביל על כל הליבות
• מדפיס TOP-N + Validation כמו v2.

הרצה:
> python -m simulation.or_param_optimizer_v3_parallel
"""
from __future__ import annotations
import itertools, os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Tuple
import pandas as pd
from collections import defaultdict
from pathlib import Path
import numpy as np

from simulation.or_backtest_engine import generate_signal_calendar
from simulation.or_sl_simulator import SL_Simulator
from simulation.or_performance_analyzer import PerformanceAnalyzer

# ---------- CONFIG ---------------------------------------------------------#
TRAIN_SPAN=("2014-01-01","2019-12-31"); VAL_SPAN=("2020-01-01","2022-12-31")
INITIAL_CASH=1_000_000; MAX_POS,FIXED_SIZE=10,10_000; TOP_N=10
# BETA_WINDOWS=[30,60,90,180,360]; TH_UP=[1.05,1.15]; TH_DN_LO=[0.9]
# MIN_SIGNAL=[1,3,5,10,20,30]; MIN_TRADES=50
BETA_WINDOWS=[30,60,90]; TH_UP=[1.3, 1.4, 1.5]; TH_DN_LO=[0.9, 0.8]
MIN_SIGNAL=[3,5,10]; MIN_TRADES=50


@dataclass(frozen=True)
class ParamSet:
    window:int; th_up:float; th_dn_lo:float; min_days:int

# ---------- Signal builder (dual-beta + look-back) -------------------------#
def build_signals(start:str,end:str,p:ParamSet)->pd.DataFrame:
    raw=generate_signal_calendar(start,end,p.window,p.th_up,p.th_dn_lo)
    betas=pd.read_parquet(Path("data_cache")/"beta_calculation.parquet")
    betas=betas[(betas["date"]>=start)&(betas["date"]<=end)]
    up,dn=f"beta_up_{p.window}",f"beta_down_{p.window}"
    mL,mS=defaultdict(list),defaultdict(list)
    for _,r in betas.iterrows():
        if r[up]>=p.th_up and r[dn]<=p.th_dn_lo: mL[pd.Timestamp(r["date"])].append(r["symbol"])
        if r[dn]>=p.th_up and r[up]<=p.th_dn_lo: mS[pd.Timestamp(r["date"])].append(r["symbol"])
    L,S=[],[]
    for d in raw["date"]:
        dates=[d-pd.Timedelta(days=i) for i in range(p.min_days)]
        ls=set(mL.get(d,[])); ss=set(mS.get(d,[]))
        for past in dates[1:]:
            ls&=set(mL.get(past,[])); ss&=set(mS.get(past,[]))
        L.append(sorted(ls)); S.append(sorted(ss))
    return pd.DataFrame(dict(date=raw["date"],long_symbols=L,short_symbols=S))

# ---------- evaluation helper ---------------------------------------------#
def _evaluate(p:ParamSet,span)->Dict|None:
    sigs=build_signals(span[0],span[1],p)
    if span==TRAIN_SPAN:
        trades=sigs["long_symbols"].str.len().sum()+sigs["short_symbols"].str.len().sum()
        if trades<MIN_TRADES: return None
    sim=SL_Simulator(sigs,INITIAL_CASH,MAX_POS,FIXED_SIZE); sim.run()
    res=sim.results(); ana=PerformanceAnalyzer(res)
    sl_ratio=sum(t["sl_hit"]for t in sim.trades if t["exit_date"])/max(1,len(sim.trades))
    return dict(Sharpe=ana.sharpe_ratio(),CAGR=ana.cagr(),DD=ana.max_drawdown(),SL_ratio=sl_ratio)

# ---------- Parallel grid --------------------------------------------------#
def run_grid():
    grid = [
        ParamSet(w, u, 0.9, md)
        for w, u, md in itertools.product(BETA_WINDOWS, TH_UP, MIN_SIGNAL)
    ]
    print(f"⚡ Running COARSE grid ({len(grid)} combos) w/ quick-reject …")

    # Training phase (parallel)
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as ex:
        fut = [ex.submit(_evaluate, p, TRAIN_SPAN) for p in grid]
        train = [
            dict(params=p, **m)
            for p, m in ((p, res.result()) for p, res in zip(grid, fut)) if m
        ]

    df_tr = (
        pd.DataFrame(train)
        .dropna(subset=["Sharpe"])
        .sort_values("Sharpe", ascending=False)
        .reset_index(drop=True)
    )
    print(f"\n— Train top-{TOP_N} —")
    print(df_tr.head(TOP_N)[["params", "Sharpe", "CAGR", "DD", "SL_ratio"]])

    # Validation phase (parallel)
    best = df_tr.head(TOP_N)["params"]
    with ProcessPoolExecutor(max_workers=min(TOP_N, os.cpu_count())) as ex:
        val = [dict(params=p, **_evaluate(p, VAL_SPAN)) for p in best]

    df_val = (
        pd.DataFrame(val)
        .sort_values("Sharpe", ascending=False)
        .reset_index(drop=True)
    )
    print("\n— Validation —")
    print(df_val[["params", "Sharpe", "CAGR", "DD", "SL_ratio"]])

    # Extra: print full table (all columns, no row limit)
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.max_rows", None)
    print("\n=== FULL Validation Table ===")
    print(df_val)
    
    df_val.to_csv("validation_results_v3.csv", index=False)
    df_tr.to_csv("train_results_v3.csv", index=False)


    # === Run best X configs and log their trades/results ===
    BEST_N = 3
    outdir = Path("outputs/best_configs")
    outdir.mkdir(parents=True, exist_ok=True)

    for i, row in enumerate(df_val.head(BEST_N).itertuples(), 1):
        p: ParamSet = row.params
        print(f"\n📊 Re-running best config #{i}: {p}")

        # Run on full span: 2014–2022
        sigs = build_signals("2014-01-01", "2022-12-31", p)
        sim = SL_Simulator(sigs, INITIAL_CASH, MAX_POS, FIXED_SIZE)
        sim.run()

        trades = pd.DataFrame(sim.trades)
        equity = sim.results()
        pa = PerformanceAnalyzer(equity)

        path = outdir / f"config_{i}_{str(p).replace(' ', '').replace(',', '_')}"
        path.mkdir(parents=True, exist_ok=True)

        trades.to_csv(path / "trades.csv", index=False)
        equity.to_csv(path / "equity.csv", index=False)

        print(pa.summarize())




# ---------- CLI ------------------------------------------------------------#
if __name__=="__main__":
    from concurrent.futures import ProcessPoolExecutor
    run_grid()
