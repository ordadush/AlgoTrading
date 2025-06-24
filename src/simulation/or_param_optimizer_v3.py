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
from simulation.or_backtest_simulator import BacktestSimulator
from simulation.or_performance_analyzer import PerformanceAnalyzer

# ---------- CONFIG ---------------------------------------------------------#
TRAIN_SPAN=("2014-01-01","2019-12-31"); VAL_SPAN=("2020-01-01","2022-12-31")
INITIAL_CASH=1_000_000; MAX_POS,FIXED_SIZE=10,10_000; TOP_N=10
BETA_WINDOWS=[30,60,90,180,360]; TH_UP=[1.05,1.15]; TH_DN_LO=[0.9]
MIN_SIGNAL=[1,3,5,10,20,30]; MIN_TRADES=50

@dataclass(frozen=True)
class ParamSet:
    window:int; th_up:float; th_dn_lo:float; min_days:int

# ---------- Simulator (as in v2) ------------------------------------------#
class SL_Simulator(BacktestSimulator):
    HARD_SL_PCT=0.03; TSL_FACTOR=0.8
    def reset(self): super().reset(); self.trades=[]
    def _open_new_positions(self,d,r):
        avail=self.max_positions-len(self.positions)
        c=[(s,"long")for s in r["long_symbols"] if s not in self.positions]+\
          [(s,"short")for s in r["short_symbols"] if s not in self.positions]
        for sym,side in c[:avail]:
            px=self._get_price(d,sym); 
            if px is None or px<=0: continue
            qty=self.fixed_size/px; sl=px*(1-self.HARD_SL_PCT if side=="long" else 1+self.HARD_SL_PCT)
            self.positions[sym]=dict(entry_price=px,size=qty,type=side,entry_date=d,stop_loss=sl,peak_price=px)
            self.cash-=qty*px
            self.trades.append(dict(symbol=sym,side=side,entry_date=d,entry_price=px,qty=qty,
                                    exit_date=None,exit_price=None,pnl=None,sl_hit=False))
    def _record_daily_state(self,d):
        eq=self.cash; exits=[]
        for sym,pos in self.positions.items():
            px=self._get_price(d,sym); 
            if px is None: continue
            if pos["type"]=="long":
                if px>pos["peak_price"]:
                    mv=px-pos["peak_price"]; pos["peak_price"]=px
                    pos["stop_loss"]=max(pos["stop_loss"],px-mv*self.TSL_FACTOR)
                if px<=pos["stop_loss"]: exits.append((sym,px,True))
                eq+=px*pos["size"]
            else:
                if px<pos["peak_price"]:
                    mv=pos["peak_price"]-px; pos["peak_price"]=px
                    pos["stop_loss"]=min(pos["stop_loss"],px+mv*self.TSL_FACTOR)
                if px>=pos["stop_loss"]: exits.append((sym,px,True))
                eq+=(2*pos["entry_price"]-px)*pos["size"]
        for sym, px, sl in exits:
            pos=self.positions.pop(sym)
            qty = pos["size"]
            en = pos["entry_price"]
            side = pos["type"]

            if side == "long":
                pnl = (px - en) * qty
                self.cash += px * qty
            else:  # short
                pnl = (en - px) * qty
                self.cash += qty * (2 * en - px)

            for rec in self.trades[::-1]:
                if rec["symbol"] == sym and rec["exit_date"] is None:
                    rec.update(exit_date=d, exit_price=px, pnl=pnl, sl_hit=sl)
                    break

        self.history.append(dict(date=d,cash=self.cash,positions=len(self.positions),equity=eq))

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




# ---------- CLI ------------------------------------------------------------#
if __name__=="__main__":
    from concurrent.futures import ProcessPoolExecutor
    run_grid()
