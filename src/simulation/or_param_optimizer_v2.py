"""
Param Optimizer v2  ––  *FULL grid, parallel*
=============================================

• סורק 900 קומבינציות     (5×3×2×30)
• רץ במקביל על כל ליבות ה-CPU.
• שומר את כל פונקציונליות v2:
    – דו-בטא              – Look-back 1-30
    – Hard-SL 3 %         – Trailing-SL 0.8
• מדפיס TOP-N (Training)  ואז Validation.

הרצה:
> python -m simulation.or_param_optimizer_v2_parallel
"""
from __future__ import annotations

import itertools, os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Tuple, DefaultDict

import numpy as np
import pandas as pd

from simulation.or_backtest_engine import generate_signal_calendar
from simulation.or_backtest_simulator import BacktestSimulator
from simulation.or_performance_analyzer import PerformanceAnalyzer

# ---------------------------------------------------------------------------#
# CONFIG                                                                      #
# ---------------------------------------------------------------------------#
TRAIN_SPAN = ("2014-01-01", "2019-12-31")
VAL_SPAN   = ("2020-01-01", "2022-12-31")

INITIAL_CASH = 1_000_000
MAX_POS, FIXED_SIZE = 10, 10_000
TOP_N = 10

# full grid
BETA_WINDOWS  = [30, 60, 90, 180, 360]
TH_UP_LIST    = [1.05, 1.10, 1.20]
TH_DN_LO_LIST = [0.90, 0.85]
MIN_SIGNAL_DAYS = list(range(1, 31))          # 1-30

# ---------------------------------------------------------------------------#
@dataclass(frozen=True)
class ParamSet:
    window: int
    th_up: float
    th_dn_lo: float
    min_days: int

# ---------------------------------------------------------------------------#
class SL_Simulator(BacktestSimulator):
    HARD_SL_PCT = 0.03
    TSL_FACTOR  = 0.8

    def reset(self):
        super().reset()
        self.trades: List[dict] = []

    def _open_new_positions(self, date, row):
        avail = self.max_positions - len(self.positions)
        if avail <= 0:
            return
        cands = [(s, "long") for s in row["long_symbols"] if s not in self.positions] + \
                [(s, "short") for s in row["short_symbols"] if s not in self.positions]
        for sym, side in cands[:avail]:
            px = self._get_price(date, sym)
            if px is None or px <= 0:  continue
            qty = self.fixed_size / px
            sl  = px*(1-self.HARD_SL_PCT) if side=="long" else px*(1+self.HARD_SL_PCT)
            self.positions[sym] = dict(entry_price=px,size=qty,type=side,
                                       entry_date=date,stop_loss=sl,peak_price=px)
            self.cash -= qty*px
            self.trades.append(dict(symbol=sym,side=side,entry_date=date,
                                    entry_price=px,qty=qty,exit_date=None,
                                    exit_price=None,pnl=None,sl_hit=False))

    def _record_daily_state(self, date):
        equity, exits = self.cash, []
        for sym,pos in self.positions.items():
            px = self._get_price(date,sym);  # noqa
            if px is None: continue
            if pos["type"]=="long":
                if px>pos["peak_price"]:
                    move=px-pos["peak_price"]; pos["peak_price"]=px
                    pos["stop_loss"]=max(pos["stop_loss"],px-move*self.TSL_FACTOR)
                if px<=pos["stop_loss"]: exits.append((sym,px,True))
                equity += px*pos["size"]
            else:
                if px<pos["peak_price"]:
                    move=pos["peak_price"]-px; pos["peak_price"]=px
                    pos["stop_loss"]=min(pos["stop_loss"],px+move*self.TSL_FACTOR)
                if px>=pos["stop_loss"]: exits.append((sym,px,True))
                equity += (2*pos["entry_price"]-px)*pos["size"]
        for sym,px,sl_flag in exits:
            pos=self.positions.pop(sym); qty=pos["size"]; entry=pos["entry_price"]
            pnl=(px-entry)*qty if pos["type"]=="long" else (entry-px)*qty
            self.cash += px*qty if pos["type"]=="long" else (entry*2-px)*qty
            for rec in self.trades[::-1]:
                if rec["symbol"]==sym and rec["exit_date"] is None:
                    rec.update(exit_date=date,exit_price=px,pnl=pnl,sl_hit=sl_flag); break
        self.history.append(dict(date=date,cash=self.cash,
                                 positions=len(self.positions),equity=equity))

# ---------------------------------------------------------------------------#
def build_signals(start:str,end:str,p:ParamSet)->pd.DataFrame:
    raw=generate_signal_calendar(start,end,p.window,p.th_up,p.th_dn_lo)
    # dual-beta + lookback (בדיוק כמו ב-v2 המקורית)
    from pathlib import Path
    betas=pd.read_parquet(Path("data_cache")/"beta_calculation.parquet")
    betas=betas[(betas["date"]>=start)&(betas["date"]<=end)]
    up, dn = f"beta_up_{p.window}", f"beta_down_{p.window}"
    m_long,m_short=DefaultDict(list),DefaultDict(list)
    for _,r in betas.iterrows():
        if r[up]>=p.th_up and r[dn]<=p.th_dn_lo:
            m_long[pd.Timestamp(r["date"])].append(r["symbol"])
        if r[dn]>=p.th_up and r[up]<=p.th_dn_lo:
            m_short[pd.Timestamp(r["date"])].append(r["symbol"])
    longs,shorts=[],[]
    for d in raw["date"]:
        days=[d-pd.Timedelta(days=i) for i in range(p.min_days)]
        l=set(m_long.get(d,[])); s=set(m_short.get(d,[]))
        for past in days[1:]:
            l&=set(m_long.get(past,[])); s&=set(m_short.get(past,[]))
        longs.append(sorted(l)); shorts.append(sorted(s))
    return pd.DataFrame(dict(date=raw["date"],long_symbols=longs,short_symbols=shorts))

# ---------------------------------------------------------------------------#
def _evaluate(param:ParamSet,span:Tuple[str,str])->Dict:
    sigs=build_signals(span[0],span[1],param)
    sim=SL_Simulator(sigs,INITIAL_CASH,MAX_POS,FIXED_SIZE); sim.run()
    res=sim.results(); ana=PerformanceAnalyzer(res)
    sl_ratio=sum(t["sl_hit"] for t in sim.trades if t["exit_date"])/max(1,len(sim.trades))
    return dict(Sharpe=ana.sharpe_ratio(),CAGR=ana.cagr(),
                DD=ana.max_drawdown(),SL_ratio=sl_ratio)

# ---------------------------------------------------------------------------#
def _worker(args):
    """Helper for ProcessPool."""
    p, span = args
    return p, _evaluate(p, span)

# ---------------------------------------------------------------------------#
def run_grid() -> None:
    grid = [
        ParamSet(w, u, d, md)
        for w, u, d, md in itertools.product(
            BETA_WINDOWS, TH_UP_LIST, TH_DN_LO_LIST, MIN_SIGNAL_DAYS
        )
    ]

    print(f"💡 Running FULL grid ({len(grid)} combos) in parallel …")

    # Training phase
    with ProcessPoolExecutor(max_workers=os.cpu_count()) as ex:
        futures = [ex.submit(_worker, (p, TRAIN_SPAN)) for p in grid]
        train_rows = []
        for fut in as_completed(futures):
            p, metric = fut.result()
            train_rows.append(dict(params=p, **metric))

    df_train = (
        pd.DataFrame(train_rows)
        .dropna(subset=["Sharpe"])
        .sort_values("Sharpe", ascending=False)
        .reset_index(drop=True)
    )
    print(f"\n=== Train top-{TOP_N} ===")
    print(df_train.head(TOP_N)[["params", "Sharpe", "CAGR", "DD", "SL_ratio"]])

    # Validation phase
    best = df_train.head(TOP_N)["params"]
    with ProcessPoolExecutor(max_workers=min(TOP_N, os.cpu_count())) as ex:
        val_rows = [
            dict(params=p, **_evaluate(p, VAL_SPAN)) for p in best
        ]

    df_val = (
        pd.DataFrame(val_rows)
        .sort_values("Sharpe", ascending=False)
        .reset_index(drop=True)
    )

    print("\n=== Validation ===")
    print(df_val[["params", "Sharpe", "CAGR", "DD", "SL_ratio"]])

    # Extra full output
    pd.set_option("display.max_colwidth", None)
    pd.set_option("display.max_rows", None)
    print("\n=== FULL Validation Table ===")
    print(df_val)

    # Save results
    df_train.to_csv("train_results_v2.csv", index=False)
    df_val.to_csv("validation_results_v2.csv", index=False)

# ---------------------------------------------------------------------------#
if __name__=="__main__":
    run_grid()
