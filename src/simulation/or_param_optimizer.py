"""
Param Optimizer (C4) – two-stage grid-search with Train / Validation split
=========================================================================

Goal
----
1. **Train step (2014-01-01 → 2019-12-31)**
   • sweep a parameter-grid and rank each configuration by performance  
   • keep the *N* best candidates (highest Sharpe¹)

2. **Validation step (2020-01-01 → 2022-12-31)**
   • re-run only the *N* candidates on a fresh, unseen slice  
   • pick the configuration that stays strong – i.e. *robust* not just
     “lucky” on the train set.

No touch of the Test period (2023-01-01 → 2024-12-31) yet – we keep it
clean for a final, one-shot evaluation later.

¹ Feel free to swap the ranking key (Sharpe, CAGR, etc.) – the code is
modular.

---------------------------------------------------------------------------
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from simulation.or_backtest_engine import generate_signal_calendar
from simulation.or_backtest_simulator import BacktestSimulator
from simulation.or_performance_analyzer import PerformanceAnalyzer


# ---------------------------------------------------------------------------
# Configuration data-class
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ParamSet:
    beta_window: int
    beta_up_th: float
    beta_dn_th: float
    min_days_strong: int = 0  # reserved for future use


# ---------------------------------------------------------------------------
# Core optimiser
# ---------------------------------------------------------------------------
class ParamOptimizer:
    """
    Two-stage (train / validation) parameter grid optimisation.

    Attributes
    ----------
    train_span : tuple[str, str]
        (start, end) for training period  –  YYYY-MM-DD
    val_span   : tuple[str, str]
        (start, end) for validation period – YYYY-MM-DD
    """

    # ---- grid definition ---------------------------------------------------
    WINDOWS      = [30, 60, 90, 180]
    BETA_UP_TH   = [1.05, 1.10, 1.20]
    BETA_DN_TH   = [1.05, 1.10, 1.20]
    TOP_N        = 10                      # keep N best from training

    def __init__(
        self,
        train_span: Tuple[str, str] = ("2014-01-01", "2019-12-31"),
        val_span: Tuple[str, str] = ("2020-01-01", "2022-12-31"),
        cash: float = 1_000_000,
    ):
        self.train_span = train_span
        self.val_span = val_span
        self.initial_cash = cash

        self.train_results: pd.DataFrame | None = None
        self.val_results: pd.DataFrame | None = None

    # ------------------------------------------------------------------- #
    # public API
    # ------------------------------------------------------------------- #
    def run(self) -> None:
        """Run full optimisation (train → pick top-N → validation)."""
        self._run_train()
        self._run_validation()
        self._report_summary()

    # ------------------------------------------------------------------- #
    # implementation details
    # ------------------------------------------------------------------- #
    def _run_train(self) -> None:
        """Grid-search on training slice."""
        rows: List[Dict] = []

        param_grid = [
            ParamSet(w, up, dn)
            for w, up, dn in product(
                self.WINDOWS,
                self.BETA_UP_TH,
                self.BETA_DN_TH,
            )
        ]

        for p in param_grid:
            train_metrics = self._backtest(
                p,
                span=self.train_span,
            )
            rows.append(
                {
                    "params": p,
                    **train_metrics,
                }
            )

        # rank
        df = pd.DataFrame(rows)
        df = df.sort_values("Sharpe", ascending=False).reset_index(drop=True)
        self.train_results = df

    def _run_validation(self) -> None:
        """Re-evaluate TOP_N configs on validation slice."""
        assert self.train_results is not None, "run train first!"

        top_params = self.train_results.head(self.TOP_N)["params"]

        rows: List[Dict] = []
        for p in top_params:
            val_metrics = self._backtest(
                p,
                span=self.val_span,
            )
            rows.append(
                {
                    "params": p,
                    **val_metrics,
                }
            )

        df = pd.DataFrame(rows)
        df = df.sort_values("Sharpe", ascending=False).reset_index(drop=True)
        self.val_results = df

    # ------------------------------------------------------------------- #
    # helper
    # ------------------------------------------------------------------- #
    def _backtest(self, p: ParamSet, span: Tuple[str, str]) -> Dict:
        """Run signals + simulation + analysis for a given ParamSet & span."""
        sigs = generate_signal_calendar(
            start_date=span[0],
            end_date=span[1],
            beta_window=p.beta_window,
            beta_up_threshold=p.beta_up_th,
            beta_down_threshold=p.beta_dn_th,
        )

        sim = BacktestSimulator(
            sigs,
            initial_cash=self.initial_cash,
            max_positions=10,
            fixed_size=10_000,
        )
        sim.run()
        res = sim.results()

        ana = PerformanceAnalyzer(res)
        return {
            "CAGR": ana.cagr(),
            "Sharpe": ana.sharpe_ratio(),
            "DD": ana.max_drawdown(),
            "TotRet": ana.total_return(),
        }

    def _report_summary(self) -> None:
        """Print nicely both tables."""
        print("\n=== Training results (top-{} by Sharpe) ===".format(self.TOP_N))
        print(
            self.train_results[
                ["params", "Sharpe", "CAGR", "DD"]
            ].head(self.TOP_N).to_string(index=False)
        )

        print("\n=== Validation results for those configs ===")
        print(
            self.val_results[
                ["params", "Sharpe", "CAGR", "DD"]
            ].to_string(index=False)
        )


# ---------------------------------------------------------------------------
# CLI helper
# ---------------------------------------------------------------------------
def _main():
    opt = ParamOptimizer()
    opt.run()


if __name__ == "__main__":
    _main()
