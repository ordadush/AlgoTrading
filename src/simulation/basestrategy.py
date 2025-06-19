"""strategy_framework.py – generic, highly‑configurable strategy skeleton

This module provides a **professional‑grade skeleton** for building systematic
trading strategies that need to be:

* **Parameter‑rich** – every aspect (windows, thresholds, risk rules) comes
  from a dataclass config object that you can tweak per run or via grid search.
* **Extensible** – write a new strategy by inheriting from `BaseStrategy` and
  implementing two small methods: `calc_entry()` and `calc_exit()` (or override
  more hooks if you need advanced behaviour).
* **Self‑contained risk management** – stop‑loss / take‑profit / trailing /
  max holding‑period handled in the base‑class but overrideable.

It **does not** run a full back‑test loop (that will live in
`backtest/engine.py`).  Instead it focuses on **signal generation & position
management**.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Optional, Any

import pandas as pd

# ---------------------------------------------------------------------------
# 1. Parameter containers – every ‘knob’ lives here
# ---------------------------------------------------------------------------

@dataclass
class WindowParams:
    """All rolling‑window lengths the strategy might need."""
    beta_up: int = 30  # e.g. use column `beta_up_30`
    beta_down: int = 30
    market_score: int = 1  # look‑back window for market regime aggregation

@dataclass
class RiskParams:
    """Rules for risk / trade management."""

    stop_loss_pct: float = 0.05          # 5 % stop‑loss from entry
    take_profit_pct: float = 0.10        # 10 % target
    trailing_stop_pct: Optional[float] = 0.03  # set None to disable
    max_holding_days: Optional[int] = None     # close after N bars regardless

@dataclass
class StrategyConfig:
    """The full config object passed to every Strategy instance."""

    windows: WindowParams = field(default_factory=WindowParams)
    risk: RiskParams = field(default_factory=RiskParams)

    # free‑form dictionaries so every concrete strategy can add its own knobs
    entry_params: Dict[str, Any] = field(default_factory=dict)
    exit_params: Dict[str, Any] = field(default_factory=dict)

# ---------------------------------------------------------------------------
# 2. Position helper
# ---------------------------------------------------------------------------

@dataclass
class Position:
    direction: int                    # 1 = long, −1 = short
    entry_price: float
    entry_date: pd.Timestamp

    stop_loss: float
    take_profit: float
    trailing_stop: Optional[float]

    max_price_seen: float = field(default=0.0)  # for trailing‑stop update
    holding_days: int = field(default=0)

# ---------------------------------------------------------------------------
# 3. BaseStrategy – inherit from this to create new strategies
# ---------------------------------------------------------------------------

class BaseStrategy(ABC):
    """Abstract base‑class every concrete strategy must inherit from."""

    def __init__(self, cfg: StrategyConfig):
        self.cfg = cfg
        self.pos: Optional[Position] = None

    # ----- Hooks developers must implement ---------------------------------
    @abstractmethod
    def calc_entry(self, market: pd.Series, stock: pd.Series) -> Optional[int]:
        """Return **1** for long entry, **−1** for short entry, or **None**."""

    @abstractmethod
    def calc_exit(self, market: pd.Series, stock: pd.Series) -> bool:
        """Return **True** if strategy wants to exit today (beyond risk rules)."""

    # ----- Risk‑rule helpers (override only if you need custom logic) -------
    def _update_trailing(self, pos: Position, bar_high: float):
        if self.cfg.risk.trailing_stop_pct is None:
            return
        pos.max_price_seen = max(pos.max_price_seen, bar_high)
        pos.trailing_stop = pos.max_price_seen * (1 - self.cfg.risk.trailing_stop_pct)

    def _should_stop(self, pos: Position, bar_low: float, bar_high: float) -> bool:
        if bar_low <= pos.stop_loss:
            return True
        if bar_high >= pos.take_profit:
            return True
        if pos.trailing_stop is not None and bar_low <= pos.trailing_stop:
            return True
        if (self.cfg.risk.max_holding_days is not None and
            pos.holding_days >= self.cfg.risk.max_holding_days):
            return True
        return False

    # ----- Main per‑bar driver ---------------------------------------------
    def on_bar(self, market: pd.Series, stock: pd.Series) -> Dict[str, Any]:
        """Call this **once per trading day** for each symbol.

        Returns a dict with optional keys:
        {"action": "BUY" | "SELL", "price": float}
        """
        signal: Dict[str, Any] = {}

        # 1. Existing position – update risk & check exit
        if self.pos is not None:
            self.pos.holding_days += 1
            self._update_trailing(self.pos, stock['high'])
            if self._should_stop(self.pos, stock['low'], stock['high']) or \
               self.calc_exit(market, stock):
                signal = {"action": "SELL", "price": stock['open']}
                self.pos = None
            return signal  # either we sold or we hold – no new entry today

        # 2. No position – check for entry
        direction = self.calc_entry(market, stock)
        if direction is None:
            return signal  # no entry today

        entry_price = stock['open']
        stop_loss = entry_price * (1 - self.cfg.risk.stop_loss_pct * direction)
        take_profit = entry_price * (1 + self.cfg.risk.take_profit_pct * direction)
        trailing = None
        if self.cfg.risk.trailing_stop_pct is not None:
            trailing = entry_price * (1 - self.cfg.risk.trailing_stop_pct * direction)

        self.pos = Position(
            direction=direction,
            entry_price=entry_price,
            entry_date=stock['date'],
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_stop=trailing,
            max_price_seen=entry_price,
        )
        signal = {"action": "BUY" if direction == 1 else "SELL", "price": entry_price}
        return signal

# ---------------------------------------------------------------------------
# 4. Example concrete implementation – Relative‑Strength + Beta
# ---------------------------------------------------------------------------

class RelativeBetaStrategy(BaseStrategy):
    """Demo strategy showing how to plug into the framework."""

    def calc_entry(self, market: pd.Series, stock: pd.Series) -> Optional[int]:
        # Require positive market regime
        if market['score'] < self.cfg.entry_params.get('min_market_score', 1):
            return None

        # beta_up window selection
        w = self.cfg.windows.beta_up
        beta_col = f"beta_up_{w}"
        if stock[beta_col] is None or stock[beta_col] <= self.cfg.entry_params.get('min_beta', 0):
            return None

        # simple example: always long when criteria met
        return 1

    def calc_exit(self, market: pd.Series, stock: pd.Series) -> bool:
        # exit if market turns negative
        return market['score'] <= self.cfg.exit_params.get('market_exit_threshold', -1)

# ---------------------------------------------------------------------------
# 5. Factory helper – get strategy by name ----------------------------------
# ---------------------------------------------------------------------------

STRATEGY_REGISTRY = {
    'relative_beta': RelativeBetaStrategy,
}

def create_strategy(name: str, cfg: StrategyConfig):
    if name not in STRATEGY_REGISTRY:
        raise KeyError(f"Unknown strategy '{name}'. Available: {list(STRATEGY_REGISTRY)}")
    return STRATEGY_REGISTRY[name](cfg)
