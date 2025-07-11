from __future__ import annotations

import pandas as pd
from typing import List, Dict, Optional
from simulation.data_loaders import load_stocks


class BacktestSimulator:
    """
    Full backtest simulator engine.
    """

    def __init__(self,
                 signal_calendar: pd.DataFrame,
                 initial_cash: float = 1_000_000,
                 max_positions: int = 10,
                 fixed_size: float = 10_000):
        """
        Initialize simulator.

        Args:
            signal_calendar (pd.DataFrame): Signal DataFrame produced by generate_signal_calendar().
            initial_cash (float): Starting cash balance.
            max_positions (int): Max open positions allowed simultaneously.
            fixed_size (float): Fixed amount to allocate per position.
        """
        self.signals = signal_calendar.sort_values("date").reset_index(drop=True)
        self.initial_cash = initial_cash
        self.max_positions = max_positions
        self.fixed_size = fixed_size

        self.reset()

        # Load stock prices once
        self.stock_prices = self._load_stock_prices()

    def reset(self):
        """Clear all state before fresh run."""
        self.cash = self.initial_cash
        self.positions: Dict[str, Dict] = {}  # symbol → {entry_price, size}
        self.history = []
        self.trades = [] 

    def _load_stock_prices(self) -> pd.DataFrame:
        """
        Load full stock price history from DB for price access.
        """
        prices = load_stocks()
        prices = prices[["date", "symbol", "close"]]
        prices["date"] = pd.to_datetime(prices["date"])
        return prices.set_index(["date", "symbol"])["close"]

    def run(self):
        """
        Run full simulation over signal calendar.
        """
        for _, row in self.signals.iterrows():
            current_date = pd.to_datetime(row["date"])

            # Sell positions if signal no longer valid
            self._close_invalid_positions(current_date, row)

            # Open new positions if room available
            self._open_new_positions(current_date, row)

            # Record account state
            self._record_daily_state(current_date)

    def _get_price(self, date, symbol):
        while date >= self.stock_prices.index.get_level_values(0).min():
            try:
                return self.stock_prices.loc[(date, symbol)]
            except KeyError:
                date -= pd.Timedelta(days=1)  # fallback ליום קודם
        return None

        
    
    def _close_invalid_positions(self, date, signal_row):
        """
        Evaluate open positions and close ones that lost their signal.
        Records the trade with exit details and PnL.
        """
        to_close = []
        long_syms = signal_row["long_symbols"]
        short_syms = signal_row["short_symbols"]
        regime = signal_row.get("regime_signal", 0)

        for symbol in self.positions:
            position = self.positions[symbol]
            side = position["type"]

            # מניה איבדה את הסיגנל שלה (כמו קודם)
            signal_invalid = (
                (side == "long" and symbol not in long_syms) or
                (side == "short" and symbol not in short_syms)
            )

            # כיוון שוק מתנגד לפוזיציה (חדש)
            regime_invalid = (
                (side == "long" and regime not in {1, 2, 0}) or
                (side == "short" and regime not in {-1, -2, 0})
            )

            if signal_invalid or regime_invalid:
                to_close.append(symbol)

        for symbol in to_close:
            exit_price = self._get_price(date, symbol)
            if exit_price is None:
                continue  # cannot close without price

            position = self.positions.pop(symbol)
            qty = position["size"]
            entry = position["entry_price"]

            if position["type"] == "long":
                pnl = (exit_price - entry) * qty
            else:
                pnl = (entry - exit_price) * qty

            self.cash += qty * exit_price + pnl

            # --- log trade in trade history ---
            self.trades.append({
                "symbol": symbol,
                "side": position["type"],
                "entry_date": position["entry_date"],
                "entry_price": entry,
                "qty": qty,
                "exit_date": date,
                "exit_price": exit_price,
                "pnl": pnl,
                "sl_hit": False,
            })


    def _open_new_positions(self, date, signal_row):
        """
        Open new positions for today's signals (if room exists).
        """
        available_slots = self.max_positions - len(self.positions)
        if available_slots <= 0:
            return

        # Prioritize long signals first, then shorts
        candidates = []
        for sym in signal_row["long_symbols"]:
            if sym not in self.positions:
                candidates.append((sym, "long"))
        for sym in signal_row["short_symbols"]:
            if sym not in self.positions:
                candidates.append((sym, "short"))

        for symbol, direction in candidates[:available_slots]:
            price = self._get_price(date, symbol)
            if price is None or price <= 0:
                continue

            qty = self.fixed_size / price
            self.positions[symbol] = {
                "entry_price": price,
                "entry_date": date,
                "size": qty,
                "type": direction
            }
            self.cash -= qty * price

    def _record_daily_state(self, date):
        """
        Store daily snapshot of portfolio state.
        """
        equity = self.cash
        for symbol, pos in self.positions.items():
            price = self._get_price(date, symbol)
            if price is None:
                continue
            if pos["type"] == "long":
                equity += price * pos["size"]
            else:
                equity += (2 * pos["entry_price"] - price) * pos["size"]

        self.history.append({
            "date": date,
            "cash": self.cash,
            "positions": len(self.positions),
            "equity": equity
        })

    def results(self) -> pd.DataFrame:
        """
        Return full equity curve as DataFrame, including daily returns.
        """
        return_df = pd.DataFrame(self.history).set_index("date")
        return_df["daily_return"] = return_df["equity"].pct_change().fillna(0)
        return return_df