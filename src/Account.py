from datetime import datetime
from typing import List
from Trade import Trade 

class Account:
    def __init__(self, starting_balance: float = 0.0):
        self.balance = starting_balance
        self.open_trades: List[Trade] = []
        self.trade_history: List[Trade] = []

    def add_trade(self, trade: Trade, is_closed: bool = False):
        if is_closed:
            self.trade_history.append(trade)
            self.balance += trade.profit_and_lose
        else:
            cost = trade.entry_price * trade.quantity
            if self.balance < cost:
                raise ValueError("Insufficient balance to open this trade.")
            self.balance -= cost
            self.open_trades.append(trade)


    def close_trade(self, open_trade_index: int, exit_price: float, exit_date: datetime):
        trade = self.open_trades.pop(open_trade_index)
        closed_trade = Trade(
            symbol=trade.symbol,
            entry_date=trade.entry_date,
            exit_date=exit_date,
            entry_price=trade.entry_price,
            exit_price=exit_price,
            quantity=trade.quantity,
            direction=trade.direction
        )
        self.trade_history.append(closed_trade)
        
        investment = trade.entry_price * trade.quantity
        self.balance += investment + closed_trade.profit_and_lose


    @property
    def total_profit_and_lose(self) -> float:
        return sum(t.profit_and_lose for t in self.trade_history)

    @property
    def total_return_percent(self) -> float:
        if not self.trade_history:
            return 0.0
        total_invested = sum(t.entry_price * t.quantity for t in self.trade_history)
        return (self.total_profit_and_lose / total_invested) * 100 if total_invested else 0.0

    @property
    def win_rate(self) -> float:
        if not self.trade_history:
            return 0.0
        wins = sum(1 for t in self.trade_history if t.profit_and_lose > 0)
        return (wins / len(self.trade_history)) * 100

    def __repr__(self):
        return (f"Account(Balance: {self.balance:.2f}, Open Trades: {len(self.open_trades)}, "
                f"Trade History: {len(self.trade_history)}, Total PnL: {self.total_profit_and_lose:.2f}, "
                f"Win Rate: {self.win_rate:.1f}%)")
