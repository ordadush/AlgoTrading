#Trade.py
from datetime import datetime

class Trade:
    def __init__(self, symbol: str, entry_date: datetime, exit_date: datetime,
                 entry_price: float, exit_price: float, quantity: int,
                 direction: str = "long"):
        self.symbol = symbol
        self.entry_date = entry_date
        self.exit_date = exit_date
        self.entry_price = entry_price
        self.exit_price = exit_price
        self.quantity = quantity
        self.direction = direction.lower()  # "long" or "short"
        

    @property
    def profit_and_lose(self) -> float:
        """Profit and Lose."""
        multiplier = 1 if self.direction == "long" else -1
        return multiplier * (self.exit_price - self.entry_price) * self.quantity

    @property
    def return_percent(self) -> float:
        """Return percentage of the trade."""
        multiplier = 1 if self.direction == "long" else -1
        return multiplier * ((self.exit_price - self.entry_price) / self.entry_price) * 100

    @property
    def duration_of_trade(self) -> int:
        """Duration of the trade in days."""
        return (self.exit_date - self.entry_date).days

    def __repr__(self):
        return (f"Trade({self.symbol}, {self.entry_date.date()} -> {self.exit_date.date()}, "
                f"{self.direction.upper()}, Entry: {self.entry_price}, Exit: {self.exit_price}, "
                f"Qty: {self.quantity}, PnL: {self.profit_and_lose:.2f}, Return: {self.return_percent:.2f}%)")
