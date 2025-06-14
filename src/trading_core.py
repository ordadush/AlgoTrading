#trading_core.py
class Trade:
    def __init__(self, symbol, entry_date, entry_price, stop_loss, take_profit):
        self.symbol = symbol
        self.entry_date = entry_date
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.exit_date = None
        self.exit_price = None
        self.closed = False
        self.reason = ""

    def close_trade(self, exit_date, exit_price, reason):
        self.exit_date = exit_date
        self.exit_price = exit_price
        self.closed = True
        self.reason = reason

    def profit_pct(self):
        if self.closed:
            return ((self.exit_price - self.entry_price) / self.entry_price) * 100
        return None

    def __repr__(self):
        status = "Closed" if self.closed else "Open"
        return (f"Trade({self.symbol} | Entry: {self.entry_price} on {self.entry_date} | "
                f"Exit: {self.exit_price} on {self.exit_date} | Status: {status} | "
                f"PnL: {self.profit_pct()}%)")


class Account:
    def __init__(self, balance=100_000):
        self.balance = balance
        self.trades = []

    def open_trade(self, trade: Trade):
        self.trades.append(trade)

    def close_trade(self, trade: Trade, exit_date, exit_price, reason):
        trade.close_trade(exit_date, exit_price, reason)
        pnl = trade.profit_pct() / 100
        self.balance *= (1 + pnl)

    def summary(self):
        wins = [t for t in self.trades if t.closed and t.exit_price > t.entry_price]
        losses = [t for t in self.trades if t.closed and t.exit_price <= t.entry_price]
        avg_win = sum(t.profit_pct() for t in wins) / len(wins) if wins else 0
        avg_loss = sum(t.profit_pct() for t in losses) / len(losses) if losses else 0
        win_rate = len(wins) / len(self.trades) * 100 if self.trades else 0

        return {
            "Total Trades": len(self.trades),
            "Balance": round(self.balance, 2),
            "Win Rate (%)": round(win_rate, 2),
            "Avg Win (%)": round(avg_win, 2),
            "Avg Loss (%)": round(avg_loss, 2),
        }


