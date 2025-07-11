from simulation.or_backtest_simulator import BacktestSimulator

class SL_Simulator(BacktestSimulator):
    HARD_SL_PCT = 0.03
    TSL_FACTOR = 0.8

    def reset(self):
        super().reset()
        self.trades = []

    def _open_new_positions(self, d, r):
        avail = self.max_positions - len(self.positions)
        candidates = (
            [(s, "long") for s in r["long_symbols"] if s not in self.positions] +
            [(s, "short") for s in r["short_symbols"] if s not in self.positions]
        )
        for sym, side in candidates[:avail]:
            px = self._get_price(d, sym)
            if px is None or px <= 0:
                continue
            qty = self.fixed_size / px
            sl = px * (1 - self.HARD_SL_PCT if side == "long" else 1 + self.HARD_SL_PCT)
            self.positions[sym] = dict(
                entry_price=px, size=qty, type=side,
                entry_date=d, stop_loss=sl, peak_price=px
            )
            self.cash -= qty * px
            self.trades.append(dict(
                symbol=sym, side=side, entry_date=d,
                entry_price=px, qty=qty,
                exit_date=None, exit_price=None,
                pnl=None, sl_hit=False
            ))

    def _record_daily_state(self, d):
        eq = self.cash
        exits = []
        for sym, pos in self.positions.items():
            px = self._get_price(d, sym)
            if px is None:
                continue

            if pos["type"] == "long":
                if px > pos["peak_price"]:
                    mv = px - pos["peak_price"]
                    pos["peak_price"] = px
                    pos["stop_loss"] = max(pos["stop_loss"], px - mv * self.TSL_FACTOR)
                if px <= pos["stop_loss"]:
                    exits.append((sym, px, True))
                eq += px * pos["size"]
            else:
                if px < pos["peak_price"]:
                    mv = pos["peak_price"] - px
                    pos["peak_price"] = px
                    pos["stop_loss"] = min(pos["stop_loss"], px + mv * self.TSL_FACTOR)
                if px >= pos["stop_loss"]:
                    exits.append((sym, px, True))
                eq += (2 * pos["entry_price"] - px) * pos["size"]

        for sym, px, sl in exits:
            pos = self.positions.pop(sym)
            qty = pos["size"]
            en = pos["entry_price"]
            side = pos["type"]

            if side == "long":
                pnl = (px - en) * qty
                self.cash += px * qty
            else:
                pnl = (en - px) * qty
                self.cash += qty * (2 * en - px)

            for rec in self.trades[::-1]:
                if rec["symbol"] == sym and rec["exit_date"] is None:
                    rec.update(exit_date=d, exit_price=px, pnl=pnl, sl_hit=sl)
                    break

        self.history.append(dict(date=d, cash=self.cash, positions=len(self.positions), equity=eq))
