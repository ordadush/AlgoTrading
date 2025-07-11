
# import backtrader as bt
# import pandas as pd
# from btIndicators import BetaStrength30        # ודא שהנתיב נכון

# class MyStrengthStrat(bt.Strategy):
#     params = dict(
#         # === פרמטרים כלליים ==================================================
#         end_dates=None,              # {symbol: last_date}  (optional)
#         strength_thresh=0.015,       # סף אינדיקטור כוח
#         pct_per_trade=5,             # % מהמזומן הפנוי
#         stop_loss_pct=0.01,          # 1 %  SL
#         take_profit_pct=0.02,        # 2 %  TP
#         max_daily_buys=10,           # מקס' קניות ביום
#         cooldown=5,                  # 5 ימי "קירור"
#         # === פרמטרים לקובץ השוק ==============================================
#         market_csv='../markov_fft_com_old_data.csv',    # שם הקובץ
#         market_col='regime_signal_combined'  # שם העמודה עם הערך –2…+2
#     )

#     # ------------------------------------------------------------------------
#     def log(self, txt, dt=None):
#         dt = dt or self.datas[0].datetime.date(0)
#         print(f'{dt.isoformat()} - {txt}')

#     # ------------------------------------------------------------------------
#     def __init__(self):
#         # ---------- 0) טעינת קובץ מצב-שוק -----------------------------------
#         df_market = pd.read_csv(
#             self.p.market_csv,
#             usecols=['date', self.p.market_col],
#             parse_dates=['date']
#         )
#         self.portfolio_values = []

#         self.market_signal = {
#             d.date(): s
#             for d, s in zip(df_market['date'], df_market[self.p.market_col])
#         }
#         # --------------------------------------------------------------------

#         self.market_feed    = self.datas[-1]     # נתון שוק (אם יש)
#         self.strength       = {}                 # {sym: indicator}
#         self.entry_prices   = {}                 # {sym: price}
#         self.pending_orders = {}                 # {sym: order | None}
#         self.last_exit_date = {}                 # {sym: date}  – קירור

#         for d in self.datas[:-1]:
#             sym = d._name
#             self.strength[sym]    = BetaStrength30(d, self.market_feed)
#             self.pending_orders[sym] = None

#     # ------------------------------------------------------------------------
#     def notify_order(self, order):
#         sym = order.data._name
#         if order.status in [order.Completed,
#                             order.Canceled,
#                             order.Margin,
#                             order.Rejected]:
#             self.pending_orders[sym] = None

#     # ------------------------------------------------------------------------
#     def notify_trade(self, trade):
#         if trade.isclosed:
#             sym = trade.data._name
#             self.last_exit_date[sym] = self.datas[0].datetime.date(0)
#             self.log(f"TRADE CLOSED {sym}  PnL: {trade.pnl:.2f}")

#     # ------------------------------------------------------------------------
#     def next(self):
#         today      = self.datas[0].datetime.date(0)
#         mkt_score  = self.market_signal.get(today, 0)   # ברירת־מחדל 0
#         buys_today = 0
#         self.portfolio_values.append(self.broker.getvalue())


#         # ----------- יציאה יזומה בגלל שוק שלילי -----------------------------
#         if mkt_score <= -1:
#             for d in self.datas[:-1]:
#                 if self.getposition(d):
#                     self.close(data=d)
#                     self.log(f"MARKET EXIT {d._name}  (score={mkt_score})")
#             # אין טעם לנסות קניות ביום שלילי
#             return

#         # ----------- המשך לוגיקה רגילה (ועם סינון קנייה) ---------------------
#         for d in self.datas[:-1]:
#             sym   = d._name
#             score = self.strength[sym].score[0]
#             pos   = self.getposition(d)

#             # ① Forced sell ביום הדאטה האחרון
#             last_day = self.p.end_dates.get(sym) if self.p.end_dates else None
#             if last_day and today >= last_day.date():
#                 if pos:
#                     self.close(data=d)
#                     self.log(f"FORCED SELL {sym} (end date)")
#                 continue

#             # ② ניהול SL / TP
#             if pos:
#                 entry = self.entry_prices.get(sym, d.close[0])
#                 px    = d.close[0]
#                 if px <= entry * (1 - self.p.stop_loss_pct):
#                     self.close(data=d)
#                     self.log(f"STOP LOSS  {sym} @ {px:.2f}")
#                     continue
#                 if px >= entry * (1 + self.p.take_profit_pct):
#                     self.close(data=d)
#                     self.log(f"TAKE PROFIT {sym} @ {px:.2f}")
#                     continue

#             # ③ תנאי כניסה (+ קירור + מגבלת BUY יומית + שוק חיובי)
#             if (mkt_score >= 1 and                    # ⇽ NEW  ✔ שוק חיובי
#                 not pos and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score > self.p.strength_thresh and
#                 buys_today < self.p.max_daily_buys):

#                 # קירור
#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 # גודל עסקה לפי מזומן פנוי
#                 cash_free  = self.broker.getcash()
#                 target_val = (self.p.pct_per_trade / 100.0) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 o = self.buy(data=d, size=size)
#                 self.pending_orders[sym] = o
#                 self.entry_prices[sym]   = d.close[0]
#                 buys_today += 1
#                 self.log(f"BUY {sym} @ {d.close[0]:.2f}  size={size:.0f}")



#עם סטופ לוס נע 

# import backtrader as bt
# import pandas as pd
# from btIndicators import BetaStrength30   # ודא שהנתיב נכון

# class MyStrengthStrat(bt.Strategy):
#     params = dict(
#         # === כלליים =========================================================
#         end_dates=None,
#         strength_thresh=0.015,
#         pct_per_trade=5,
#         stop_loss_pct=0.03,          # ⭐ 2 % Trailing SL
#         take_profit_pct=0.02,
#         max_daily_buys=10,
#         cooldown=5,
#         # === קובץ מצב-שוק ====================================================
#         market_csv='../markov_fft_com_old_data.csv',
#         market_col='regime_signal_combined'
#     )

#     # -----------------------------------------------------------------------
#     def log(self, txt, dt=None):
#         dt = dt or self.datas[0].datetime.date(0)
#         print(f'{dt.isoformat()} - {txt}')

#     # -----------------------------------------------------------------------
#     def __init__(self):
#         # 0) טוען את קובץ מצב-השוּק
#         df_mkt = pd.read_csv(
#             self.p.market_csv,
#             usecols=['date', self.p.market_col],
#             parse_dates=['date']
#         )
#         self.market_signal = {
#             d.date(): s
#             for d, s in zip(df_mkt['date'], df_mkt[self.p.market_col])
#         }

#         self.market_feed     = self.datas[-1]
#         self.strength        = {}
#         self.pending_orders  = {}
#         self.entry_prices    = {}
#         self.highest_price   = {}   # ⭐ שיא מאז פתיחה (ל-Trailing SL)
#         self.last_exit_date  = {}

#         for d in self.datas[:-1]:
#             sym = d._name
#             self.strength[sym]        = BetaStrength30(d, self.market_feed)
#             self.pending_orders[sym]  = None
#             self.highest_price[sym]   = None

#     # -----------------------------------------------------------------------
#     def notify_order(self, order):
#         sym = order.data._name
#         if order.status in [order.Completed,
#                             order.Canceled,
#                             order.Margin,
#                             order.Rejected]:
#             self.pending_orders[sym] = None

#     # -----------------------------------------------------------------------
#     def notify_trade(self, trade):
#         if trade.isclosed:
#             sym = trade.data._name
#             self.last_exit_date[sym] = self.datas[0].datetime.date(0)
#             self.highest_price[sym]  = None         # איפוס השיא
#             self.log(f"TRADE CLOSED {sym}  PnL: {trade.pnl:.2f}")

#     # -----------------------------------------------------------------------
#     def next(self):
#         today     = self.datas[0].datetime.date(0)
#         mkt_score = self.market_signal.get(today, 0)
#         buys_today = 0

#         # ===== יציאה גורפת כאשר השוק שלילי ==================================
#         if mkt_score <= -1:
#             for d in self.datas[:-1]:
#                 if self.getposition(d):
#                     self.close(data=d)
#                     self.log(f"MARKET EXIT {d._name} (score={mkt_score})")
#             return

#         # ===== לולאה על כל המניות ===========================================
#         for d in self.datas[:-1]:
#             sym   = d._name
#             score = self.strength[sym].score[0]
#             pos   = self.getposition(d)

#             # ① forced-sell ביום האחרון לנתוני המניה
#             last_day = self.p.end_dates.get(sym) if self.p.end_dates else None
#             if last_day and today >= last_day.date():
#                 if pos:
#                     self.close(data=d)
#                     self.log(f"FORCED SELL {sym} (end date)")
#                 continue

#             # ② Trailing-SL + Take-Profit
#             if pos:
#                 # --- עדכון שיא חדש -----------------------------------------
#                 curr_px = d.close[0]
#                 if self.highest_price[sym] is None:
#                     self.highest_price[sym] = curr_px
#                 elif curr_px > self.highest_price[sym]:
#                     self.highest_price[sym] = curr_px

#                 # --- חישוב SL דינמי ----------------------------------------
#                 trail_stop = self.highest_price[sym] * (1 - self.p.stop_loss_pct)

#                 # Stop-Loss
#                 if curr_px <= trail_stop:
#                     self.close(data=d)
#                     self.log(f"TRAIL SL {sym} @ {curr_px:.2f}")
#                     continue

#                 # Take-Profit (עדיין קבוע)
#                 entry = self.entry_prices.get(sym, curr_px)
#                 if curr_px >= entry * (1 + self.p.take_profit_pct):
#                     self.close(data=d)
#                     self.log(f"TAKE PROFIT {sym} @ {curr_px:.2f}")
#                     continue

#             # ③ פתיחת פוזיציה חדשה (רק אם השוק חיובי)
#             if (mkt_score >= 1 and
#                 not pos and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score > self.p.strength_thresh and
#                 buys_today < self.p.max_daily_buys):

#                 # קירור
#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 # גודל עסקה
#                 cash_free  = self.broker.getcash()
#                 target_val = (self.p.pct_per_trade / 100.0) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 o = self.buy(data=d, size=size)
#                 self.pending_orders[sym] = o
#                 self.entry_prices[sym]   = d.close[0]
#                 self.highest_price[sym]  = d.close[0]   # ⭐ מאתחלים שיא
#                 buys_today += 1
#                 self.log(f"BUY {sym} @ {d.close[0]:.2f}  size={size:.0f}")


# ---------------------------------------------------------
# strategy.py –  MyStrengthStrat  (long + short, stop-trail)
# ---------------------------------------------------------
# import backtrader as bt
# import pandas as pd
# from btIndicators import BetaStrength30        # ודא שהנתיב נכון


# class MyStrengthStrat(bt.Strategy):
#     params = dict(
#         # ==== thresholds ====
#         strength_thresh_long=0.2,   # לונג:  β-strength חייב להיות גבוה מזה
#         strength_thresh_short=-0.1, # שורט:  β-strength חייב להיות מתחת לזה
#         # ==== money/risk ====
#         pct_per_trade=5,              # % מהמזומן הפנוי בכל פוזיציה
#         stop_loss_pct=0.02,           # 2 %  SL (דינמי – trailing)
#         take_profit_pct=0.02,         # 2 %  TP
#         max_daily_buys=10,
#         cooldown=5,                   # ימי “קירור” אחרי יציאה
#         # ==== market regime CSV ====
#         market_csv='../markov_fft_com_old_data.csv',
#         market_col='regime_signal_combined',
#         # ==== forced-exit dates ====
#         end_dates=None                # {symbol: last_date}  (אופציונלי)
#     )

#     # -------------- utils ---------------------------------------------------
#     def log(self, txt, dt=None):
#         dt = dt or self.datas[0].datetime.date(0)
#         print(f'{dt.isoformat()} - {txt}')

#     # -------------- init ----------------------------------------------------
#     def __init__(self):
#         # 0) מצב-שוק יומי מתוך CSV
#         mkt = pd.read_csv(self.p.market_csv,
#                           usecols=['date', self.p.market_col],
#                           parse_dates=['date'])
#         self.market_signal = {d.date(): s
#                               for d, s in zip(mkt['date'], mkt[self.p.market_col])}

#         # 1) אינדיקטור כוח לכל מניה
#         self.market_feed    = self.datas[-1]
#         self.strength       = {}
#         self.entry_prices   = {}     # {sym: price}
#         self.pending_orders = {}     # {sym: order|None}
#         self.last_exit_date = {}     # {sym: date}

#         for d in self.datas[:-1]:
#             sym = d._name
#             self.strength[sym] = BetaStrength30(d, self.market_feed)
#             self.pending_orders[sym] = None

#     # -------------- order/trade callbacks -----------------------------------
#     def notify_order(self, order):
#         if order.status in [order.Completed,
#                             order.Canceled,
#                             order.Margin,
#                             order.Rejected]:
#             self.pending_orders[order.data._name] = None

#     def notify_trade(self, trade):
#         if trade.isclosed:
#             self.last_exit_date[trade.data._name] = self.datas[0].datetime.date(0)
#             self.log(f"TRADE CLOSED {trade.data._name}  PnL: {trade.pnl:.2f}")

#     # -------------- main loop ----------------------------------------------
#     def next(self):
#         today      = self.datas[0].datetime.date(0)
#         mkt_score  = self.market_signal.get(today, 0)  # ברירת־מחדל 0
#         buys_today = 0

#         # A) יציאה כללית לפי מצב-שוק
#         if mkt_score <= -1:   # שוק שלילי → סגור לונגים
#             for d in self.datas[:-1]:
#                 if self.getposition(d).size > 0:
#                     self.close(d); self.log(f"MARKET EXIT LONG {d._name}")

#         if mkt_score >= 1:    # שוק חיובי → סגור שורטים
#             for d in self.datas[:-1]:
#                 if self.getposition(d).size < 0:
#                     self.close(d); self.log(f"MARKET EXIT SHORT {d._name}")

#         # B) לולאת מניות
#         for d in self.datas[:-1]:
#             sym   = d._name
#             score = self.strength[sym].score[0]
#             pos   = self.getposition(d)

#             # 1) Forced-exit ביום האחרון של הדאטה
#             last_day = self.p.end_dates.get(sym) if self.p.end_dates else None
#             if last_day and today >= last_day.date():
#                 if pos: self.close(d); self.log(f"FORCED CLOSE {sym}")
#                 continue

#             # 2) Trailing-SL / TP (עבור לונגים ושורטים)
#             if pos:
#                 entry = self.entry_prices[sym]
#                 price = d.close[0]

#                 if pos.size > 0:                # לונג
#                     stop = entry * (1 - self.p.stop_loss_pct)
#                     tp   = entry * (1 + self.p.take_profit_pct)
#                     if price <= stop:
#                         self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
#                         continue
#                     if price >= tp:
#                         self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
#                         continue
#                     # טריילינג-SL – מעדכן entry כל עוד המחיר עולה
#                     if price > entry:
#                         self.entry_prices[sym] = price

#                 else:                           # שורט
#                     stop = entry * (1 + self.p.stop_loss_pct)
#                     tp   = entry * (1 - self.p.take_profit_pct)
#                     if price >= stop:
#                         self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
#                         continue
#                     if price <= tp:
#                         self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
#                         continue
#                     # טריילינג לשורט – מעדכן entry כשהמחיר יורד
#                     if price < entry:
#                         self.entry_prices[sym] = price

#             # 3) כניסה ללונג
#             if (mkt_score >= 1 and                      # שוק חיובי
#                 pos.size == 0 and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score > self.p.strength_thresh_long and
#                 buys_today < self.p.max_daily_buys):

#                 # קירור
#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 cash_free  = self.broker.getcash()
#                 target_val = (self.p.pct_per_trade / 100) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 self.pending_orders[sym] = self.buy(d, size=size)
#                 self.entry_prices[sym]   = d.close[0]
#                 buys_today += 1
#                 self.log(f"BUY {sym} @ {d.close[0]:.2f}  size={size:.0f}")

#             # 4) כניסה לשורט
#             if (mkt_score <= -1 and                     # שוק שלילי
#                 pos.size == 0 and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score < self.p.strength_thresh_short and
#                 buys_today < self.p.max_daily_buys):

#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 cash_free  = self.broker.getcash()
#                 target_val = (self.p.pct_per_trade / 100) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 self.pending_orders[sym] = self.sell(d, size=size)
#                 self.entry_prices[sym]   = d.close[0]
#                 buys_today += 1
#                 self.log(f"SHORT {sym} @ {d.close[0]:.2f}  size={size:.0f}")


# לפי יחס קנייה של 3 אחוז ו6 אחוז 
# ---------------------------------------------------------
# strategy.py –  MyStrengthStrat  (long + short, pct per trade by regime)
# ---------------------------------------------------------
import backtrader as bt
import pandas as pd
from btIndicators import BetaStrength30        # ודא שהנתיב נכון


class MyStrengthStrat(bt.Strategy):
    params = dict(
        # ==== thresholds ====
        strength_thresh_long=0.2,    # לונג:  β-strength חייב להיות גבוה מזה
        strength_thresh_short=-0.1,  # שורט:  β-strength חייב להיות מתחת לזה
        # ==== money/risk ====
        stop_loss_pct=0.02,            # 2 %  SL (דינמי – trailing)
        take_profit_pct=0.02,          # 2 %  TP
        max_daily_buys=10,
        cooldown=5,                    # ימי “קירור” אחרי יציאה
        # ==== market regime CSV ====
        market_csv='../markov_fft_com_old_data.csv',
        market_col='regime_signal_combined',
        # ==== forced-exit dates ====
        end_dates=None                 # {symbol: last_date}  (אופציונלי)
    )

    # ---------- utilities ---------------------------------------------------
    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} - {txt}')

    # ---------- initialise --------------------------------------------------
    def __init__(self):
        # 0) daily market-regime from CSV
        mkt = pd.read_csv(
            self.p.market_csv,
            usecols=['date', self.p.market_col],
            parse_dates=['date']
        )
        self.market_signal = {d.date(): s
                              for d, s in zip(mkt['date'], mkt[self.p.market_col])}

        # 1) strength indicator per stock
        self.market_feed    = self.datas[-1]
        self.strength       = {}
        self.entry_prices   = {}     # {sym: price}
        self.pending_orders = {}     # {sym: order | None}
        self.last_exit_date = {}     # {sym: date}

        for d in self.datas[:-1]:
            sym = d._name
            self.strength[sym]        = BetaStrength30(d, self.market_feed)
            self.pending_orders[sym]  = None

    # ---------- order / trade callbacks ------------------------------------
    def notify_order(self, order):
        if order.status in [order.Completed,
                            order.Canceled,
                            order.Margin,
                            order.Rejected]:
            self.pending_orders[order.data._name] = None

    def notify_trade(self, trade):
        if trade.isclosed:
            self.last_exit_date[trade.data._name] = self.datas[0].datetime.date(0)
            self.log(f"TRADE CLOSED {trade.data._name}  PnL: {trade.pnl:.2f}")

    # ---------- main loop ---------------------------------------------------
    def next(self):
        today      = self.datas[0].datetime.date(0)
        mkt_score  = self.market_signal.get(today, 0)  # default 0
        buys_today = 0

        # A) global exits by regime
        if mkt_score <= -1:            # bearish → close longs
            for d in self.datas[:-1]:
                if self.getposition(d).size > 0:
                    self.close(d)
                    self.log(f"MARKET EXIT LONG {d._name}")

        if mkt_score >= 1:             # bullish → close shorts
            for d in self.datas[:-1]:
                if self.getposition(d).size < 0:
                    self.close(d)
                    self.log(f"MARKET EXIT SHORT {d._name}")

        # B) per-stock logic
        for d in self.datas[:-1]:
            sym   = d._name
            score = self.strength[sym].score[0]
            pos   = self.getposition(d)

            # 1) forced exit at data end
            last_day = self.p.end_dates.get(sym) if self.p.end_dates else None
            if last_day and today >= last_day.date():
                if pos:
                    self.close(d)
                    self.log(f"FORCED CLOSE {sym}")
                continue

            # 2) trailing SL / TP
            if pos:
                entry = self.entry_prices[sym]
                price = d.close[0]

                if pos.size > 0:                      # long
                    stop = entry * (1 - self.p.stop_loss_pct)
                    tp   = entry * (1 + self.p.take_profit_pct)
                    if price <= stop:
                        self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
                        continue
                    if price >= tp:
                        self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
                        continue
                    if price > entry:                 # update trailing
                        self.entry_prices[sym] = price

                else:                                 # short
                    stop = entry * (1 + self.p.stop_loss_pct)
                    tp   = entry * (1 - self.p.take_profit_pct)
                    if price >= stop:
                        self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
                        continue
                    if price <= tp:
                        self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
                        continue
                    if price < entry:                 # update trailing
                        self.entry_prices[sym] = price

            # helper ── percentage of cash for new trades
            def pct_for_regime(mkt_score: int, long: bool) -> float:
                if long:
                    if mkt_score == 1:  return 3
                    if mkt_score == 2:  return 6
                else:  # short
                    if mkt_score == -1: return 3
                    if mkt_score == -2: return 6
                return 0  # regime not suitable

            # 3) open long
            if (mkt_score >= 1 and
                pos.size == 0 and
                self.pending_orders[sym] is None and
                not pd.isna(d.close[0]) and
                score > self.p.strength_thresh_long and
                buys_today < self.p.max_daily_buys):

                pct = pct_for_regime(mkt_score, long=True)
                if pct == 0:   # shouldn’t happen, but safety
                    continue

                # cooldown
                last_exit = self.last_exit_date.get(sym)
                if last_exit and (today - last_exit).days < self.p.cooldown:
                    continue

                cash_free  = self.broker.getcash()
                target_val = (pct / 100.0) * cash_free
                if target_val < d.close[0]:
                    continue

                size = target_val / d.close[0]
                self.pending_orders[sym] = self.buy(d, size=size)
                self.entry_prices[sym]   = d.close[0]
                buys_today += 1
                self.log(f"BUY {sym} @ {d.close[0]:.2f}  size={size:.0f}  pct={pct}%")

            # 4) open short
            if (mkt_score <= -1 and
                pos.size == 0 and
                self.pending_orders[sym] is None and
                not pd.isna(d.close[0]) and
                score < self.p.strength_thresh_short and
                buys_today < self.p.max_daily_buys):

                pct = pct_for_regime(mkt_score, long=False)
                if pct == 0:
                    continue

                last_exit = self.last_exit_date.get(sym)
                if last_exit and (today - last_exit).days < self.p.cooldown:
                    continue

                cash_free  = self.broker.getcash()
                target_val = (pct / 100.0) * cash_free
                if target_val < d.close[0]:
                    continue

                size = target_val / d.close[0]
                self.pending_orders[sym] = self.sell(d, size=size)
                self.entry_prices[sym]   = d.close[0]
                buys_today += 1
                self.log(f"SHORT {sym} @ {d.close[0]:.2f}  size={size:.0f}  pct={pct}%")






#לפי יחס קנייה של 5 אחוז ו7 אחוז

# # ---------------------------------------------------------
# # strategy.py –  MyStrengthStrat  (long + short, dynamic %
# #               5 % במצב ±1  |  7 % במצב ±2)
# # ---------------------------------------------------------
# import backtrader as bt
# import pandas as pd
# from btIndicators import BetaStrength30        # ודא שהנתיב נכון


# class MyStrengthStrat(bt.Strategy):
#     params = dict(
#         # ==== thresholds =====================================================
#         strength_thresh_long=0.20,     # לונג  – β-strength חייב להיות מעל
#         strength_thresh_short=-0.1,   # שורט –  β-strength מתחת
#         # ==== money / risk ===================================================
#         stop_loss_pct=0.02,            # 2 %  SL (טריילינג)
#         take_profit_pct=0.02,          # 2 %  TP
#         max_daily_buys=10,
#         cooldown=5,                    # ימי “קירור” אחרי יציאה
#         # ==== market–regime CSV =============================================
#         market_csv='../markov_fft_com_old_data.csv',
#         market_col='regime_signal_combined',
#         # ==== forced-exit dates =============================================
#         end_dates=None                 # {symbol: last_date}  (אופציונלי)
#     )

#     # ---------- utilities ---------------------------------------------------
#     def log(self, txt, dt=None):
#         dt = dt or self.datas[0].datetime.date(0)
#         print(f'{dt.isoformat()} - {txt}')

#     # ---------- initialise --------------------------------------------------
#     def __init__(self):

#         # 0) daily market-regime mapping  {date → score (–2…+2)}
#         mkt_df = pd.read_csv(
#             self.p.market_csv,
#             usecols=['date', self.p.market_col],
#             parse_dates=['date']
#         )
#         self.market_signal = {d.date(): s
#                               for d, s in zip(mkt_df['date'],
#                                               mkt_df[self.p.market_col])}

#         # 1) strength indicator for each stock
#         self.market_feed     = self.datas[-1]
#         self.strength        = {}
#         self.entry_prices    = {}     # {sym: entry_price}
#         self.pending_orders  = {}     # {sym: order | None}
#         self.last_exit_date  = {}     # {sym: date}

#         for d in self.datas[:-1]:
#             sym = d._name
#             self.strength[sym]        = BetaStrength30(d, self.market_feed)
#             self.pending_orders[sym]  = None

#     # ---------- order / trade callbacks ------------------------------------
#     def notify_order(self, order):
#         if order.status in [order.Completed,
#                             order.Canceled,
#                             order.Margin,
#                             order.Rejected]:
#             self.pending_orders[order.data._name] = None

#     def notify_trade(self, trade):
#         if trade.isclosed:
#             self.last_exit_date[trade.data._name] = self.datas[0].datetime.date(0)
#             self.log(f"TRADE CLOSED {trade.data._name}  PnL: {trade.pnl:.2f}")

#     # ---------- helper – %-of-cash by regime --------------------------------
#     @staticmethod
#     def pct_for_regime(mkt_score: int, long_pos: bool) -> float:
#         """
#         מחזיר את אחוז המזומן לפוזיציה חדשה לפי מצב השוק:
#         ±1 → 5 %   |   ±2 → 7 %
#         """
#         if long_pos:
#             if   mkt_score == 1:  return 5
#             elif mkt_score == 2:  return 7
#         else:  # short
#             if   mkt_score == -1: return 5
#             elif mkt_score == -2: return 7
#         return 0.0

#     # ---------- main loop ---------------------------------------------------
#     def next(self):

#         today      = self.datas[0].datetime.date(0)
#         mkt_score  = self.market_signal.get(today, 0)   # ברירת-מחדל 0
#         buys_today = 0

#         # A) global exits triggered by regime
#         if mkt_score <= -1:       # bearish → close longs
#             for d in self.datas[:-1]:
#                 if self.getposition(d).size > 0:
#                     self.close(d)
#                     self.log(f"MARKET EXIT LONG  {d._name}")

#         if mkt_score >= 1:        # bullish → close shorts
#             for d in self.datas[:-1]:
#                 if self.getposition(d).size < 0:
#                     self.close(d)
#                     self.log(f"MARKET EXIT SHORT {d._name}")

#         # B) per-stock logic
#         for d in self.datas[:-1]:
#             sym   = d._name
#             score = self.strength[sym].score[0]
#             pos   = self.getposition(d)

#             # 1) forced exit on last data-day
#             last_day = self.p.end_dates.get(sym) if self.p.end_dates else None
#             if last_day and today >= last_day.date():
#                 if pos:
#                     self.close(d)
#                     self.log(f"FORCED CLOSE {sym}")
#                 continue

#             # 2) trailing SL / TP
#             if pos:
#                 entry = self.entry_prices[sym]
#                 price = d.close[0]

#                 if pos.size > 0:            # long
#                     stop = entry * (1 - self.p.stop_loss_pct)
#                     tp   = entry * (1 + self.p.take_profit_pct)
#                     if price <= stop:
#                         self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
#                         continue
#                     if price >= tp:
#                         self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
#                         continue
#                     if price > entry:       # update trailing
#                         self.entry_prices[sym] = price

#                 else:                       # short
#                     stop = entry * (1 + self.p.stop_loss_pct)
#                     tp   = entry * (1 - self.p.take_profit_pct)
#                     if price >= stop:
#                         self.close(d); self.log(f"STOP LOSS  {sym} {price:.2f}")
#                         continue
#                     if price <= tp:
#                         self.close(d); self.log(f"TAKE PROFIT {sym} {price:.2f}")
#                         continue
#                     if price < entry:       # update trailing
#                         self.entry_prices[sym] = price

#             # 3) open long
#             if (mkt_score >= 1 and
#                 pos.size == 0 and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score > self.p.strength_thresh_long and
#                 buys_today < self.p.max_daily_buys):

#                 pct = self.pct_for_regime(mkt_score, long_pos=True)
#                 if pct == 0:
#                     continue

#                 # cooldown
#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 cash_free  = self.broker.getcash()
#                 target_val = (pct / 100.0) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 self.pending_orders[sym] = self.buy(d, size=size)
#                 self.entry_prices[sym]   = d.close[0]
#                 buys_today += 1
#                 self.log(f"BUY   {sym} @ {d.close[0]:.2f}  size={size:.0f}  {pct}%")

#             # 4) open short
#             if (mkt_score <= -1 and
#                 pos.size == 0 and
#                 self.pending_orders[sym] is None and
#                 not pd.isna(d.close[0]) and
#                 score < self.p.strength_thresh_short and
#                 buys_today < self.p.max_daily_buys):

#                 pct = self.pct_for_regime(mkt_score, long_pos=False)
#                 if pct == 0:
#                     continue

#                 last_exit = self.last_exit_date.get(sym)
#                 if last_exit and (today - last_exit).days < self.p.cooldown:
#                     continue

#                 cash_free  = self.broker.getcash()
#                 target_val = (pct / 100.0) * cash_free
#                 if target_val < d.close[0]:
#                     continue

#                 size = target_val / d.close[0]
#                 self.pending_orders[sym] = self.sell(d, size=size)
#                 self.entry_prices[sym]   = d.close[0]
#                 buys_today += 1
#                 self.log(f"SHORT {sym} @ {d.close[0]:.2f}  size={size:.0f}  {pct}%")
