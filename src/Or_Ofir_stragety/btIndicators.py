import backtrader as bt
import numpy as np
import pandas as pd

class BetaStrength30(bt.Indicator):
    """
    BetaStrength30 – אינדיקטור 'חוזק יחסי' ל-30 הימים האחרונים.

    רעיון:
    1. מפריד את 30 הימים האחרונים לימי ↑ (שוק עולה) ו-ימי ↓ (שוק יורד/אפס).
    2. מחשב לכל קבוצה את סך-כל תשואת המניה מול סך-כל תשואת השוק.
    3. הציון (score) חיובי כאשר:
         • במגמת ↑   המניה עלתה *יותר* מהשוק
         • במגמת ↓   המניה ירדה *פחות* מהשוק
       ומנורמל לפי שיעור הימים.
    """

    lines = ('score', 'p_up', 'p_down',
             'diff_up', 'diff_down')   # שורות עזר אם תרצה לראות
    params = dict(
        period=30,              # חלון הגלילה
        eps=1e-9                # למניעת חלוקה ב-0
    )

    def __init__(self):
        super().__init__()

        # תשואה יומית (r_t = P_t / P_{t-1} − 1)
        stock_ret  = (self.data0 / self.data0(-1)) - 1.0
        market_ret = (self.data1 / self.data1(-1)) - 1.0

        # מסיכות לימי ↑ / ↓
        up_mask   = market_ret  > 0
        down_mask = market_ret <= 0

        period = self.p.period
        eps    = self.p.eps

        # ------------------------------------------------------------------
        # ספירת ימים
        n_up   = bt.ind.SumN(up_mask,   period=period) + eps
        n_down = bt.ind.SumN(down_mask, period=period) + eps

        # שיעור הימים (n / period)
        p_up   = n_up   / float(period)
        p_down = n_down / float(period)

        # ------------------------------------------------------------------
        # סכום תשואות
        sum_stock_up   = bt.ind.SumN(stock_ret  * up_mask,   period=period)
        sum_market_up  = bt.ind.SumN(market_ret * up_mask,   period=period)

        sum_stock_down  = bt.ind.SumN(stock_ret  * down_mask, period=period)
        sum_market_down = bt.ind.SumN(market_ret * down_mask, period=period)

        # הפערים (דלתא) – כמה המניה ניצחה/הפסידה ביחס לשוק
        diff_up   = sum_stock_up  - sum_market_up
        diff_down = sum_market_down - sum_stock_down  # שים לב: שוק – מניה

        # ציון סופי (חיובי = חזק מהשוק)
        score = p_up * diff_up  -  p_down * diff_down

        # ------------------------------------------------------------------
        # הצמדת הליינים
        self.lines.score      = score
        self.lines.p_up       = p_up
        self.lines.p_down     = p_down
        self.lines.diff_up    = diff_up
        self.lines.diff_down  = diff_down

        # צריך לפחות 30 תצפיות כדי להתחיל
        self.addminperiod(period)
