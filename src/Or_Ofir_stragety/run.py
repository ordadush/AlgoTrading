# # ============================================
# # run.py  –  main back-testing script
# # ============================================

# import sys, os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from DBintegration.models import DailyStockData, SP500Index
# from DBintegration.db_utils import model_to_dataframe
# from sqlalchemy.orm import sessionmaker

# import backtrader as bt
# import pandas as pd
# import numpy as np
# from pathlib import Path
# from pandas.tseries.offsets import BDay
# import matplotlib

# # ---- strategy / indicators imports ----
# from Or_Ofir_stragety.btIndicators import *
# from Or_Ofir_stragety.strategy     import MyStrengthStrat
# from ophir.utils                   import split_dataframe_by_dates

# # ---------------------------------------
# matplotlib.use('TkAgg')    # ← keeps original behaviour

# class SP500IndexWithScore(bt.feeds.PandasData):
#     """Adds ‘score’ line to S&P-500 feed (col #6)."""
#     lines = ('score',)
#     params = (('score', 6),)

# # --------------------------------------------------------------------
# #                1.  Data loading  & prep
# # --------------------------------------------------------------------
# CHECKPOINT_DIR = "data_cache"
# os.makedirs(CHECKPOINT_DIR, exist_ok=True)
# CP_MAIN  = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
# CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")
# use_cp   = os.path.exists(CP_MAIN) and os.path.exists(CP_SP500)

# if use_cp:
#     print("Loading data from checkpoints…")
#     df_main  = pd.read_parquet(CP_MAIN)
#     df_sp500 = pd.read_parquet(CP_SP500)
# else:
#     print("Building data from database…")
#     df_main  = model_to_dataframe(DailyStockData)
#     df_sp500 = model_to_dataframe(SP500Index)
#     df_main.to_parquet(CP_MAIN);  df_sp500.to_parquet(CP_SP500)

# # --- split sets ---
# df_train, _, _       = split_dataframe_by_dates(df_main)
# df_sp500_train, _, _ = split_dataframe_by_dates(df_sp500)

# # --- set index & sort ---
# for _df in (df_train, df_sp500_train):
#     if 'date' in _df.columns:
#         _df['date'] = pd.to_datetime(_df['date'])
#         _df.set_index('date', inplace=True)
#     _df.sort_index(inplace=True)

# # --- end-date per symbol (for forced exit) ---
# print("Calculating end dates for each stock…")
# end_dates = {sym: min(g.index.max(), pd.Timestamp('2021-01-01')) - BDay(1)
#              for sym, g in df_train.groupby('symbol')}

# # --- align to master index (S&P-500) ---
# print("Aligning all stock data to the S&P 500 master index…")
# aligned_stock_dfs = {}
# master_index      = df_sp500_train.index

# for sym, g in df_train.groupby('symbol'):
#     aligned = g.reindex(master_index)
#     aligned.fillna(method='ffill', inplace=True)
#     aligned_stock_dfs[sym] = aligned
# print(f"{len(aligned_stock_dfs)} stocks aligned.\n")

# # --------------------------------------------------------------------
# #                2.  Cerebro engine
# # --------------------------------------------------------------------
# if __name__ == '__main__':
#     cerebro = bt.Cerebro(stdstats=False)
#     print("Cerebro initialised.")

#     # add stock feeds
#     for sym, data in aligned_stock_dfs.items():
#         cerebro.adddata(bt.feeds.PandasData(dataname=data, plot=False), name=sym)

#     # add market (S&P-500) feed
#     sp500_feed = SP500IndexWithScore(dataname=df_sp500_train)
#     cerebro.adddata(sp500_feed, name='sp500_feed')
#     print("Feeds loaded.")

#     # --- strategy ---
#     cerebro.addstrategy(MyStrengthStrat)


#     # --- analyzers ---
#     cerebro.addobserver(bt.observers.Value)
#     cerebro.addanalyzer(bt.analyzers.TradeAnalyzer,  _name='trade_analyzer')
#     cerebro.addanalyzer(bt.analyzers.DrawDown,       _name='drawdown')
#     cerebro.addanalyzer(
#         bt.analyzers.TimeReturn,                     # ✅ returns per day
#         _name='daily_returns',
#         timeframe=bt.TimeFrame.Days
#     )                                                # ### NEW / CHANGED
#     # (SharpeRatio analyzer נשאר למקרה שירצה לבד)
#     cerebro.addanalyzer(bt.analyzers.SharpeRatio,
#                         _name='sharpe_ratio',
#                         timeframe=bt.TimeFrame.Days,
#                         compression=252,
#                         riskfreerate=0.0)

#     # --- broker / sizer ---
#     initial_cash = 100000.0
#     cerebro.broker.setcash(initial_cash)
#     cerebro.broker.setcommission(commission=0.0)
#     cerebro.addsizer(bt.sizers.PercentSizer, percents=1)

#     # ----------------------------------------------------------------
#     print("\n--- Starting Backtest ---")
#     results = cerebro.run(runonce=False)

#     final_strategy_results = results[0]          # <-- חייב להיות לפני השמירה ל-CSV
#     trade_analysis = final_strategy_results.analyzers.trade_analyzer.get_analysis()


#     strat   = results[0]

#     # ----------------------------------------------------------------
#     #            3.  Metrics & output  (Sharpe working)
#     # ----------------------------------------------------------------
#     # Trade analysis
#     ta = strat.analyzers.trade_analyzer.get_analysis()
#     total = ta.total.closed if 'closed' in ta.total else 0
#     won   = ta.won.total    if 'won'    in ta         else 0
#     lost  = ta.lost.total   if 'lost'   in ta         else 0
#     pnl   = ta.pnl.net.total if ('pnl' in ta and 'net' in ta.pnl) else 0.0

#     print("\n--- Trade Analysis ---")
#     print(f"Total Trades: {total}")
#     print(f"Winning Trades: {won}")
#     print(f"Losing Trades: {lost}")
#     print(f"Win Rate: {100.0*won/total:.2f}%" if total else "Win Rate: N/A")
#     print(f"Total Net Profit/Loss: ${pnl:,.2f}")

#     # Drawdown
#     dd = strat.analyzers.drawdown.get_analysis()
#     print("\n--- Risk Metrics ---")
#     print(f"Max Drawdown: {dd.max.drawdown:.2f}%")
#     print(f"Max $$ Drawdown: ${dd.max.moneydown:,.2f}")

#     # =====  Sharpe  =====
#     daily_ret_series = pd.Series(strat.analyzers.daily_returns.get_analysis()).replace(
#                             [np.inf, -np.inf], np.nan).dropna()

#     if len(daily_ret_series) > 1 and daily_ret_series.std() != 0:
#         sharpe = (daily_ret_series.mean() / daily_ret_series.std()) * np.sqrt(252)
#         print(f"\nSharpe Ratio (Annualised): {sharpe:.2f}")
#     else:
#         print("\nSharpe Ratio (Annualised): N/A  –  insufficient data / zero st-dev")

#     # (אם תרצה גם את תוצאת האנלייזר של Backtrader:)
#     # sr_an  = strat.analyzers.sharpe_ratio.get_analysis().get('sharperatio', None)
#     # print(f"Sharpe (BT analyzer): {sr_an if sr_an is not None else 'N/A'}")

#     # ----------------------------------------------------------------
#     #  plot (optional) – השארתי כמו שהיה
#     # ----------------------------------------------------------------
#     # cerebro.plot(style='candlestick', barup='green', bardown='red')









import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DBintegration.models import DailyStockData, SP500Index
from DBintegration.db_utils import model_to_dataframe
import backtrader as bt
import pandas as pd
import numpy as np
from pathlib import Path
from pandas.tseries.offsets import BDay
import matplotlib

# ---- strategy / indicators imports ----
from Or_Ofir_stragety.btIndicators import *
from Or_Ofir_stragety.strategy     import MyStrengthStrat
from ophir.utils                   import split_dataframe_by_dates

# -------------------------------------------------
matplotlib.use('TkAgg')        # keep original behaviour

class SP500IndexWithScore(bt.feeds.PandasData):
    lines = ('score',)
    params = (('score', 6),)

# ========== 1.  Load / prep data ===========================================
CHECKPOINT_DIR = "data_cache"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CP_MAIN  = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")
use_cp   = os.path.exists(CP_MAIN) and os.path.exists(CP_SP500)

if use_cp:
    print("Loading data from checkpoints…")
    df_main  = pd.read_parquet(CP_MAIN)
    df_sp500 = pd.read_parquet(CP_SP500)
else:
    print("Building data from database…")
    df_main  = model_to_dataframe(DailyStockData)
    df_sp500 = model_to_dataframe(SP500Index)
    df_main.to_parquet(CP_MAIN);  df_sp500.to_parquet(CP_SP500)

# --- split to the desired period ------------------------------------------
df_train, _, _       = split_dataframe_by_dates(df_main)
df_sp500_train, _, _ = split_dataframe_by_dates(df_sp500)

for _df in (df_train, df_sp500_train):
    if 'date' in _df.columns:
        _df['date'] = pd.to_datetime(_df['date'])
        _df.set_index('date', inplace=True)
    _df.sort_index(inplace=True)

# end-date per symbol (forced exit)
end_dates = {sym: min(g.index.max(), pd.Timestamp('2021-01-01')) - BDay(1)
             for sym, g in df_train.groupby('symbol')}

# align each stock series to S&P-500 calendar
print("Aligning all stock data to the S&P 500 master index…")
aligned_stock_dfs = {}
master_index      = df_sp500_train.index
for sym, g in df_train.groupby('symbol'):
    aligned = g.reindex(master_index).ffill()
    aligned_stock_dfs[sym] = aligned
print(f"{len(aligned_stock_dfs)} stocks aligned.\n")

# ========== 2.  Cerebro ====================================================
if __name__ == '__main__':
    cerebro = bt.Cerebro(stdstats=False)
    print("Cerebro initialised.")

    # feeds
    for sym, data in aligned_stock_dfs.items():
        cerebro.adddata(bt.feeds.PandasData(dataname=data, plot=False), name=sym)

    sp500_feed = SP500IndexWithScore(dataname=df_sp500_train)
    cerebro.adddata(sp500_feed, name='sp500_feed')
    print("Feeds loaded.")

    # strategy
    cerebro.addstrategy(MyStrengthStrat)

    # analyzers / observers
    cerebro.addobserver(bt.observers.Value)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.DrawDown,      _name='drawdown')

    # === daily returns (for PnL curve & Sharpe) ===
    cerebro.addanalyzer(bt.analyzers.TimeReturn,
                        _name='daily_returns',
                        timeframe=bt.TimeFrame.Days)

    cerebro.addanalyzer(bt.analyzers.SharpeRatio,
                        _name='sharpe_ratio',
                        timeframe=bt.TimeFrame.Days,
                        compression=252,
                        riskfreerate=0.0)

    # broker
    initial_cash = 100_000.0
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0.0)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=1)

    # ========== 3.  Run =====================================================
    print("\n--- Starting Backtest ---")
    results = cerebro.run(runonce=False)
    strat   = results[0]

    # ---------- cumulative PnL plot ----------------------------------------
    daily_ret = strat.analyzers.daily_returns.get_analysis()   # {datetime: ret}
    dates, rets = zip(*sorted(daily_ret.items()))
    rets = pd.Series(rets, index=pd.to_datetime(dates))

    equity = initial_cash * (1 + rets).cumprod()     #  ✦ compound, not sum

    import matplotlib.pyplot as plt
    plt.figure(figsize=(12,6))
    plt.plot(equity.index, equity.values, label='Equity Curve')
    plt.title("Cumulative Profit / Loss")
    plt.xlabel("Date"); plt.ylabel("Portfolio Value ($)")
    plt.grid(True); plt.legend(); plt.tight_layout(); plt.show()


    # ---------- metrics ----------------------------------------------------
    ta = strat.analyzers.trade_analyzer.get_analysis()
    total = ta.total.closed if 'closed' in ta.total else 0
    won   = ta.won.total    if 'won'    in ta         else 0
    lost  = ta.lost.total   if 'lost'   in ta         else 0
    pnl   = ta.pnl.net.total if ('pnl' in ta and 'net' in ta.pnl) else 0.0

    print("\n--- Trade Analysis ---")
    print(f"Total Trades: {total}")
    print(f"Winning Trades: {won}")
    print(f"Losing Trades:  {lost}")
    print(f"Win Rate: {100*won/total:.2f}%" if total else "Win Rate: N/A")
    print(f"Total Net Profit/Loss: ${pnl:,.2f}")

    dd = strat.analyzers.drawdown.get_analysis()
    print("\n--- Risk Metrics ---")
    print(f"Max Drawdown: {dd.max.drawdown:.2f}%")
    print(f"Max $$ Drawdown: ${dd.max.moneydown:,.2f}")

    daily_series = pd.Series(rets)
    if len(daily_series) > 1 and daily_series.std() != 0:
        sharpe = (daily_series.mean() / daily_series.std()) * np.sqrt(252)
        print(f"\nSharpe Ratio (Annualised): {sharpe:.2f}")
    else:
        print("\nSharpe Ratio (Annualised): N/A")














# ------------------------------------------------------------
# run.py   –   Back-test 01-01-2019 … 31-12-2024
# ------------------------------------------------------------
# הפעלה:  python run.py   (אחרי הפעלת venv והתקנת requirements)
# ------------------------------------------------------------
import os, sys, pandas as pd
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from DBintegration.models import DailyStockData, SP500Index
from DBintegration.db_utils import model_to_dataframe
from pandas.tseries.offsets import BDay
import backtrader as bt
import numpy as np
import matplotlib
matplotlib.use('TkAgg')

# ==== אסטרטגיה / אינדיקטורים שלך ==========================================
from Or_Ofir_stragety.strategy import MyStrengthStrat
# ===========================================================================
#           0.  טווח התאריכים המבוקש
# ===========================================================================
START_DATE = pd.Timestamp("2019-01-01")
END_DATE   = pd.Timestamp("2024-12-31")

# ===========================================================================
#           1.  טעינת נתונים  (עם מערכת Checkpoint חכמה)
# ===========================================================================
CHECKPOINT_DIR = "data_cache"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CP_MAIN  = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")

def load_or_build(name, checkpoint_path, model_class):
    if os.path.exists(checkpoint_path):
        print(f"Loading {name} data from checkpoint…")
        df = pd.read_parquet(checkpoint_path)
    else:
        print(f"Building {name} data from database…")
        df = model_to_dataframe(model_class)
        df.to_parquet(checkpoint_path)

    # 🔧 תיקון כאן:
    df['date'] = pd.to_datetime(df['date'])

    if (df['date'].min() <= START_DATE) and (df['date'].max() >= END_DATE):
        print(f"{name} data covers full period.")
    else:
        print(f"⚠️ {name} data does NOT fully cover desired period.")

    return df



df_main  = load_or_build("STOCKS", CP_MAIN,  DailyStockData)
df_sp500 = load_or_build("S&P-500", CP_SP500, SP500Index)

# ===========================================================================
#           2.  סינון לטווח 2019-24  + עיצוב אינדקס
# ===========================================================================
def prep(df):
    df['date'] = pd.to_datetime(df['date'])
    df = df[(df['date'] >= START_DATE) & (df['date'] <= END_DATE)].copy()
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)
    return df

df_main  = prep(df_main)
df_sp500 = prep(df_sp500)

print("\n=== Date-range after filtering ===")
print(f"Stocks : {df_main.index.min().date()} → {df_main.index.max().date()}")
print(f"S&P500 : {df_sp500.index.min().date()} → {df_sp500.index.max().date()}")
print("==================================\n")

# ===========================================================================
#           3.  end-dates  (כפוי יציאה)
# ===========================================================================
end_dates = {sym: min(g.index.max(), END_DATE) - BDay(1)
             for sym, g in df_main.groupby('symbol')}

# ===========================================================================
#           4.  יישור כל המניות לאינדקס המדד
# ===========================================================================
master_idx        = df_sp500.index
aligned_stock_dfs = {}
for sym, g in df_main.groupby('symbol'):
    aligned = g.reindex(master_idx).ffill()
    aligned_stock_dfs[sym] = aligned
print(f"Aligned {len(aligned_stock_dfs)} symbols to S&P-500 calendar.\n")

# ===========================================================================
#           5.  Backtrader – feeds
# ===========================================================================
class SP500Feed(bt.feeds.PandasData):
    lines = ('score',)           # העמודה השישית היא score
    params = (('score', 6),)

cerebro = bt.Cerebro(stdstats=False)
for sym, data in aligned_stock_dfs.items():
    cerebro.adddata(bt.feeds.PandasData(dataname=data, plot=False), name=sym)
cerebro.adddata(SP500Feed(dataname=df_sp500), name='sp500_feed')
print("Feeds attached to Cerebro.")

# ===========================================================================
#           6.  אסטרטגיה + אנלייזרים
# ===========================================================================
cerebro.addstrategy(
    MyStrengthStrat,
    end_dates             = end_dates,
    strength_thresh_long  = 0.20,
    strength_thresh_short = -0.10,
    stop_loss_pct         = 0.02,
    take_profit_pct       = 0.02,
    max_daily_buys        = 10,
    cooldown              = 5
)
cerebro.addobserver(bt.observers.Value)
cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
cerebro.addanalyzer(bt.analyzers.DrawDown,      _name='dd')
cerebro.addanalyzer(
    bt.analyzers.TimeReturn, _name='daily_ret', timeframe=bt.TimeFrame.Days
)

# ===========================================================================
#           7.  Broker / sizing
# ===========================================================================
cerebro.broker.setcash(100_000.0)
cerebro.broker.setcommission(commission=0.0)
cerebro.addsizer(bt.sizers.PercentSizer, percents=1)

# ===========================================================================
#           8.  Run
# ===========================================================================
print("\n--- Starting Back-test 2019-24 ---")
strat = cerebro.run(runonce=False)[0]
print("--- Back-test finished ---\n")

# ===========================================================================
#           9.  Results
# ===========================================================================
ta = strat.analyzers.ta.get_analysis()
dd = strat.analyzers.dd.get_analysis()
total = ta.total.closed if 'closed' in ta.total else 0
won   = ta.won.total    if 'won'    in ta         else 0
lost  = ta.lost.total   if 'lost'   in ta         else 0
pnl   = ta.pnl.net.total if ('pnl' in ta and 'net' in ta.pnl) else 0.0

print("--- Trade Analysis ---")
print(f"Total trades : {total}")
print(f"Win / Loss   : {won} / {lost}  (win-rate {100*won/total:.2f}% )" if total else "N/A")
print(f"Net P&L      : ${pnl:,.2f}")

print("\n--- Risk Metrics ---")
print(f"Max Drawdown : {dd.max.drawdown:.2f}%")
print(f"Max $$ DD    : ${dd.max.moneydown:,.2f}")

rets = pd.Series(strat.analyzers.daily_ret.get_analysis()).replace(
            [np.inf, -np.inf], np.nan).dropna()
if len(rets) > 1 and rets.std() != 0:
    sharpe = (rets.mean() / rets.std()) * np.sqrt(252)
    print(f"\nSharpe ratio : {sharpe:.2f}")
else:
    print("\nSharpe ratio : N/A")

# ---------- 10.  (אופציונאלי) גרף ------------------------------------------
#cerebro.plot(style='candlestick', barup='green', bardown='red')

