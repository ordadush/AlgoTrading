#start from WorkingDir = src
#(*)preActivation:  
#   .\Algo_env\Scripts\activate
#   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#   PreRun  .\DBintegration\database.py
#   pip install -r requirements.txt
#run goes with : python -m Indicators.indicators
#this line should be included everywhere so you could import func from anothre folder (i.e adds src to Path)
#%%
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DBintegration.models import DailyStockData
from DBintegration.models import SP500Index# or any other model you want to use
from DBintegration.models import *
from pathlib import Path
#from Indicators.df_utils import count_symbols
from DBintegration.db_utils import *
from sqlalchemy.orm import sessionmaker
import backtrader as bt
import pandas as pd
import os
from ophir.btIndicators import *
from ophir.strategy import *
from ophir.utils import *
from ophir.utils import split_dataframe_by_dates
import matplotlib
from pandas.tseries.offsets import BDay
matplotlib.use('TkAgg') 
#added coloumn (variable class)
class SP500IndexWithScore(bt.feeds.PandasData): #bt pseudo constructor
    """
    Custom data feed for the S&P 500 index that includes the 'score' line.
    """
    lines = ('score',)
    params = (('score', 6),) #score is the coloumn[6]
    
CHECKPOINT_DIR = "data_cache"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CP_MAIN = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")
use_cp = os.path.exists(CP_MAIN) and os.path.exists(CP_SP500)

# --- REVISED DATA LOADING AND PREPARATION ---
#stage 1: clean &split the data
if use_cp:
    print("Loading data from checkpoints…")
    df_main  = pd.read_parquet(CP_MAIN)
    df_sp500 = pd.read_parquet(CP_SP500)
else:
    print("Building data from database…")
    df_main = model_to_dataframe(DailyStockData)
    df_sp500 = model_to_dataframe(SP500Index)
    # Save the raw data for next time
    df_main.to_parquet(CP_MAIN)
    df_sp500.to_parquet(CP_SP500)

# --- ALL PROCESSING NOW HAPPENS ONCE ---

# 1. Split data into train/val/test sets
df_train, df_val, df_test = split_dataframe_by_dates(df_main)
df_sp500_train, df_sp500_val, df_sp500_test = split_dataframe_by_dates(df_sp500)

# 2. Set datetime index for the training data
for _df in (df_train, df_sp500_train):
    if 'date' in _df.columns:
        _df['date'] = pd.to_datetime(_df['date'])
        _df.set_index('date', inplace=True)
    _df.sort_index(inplace=True)

# 3. Calculate end dates for all stocks (from original, unaligned data)
print("Calculating end dates for each stock...")
end_dates = {}
grouped = df_train.groupby('symbol')
for symbol, group_df in grouped:
    end_dates[symbol] = min(group_df.index.max(), pd.Timestamp('2021-01-01'))-BDay(1)
print("End dates calculation complete.")

# 4. Align all stock data to the master index
print("Aligning all stock data to the S&P 500 master index...")
aligned_stock_dfs = {}
master_index = df_sp500_train.index
# Note: Using the same 'grouped' object from step 3
for symbol, group_df in list(grouped)[:-1]: # <-------------- Change  for a full run
    aligned_df = group_df.reindex(master_index)
    # Forward-fill NaN values to prevent broker errors
    aligned_df.fillna(method='ffill', inplace=True)
    aligned_stock_dfs[symbol] = aligned_df
print(f"Data alignment complete. {len(aligned_stock_dfs)} stocks aligned.")

# --- Data Verification Log ---
# --- Data Verification Log ---
print("\n--- Data Date Verification ---")

# Access the date from the DataFrame's INDEX, not from a column
print(f"Earliest date in Stock Data (df_train): {df_train.index.min().date()}")
print(f"Latest date in Stock Data (df_train):   {df_train.index.max().date()}")
print(f"Earliest date in S&P 500 Data (df_sp500_train): {df_sp500_train.index.min().date()}")
print(f"Latest date in S&P 500 Data (df_sp500_train):   {df_sp500_train.index.max().date()}")
print("----------------------------\n")
#%%

#stage 2: define cerebro
    # i)add data feeds
if __name__ == '__main__':
    print("Aligning all stock data to the S&P 500 master index...")
    cerebro = bt.Cerebro()
    print("Cerebro engine initialized.")
    for symbol, data in aligned_stock_dfs.items(): #<----------    when running for real, change here the 20 stocks limitation. 
        stock_feed = bt.feeds.PandasData(dataname=data, plot = False)
        cerebro.adddata(stock_feed, name=symbol)

    # --- Add the Custom S&P 500 Data Feed ---
    # This feed provides both OHLC for beta and the 'score' for logic
    sp500_feed = SP500IndexWithScore(dataname=df_sp500_train)
    cerebro.adddata(sp500_feed, name='sp500_feed')
    print("All data feeds have been loaded into Cerebro.")
    #ii)add strategy
    cerebro.addstrategy(RelativeBIDX, beta_period=250, beta_short_window=20, end_dates = end_dates,low_percentage = 0.15, high_percentage = 0.05) #<----------------------around 250 trading days per year
    print("Strategy has been added to Cerebro.")
    #iii)set Broker & trading rules
    initial_cash = 100000.0
    cerebro.broker.setcash(initial_cash)
    cerebro.broker.setcommission(commission=0)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=1) #1 percent invest at each buy
    print(f"Initial portfolio value set to: ${initial_cash:,.2f}")
    # iv) adding Analyzers
    cerebro.addobserver(bt.observers.Value)
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trade_analyzer')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe_ratio', timeframe=bt.TimeFrame.Days, compression=252, riskfreerate=0.0)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    # v) backtesting
    print("\n--- Starting Backtest ---")
    results = cerebro.run(runonce=False)
    
    # הוספה בסיום קובץ run.py
    portfolio_values = final_strategy_results.analyzers.returns.get_analysis().get('rlogret', [])
    dates = df_sp500_train.index[:len(portfolio_values)]
    df_returns = pd.DataFrame({
        'Date': dates,
        'Cumulative_Return': pd.Series(portfolio_values).cumsum()
    })
    df_returns['Portfolio_Value'] = initial_cash * (1 + df_returns['Cumulative_Return'])
    df_returns.to_csv('strategy_pnl.csv', index=False)
    print("Saved cumulative PnL to strategy_pnl.csv")
    print("--- Backtest Finished ---")
    # vi) processing results
    final_strategy_results = results[0]
    trade_analysis = final_strategy_results.analyzers.trade_analyzer.get_analysis()
    if trade_analysis:
        print("\n--- Trade Analysis ---")
        print(f"Total Trades: {trade_analysis.total.total}")
        print(f"Winning Trades: {trade_analysis.won.total}")
        print(f"Losing Trades: {trade_analysis.lost.total}")
        print(f"Win Rate: {trade_analysis.won.total / trade_analysis.total.total * 100:.2f}%" if trade_analysis.total.total > 0 else "N/A")
        print(f"Total Net Profit/Loss: ${trade_analysis.pnl.net.total:.2f}")

    # -sharp
    sharpe_ratio = final_strategy_results.analyzers.sharpe_ratio.get_analysis()
    print("\n--- Performance Metrics ---")
    print(f"Sharpe Ratio: {sharpe_ratio.get('sharperatio', 'N/A'):.2f}")

    # -DrawDown
    drawdown_analysis = final_strategy_results.analyzers.drawdown.get_analysis()
    print(f"Max Drawdown: {drawdown_analysis.max.drawdown:.2f}%")
    print(f"Max Money Drawdown: ${drawdown_analysis.max.moneydown:.2f}")
    # -Ploting.
    print("\nGenerating plot...")
    cerebro.plot(style='candlestick', barup='green', bardown='red')



# ------------------------------------------------------------
# run.py   –   Back-test 01-01-2019 … 31-12-2024
# ------------------------------------------------------------
# הפעלה:  python run.py   (אחרי הפעלת -venv והתקנת requirements)

# import sys, os
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# from DBintegration.models import DailyStockData, SP500Index
# from DBintegration.db_utils import model_to_dataframe
# from pandas.tseries.offsets import BDay
# import backtrader as bt
# import pandas as pd
# import numpy as np
# import matplotlib
# matplotlib.use('TkAgg')

# # =======  STRATEGY / INDICATORS  ============================================
# from Or_Ofir_stragety.strategy     import MyStrengthStrat        # ← שלך
# from Or_Ofir_stragety.btIndicators import *                      # אם נדרש
# # ============================================================================


# # ---------- 0.  קבועים ------------------------------------------------------
# START_DATE = pd.Timestamp("2019-01-01")
# END_DATE   = pd.Timestamp("2024-12-31")
# CHECKPOINT_DIR = "data_cache"
# CP_MAIN  = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
# CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")
# os.makedirs(CHECKPOINT_DIR, exist_ok=True)


# # ---------- 1.  טעינת נתונים -----------------------------------------------
# def load_data():
#     """מחזיר (df_main, df_sp500) אחרי סינון לטווח-התאריכים."""
#     if os.path.exists(CP_MAIN) and os.path.exists(CP_SP500):
#         print("Loading data from checkpoints …")
#         df_main  = pd.read_parquet(CP_MAIN)
#         df_sp500 = pd.read_parquet(CP_SP500)
#     else:
#         print("Building data from DB …")
#         df_main  = model_to_dataframe(DailyStockData)
#         df_sp500 = model_to_dataframe(SP500Index)
#         df_main.to_parquet(CP_MAIN);  df_sp500.to_parquet(CP_SP500)

#     # --- חיתוך הטווח המבוקש -------------------------------------------------
#     mask_main  = (df_main ['date'] >= START_DATE) & (df_main ['date'] <= END_DATE)
#     mask_sp500 = (df_sp500['date'] >= START_DATE) & (df_sp500['date'] <= END_DATE)
#     df_main  = df_main .loc[mask_main ].reset_index(drop=True)
#     df_sp500 = df_sp500.loc[mask_sp500].reset_index(drop=True)

#     return df_main, df_sp500


# df_main, df_sp500 = load_data()

# # ---------- 2.  Index → DatetimeIndex  --------------------------------------
# for _df in (df_main, df_sp500):
#     _df['date'] = pd.to_datetime(_df['date'])
#     _df.set_index('date', inplace=True)
#     _df.sort_index(inplace=True)

# # ---------- 3.  יצירת end_dates לכל מניה ------------------------------------
# print("Calculating end-dates …")
# end_dates = {sym: min(g.index.max(), END_DATE) - BDay(1)
#              for sym, g in df_main.groupby('symbol')}

# # ---------- 4.  יישור כל המניות ללוח-שנה של S&P-500 -------------------------
# master_index      = df_sp500.index
# aligned_stock_dfs = {}
# for sym, g in df_main.groupby('symbol'):
#     aligned = g.reindex(master_index)
#     aligned.ffill(inplace=True)        # fill holes
#     aligned_stock_dfs[sym] = aligned
# print(f"Aligned {len(aligned_stock_dfs)} stocks to master index.\n")


# # ---------- 5.  Backtrader – feeds ------------------------------------------
# class SP500Feed(bt.feeds.PandasData):
#     """Adds ‘score’ line (col #6) לטובת האינדיקטור."""
#     lines = ('score',)
#     params = (('score', 6),)


# cerebro = bt.Cerebro(stdstats=False)
# print("Cerebro initialised.")

# #   • מניות
# for sym, data in aligned_stock_dfs.items():
#     cerebro.adddata(bt.feeds.PandasData(dataname=data, plot=False), name=sym)

# #   • מדד S&P-500
# cerebro.adddata(SP500Feed(dataname=df_sp500), name='sp500_feed')
# print("Feeds loaded.")


# # ---------- 6.  אסטרטגיה + אנלייזרים ---------------------------------------
# cerebro.addstrategy(
#     MyStrengthStrat,
#     end_dates=end_dates,
#     strength_thresh_long = 0.2,
#     strength_thresh_short=-0.12,
#     stop_loss_pct  = 0.02,
#     take_profit_pct= 0.02,
#     max_daily_buys = 10,
#     cooldown       = 5
# )

# cerebro.addobserver(bt.observers.Value)
# cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
# cerebro.addanalyzer(bt.analyzers.DrawDown,      _name='dd')
# cerebro.addanalyzer(bt.analyzers.TimeReturn,
#                     _name='daily_ret',
#                     timeframe=bt.TimeFrame.Days)

# # ---------- 7.  ברוקר -------------------------------------------------------
# cerebro.broker.setcash(100_000.0)
# cerebro.broker.setcommission(commission=0.0)
# cerebro.addsizer(bt.sizers.PercentSizer, percents=1)

# # ---------- 8.  הרצה --------------------------------------------------------
# print("\n--- Starting Backtest ---")
# results = cerebro.run(runonce=False)
# strat   = results[0]
# print("--- Backtest Finished ---\n")


# # ---------- 9.  תוצאות ------------------------------------------------------
# ta = strat.analyzers.ta.get_analysis()
# dd = strat.analyzers.dd.get_analysis()

# total = ta.total.closed if 'closed' in ta.total else 0
# won   = ta.won.total    if 'won'    in ta         else 0
# lost  = ta.lost.total   if 'lost'   in ta         else 0
# pnl   = ta.pnl.net.total if ('pnl' in ta and 'net' in ta.pnl) else 0.0

# print("--- Trade Analysis ---")
# print(f"Total Trades:   {total}")
# print(f"Winning trades: {won}")
# print(f"Losing  trades: {lost}")
# print(f"Win Rate:       {100*won/total:.2f}%") if total else print("Win Rate: N/A")
# print(f"Net P/L:        ${pnl:,.2f}")

# print("\n--- Risk Metrics ---")
# print(f"Max Drawdown:   {dd.max.drawdown:.2f}%")
# print(f"Max Money DD:   ${dd.max.moneydown:,.2f}")

# # Sharpe (מחישוב ידני)
# rets = pd.Series(strat.analyzers.daily_ret.get_analysis()).replace(
#             [np.inf, -np.inf], np.nan).dropna()
# if len(rets) > 1 and rets.std() != 0:
#     sharpe = (rets.mean() / rets.std()) * np.sqrt(252)
#     print(f"\nSharpe Ratio (Annualised): {sharpe:.2f}")
# else:
#     print("\nSharpe Ratio: N/A")

# ---------- 10.  (אופציונלי) גרף -------------------------------------------
#  cerebro.plot(style='candlestick', barup='green', bardown='red')
