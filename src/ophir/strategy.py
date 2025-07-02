import backtrader as bt
import pandas as pd
import datetime
from ophir.utils import *
from ophir.btIndicators import BetaIndex
import datetime
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#TODO: 1. commitions + cheap stocks filtering 2. filter liquidity: dont want to
class RelativeBIDX(bt.Strategy):
    """Strategy rules
        
        bt (_type_): _description_
    """
    params = (
        ('beta_period', 360),
        ('beta_short_window', 20),
        ('end_dates', None),
        ('high_percentage', 0.05),
        ('low_percentage', 0.15),
    )    
    
    def __init__(self):
        """
        Prepares the strategy's working environment. Runs only once at the start.
        """
        print("--- Initializing Multi-Stock Strategy ---")
        print(f"self.p.beta_period = {self.p.beta_period}")
        # print(f"self.p.beta_short_window = {self.p.beta_short_window}")
        print(f"self.p.high_percent = {self.p.low_percentage}")
        print(f"self.p.low_percent = {self.p.low_percentage}")
        print("------------------------------------------")
        self.market_feed = self.datas[-1] #Market feed enters last.
        self.stock_feeds = self.datas[:-1]
        print(f"Market feed identified: {self.market_feed._name}")

        self.beta_indicators = {}
        self.orders_by_stock = {}
        for d in self.datas[:-1]:
            stock_symbol = d._name
            # Create a BetaIndex indicator for the current stock (d) 
            # against the common market feed (self.market_feed),
            # and store it in the dictionary with the stock symbol as the key.
            self.beta_indicators[stock_symbol] = BetaIndex(
                d,                      # The specific stock feed
                self.market_feed,       # The common market feed for all stocks
                period=self.p.beta_period,
                short_window=self.p.beta_short_window
            )
            # Initialize the placeholder for order tracking for this specific stock
            self.orders_by_stock[stock_symbol] = None
            print(f" > Initialized BetaIndex for stock: {stock_symbol}")
            
        print("--- Strategy Initialization Complete ---")
  
    def next(self):    
        #rank the stocks
        ranked_stocks = []
        for d in self.stock_feeds:
            if len(d) > self.p.beta_period:
                stock_symbol = d._name
                # Use the recent beta index for ranking
                beta_val = self.beta_indicators[stock_symbol].beta_index_recent[0]
                if not pd.isna(beta_val):
                    ranked_stocks.append({'symbol': stock_symbol, 'beta': beta_val, 'data': d})

        # Sort the list from highest beta to lowest
        ranked_stocks.sort(key=lambda x: x['beta'], reverse=True)
        #pick stocks   
        num_stocks = len(ranked_stocks)
        # Define the cutoff points for the top 10% and 20%
        top_10_percent_cutoff = int(num_stocks * self.p.high_percentage) #<---------------------------------persentage control
        top_20_percent_cutoff = int(num_stocks * self.p.low_percentage)
        stocks_to_buy = {s['symbol'] for s in ranked_stocks[:top_10_percent_cutoff]}
        stocks_to_hold = {s['symbol'] for s in ranked_stocks[:top_20_percent_cutoff]}

        for d in self.datas[:-1]:
            stock_symbol = d._name
            if self.orders_by_stock.get(stock_symbol):
                continue
            if pd.isna(d.close[0]): # skip unavaliable data
                continue
            if len(d) <= self.p.beta_period:
                continue
            position = self.getposition(d)
            if position: #case open trade
                end_date_for_stock = self.p.end_dates.get(stock_symbol)
                current_date = d.datetime.date(0)
                # Forced sell on the stock's last day of data
                if end_date_for_stock and current_date == end_date_for_stock.date():
                    print(f'--- FORCED SELL (End of Data) for {stock_symbol} on {current_date} ---')
                    self.sell(data=d)
                    continue  # Done with this stock for today
                # sell
                beta_val_recent = self.beta_indicators[stock_symbol].beta_index_recent[0]
                if stock_symbol not in stocks_to_hold:
                    print(f'SELL SIGNAL for {stock_symbol}')
                    self.orders_by_stock[stock_symbol] = self.sell(data=d)
                    continue
            # BUY_settings
            else:
                if not position and stock_symbol in stocks_to_buy:
                    print(f'BUY SIGNAL for {stock_symbol} (Entered Top 10%)')
                    buy_order = self.buy(data=d)
                    self.orders_by_stock[stock_symbol] = buy_order
                    self.sell(data=d, parent=buy_order, exectype=bt.Order.StopTrail, trailpercent=0.08)
                    continue               
    def notify_order(self, order):
        stock_symbol = order.data._name
        if order.status in [order.Submitted, order.Accepted]:
            # order is already in process
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
            elif order.issell():
                print(f'SELL EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f'Order for {stock_symbol} was Canceled/Margin/Rejected')

        #restarting the orders to enable new ones
        self.orders_by_stock[stock_symbol] = None
        
def notify_trade(self, trade):
    # Only execute the block if the trade has been closed
    if trade.isclosed:
        stock_symbol = trade.data._name
        
        # Get the opening and closing datetime objects
        trade_open_date = trade.open_datetime()
        trade_close_date = trade.close_datetime()

        # Format and print the trade details
        print(f'--- TRADE CLOSED for {stock_symbol} ---')
        print(f'OPENED: {trade_open_date.strftime("%Y-%m-%d %H:%M:%S")} CLOSED: {trade_close_date.strftime("%Y-%m-%d %H:%M:%S")}')
        print(f'PROFIT/LOSS: Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}')
        print('-----------------------------------------')
def stop(self):
    print('--- Backtest Finished ---')
    final_value = self.broker.getvalue()
    print(f'Final Portfolio Value: {final_value:.2f}')
    print('--------------------------')
    
    
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
from Indicators.df_utils import count_symbols
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
    cerebro.addstrategy(RelativeBIDX, beta_period=90, beta_short_window=20, end_dates = end_dates) #<----------------------around 250 trading days per year
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

  
#------------------------------------------------------------------------------------------------------------------------------------------------------------
  
#TODO: 1. commitions + cheap stocks filtering 2. filter liquidity: dont want to
class MyStrategy(bt.Strategy):
    """Strategy rules
        
        bt (_type_): _description_
    """
    params = (
        ('beta_period', 360),
        ('beta_short_window', 20),
        ('end_dates', None),
    )    
    
    def __init__(self):
        """
        Prepares the strategy's working environment. Runs only once at the start.
        """
        print("--- Initializing Multi-Stock Strategy ---")

        # Step 1: Identify the market index feed.
        self.market_feed = self.datas[-1] #Market feed enters last.
        print(f"Market feed identified: {self.market_feed._name}")

        self.beta_indicators = {}
        self.orders_by_stock = {}
        for d in self.datas[:-1]:
            stock_symbol = d._name
            # Create a BetaIndex indicator for the current stock (d) 
            # against the common market feed (self.market_feed),
            # and store it in the dictionary with the stock symbol as the key.
            self.beta_indicators[stock_symbol] = BetaIndex(
                d,                      # The specific stock feed
                self.market_feed,       # The common market feed for all stocks
                period=self.p.beta_period,
                short_window=self.p.beta_short_window
            )
            # Initialize the placeholder for order tracking for this specific stock
            self.orders_by_stock[stock_symbol] = None
            print(f" > Initialized BetaIndex for stock: {stock_symbol}")
            
        print("--- Strategy Initialization Complete ---")
  
    def next(self):
           
        for d in self.datas[:-1]:
            stock_symbol = d._name
            if self.orders_by_stock.get(stock_symbol):
                continue
            if pd.isna(d.close[0]): # If today's data is invalid (NaN), skip
                continue
            position = self.getposition(d)
            #position
            if position:
                end_date_for_stock = self.p.end_dates.get(stock_symbol)
                current_date = d.datetime.date(0)
                # Exit Condition 1: Forced sell on the stock's last day of data
                if end_date_for_stock and current_date == end_date_for_stock.date():
                    print(f'--- FORCED SELL (End of Data) for {stock_symbol} on {current_date} ---')
                    self.sell(data=d)
                    continue  # Done with this stock for today
                # Exit Condition 2: Regular sell based on indicator signal
                # sell
                if len(d) > self.p.beta_period:
                    beta_val_recent = self.beta_indicators[stock_symbol].beta_index_recent[0]
                    if 1 < 0:
                        print(f'{d.datetime.date(0)} - SELL SIGNAL for {stock_symbol}, Recent Beta Index: {beta_val_recent:.2f}')
                        self.orders_by_stock[stock_symbol] = self.sell(data=d)   #stop loss             
                continue
            if len(d) <= self.p.beta_period:
                continue
            # BUY
            betaIdx = self.beta_indicators[stock_symbol].beta_index[0]
            betaIdxRecent = self.beta_indicators[stock_symbol].beta_index_recent[0]
            if (betaIdx > 0.5 and betaIdxRecent > 0.5):
                print(f'{d.datetime.date(0)} - BUY SIGNAL for {stock_symbol}, Beta Index: {betaIdx:.2f}, Recent Beta Index: {betaIdxRecent:.2f}')
                buy_order = self.buy(data=d)
                self.orders_by_stock[stock_symbol] = buy_order
                self.sell(data=d, parent = buy_order, exectype=bt.Order.StopTrail, trailpercent=0.08)
                self.sell(data=d, parent=buy_order, exectype=bt.Order.Limit, price=d.close[0] * 1.15)                
    def notify_order(self, order):
        stock_symbol = order.data._name
        if order.status in [order.Submitted, order.Accepted]:
            # order is already in process
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
            elif order.issell():
                print(f'SELL EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f'Order for {stock_symbol} was Canceled/Margin/Rejected')

        #restarting the orders to enable new ones
        self.orders_by_stock[stock_symbol] = None
        
def notify_trade(self, trade):
    # Only execute the block if the trade has been closed
    if trade.isclosed:
        stock_symbol = trade.data._name
        
        # Get the opening and closing datetime objects
        trade_open_date = trade.open_datetime()
        trade_close_date = trade.close_datetime()

        # Format and print the trade details
        print(f'--- TRADE CLOSED for {stock_symbol} ---')
        print(f'OPENED: {trade_open_date.strftime("%Y-%m-%d %H:%M:%S")} CLOSED: {trade_close_date.strftime("%Y-%m-%d %H:%M:%S")}')
        print(f'PROFIT/LOSS: Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}')
        print('-----------------------------------------')
def stop(self):
    print('--- Backtest Finished ---')
    final_value = self.broker.getvalue()
    print(f'Final Portfolio Value: {final_value:.2f}')
    print('--------------------------')
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  

  
  
  
  
  
  
  
  
  
  
  
  
  
