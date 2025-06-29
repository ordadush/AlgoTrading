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
from sqlalchemy.orm import sessionmaker
from DBintegration.models import Base
from pathlib import Path
from Indicators.df_utils import count_symbols
from Indicators.df_utils import split_dataframe_by_dates
from DBintegration.db_utils import *
from DBintegration.db_utils import remove_data #gets model, list of ymbols to remove. do it in cloud
from DBintegration.db_utils import model_to_dataframe 
import backtrader as bt
import pandas as pd
#added coloumn (variable class)
#df_train, df_val, df_test = split_dataframe_by_dates(df_main) im saving it to remember this function exists.
class SP500IndexWithScore(bt.feeds.PandasData): #bt pseudo constructor
    """
    Custom data feed for the S&P 500 index that includes the 'score' line.
    """
    lines = ('score',)
    params = (('score', 6),) #score is the coloumn[6]

print("Loading daily stock data for all symbols:")
df_main = model_to_dataframe(DailyStockData)
df_main['date'] = pd.to_datetime(df_main['date'])
df_main.set_index('date', inplace=True)
df_main.sort_values(by=['symbol', 'date'], inplace=True)

stocks_num = len(df_main)
print(stocks_num) #here just to make sure it worked

print("Loading snp data:")
df_sp500 = model_to_dataframe(SP500Index)
df_sp500['date'] = pd.to_datetime(df_sp500['date'])
df_sp500.set_index('date', inplace=True)
df_sp500.sort_index(inplace=True)

#%%
if __name__ == '__main__':
    # Initialize the engine
    cerebro = bt.Cerebro()
    print("Cerebro engine initialized.")
    grouped = df_main.groupby('symbol') #individual data feeds
    for symbol, data in list(grouped)[:20]: #<----------when running for real, change here the 20 stocks limitation. 
        df_single_stock = data.set_index('date') # Already sorted
        stock_feed = bt.feeds.PandasData(dataname=df_single_stock)
        cerebro.adddata(stock_feed, name=symbol)

    # --- Add the Custom S&P 500 Data Feed ---
    # This feed provides both OHLC for beta and the 'score' for logic
    sp500_feed = SP500IndexWithScore(dataname=df_sp500)
    cerebro.adddata(sp500_feed, name='sp500_feed')
    print("All data feeds have been loaded into Cerebro.")






