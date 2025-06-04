#start from WorkingDir = src
#remember to  activate Algo_env: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser , .\Algo_env\Scripts\activate
#run goes with : python -m Indicators.indicators
# PreRun  .\DBintegration\database.py
#this line should be included everywhere so you could import func from anothre folder (i.e adds src to Path)
#%%
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DBintegration.models import DailyStockData
from DBintegration.models import SP500Index# or any other model you want to use
from sqlalchemy.orm import sessionmaker
from DBintegration.models import Base
from pathlib import Path
from Indicators.df_utils import count_symbols
import pandas as pd
from DBintegration.db_utils import *
#filters out the ETFs 
#%%
df = model_to_dataframe(DailyStockData)
org_len = len(df)
print(org_len)
#%%

n = count_symbols(df)
print(f"unique symbols: {n}")
# %%
n = count_symbols(df)
print(f"Unique symbols before pruning: {n}")

#%%
# Identify symbols with fewer than 3100 rows
symbol_counts = df.groupby("symbol").size()
symbols_to_remove = symbol_counts[symbol_counts < 3100].index.tolist()

print(f"Symbols to remove (fewer than 3100 rows): {len(symbols_to_remove)}")
print(symbols_to_remove)

# Use remove_data to delete all rows for those symbols
if symbols_to_remove:
    remove_data(DailyStockData, symbols_to_remove)

#%%
# (Optional) Reload the DataFrame and report counts after deletion
df_after = model_to_dataframe(DailyStockData)
new_len = len(df_after)
print(f"Total rows in DB after pruning: {new_len}")

n_after = count_symbols(df_after)
print(f"Unique symbols after pruning: {n_after}")
# %%
df_s&p = model_to_dataframe()
