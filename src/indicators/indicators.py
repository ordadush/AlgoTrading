#start from WorkingDir = src
#remember to  activate Algo_env:Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
#run goes with : python -m Indicators.indicators
# PreRun  .\DBintegration\database.py
#this line should be included everywhere so you could import func from anothre folder (i.e adds src to Path)
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from DBintegration.db_utils import model_to_dataframe  
from DBintegration.models import StockPrice  # or any other model you want to use
from sqlalchemy.orm import sessionmaker
from DBintegration.models import Base
from pathlib import Path
import pandas as pd
#filters out the ETFs 
df = model_to_dataframe(StockPrice)
org_len = len(df)
symbols = ['^GSPC','XLC','XLY','XLP','XLE','XLF','XLV','XLI','XLB','XLRE','XLK','XLU']
df_market    = df[df['symbol'].isin(symbols)].reset_index(drop=True)
df_sectors = df[~df['symbol'].isin(symbols)].reset_index(drop=True)
print(org_len)
print("With symbols:   ", len(df_market))
print("Without symbols:", len(df_sectors))