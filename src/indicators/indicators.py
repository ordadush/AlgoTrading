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
import pandas as pd
from DBintegration.db_utils import *
from DBintegration.db_utils import remove_data #gets model, list of ymbols to remove. do it in cloud
from DBintegration.db_utils import model_to_dataframe 
#filters out the ETFs 
#%%
df = model_to_dataframe(DailyStockData)
org_len = len(df)
print(org_len)
df_train, df_val, df_test = split_dataframe_by_dates(df)
