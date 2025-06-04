#run goes with : python -m Indicators.indicators
# PreRun  .\DBintegration\database.py
#this line should be included everywhere so you could import func from anothre folder (i.e adds src to Path)
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))) 
from DBintegration.models import DailyStockData  # or any other model you want to use
from sqlalchemy.orm import sessionmaker
from alpha_vantage.timeseries import TimeSeries
from DBintegration.models import Base
from pathlib import Path
import pandas as pd
from DBintegration.db_utils import fetch_and_store_data
from alpha_vantage.timeseries import TimeSeries
from DBintegration.db_utils import model_to_dataframe
from DBintegration.db_utils import csv_upload
df = model_to_dataframe(DailyStockData)
print("Number of rows in DataFrame:", len(df),"\n")
BASE_DIR = Path.cwd()

path = BASE_DIR.parent / "filtered_nasdaq.csv"
csv_upload(csv_path=str(path), model= "stock")
df = model_to_dataframe(DailyStockData)
print("Number of rows in DataFrame:", len(df))