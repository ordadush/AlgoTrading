import os
import pandas as pd
from db_utils import save_dataframe_to_db
from database import SessionLocal
from models import StockPrice

# 🗂️ נתיב לקבצים
data_dir = "data/sectors"
sector_files = [f for f in os.listdir(data_dir) if f.endswith("ETF Stock Price History.csv")]

for file_name in sector_files:
    symbol = file_name.split()[0]  # לדוגמה XLF מתוך "XLF ETF Stock Price History.csv"
    csv_path = os.path.join(data_dir, file_name)
    
    print(f"🔄 טוען את {symbol} מתוך {file_name}")

    # קריאה
    df = pd.read_csv(csv_path)

    # שינוי שמות עמודות
    df = df.rename(columns={
        "Price": "Close",
        "Open": "Open",
        "High": "High",
        "Low": "Low",
        "Vol.": "Volume",
        "Change %": "ChangePercent"
    })

    # המרת תאריך
    df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")

    # המרת עמודות מס
