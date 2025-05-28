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

    # המרת עמודות מספריות
    for col in ["Open", "High", "Low", "Close"]:
        df[col] = df[col].replace(",", "", regex=True).astype(float)

    # ניקוי עמודת Volume
    df["Volume"] = df["Volume"].replace(",", "", regex=True)
    df["Volume"] = df["Volume"].replace("K", "e3", regex=True).replace("M", "e6", regex=True).replace("B", "e9", regex=True)
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # 🟢 הוספת העמודה symbol – זאת השורה שאמרתי קודם:
    df["symbol"] = symbol

    # 🆕 כאן שימי את ההדפסות:
    print(f"📥 מנסה לשמור {symbol} ל-DB...")
    save_dataframe_to_db(symbol, df)
    print(f"✅ {symbol} נשמר בהצלחה!")
