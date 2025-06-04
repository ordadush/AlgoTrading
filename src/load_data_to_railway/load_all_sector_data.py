import os
import sys
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# ✅ הוספת נתיב ל־src בשביל ש־py יזהה את DBintegration
sys.path.append(str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from DBintegration.models import SectorData, DATABASE_URL

# 💾 טען את קובץ .env אם לא נטען עדיין
env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

# 🔌 התחברות למסד הנתונים
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# 📂 קבצי הסקטורים
data_dir = "data/sectors"
sector_files = [f for f in os.listdir(data_dir) if f.endswith("ETF Stock Price History.csv")]

records = []

for file_name in sector_files:
    symbol = file_name.split()[0]
    csv_path = os.path.join(data_dir, file_name)
    print(f"🔄 טוען את {symbol} מתוך {file_name}")

    df = pd.read_csv(csv_path)

    # עיבוד וסטנדרטיזציה
    df = df.rename(columns={
        "Price": "close",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Vol.": "volume",
        "Change %": "ChangePercent"
    })
    df["date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].replace(",", "", regex=True).astype(float)

    df["volume"] = df["volume"].replace(",", "", regex=True)
    df["volume"] = df["volume"].replace("K", "e3", regex=True).replace("M", "e6", regex=True).replace("B", "e9", regex=True)
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df["symbol"] = symbol

    for _, row in df.iterrows():
        record = SectorData(
            symbol=row["symbol"],
            date=row["date"],
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=int(row["volume"]) if pd.notnull(row["volume"]) else None
        )
        records.append(record)

# ✅ הוספה לטבלה קיימת
session.bulk_save_objects(records)
session.commit()
print(f"✅ הוזנו {len(records)} שורות חדשות לטבלה sector_data בהצלחה!")
print(f"📦 הסתיימה הטעינה. הטבלה sector_data כוללת כעת {session.query(SectorData).count()} שורות.")
