import pandas as pd
import os
from sqlalchemy.orm import sessionmaker
from DBintegration.models import DailyStockData, Base
from DBintegration.database import engine
from datetime import datetime

# 📁 Folder where Alpha Vantage CSVs are stored
cache_dir = "data/cache_alpha"

# 🧱 Prepare DB session
Base.metadata.create_all(bind=engine)
SessionLocal = sessionmaker(bind=engine)
session = SessionLocal()

# 🔁 Loop through all CSVs
csv_files = [f for f in os.listdir(cache_dir) if f.endswith(".csv")]
print(f"📂 Found {len(csv_files)} cached CSVs to insert.")

for file in csv_files:
    symbol = file.replace(".csv", "")
    path = os.path.join(cache_dir, file)

    try:
        df = pd.read_csv(path)

        # ✅ Rename Alpha Vantage columns
        df.rename(columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. volume": "volume",
        }, inplace=True)

        # ✅ Normalize date column
        if "Date" not in df.columns and "date" in df.columns:
            df.rename(columns={"date": "Date"}, inplace=True)

        if "Date" not in df.columns:
            print(f"⚠️ Skipping {symbol}: no 'Date' column found.")
            continue

        print(f"🧹 Deleting existing rows for {symbol}...")
        session.query(DailyStockData).filter_by(symbol=symbol).delete()

        print(f"📊 Inserting {len(df)} rows for {symbol}...")

        stock_rows = []
        for _, row in df.iterrows():
            if pd.isnull(row["Date"]):
                continue

            stock_rows.append(DailyStockData(
                symbol=symbol,
                date=pd.to_datetime(row["Date"]),
                open=float(row["open"]) if not pd.isnull(row["open"]) else None,
                high=float(row["high"]) if not pd.isnull(row["high"]) else None,
                low=float(row["low"]) if not pd.isnull(row["low"]) else None,
                close=float(row["close"]) if not pd.isnull(row["close"]) else None,
                volume=int(row["volume"]) if not pd.isnull(row["volume"]) else 0,
                split="train"
            ))

        if stock_rows:
            session.bulk_save_objects(stock_rows)
            session.commit()
            print(f"✅ {symbol} inserted to DB")
        else:
            print(f"⚠️ {symbol} has no valid rows to insert.")

    except Exception as e:
        print(f"❌ Failed to insert {symbol}: {e}")
        session.rollback()

session.close()
print("🎉 All done loading to DB.")
