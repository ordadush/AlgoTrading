#!/usr/bin/env python3
# ophir.py
#
# 1) Deletes all existing rows in daily_stock_data
# 2) Reads each CSV in ../snp500_data/ (relative to src/)
# 3) Filters to dates ≥ 2013-01-01, drops rows with missing data,
#    and inserts into DailyStockData.

import glob
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from DBintegration.models import Base, DailyStockData, DATABASE_URL

def main():
    # --- 1) Set up the DB engine & session ---
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    # --- 1a) Create tables if they don't exist ---
    Base.metadata.create_all(engine)

    # --- 1b) Clear out existing rows to avoid duplicates ---
    try:
        session.query(DailyStockData).delete()
        session.commit()
        print("Cleared existing daily_stock_data rows.")
    except Exception as e:
        session.rollback()
        print(f"ERROR clearing daily_stock_data: {e}")
        session.close()
        return

    # --- 2) Locate the "snp500_data" folder (one level up from src/) ---
    data_dir = Path(__file__).resolve().parent.parent / "snp500_data"
    if not data_dir.exists():
        print(f"ERROR: Couldn't find directory: {data_dir}")
        session.close()
        return

    cutoff = pd.to_datetime("2013-01-01")

    # --- 3) Loop through each CSV file in ../snp500_data/ ---
    csv_pattern = str(data_dir / "*.csv")
    for csv_path in glob.glob(csv_pattern):
        file_path = Path(csv_path)
        symbol = file_path.stem.upper()  # e.g. "AAPL.csv" → symbol = "AAPL"

        # Read CSV into a DataFrame
        df = pd.read_csv(file_path)

        # Parse "Date" as datetime, then filter on cutoff
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"] >= cutoff]
        if df.empty:
            continue

        # Drop the "Adj Close" column if it exists
        if "Adj Close" in df.columns:
            df = df.drop(columns=["Adj Close"])

        # Drop any row where one of the required columns is NA/NaT
        required = ["Date", "Open", "High", "Low", "Close", "Volume"]
        df = df.dropna(subset=required)
        if df.empty:
            continue

        # Verify all required columns are still present
        if not set(required).issubset(df.columns):
            print(f"WARNING: File {file_path.name} missing columns → skipping.")
            continue

        inserted_count = 0
        for _, row in df.iterrows():
            record = DailyStockData(
                date   = row["Date"].date(),
                symbol = symbol,
                open   = float(row["Open"]),
                high   = float(row["High"]),
                low    = float(row["Low"]),
                close  = float(row["Close"]),
                volume = int(row["Volume"])
                # All other fields (ema_20, rsi_14, etc.) remain None/NULL
            )
            session.add(record)
            inserted_count += 1

        # Commit after each symbol to avoid a giant transaction
        try:
            session.commit()
            print(f"Inserted {inserted_count} rows for {symbol}.")
        except Exception as e:
            session.rollback()
            print(f"ERROR inserting {symbol}: {e}")

    session.close()
    print("Done uploading all symbols with data ≥ 2013-01-01.")

if __name__ == "__main__":
    main()
