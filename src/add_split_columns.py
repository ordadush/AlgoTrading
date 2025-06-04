"""
📁 File: add_split_columns.py
🎯 Adds a 'split' column ('train', 'validation', 'test') to daily_stock_data and sp500_index tables in the database, based on date ranges.
📥 Input: 'daily_stock_data', 'sp500_index' tables from Railway DB
📤 Output: Updated tables: 'daily_stock_data_split' and updated 'sp500_index' with split column
"""

# this script adds 'split' column to both daily_stock_data and sp500_index
import pandas as pd
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:LMilshujDuGlABeVjVvBvdhGHYZkrhBr@trolley.proxy.rlwy.net:32659/railway"
engine = create_engine(DATABASE_URL)

# הגדרת הגבולות והתגיות ל־split
split_bins = [
    pd.to_datetime("2015-01-01"),
    pd.to_datetime("2020-01-01"),
    pd.to_datetime("2023-01-01"),
    pd.to_datetime("2025-12-31")
]
split_labels = ["train", "validation", "test"]

# === 🔹 עדכון daily_stock_data_split ===
print("🔄 Processing: daily_stock_data")

df_stocks = pd.read_sql("SELECT * FROM daily_stock_data ORDER BY date", con=engine)
df_stocks["date"] = pd.to_datetime(df_stocks["date"], errors="coerce")

df_stocks["split"] = pd.cut(df_stocks["date"], bins=split_bins, labels=split_labels, right=False)

print("📊 daily_stock_data split counts:")
print(df_stocks["split"].value_counts())

df_stocks.to_sql("daily_stock_data_split", con=engine, if_exists="replace", index=False)
print("✅ Saved table: daily_stock_data_split")

# === 🔹 עדכון sp500_index ===
print("\n🔄 Processing: sp500_index")

df_sp500 = pd.read_sql("SELECT * FROM sp500_index ORDER BY date", con=engine)
df_sp500["date"] = pd.to_datetime(df_sp500["date"], errors="coerce")

df_sp500["split"] = pd.cut(df_sp500["date"], bins=split_bins, labels=split_labels, right=False)

print("📊 sp500_index split counts:")
print(df_sp500["split"].value_counts())

df_sp500.to_sql("sp500_index", con=engine, if_exists="replace", index=False)
print("✅ Saved table: sp500_index (with split)")
