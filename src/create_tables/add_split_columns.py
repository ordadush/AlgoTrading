"""
📁 File: add_split_columns.py
🎯 Adds a 'split' column ('train', 'validation', 'test') to daily_stock_data, sp500_index, and sector_data tables (if exist) in the Railway DB.
📥 Input: 'daily_stock_data', 'sp500_index', 'sector_data'
📤 Output: Updated tables with 'split' column
"""

import pandas as pd
from sqlalchemy import create_engine, inspect, text

# === 🔐 התחברות למסד הנתונים
DATABASE_URL = "postgresql://postgres:LMilshujDuGlABeVjVvBvdhGHYZkrhBr@trolley.proxy.rlwy.net:32659/railway"
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

# === 🧭 טווחי SPLIT לפי תאריכים
split_bins = [
    pd.to_datetime("2013-01-01"),
    pd.to_datetime("2020-01-01"),
    pd.to_datetime("2023-01-01"),
    pd.to_datetime("2025-12-31")
]
split_labels = ["train", "validation", "test"]

# === 🧩 פונקציה להוספת עמודת SPLIT רק אם היא לא קיימת
def ensure_split_column(table_name):
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    if "split" not in columns:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN split TEXT"))
        print(f"➕ Added 'split' column to {table_name}")
    else:
        print(f"ℹ️ 'split' column already exists in {table_name}")

# === ✍️ עדכון ערכי SPLIT
def update_split_column(table_name):
    print(f"\n🔄 Updating: {table_name}")
    df = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)

    if "date" not in df.columns:
        print(f"⛔ Skipping {table_name} – no 'date' column")
        return

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["split"] = pd.cut(df["date"], bins=split_bins, labels=split_labels, right=False)

    # שמירה חכמה: מחיקה זמנית והכנסה מחדש עם split
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {table_name}"))

    df.to_sql(table_name, con=engine, if_exists="append", index=False)
    print(f"✅ Updated {table_name} with 'split' column and saved {len(df)} rows")

# === 🔁 ריצה על כל טבלה שרלוונטית
for table in ["daily_stock_data", "sp500_index", "sector_data"]:
    if table in inspector.get_table_names():
        ensure_split_column(table)
        update_split_column(table)
