"""
📁 File: add_split_columns.py
🎯 Adds a 'split' column ('train', 'validation', 'test') to daily_stock_data, sp500_index, and sectors tables (if exist) in the Railway DB.
📥 Input: 'daily_stock_data', 'sp500_index', 'sectors'
📤 Output: Updated tables with 'split' column
"""

import pandas as pd
from sqlalchemy import create_engine, inspect

# === 🔐 התחברות למסד הנתונים
DATABASE_URL = "postgresql://postgres:LMilshujDuGlABeVjVvBvdhGHYZkrhBr@trolley.proxy.rlwy.net:32659/railway"
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

# === 🧭 חלוקה לפי תאריכים (2013-2025)
split_bins = [
    pd.to_datetime("2013-01-01"),
    pd.to_datetime("2020-01-01"),
    pd.to_datetime("2023-01-01"),
    pd.to_datetime("2025-12-31")
]
split_labels = ["train", "validation", "test"]

# === 🧩 פונקציה להוספת SPLIT
def add_split_column(table_name, sort_column="date", output_table=None):
    print(f"\n🔄 Processing: {table_name}")
    df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY {sort_column}", con=engine)
    df[sort_column] = pd.to_datetime(df[sort_column], errors="coerce")
    df["split"] = pd.cut(df[sort_column], bins=split_bins, labels=split_labels, right=False)

    print(f"📊 {table_name} split counts:")
    print(df["split"].value_counts())

    save_name = output_table if output_table else table_name
    df.to_sql(save_name, con=engine, if_exists="replace", index=False)
    print(f"✅ Saved table: {save_name} (with split)")

# === 🧪 בדיקה והרצה לפי אילו טבלאות קיימות
if "daily_stock_data" in inspector.get_table_names():
    add_split_column("daily_stock_data", output_table="daily_stock_data_split")

if "sp500_index" in inspector.get_table_names():
    add_split_column("sp500_index")

if "sectors" in inspector.get_table_names():
    add_split_column("sectors")
