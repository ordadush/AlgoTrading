"""
📁 File: add_split_columns.py
🎯 Adds or updates a 'split' column ('train', 'validation', 'test')
    in daily_stock_data and sp500_index tables in the Railway DB.
"""

import pandas as pd
from sqlalchemy import create_engine, inspect, text

# === 🔐 Database connection
DATABASE_URL = "postgresql://postgres:ktZfeATiNpDOJUNfkBxbZcpTDkZpBfTg@centerbeam.proxy.rlwy.net:42148/railway"
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

# === 🧭 Split ranges by date
split_bins = [
    pd.to_datetime("2013-01-01"),
    pd.to_datetime("2020-01-01"),
    pd.to_datetime("2023-01-01"),
    pd.to_datetime("2025-12-31")
]
split_labels = ["train", "validation", "test"]

# === 🧩 Add split column if missing
def ensure_split_column(table_name):
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 1"))
        current_columns = result.keys()
        print(f"📋 Columns in table {table_name}: {current_columns}")

    if "split" not in current_columns:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN split TEXT"))
        print(f"➕ Added 'split' column to {table_name}")
    else:
        print(f"ℹ️ 'split' column already exists in {table_name}")

# === ⚡ Efficient bulk update of 'split'
def update_split_values(table_name):
    print(f"\n🔄 Updating SPLIT values in: {table_name}")
    df = pd.read_sql(f"SELECT date FROM {table_name}", con=engine)

    if "date" not in df.columns:
        print(f"⛔ Skipping {table_name} – no 'date' column")
        return

    # 🕒 Prepare split values
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["split"] = pd.cut(df["date"], bins=split_bins, labels=split_labels, right=False)

    # 📄 Save temp table
    temp_table = f"{table_name}_split_temp"
    df[["date", "split"]].dropna().to_sql(temp_table, con=engine, if_exists='replace', index=False)

    # ⚙️ Update main table using temp
    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE {table_name} AS main
            SET split = temp.split
            FROM {temp_table} AS temp
            WHERE main.date = temp.date
        """))

    print(f"✅ SPLIT values updated for {df['split'].notna().sum()} rows in {table_name}")

# === 🚀 Run on both tables
for table in ["daily_stock_data", "sp500_index"]:
    if table in inspector.get_table_names():
        ensure_split_column(table)
        update_split_values(table)
    else:
        print(f"⚠️ Table {table} not found in the database.")
