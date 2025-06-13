# src/c.py
from sqlalchemy import create_engine, inspect, text
import pandas as pd

# 🔗 התחברות לבסיס הנתונים
DATABASE_URL = "postgresql://postgres:ktZfeATiNpDOJUNfkBxbZcpTDkZpBfTg@centerbeam.proxy.rlwy.net:42148/railway"
engine = create_engine(DATABASE_URL)
inspector = inspect(engine)

# 🔍 הדפסת שמות העמודות בכל טבלה
for table in ["daily_stock_data", "sp500_index"]:
    if table in inspector.get_table_names():
        print(f"\n🔎 עמודות בטבלה: {table}")
        columns = [col["name"] for col in inspector.get_columns(table)]
        for col in columns:
            print(f"- {col}")
    else:
        print(f"⚠️ הטבלה {table} לא קיימת במסד הנתונים")

# 👀 תצוגת עמודות לדוגמה מתוך daily_stock_data
print("\n👀 דוגמה לעמודות מהטבלה daily_stock_data:")
df = pd.read_sql("SELECT * FROM daily_stock_data LIMIT 5", con=engine)
print(df.columns.tolist())

# 📊 ספירת ערכים בעמודת split
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT split, COUNT(*) 
        FROM daily_stock_data 
        GROUP BY split
    """))
    print("\n📊 ערכים בטור split:")
    for row in result:
        print(f"{row[0]}: {row[1]}")
