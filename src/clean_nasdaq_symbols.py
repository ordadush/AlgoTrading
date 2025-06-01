# ===== src/clean_nasdaq_symbols.py =====
import pandas as pd

# 1. טען את הקובץ הקיים
df = pd.read_csv("filtered_nasdaq.csv")

# 2. מחק שורות בלי סימבול
df = df.dropna(subset=["Symbol"])

# 3. מחק שורות כפולות לפי סימבול
df = df.drop_duplicates(subset=["Symbol"])

# 4. שמור קובץ חדש לניקוי
df.to_csv("data/unique_nasdaq_symbols.csv", index=False)

# 5. הדפסות לבדיקה
print(f"✅ נשמר קובץ חדש עם {len(df)} מניות ייחודיות.")
print(df.head())
