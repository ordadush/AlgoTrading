import pandas as pd
import os
import glob

# 🗂️ 1. קריאת כל קובצי הסקטורים
sector_dir = "data/sectors"
sector_files = glob.glob(os.path.join(sector_dir, "*.csv"))

dfs = []

for file in sector_files:
    symbol = os.path.basename(file).split()[0]  # לדוגמה "XLF" מתוך "XLF ETF Stock Price History.csv"
    df = pd.read_csv(file)
    df["symbol"] = symbol
    dfs.append(df)

# 🏦 2. קריאת קובץ השוק הכללי
market_path = "data/S&P 500 Historical Data.csv"
df_market = pd.read_csv(market_path)
df_market["symbol"] = "^GSPC"
dfs.append(df_market)

# 🧩 3. איחוד כל הדאטות
combined_df = pd.concat(dfs, ignore_index=True)

# 🕒 4. המרת עמודת תאריך
combined_df["Date"] = pd.to_datetime(combined_df["Date"], format="%m/%d/%Y")

# 📆 5. הוספת split לפי bins
combined_df["split"] = pd.cut(
    combined_df["Date"],
    bins=[
        pd.to_datetime("2015-01-01"),
        pd.to_datetime("2020-01-01"),
        pd.to_datetime("2023-01-01"),
        pd.to_datetime("2025-12-31")
    ],
    labels=["train", "validation", "test"],
    right=False
)

# 📊 6. הדפסת סיכום
print("✅ סיכום לפי תקופות:")
print(combined_df["split"].value_counts())

# 💾 7. שמירה
output_path = "data/market_and_sectors_with_split.csv"
combined_df.to_csv(output_path, index=False)
print(f"\n📁 נשמר בהצלחה: {output_path}")
