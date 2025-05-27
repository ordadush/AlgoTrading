import pandas as pd
from db_utils import save_dataframe_to_db

csv_path = "data/market_index.csv"

# שלב 1: קריאה לקובץ
df = pd.read_csv(csv_path)

# שלב 2: שינוי שמות עמודות
df = df.rename(columns={
    "Price": "Close",
    "Vol.": "Volume",
    "Change %": "ChangePercent"
})

# שלב 3: ניקוי ערכים
df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")
for col in ["Open", "High", "Low", "Close"]:
    df[col] = df[col].replace(",", "", regex=True).astype(float)

if "Volume" in df.columns:
    df["Volume"] = df["Volume"].replace(",", "", regex=True)
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

# הדפסת בדיקה
print("📊 מנקה נתונים... הנה טעימה:")
print(df[["Date", "Open", "High", "Low", "Close"]].head())

# שלב 4: שליחה למסד הנתונים (Railway)
save_dataframe_to_db("^GSPC", df)
