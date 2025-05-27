import pandas as pd
from db_utils import save_dataframe_to_db
from database import SessionLocal
from models import StockPrice

# 🔄 מסלול לקובץ החדש
csv_path = "data/S&P 500 Historical Data.csv"

# 🧼 שלב 1: קריאה וניקוי
df = pd.read_csv(csv_path)

# שינוי שמות עמודות
df = df.rename(columns={
    "Price": "Close",
    "Open": "Open",
    "High": "High",
    "Low": "Low",
    "Vol.": "Volume",
    "Change %": "ChangePercent"
})

# המרת תאריך
df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y")

# המרת עמודות מספריות
for col in ["Open", "High", "Low", "Close"]:
    df[col] = df[col].replace(",", "", regex=True).astype(float)

# ניקוי עמודת נפח
df["Volume"] = df["Volume"].replace(",", "", regex=True).replace("K", "e3", regex=True).replace("M", "e6", regex=True).replace("B", "e9", regex=True)
df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

# הדפסה לבדיקה
print("📊 הנה טעימה מהנתונים לאחר ניקוי:")
print(df[["Date", "Open", "High", "Low", "Close"]].head())

# 🧨 שלב 2: מחיקת הנתונים הקודמים של ^GSPC מה-DB
session = SessionLocal()
session.query(StockPrice).filter_by(symbol="^GSPC").delete()
session.commit()
session.close()

# 💾 שלב 3: שמירה מחדש
save_dataframe_to_db("^GSPC", df)
