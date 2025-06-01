# ===== split_labels_daily_stock_data.py =====
import pandas as pd
from sqlalchemy.orm import sessionmaker
from DBintegration.database import engine
from DBintegration.models import DailyStockData

# 🧱 פתיחת סשן למסד הנתונים
Session = sessionmaker(bind=engine)
session = Session()

# 📥 שליפת הנתונים מהטבלה
df = pd.read_sql("SELECT * FROM daily_stock_data ORDER BY date", con=engine)
df["date"] = pd.to_datetime(df["date"])

# 🧠 יצירת עמודת split לפי תאריכים
df["split"] = pd.cut(df["date"],
    bins=[
        pd.to_datetime("2015-01-01"),
        pd.to_datetime("2020-01-01"),
        pd.to_datetime("2023-01-01"),
        pd.to_datetime("2025-12-31")
    ],
    labels=["train", "validation", "test"],
    right=False
)

# 🔄 עדכון במסד הנתונים
for _, row in df.iterrows():
    session.query(DailyStockData).filter_by(symbol=row["symbol"], date=row["date"]).update({
        "split": row["split"]
    })

session.commit()
session.close()

# 🖨️ בדיקה
print("✅ פיצול עודכן בטבלת daily_stock_data:")
print(df["split"].value_counts())

# 💾 שמירה לקובץ CSV (אופציונלי)
df.to_csv("data/daily_stock_data_with_split.csv", index=False)
