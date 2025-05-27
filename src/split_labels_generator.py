import pandas as pd
from database import engine

# טען את כל הנתונים
df = pd.read_sql("SELECT * FROM stock_prices ORDER BY date", con=engine)

# צור עמודת split לפי תאריך
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

# הדפסה לבדיקה
print("📊 מספר שורות בכל קבוצה:")
print(df["split"].value_counts())

# שמירה זמנית לקובץ CSV (לבדיקה או הדבקה חזרה ל-DB אם צריך)
df.to_csv("data/full_dataset_with_split.csv", index=False)
