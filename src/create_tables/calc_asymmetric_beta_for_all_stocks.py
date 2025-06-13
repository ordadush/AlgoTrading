
"""
📊 calc_asymmetric_beta_for_all_stocks.py

🔍 מטרה:
חישוב בטא אסימטרית (β⁺ ו־β⁻) *לכל מניה* במסד הנתונים, על בסיס חלון נע של 20 ימים, והוספת התוצאה כטבלה נפרדת ב־PostgreSQL בשם: `asymmetric_betas`.

📥 קלט:
- טבלת המניות: `daily_stock_data`
- טבלת המדד: `sp500_index`
- חלון זמן לחישוב: ברירת מחדל 20 ימים
- הנתונים נלקחים רק עבור split='train'

📤 פלט:
- טבלה חדשה במסד הנתונים בשם `asymmetric_betas`
  עם העמודות:
    • date
    • symbol
    • beta_up   ← בטא עבור ימים שבהם השוק עלה
    • beta_down ← בטא עבור ימים שבהם השוק ירד

🎯 שימושים:
- בניית אסטרטגיות שמבוססות על חוזקה יחסית של מניה לשוק.
- ניתוח התנהגות שונה של מניות בשוק שורי לעומת שוק דובי.
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.linear_model import LinearRegression
from dotenv import load_dotenv
import os

from src.indicators.asymmetric_beta import compute_asymmetric_beta

# טען סביבה
load_dotenv()
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

# שלוף את נתוני המניות והשוק
df_stocks = pd.read_sql("SELECT date, symbol, close FROM daily_stock_data", engine)
df_market = pd.read_sql("SELECT date, close FROM sp500_index", engine)
df_market = df_market.rename(columns={"close": "close_market"})

# התוצאה הכוללת תיאגר כאן
all_results = []

for symbol in df_stocks['symbol'].unique():
    df_symbol = df_stocks[df_stocks['symbol'] == symbol].copy()
    df_symbol = df_symbol.sort_values("date").reset_index(drop=True)
    df_symbol = df_symbol.rename(columns={"close": "close_stock"})

    # חישוב ביטאות
    beta_df = compute_asymmetric_beta(df_symbol, df_market, window=20)
    beta_df["symbol"] = symbol
    all_results.append(beta_df)

# מיזוג התוצאות לטבלה אחת
final_df = pd.concat(all_results, ignore_index=True)

# שלח את זה ל־DB
final_df.to_sql("asymmetric_betas", con=engine, if_exists="replace", index=False)

print("✅ asymmetric_betas table created and saved to DB")
