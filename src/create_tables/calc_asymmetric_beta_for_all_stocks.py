"""
📊 calc_asymmetric_beta_for_all_stocks.py

🔍 מטרה:
חישוב בטא אסימטרית (β⁺ ו־β⁻) *לכל מניה* במסד הנתונים, על בסיס חלון נע של 20 ימים,
רק עבור תקופת ה־TRAIN, ושמירת התוצאה עם עמודת SPLIT.

📥 קלט:
- טבלת המניות: `daily_stock_data` (רק split='train')
- טבלת המדד: `sp500_index` (רק split='train')

📤 פלט:
- טבלה `asymmetric_betas` עם העמודות:
    • date
    • symbol
    • beta_up
    • beta_down
    • split = 'train' ← מאפשר סינון downstream

🎯 הערה עתידית:
אם תורחב התמיכה גם ל־validation/test – יש לעבור בלולאה גם על splits נוספים,
או לשנות את הערך של עמודת split בהתאם לקלט.
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
df_stocks = pd.read_sql("SELECT date, symbol, close FROM daily_stock_data WHERE split = 'train'", engine)
df_market = pd.read_sql("SELECT date, close FROM sp500_index WHERE split = 'train'", engine)
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
final_df["split"] = "train"

# שלח את זה ל־DB
final_df.to_sql("asymmetric_betas", con=engine, if_exists="replace", index=False)

print("✅ asymmetric_betas table created and saved to DB")
