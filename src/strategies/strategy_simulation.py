# src/strategies/strategy_simulation.py
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from src.entry_conditions import is_strong_stock

# טען משתני סביבה
load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

# טען נתונים
betas = pd.read_sql("SELECT * FROM asymmetric_betas WHERE split = 'train'", engine)
market_trend = pd.read_sql("SELECT date, market_trend FROM sp500_index WHERE split = 'train'", engine)

# מזג לפי תאריך
df = betas.merge(market_trend, on="date", how="inner")

# סנן רק ימים שוק שורי (לפי Markov)
bull_states = ["bull_strong", "bull_weak"]  # שנה לפי איך שהמודל שלך מתייג
df = df[df["market_trend"].isin(bull_states)].copy()

# בדוק מי מניות חזקות מהשוק
df["is_strong"] = df.apply(lambda row: is_strong_stock(row["beta_up"], row["beta_down"]), axis=1)
df = df[df["is_strong"] == True]

# שמור קובץ Trade Log
df[["date", "symbol", "beta_up", "beta_down"]].to_csv("data/tradelog.csv", index=False)
print(f"✅ נשמרו {len(df)} עסקאות בקובץ trade_log.csv")

# אפשרות: הצצה לימי מסחר
print(df.groupby("date")["symbol"].nunique().sort_values(ascending=False).head(10))
print(f"שורות ב־bull_states: {len(df)}")
print(market_trend["market_trend"].value_counts())
