# strategy_simulation.py
# ------------------------

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

from src.entry_conditions import is_strong_stock

# טען גישה
load_dotenv()
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

# טען נתונים
betas = pd.read_sql("SELECT * FROM asymmetric_betas WHERE split = 'train'", engine)
stocks = pd.read_sql("SELECT date, symbol, close FROM daily_stock_data WHERE split = 'train'", engine)
index = pd.read_sql("SELECT date, close FROM sp500_index WHERE split = 'train'", engine)
market_regimes = pd.read_sql("SELECT date, market_trend FROM sp500_index WHERE split = 'train'", engine)

# חישוב תשואות יומיות
stocks = stocks.sort_values(["symbol", "date"])
stocks["stock_return"] = stocks.groupby("symbol")["close"].pct_change()
index = index.sort_values("date")
index["market_return"] = index["close"].pct_change()

# מיזוג נתונים
df = stocks.merge(index[["date", "market_return"]], on="date")
df = df.merge(betas, on=["date", "symbol"])
df = df.merge(market_regimes, on="date")  # כולל עמודת market_trend

positions = []      # רשימת פוזיציות פתוחות
trades_log = []     # תיעוד עסקאות

for date in sorted(df["date"].unique()):
    daily_data = df[df["date"] == date]
    market_today = daily_data["market_trend"].iloc[0]

    # תנאי שוק – רק שוק שורי
    if market_today != "bull":
        continue

    for _, row in daily_data.iterrows():
        symbol = row["symbol"]
        if symbol in [p["symbol"] for p in positions]:
            continue  # כבר בפוזיציה

        # תנאי כניסה
        if (
            row["stock_return"] > row["market_return"] and
            is_strong_stock(row["beta_up"], row["beta_down"])
        ):
            positions.append({
                "symbol": symbol,
                "entry_date": date,
                "entry_price": row["close"]
            })

    # בדיקת תנאי יציאה
    for p in positions[:]:  # העתק כדי לא להסיר תוך כדי איטרציה
        today_row = df[(df["symbol"] == p["symbol"]) & (df["date"] == date)]
        if today_row.empty:
            continue

        row = today_row.iloc[0]
        is_bear_market = row["market_trend"] != "bull"
        is_weak_now = (
            row["stock_return"] < row["market_return"] or
            not is_strong_stock(row["beta_up"], row["beta_down"])
        )

        if is_bear_market or is_weak_now:
            return_pct = (row["close"] - p["entry_price"]) / p["entry_price"]
            trades_log.append({
                "symbol": p["symbol"],
                "entry_date": p["entry_date"],
                "exit_date": date,
                "return": return_pct,
                "reason": "exit_condition"
            })
            positions.remove(p)

# שמירה למסד
log_df = pd.DataFrame(trades_log)
log_df.to_sql("trades_log", con=engine, if_exists="replace", index=False)
print("✅ Trades saved to 'trades_log'")
