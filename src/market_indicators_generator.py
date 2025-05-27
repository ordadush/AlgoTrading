# src/market_indicators_generator.py
import pandas as pd
from database import engine

# טען את הנתונים עם פיצול
df = pd.read_sql("SELECT * FROM stock_prices ORDER BY date", con=engine)

# שמור רק את השוק ואת תקופת האימון
df = df[(df["symbol"] == "^GSPC") & (df["split"] == "train")].copy()
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# אינדיקטור MA200
df["ma200"] = df["Close"].rolling(window=200).mean()

# אינדיקטור MACD
ema12 = df["Close"].ewm(span=12, adjust=False).mean()
ema26 = df["Close"].ewm(span=26, adjust=False).mean()
df["macd"] = ema12 - ema26
df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()

# אינדיקטור TD (Top-Down, λ=15%)
lambda_ = 0.15
df["td_trend"] = 0  # ניטרלי בהתחלה
high = df["Close"].iloc[0]
low = df["Close"].iloc[0]
trend = 0

for i in range(1, len(df)):
    price = df["Close"].iloc[i]
    if trend <= 0 and price >= low * (1 + lambda_):
        trend = 1
        high = price
    elif trend >= 0 and price <= high * (1 - lambda_):
        trend = -1
        low = price
    df.at[df.index[i], "td_trend"] = trend
    if trend == 1:
        high = max(high, price)
    elif trend == -1:
        low = min(low, price)

# סינון תאריכים ואחסון לקובץ
df_out = df[["date", "ma200", "macd", "macd_signal", "td_trend"]].copy()
df_out.to_csv("data/market_indicators_train.csv", index=False)
print("✅ שמרתי אינדיקטורים עבור תקופת האימון.")
