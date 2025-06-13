"""
📊 compute_split_betas.py – חישוב ביטא אסימטרית, מתאם ו־R² מול השוק

🔍 מטרה:
- לחשב לכל מניה:
    • β⁺ – ביטא חיובית: כשהשוק עולה
    • β⁻ – ביטא שלילית: כשהשוק יורד
    • מתאם (Correlation) מול השוק
    • מקדם הסבר R² בין תשואות המניה לתשואות השוק

📥 קלט:
- נתוני מסד הנתונים מתוך Railway:
    • daily_stock_data (רק split='train')
    • sp500_index (רק split='train')

📤 פלט:
- קובץ CSV בשם beta_split_with_corr_r2.csv עם העמודות:
    • symbol, beta_pos, beta_neg, beta_diff, correlation, r_squared
- תרשים פיזור של correlation מול R² לזיהוי מניות מתואמות

🎯 שימוש:
- לניפוי מניות שאינן מראות קשר מובהק (אפילו קלוש) לשוק הכללי
"""


import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# טען משתני סביבה
load_dotenv()
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

# 📥 שליפת נתוני המניות והשוק (train בלבד)
df_stocks = pd.read_sql("SELECT * FROM daily_stock_data WHERE split='train'", engine)
df_market = pd.read_sql("SELECT * FROM sp500_index WHERE split='train'", engine)

# ✅ מיון וחישוב תשואות
df_stocks = df_stocks.sort_values(by=["symbol", "date"])
df_market = df_market.sort_values(by="date")

df_stocks["stock_return"] = df_stocks.groupby("symbol")["close"].pct_change()
df_market["market_return"] = df_market["close"].pct_change()

def compute_split_betas(df_stocks, df_market):
    results = []

    for symbol in df_stocks['symbol'].dropna().unique():
        df_sym = df_stocks[df_stocks['symbol'] == symbol].copy()

        df_merge = pd.merge(
            df_sym[['date', 'stock_return']],
            df_market[['date', 'market_return']],
            on='date'
        ).dropna()

        if len(df_merge) < 30:
            continue

        up = df_merge[df_merge['market_return'] > 0]
        down = df_merge[df_merge['market_return'] < 0]

        if len(up) < 10 or len(down) < 10:
            continue

        beta_pos = np.cov(up['stock_return'], up['market_return'])[0, 1] / np.var(up['market_return'])
        beta_neg = np.cov(down['stock_return'], down['market_return'])[0, 1] / np.var(down['market_return'])
        correlation = df_merge['stock_return'].corr(df_merge['market_return'])

        X = df_merge['market_return'].values.reshape(-1, 1)
        y = df_merge['stock_return'].values
        r_squared = LinearRegression().fit(X, y).score(X, y)

        results.append({
            'symbol': symbol,
            'beta_pos': beta_pos,
            'beta_neg': beta_neg,
            'beta_diff': beta_pos - beta_neg,
            'correlation': correlation,
            'r_squared': r_squared
        })

    return pd.DataFrame(results)

# 🧮 הפעלת הפונקציה
df_results = compute_split_betas(df_stocks, df_market)

# 💾 שמירה לקובץ CSV
df_results.to_csv("beta_split_with_corr_r2.csv", index=False)
print("✅ נוצר הקובץ: beta_split_with_corr_r2.csv")
# 📊 הדפסת Top 10 מניות עם הבדל הכי גדול בין ביטא חיובית לשלילית
print("\n🏆 Top 10 symbols with largest |beta_pos - beta_neg|:")
top_diff = df_results.copy()
top_diff["abs_diff"] = top_diff["beta_diff"].abs()
top_diff = top_diff.sort_values(by="abs_diff", ascending=False).drop(columns="abs_diff")
print(top_diff.head(10))
import matplotlib.pyplot as plt

plt.scatter(df_results['correlation'], df_results['r_squared'])
plt.axhline(0.02, color='red', linestyle='--', label='R² = 0.02')
plt.axvline(0.1, color='orange', linestyle='--', label='Correlation = 0.1')
plt.xlabel("Correlation with market")
plt.ylabel("R² with market")
plt.title("התאמה בין מניות לשוק")
plt.legend()
plt.grid(True)
plt.show()
