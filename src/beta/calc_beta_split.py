# calc_beta_split.py
# ------------------
# Calculates two beta values per stock:
# - β⁺: beta for days when the market goes up
# - β⁻: beta for days when the market goes down
# Input: daily_stock_data and sp500_index tables in Railway DB
# Output: CSV of beta values per stock

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
def load_data(engine):
    df_stocks = pd.read_sql("""
    SELECT symbol, date, close
    FROM daily_stock_data_split
    WHERE split = 'train'
    ORDER BY symbol, date;
""", engine)


    df_stocks['close'] = df_stocks['close'].astype(float)
    df_stocks['stock_return'] = df_stocks.groupby('symbol')['close'].pct_change()
    df_stocks.dropna(subset=['stock_return'], inplace=True)

    # ✅ טוען את נתוני השוק הכללי ל־train בלבד (אם יש split)
    df_market = pd.read_sql("""
        SELECT date, close
        FROM sp500_index
        WHERE split = 'train'
        ORDER BY date;
    """, engine)

    df_market['close'] = df_market['close'].astype(float)
    df_market['market_return'] = df_market['close'].pct_change()
    df_market.dropna(subset=['market_return'], inplace=True)

    return df_stocks[['symbol', 'date', 'stock_return']], df_market[['date', 'market_return']]

def compute_split_betas(df_stocks, df_market):
    results = []

    for symbol in df_stocks['symbol'].unique():
        df_sym = df_stocks[df_stocks['symbol'] == symbol].copy()

        df_merge = pd.merge(
            df_sym[['date', 'stock_return']],
            df_market[['date', 'market_return']],
            on='date'
        )

        if len(df_merge) < 30:
            continue

        up = df_merge[df_merge['market_return'] > 0]
        down = df_merge[df_merge['market_return'] < 0]

        if len(up) < 10 or len(down) < 10:
            continue

        beta_pos = np.cov(up['stock_return'], up['market_return'])[0, 1] / np.var(up['market_return'])
        beta_neg = np.cov(down['stock_return'], down['market_return'])[0, 1] / np.var(down['market_return'])

        results.append({
            'symbol': symbol,
            'beta_pos': beta_pos,
            'beta_neg': beta_neg,
            'beta_diff': beta_pos - beta_neg
        })

    return pd.DataFrame(results)


def main():
    DATABASE_URL = "postgresql://postgres:LMilshujDuGlABeVjVvBvdhGHYZkrhBr@trolley.proxy.rlwy.net:32659/railway"
    engine = create_engine(DATABASE_URL)

    df_stocks, df_market = load_data(engine)

    # ✅ Debug info
    print(f"🧾 Found {df_stocks['symbol'].nunique()} unique symbols")

    counts = df_stocks.groupby('symbol').size()
    print("📊 Top 10 symbols by number of trading days:")
    print(counts.sort_values(ascending=False).head(10))

    df_results = compute_split_betas(df_stocks, df_market)

    if df_results.empty:
        print("⚠️ No beta results were calculated. Not enough data per stock.")
    else:
        df_results = df_results.sort_values(by='beta_diff', ascending=False).reset_index(drop=True)
        print("✅ Top asymmetric beta stocks:")
        print(df_results.head(10))

        df_results.to_csv("beta_split_results.csv", index=False)
        print("💾 Results saved to 'beta_split_results.csv'")


if __name__ == "__main__":
    main()
