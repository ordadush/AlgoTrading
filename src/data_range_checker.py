from database import engine
import pandas as pd

query_market = """
SELECT MIN(date) AS market_start_date, MAX(date) AS market_end_date
FROM stock_prices
WHERE symbol IN ('^GSPC', '^SPX', 'SPY', 'MARKET');
"""

df_market = pd.read_sql(query_market, con=engine)

print("📈 טווח תאריכים לנתוני שוק:")
print(df_market)
