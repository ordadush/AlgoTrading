from database import engine
import pandas as pd

# רשימת הסקטורים
sector_symbols = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']

# בנה שאילתה דינמית לפי הסקטורים
query_sectors = f"""
SELECT symbol, MIN(date) AS start_date, MAX(date) AS end_date, COUNT(*) AS total_rows
FROM stock_prices
WHERE symbol IN ({','.join(f"'{s}'" for s in sector_symbols)})
GROUP BY symbol
ORDER BY symbol;
"""

df_sectors = pd.read_sql(query_sectors, con=engine)

print("📊 טווח תאריכים לכל סקטור:")
print(df_sectors)
