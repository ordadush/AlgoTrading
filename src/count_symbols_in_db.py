from database import engine
import pandas as pd

query_list = """
SELECT DISTINCT symbol 
FROM stock_prices
ORDER BY symbol;
"""

df_symbols = pd.read_sql(query_list, con=engine)
print(df_symbols)
