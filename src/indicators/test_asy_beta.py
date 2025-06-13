import pandas as pd
from src.indicators.asymmetric_beta import compute_asymmetric_beta

# Dummy data
dates = pd.date_range("2022-01-01", periods=30, freq="D")
df_stock = pd.DataFrame({
    "date": dates,
    "close": [100 + i + (i % 3 - 1) * 0.5 for i in range(30)]
})
df_market = pd.DataFrame({
    "date": dates,
    "close": [3000 + i*2 + (i % 2 - 0.5) * 0.8 for i in range(30)]
})

df_stock = df_stock.rename(columns={"close": "close_stock"})
df_market = df_market.rename(columns={"close": "close_market"})

result = compute_asymmetric_beta(df_stock, df_market, window=10)
print(result.head())
