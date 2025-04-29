import yfinance as yf
import pandas as pd
import os
from datetime import datetime, timedelta

output_dir = "../data"
os.makedirs(output_dir, exist_ok=True)

end_date = datetime(2024, 12, 31)
start_date = end_date - timedelta(days=365 * 10)

filtered_file = "../filtered_nasdaq.csv"

if os.path.exists(filtered_file):
    print("Loading filtered stock list...")
    filtered_stocks = pd.read_csv(filtered_file)
else:
    print("Filtering stock list from nasdaqlisted.csv...")
    nasdaq_df = pd.read_csv("../nasdaqlisted.csv", sep='|')

    nasdaq_df['Symbol'] = nasdaq_df['Symbol'].astype(str)
    nasdaq_df['Security Name'] = nasdaq_df['Security Name'].astype(str)

    filtered_stocks = nasdaq_df[
        (nasdaq_df['ETF'] != 'Y') &
        (nasdaq_df['Test Issue'] != 'Y') &
        (~nasdaq_df['Symbol'].str.contains(r'[\.\-\/]', na=False)) &
        (~nasdaq_df['Security Name'].str.contains("Warrant|Right|Unit", case=False, na=False))
    ]

    filtered_stocks.to_csv(filtered_file, index=False)
    print(f"Saved filtered list to {filtered_file}")

def has_sufficient_history(ticker, min_years=5):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max")

        if hist.empty:
            return False

        first_date = hist.index[0].to_pydatetime().replace(tzinfo=None) 
        age_years = (datetime.today() - first_date).days / 365
        return age_years >= min_years

    except Exception as e:
        print(f"Error checking {ticker}: {e}")
        return False

for symbol in filtered_stocks["Symbol"][:50]: 
    if not has_sufficient_history(symbol, min_years=5):
        print(f"Skipping {symbol} - not old enough")
        continue

    try:
        print(f"Downloading {symbol}...")
        data = yf.download(
            symbol,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            progress=False
        )

        if not data.empty:
            data.to_csv(f"{output_dir}/{symbol}.csv")
            print(f"{symbol} saved successfully.")
        else:
            print(f"No data for {symbol}.")

    except Exception as e:
        print(f"Failed to download {symbol}: {e}")
