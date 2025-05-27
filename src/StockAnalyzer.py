# download_and_store.py
# Filters NASDAQ stocks, validates based on history and volume,
# and downloads historical price data to store in the database.

import yfinance as yf
import pandas as pd
from db_utils import save_dataframe_to_db
from datetime import datetime, timedelta
import os
import time

# Set date range (last 10 years)
end_date = end_date = datetime(2024, 12, 31)
start_date = end_date - timedelta(days=365 * 10)

# CSV path for filtered symbols
filtered_file = "../filtered_nasdaq.csv"

def make_filltered_file():
    """
    Creates a filtered NASDAQ file excluding ETFs, warrants, units, etc.
    Loads existing file if available.
    Returns: DataFrame of filtered stocks.
    """
    if os.path.exists(filtered_file):
        print("Loading filtered stock list...")
        filtered_stocks = pd.read_csv(filtered_file, on_bad_lines='skip')
    
    else:
        print("Filtering stock list from nasdaqlisted.csv...")
        try:
            nasdaq_df = pd.read_csv("nasdaqlisted.csv", sep='|')
        except FileNotFoundError:
            print("Error: nasdaqlisted.csv file not found.")
            print("Please download it from: https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt")
            print("And save it as nasdaqlisted.csv after removing the last line")
            exit(1)

        nasdaq_df['Symbol'] = nasdaq_df['Symbol'].astype(str)
        nasdaq_df['Security Name'] = nasdaq_df['Security Name'].astype(str)

        filtered_stocks = nasdaq_df[
            (nasdaq_df['ETF'] != 'Y') &
            (nasdaq_df['Test Issue'] != 'Y') &
            (~nasdaq_df['Symbol'].str.contains(r'[.\-/]', na=False)) &
            (~nasdaq_df['Security Name'].str.contains("Warrant|Right|Unit", case=False, na=False))
        ]

        filtered_stocks.to_csv(filtered_file, index=False)
        print(f"Saved filtered list to {filtered_file}")
        
    return filtered_stocks

def has_sufficient_volume(hist, min_volume=500000):
    """
    Checks if the stock's historical data has sufficient average dollar volume.
    Returns: True if average volume is above threshold, False otherwise.
    """
    if 'Close' not in hist.columns or 'Volume' not in hist.columns:
        return False

    df = hist.copy()
    df['dollar_volume'] = df['Close'] * df['Volume'] * 1000
    avg_volume = df['dollar_volume'].mean()
    return avg_volume >= min_volume

def valid_stock(ticker, min_years=10, min_value=0.01):
    """
    Validates a stock by checking sufficient history and volume.
    Returns: True if the stock passes all filters, else False.
    """
    try:

        if pd.isna(ticker) or ticker == "":
            return False
        
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max")

        if hist.empty or not has_sufficient_volume(hist):
            return False

        first_date = hist.index[0].to_pydatetime().replace(tzinfo=None)
        age_years = (datetime.today() - first_date).days / 365
        return age_years >= min_years

    except Exception as e:
        print(f"Error checking {ticker}: {e}")
        return False

MAX_STOCKS = 1  
valid_stocks = []

filtered_stocks = make_filltered_file()

print(f"Finding stocks with sufficient history...")
for symbol in filtered_stocks["Symbol"]:
    if valid_stock(symbol):
        valid_stocks.append(symbol)
        print(f"Found valid stock: {symbol} ({len(valid_stocks)}/{MAX_STOCKS})")
    
    if len(valid_stocks) >= MAX_STOCKS:
        break

# Download data and store in DB
for i, symbol in enumerate(valid_stocks):
    try:
        print(f"[{i+1}/{len(valid_stocks)}] Downloading {symbol}...")
        data = yf.download(
            symbol,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            progress=False
        )

        if not data.empty:
            data_processed = data.copy()
            data_processed.reset_index(inplace=True)

            # Handle MultiIndex column names if present
            if isinstance(data_processed.columns, pd.MultiIndex):
                new_cols = []
                for col in data_processed.columns:
                    if isinstance(col, tuple) and len(col) > 1:
                        new_cols.append(col[0] if col[0] else 'Date')
                    else:
                        new_cols.append(col)
                data_processed.columns = new_cols
            
            save_dataframe_to_db(symbol, data_processed)
        else:
            print(f"No data for {symbol}.")

    except Exception as e:
        print(f"❌ Failed to download {symbol}: {e}")
        import traceback
        traceback.print_exc()
