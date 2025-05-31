# This script fetches full historical daily data for the S&P 500 index using the SPY ETF from Alpha Vantage.
# It filters the data by date range (2014–2024), cleans it, and stores it into a PostgreSQL database.

import os
from alpha_vantage.timeseries import TimeSeries
from DBintegration.database import SessionLocal
from DBintegration.models import SP500Index
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY in environment variables")

def fetch_and_store_sp500_data():
    ts = TimeSeries(key=API_KEY, output_format='pandas')

    print("Fetching full daily data for S&P 500 via SPY ETF")
    try:
        # Use SPY ETF as proxy for S&P 500 index
        data, meta_data = ts.get_daily(symbol='SPY', outputsize='full')
    except Exception as e:
        print(f"Error fetching data from Alpha Vantage: {e}")
        return

    if data.empty:
        print("No data fetched from Alpha Vantage.")
        return

    # Ensure the index is sorted before slicing by date
    data = data.sort_index()
    data = data.loc["2014-01-01":"2024-12-31"]

    # Rename columns to match internal schema
    data = data.rename(columns={
        '1. open': 'Open',
        '2. high': 'High',
        '3. low': 'Low',
        '4. close': 'Close',
        '5. volume': 'Volume'
    })

    session = SessionLocal()
    try:
        for date, row in data.iterrows():
            volume = row['Volume']
            if pd.isna(volume):
                volume = None
            else:
                volume = int(volume)

            entry = SP500Index(
                date=date.date(),
                open=round(float(row['Open']), 2),
                high=round(float(row['High']), 2),
                low=round(float(row['Low']), 2),
                close=round(float(row['Close']), 2),
                volume=int(volume) if volume is not None else None
            )
            session.merge(entry)
        session.commit()
        print("SPY (S&P 500) data saved to database.")
    except Exception as e:
        session.rollback()
        print(f"Error while inserting data: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    fetch_and_store_sp500_data()
