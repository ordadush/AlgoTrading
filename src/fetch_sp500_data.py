import os
from alpha_vantage.timeseries import TimeSeries
from DBintegration.database import SessionLocal
from DBintegration.models import SP500Index
from dotenv import load_dotenv
import pandas as pd

# Load environment variables from a .env file
load_dotenv()

# Retrieve Alpha Vantage API key from environment variables, raise error if missing
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY in environment variables")

def fetch_and_store_sp500_data():
    """
    Fetches full daily historical data for the S&P 500 proxy ETF 'SPY' from Alpha Vantage,
    processes the data, and stores it into the local database using SQLAlchemy ORM.
    """
    # Initialize Alpha Vantage TimeSeries client with pandas output format
    ts = TimeSeries(key=API_KEY, output_format='pandas')

    print("Fetching full daily data for S&P 500 via SPY ETF")
    try:
        # Get full daily data for 'SPY' symbol; may raise exceptions on network/API errors
        data, meta_data = ts.get_daily(symbol='SPY', outputsize='full')
    except Exception as e:
        print(f"Error fetching data from Alpha Vantage: {e}")
        return

    if data.empty:
        print("No data fetched from Alpha Vantage.")
        return

    data = data.sort_index()
    data = data.loc["2013-01-01":"2024-12-31"]

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

            # Create or update an SP500Index ORM object for each date's data
            entry = SP500Index(
                date=date.date(), 
                open=round(float(row['Open']), 2),
                high=round(float(row['High']), 2),
                low=round(float(row['Low']), 2),
                close=round(float(row['Close']), 2),
                volume=volume
            )
            # Use merge to insert new or update existing record based on primary key (date)
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
