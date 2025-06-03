# db_utils.py
# Contains utility functions for saving stock data to the database and previewing or clearing data.

import sys
import os
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# הוספת תיקיית src ל-Python path, כדי שיוכל לייבא מ-DBintegration
sys.path.append(str(Path(__file__).resolve().parents[1]))  # זו תיקיית src

# הגדרת נתיב מדויק ל-.env בתיקיית Algo_env
env_path = Path(__file__).resolve().parents[2] / "Algo_env" / ".env"
load_dotenv(dotenv_path=env_path)

import os

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
if not API_KEY:
    raise ValueError("Missing ALPHAVANTAGE_API_KEY in environment variables")


from DBintegration.database import SessionLocal
from DBintegration.database import engine
from DBintegration.models import StockPrice
from DBintegration.models import SP500Index
from DBintegration.models import SectorData
from DBintegration.models import DailyStockData
from DBintegration.models import Base
from sqlalchemy.orm import Session
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import delete
from alpha_vantage.timeseries import TimeSeries
import pandas as pd
import yfinance as yf


def update_data(model, df):
    """
    Updates the database with only the changes in the DataFrame.
    - Existing rows are updated if any field has changed.
    - New rows are inserted.
    - Rows not in df but present in DB are left untouched.
    """
    session = SessionLocal()
    Base.metadata.create_all(bind=engine)  # Ensure tables are created
    df = df.copy()

    # Detect date column
    date_column = next((col for col in df.columns if col.lower() in ['date', 'datetime']), None)
    if date_column is None:
        print("❌ No date column found in DataFrame.")
        session.close()
        return

    df[date_column] = pd.to_datetime(df[date_column]).dt.date

    if df.empty:
        print("❌ DataFrame is empty.")
        session.close()
        return

    primary_keys = ['symbol', 'date']  # You can generalize if your model differs

    updates = 0
    inserts = 0

    for _, row in df.iterrows():
        try:
            query = {key: row[key] for key in primary_keys if key in row}
            existing = session.query(model).filter_by(**query).first()

            row_data = {col.name: row[col.name] for col in model.__table__.columns if col.name in row}

            if existing:
                # Check for differences
                changed = False
                for field, value in row_data.items():
                    if getattr(existing, field) != value:
                        setattr(existing, field, value)
                        changed = True
                if changed:
                    updates += 1
            else:
                new_obj = model(**row_data)
                session.add(new_obj)
                inserts += 1

            if (updates + inserts) % 100 == 0:
                session.commit()

        except Exception as e:
            print(f"⚠️ Error processing row {row.to_dict()}: {e}")
            session.rollback()
            continue

    try:
        session.commit()
        print(f"✅ Update complete. {updates} updated, {inserts} inserted.")
    except Exception as e:
        session.rollback()
        print(f"❌ Final commit failed: {e}")
    finally:
        session.close()
        
def model_to_dataframe(model_class): ###input: model, output: 
    """
    Given a SQLAlchemy model class, return a Pandas DataFrame of all its rows.
    
    Args:
        model_class: The SQLAlchemy model class (e.g., StockPrice, User, etc.)
    
    Returns:
        pd.DataFrame: DataFrame with the table's data.
    """
    session: Session = SessionLocal()
    try:
        results = session.query(model_class).all()
        if not results:
            return pd.DataFrame()  # empty table

        data = [row.__dict__.copy() for row in results]
        for row in data:
            row.pop("_sa_instance_state", None)

        return pd.DataFrame(data)
    finally:
        session.close()

def delete_all_rows(model: DeclarativeMeta):
    """
    Deletes all rows from the table associated with the given SQLAlchemy model class.

    Parameters:
        model (DeclarativeMeta): A SQLAlchemy model class.
    """
    session = SessionLocal()
    try:
        session.execute(delete(model))
        session.commit()
        print(f"All rows deleted from {model.__tablename__}")
    except Exception as e:
        session.rollback()
        print(f"Error deleting rows from {model.__tablename__}: {e}")
    finally:
        session.close()

def save_dataframe_to_db(symbol, df):
    """
    Saves a stock's historical data from a DataFrame to the database.
    Filters out existing entries and commits in batches of 100.
    """
    session = SessionLocal()
    Base.metadata.create_all(bind=engine) #sync model with DB

    df = df.copy()
    symbol = symbol.upper()
    
    date_column = None
    for col in df.columns:
        if col in ['Date', 'Datetime', 'date', 'datetime']:
            date_column = col
            break
    
    if date_column is None:
        print(f"❌ No date column found in DataFrame for {symbol}")
        print(f"Available columns: {df.columns.tolist()}")
        session.close()
        return
    
    df[date_column] = pd.to_datetime(df[date_column])
    
    if df.empty:
        print(f"❌ DataFrame for {symbol} is empty.")
        session.close()
        return
    
    required_columns = ["Open", "High", "Low", "Close"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        print(f"❌ Missing columns for {symbol}: {missing_columns}")
        session.close()
        return
    
    records_added = 0
    for idx, row in df.iterrows():
        try:
   
            stock_data = {
                'symbol': symbol,
                'date': row[date_column].date(),
                'open': float(row["Open"]),
                'high': float(row["High"]),
                'low': float(row["Low"]),
                'close': float(row["Close"]),
            }

            # Volume is optional
            if 'Volume' in df.columns:
                stock_data['volume'] = int(row["Volume"]) if not pd.isna(row["Volume"]) else None
            
            stock = StockPrice(**stock_data)
            session.add(stock)
            records_added += 1
            
            if records_added % 100 == 0:
                session.commit()
                print(f"Committed {records_added} records so far for {symbol}")
                
        except Exception as e:
            print(f"⚠️ Skipping row {idx} due to error: {e}")
            continue
    
    try:
        if records_added % 100 != 0:
            session.commit()
        print(f"✅ {symbol} saved successfully. Total records: {records_added}")
    except Exception as e:
        session.rollback()
        print(f"❌ Error saving {symbol} to DB: {e}")
    finally:
        session.close()

def fetch_and_store_data(symbol: str, model: str):
    """
    Fetches full daily historical data for a given symbol (stock or index or sector),
    processes the data, and stores it into the appropriate database table
    based on the given model.

    Parameters:
        symbol (str): The stock/index symbol to fetch .
        model (str): 'index' to store in SP500Index, 'stock' to store in DailyStockData, 'sector
                    'sector' to store in SectorData."""
    
    if model not in ['index', 'stock', 'sector']:
        raise ValueError("Model must be either 'index', 'stock', or 'sector'.")
    
    ts = TimeSeries(key=API_KEY, output_format='pandas')

    print(f"Fetching full daily data for {symbol}")
    try:
        data, meta_data = ts.get_daily(symbol=symbol, outputsize='full')
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return

    if data.empty:
        print(f"No data fetched for {symbol}.")
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
            volume = int(volume) if not pd.isna(volume) else None

            if model == 'index':
                entry = SP500Index(
                    date=date.date(),
                    open=round(float(row['Open']), 2),
                    high=round(float(row['High']), 2),
                    low=round(float(row['Low']), 2),
                    close=round(float(row['Close']), 2),
                    volume=volume
                )
            elif model == 'stock':
                entry = DailyStockData(
                    symbol=symbol,
                    date=date.date(),
                    open=round(float(row['Open']), 2),
                    high=round(float(row['High']), 2),
                    low=round(float(row['Low']), 2),
                    close=round(float(row['Close']), 2),
                    volume=volume
                )
            elif model == 'sector':
                entry = SectorData(
                    symbol=symbol,
                    date=date.date(),
                    open=round(float(row['Open']), 2),
                    high=round(float(row['High']), 2),
                    low=round(float(row['Low']), 2),
                    close=round(float(row['Close']), 2),
                    volume=volume
                )
            else:
                raise ValueError("Model must be either 'index' or 'stock'.")

            session.merge(entry)
        session.commit()
        print(f"{symbol} data saved to database in '{model}' model.")
    except Exception as e:
        session.rollback()
        print(f"Error inserting data for {symbol}: {e}")
    finally:
        session.close()

def fetch_and_store_sector_etfs(etf_list=None):
    """
    Fetches and stores historical daily OHLCV data for a list of sector ETFs
    from Alpha Vantage into the 'sector_data' table in the database.
    """
    if etf_list is None:
        etf_list = ["XLF", "XLK", "XLE", "XLI", "XLY", "XLV", "XLP", "XLU", "XLC", "XLRE", "XLB"]

    ts = TimeSeries(key=API_KEY, output_format='pandas')

    for symbol in etf_list:
        try:
            print(f"📥 Fetching data for {symbol}...")
            data, meta = ts.get_daily(symbol=symbol, outputsize='full')

            if data.empty:
                print(f"⚠️ No data returned for {symbol}")
                continue

            data = data.sort_index()
            data = data.loc["2013-01-01":"2024-12-31"]

            data = data.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })

            data = data.reset_index()
            data = data.rename(columns={"date": "date"})  # for clarity
            data["symbol"] = symbol
            data["date"] = pd.to_datetime(data["date"]).dt.date

            df = data[["date", "open", "high", "low", "close", "volume", "symbol"]].copy()

            update_data(SectorData, df)
            print(f"✅ {symbol} saved to DB.")

        except Exception as e:
            print(f"❌ Error processing {symbol}: {e}")

if __name__ == "__main__":
   pass