# db_utils.py
# Contains utility functions for saving stock data to the database and previewing or clearing data.

from database import SessionLocal
from models import StockPrice
import pandas as pd
from sqlalchemy.orm import Session
from database import engine
from models import Base

def save_dataframe_to_db(symbol, df):
    """
    Saves a stock's historical data from a DataFrame to the database.
    Filters out existing entries and commits in batches of 100.
    """
    session = SessionLocal()
    Base.metadata.create_all(bind=engine)

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
            exists = session.query(StockPrice).filter_by(
                symbol=symbol,
                date=row[date_column].date()
            ).first()

            if exists:
                continue 
            
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

def delete_all_stock_data():
    """
    Deletes all rows from the stock_prices table.
    """
    with Session(engine) as session:
        session.query(StockPrice).delete()
        session.commit()
        print("✅ All stock price data deleted.")

def preview_data():
    """
    Prints the first 5 records from the stock_prices table.
    """
    session = SessionLocal()
    results = session.query(StockPrice).limit(5).all()
    for row in results:
        print(row.symbol, row.date, row.close)

if __name__ == "__main__":
    delete_all_stock_data()
