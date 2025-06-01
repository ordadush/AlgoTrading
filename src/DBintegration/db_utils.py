# db_utils.py
# Contains utility functions for saving stock data to the database and previewing or clearing data.
import pandas as pd
from DBintegration.database import SessionLocal
from DBintegration.models import StockPrice
from sqlalchemy.orm import Session
from DBintegration.database import engine
from DBintegration.models import Base

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

def delete_all_stock_data():
    """
    Deletes all rows from the stock_prices table.
    """
    with Session(engine) as session:
        session.query(StockPrice).delete()
        session.commit()
        print("✅ All stock price data deleted.")

    """
    Prints the first 5 records from the stock_prices table.
    """
    session = SessionLocal()
    results = session.query(StockPrice).limit(5).all()
    for row in results:
        print(row.symbol, row.date, row.close)

    """
    Persists macd, signal, hist columns from a DataFrame into stock_factors.
    Assumes df has columns ['Date','macd','signal','hist'] after add_macd().
    """
    session = SessionLocal()
    records_added = 0

    for _, row in df.iterrows():
        exists = session.query(StockFactor).filter_by(
            symbol=symbol,
            date=row['Date'].date()
        ).first()
        if exists:
            continue

        factor = StockFactor(
            symbol=symbol,
            date=row['Date'].date(),
            macd=row['macd'],
            signal=row['signal'],
            hist=row['hist']
        )
        session.add(factor)
        records_added += 1
        if records_added % 100 == 0:
            session.commit()

    if records_added % 100 != 0:
        session.commit()
    print(f"✅ MACD saved for {symbol}: {records_added} rows")
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

            #remove comments after initial download
            # exists = session.query(StockPrice).filter_by
            #     symbol=symbol,
            #     date=row[date_column].date()
            # ).first()

            # if exists:
            #     continue 
            
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
        
if __name__ == "__main__":
    delete_all_stock_data()
