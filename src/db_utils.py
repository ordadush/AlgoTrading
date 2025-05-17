from database import SessionLocal
from models import StockPrice
import pandas as pd

def save_dataframe_to_db(symbol, df):
    session = SessionLocal()
    
    df = df.copy()
    
    # הדפסת העמודות לצורך דיבאג
    print(f"DataFrame columns in save_dataframe_to_db: {df.columns.tolist()}")
    
    # בדיקה מהו שם עמודת התאריך
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
    
    # המרת עמודת תאריך לפורמט datetime (אם היא לא כבר בפורמט זה)
    df[date_column] = pd.to_datetime(df[date_column])
    
    # בדיקת ריקון
    if df.empty:
        print(f"❌ DataFrame for {symbol} is empty.")
        session.close()
        return
    
    # הדפסת דוגמה של נתונים לצורך דיבאג
    print(f"Sample data for {symbol} in save_dataframe_to_db:")
    print(df.head(1).to_string())
    
    # בדיקה אם כל העמודות הדרושות קיימות
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
            
            # הוספת Volume אם קיים
            if 'Volume' in df.columns:
                stock_data['volume'] = int(row["Volume"]) if not pd.isna(row["Volume"]) else None
            
            stock = StockPrice(**stock_data)
            session.add(stock)
            records_added += 1
            
            # Commit בקבוצות של 100 רשומות לחסוך בזיכרון
            if records_added % 100 == 0:
                session.commit()
                print(f"Committed {records_added} records so far for {symbol}")
                
        except Exception as e:
            print(f"⚠️ Skipping row {idx} due to error: {e}")
            continue
    
    try:
        # Commit סופי אם נשארו רשומות
        if records_added % 100 != 0:
            session.commit()
        print(f"✅ {symbol} saved successfully. Total records: {records_added}")
    except Exception as e:
        session.rollback()
        print(f"❌ Error saving {symbol} to DB: {e}")
    finally:
        session.close()