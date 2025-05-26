from models import Base
from database import engine
# IF you added a new table, this line updates the table in the cloud.
Base.metadata.create_all(bind=engine)
import pandas as pd

def add_macd(df, fast=12, slow=26, signal=9):
    """
    Expects df with a 'Close' column.
    Returns the same DataFrame with macd, signal, hist columns added.
    """
    ema_fast   = df['Close'].ewm(span=fast, adjust=False).mean()
    ema_slow   = df['Close'].ewm(span=slow, adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist       = macd_line - signal_line

    df['macd']   = macd_line.round(6)
    df['signal'] = signal_line.round(6)
    df['hist']   = hist.round(6)

    return df
def save_macd_to_db(symbol, df):
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

