# indicators.py
# Computes technical indicators for the S&P 500 index (via SPY ETF) using historical data from the database
# and updates the database with the computed values.

import pandas as pd
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import AverageTrueRange
from DBintegration.database import SessionLocal
from DBintegration.models import SP500Index
from sqlalchemy import update

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds technical indicators (EMA, RSI, MACD, ATR, OBV) and a derived market score/trend to the DataFrame.
    """
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    df = df.sort_index()

    # Compute individual technical indicators
    df['ema_50'] = EMAIndicator(close=df['close'], window=50).ema_indicator()
    df['ema_200'] = EMAIndicator(close=df['close'], window=200).ema_indicator()
    df['rsi'] = RSIIndicator(close=df['close'], window=14).rsi()
    df['macd'] = MACD(close=df['close']).macd()
    df['macd_signal'] = MACD(close=df['close']).macd_signal()
    df['atr'] = AverageTrueRange(high=df['high'], low=df['low'], close=df['close']).average_true_range()
    df['obv'] = OnBalanceVolumeIndicator(close=df['close'], volume=df['volume']).on_balance_volume()

    df = df.loc[pd.to_datetime('2014-01-01'):pd.to_datetime('2024-12-31')]

    # Combine indicators into a simplified market score and normalized trend
    df['market_score'] = (
        ((df['ema_50'] > df['ema_200']).astype(int) * 1) +
        ((df['macd'] > df['macd_signal']).astype(int) * 1) +
        ((df['rsi'] > 50).astype(int) * 1) +
        ((df['obv'].diff() > 0).astype(int) * 1) +
        ((df['atr'].pct_change() > 0).astype(int) * 1)
    )
    df['market_trend'] = df['market_score'].apply(lambda x: round((x - 2.5) / 1.25, 2))

    return df

def load_data_from_db():
    """
    Loads raw S&P 500 (SPY) data from the database into a DataFrame.
    """
    session = SessionLocal()
    try:
        rows = session.query(SP500Index).all()
        df = pd.DataFrame([{
            'date': row.date,
            'open': row.open,
            'high': row.high,
            'low': row.low,
            'close': row.close,
            'volume': row.volume
        } for row in rows])
        df.set_index('date', inplace=True)
        return df
    finally:
        session.close()

def update_database_with_indicators(df: pd.DataFrame):
    """
    Updates or inserts computed indicators for each trading day into the database using SQLAlchemy's merge.
    """
    session = SessionLocal()
    try:
        for date, row in df.iterrows():
            date_to_use = date.date()

            # Merge ensures insert if new, or update if existing (by primary key)
            entry = SP500Index(
                date=date_to_use,
                ema_50=float(row['ema_50']),
                ema_200=float(row['ema_200']),
                rsi=float(row['rsi']),
                macd=float(row['macd']),
                macd_signal=float(row['macd_signal']),
                atr=float(row['atr']),
                obv=float(row['obv']),
                market_score=float(row['market_score']),
                market_trend=float(row['market_trend'])
            )

            session.merge(entry)

        session.commit()
    except Exception as e:
        session.rollback()
        print(f"❌ Error updating indicators: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    df = load_data_from_db()
    enriched_df = compute_indicators(df)
    update_database_with_indicators(enriched_df)
