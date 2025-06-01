# models.py
# Defines the SQLAlchemy ORM models for the database tables.

from sqlalchemy import Column, Integer, String, Float, Date, BigInteger
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StockPrice(Base):
    __tablename__ = 'stock_prices'

    symbol = Column(String, primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger, nullable=True)


    def __init__(self, **kwargs):
        """
        Initializes StockPrice with optional rounding of OHLC fields.
        Rounds 'open', 'high', 'low', 'close' to 2 decimals if provided.
        """
        for key in ['open', 'high', 'low', 'close']:
            if key in kwargs and kwargs[key] is not None:
                kwargs[key] = round(float(kwargs[key]), 2)
        super().__init__(**kwargs)

class SP500Index(Base):
    __tablename__ = 'sp500_index'

    date = Column(Date, primary_key=True, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger, nullable=True)

    ema_50 = Column(Float, nullable=True)
    ema_200 = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    macd = Column(Float, nullable=True)
    macd_signal = Column(Float, nullable=True)
    atr = Column(Float, nullable=True)
    obv = Column(BigInteger, nullable=True)
    market_score = Column(Integer, nullable=True)
    market_trend = Column(Float, nullable=True)


class DailyStockData(Base):
    __tablename__ = 'daily_stock_data'

    symbol = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    split = Column(String)  # train / validation / test

class SectorData(Base):
    __tablename__ = 'sector_data'

    sector = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger, nullable=True)

    ema_50 = Column(Float, nullable=True)
    ema_200 = Column(Float, nullable=True)
    rsi = Column(Float, nullable=True)
    macd = Column(Float, nullable=True)
    macd_signal = Column(Float, nullable=True)
    atr = Column(Float, nullable=True)
    obv = Column(BigInteger, nullable=True)
    market_score = Column(Integer, nullable=True)
    market_trend = Column(Float, nullable=True)


    def __init__(self, **kwargs):
        """
        Initializes SectorData with optional rounding of OHLC fields.
        Rounds 'open', 'high', 'low', 'close' to 2 decimals if provided.
        """
        for key in ['open', 'high', 'low', 'close']:
            if key in kwargs and kwargs[key] is not None:
                kwargs[key] = round(float(kwargs[key]), 2)
        super().__init__(**kwargs)