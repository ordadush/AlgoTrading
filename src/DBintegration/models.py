# models.py
# Defines the SQLAlchemy ORM models for the database tables.

from sqlalchemy import Column, Integer, String, Float, Date, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
Base = declarative_base()

class DailyStockData(Base):
    __tablename__ = "daily_stock_data"
    date = Column(Date, primary_key=True)
    symbol = Column(String, primary_key=True)

    # OHLCV
    open   = Column(Float)
    high   = Column(Float)
    low    = Column(Float)
    close  = Column(Float)
    volume = Column(Integer)

    # Indicators
    ema_20  = Column(Float)
    ema_50  = Column(Float)
    ema_200 = Column(Float)
    rsi_14  = Column(Float)
    macd         = Column(Float)
    macd_signal  = Column(Float)
    obv          = Column(Float)
    atr          = Column(Float)

    # Returns
    daily_return        = Column(Float)
    rolling_return_14   = Column(Float)
    market_return_14    = Column(Float)

    # Strength / Regression
    relative_strength_14 = Column(Float)
    beta_up_20           = Column(Float)
    beta_down_20         = Column(Float)
    alpha_20             = Column(Float)

    # Filtering flags
    is_candidate_long  = Column(Boolean)
    is_candidate_short = Column(Boolean)

    def __init__(self, **kwargs):
        """
        Initializes SectorData with optional rounding of OHLC fields.
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

class SectorData(Base):
    __tablename__ = 'sector_data'

    symbol = Column(String, primary_key=True)
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
    obv = Column(Float, nullable=True)
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
