# models.py
# Defines the SQLAlchemy ORM models for the database tables.

from sqlalchemy import Column, Integer, String, Float, Date, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).resolve().parents[2] / "Algo_env" / ".env"
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
Base = declarative_base()

class DailyStockData(Base):
    __tablename__ = "daily_stock_data"

    symbol = Column(String, primary_key=True)
    date = Column(Date, primary_key=True, index=True)

    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)

    return_daily = Column(Float, nullable=True)
    sp_return = Column(Float, nullable=True)

    beta_positive = Column(Float, nullable=True)
    beta_negative = Column(Float, nullable=True)

    strength_label = Column(Float, nullable=True)  


    def __init__(self, **kwargs):
        """
        Initializes DailyStockData with optional rounding of OHLC fields.
        Rounds 'open', 'high', 'low', 'close' to 2 decimals if provided.
        """
        for key in ['open', 'high', 'low', 'close', "return_daily", "sp_return"]:
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

    score = Column(Float, nullable=True) 
    beta_positive = Column(Float, nullable=True)
    beta_negative = Column(Float, nullable=True)
    dataset_split = Column(String, nullable=True)  #"train" or "test"

class BetaCalculation(Base):
    __tablename__ = 'beta_calculation'

    symbol = Column(String, primary_key=True, index=True)
    date = Column(Date, primary_key=True, index=True)

    beta_up_30 = Column(Float, nullable=True)
    beta_down_30 = Column(Float, nullable=True)
    beta_up_60 = Column(Float, nullable=True)
    beta_down_60 = Column(Float, nullable=True)
    beta_up_90 = Column(Float, nullable=True)
    beta_down_90 = Column(Float, nullable=True)
    beta_up_180 = Column(Float, nullable=True)
    beta_down_180 = Column(Float, nullable=True)
    beta_up_360 = Column(Float, nullable=True)
    beta_down_360 = Column(Float, nullable=True)