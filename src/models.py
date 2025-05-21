from sqlalchemy import Column, Integer, String, Float, Date, BigInteger
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

class StockPrice(Base):
    __tablename__ = 'stock_prices'

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger, nullable=True)

    def __init__(self, **kwargs):
        for key in ['open', 'high', 'low', 'close']:
            if key in kwargs and kwargs[key] is not None:
                kwargs[key] = round(float(kwargs[key]), 2)
        super().__init__(**kwargs)