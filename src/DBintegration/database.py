# database.py
# Sets up the SQLAlchemy engine and session for database operations.
# Also provides a function to test the connection.

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from DBintegration.models import Base
from DBintegration.models import StockPrice
from dotenv import load_dotenv
from pathlib import Path
import os

env_path = Path(__file__).resolve().parent.parent.parent / 'Algo_env' / '.env'
load_dotenv(dotenv_path=env_path)
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)


print(os.getenv("DATABASE_URL"))

SessionLocal = sessionmaker(bind=engine)

def test_connection():
    """
    Tries to create all tables and confirms database connection.
    """
    try:
        Base.metadata.create_all(bind=engine) 
        print("all good!")
    except Exception as e:
        print(f"something bad happend: {e}")

if __name__ == "__main__":
    test_connection()
