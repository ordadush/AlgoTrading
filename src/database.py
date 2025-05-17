from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
from models import StockPrice
from dotenv import load_dotenv
import os

'''
File for database connection.
This file connects to the database using SQLAlchemy and creates a session.
'''

load_dotenv(dotenv_path='../Algo_env/.env')

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

print(os.getenv("DATABASE_URL"))

SessionLocal = sessionmaker(bind=engine)

def preview_data():
    session = SessionLocal()
    results = session.query(StockPrice).limit(5).all()
    for row in results:
        print(row.symbol, row.date, row.close)

def test_connection():
    try:
        # נסה להתחבר ולבצע פעולה פשוטה כמו יצירת טבלה
        Base.metadata.create_all(bind=engine)  # אם הטבלאות לא קיימות, הן ייווצרו
        print("all good!")
    except Exception as e:
        print(f"something bad happend: {e}")

if __name__ == "__main__":
    test_connection()
    preview_data()