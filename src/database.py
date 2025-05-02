from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
from dotenv import load_dotenv
import os


load_dotenv(dotenv_path='../Algo_env/.env')

DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(bind=engine)

def test_connection():
    try:
        # נסה להתחבר ולבצע פעולה פשוטה כמו יצירת טבלה
        Base.metadata.create_all(bind=engine)  # אם הטבלאות לא קיימות, הן ייווצרו
        print("all good!")
    except Exception as e:
        print(f"something bad happend: {e}")

if __name__ == "__main__":
    test_connection()