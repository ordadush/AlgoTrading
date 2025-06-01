import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))  # מוסיף את src לנתיב הייבוא

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from DBintegration import models
from dotenv import load_dotenv

# ✅ טוען את הקובץ .env מתיקיית AlgoTrading הראשית
env_path = Path(__file__).resolve().parents[3] / ".env"
print(f"🔍 טוען את: {env_path}")  # אפשר למחוק אחרי הבדיקה
load_dotenv(dotenv_path=env_path)

DATABASE_URL = os.getenv("DATABASE_URL")
print("Loaded DATABASE_URL:", DATABASE_URL)  # גם את זה אפשר למחוק אחר כך

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def test_connection():
    try:
        models.Base.metadata.create_all(bind=engine)
        print("✅ all good! DB connection and tables synced.")
    except Exception as e:
        print(f"❌ something bad happened: {e}")

if __name__ == "__main__":
    test_connection()
