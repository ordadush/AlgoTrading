# src/debug_preview.py

from database import SessionLocal
from models import StockPrice

session = SessionLocal()
symbols = session.query(StockPrice.symbol).distinct().all()

print("🗂️ סימבולים שיש בטבלה:")
for sym in symbols:
    print(sym[0])

session.close()
