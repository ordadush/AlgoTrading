from sqlalchemy.orm import Session
from database import engine
from models import StockPrice  # ודא שאתה מייבא נכון את המודל

def delete_all_stock_data():
    with Session(engine) as session:
        session.query(StockPrice).delete()
        session.commit()
        print("✅ All stock price data deleted.")
