from database import SessionLocal
from models import StockPrice

def preview_market_data():
    session = SessionLocal()
    try:
        # שליפת חמשת השורות האחרונות של השוק
        results = session.query(StockPrice)\
            .filter_by(symbol="^GSPC")\
            .order_by(StockPrice.date.desc())\
            .limit(5).all()

        print("📊 חמשת הרשומות האחרונות של ^GSPC:")
        for row in results:
            print(row.symbol, row.date, row.close)

    except Exception as e:
        print(f"❌ Error while querying market data: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    preview_market_data()
