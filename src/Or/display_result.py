import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))  # מוסיף את src
import pandas as pd
import matplotlib.pyplot as plt

from DBintegration.database import SessionLocal
from DBintegration.models import SP500Index

def display_market_trend_colored():
    session = SessionLocal()
    try:
        rows = (
            session.query(SP500Index.date,
                          SP500Index.close,
                          SP500Index.market_trend)
            .order_by(SP500Index.date)
            .all()
        )
    finally:
        session.close()

    df = pd.DataFrame(rows, columns=["date", "close", "trend"])
    df["date"] = pd.to_datetime(df["date"])
    df = df[(df["date"] >= "2014-01-01") & (df["date"] <= "2024-12-31")]

    # צבעים לפי תנאי trend
    def trend_color(score):
        if score > 0.3:
            return "green"
        elif score < -0.25:
            return "red"
        else:
            return "yellow"

    colors = df["trend"].apply(trend_color)

    # ציור
    plt.figure(figsize=(14, 6))
    plt.scatter(df["date"], df["close"],
                c=colors, s=6, linewidths=0)

    plt.title("S&P 500 – Close coloured by Market Trend (2014-2024)")
    plt.xlabel("Date")
    plt.ylabel("Close Price")
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    display_market_trend_colored()
