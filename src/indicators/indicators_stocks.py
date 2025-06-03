# generate_stock_indicators.py
# -------------------------------------------------------------
# This script calculates technical indicators for each stock in
# the 'daily_stock_data' table based on historical OHLCV data.
#
# Indicators calculated:
# - EMA (20, 50, 200)
# - MACD and MACD signal
# - RSI (14)
# - ATR (Average True Range)
# - OBV (On-Balance Volume)
# - Daily and 14-day returns
#
# The results are saved back into the same table for each stock
# and date combination using SQLAlchemy ORM.
# -------------------------------------------------------------

import pandas as pd
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
from ta.volume import OnBalanceVolumeIndicator

from DBintegration.database import SessionLocal
from DBintegration.models import DailyStockData

# -------------------------------------------------------------
# compute_indicators(df: pd.DataFrame) -> pd.DataFrame
# -------------------------------------------------------------
# Input:  A DataFrame of OHLCV data for a specific stock, indexed by date.
# Output: The same DataFrame with additional technical indicator columns.
# Purpose: Calculate common technical indicators for trend, momentum,
#          and volatility, and attach them to the stock's historical data.
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.sort_index()

    df["ema_20"]  = EMAIndicator(df["close"], window=20).ema_indicator()
    df["ema_50"]  = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema_200"] = EMAIndicator(df["close"], window=200).ema_indicator()

    macd_obj = MACD(df["close"])
    df["macd"]        = macd_obj.macd()
    df["macd_signal"] = macd_obj.macd_signal()

    df["rsi_14"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr"]    = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
    df["obv"]    = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()

    df["daily_return"] = df["close"].pct_change()
    df["rolling_return_14"] = df["close"].pct_change(periods=14)

    return df

# -------------------------------------------------------------
# load_all_symbols() -> list
# -------------------------------------------------------------
# Input:  None
# Output: A list of unique stock symbols from the database.
# Purpose: Retrieve all distinct stock symbols from the 'daily_stock_data' table.
def load_all_symbols() -> list:
    session = SessionLocal()
    try:
        return list(session.query(DailyStockData.symbol).distinct())
    finally:
        session.close()

# -------------------------------------------------------------
# load_stock_data(symbol: str) -> pd.DataFrame
# -------------------------------------------------------------
# Input:  A stock symbol (str)
# Output: DataFrame with OHLCV data for that symbol
# Purpose: Fetch historical data for a specific stock from the database.
def load_stock_data(symbol: str) -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = session.query(DailyStockData).filter(DailyStockData.symbol == symbol).all()
        df = pd.DataFrame([{
            "date":   r.date,
            "open":   r.open,
            "high":   r.high,
            "low":    r.low,
            "close":  r.close,
            "volume": r.volume
        } for r in rows])
        df.set_index("date", inplace=True)
        return df
    finally:
        session.close()

# -------------------------------------------------------------
# _cast(x, is_int=False) -> Optional[int|float]
# -------------------------------------------------------------
# Input:  A value x (float or NaN), and a boolean flag if it should be int
# Output: Cleaned and type-cast value (float or int), or None if NaN
# Purpose: Ensure safe values for inserting into the database without NaN issues.
def _cast(x, is_int=False):
    if pd.isna(x):
        return None
    return int(x) if is_int else float(x)

# -------------------------------------------------------------
# update_stock_indicators(symbol: str, df: pd.DataFrame)
# -------------------------------------------------------------
# Input:  Symbol (str), and the DataFrame with indicators for that stock
# Output: None
# Purpose: Merge updated indicators into the 'daily_stock_data' table
#          by symbol and date.
def update_stock_indicators(symbol: str, df: pd.DataFrame):
    session = SessionLocal()
    try:
        for date, r in df.iterrows():
            entry = DailyStockData(
                symbol = symbol,
                date   = date.date(),
                open   = _cast(r["open"]),
                high   = _cast(r["high"]),
                low    = _cast(r["low"]),
                close  = _cast(r["close"]),
                volume = _cast(r["volume"], is_int=True),

                ema_20       = _cast(r["ema_20"]),
                ema_50       = _cast(r["ema_50"]),
                ema_200      = _cast(r["ema_200"]),
                rsi_14       = _cast(r["rsi_14"]),
                macd         = _cast(r["macd"]),
                macd_signal  = _cast(r["macd_signal"]),
                atr          = _cast(r["atr"]),
                obv          = _cast(r["obv"], is_int=True),
                daily_return = _cast(r["daily_return"]),
                rolling_return_14 = _cast(r["rolling_return_14"])
            )
            session.merge(entry)
        session.commit()
        print(f"{symbol} indicators updated.")
    except Exception as e:
        session.rollback()
        print(f"Error updating {symbol}: {e}")
    finally:
        session.close()

# -------------------------------------------------------------
# Execution
# -------------------------------------------------------------
# Iterates through all distinct stock symbols, calculates technical
# indicators for each, and stores them back into the database.
if __name__ == "__main__":
    symbols = load_all_symbols()
    for sym in symbols:
        print(f"Processing {sym}...")
        df = load_stock_data(sym)
        if df.empty:
            print(f"No data for {sym}")
            continue
        enriched = compute_indicators(df)
        update_stock_indicators(sym, enriched)
