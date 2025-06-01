# indicators_sp500.py
# -------------------
# Computes technical indicators for S&P 500 index data, scores daily market trend strength,
# and updates the database with enhanced trend detection logic including EMA crossovers.

import pandas as pd
from ta.trend     import EMAIndicator, MACD, ADXIndicator
from ta.momentum  import RSIIndicator
from ta.volatility import AverageTrueRange
from ta.volume    import OnBalanceVolumeIndicator

from DBintegration.database import SessionLocal
from DBintegration.models   import SP500Index

# -----------------------------------------------------------
# compute_indicators(df: pd.DataFrame) -> pd.DataFrame
# -----------------------------------------------------------
# Takes a DataFrame of historical SP500 index data (with OHLCV format),
# calculates technical indicators (EMA, MACD, RSI, ADX, ATR, OBV),
# assigns a daily trend score and normalized trend signal.
# Returns the enriched DataFrame with added indicator columns.
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.sort_index()

    # Technical indicators
    df["ema_50"]  = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema_200"] = EMAIndicator(df["close"], window=200).ema_indicator()

    macd_obj = MACD(df["close"])
    df["macd"]        = macd_obj.macd()
    df["macd_signal"] = macd_obj.macd_signal()

    df["adx"] = ADXIndicator(df["high"], df["low"], df["close"]).adx()
    df["rsi"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr"] = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
    df["obv"] = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()

    # EMA crossover detection
    df["ema_cross_up"] = (df["ema_50"] > df["ema_200"]) & (df["ema_50"].shift(1) <= df["ema_200"].shift(1))

    # Trend scoring logic
    def score_row(i, r):
        if pd.isna(r.ema_50) or pd.isna(r.ema_200):
            return None

        trend_sign = 1 if r.ema_50 > r.ema_200 else -1 if r.ema_50 < r.ema_200 else 0
        base = trend_sign * 3
        base += 0.5 * trend_sign if r.close > r.ema_50 else -0.5 * trend_sign
        base += 0.5 * trend_sign if r.macd > r.macd_signal else -0.5 * trend_sign

        if df["ema_cross_up"].iloc[i]:
            base += 1
        if i > 0 and r.ema_50 > df["ema_50"].iloc[i - 1]:
            base += 0.5

        adx = r.adx if not pd.isna(r.adx) else 0
        mult = 1.0 if adx < 20 else 1.5 if adx < 40 else 2.0
        score = base * mult

        if trend_sign == 1 and r.rsi > 70:
            recent_cross = df["ema_cross_up"].iloc[max(0, i - 5):i].any()
            if not recent_cross:
                score -= 1
        elif trend_sign == -1 and r.rsi < 30:
            score += 1

        return round(max(-10, min(10, score)), 2)

    df["market_score"] = [score_row(i, r) for i, (idx, r) in enumerate(df.iterrows())]
    df["market_trend"] = (df["market_score"] / 10).round(2)
    df = df.loc["2014-01-01":"2024-12-31"]
    return df

# -----------------------------------------------------------
# load_data_from_db() -> pd.DataFrame
# -----------------------------------------------------------
# Loads OHLCV historical data for the S&P 500 index from the database.
# Returns a DataFrame indexed by date.
def load_data_from_db() -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = session.query(SP500Index).all()
        df = pd.DataFrame([{
            "date":   r.date,
            "open":   r.open,
            "high":   r.high,
            "low":    r.low,
            "close":  r.close,
            "volume": r.volume,
        } for r in rows])
        df.set_index("date", inplace=True)
        return df
    finally:
        session.close()

# -----------------------------------------------------------
# _cast(x, is_int=False) -> Optional[int|float]
# -----------------------------------------------------------
# Converts values to float or int, handling NaN and preserving nulls.
# Used before inserting indicators into the database.
def _cast(x, is_int=False):
    if pd.isna(x):
        return None
    return int(x) if is_int else float(x)

# -----------------------------------------------------------
# update_database_with_indicators(df: pd.DataFrame)
# -----------------------------------------------------------
# Writes the calculated indicators from a DataFrame into the SP500Index table.
# Merges data by date, commits changes to the database.
def update_database_with_indicators(df: pd.DataFrame):
    session = SessionLocal()
    try:
        for date, r in df.iterrows():
            entry = SP500Index(
                date=date.date(),
                ema_50      = _cast(r["ema_50"]),
                ema_200     = _cast(r["ema_200"]),
                rsi         = _cast(r["rsi"]),
                macd        = _cast(r["macd"]),
                macd_signal = _cast(r["macd_signal"]),
                atr         = _cast(r["atr"]),
                obv         = _cast(r["obv"], is_int=True),
                market_score= _cast(r["market_score"]),
                market_trend= _cast(r["market_trend"]),
            )
            session.merge(entry)
        session.commit()
        print("indicators updated")
    except Exception as e:
        session.rollback()
        print(f"Error updating indicators: {e}")
    finally:
        session.close()

# -----------------------------------------------------------
# Manual execution
# -----------------------------------------------------------
# Loads data, calculates indicators, and updates the database when run directly.
if __name__ == "__main__":
    raw_df     = load_data_from_db()
    enriched   = compute_indicators(raw_df)
    update_database_with_indicators(enriched)
