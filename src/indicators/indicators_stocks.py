# generate_stock_indicators.py
# -------------------------------------------------------------
# Calculates per-stock technical indicators **and** market-relative
# metrics, then stores them in the 'daily_stock_data' table.
#
# Per-stock indicators:
#   EMA-20/50/200 • MACD (+signal) • RSI-14 • ATR • OBV
#   Daily return • 14-day rolling return
#
# Market-relative metrics (vs. SP500Index):
#   market_return_14 • relative_strength_14
#   beta_up_20 • beta_down_20 • alpha_20
#
# All values are merged back into daily_stock_data via SQLAlchemy.
# -------------------------------------------------------------

import pandas as pd
from ta.trend      import EMAIndicator, MACD
from ta.momentum   import RSIIndicator
from ta.volatility import AverageTrueRange
from ta.volume     import OnBalanceVolumeIndicator

from DBintegration.database import SessionLocal
from DBintegration.models   import DailyStockData, SP500Index

# ──────────────────────────────────────────────────────────────
# Helper to cast values (avoids NumPy objects / NaN in SQL)
# ──────────────────────────────────────────────────────────────
def _cast(x, to_int=False):
    if pd.isna(x):
        return None
    return int(x) if to_int else float(x)

# ──────────────────────────────────────────────────────────────
# Market dataframe (SP500 close + returns)
# ──────────────────────────────────────────────────────────────
def load_market_df() -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = session.query(SP500Index.date, SP500Index.close).all()
        mkt  = pd.DataFrame({"date": [r.date for r in rows],
                             "close_mkt": [r.close for r in rows]})
    finally:
        session.close()

    mkt = mkt.sort_values("date").set_index("date")
    mkt["daily_return_mkt"]        = mkt["close_mkt"].pct_change()
    mkt["rolling_return_14_mkt"]   = mkt["close_mkt"].pct_change(periods=14)
    return mkt

# ──────────────────────────────────────────────────────────────
# Stock-level OHLCV loader
# ──────────────────────────────────────────────────────────────
def load_stock_df(symbol: str) -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = (session.query(DailyStockData)
                       .filter(DailyStockData.symbol == symbol)
                       .all())
        df = pd.DataFrame([{
            "date":   r.date,
            "open":   r.open,
            "high":   r.high,
            "low":    r.low,
            "close":  r.close,
            "volume": r.volume
        } for r in rows])
    finally:
        session.close()

    if df.empty:
        return df
    df.set_index("date", inplace=True)
    return df.sort_index()

# ──────────────────────────────────────────────────────────────
# Technical indicators per stock (EMA, MACD, RSI, ATR, OBV)
# ──────────────────────────────────────────────────────────────
def compute_stock_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.sort_index()

    df["ema_20"]  = EMAIndicator(df["close"], window=20).ema_indicator()
    df["ema_50"]  = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema_200"] = EMAIndicator(df["close"], window=200).ema_indicator()

    macd = MACD(df["close"])
    df["macd"]        = macd.macd()
    df["macd_signal"] = macd.macd_signal()

    df["rsi_14"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr"]    = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
    df["obv"]    = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()

    df["daily_return"]        = df["close"].pct_change()
    df["rolling_return_14"]   = df["close"].pct_change(periods=14)
    return df

# ──────────────────────────────────────────────────────────────
# Merge with market & compute relative metrics
# ──────────────────────────────────────────────────────────────
def add_market_metrics(stock: pd.DataFrame,
                       market: pd.DataFrame) -> pd.DataFrame:
    merged = stock.join(market, how="left")

    # Relative strength
    merged["market_return_14"]     = merged["rolling_return_14_mkt"]
    merged["relative_strength_14"] = (
        merged["rolling_return_14"] / merged["rolling_return_14_mkt"]
    )

    # Rolling beta / alpha (20-day window)
    w = 20
    cov = merged["daily_return"].rolling(w).cov(merged["daily_return_mkt"])
    var = merged["daily_return_mkt"].rolling(w).var()
    merged["beta_20"] = cov / var
    merged["alpha_20"] = (
        merged["daily_return"].rolling(w).mean()
        - merged["beta_20"] * merged["daily_return_mkt"].rolling(w).mean()
    )

    # Beta for up / down market days
    def beta_signed(direction: str):
        mask = merged["daily_return_mkt"] > 0 if direction == "up" else merged["daily_return_mkt"] < 0
        return (
            (merged["daily_return"] * mask).rolling(w)
             .cov((merged["daily_return_mkt"] * mask))
        ) / (
            (merged["daily_return_mkt"] * mask).rolling(w).var()
        )

    merged["beta_up_20"]   = beta_signed("up")
    merged["beta_down_20"] = beta_signed("down")
    return merged

# ──────────────────────────────────────────────────────────────
# Write back to DB
# ──────────────────────────────────────────────────────────────
def upsert_stock(symbol: str, df: pd.DataFrame):
    session = SessionLocal()
    try:
        for date, row in df.iterrows():
            entry = DailyStockData(
                symbol = symbol,
                date   = date.date(),

                # OHLCV (keep originals)
                open   = _cast(row["open"]),
                high   = _cast(row["high"]),
                low    = _cast(row["low"]),
                close  = _cast(row["close"]),
                volume = _cast(row["volume"], to_int=True),

                # Core indicators
                ema_20 = _cast(row["ema_20"]),
                ema_50 = _cast(row["ema_50"]),
                ema_200= _cast(row["ema_200"]),
                rsi_14 = _cast(row["rsi_14"]),
                macd   = _cast(row["macd"]),
                macd_signal = _cast(row["macd_signal"]),
                atr    = _cast(row["atr"]),
                obv    = _cast(row["obv"], to_int=True),

                # Returns
                daily_return      = _cast(row["daily_return"]),
                rolling_return_14 = _cast(row["rolling_return_14"]),
                market_return_14  = _cast(row["market_return_14"]),

                # Market-relative
                relative_strength_14 = _cast(row["relative_strength_14"]),
                beta_up_20           = _cast(row["beta_up_20"]),
                beta_down_20         = _cast(row["beta_down_20"]),
                alpha_20             = _cast(row["alpha_20"]),
            )
            session.merge(entry)
        session.commit()
        print(f"✅ {symbol} updated.")
    except Exception as exc:
        session.rollback()
        print(f"❌ {symbol} failed: {exc}")
    finally:
        session.close()
# ──────────────────────────────────────────────────────────────
# DB – list all unique symbols
# ──────────────────────────────────────────────────────────────
def load_all_symbols() -> list:
    """Return distinct stock symbols present in daily_stock_data."""
    session = SessionLocal()
    try:
        return [row[0] for row in session.query(DailyStockData.symbol).distinct()]
    finally:
        session.close()
# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    market_df = load_market_df()        # one fetch only
    session_symbols = SessionLocal()    # quick check for empty DB
    if session_symbols.query(DailyStockData).count() == 0:
        print("daily_stock_data is empty – load price history first.")
        session_symbols.close()
        exit()
    session_symbols.close()

    print("Calculating indicators for all symbols…")
    for symbol in load_all_symbols():
        stock_df = load_stock_df(symbol)
        if stock_df.empty:
            print(f"{symbol} – no price data")
            continue

        tech_df  = compute_stock_indicators(stock_df)
        full_df  = add_market_metrics(tech_df, market_df)
        upsert_stock(symbol, full_df)
