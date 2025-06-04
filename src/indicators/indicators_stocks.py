# generate_stock_indicators.py
# -------------------------------------------------------------
# Per-stock technical indicators  +  market-relative metrics
# Saves results back into daily_stock_data  (now via BULK INSERT)
# -------------------------------------------------------------

import pandas as pd
from ta.trend      import EMAIndicator, MACD
from ta.momentum   import RSIIndicator
from ta.volatility import AverageTrueRange
from ta.volume     import OnBalanceVolumeIndicator

from DBintegration.database import SessionLocal
from DBintegration.models   import DailyStockData, SP500Index

from sqlalchemy.dialects.postgresql import insert  


# ──────────────────────────────────────────────────────────────
# Cast helper  (avoids NumPy objects / NaN in SQL)
# ──────────────────────────────────────────────────────────────
def _cast(x, to_int=False):
    if pd.isna(x):
        return None
    return int(x) if to_int else float(x)

# ──────────────────────────────────────────────────────────────
# Load market (SP500)  → returns
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
    mkt["daily_return_mkt"]      = mkt["close_mkt"].pct_change()
    mkt["rolling_return_14_mkt"] = mkt["close_mkt"].pct_change(periods=14)
    return mkt

# ──────────────────────────────────────────────────────────────
# Load OHLCV for a single symbol
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
# Technical indicators (EMA, MACD, RSI, ATR, OBV)
# ──────────────────────────────────────────────────────────────
def compute_stock_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None).sort_values()

    df["ema_20"]  = EMAIndicator(df["close"], window=20).ema_indicator()
    df["ema_50"]  = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema_200"] = EMAIndicator(df["close"], window=200).ema_indicator()

    macd = MACD(df["close"])
    df["macd"]        = macd.macd()
    df["macd_signal"] = macd.macd_signal()

    df["rsi_14"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr"]    = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
    df["obv"]    = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()

    df["daily_return"]      = df["close"].pct_change()
    df["rolling_return_14"] = df["close"].pct_change(periods=14)
    return df

# ──────────────────────────────────────────────────────────────
# Merge with market, add relative metrics
# ──────────────────────────────────────────────────────────────
def add_market_metrics(stock: pd.DataFrame,
                       market: pd.DataFrame) -> pd.DataFrame:
    merged = stock.join(market, how="left")

    merged["market_return_14"]     = merged["rolling_return_14_mkt"]
    merged["relative_strength_14"] = (
        merged["rolling_return_14"] / merged["rolling_return_14_mkt"]
    )

    w = 20
    cov = merged["daily_return"].rolling(w).cov(merged["daily_return_mkt"])
    var = merged["daily_return_mkt"].rolling(w).var()
    merged["beta_20"]  = cov / var
    merged["alpha_20"] = (
        merged["daily_return"].rolling(w).mean()
        - merged["beta_20"] * merged["daily_return_mkt"].rolling(w).mean()
    )

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
# Bulk-insert / update  (new!)
# ──────────────────────────────────────────────────────────────

def upsert_stock(symbol: str, df: pd.DataFrame):
    if df.empty:
        return

    # נמיר את ה-DataFrame לרשימת dict-ים (מהיר)
    rows = []
    for date, r in df.iterrows():
        rows.append({
            "symbol":  symbol,
            "date":    date.date(),
            "open":    _cast(r["open"]),
            "high":    _cast(r["high"]),
            "low":     _cast(r["low"]),
            "close":   _cast(r["close"]),
            "volume":  _cast(r["volume"], to_int=True),

            "ema_20":  _cast(r["ema_20"]),
            "ema_50":  _cast(r["ema_50"]),
            "ema_200": _cast(r["ema_200"]),
            "rsi_14":  _cast(r["rsi_14"]),
            "macd":    _cast(r["macd"]),
            "macd_signal": _cast(r["macd_signal"]),
            "atr":     _cast(r["atr"]),
            "obv":     _cast(r["obv"], to_int=True),

            "daily_return":      _cast(r["daily_return"]),
            "rolling_return_14": _cast(r["rolling_return_14"]),
            "market_return_14":  _cast(r["market_return_14"]),
            "relative_strength_14": _cast(r["relative_strength_14"]),
            "beta_up_20":        _cast(r["beta_up_20"]),
            "beta_down_20":      _cast(r["beta_down_20"]),
            "alpha_20":          _cast(r["alpha_20"]),
        })

    session = SessionLocal()
    try:
        stmt = insert(DailyStockData).values(rows)
        # columns to update if (symbol,date) already exist
        update_cols = {c.key: c for c in stmt.excluded if c.key not in ("symbol", "date")}
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "date"],  # primary key
            set_=update_cols
        )
        session.execute(stmt)
        session.commit()
        print(f"✅ {symbol}: {len(rows)} rows upserted/updated")
    except Exception as exc:
        session.rollback()
        print(f"❌ {symbol}: {exc}")
    finally:
        session.close()


# ──────────────────────────────────────────────────────────────
# Load all symbols present in the table
# ──────────────────────────────────────────────────────────────
def load_all_symbols() -> list:
    session = SessionLocal()
    try:
        return [row[0] for row in session.query(DailyStockData.symbol).distinct()]
    finally:
        session.close()

# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    market_df = load_market_df()
    if market_df.empty:
        print("⚠️  sp500_index table is empty – abort.")
        exit()

    if SessionLocal().query(DailyStockData).count() == 0:
        print("⚠️  daily_stock_data is empty – load price history first.")
        exit()

    print("⏳ Calculating indicators for all symbols…")
    for sym in load_all_symbols():
        stock_df = load_stock_df(sym)
        if stock_df.empty:
            continue

        tech_df = compute_stock_indicators(stock_df)
        full_df = add_market_metrics(tech_df, market_df)
        upsert_stock(sym, full_df)
