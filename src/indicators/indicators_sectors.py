# generate_sector_indicators.py
# -------------------------------------------------------------
# Computes technical indicators for every sector ETF stored in
# the `sector_data` table and writes them back into the same
# table.  Logic = indicators_sp500.py, applied per symbol.
# -------------------------------------------------------------

import pandas as pd
from ta.trend      import EMAIndicator, MACD, ADXIndicator
from ta.momentum   import RSIIndicator
from ta.volatility import AverageTrueRange
from ta.volume     import OnBalanceVolumeIndicator

from DBintegration.database import SessionLocal
from DBintegration.models   import SectorData


# ──────────────────────────────────────────────────────────────
# Helper – safe cast (handles NaN / numpy types)
# ──────────────────────────────────────────────────────────────
def _cast(x, to_int=False):
    if pd.isna(x):
        return None
    return int(x) if to_int else float(x)


# ──────────────────────────────────────────────────────────────
# Load all sector symbols present in the table
# ──────────────────────────────────────────────────────────────
def load_all_sector_symbols() -> list:
    session = SessionLocal()
    try:
        return [row[0]
                for row in session.query(SectorData.symbol).distinct()]
    finally:
        session.close()


# ──────────────────────────────────────────────────────────────
# Load OHLCV history for a given sector ETF
# ──────────────────────────────────────────────────────────────
def load_sector_df(symbol: str) -> pd.DataFrame:
    session = SessionLocal()
    try:
        rows = (
            session.query(SectorData)
                   .filter(SectorData.symbol == symbol)
                   .all()
        )
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
# Indicator computation – identical to SP500 logic
# ──────────────────────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index).tz_localize(None)
    df = df.sort_index()

    # EMA, MACD, RSI, ATR, OBV
    df["ema_50"]  = EMAIndicator(df["close"], window=50).ema_indicator()
    df["ema_200"] = EMAIndicator(df["close"], window=200).ema_indicator()

    macd = MACD(df["close"])
    df["macd"]        = macd.macd()
    df["macd_signal"] = macd.macd_signal()

    df["adx"] = ADXIndicator(df["high"], df["low"], df["close"]).adx()
    df["rsi"] = RSIIndicator(df["close"], window=14).rsi()
    df["atr"] = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
    df["obv"] = OnBalanceVolumeIndicator(df["close"], df["volume"]).on_balance_volume()

    # EMA cross-up detection
    df["ema_cross_up"] = (df["ema_50"] > df["ema_200"]) & (
        df["ema_50"].shift(1) <= df["ema_200"].shift(1)
    )

    # Trend scoring – same rules as for SP500
    def score_row(i, r):
        if pd.isna(r.ema_50) or pd.isna(r.ema_200):
            return None

        trend_sign = 1 if r.ema_50 > r.ema_200 else -1
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
            recent = df["ema_cross_up"].iloc[max(0, i - 5):i].any()
            if not recent:
                score -= 1
        elif trend_sign == -1 and r.rsi < 30:
            score += 1

        return round(max(-10, min(10, score)), 2)

    df["market_score"] = [score_row(i, r) for i, (idx, r) in enumerate(df.iterrows())]
    df["market_trend"] = (df["market_score"] / 10).round(2)

    # Keep only 2014-01-01 .. 2024-12-31
    return df.loc["2014-01-01":"2024-12-31"]


# ──────────────────────────────────────────────────────────────
# Write back to DB (row-by-row merge, like original script)
# ──────────────────────────────────────────────────────────────
def update_sector_table(symbol: str, df: pd.DataFrame):
    session = SessionLocal()
    try:
        for date, r in df.iterrows():
            entry = SectorData(
                symbol = symbol,
                date   = date.date(),

                ema_50      = _cast(r["ema_50"]),
                ema_200     = _cast(r["ema_200"]),
                rsi         = _cast(r["rsi"]),
                macd        = _cast(r["macd"]),
                macd_signal = _cast(r["macd_signal"]),
                atr         = _cast(r["atr"]),
                obv         = _cast(r["obv"], to_int=True),
                market_score= _cast(r["market_score"]),
                market_trend= _cast(r["market_trend"]),
            )
            session.merge(entry)
        session.commit()
        print(f"✅ {symbol} updated")
    except Exception as exc:
        session.rollback()
        print(f"❌ {symbol}: {exc}")
    finally:
        session.close()


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    symbols = load_all_sector_symbols()
    if not symbols:
        print("sector_data table is empty – nothing to do.")
        exit()

    print("⏳ Calculating indicators for sector ETFs …")
    for sym in symbols:
        df = load_sector_df(sym)
        if df.empty:
            print(f"{sym} – no OHLCV data")
            continue

        enriched = compute_indicators(df)
        update_sector_table(sym, enriched)
