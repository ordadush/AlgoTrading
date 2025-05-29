import pandas as pd

# ===== שלב 1: טעינת הדאטה וסינון ל־train בלבד =====
df = pd.read_csv("data/full_dataset_with_split.csv")
df["date"] = pd.to_datetime(df["date"])
df = df[df["split"] == "train"].copy()
df = df.sort_values(["symbol", "date"]).reset_index(drop=True)

# ===== שלב 2: פונקציות אינדיקטורים =====

def calc_sector_rs(df, market_symbol="^GSPC", windows=[21, 55, 123]):
    results = []
    for window in windows:
        df[f'return_{window}'] = df.groupby("symbol")["close"].transform(lambda x: x.pct_change(periods=window))
        market_returns = df[df["symbol"] == market_symbol][["date", f'return_{window}']].rename(columns={f'return_{window}': 'market_return'})
        merged = df.merge(market_returns, on="date", how="left")
        merged[f'RS_{window}'] = merged[f'return_{window}'] / merged['market_return']
        results.append(merged[["symbol", "date", f'RS_{window}']])
    rs_df = results[0]
    for other in results[1:]:
        rs_df = rs_df.merge(other, on=["symbol", "date"])
    return rs_df

def calc_anchored_rs(df, market_symbol="^GSPC", anchor_date="2015-01-01"):
    anchor_prices = df[df["date"] == anchor_date][["symbol", "close"]].rename(columns={"close": "anchor_price"})
    df = df.merge(anchor_prices, on="symbol", how="left")
    df["anchored_return"] = df["close"] / df["anchor_price"] - 1
    market_anchored = df[df["symbol"] == market_symbol][["date", "anchored_return"]].rename(columns={"anchored_return": "market_anchor"})
    df = df.merge(market_anchored, on="date", how="left")
    df["anchored_RS"] = df["anchored_return"] / df["market_anchor"]
    return df[["symbol", "date", "anchored_RS"]]

def calc_rs_ribbon(df):
    df["daily_return"] = df.groupby("symbol")["close"].transform(lambda x: x.pct_change())
    for window in [8, 21, 42]:
        df[f'RSribbon_{window}'] = df.groupby("symbol")["daily_return"].transform(lambda x: x.rolling(window).mean())
        df[f'slope_{window}'] = df.groupby("symbol")[f'RSribbon_{window}'].transform(lambda x: x.diff())
    return df[["symbol", "date", "RSribbon_8", "RSribbon_21", "RSribbon_42", "slope_21", "slope_42"]]

# ===== שלב 3: הרצת החישובים =====
rs_df = calc_sector_rs(df)
anchored_df = calc_anchored_rs(df)
ribbon_df = calc_rs_ribbon(df)

# ===== שלב 4: מיזוג ושמירה =====
final = rs_df.merge(anchored_df, on=["symbol", "date"])
final = final.merge(ribbon_df, on=["symbol", "date"])
final.to_csv("data/sector_indicators_train.csv", index=False)

print("✅ sector_indicators_train.csv נוצר בהצלחה!")
