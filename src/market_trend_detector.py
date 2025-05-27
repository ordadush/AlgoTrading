import pandas as pd
import numpy as np

# חישוב MACD
def calculate_macd(df, short_window=12, long_window=26, signal_window=9):
    df['EMA12'] = df['Close'].ewm(span=short_window, adjust=False).mean()
    df['EMA26'] = df['Close'].ewm(span=long_window, adjust=False).mean()
    df['MACD'] = df['EMA12'] - df['EMA26']
    df['MACD_signal'] = df['MACD'].ewm(span=signal_window, adjust=False).mean()
    return df

# TD - Top-Down Method
def td_market_trend(df, lambda_threshold=0.15):
    trend = []
    bull = True
    peak = trough = df['Close'].iloc[0]
    start_idx = df.index[0]

    for date, price in df['Close'].items():
        if bull:
            if price > peak:
                peak = price
            elif price <= peak * (1 - lambda_threshold):
                trend.append((start_idx, date, 'Bull'))
                bull = False
                trough = price
                start_idx = date
        else:
            if price < trough:
                trough = price
            elif price >= trough * (1 + lambda_threshold):
                trend.append((start_idx, date, 'Bear'))
                bull = True
                peak = price
                start_idx = date

    trend_df = pd.DataFrame(trend, columns=['Start', 'End', 'Market_Trend'])
    return trend_df

# פונקציית עזר – החלת ה-TD על כל יום
def label_market_trend(df, td_df):
    df['Market_Trend'] = 'Neutral'
    for _, row in td_df.iterrows():
        df.loc[row['Start']:row['End'], 'Market_Trend'] = row['Market_Trend']
    return df

# סינון לפי שוק שורי + MACD + מעל MA200
def is_bull_market(df):
    df['MA200'] = df['Close'].rolling(window=200).mean()
    df = calculate_macd(df)
    df['Bull_Signal'] = (
        (df['Market_Trend'] == 'Bull') &
        (df['MACD'] > df['MACD_signal']) &
        (df['Close'] > df['MA200'])
    )
    return df
