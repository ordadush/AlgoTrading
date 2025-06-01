import pandas as pd

def identify_darvas_boxes(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    df.columns = df.columns.str.lower()  # 💡 הופך את שמות העמודות ל-lowercase

    boxes = []
    for i in range(window, len(df)):
        high_window = df['high'].iloc[i - window:i]
        low_window = df['low'].iloc[i - window:i]
        top = high_window.max()
        bottom = low_window.min()
        if all(high_window <= top) and all(low_window >= bottom):
            boxes.append({
                'start_index': i - window,
                'end_index': i,
                'top': top,
                'bottom': bottom
            })
    return pd.DataFrame(boxes)

def detect_breakout_with_volume(df: pd.DataFrame, boxes_df: pd.DataFrame, volume_window: int = 20) -> pd.DataFrame:
    df.columns = df.columns.str.lower()  # 💡 לוודא ש-Close ו-Volume זמינים

    signals = []
    avg_volume = df['volume'].rolling(volume_window).mean()

    for _, box in boxes_df.iterrows():
        breakout_index = int(box['end_index']) + 1
        if breakout_index >= len(df):
            continue
        breakout_price = df['close'].iloc[breakout_index]
        breakout_volume = df['volume'].iloc[breakout_index]
        if breakout_price > box['top'] and breakout_volume > avg_volume.iloc[breakout_index]:
            signals.append({
                'breakout_date': df.index[breakout_index],
                'breakout_price': breakout_price,
                'volume': breakout_volume
            })

    return pd.DataFrame(signals)
