# ===== src/patterns/darvas_box.py =====
import pandas as pd

def identify_darvas_boxes(df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
    """
    קלט:
        df: DataFrame עם עמודות 'High' ו-'Low', כאשר האינדקס הוא תאריכים
        window: מספר ימים לדשדוש
    פלט:
        DataFrame עם קופסאות דארווס: start_index, end_index, top, bottom
    """
    boxes = []
    for i in range(window, len(df)):
        high_window = df['High'].iloc[i - window:i]
        low_window = df['Low'].iloc[i - window:i]
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
    """
    קלט:
        df: DataFrame עם עמודות 'Close' ו-'Volume'
        boxes_df: תוצאה של identify_darvas_boxes
        volume_window: מספר ימים לחישוב ממוצע נפח מסחר
    פלט:
        DataFrame עם פריצות: breakout_date, breakout_price, volume
    """
    signals = []
    avg_volume = df['Volume'].rolling(volume_window).mean()
    for _, box in boxes_df.iterrows():
        breakout_index = box['end_index'] + 1
        if breakout_index >= len(df):
            continue
        breakout_price = df['Close'].iloc[breakout_index]
        breakout_volume = df['Volume'].iloc[breakout_index]
        if breakout_price > box['top'] and breakout_volume > avg_volume.iloc[breakout_index]:
            signals.append({
                'breakout_date': df.index[breakout_index],
                'breakout_price': breakout_price,
                'volume': breakout_volume
            })
    return pd.DataFrame(signals)
