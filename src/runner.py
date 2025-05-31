# ===== src/runner.py =====
import pandas as pd
from patterns.darvas_box import identify_darvas_boxes, detect_breakout_with_volume

# קלט: קובץ CSV עם נתוני מניה כולל עמודות 'High', 'Low', 'Close', 'Volume'
df = pd.read_csv("data/AAPL.csv", parse_dates=["Date"], index_col="Date")

# חישוב קופסאות דארווס
boxes = identify_darvas_boxes(df)

# זיהוי פריצות בליווי ווליום גבוה
signals = detect_breakout_with_volume(df, boxes)

# פלט: הדפסת פריצות שאותרו
print(signals.head())
