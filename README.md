# AlgoTrading Project

פרויקט אלגו-טריידינג לבחינת אסטרטגיות מסחר מבוססות דאטה.
כולל קוד לפנייה ל-Yahoo Finance ו-Investing.com.

## מבנה הפרויקט

- `src/` – קוד המקור
- `data/` – קבצים שמכילים נתוני מניות

## התקנה

```bash
python -m venv Algo_env
.\Algo_env\Scripts\activate
pip install -r requirements.txt



generate_sector_indicators.py
This script calculates Relative Strength indicators for sector-level analysis relative to the S&P 500.
It generates the following metrics and exports them to data/sector_indicators_train.csv:

Classic RS: RS_21, RS_55, RS_123

Anchored RS: Relative strength since a specific anchor date (e.g., YTD)

RS Ribbon: RSribbon_8, RSribbon_21, RSribbon_42 based on EMA momentum

Momentum Slope: slope_21, slope_42 to detect acceleration/deceleration in RS

Output is used in Phase 2: Sector vs Market Trend Analysis