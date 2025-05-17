import pandas as pd
import os

'''
File for checking stocks data in the data folder.
This file checks the data of stocks in the data folder.
It checks if the stocks have enough data for backtesting and if they are still trading.
'''

# הגדרת הנתיב של התיקייה
data_path = "data"

# רשימות לאיסוף נתונים
sufficient_history = []
still_trading = []

# מעבר על כל קובץ בתיקייה
for file in os.listdir(data_path):
    if file.endswith(".csv"):
        symbol = file.replace(".csv", "")
        file_path = os.path.join(data_path, file)
        
        # טעינת הקובץ
        try:
            # טעינה עם זיהוי תאריך בעמודה הנכונה
            df = pd.read_csv(file_path, header=2)
            
            # עדכון שמות העמודות בהתאם למה שיש בקובץ
            df.columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume']
            
            # המרת העמודה 'Date' לתאריך
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

            # סינון של שורות בלי תאריך תקין
            df = df.dropna(subset=['Date'])
            
            start_date = df['Date'].min()
            end_date = df['Date'].max()
            years_of_data = (end_date - start_date).days / 365

            # הדפסת מידע על המניה
            print(f"{symbol}: {years_of_data:.2f} years of data | Last Date: {end_date.date()}")

            # בדיקה אם יש מספיק נתונים
            if years_of_data >= 5:
                sufficient_history.append(symbol)

            # בדיקה אם עדיין נסחרת
            last_year = end_date.year
            if df[df['Date'].dt.year == last_year]['Volume'].sum() > 0:
                still_trading.append(symbol)
        
        except Exception as e:
            print(f"Error processing {symbol}: {e}")

# סיכום נתונים
print("\n--- מניות מתאימות ל-Backtest ---")
print(sufficient_history)

print("\n--- מניות שעדיין נסחרות ---")
print(still_trading)
