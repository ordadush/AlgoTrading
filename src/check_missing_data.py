import os
import pandas as pd

# קריאת הקובץ nasdaqlisted.csv
nasdaq_list = pd.read_csv("nasdaqlisted.csv", sep='|')
symbols_in_csv = nasdaq_list['Symbol'].tolist()

# רשימת הקבצים בתיקייה data
files_in_data = [f.replace('.csv', '') for f in os.listdir('data') if f.endswith('.csv')]

# מציאת ההבדלים
missing_symbols = list(set(symbols_in_csv) - set(files_in_data))

print(f"סה\"כ מניות בקובץ NASDAQ: {len(symbols_in_csv)}")
print(f"סה\"כ מניות בתיקייה data: {len(files_in_data)}")
print(f"סה\"כ מניות שחסרות בתיקייה data: {len(missing_symbols)}")

# להציג כמה דוגמאות
print("\nחסרות לדוגמה:")
print(missing_symbols[:20])
