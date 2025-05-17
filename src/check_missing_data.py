import os
import pandas as pd

'''
File for checking missing data in the data folder.
This file compares the symbols in the nasdaqlisted.csv file with the files in the data folder.
'''
# Read nasdaqlisted.csv
nasdaq_list = pd.read_csv("nasdaqlisted.csv", sep='|')
symbols_in_csv = nasdaq_list['Symbol'].tolist()

# רשימת הקבצים בתיקייה data
files_in_data = [f.replace('.csv', '') for f in os.listdir('data') if f.endswith('.csv')]

# Find difference
missing_symbols = list(set(symbols_in_csv) - set(files_in_data))

print(f"סה\"כ מניות בקובץ NASDAQ: {len(symbols_in_csv)}")
print(f"סה\"כ מניות בתיקייה data: {len(files_in_data)}")
print(f"סה\"כ מניות שחסרות בתיקייה data: {len(missing_symbols)}")

# Show missing symbols
print("\nחסרות לדוגמה:")
print(missing_symbols[:20])
