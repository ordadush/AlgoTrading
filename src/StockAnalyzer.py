import yfinance as yf
import pandas as pd
from db_utils import save_dataframe_to_db
from datetime import datetime, timedelta
import os
import time

'''
Rewritten stockAnalyzer to save directly into the Railway database.
''' 

# Dates for data range
end_date = datetime.now()  # Using current date instead of fixed date
start_date = end_date - timedelta(days=365 * 5)  # 5 years of data

# Filtered list preparation
filtered_file = "../filtered_nasdaq.csv"

if os.path.exists(filtered_file):
    print("Loading filtered stock list...")
    filtered_stocks = pd.read_csv(filtered_file)
else:
    print("Filtering stock list from nasdaqlisted.csv...")
    try:
        nasdaq_df = pd.read_csv("nasdaqlisted.csv", sep='|')
    except FileNotFoundError:
        print("Error: nasdaqlisted.csv file not found.")
        print("Please download it from: https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt")
        print("And save it as nasdaqlisted.csv after removing the last line")
        exit(1)

    nasdaq_df['Symbol'] = nasdaq_df['Symbol'].astype(str)
    nasdaq_df['Security Name'] = nasdaq_df['Security Name'].astype(str)

    filtered_stocks = nasdaq_df[
        (nasdaq_df['ETF'] != 'Y') &
        (nasdaq_df['Test Issue'] != 'Y') &
        (~nasdaq_df['Symbol'].str.contains(r'[.\-/]', na=False)) &
        (~nasdaq_df['Security Name'].str.contains("Warrant|Right|Unit", case=False, na=False))
    ]

    filtered_stocks.to_csv(filtered_file, index=False)
    print(f"Saved filtered list to {filtered_file}")

def has_sufficient_history(ticker, min_years=5):
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max")

        if hist.empty:
            return False

        first_date = hist.index[0].to_pydatetime().replace(tzinfo=None)
        age_years = (datetime.today() - first_date).days / 365
        return age_years >= min_years

    except Exception as e:
        print(f"Error checking {ticker}: {e}")
        return False

# Collect valid stocks
MAX_STOCKS = 5  # הקטנת מספר המניות לבדיקה בלבד
valid_stocks = []
checked_count = 0

print(f"Finding stocks with sufficient history...")
for symbol in filtered_stocks["Symbol"]:
    checked_count += 1
    if has_sufficient_history(symbol, min_years=5):
        valid_stocks.append(symbol)
        print(f"Found valid stock: {symbol} ({len(valid_stocks)}/{MAX_STOCKS})")
    
    if checked_count % 10 == 0:
        print(f"Checked {checked_count} stocks so far...")
    
    if len(valid_stocks) >= MAX_STOCKS:
        break

print(f"Found {len(valid_stocks)} valid stocks out of {checked_count} checked")

# Download and save to DB
for i, symbol in enumerate(valid_stocks):
    try:
        print(f"[{i+1}/{len(valid_stocks)}] Downloading {symbol}...")
        data = yf.download(
            symbol,
            start=start_date.strftime('%Y-%m-%d'),
            end=end_date.strftime('%Y-%m-%d'),
            progress=False
        )

        if not data.empty:
            # חשוב מאוד: ודא שזה DataFrame רגיל עם המבנה הנכון
            print(f"Original data shape: {data.shape}")
            print(f"Original data columns: {data.columns}")
            print(f"Original data index: {type(data.index)}")
            print(f"First row sample: {data.iloc[0]}")
            
            # כאן טיפול מיוחד לשמירת התאריכים כעמודה וקביעת שמות העמודות
            data_processed = data.copy()
            data_processed.reset_index(inplace=True)  # הפיכת התאריך לעמודה
            
            # אם השמות של העמודות הם MultiIndex, אנחנו נפריד אותם בצורה נכונה
            if isinstance(data_processed.columns, pd.MultiIndex):
                # יצירת שמות עמודות חדשים
                new_cols = []
                for col in data_processed.columns:
                    if isinstance(col, tuple) and len(col) > 1:
                        # הערכים הסטנדרטיים של yfinance הם ('Open', symbol), ('High', symbol) וכו'
                        if col[0] != '':  # זה עבור המקרה שזה לא שם העמודה הראשונה (Date)
                            new_cols.append(col[0])  # לוקח רק את השם הראשון (Open, High...)
                        else:
                            new_cols.append('Date')  # אם זה ריק זה כנראה עמודת התאריך
                    else:
                        new_cols.append(col)  # משאיר כמו שהוא אם זה לא tuple
                
                data_processed.columns = new_cols
            
            print(f"Processed data columns: {data_processed.columns.tolist()}")
            print(f"Sample data after processing:")
            print(data_processed.head(1).to_string())
            
            save_dataframe_to_db(symbol, data_processed)
        else:
            print(f"No data for {symbol}.")

        # Sleep a bit to avoid rate limiting
        if i < len(valid_stocks) - 1:
            time.sleep(1)

    except Exception as e:
        print(f"❌ Failed to download {symbol}: {e}")
        # הדפסת מידע נוסף בעת שגיאה
        import traceback
        traceback.print_exc()