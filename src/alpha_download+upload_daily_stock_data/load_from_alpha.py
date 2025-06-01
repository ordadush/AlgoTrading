# ===== src/load_from_alpha.py =====
import pandas as pd
import time
import os
from alpha_vantage.timeseries import TimeSeries

# 🔑 Your Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = "W7XJZ5Y26OP4RDTI"

# 📁 Local cache folder
cache_dir = "data/cache_alpha"
os.makedirs(cache_dir, exist_ok=True)

# 📦 Initialize API
ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format='pandas')

# 📄 Load all symbols
symbols_df = pd.read_csv("data/unique_nasdaq_symbols.csv")
symbols = symbols_df["Symbol"].dropna().unique().tolist()

# 📦 Only download those not yet cached
already_downloaded = set(f.replace(".csv", "") for f in os.listdir(cache_dir) if f.endswith(".csv"))
to_download = [s for s in symbols if s not in already_downloaded]

# ⏱️ Download max 25 per run (Alpha Vantage limit)
batch = to_download[:25]
print(f"🚀 Starting batch of {len(batch)} symbols...")

def download_alpha(symbol):
    try:
        data, meta = ts.get_daily(symbol=symbol, outputsize='full')
        data.reset_index(inplace=True)
        data.rename(columns={"date": "Date"}, inplace=True)
        path = os.path.join(cache_dir, f"{symbol}.csv")
        data.to_csv(path, index=False)
        print(f"✅ {symbol} saved to {path}")
        return True
    except Exception as e:
        print(f"❌ {symbol} failed: {e}")
        return False

# 🔁 Loop with spacing to avoid errors (extra safe)
for i, symbol in enumerate(batch):
    download_alpha(symbol)
    if i < len(batch) - 1:
        time.sleep(15)

print("🎉 Done for today. Run again tomorrow to continue.")
