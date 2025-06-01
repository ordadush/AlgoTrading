# ===== src/simulate_volume_thresholds.py =====
import pandas as pd
from DBintegration.database import engine
from trading_core import Trade, Account
from patterns.darvas_box import identify_darvas_boxes, detect_breakout_with_volume

query = """
SELECT date, high, low, close, volume, symbol
FROM daily_stock_data
WHERE split = 'train'
ORDER BY symbol, date
"""

df = pd.read_sql(query, con=engine)
df.columns = df.columns.str.lower()
df['date'] = pd.to_datetime(df['date'])

# 🧱 Grid of thresholds to test
volume_thresholds = [1.2, 1.3, 1.5, 1.7, 2.0]
results = []

for symbol in df['symbol'].unique():
    df_symbol = df[df['symbol'] == symbol].copy()
    df_symbol.set_index('date', inplace=True)

    for vt in volume_thresholds:
        total_breakouts = 0
        successful_breakouts = 0
        total_profit_pct = 0
        total_trades = 0

        boxes_df = identify_darvas_boxes(df_symbol, window=5)
        breakouts = detect_breakout_with_volume(df_symbol, boxes_df, volume_window=20)

        for _, b in breakouts.iterrows():
            breakout_date = b['breakout_date']
            if breakout_date not in df_symbol.index:
                continue

            vol_at_breakout = df_symbol.loc[breakout_date, 'volume']
            avg_vol = df_symbol['volume'].rolling(20).mean().loc[breakout_date]

            # ✅ Only enter if volume > threshold × average volume
            if vol_at_breakout <= vt * avg_vol:
                continue

            # Get next 5 days of price data
            try:
                next_index = df_symbol.index.get_loc(breakout_date) + 1
                future_prices = df_symbol.iloc[next_index: next_index + 5]
            except:
                continue

            if future_prices.empty:
                continue

            entry_price = b['breakout_price']
            stop_loss = entry_price * 0.97
            take_profit = entry_price * 1.05

            max_price = future_prices['high'].max()
            min_price = future_prices['low'].min()

            # Simulate trade outcome
            if max_price >= take_profit:
                exit_price = take_profit
                successful_breakouts += 1
            elif min_price <= stop_loss:
                exit_price = stop_loss
            else:
                exit_price = future_prices['close'].iloc[-1]

            profit_pct = ((exit_price - entry_price) / entry_price) * 100
            total_profit_pct += profit_pct
            total_breakouts += 1
            total_trades += 1

        # Store summary per threshold per symbol
        if total_breakouts > 0:
            results.append({
                "Symbol": symbol,
                "Threshold": vt,
                "Total Breakouts": total_breakouts,
                "Win Rate (%)": round((successful_breakouts / total_breakouts) * 100, 2),
                "Avg Profit (%)": round(total_profit_pct / total_trades, 2)
            })

# 💾 Save to CSV
pd.DataFrame(results).to_csv("data/volume_threshold_test_results.csv", index=False)
print("✅ Results saved to data/volume_threshold_test_results.csv")
