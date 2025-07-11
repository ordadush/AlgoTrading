import pandas as pd
import matplotlib.pyplot as plt

# טען את קובץ הרווחים שנשמר
df = pd.read_csv('strategy_pnl.csv', parse_dates=['Date'])
df.set_index('Date', inplace=True)

# צייר גרף של ערך התיק לאורך זמן
plt.figure(figsize=(12, 6))
plt.plot(df.index, df['Portfolio_Value'], label='Portfolio Value', linewidth=2)
plt.title("Cumulative Profit/Loss Over Time")
plt.xlabel("Date")
plt.ylabel("Portfolio Value ($)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
