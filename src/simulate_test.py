from trading_core import Trade, Account

# יצירת חשבון עם 100,000 דולר
account = Account()

# טרייד 1 – רווח
t1 = Trade("AAPL", "2023-06-01", 150, 145, 160)
account.open_trade(t1)
account.close_trade(t1, "2023-06-05", 158, "take profit")

# טרייד 2 – הפסד
t2 = Trade("MSFT", "2023-06-10", 320, 310, 340)
account.open_trade(t2)
account.close_trade(t2, "2023-06-14", 308, "stop loss")

# טרייד 3 – רווח
t3 = Trade("GOOG", "2023-06-20", 100, 95, 110)
account.open_trade(t3)
account.close_trade(t3, "2023-06-25", 109, "take profit")

# הדפסות
for trade in account.trades:
    print(trade)

print("\n📊 Summary:")
print(account.summary())
