#בודק איזה סף פריצת ווליום אחרי דארווס הוא הכי טוב
#מסקנה: 1.3-1.5 הרבה פריצות לאחר יום אחד, 2.0 יותר אחוזי הצלחה יום למחרת.
import pandas as pd
import matplotlib.pyplot as plt

# טען את הקובץ שנשמר
df = pd.read_csv("data/volume_threshold_test_results.csv")

# ננקה שמות עמודות מרווחים
df.columns = df.columns.str.strip()

# ניתוח סף הווליום הכי אפקטיבי לפי:
# מספר הפריצות המוצלחות = (Win Rate%) × (Total Breakouts)
df["Successful Breakouts"] = (df["Win Rate (%)"] / 100) * df["Total Breakouts"]
grouped = df.groupby("Threshold").agg({
    "Total Breakouts": "sum",
    "Successful Breakouts": "sum",
    "Avg Profit (%)": "mean"
}).reset_index()

grouped["Win Rate (%)"] = (grouped["Successful Breakouts"] / grouped["Total Breakouts"]) * 100
best = grouped.sort_values("Successful Breakouts", ascending=False).head(1)

print("📊 ניתוח תוצאות מאוחד לפי סף ווליום:")
print(grouped)
print("\n⭐️ הסף הכי אפקטיבי לפי הכי הרבה הצלחות:\n", best)

# גרף Win Rate
plt.figure(figsize=(8, 5))
plt.plot(grouped["Threshold"], grouped["Win Rate (%)"], marker='o')
plt.title("🔼 אחוז הצלחה לפי סף ווליום")
plt.xlabel("סף ווליום")
plt.ylabel("אחוז הצלחה (%)")
plt.grid(True)
plt.tight_layout()
plt.show()

# גרף כמות פריצות
plt.figure(figsize=(8, 5))
plt.bar(grouped["Threshold"], grouped["Total Breakouts"], color='skyblue')
plt.title("📦 כמות פריצות לפי סף ווליום")
plt.xlabel("סף ווליום")
plt.ylabel("כמות פריצות")
plt.grid(True)
plt.tight_layout()
plt.show()

# גרף רווח ממוצע
plt.figure(figsize=(8, 5))
plt.plot(grouped["Threshold"], grouped["Avg Profit (%)"], marker='s', color='green')
plt.title("💰 רווח ממוצע לפי סף ווליום")
plt.xlabel("סף ווליום")
plt.ylabel("רווח ממוצע (%)")
plt.grid(True)
plt.tight_layout()
plt.show()
