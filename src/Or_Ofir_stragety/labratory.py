#%%
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Assuming all user imports are correct and working
from DBintegration.models import DailyStockData, SP500Index
from DBintegration.db_utils import model_to_dataframe
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from tqdm import tqdm
import matplotlib.style as style
from ophir.utils import *
import plotly.express as px
import plotly.express as px
import plotly.io as pio
from pathlib import Path

def calculate_beta_index(df: pd.DataFrame, window: int = 250):
    """
    Calculates ‘beta_pos’, ‘beta_neg’ and ‘beta_index’ on a rolling window.
    If not enough historical data is available, it assigns NaN.
    """
    # --- 0. Normalise the date axis (No changes here) ---
    if 'date' in df.columns:
        df = df.copy()
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
    elif isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index)
    else:
        raise ValueError("DataFrame must have a 'date' column or a DatetimeIndex.")

    data = df[['return_daily', 'sp_return']]
    results = {'beta_pos': [], 'beta_neg': [], 'num_up': [], 'num_down': []}

    # --- 1. Rolling computations with data availability check ---
    for end in range(len(data)):
        # Check if there are enough preceding data points for a full window
        if end + 1 < window:
            # Not enough data, append NaN for all results
            results['beta_pos'].append(np.nan)
            results['beta_neg'].append(np.nan)
            results['num_up'].append(np.nan)
            results['num_down'].append(np.nan)
        else:
            # Enough data, proceed with calculation as before
            start = end + 1 - window
            win = data.iloc[start:end + 1]

            up = win[win['sp_return'] > 0]
            down = win[win['sp_return'] <= 0]

            def beta(block):
                if len(block) > 1 and block['sp_return'].var():
                    return block['return_daily'].cov(block['sp_return']) / block['sp_return'].var()
                return np.nan

            results['beta_pos'].append(beta(up))
            results['beta_neg'].append(beta(down))
            results['num_up'].append(len(up))
            results['num_down'].append(len(down))
        # --- CHANGE ENDS HERE ---

    # --- 2. Stitch results (No changes here) ---
    res = pd.DataFrame(results, index=data.index)
    df = pd.concat([df, res], axis=1)

    df['beta_index'] = (
        res['num_up'] * res['beta_pos'] - res['num_down'] * res['beta_neg']
    ) / window
    return df
######################################################################
CHECKPOINT_DIR = "data_cache" # checkpoints
STAT_DIR = "statistics"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
CP_MAIN = os.path.join(CHECKPOINT_DIR, "df_main.parquet")
CP_SP500 = os.path.join(CHECKPOINT_DIR, "df_sp500.parquet")
CP_BETA = os.path.join(CHECKPOINT_DIR, "beta_df.parquet")
use_cp = os.path.exists(CP_MAIN) and os.path.exists(CP_SP500) and os.path.exists(CP_BETA)
if use_cp:
    print("Loading data from checkpoints…")
    df_main  = pd.read_parquet(CP_MAIN)
    df_sp500 = pd.read_parquet(CP_SP500)
    beta_df = pd.read_parquet(CP_BETA)
    
else:
    #stage 0: gets the data from server. (working)
    print("Building data from database…")
    print("Loading daily stock data for all symbols:")
    df_main = model_to_dataframe(DailyStockData)
    df_train, df_val, df_test = split_dataframe_by_dates(df_main)
    df_train['date'] = pd.to_datetime(df_train['date'])
    df_train.set_index('date', inplace=True)
    df_train.sort_values(by=['symbol', 'date'], inplace=True)

    stocks_num = len(df_train)
    print(stocks_num) #here just to make sure it worked

    print("Loading snp data:")
    df_sp500 = model_to_dataframe(SP500Index)
    df_sp500_train, df_sp500_val, df_sp500_test = split_dataframe_by_dates(df_sp500)
    df_sp500_train['date'] = pd.to_datetime(df_sp500_train['date'])
    df_sp500_train.set_index('date', inplace=True)
    df_sp500_train.sort_index(inplace=True)
    df_main.to_parquet(CP_MAIN)
    df_sp500.to_parquet(CP_SP500)
    beta_df = calc_beta_grouped(df_train,250) #<----- Window
    beta_df.to_parquet(CP_BETA)
    
#====================================================================================    
#variense of beta_index
# if 'date' not in beta_df.columns:
#     # date is the index → reset
#     beta_df = beta_df.reset_index(names='date')

# beta_df['date'] = pd.to_datetime(beta_df['date'])
# beta_df = beta_df.sort_values(['symbol', 'date'])

# # --------------------------------------------------------------------
# # >>> 2.  per-symbol volatility (std-dev) of the β-index
# # --------------------------------------------------------------------
# vol_table = (
#     beta_df.groupby('symbol')['beta_index']
#            .std(ddof=0)                 # population σ
#            .rename('beta_vol')
#            .to_frame()
# )

# mean_vol   = vol_table['beta_vol'].mean()
# median_vol = vol_table['beta_vol'].median()

# print("\n=== β-index volatility per symbol ===")
# print(f"mean   σ(β) : {mean_vol:.4f}")
# print(f"median σ(β) : {median_vol:.4f}\n")
# print(vol_table.sort_values('beta_vol').head(10)
#                .to_string(float_format='%.4f'))

# # --------------------------------------------------------------------
# # >>> 3.  histogram for a quick visual check
# #      (shows automatically in VS-Code / Jupyter)
# # --------------------------------------------------------------------
# import matplotlib.pyplot as plt
# plt.figure(figsize=(9,5))
# plt.hist(vol_table['beta_vol'], bins=40, edgecolor='k', alpha=0.75)
# plt.axvline(mean_vol,   ls='--', c='red',    label=f"mean {mean_vol:.3f}")
# plt.axvline(median_vol, ls=':',  c='orange', label=f"median {median_vol:.3f}")
# plt.title("Distribution of β-index volatility (per symbol):250 days")
# plt.xlabel("σ(β-index)  over entire sample")
# plt.ylabel("Number of symbols")
# plt.legend()
# plt.tight_layout()
# plt.show()



g = beta_df.groupby('symbol')['beta_index']
stats = (
    pd.DataFrame({
        'sigma' : g.std(),
        'mean'  : g.mean(),
        'cv'    : g.std() / g.mean().abs(),
        'autocorr1': g.apply(lambda s: s.autocorr(1)),
    })
    .dropna()
)

# "alpha": avg return: 90 FWD days forward
FWD = 90
beta_df['ret_fwd'] = beta_df.groupby('symbol')['return_daily'].shift(-FWD).rolling(FWD).sum()

def alpha_on_extremes(sub):
    lo, hi = sub['beta_index'].quantile([0.05, 0.95])
    mask = (sub['beta_index'] < lo) | (sub['beta_index'] > hi)
    return sub.loc[mask, 'ret_fwd'].mean()

stats['alpha'] = beta_df.groupby('symbol').apply(alpha_on_extremes)

# ==== 2. שומרים ל-CSV ====
CSV_STATS = os.path.join(CHECKPOINT_DIR, "beta_stats_per_symbol.csv")
stats.to_csv(CSV_STATS)
print(f"symbol-level stats saved: {CSV_STATS}")

# ==== 3. תצוגה תלת-ממדית אינטראקטיבית ====
fig = px.scatter_3d(
    stats.reset_index(),
    x='sigma',
    y='cv',         # <-- הוחלף
    z='alpha',      # <-- הוחלף, זהו כעת הציר האנכי
    hover_name='symbol',
    title='Predictive Power (α) vs Stability (σ) vs CV: returns computed 90 days forward.'
)
#to run:start data_cache\beta_3d_plot_alpha.html
# עדכון כותרות הצירים כדי לשקף את השינוי
fig.update_layout(scene = dict(
                    xaxis_title='Sigma (σ) - Stability',
                    yaxis_title='CV - Relative Volatility',
                    zaxis_title='Alpha (α) - Predictive Power'))


HTML_PATH = os.path.join(CHECKPOINT_DIR, 'beta_3d_plot_alpha_vertical.html')
pio.write_html(fig, file=HTML_PATH, auto_open=False)
print(f"Interactive 3-D plot saved to: {HTML_PATH}")






















#%%
# statistical analysis and presentation -  regular
# desc_stats = beta_df['beta_index'].describe()
# print(desc_stats)


# print("\nPlotting distribution...")
# plt.style.use('seaborn-v0_8-whitegrid') # Using a nice style
# fig, ax = plt.subplots(figsize=(14, 7))

# # The main histogram plot
# sns.histplot(beta_df['beta_index'], bins=100, kde=True, ax=ax, label='Beta Index Distribution')

# # Adding vertical lines for key statistics
# ax.axvline(desc_stats['mean'], color='red', linestyle='--', linewidth=2, label=f"Mean: {desc_stats['mean']:.2f}")
# ax.axvline(desc_stats['50%'], color='orange', linestyle='-', linewidth=2, label=f"Median (50%): {desc_stats['50%']:.2f}")
# ax.axvline(desc_stats['25%'], color='green', linestyle=':', linewidth=2, label=f"25th Percentile: {desc_stats['25%']:.2f}")
# ax.axvline(desc_stats['75%'], color='purple', linestyle=':', linewidth=2, label=f"75th Percentile: {desc_stats['75%']:.2f}")

# # Formatting the plot
# ax.set_title('Distribution of Beta Index (Window = 250 Days)', fontsize=16)
# ax.set_xlabel('Beta Index Value', fontsize=12)
# ax.set_ylabel('Frequency', fontsize=12)
# ax.legend()
# plt.tight_layout()
# plt.show()


#statistical analysis ranked:
# beta_df['beta_index_ranked'] = beta_df.groupby('date')['beta_index'].rank(pct=True)
# desc_stats_ranked = beta_df['beta_index_ranked'].describe()
# print("beta index - ranked")
# print(desc_stats_ranked)
# print("\nPlotting distribution of RANKED Beta Index...")
# plt.style.use('seaborn-v0_8-whitegrid')
# fig, ax = plt.subplots(figsize=(14, 7))

# sns.histplot(beta_df['beta_index_ranked'], bins=100, kde=False, ax=ax, label='Ranked Beta Index Distribution')

# # 
# ax.axvline(desc_stats_ranked['mean'], color='red', linestyle='--', linewidth=2, label=f"Mean: {desc_stats_ranked['mean']:.2f}")
# ax.axvline(desc_stats_ranked['50%'], color='orange', linestyle='-', linewidth=2, label=f"Median (50%): {desc_stats_ranked['50%']:.2f}")
# ax.axvline(desc_stats_ranked['25%'], color='green', linestyle=':', linewidth=2, label=f"25th Percentile: {desc_stats_ranked['25%']:.2f}")
# ax.axvline(desc_stats_ranked['75%'], color='purple', linestyle=':', linewidth=2, label=f"75th Percentile: {desc_stats_ranked['75%']:.2f}")


# # titles
# ax.set_title('Distribution of Ranked Beta Index (Relative Strength)', fontsize=16)
# ax.set_xlabel('Beta Index Ranked Value (0.0 to 1.0)', fontsize=12)
# ax.set_ylabel('Frequency', fontsize=12)
# ax.legend()
# plt.tight_layout()
# plt.show()

