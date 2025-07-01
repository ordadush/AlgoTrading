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
        # --- CHANGE STARTS HERE ---
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
    
    # --- 3. Optional plotting (No changes here, this part is now in the main script) ---
    # ...

    return df

CHECKPOINT_DIR = "data_cache" # checkpoints
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
    # # recreate derived frames
    # df_train, df_val, df_test = split_dataframe_by_dates(df_main)
    # df_sp500_train, df_sp500_val, df_sp500_test = split_dataframe_by_dates(df_sp500)

    # # restore indexes exactly as in the build path
    # for _df in (df_train, df_sp500_train):
    #     _df['date'] = pd.to_datetime(_df['date'])
    #     _df.set_index('date', inplace=True)
    #     _df.sort_index(inplace=True)
    
    

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
    beta_df = calc_beta_grouped(df_train,250)
    beta_df.to_parquet(CP_BETA)
#%%
start_date = pd.to_datetime('2013-01-01')
end_date = pd.to_datetime('2021-01-01')
#small range sanity block
# first_20_symbols = df_main['symbol'].drop_duplicates().head(20).tolist()
# df_20_stocks = df_main[df_main['symbol'].isin(first_20_symbols)].sort_index()
# df_filtered_by_date = df_20_stocks[(df_20_stocks.index >= start_date) & (df_20_stocks.index <= end_date)]
# trading_days = df_sp500[(df_sp500.index >= start_date) & (df_sp500.index <= end_date)].index
# df_filtered_by_date = df_20_stocks.loc[start_date:end_date]
# df_filtered_by_date = df_train.loc[start_date:end_date]


#%%
#statistical analysis and presentation -  regular
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

beta_df['beta_index_ranked'] = beta_df.groupby('date')['beta_index'].rank(pct=True)
desc_stats_ranked = beta_df['beta_index_ranked'].describe()
print("beta index - ranked")
print(desc_stats_ranked)
print("\nPlotting distribution of RANKED Beta Index...")
plt.style.use('seaborn-v0_8-whitegrid')
fig, ax = plt.subplots(figsize=(14, 7))

sns.histplot(beta_df['beta_index_ranked'], bins=100, kde=False, ax=ax, label='Ranked Beta Index Distribution')

# 
ax.axvline(desc_stats_ranked['mean'], color='red', linestyle='--', linewidth=2, label=f"Mean: {desc_stats_ranked['mean']:.2f}")
ax.axvline(desc_stats_ranked['50%'], color='orange', linestyle='-', linewidth=2, label=f"Median (50%): {desc_stats_ranked['50%']:.2f}")
ax.axvline(desc_stats_ranked['25%'], color='green', linestyle=':', linewidth=2, label=f"25th Percentile: {desc_stats_ranked['25%']:.2f}")
ax.axvline(desc_stats_ranked['75%'], color='purple', linestyle=':', linewidth=2, label=f"75th Percentile: {desc_stats_ranked['75%']:.2f}")


# titles
ax.set_title('Distribution of Ranked Beta Index (Relative Strength)', fontsize=16)
ax.set_xlabel('Beta Index Ranked Value (0.0 to 1.0)', fontsize=12)
ax.set_ylabel('Frequency', fontsize=12)
ax.legend()
plt.tight_layout()
plt.show()

