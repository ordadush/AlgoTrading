# src/indicators/asymmetric_beta.py

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

def compute_asymmetric_beta(df_stock, df_market, window=20):
    df = df_stock.merge(df_market, on="date", suffixes=("_stock", "_market"))
    df["ret_stock"] = df["close_stock"].pct_change()
    df["ret_market"] = df["close_market"].pct_change()

    beta_up_list = []
    beta_down_list = []
    dates = []

    for i in range(window, len(df)):
        window_df = df.iloc[i-window:i]
        up_days = window_df[window_df["ret_market"] > 0]
        down_days = window_df[window_df["ret_market"] < 0]

        def calc_beta(sub_df):
            X = sub_df["ret_market"].values.reshape(-1, 1)
            y = sub_df["ret_stock"].values
            if len(X) < 2:
                return np.nan
            model = LinearRegression().fit(X, y)
            return model.coef_[0]

        beta_up = calc_beta(up_days)
        beta_down = calc_beta(down_days)

        beta_up_list.append(beta_up)
        beta_down_list.append(beta_down)
        dates.append(df.iloc[i]["date"])

    return pd.DataFrame({
        "date": dates,
        "beta_up": beta_up_list,
        "beta_down": beta_down_list
    })
