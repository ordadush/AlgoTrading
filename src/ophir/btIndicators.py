import backtrader as bt
import numpy as np
import pandas as pd

# premises: data0 = stockFeed, data1 = marketFeed
class BetaIndex(bt.Indicator):
    """
    An OPTIMIZED version of the BetaIndex indicator.
    It calculates all values in a single pass to avoid redundant data fetching.
    """
    
    lines = ('beta_up', 'beta_down', 'beta_index', 'beta_index_recent',)
    params = (
        ('period', 360),
        ('short_window', 20),
        ('func', lambda b_up, b_down, n_up, n_down, p: 
                 (n_up * b_up - n_down * b_down) / p if p > 0 else 0.0),
    )

    def __init__(self):
        # Add a check to ensure short_window is not larger than period
        if self.p.short_window > self.p.period:
            raise ValueError("short_window cannot be greater than period")

    def _calculate_beta(self, stock_returns, market_returns):
        """Helper function to calculate a single beta value from pandas Series."""
        if len(market_returns) < 2:
            return 0.0
        market_variance = market_returns.var()
        if market_variance == 0:
            return 0.0
        covariance = stock_returns.cov(market_returns)
        beta = covariance / market_variance
        return beta

    def next(self):
        """
        Main calculation loop. This version is fully optimized.
        It fetches data once and performs all calculations on the same DataFrame.
        """
        # We only need to run if we have enough data for the long window
        if len(self.data0) < self.p.period:
            return

        # 1. Fetch data ONCE for the long window
        stock_close_window = self.data0.close.get(size=self.p.period)
        market_close_window = self.data1.close.get(size=self.p.period)

        # 2. Create the main DataFrame
        df = pd.DataFrame({
            'stock_close': stock_close_window,
            'market_close': market_close_window
        })
        
        # 3. Calculate log returns for the entire window
        df['stock_ret'] = np.log(df['stock_close'] / pd.Series(df['stock_close']).shift(1))
        df['market_ret'] = np.log(df['market_close'] / pd.Series(df['market_close']).shift(1))

        # --- A. CALCULATE FOR THE LONG-TERM WINDOW ---
        
        # 4a. Create mask and filter for the long window (uses the whole df)
        is_market_up_long = df['market_ret'] > 0
        up_stock_long = df.loc[is_market_up_long, 'stock_ret']
        up_market_long = df.loc[is_market_up_long, 'market_ret']
        down_stock_long = df.loc[~is_market_up_long, 'stock_ret']
        down_market_long = df.loc[~is_market_up_long, 'market_ret']

        # 5a. Calculate long-term betas and index
        b_up_long = self._calculate_beta(up_stock_long, up_market_long)
        b_down_long = self._calculate_beta(down_stock_long, down_market_long)
        index_long = self.p.func(
            b_up_long, b_down_long, len(up_market_long), len(down_market_long), self.p.period
        )

        # --- B. CALCULATE FOR THE SHORT-TERM WINDOW ---

        # 4b. Create a smaller DataFrame for the short window by taking the 'tail'.
        # This REUSES the existing data and avoids fetching again.
        df_short = df.tail(self.p.short_window)
        
        # 5b. Create mask and filter for the short window
        is_market_up_short = df_short['market_ret'] > 0
        up_stock_short = df_short.loc[is_market_up_short, 'stock_ret']
        up_market_short = df_short.loc[is_market_up_short, 'market_ret']
        down_stock_short = df_short.loc[~is_market_up_short, 'stock_ret']
        down_market_short = df_short.loc[~is_market_up_short, 'market_ret']

        # 6b. Calculate short-term betas and index
        b_up_short = self._calculate_beta(up_stock_short, up_market_short)
        b_down_short = self._calculate_beta(down_stock_short, down_market_short)
        index_short = self.p.func(
            b_up_short, b_down_short, len(up_market_short), len(down_market_short), self.p.short_window
        )
        
        # --- C. SET ALL OUTPUT LINES ---
        self.lines.beta_up[0] = b_up_long
        self.lines.beta_down[0] = b_down_long
        self.lines.beta_index[0] = index_long
        self.lines.beta_index_recent[0] = index_short
