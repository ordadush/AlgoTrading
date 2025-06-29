import backtrader as bt
import numpy as np

class SupremeBeta(bt.Indicator):
    """
    Calculates three beta-related values:
    - beta_up: Beta calculated only on days the market was up.
    - beta_down: Beta calculated only on days the market was down.
    - supreme_beta: A custom function of beta_up and beta_down.
    """
    
    # 1. Define the output lines of the indicator
    lines = ('beta_up', 'beta_down', 'supreme_beta',)

    # 2. Define the parameters the indicator will accept
    params = (
        ('period', 60),  # 'w' - The lookback window
        # 'f' - The custom function. Default is a simple average.
        ('func', lambda up, down: (up + down) / 2.0),
    )

    def __init__(self):
        """Indicator initialization"""
        # data0 is the stock, data1 is the market (sp500_feed)
        # We need to calculate daily returns to compute beta
        self.stock_returns = self.data0.close / self.data0.close(-1) - 1
        self.market_returns = self.data1.close / self.data1.close(-1) - 1

    def _calculate_beta(self, stock_returns, market_returns):
        """
        Helper function to calculate beta from lists of returns.
        Beta = Covariance(stock, market) / Variance(market)
        """
        # Beta calculation requires at least 2 data points
        if len(market_returns) < 2:
            return 0.0  # Not enough data for calculation

        # Convert lists to numpy arrays for statistical calculations
        s_returns = np.array(stock_returns)
        m_returns = np.array(market_returns)

        # Calculate variance of the market. Handle the case of zero variance.
        market_variance = np.var(m_returns)
        if market_variance == 0:
            return 0.0  # Cannot divide by zero, no market movement

        # Calculate covariance between stock and market
        # The covariance matrix is [[var(s), cov(s,m)], [cov(m,s), var(m)]]
        covariance = np.cov(s_returns, m_returns)[0][1]

        # Calculate Beta
        beta = covariance / market_variance
        return beta

    def next(self):
        """Main calculation loop, runs on each bar."""
        # Wait until we have enough data for the full period
        if len(self.data0) < self.p.period:
            return

        # Prepare lists to hold returns for up-days and down-days
        stock_returns_up_days = []
        market_returns_up_days = []
        stock_returns_down_days = []
        market_returns_down_days = []

        # Loop backwards over the defined period (w)
        for i in range(self.p.period):
            # Get the market return for the i-th day back
            market_return_i = self.market_returns[-i]
            stock_return_i = self.stock_returns[-i]

            # Sort returns into up/down lists based on market performance
            # We consider market up if return > 0
            if market_return_i > 0:
                stock_returns_up_days.append(stock_return_i)
                market_returns_up_days.append(market_return_i)
            else:
                stock_returns_down_days.append(stock_return_i)
                market_returns_down_days.append(market_return_i)

        # Calculate beta for up-days and down-days using the helper function
        b_up = self._calculate_beta(stock_returns_up_days, market_returns_up_days)
        b_down = self._calculate_beta(stock_returns_down_days, market_returns_down_days)
        
        # Set the values for the output lines for the current bar
        self.lines.beta_up[0] = b_up
        self.lines.beta_down[0] = b_down
        
        # Apply the custom function 'f' to calculate supreme_beta
        self.lines.supreme_beta[0] = self.p.func(b_up, b_down)