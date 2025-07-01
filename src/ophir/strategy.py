import backtrader as bt
import pandas as pd
import datetime
from ophir.utils import *
from ophir.btIndicators import BetaIndex
import sys, os; sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#TODO: 1. commitions + cheap stocks filtering 2. filter liquidity: dont want to
class MyStrategy(bt.Strategy):
    """Strategy rules
        
        bt (_type_): _description_
    """
    params = (
        ('beta_period', 360),
        ('beta_short_window', 20),
        ('end_dates', None),
    )    
    
    def __init__(self):
        """
        Prepares the strategy's working environment. Runs only once at the start.
        """
        print("--- Initializing Multi-Stock Strategy ---")

        # Step 1: Identify the market index feed.
        self.market_feed = self.datas[-1] #Market feed enters last.
        print(f"Market feed identified: {self.market_feed._name}")

        self.beta_indicators = {}
        self.orders_by_stock = {}
        for d in self.datas[:-1]:
            stock_symbol = d._name
            # Create a BetaIndex indicator for the current stock (d) 
            # against the common market feed (self.market_feed),
            # and store it in the dictionary with the stock symbol as the key.
            self.beta_indicators[stock_symbol] = BetaIndex(
                d,                      # The specific stock feed
                self.market_feed,       # The common market feed for all stocks
                period=self.p.beta_period,
                short_window=self.p.beta_short_window
            )
            # Initialize the placeholder for order tracking for this specific stock
            self.orders_by_stock[stock_symbol] = None
            print(f" > Initialized BetaIndex for stock: {stock_symbol}")
            
        print("--- Strategy Initialization Complete ---")
  
    def next(self):
        print(self.datetime.date(0),'Portfolio value:', self.broker.getvalue())
        for d in self.datas[:-1]:
            stock_symbol = d._name
            if self.orders_by_stock.get(stock_symbol):
                continue
            if pd.isna(d.close[0]): # If today's data is invalid (NaN), skip
                continue
            position = self.getposition(d)
            #position
            if position:
                end_date_for_stock = self.p.end_dates.get(stock_symbol)
                current_date = d.datetime.date(0)
                # Exit Condition 1: Forced sell on the stock's last day of data
                if end_date_for_stock and current_date == end_date_for_stock.date():
                    print(f'--- FORCED SELL (End of Data) for {stock_symbol} on {current_date} ---')
                    self.sell(data=d)
                    continue  # Done with this stock for today
                # Exit Condition 2: Regular sell based on indicator signal
                # sell
                if len(d) > self.p.beta_period:
                    beta_val_recent = self.beta_indicators[stock_symbol].beta_index_recent[0]
                    if 1 < 0:
                        print(f'{d.datetime.date(0)} - SELL SIGNAL for {stock_symbol}, Recent Beta Index: {beta_val_recent:.2f}')
                        self.orders_by_stock[stock_symbol] = self.sell(data=d)   #stop loss             
                continue
            if len(d) <= self.p.beta_period:
                continue
            # BUY
            betaIdx = self.beta_indicators[stock_symbol].beta_index[0]
            betaIdxRecent = self.beta_indicators[stock_symbol].beta_index_recent[0]
            if (betaIdx > 0.5 and betaIdxRecent > 0.5):
                print(f'{d.datetime.date(0)} - BUY SIGNAL for {stock_symbol}, Beta Index: {betaIdx:.2f}, Recent Beta Index: {betaIdxRecent:.2f}')
                self.orders_by_stock[stock_symbol] = self.buy(data=d)
                self.sell(data=d, parent = self.orders_by_stock[stock_symbol], exectype=bt.Order.StopTrail, trailpercent=0.08)
                
                
    def notify_order(self, order):
        stock_symbol = order.data._name
        if order.status in [order.Submitted, order.Accepted]:
            # order is already in process
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                print(f'BUY EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
            elif order.issell():
                print(f'SELL EXECUTED for {stock_symbol} at {order.executed.price:.2f}')
        
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            print(f'Order for {stock_symbol} was Canceled/Margin/Rejected')

        #restarting the orders to enable new ones
        self.orders_by_stock[stock_symbol] = None
        
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        
        stock_symbol = trade.data._name
        print(f'--- TRADE CLOSED for {stock_symbol} ---')
        print(f'PROFIT/LOSS: Gross {trade.pnl:.2f}, Net {trade.pnlcomm:.2f}')
        
    def stop(self):
        print('--- Backtest Finished ---')
        final_value = self.broker.getvalue()
        print(f'Final Portfolio Value: {final_value:.2f}')
        print('--------------------------')
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
