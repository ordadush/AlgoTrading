import backtrader as bt
import pandas as pd
import datetime
class MyStrategy(bt.Strategy):
    """Strategy rules
        
        bt (_type_): _description_
    """
    params = (('beta_up_long', 360),
                ('beta_down_long',360),
                ('beta_up_short',30),
                ('beta_down_short',30)
    )
    def log(self, txt, dt = None):
        dt = dt or self.datas[0].datatime.date(0)
        print(f'{dt.isoformat()}, {txt}')
    def __init__(self):
