import time

import pandas as pd
from ibapi.order_state import OrderState
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.execution import Execution
#from sharadar.live.brokers.ib_broker import *
from sharadar.live.brokers.ib_broker import TWSConnection, IBBroker
from itertools import zip_longest

import pandas as pd
from sharadar.util import performance
from sharadar.util.events import date_rules
from sharadar.util.logger import BacktestLogger 
from sharadar.util.run_algo import run_algorithm
from zipline.algorithm import time_rules
from zipline.api import (
    order_target_percent,
    schedule_function,
    sid
)


log = BacktestLogger('__file__')

import talib

def initialize(context):
    context.begin = time.time()
    context.security = sid(118267)  # ISHARES RUSSELL 3000 ETF
    context.small = 30
    context.large = 70

    schedule_function(rebalance,
                      date_rules.week_start(),
                      time_rules.market_open())


def rebalance(context, data):
    prices = data.history(context.security, 'price', 100, '1d')
    macd, signal, hist = talib.MACD(prices)

    if macd[-1] > signal[-1]:
        order_target_percent(context.security, 1)
    else:
        order_target_percent(context.security, -1)


def analyze(context, perf):
    duration = (time.time() - context.begin)
    log.info('Backtest executed in %s' % (time.strftime("%H:%M:%S", time.gmtime(duration))))
    performance.analyze(perf, '__file__', __doc__, duration)


def run_this_algorithm():
    tws_uri = "localhost:7497:0"
    account_id = "your_account_id"
    broker = IBBroker(tws_uri, account_id) # call 
    run_algorithm(initialize=initialize,
                  start=pd.Timestamp('2020-01-05'),
                  analyze=analyze, 
                  broker=broker,
                  )
    
if __name__ == "__main__":
    # execute only if run as a script
    run_this_algorithm()
