#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Alpaca Broker implementation using the alpaca-py package.
"""
import threading
from collections import defaultdict
from time import sleep
import pandas as pd
import numpy as np
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest, StopOrderRequest, StopLimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType
from sharadar.live.brokers.broker import Broker
import logging

log = logging.getLogger("alpaca_broker")

class AlpacaBroker(Broker):
    def __init__(self, api_key, api_secret, paper=True):
        self.api_key = api_key
        self.api_secret = api_secret
        self.paper = paper
        self.trading_client = TradingClient(api_key, api_secret, paper=paper)
        self.data_client = StockHistoricalDataClient(api_key, api_secret)
        self._orders = {}
        self._transactions = {}
        self._subscribed_assets = []
        self.metrics_tracker = None
        self._bars = defaultdict(pd.DataFrame)
        self._positions = {}
        self._account = None
        self._portfolio = None
        self._time_skew = None
        self._update_account()
        self._update_positions()

    @property
    def subscribed_assets(self):
        return self._subscribed_assets

    def disconnect(self):
        # Alpaca is HTTP-based, no persistent connection to close
        pass

    def subscribe_to_market_data(self, asset):
        if asset not in self._subscribed_assets:
            self._subscribed_assets.append(asset)
            self._update_bars(asset)

    @property
    def positions(self):
        self._update_positions()
        return self._positions

    @property
    def portfolio(self):
        self._update_account()
        return self._portfolio

    @property
    def account(self):
        self._update_account()
        return self._account

    @property
    def time_skew(self):
        # Alpaca is HTTP-based, so time skew is negligible
        return pd.Timedelta(0)

    def order(self, asset, amount, style):
        symbol = str(asset.symbol)
        side = OrderSide.BUY if amount > 0 else OrderSide.SELL
        qty = abs(int(amount))
        order_req = None
        if hasattr(style, 'get_limit_price') and style.get_limit_price(amount > 0):
            order_req = LimitOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                limit_price=style.get_limit_price(amount > 0),
                time_in_force=TimeInForce.GTC
            )
        elif hasattr(style, 'get_stop_price') and style.get_stop_price(amount > 0):
            order_req = StopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                stop_price=style.get_stop_price(amount > 0),
                time_in_force=TimeInForce.GTC
            )
        else:
            order_req = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.GTC
            )
        alpaca_order = self.trading_client.submit_order(order_req)
        self._orders[alpaca_order.id] = alpaca_order
        return alpaca_order

    @property
    def orders(self):
        self._update_orders()
        return self._orders

    @property
    def transactions(self):
        # Not implemented: Alpaca does not provide transaction objects directly
        return self._transactions

    def cancel_order(self, order_id):
        self.trading_client.cancel_order_by_id(order_id)

    def get_last_traded_dt(self, asset):
        self._update_bars(asset)
        bars = self._bars[str(asset.symbol)]
        if not bars.empty:
            return bars.index[-1]
        return pd.NaT

    def get_spot_value(self, asset, field, dt, data_frequency):
        self._update_bars(asset)
        bars = self._bars[str(asset.symbol)]
        if bars.empty:
            return np.NaN
        if field == 'price':
            return bars['close'].iloc[-1]
        elif field == 'open':
            return bars['open'].iloc[-1]
        elif field == 'close':
            return bars['close'].iloc[-1]
        elif field == 'high':
            return bars['high'].iloc[-1]
        elif field == 'low':
            return bars['low'].iloc[-1]
        elif field == 'volume':
            return bars['volume'].iloc[-1]
        elif field == 'last_traded':
            return bars.index[-1]
        return np.NaN

    def get_realtime_bars(self, assets, frequency):
        freq = TimeFrame.Day if frequency == '1d' else TimeFrame.Minute
        dfs = []
        for asset in assets:
            self._update_bars(asset, freq)
            bars = self._bars[str(asset.symbol)]
            if not bars.empty:
                bars.columns = pd.MultiIndex.from_product([[asset], bars.columns])
                dfs.append(bars)
        if dfs:
            return pd.concat(dfs, axis=1)
        return pd.DataFrame()

    def _update_bars(self, asset, freq=TimeFrame.Minute):
        symbol = str(asset.symbol)
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=freq,
            limit=100
        )
        bars = self.data_client.get_stock_bars(request_params).df
        if not bars.empty:
            self._bars[symbol] = bars

    def _update_positions(self):
        positions = self.trading_client.get_all_positions()
        self._positions = {p.symbol: p for p in positions}

    def _update_account(self):
        self._account = self.trading_client.get_account()
        # Portfolio logic can be expanded as needed
        self._portfolio = self._account
