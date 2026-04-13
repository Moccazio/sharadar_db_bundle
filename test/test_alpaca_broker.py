import os
import unittest
import pandas as pd
from sharadar.live.brokers.alpaca_broker import AlpacaBroker

class Asset:
    def __init__(self, symbol):
        self.symbol = symbol

class TestAlpacaBroker(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        API_KEY = os.getenv("ALPACA_API_KEY", "your_api_key")
        API_SECRET = os.getenv("ALPACA_API_SECRET", "your_api_secret")
        cls.broker = AlpacaBroker(API_KEY, API_SECRET, paper=True)
        cls.asset = Asset("AAPL")

    def test_subscribe_to_market_data(self):
        self.broker.subscribe_to_market_data(self.asset)
        dt = self.broker.get_last_traded_dt(self.asset)
        self.assertIsInstance(dt, pd.Timestamp)

    def test_get_spot_value(self):
        self.broker.subscribe_to_market_data(self.asset)
        price = self.broker.get_spot_value(self.asset, 'price', pd.Timestamp.now(), 'minute')
        self.assertTrue(isinstance(price, float) or pd.isna(price))

    def test_order_and_cancel(self):
        class MarketOrderStyle:
            def get_limit_price(self, is_buy):
                return None
            def get_stop_price(self, is_buy):
                return None
        order = self.broker.order(self.asset, 1, MarketOrderStyle())
        self.assertIsNotNone(order)
        self.broker.cancel_order(order.id)

if __name__ == "__main__":
    unittest.main()
