import os
import unittest
import pandas as pd
from sharadar.live.brokers.alpaca_broker import AlpacaBroker
from sharadar.live.brokers.alpaca_execution import AlpacaMarketOrder
from sharadar.live.algorithm_live import LiveTradingAlgorithm

class Asset:
    def __init__(self, symbol):
        self.symbol = symbol
        self.price_multiplier = 1
    def __str__(self):
        return self.symbol

class TestLiveAlpacaAlgorithm(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        API_KEY = os.getenv("ALPACA_API_KEY", "your_api_key")
        API_SECRET = os.getenv("ALPACA_API_SECRET", "your_api_secret")
        cls.broker = AlpacaBroker(API_KEY, API_SECRET, paper=True)
        cls.state_file = "test_live_algo_state.pkl"

    def test_initialize_and_order(self):
        def initialize(context):
            context.asset = Asset("AAPL")
            context.has_ordered = False
        def handle_data(context, data):
            if not context.has_ordered:
                order_style = AlpacaMarketOrder()
                context.order(context.asset, 1, style=order_style)
                context.has_ordered = True
        algo = LiveTradingAlgorithm(
            initialize=initialize,
            handle_data=handle_data,
            broker=self.broker,
            state_filename=self.state_file,
            algo_filename="test_algo.py",
        )
        # We do not call algo.run() to avoid blocking; instead, test instantiation and setup
        self.assertIsNotNone(algo.broker)
        self.assertEqual(algo.state_filename, self.state_file)
        self.assertTrue(hasattr(algo, 'initialize'))
        self.assertTrue(hasattr(algo, 'handle_data'))

if __name__ == "__main__":
    unittest.main()
