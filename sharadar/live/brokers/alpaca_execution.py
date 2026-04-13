from alpaca.trading.enums import OrderType, TimeInForce

class AlpacaOrderStyle:
    def __init__(self, order_type=OrderType.MARKET, time_in_force=TimeInForce.GTC, limit_price=None, stop_price=None):
        self._order_type = order_type
        self._time_in_force = time_in_force
        self._limit_price = limit_price
        self._stop_price = stop_price

    def get_order_type(self):
        return self._order_type

    def get_time_in_force(self):
        return self._time_in_force

    def get_limit_price(self, _is_buy):
        return self._limit_price

    def get_stop_price(self, _is_buy):
        return self._stop_price

class AlpacaMarketOrder(AlpacaOrderStyle):
    def __init__(self, time_in_force=TimeInForce.GTC):
        super().__init__(OrderType.MARKET, time_in_force)

class AlpacaLimitOrder(AlpacaOrderStyle):
    def __init__(self, limit_price, time_in_force=TimeInForce.GTC):
        super().__init__(OrderType.LIMIT, time_in_force, limit_price=limit_price)

class AlpacaStopOrder(AlpacaOrderStyle):
    def __init__(self, stop_price, time_in_force=TimeInForce.GTC):
        super().__init__(OrderType.STOP, time_in_force, stop_price=stop_price)

class AlpacaStopLimitOrder(AlpacaOrderStyle):
    def __init__(self, limit_price, stop_price, time_in_force=TimeInForce.GTC):
        super().__init__(OrderType.STOP_LIMIT, time_in_force, limit_price=limit_price, stop_price=stop_price)
