import requests
import time, datetime, sys

from conf.local_conf import auth_token, username
from utils.log import logger_instance
from kiteconnect import KiteConnect

logger = logger_instance


class Order(object):
    def __init__(self):
        self.initiate_buffer = 0
        self.stoploss_buffer = 0
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json, text/plain, */*',
            'Authorization': '{}'.format(auth_token),
            'Accept-Language': 'en-us',
            'Host': 'kite.zerodha.com',
            'Origin': 'https://kite.zerodha.com',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15',
            'Referer': 'https://kite.zerodha.com/positions',
            'X-Kite-Version': '2.9.2',
            'X-Kite-Userid': username,
        }


    def get_positions(self):
        logger.info("Getting position details")
        url = 'https://kite.zerodha.com/oms/portfolio/positions'
        response = requests.get(url, headers=self.headers)
        logger.debug("Position Details  Status:{}, Response:{}".format(response.status_code, response.json()))
        if response.status_code == 200:
            return response

    def place_order(self, trading_symbol, transaction_type=KiteConnect.TRANSACTION_TYPE_BUY, quantity=0,
                    order_type=KiteConnect.ORDER_TYPE_MARKET, trigger_price=0,
                    exchange=KiteConnect.EXCHANGE_NSE, product=KiteConnect.PRODUCT_MIS):
        data = {
            'variety': 'regular',
            'exchange': exchange,
            'tradingsymbol': trading_symbol,
            'transaction_type': transaction_type,
            'order_type': order_type,
            'quantity': quantity,
            'price': '0',
            'product': product,
            'validity': 'DAY',
            'disclosed_quantity': '0',
            'trigger_price': trigger_price,
            'squareoff': '0',
            'stoploss': '0',
            'trailing_stoploss': '0',
            'user_id': username
        }
        logger.info(
            "Firing {} Position  for {} for {} quantity ".format(
                transaction_type,
                trading_symbol,
                quantity))
        response = requests.post('https://kite.zerodha.com/oms/orders/regular', headers=self.headers, cookies={},
                                 data=data)
        logger.debug("Position attempted Status:{}, Response:{}".format(response.status_code, response.json()))
        return response
