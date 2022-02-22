import requests
import time, datetime, sys

from conf.local_conf import auth_token, username
from utils.log import logger_instance
from cache.aerospike import aero_client

logger = logger_instance


class Order(object):
    def __init__(self):
        self.initiate_buffer = 0
        self.stoploss_buffer = 0
        self.headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json, text/plain, */*',
            'Authorization': 'enctoken {}'.format(auth_token),
            'Accept-Language': 'en-us',
            'Accept-Encoding': 'gzip, deflate, br',
            'Host': 'kite.zerodha.com',
            'Origin': 'https://kite.zerodha.com',
            'Content-Length': '240',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.4 Safari/605.1.15',
            'Referer': 'https://kite.zerodha.com/chart/web/ciq/INDICES/NIFTY%2050/256265',
            'Connection': 'keep-alive',
            'X-Kite-Version': '2.9.2',
            'X-Kite-Userid': username,
        }

    def place_order(self, trading_symbol, transaction_type, quantity):
        data = {
            'variety': 'regular',
            'exchange': 'NFO',
            'tradingsymbol': trading_symbol,
            'transaction_type': transaction_type,
            'order_type': 'MARKET',
            'quantity': quantity,
            'price': '0',
            'product': 'MIS',
            'validity': 'DAY',
            'disclosed_quantity': '0',
            'trigger_price': '0',
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
        logger.debug("Position attempted Status:{}, Response:{}".format(response.status_code, response.content))

    def initiate_position_with_level(self, instrument, transaction_type, level, position_type, trading_symbol,
                                     quantity=50):
        logger.debug(
            "Initiate {}-{} Position called for {} for {} quantity".format(position_type, transaction_type, instrument,
                                                                           quantity))

        logger.info("Starting price check for level {}".format(level))
        while True:
            # wait for 200 ms
            time.sleep(0.5)
            if transaction_type == "SELL":
                last_price = aero_client.get(instrument)['last_price']
                if last_price < level - self.initiate_buffer:
                    pass
                logger.info("Waiting for price {} to reach level {} for {}-{} for {} for {}".format(last_price, level,
                                                                                                    position_type,
                                                                                                    transaction_type,
                                                                                                    trading_symbol,
                                                                                                    quantity))
            if transaction_type == "BUY":
                last_price = aero_client.get(instrument)['last_price']
                if last_price > level + self.initiate_buffer:
                    pass

                logger.info("Waiting for price {} to reach level {} for {}-{} for {} for {}".format(last_price, level,
                                                                                                    position_type,
                                                                                                    transaction_type,
                                                                                                    trading_symbol,
                                                                                                    quantity))


def main():
    instrument = sys.argv[1]  # 256265
    transaction_type = sys.argv[2]
    level = sys.argv[3]
    position_type = sys.argv[4]
    trading_symbol = sys.argv[5]
    quantity = sys.argv[6]
    # position_types = OPEN, SL, TP
    o = Order()
    o.initiate_position_with_level(instrument, transaction_type, int(level), position_type, trading_symbol, quantity)


# 256265 BUY 15752 OPEN NIFTY21JUL15750CE 50
# 256265 SELL 15752 OPEN NIFTY21JUL15750CE 50
if __name__ == '__main__':
    main()
