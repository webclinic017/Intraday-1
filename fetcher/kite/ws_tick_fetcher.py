from kiteconnect import KiteTicker
from conf.local_conf import wss_auth_token, username
from cache.aerospike import aero_client, get_ltp_key
from utils.log import logger_instance
from conf.nifty_stocks import nifty_stock_list

logger = logger_instance
import sys

nifty_instrument_token = 256265

instrument_list = [x['instrumenttoken'] for x in nifty_stock_list]

instrument_list.append(nifty_instrument_token)


class KT(KiteTicker):
    def _create_connection(self, url, **kwargs):
        url = 'wss://ws.zerodha.com/?api_key=kitefront&user_id={}&enctoken={}&uid=1624852235716&user-agent=kite3-web&version=2.9.1'.format(
            username, wss_auth_token)
        super(KT, self)._create_connection(url, **kwargs)


def on_ticks(ws, ticks):  # noqa
    # logger.debug(ticks)
    for i in ticks:
        key  = get_ltp_key(i['instrument_token'])
        aero_client.put(key, i['last_price'])


def on_connect(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(instrument_list)
    ws.set_mode(ws.MODE_LTP, instrument_list)


def start_stream():
    kws = KT("", "")
    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect()


def main():
    start_stream()


if __name__ == '__main__':
    main()
