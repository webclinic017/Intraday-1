from kiteconnect import KiteTicker
from conf.local_conf import wss_auth_token
from cache.aerospike import aero_client
from utils.log import logger_instance
logger = logger_instance
import sys


class KT(KiteTicker):
    def _create_connection(self, url, **kwargs):
        url = 'wss://ws.zerodha.com/?api_key=kitefront&user_id=YS5813&enctoken={}&uid=1624852235716&user-agent=kite3-web&version=2.9.1'.format(wss_auth_token)
        super(KT, self)._create_connection(url, **kwargs)


def on_ticks(ws, ticks):  # noqa
    logger.debug(ticks)
    key = str(instrument_token)
    aero_client.put(key, ticks[0])


def on_connect(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe([instrument_token])
    ws.set_mode(ws.MODE_FULL, [instrument_token])


def start_stream():
    kws = KT("", "")
    # Assign the callbacks.
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect()


def main():
    start_stream()


instrument_token = 256265

if __name__ == '__main__':
    main()
