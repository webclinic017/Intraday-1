from kiteconnect import KiteTicker
from conf.local_conf import username, auth_token
from utils.log import logger_instance
from conf.nifty_stocks import nifty_stock_list, nifty_expiry_strikes, nifty_indices_list
from urllib.parse import quote_plus
from cache.sqllite_cache import Sqllite
import sqlite3

logger = logger_instance

nifty_stocks = [x['instrumenttoken'] for x in nifty_stock_list]

nifty_indices = [x['instrumenttoken'] for x in nifty_indices_list]

instrument_list = nifty_indices + nifty_expiry_strikes + nifty_stocks


class KT(KiteTicker):
    sql = Sqllite()
    sql.init_ltp_db()

    def _create_connection(self, url, **kwargs):
        token = auth_token.split(" ")[1]
        wss_auth_token = quote_plus(token)
        url = 'wss://ws.zerodha.com/?api_key=kitefront&user_id={}&enctoken={}&uid=1624852235716&user-agent=kite3-web&version=2.9.1'.format(
            username, wss_auth_token)
        super(KT, self)._create_connection(url, **kwargs)


def on_ticks(ws, ticks):  # noqa
    logger.info("Sample Data: {}".format(ticks[0]))
    for i in ticks:
        key = str(i['instrument_token'])
        KT.sql.set_ltp(key, i['last_price'])


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
