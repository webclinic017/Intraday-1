from kiteconnect import KiteTicker
from conf.local_conf import wss_auth_token, username
from cache.aerospike import aero_client, get_ltp_key
from utils.log import logger_instance
from conf.nifty_stocks import nifty_stock_list

logger = logger_instance
import sys

nifty_instrument_token = [256265]

strike_list = [
    13807618,
    13807874,
    13808130,
    13808386,
    13808642,
    13808898,
    13809154,
    13809410,
    13810178,
    13810434,
    13813250,
    13813506,
    13813762,
    13814018,
    13814274,
    13814530,
    13817858,
    13818114,
    13818370,
    13818626,
    13821954,
    13822210,
    13826050,
    13826306,
    13826562,
    13826818,
    13827074,
    13827330,
    13827586,
    13827842,
    13828866,
    13829122,
    13830146,
    13830402,
    13833218,
    13833986,
    13840642,
    13840898,
    13841154,
    13842946,
    13845762,
    13848834,
    13850114,
    13851650
]

nifty_stocks = [x['instrumenttoken'] for x in nifty_stock_list]

#instrument_list = nifty_stocks + strike_list + nifty_instrument_token
#instrument_list = nifty_stocks  + nifty_instrument_token

instrument_list = nifty_instrument_token + strike_list



class KT(KiteTicker):
    def _create_connection(self, url, **kwargs):
        url = 'wss://ws.zerodha.com/?api_key=kitefront&user_id={}&enctoken={}&uid=1624852235716&user-agent=kite3-web&version=2.9.1'.format(
            username, wss_auth_token)
        super(KT, self)._create_connection(url, **kwargs)


def on_ticks(ws, ticks):  # noqa
    # logger.debug(ticks)
    for i in ticks:
        key = get_ltp_key(i['instrument_token'])
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
