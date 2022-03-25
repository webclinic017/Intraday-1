import time, datetime, sys
from fetcher.kite.orders import Order
from kiteconnect import KiteConnect
from utils.log import logger_instance
from cache.aerospike import aero_client, get_ltp_key
from cache.sqllite_cache import Sqllite

logging = logger_instance

# This script checks the current days's running MTM every 1-2 second and squares of all positions
# if the loss < X% of the capital or profit is > X% of the capital
# it gets the position only once every minute from broker but gets the live LTP from websockets to calculate MTM
# it also considers an exclude list to exclude from MTM calculation for positions belonging to diff strategy

o = Order()


def square_off_positions(open_buy_positions, open_sell_positions, sl):
    for pos in open_buy_positions:
        logging.info("Closing all open BUY positions as SL/TP of {} is hit".format(sl))
        tradingsymbol = pos['tradingsymbol']
        transaction_type = KiteConnect.TRANSACTION_TYPE_SELL
        quantity = abs(pos['quantity'])
        exchange = KiteConnect.EXCHANGE_NFO
        o.place_order(tradingsymbol, transaction_type=transaction_type, quantity=quantity,
                      exchange=exchange)

    for pos in open_sell_positions:
        logging.info("Closing all open SELL positions as SL/TP of {} is hit".format(sl))
        tradingsymbol = pos['tradingsymbol']
        transaction_type = KiteConnect.TRANSACTION_TYPE_BUY
        quantity = abs(pos['quantity'])
        exchange = KiteConnect.EXCHANGE_NFO
        o.place_order(tradingsymbol, transaction_type=transaction_type, quantity=quantity,
                      exchange=exchange)


def calculate_running_profit_loss():
    exclude_symbol_list = []
    capital = 250000
    sl_percent = 0.01
    tp_percent = 0.015
    sl = capital * sl_percent * -1
    # tp = capital * tp_percent
    tp = None
    sql = Sqllite()
    sql.init_ltp_db()
    pos = o.get_positions().json()
    while True:
        dat = datetime.datetime.now()
        # call positions api once a minute only
        if dat.second == 59:
            # clear position here so that old position is not used
            pos = None
            pos = o.get_positions().json()
            time.sleep(1)
        if pos:
            total_pnl = 0
            open_sell_positions = []
            open_buy_positions = []
            for x in pos['data']['day']:
                if x['tradingsymbol'] in exclude_symbol_list:
                    continue
                # key = get_ltp_key(x['instrument_token'])
                # ltp = aero_client.get(key)
                ltp = sql.get_ltp(x['instrument_token'],100000)
                pnl = (x['sell_value'] - x['buy_value']) + (x['quantity'] * ltp * x['multiplier'])
                if x['quantity'] < 0:
                    open_sell_positions.append(x)
                if x['quantity'] > 0:
                    open_buy_positions.append(x)
                total_pnl += pnl
            total_pnl = round(total_pnl)
            logging.info("Total PNL {}".format(total_pnl))
            # Close all open positions if loss/profit is around X% of capital
            if total_pnl < sl:
                square_off_positions(open_buy_positions, open_sell_positions, sl)
                break
            if tp and total_pnl > tp:
                square_off_positions(open_buy_positions, open_sell_positions, tp)
                break
        else:
            logging.info("Error getting positions")
        # if you change sleep then change 59 to even number above in the loop
        time.sleep(1)


if __name__ == '__main__':
    calculate_running_profit_loss()
