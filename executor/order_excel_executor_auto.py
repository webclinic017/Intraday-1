import time
import gspread
from utils.log import logger_instance
from fetcher.kite.orders import Order
from cache.aerospike import aero_client, get_ltp_key
import datetime
from kiteconnect import KiteConnect
from cache.sqllite_cache import Sqllite

logger = logger_instance


def execute_order():
    gc = gspread.service_account()
    sh = gc.open("order")
    ws = sh.worksheets()[1]
    o = Order()
    sql = Sqllite()
    sql.init_ltp_db()
    update_flag = False
    while True:
        try:
            logger.debug("Polling on Sheet for Auto orders")
            ex_list = ws.get_values('A3:I26')
            for i in ex_list:
                if i[6] == '1':
                    trading_symbol = i[0]
                    quantity = int(i[1])
                    transaction_type = i[2]
                    instrument = i[3]
                    comparator = i[4]
                    trigger = float(i[5])
                    ltp = sql.get_ltp(instrument)
                    logger.debug("LTP: {} for {}:".format(ltp, trading_symbol))

                    if comparator == ">":
                        if ltp > trigger:
                            logger.debug(
                                "{} as LTP: {} greater the trigger price: {} for {}".format(transaction_type, ltp,
                                                                                            trigger,
                                                                                            trading_symbol))
                            o.place_order(trading_symbol, transaction_type, quantity, exchange=KiteConnect.EXCHANGE_NFO)
                            i[7] = ltp  # store ltp
                            i[8] = str(datetime.datetime.now())
                            # cancel all upcoming trades after an order gets placed - safety net
                            # for j in ex_list:
                            #     j[6] = '0'
                            i[6] = '0'
                            update_flag = True
                    elif comparator == "<":
                        if ltp < trigger:
                            logger.debug(
                                "{} as LTP: {} smaller the trigger price: {} for {}".format(transaction_type, ltp,
                                                                                            trigger,
                                                                                            trading_symbol))
                            o.place_order(trading_symbol, transaction_type, quantity, exchange=KiteConnect.EXCHANGE_NFO)
                            i[7] = ltp  # store ltp
                            i[8] = str(datetime.datetime.now())
                            # cancel all upcoming trades after an order gets placed - safety net
                            # for j in ex_list:
                            #     j[6] = '0'
                            i[6] = '0'
                            update_flag = True
            if update_flag:
                ws.update('A3:I26', ex_list)
                update_flag = False
            time.sleep(2)
        except Exception as e:
            logger.exception("Exception detected - cancelling all open orders {}".format(e))
            for j in ex_list:
                j[6] = '0'
            ws.update('A3:I26', ex_list)
            time.sleep(5)
            logger.debug("Sleeping for {} seconds due to exception".format(5))


def main():
    execute_order()


if __name__ == '__main__':
    main()
