import time
import gspread
from utils.log import logger_instance
from fetcher.kite.orders import Order

logger = logger_instance


def execute_order():
    gc = gspread.service_account()
    sh = gc.open("test")
    ws = sh.worksheets()[0]
    o = Order()
    update_flag = False
    while True:
        logger.info("Starting Poll on Sheet for Manual orders")
        ex_list = ws.get_values('A3:D11')
        for i in ex_list:
            if i[3] == '1':
                trading_symbol = i[0]
                quantity = int(i[1])
                transaction_type = i[2]
                o.place_order(trading_symbol, transaction_type, quantity)
                i[3] = '0'
                update_flag = True
        if update_flag:
            ws.update('A3:D11', ex_list)
            update_flag = False
        time.sleep(1.1)


def main():
    execute_order()


if __name__ == '__main__':
    main()
