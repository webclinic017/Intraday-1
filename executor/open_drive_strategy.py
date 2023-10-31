from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc
from kiteconnect import KiteConnect
import datetime
import pandas as pd
import time
from fetcher.kite.orders import Order
from utils.log import logger_instance
from conf.nifty_stocks import nifty_stock_list
from cache.sqllite_cache import Sqllite
import time, sys

logging = logger_instance


def get_previous_day_data(instrument, current_date):
    # This method is used to account for previous days holidays
    pdh = None
    pdl = None
    df_previous_day_data = None
    for i in range(1, 4):
        previous_date = current_date - datetime.timedelta(days=i)

        previous_day_data = fetch_http_ohlc(stock=instrument, from_date=previous_date, to_date=previous_date,
                                            period="day")
        if not previous_day_data:
            continue
        else:
            df_previous_day_data = pd.DataFrame(previous_day_data,
                                                columns=["date", "open", "high", "low", "close", "volume", "oi"])
            # pivot_points = df_calculate_pivot_points(df_previous_day_data)
            pdh = df_previous_day_data.iloc[0].high
            pdl = df_previous_day_data.iloc[0].low
            break
    return df_previous_day_data, pdh, pdl


def open_drive_execute(instrument_list):
    current_date = datetime.datetime.now().date()
    o = Order()
    sql = Sqllite()
    sql.init_ltp_db()
    loop_flag = True
    while True:
        if not loop_flag:
            logging.info("Breaking out of the loop")
            break
        current_time = datetime.datetime.now()
        if current_time.hour == 9 and current_time.minute == 21:
            for instrument in instrument_list:
                loop_flag = False
                df_previous_day_data, pdh, pdl = get_previous_day_data(instrument, current_date)
                current_5minute_data = fetch_http_ohlc(stock=instrument, from_date=current_date, to_date=current_date,
                                                       period="5minute")
                df_current_5minute_data = pd.DataFrame(current_5minute_data,
                                                       columns=["date", "open", "high", "low", "close", "volume", "oi"])

                current_1minute_data = fetch_http_ohlc(stock=instrument, from_date=current_date, to_date=current_date,
                                                       period="minute")
                df_current_1minute_data = pd.DataFrame(current_1minute_data,
                                                       columns=["date", "open", "high", "low", "close",
                                                                "volume", "oi"])
                ltp = sql.get_ltp(instrument['instrumenttoken'])
                logging.info(
                    "PDH:{}, PDL:{}, Open 5min:{}, Close 5min:{}, Open 1min:{}, LTP:{} for  {}".format(pdh, pdl,
                                                                                                       df_current_5minute_data.iloc[
                                                                                                           0].close,
                                                                                                       df_current_5minute_data.iloc[
                                                                                                           0].open,
                                                                                                       df_current_1minute_data.iloc[
                                                                                                           5].open, ltp,
                                                                                                       instrument))
                # if the first 5 minute candle opens with low=close and is above pdh and
                # 6th 1 minute candle open is above the close of 1st 5 minute candle
                # candle to candle stop loss i.e close of first 5 minute candle is SL
                # we are only hunting for parabolic candles i.e one candle above the other
                if df_current_5minute_data.iloc[0].open == df_current_5minute_data.iloc[0].low and \
                        df_current_5minute_data.iloc[0].open > pdh and df_current_5minute_data.iloc[0].close < \
                        df_current_1minute_data.iloc[5].open < ltp:
                    response_main = o.place_order(instrument['tradingsymbol'],
                                                  transaction_type=KiteConnect.TRANSACTION_TYPE_BUY, quantity=1,
                                                  order_type=KiteConnect.ORDER_TYPE_MARKET,
                                                  exchange=KiteConnect.EXCHANGE_NSE)
                    logging.info(
                        "Buy Order Placement Status Response :{} for instrument {}".format(response_main.json(),
                                                                                           instrument))

                    response_sl = o.place_order(instrument['tradingsymbol'],
                                                transaction_type=KiteConnect.TRANSACTION_TYPE_SELL, quantity=1,
                                                order_type=KiteConnect.ORDER_TYPE_SLM,
                                                trigger_price=df_current_5minute_data.iloc[0].close,
                                                exchange=KiteConnect.EXCHANGE_NSE)
                    logging.info(
                        "Sell SL Order Placement Status Response :{} for instrument {}".format(response_sl.json(),
                                                                                               instrument))



                elif df_current_5minute_data.iloc[0].open == df_current_5minute_data.iloc[0].high and \
                        df_current_5minute_data.iloc[0].open < pdl and df_current_5minute_data.iloc[0].close > \
                        df_current_1minute_data.iloc[5].open > ltp:
                    response_main = o.place_order(instrument['tradingsymbol'],
                                                  transaction_type=KiteConnect.TRANSACTION_TYPE_SELL, quantity=1,
                                                  order_type=KiteConnect.ORDER_TYPE_MARKET,
                                                  trigger_price=df_current_5minute_data.iloc[0].close,
                                                  exchange=KiteConnect.EXCHANGE_NSE)
                    logging.info(
                        "Sell Order Placement Status Response :{} for instrument {}".format(response_main.json(),
                                                                                            instrument))
                    response_sl = o.place_order(instrument['tradingsymbol'],
                                                transaction_type=KiteConnect.TRANSACTION_TYPE_BUY, quantity=1,
                                                order_type=KiteConnect.ORDER_TYPE_SLM,
                                                trigger_price=df_current_5minute_data.iloc[0].close,
                                                exchange=KiteConnect.EXCHANGE_NSE)

                    logging.info(
                        "Buy SL Order Placement Status Response :{} for instrument {}".format(response_sl.json(),
                                                                                              instrument))
                else:
                    logging.info("Open Drive Criteria not satisfied for instrument {}".format(instrument))
        logging.info("Sleeping and waiting for 9:20am")
        time.sleep(2)


def main():
    logging.info("Starting Scan for OpenDrive")
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0],
                                                      "<start_index> <end_index>"))
        exit(1)
    start_index = int(sys.argv[1])
    end_index = int(sys.argv[2])
    open_drive_execute(nifty_stock_list[start_index:end_index])
    logging.info("Ending Scan for OpenDrive")


if __name__ == '__main__':
    main()
