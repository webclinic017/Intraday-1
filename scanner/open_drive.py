import datetime
import pandas as pd
from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc
from indicator.pivot import df_calculate_pivot_points
from utils.log import logger_instance

logger = logger_instance

# nifty_stock_list = [{"instrumenttoken": 3861249, "tradingsymbol": "ADANIPORTS"}]


nifty_stock_list = [{"instrumenttoken": 3861249, "tradingsymbol": "ADANIPORTS"},
                    {"instrumenttoken": 60417, "tradingsymbol": "ASIANPAINT"},
                    {"instrumenttoken": 1510401, "tradingsymbol": "AXISBANK"},
                    {"instrumenttoken": 4267265, "tradingsymbol": "BAJAJ-AUTO"},
                    {"instrumenttoken": 4268801, "tradingsymbol": "BAJAJFINSV"},
                    {"instrumenttoken": 81153, "tradingsymbol": "BAJFINANCE"},
                    {"instrumenttoken": 2714625, "tradingsymbol": "BHARTIARTL"},
                    {"instrumenttoken": 134657, "tradingsymbol": "BPCL"},
                    {"instrumenttoken": 177665, "tradingsymbol": "CIPLA"},
                    {"instrumenttoken": 5215745, "tradingsymbol": "COALINDIA"},
                    {"instrumenttoken": 225537, "tradingsymbol": "DRREDDY"},
                    {"instrumenttoken": 232961, "tradingsymbol": "EICHERMOT"},
                    {"instrumenttoken": 1207553, "tradingsymbol": "GAIL"},
                    {"instrumenttoken": 315393, "tradingsymbol": "GRASIM"},
                    {"instrumenttoken": 1850625, "tradingsymbol": "HCLTECH"},
                    {"instrumenttoken": 340481, "tradingsymbol": "HDFC"},
                    {"instrumenttoken": 341249, "tradingsymbol": "HDFCBANK"},
                    {"instrumenttoken": 345089, "tradingsymbol": "HEROMOTOCO"},
                    {"instrumenttoken": 348929, "tradingsymbol": "HINDALCO"},
                    {"instrumenttoken": 359937, "tradingsymbol": "HINDPETRO"},
                    {"instrumenttoken": 356865, "tradingsymbol": "HINDUNILVR"},
                    {"instrumenttoken": 7712001, "tradingsymbol": "IBULHSGFIN"},
                    {"instrumenttoken": 1270529, "tradingsymbol": "ICICIBANK"},
                    {"instrumenttoken": 1346049, "tradingsymbol": "INDUSINDBK"},
                    {"instrumenttoken": 7458561, "tradingsymbol": "INFRATEL"},
                    {"instrumenttoken": 408065, "tradingsymbol": "INFY"},
                    {"instrumenttoken": 415745, "tradingsymbol": "IOC"},
                    {"instrumenttoken": 424961, "tradingsymbol": "ITC"},
                    {"instrumenttoken": 492033, "tradingsymbol": "KOTAKBANK"},
                    {"instrumenttoken": 2939649, "tradingsymbol": "LT"},
                    {"instrumenttoken": 2672641, "tradingsymbol": "LUPIN"},
                    {"instrumenttoken": 519937, "tradingsymbol": "M&M"},
                    {"instrumenttoken": 2815745, "tradingsymbol": "MARUTI"},
                    {"instrumenttoken": 2977281, "tradingsymbol": "NTPC"},
                    {"instrumenttoken": 633601, "tradingsymbol": "ONGC"},
                    {"instrumenttoken": 3834113, "tradingsymbol": "POWERGRID"},
                    {"instrumenttoken": 738561, "tradingsymbol": "RELIANCE"},
                    {"instrumenttoken": 779521, "tradingsymbol": "SBIN"},
                    {"instrumenttoken": 857857, "tradingsymbol": "SUNPHARMA"},
                    {"instrumenttoken": 884737, "tradingsymbol": "TATAMOTORS"},
                    {"instrumenttoken": 895745, "tradingsymbol": "TATASTEEL"},
                    {"instrumenttoken": 2953217, "tradingsymbol": "TCS"},
                    {"instrumenttoken": 3465729, "tradingsymbol": "TECHM"},
                    {"instrumenttoken": 897537, "tradingsymbol": "TITAN"},
                    {"instrumenttoken": 2952193, "tradingsymbol": "ULTRACEMCO"},
                    {"instrumenttoken": 2889473, "tradingsymbol": "UPL"},
                    {"instrumenttoken": 784129, "tradingsymbol": "VEDL"},
                    {"instrumenttoken": 969473, "tradingsymbol": "WIPRO"},
                    {"instrumenttoken": 3050241, "tradingsymbol": "YESBANK"},
                    {"instrumenttoken": 975873, "tradingsymbol": "ZEEL"}]


def start_scanner():
    current_time = datetime.datetime.now()
    current_date = datetime.datetime.now().date()
    previous_date = current_date - datetime.timedelta(days=3)
    for stock in nifty_stock_list:
        previous_day_data = fetch_http_ohlc(stock=stock, from_date=previous_date, to_date=previous_date,
                                            period="day")
        current_15minute_data = fetch_http_ohlc(stock=stock, from_date=current_date, to_date=current_date,
                                                period="5minute")
        df_previous_day_data = pd.DataFrame(previous_day_data,
                                            columns=["date", "open", "high", "low", "close", "volume", "oi"])
        df_current_15minute_data = pd.DataFrame(current_15minute_data,
                                                columns=["date", "open", "high", "low", "close", "volume", "oi"])
        #pivot_points = df_calculate_pivot_points(df_previous_day_data)
        pdh = df_previous_day_data.iloc[0].high
        pdl = df_previous_day_data.iloc[0].low
        current_day_open = df_current_15minute_data.iloc[0].open
        current_day_high = df_current_15minute_data.iloc[0].high
        current_day_low = df_current_15minute_data.iloc[0].low
        current_day_close = df_current_15minute_data.iloc[0].close
        print(current_day_open, current_day_low, pdh, stock)

        #Open Drive Breakout
        if current_day_open == current_day_low and current_day_open > pdh:
            logger.info("Open Drive Buy Candidate {}".format(stock))
        if current_day_open == current_day_high and current_day_open < pdl:
            logger.info("Open Drive Sell Candidate {}".format(stock))

        # #Previous Day High Low Breakout
        # if current_day_open == current_day_low and current_day_close > pdh:
        #     logger.info("PDL Buy Candidate {}".format(stock))
        # if current_day_open == current_day_high and current_day_close < pdl:
        #     logger.info("PDL  Sell Candidate {}".format(stock))



start_scanner()

if __name__ == '__main__':
    start_scanner()