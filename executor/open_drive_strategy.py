from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc
from indicator.pivot import df_calculate_pivot_points
from cache.aerospike import aero_client
import datetime
import pandas as pd
import time


def is_multiple_of_5(minute):
    if minute / 5 == 0:
        return True


def open_drive_execute(instrument):
    current_date = datetime.datetime.now().date()
    previous_date = current_date - datetime.timedelta(days=1)
    previous_day_data = fetch_http_ohlc(stock=instrument, from_date=previous_date, to_date=previous_date,
                                        period="day")
    df_previous_day_data = pd.DataFrame(previous_day_data,
                                        columns=["date", "open", "high", "low", "close", "volume", "oi"])
    pivot_points = df_calculate_pivot_points(df_previous_day_data)
    pdh = df_previous_day_data.iloc[0].high
    pdl = df_previous_day_data.iloc[0].low

    while True:
        current_time = datetime.datetime.now()
        if is_multiple_of_5(current_time.minute) and current_time.second > 10:
            # pick the last candle
            current_5minute_data = fetch_http_ohlc(stock=instrument, from_date=current_date, to_date=current_date,
                                                   period="5minute")[-1]
            df_current_5minute_data = pd.DataFrame(current_5minute_data,
                                                   columns=["date", "open", "high", "low", "close", "volume", "oi"])
            # if the last candle closes above the pdh then start reading live prices
            if df_current_5minute_data.iloc[0].close > pdh:
                while True:
                    time.sleep(0.2)
                    last_price = aero_client.get(instrument)['last_price']
                    if last_price > df_current_5minute_data.iloc[0].high:
                        print("BUYING")

