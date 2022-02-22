from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc
from indicator.pivot import df_calculate_pivot_points
from cache.aerospike import aero_client
import datetime
import pandas as pd
import time
from fetcher.kite.orders import Order
import talib
import sys, json, os
from dateutil.parser import parse
from fetcher.kite import instruments

current_date = datetime.datetime.now().date()
previous_date = current_date - datetime.timedelta(days=1)


def is_multiple_of_5(minute):
    if minute / 5 == 0:
        return True


def get_atm_strike(instrument, type='CE', df=None):
    current_5minute_data = fetch_http_ohlc(stock=instrument, from_date=current_date, to_date=current_date,
                                           period="5minute")[-1]

    price = current_5minute_data[4]  # closing price
    price = round(price / 100) * 100  # closing price rounding to 100th place

    sdf = df.loc[(df['type'] == type) & (df['strike'] == price)]
    return price, sdf.iloc[0]['instrument_token'], sdf.iloc[0]['tradingsymbol']


def get_itm_strike(price=None, distance=100, type='CE', df=None):
    if type == 'CE':
        price -= distance
        price = round(price / 100) * 100
    elif type == 'PE':
        price += distance
        price = round(price / 100) * 100

    sdf = df.loc[(df['type'] == type) & (df['strike'] == price)]
    return sdf.iloc[0]['instrument_token'], sdf.iloc[0]['tradingsymbol']


def get_otm_strike(price=None, distance=100, type='CE', df=None):
    if type == 'CE':
        price += distance
        price = round(price / 100) * 100
    elif type == 'PE':
        price -= distance
        price = round(price / 100) * 100

    sdf = df.loc[(df['type'] == type) & (df['strike'] == price)]
    return sdf.iloc[0]['instrument_token'], sdf.iloc[0]['tradingsymbol']


def check_conditions(instrument):
    current_5minute_data = fetch_http_ohlc(stock=instrument, from_date=previous_date, to_date=current_date,
                                           period="5minute")

    df_current_5minute_data = pd.DataFrame(current_5minute_data,
                                           columns=["date", "open", "high", "low", "close", "volume", "oi"])

    df_current_5minute_data.set_index('date')
    vwap_status = check_vwap(df_current_5minute_data)
    open_interest_status = check_open_interest(df_current_5minute_data)
    rsi_status = check_rsi(df_current_5minute_data)
    volume_status = check_volume(df_current_5minute_data)

    status = vwap_status and open_interest_status and rsi_status and volume_status
    return status


def check_vwap(df_current_5minute_data):
    # Vwap calculation should be for current day only, this is currently not right as its starting calculation from
    # previous day
    v = df_current_5minute_data['volume'].values
    tp = (df_current_5minute_data['low'] + df_current_5minute_data['close'] + df_current_5minute_data['high']).div(
        3).values
    df_current_5minute_data['vwap'] = (tp * v).cumsum() / v.cumsum()
    if df_current_5minute_data['close'].iloc[-1] > df_current_5minute_data['vwap'].iloc[-1]:
        return True


def check_open_interest(df_current_5minute_data):
    multiplier = 0.8  # current meaning oi is less than 80% of the avg , will be great if we can get slope added here
    df_current_5minute_data['oi_sma'] = talib.SMA(df_current_5minute_data['oi'], timeperiod=20)
    if df_current_5minute_data['oi'].iloc[-1] < multiplier * df_current_5minute_data['oi_sma'].iloc[-1]:
        return True


def check_volume(df_current_5minute_data):
    multiplier = 1.5 # current volume is more than 1.5X of the avg , will be great if we can get slope added here
    df_current_5minute_data['volume_sma'] = talib.SMA(df_current_5minute_data['volume'], timeperiod=20)
    if df_current_5minute_data['volume'].iloc[-1] > multiplier * df_current_5minute_data['volume_sma'].iloc[-1]:
        return True


def check_rsi(df_current_5minute_data):
    df_current_5minute_data['rsi'] = talib.RSI(df_current_5minute_data['close'], timeperiod=14)
    if df_current_5minute_data['rsi'].iloc[-1] > 60:
        return True


def scalium_execute(instrument, expiry_date, quantity, df):
    o = Order()
    distance = 100
    previous_day_data = fetch_http_ohlc(stock=instrument, from_date=previous_date, to_date=previous_date,
                                        period="day")
    df_previous_day_data = pd.DataFrame(previous_day_data,
                                        columns=["date", "open", "high", "low", "close", "volume", "oi"])
    pivot_points = df_calculate_pivot_points(df_previous_day_data)
    pdh = df_previous_day_data.iloc[0].high
    pdl = df_previous_day_data.iloc[0].low
    order_placed = False
    while True:
        current_time = datetime.datetime.now()
        if is_multiple_of_5(current_time.minute) and current_time.second > 2:
            price, ce_instrument, ce_strike = get_atm_strike(instrument, type='CE', df=df)
            price, pe_instrument, pe_strike = get_atm_strike(instrument, type='PE', df=df)

            ce_instrument_for_oi, ce_strike_for_oi = get_itm_strike(price=price, distance=distance, type='CE', df=df)
            ce_instrument_for_trade, ce_strike_for_trade = get_otm_strike(price=price, distance=distance, type='CE',
                                                                          df=df)

            pe_instrument_for_oi, pe_strike_for_oi = get_itm_strike(price=price, distance=distance, type='PE', df=df)
            pe_instrument_for_trade, pe_strike_for_trade = get_otm_strike(price=price, distance=distance, type='PE',
                                                                          df=df)
            ce_check = check_conditions(ce_instrument_for_oi)
            pe_check = check_conditions(pe_instrument_for_oi)

            if ce_check:
                print("CE")
                # o.place_order(ce_strike_for_trade, 'BUY', quantity=quantity)
            elif pe_check:
                print("PE")
                # o.place_order(pe_strike_for_trade, 'BUY', quantity=quantity)
            time.sleep(5)
        else:
            if order_placed:
                pass
                # star reading websocket
                # apply SL
                # apply TSL  below pivot when price moves above the pivot levels


def main():
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0], "<filename.json>"))
        exit(1)

    file_name = sys.argv[1]
    with open(file_name) as config_file:
        data = json.load(config_file)

    instrument = data['instrument']
    name = data['name']
    quantity = data['quantity']
    expiry_date_str = data['expiry_date']
    expiry_date = parse(expiry_date_str)

    # Download the instruments file once a day
    instruments.download_instruments_file()

    # Read only NIFTY or BANKNIFTY dataframe based on config of expiry_date
    df = instruments.read_instruments_file(expiry_date_str, name)

    scalium_execute(instrument, expiry_date, quantity, df)


if __name__ == '__main__':
    main()
