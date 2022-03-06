import sqlite3, datetime, backtrader as bt
import pandas as pd
from dateutil.parser import parse
from conf.expiry_dates import sorted_expiry_dates
from utils.log import logger_instance

logging = logger_instance

# start preparing the DF
# get distinct expiry date list for the year and store in a list
# Loop start at date level , re-sample the 1 minute to 5 minute and fetch the 5 minutes futures DF
# From the futures price calculate the ATM strike
# Use the ATM price and get the nearest strike for CE/PE from DB using the expiry date list , this is the working DF
# Feed this DF to the Strategy
# Within the Strategy next() method if the candle time is 09:20 then sell PE and CE and set 30% as SL
# Within the Strategy next() method if the candle time is 03:15 then buy PE and CE if not already bought
# Plot the graph


from backtesting import Strategy, Backtest
import numpy as np


class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def next(self):
        # Simply log the closing price of the series from the reference
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            if self.data0.datetime.datetime().time().hour == 9 and self.data0.datetime.datetime().time().minute == 20:
                self.log('SELL CREATE, {}  '.format(self.data0.close[0]))
                self.log('SELL CREATE, {}'.format(self.data1.close[0]))
                self.order = self.sell(self.data0)
                self.order1 = self.sell(self.data1)

        else:

            if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute == 20:
                self.log('BUY CREATE, {}'.format(self.data0.close[0]))
                self.log('BUY CREATE, {}'.format(self.data1.close[0]))
                self.order = self.buy(self.data0)
                self.order1 = self.buy(self.data1)


def get_nearest_expiry(d):
    for i in sorted_expiry_dates:
        x = parse(i).date()
        if x >= d:
            return x


def straddle_strategy():
    con = sqlite3.connect("nifty")
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    datelist = pd.date_range(datetime.date(2016, 6, 1), periods=100).tolist()
    # d = datetime.date(2016, 6, 20)
    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum',
                'oi': 'sum'}
        df_fut = pd.read_sql_query("SELECT * from nifty_futures_2016 where ticker = ? and date(Date_Time) = date(?)",
                                   con, params=["NIFTY-I", d], parse_dates=True, index_col='Date_Time')
        if df_fut.empty:
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        df5_fut = df_fut.resample('5min').apply(ohlc)

        close = df5_fut.iloc[1].Close
        atm_strike = round(close / 100) * 100  # round to nearest 100
        logging.info("Reading data for {}".format(d))
        df_opt_pe = pd.read_sql_query(
            "SELECT * from nifty_options_2016 where strike = ? and date(Date_Time) = date(?) and expiry_date = ? and type = ?",
            con, params=[atm_strike, d, d1, 'PE'], parse_dates=True, index_col='Date_Time')
        df_opt_ce = pd.read_sql_query(
            "SELECT * from nifty_options_2016 where strike = ? and date(Date_Time) = date(?) and expiry_date = ? and type = ?",
            con, params=[atm_strike, d, d1, 'CE'], parse_dates=True, index_col='Date_Time')

        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df5_opt_pe = df_opt_pe.resample('5min').apply(ohlc)

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df5_opt_ce = df_opt_ce.resample('5min').apply(ohlc)
        df_final_ce = df_final_ce.append(df5_opt_ce)
        df_final_pe = df_final_pe.append(df5_opt_pe)

    cerebro = bt.Cerebro()

    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)
    cerebro.adddata(data_pe, name='PE')
    cerebro.adddata(data_ce, name='CE')
    cerebro.addstrategy(TestStrategy)
    cerebro.addanalyzer(bt.analyzers.PyFolio)
    print("Run start")

    strats = cerebro.run()

    pyfolio = strats[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    print(returns,positions,transactions)
    import pyfolio as pf
    # pf.create_full_tear_sheet(
    #     returns,
    #     positions=positions,
    #     transactions=transactions,
    #     live_start_date='2016-06-1',
    #     round_trips=True)

    portvalue = cerebro.broker.getvalue()
    #cerebro.plot()
    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == '__main__':
    straddle_strategy()
