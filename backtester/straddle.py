import sqlite3, datetime, backtrader as bt
import pandas as pd
from utils.log import logger_instance
from utils.misc import get_nearest_expiry
import plotly.graph_objects as go
import quantstats as qs

logging = logger_instance


# Strategy Details:
# Simple straddle where we take position at 9:20 and sell at 15:15 if SL's are not hit on any leg
# If any leg has a SL hit of 30% then we exit only that leg and wait for other leg to get squared off on SL or
# at 15:15


class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order_close = None
        self.order1_close = None
        self.order = None
        self.order1 = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED {} for {}'.format(order.executed.price, order.product_type))
            elif order.issell():
                self.log('SELL EXECUTED {} for {}'.format(order.executed.price, order.product_type))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected for {}'.format(order.product_type))

    def next(self):
        sl = 0.30
        # skip mondays
        if self.data0.datetime.datetime().date().weekday() == 0:
            return

        # Check if we are in the market
        if (not self.getposition(self.data0)) and (not self.getposition(self.data1)):
            if self.data0.datetime.datetime().time().hour == 9 and self.data0.datetime.datetime().time().minute == 20:
                self.log('SELL CREATE, {} for {} '.format(self.data0.close[0], self.data0._name))
                self.log('SELL CREATE, {} for {}'.format(self.data1.close[0], self.data1._name))
                self.order = self.sell(self.data0)
                self.order.product_type = self.data0._name
                self.order1 = self.sell(self.data1)
                self.order1.product_type = self.data1._name

        else:

            if self.data0.close[0] > (self.order.executed.price * (1 + sl)) and not self.order_close:
                self.log('BUY CREATE, {}, {}-{} - {}'.format(self.data0.close[0], self.data0.close[0],
                                                             self.order.executed.price * (1 + sl),
                                                             self.order.product_type))
                self.order_close = self.buy(self.data0)
                self.order_close.product_type = self.data0._name

            if self.data1.close[0] > (self.order1.executed.price * (1 + sl)) and not self.order1_close:
                self.log('BUY CREATE, {}, {}-{}'.format(self.data1.close[0], self.data1.close[0],
                                                        self.order1.executed.price * (1 + sl),
                                                        self.order1.product_type))
                self.order1_close = self.buy(self.data1)
                self.order1_close.product_type = self.data1._name

        if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute == 15:
            if not self.order_close:
                o = self.buy(self.data0)
                o.product_type = self.data0._name
                self.log('BUY CREATE, {} {}'.format(self.data0.close[0], o.product_type))

            if not self.order1_close:
                o1 = self.buy(self.data1)
                o1.product_type = self.data1._name
                self.log('BUY CREATE, {} {}'.format(self.data1.close[0], o1.product_type))
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None


def straddle_strategy():
    con = sqlite3.connect("/Volumes/HD2/OptionData/NIFTY2020.db")
    con1 = sqlite3.connect("/Volumes/HD2/OptionData/NIFTY50.db")
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    datelist = pd.date_range(datetime.date(2020, 1, 1), periods=365).tolist()
    start_date = str(datelist[0].date())
    end_date = str(datelist[-1].date())
    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'sum'}
        # df_fut = pd.read_sql_query("SELECT * from nifty_futures_2019 where ticker = ? and date(date) = date(?)",
        #                            con, params=["NIFTY-I", d], parse_dates=True, index_col='date')
        df_fut = pd.read_sql_query("SELECT * from nifty where date(date) = date(?)",
                                   con1, params=[d], parse_dates=True, index_col='date')
        if df_fut.empty:
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        df5_fut = df_fut.resample('5min').apply(ohlc)

        close = df_fut.iloc[1].close
        atm_strike = round(close / 50) * 50  # round to nearest 100
        logging.info("Reading data for {}".format(d))
        logging.info("Picking atm strike {} for expiry {}".format(atm_strike, d1))
        df_opt_pe = pd.read_sql_query(
            "SELECT * from nifty_options_2020 where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?",
            con, params=[atm_strike, d, d1, 'PE'], parse_dates=True, index_col='date')
        df_opt_ce = pd.read_sql_query(
            "SELECT * from nifty_options_2020 where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?",
            con, params=[atm_strike, d, d1, 'CE'], parse_dates=True, index_col='date')

        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df_opt_pe = df_opt_pe.sort_index()
        df5_opt_pe = df_opt_pe.resample('5min').apply(ohlc)

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df_opt_ce = df_opt_ce.sort_index()
        df5_opt_ce = df_opt_ce.resample('5min').apply(ohlc)
        # skip the day if first tick is not at 9:15
        if not df_opt_ce.empty and (df_opt_ce.index[0].minute != 15 or df_opt_pe.index[0].minute != 15):
            continue

        df_final_ce = df_final_ce.append(df_opt_ce)
        df_final_pe = df_final_pe.append(df_opt_pe)

    cerebro = bt.Cerebro()

    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)
    cerebro.adddata(data_pe, name='PE')
    cerebro.adddata(data_ce, name='CE')
    cerebro.addstrategy(TestStrategy)
    cerebro.addsizer(bt.sizers.SizerFix, stake=50)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

    cerebro.broker.setcash(150000.0)
    print("Run start")

    strats = cerebro.run()

    pyfolio = strats[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    con1 = sqlite3.connect("/Volumes/HD2/OptionData/VIX.db")
    df_vix = pd.read_sql_query(
        "SELECT * from vix where date(date) >= date(?) and date(date) <= date(?)",
        con1, params=[start_date, end_date], parse_dates=True, index_col='date')
    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'}

    df_vix.index = pd.to_datetime(df_vix.index)
    df_vix = df_vix.tz_localize(None)
    df1day = df_vix.resample('1D').apply(ohlc)
    df1day.dropna(inplace=True)
    qs.extend_pandas()
    qs.reports.html(returns, output='backtester/stats.html', download_filename='backtester/stats.html',
                    title='Simple Straddle')
    test = returns.apply(lambda x: x * 100)
    vix = df1day.open.apply(lambda x: x / 100)
    fig = go.Figure(data=[
        go.Scatter(x=test.index, y=vix, line=dict(color='black', width=1), name="VIX", mode="lines+markers"),
        go.Scatter(x=test.index, y=test, line=dict(color='blue', width=1), name="Returns", mode="lines+markers")])
    fig.show()
    portvalue = cerebro.broker.getvalue()
    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == '__main__':
    straddle_strategy()
