import sqlite3, datetime, backtrader as bt
import pandas as pd
import pandas_ta as ta
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
        self.straddle_underlying_price = None
        self.rolling_dict = {
            "ATM_CE": ['LOWER_PE1', 'LOWER_CE1', 'UPPER_PE1', 'UPPER_CE1'],
            "LOWER_CE1": ['LOWER_PE2', 'LOWER_CE2', 'ATM_PE', 'ATM_CE'],
            "LOWER_CE2": ['LOWER_PE3', 'LOWER_CE3', 'LOWER_PE1', 'LOWER_CE1'],
            "UPPER_CE1": ['ATM_CE', 'ATM_PE', 'UPPER_PE2', 'UPPER_CE2'],
            "UPPER_CE2": ['UPPER_PE1', 'UPPER_CE1', 'UPPER_PE3', 'UPPER_CE3']
        }

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
        max_count = 3
        distance = 75
        underlying_data = self.getdatabyname('UNDERLYING')

        # Check if we are in the market
        if (not self.getposition(self.data0)) and (not self.getposition(self.data1)):
            if self.data0.datetime.datetime().time().hour == 9 and self.data0.datetime.datetime().time().minute == 20:
                self.log('SELL CREATE, {} for {} '.format(self.data0.close[0], self.data0._name))
                self.log('SELL CREATE, {} for {}'.format(self.data1.close[0], self.data1._name))
                self.order = self.sell(self.data0)
                self.order.product_type = self.data0._name
                self.order1 = self.sell(self.data1)
                self.order1.product_type = self.data1._name
                self.straddle_underlying_price = underlying_data.close[0]
                self.straddle_pe_dataname = self.data0._name
                self.straddle_ce_dataname = self.data1._name
        else:

            if underlying_data.close[0] > (self.straddle_underlying_price + distance):
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


def get_atm_surrounding_strikes(atm, round_distance=100, length=3):
    upper_strikes = []
    lower_strikes = []
    strike = atm
    for i in range(length):
        strike = strike + round_distance
        upper_strikes.append(strike)
    strike = atm
    for i in range(length):
        strike = strike - round_distance
        lower_strikes.append(strike)
    return lower_strikes, upper_strikes


def straddle_strategy():
    option_db = "/Volumes/HD2/OptionData/NIFTY2021.db"
    underlying_db = "/Volumes/HD2/OptionData/NIFTY50.db"
    option_table = "nifty_options_2021"
    underlying_table = "nifty"
    con = sqlite3.connect(option_db)
    con1 = sqlite3.connect(underlying_db)
    round_distance = 50
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    df_final_lower_pe1 = pd.DataFrame()
    df_final_lower_ce1 = pd.DataFrame()
    df_final_lower_pe2 = pd.DataFrame()
    df_final_lower_ce2 = pd.DataFrame()
    df_final_lower_pe3 = pd.DataFrame()
    df_final_lower_ce3 = pd.DataFrame()
    df_final_upper_pe1 = pd.DataFrame()
    df_final_upper_ce1 = pd.DataFrame()
    df_final_upper_pe2 = pd.DataFrame()
    df_final_upper_ce2 = pd.DataFrame()
    df_final_upper_pe3 = pd.DataFrame()
    df_final_upper_ce3 = pd.DataFrame()
    df_fut_final = pd.DataFrame()

    datelist = pd.date_range(datetime.date(2021, 1, 1), periods=30).tolist()
    for d in datelist:
        d = d.date()
        d1 = get_nearest_expiry(d)
        ohlc = {'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'sum'}
        df_fut = pd.read_sql_query("SELECT * from {} where date(date) = date(?)".format(underlying_table),
                                   con1, params=[d], parse_dates=True, index_col='date')
        if df_fut.empty:
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        df_fut = df_fut.sort_index()
        close = df_fut.iloc[1].close
        atm_strike = round(close / round_distance) * round_distance  # round to nearest 50 or 100
        logging.info("Reading data for {}".format(d))
        logging.info("Picking atm strike {} for expiry {}".format(atm_strike, d1))
        df_opt_pe = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[atm_strike, d, d1, 'PE'], parse_dates=True, index_col='date')
        df_opt_ce = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[atm_strike, d, d1, 'CE'], parse_dates=True, index_col='date')

        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df_opt_pe = df_opt_pe.sort_index()

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df_opt_ce = df_opt_ce.sort_index()

        # skip the day if first tick is not at 9:15
        if not df_opt_ce.empty and (df_opt_ce.index[0].minute != 15 or df_opt_pe.index[0].minute != 15):
            continue

        df_final_ce = df_final_ce.append(df_opt_ce)
        df_final_pe = df_final_pe.append(df_opt_pe)

        lower_strikes, upper_strikes = get_atm_surrounding_strikes(atm=atm_strike, round_distance=round_distance)

        df_lower_pe1 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[0], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_lower_ce1 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[0], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_lower_pe1.index = pd.to_datetime(df_lower_pe1.index)
        df_lower_pe1 = df_lower_pe1.sort_index()

        df_lower_ce1.index = pd.to_datetime(df_lower_ce1.index)
        df_lower_ce1 = df_lower_ce1.sort_index()

        df_final_lower_pe1 = df_final_lower_pe1.append(df_lower_pe1)
        df_final_lower_ce1 = df_final_lower_ce1.append(df_lower_ce1)

        df_lower_pe2 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[1], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_lower_ce2 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[1], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_lower_pe2.index = pd.to_datetime(df_lower_pe2.index)
        df_lower_pe2 = df_lower_pe2.sort_index()

        df_lower_ce2.index = pd.to_datetime(df_lower_ce2.index)
        df_lower_ce2 = df_lower_ce2.sort_index()

        df_final_lower_pe2 = df_final_lower_pe2.append(df_lower_pe2)
        df_final_lower_ce2 = df_final_lower_ce2.append(df_lower_ce2)

        df_lower_pe3 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[2], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_lower_ce3 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[lower_strikes[2], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_lower_pe3.index = pd.to_datetime(df_lower_pe3.index)
        df_lower_pe3 = df_lower_pe3.sort_index()

        df_lower_ce3.index = pd.to_datetime(df_lower_ce3.index)
        df_lower_ce3 = df_lower_ce3.sort_index()

        df_final_lower_pe3 = df_final_lower_pe3.append(df_lower_pe3)
        df_final_lower_ce3 = df_final_lower_ce3.append(df_lower_ce3)

        df_upper_pe1 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[0], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_upper_ce1 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[0], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_upper_pe1.index = pd.to_datetime(df_upper_pe1.index)
        df_upper_pe1 = df_upper_pe1.sort_index()

        df_upper_ce1.index = pd.to_datetime(df_upper_ce1.index)
        df_upper_ce1 = df_upper_ce1.sort_index()

        df_final_upper_pe1 = df_final_upper_pe1.append(df_upper_pe1)
        df_final_upper_ce1 = df_final_upper_ce1.append(df_upper_ce1)

        df_upper_pe2 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[1], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_upper_ce2 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[1], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_upper_pe2.index = pd.to_datetime(df_upper_pe2.index)
        df_upper_pe2 = df_upper_pe2.sort_index()

        df_upper_ce2.index = pd.to_datetime(df_upper_ce2.index)
        df_upper_ce2 = df_upper_ce2.sort_index()

        df_final_upper_pe2 = df_final_upper_pe2.append(df_upper_pe2)
        df_final_upper_ce2 = df_final_upper_ce2.append(df_upper_ce2)

        df_upper_pe3 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[2], d, d1, 'PE'], parse_dates=True, index_col='date')
        df_upper_ce3 = pd.read_sql_query(
            "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
                option_table),
            con, params=[upper_strikes[2], d, d1, 'CE'], parse_dates=True, index_col='date')

        df_upper_pe3.index = pd.to_datetime(df_upper_pe3.index)
        df_upper_pe3 = df_upper_pe3.sort_index()

        df_upper_ce3.index = pd.to_datetime(df_upper_ce3.index)
        df_upper_ce3 = df_upper_ce3.sort_index()

        df_final_upper_pe3 = df_final_upper_pe3.append(df_upper_pe3)
        df_final_upper_ce3 = df_final_upper_ce3.append(df_upper_ce3)

        df_fut_final = df_fut_final.append(df_fut)
    cerebro = bt.Cerebro()

    data_pe = bt.feeds.PandasData(dataname=df_final_pe)
    data_ce = bt.feeds.PandasData(dataname=df_final_ce)

    df_final_lower_pe1 = bt.feeds.PandasData(dataname=df_final_lower_pe1)
    df_final_lower_ce1 = bt.feeds.PandasData(dataname=df_final_lower_ce1)

    df_final_lower_pe2 = bt.feeds.PandasData(dataname=df_final_lower_pe2)
    df_final_lower_ce2 = bt.feeds.PandasData(dataname=df_final_lower_ce2)

    df_final_lower_pe3 = bt.feeds.PandasData(dataname=df_final_lower_pe3)
    df_final_lower_ce3 = bt.feeds.PandasData(dataname=df_final_lower_ce3)

    df_final_upper_pe1 = bt.feeds.PandasData(dataname=df_final_upper_pe1)
    df_final_upper_ce1 = bt.feeds.PandasData(dataname=df_final_upper_ce1)

    df_final_upper_pe2 = bt.feeds.PandasData(dataname=df_final_upper_pe2)
    df_final_upper_ce2 = bt.feeds.PandasData(dataname=df_final_upper_ce2)

    df_final_upper_pe3 = bt.feeds.PandasData(dataname=df_final_upper_pe3)
    df_final_upper_ce3 = bt.feeds.PandasData(dataname=df_final_upper_ce3)

    df_fut_final = bt.feeds.PandasData(dataname=df_fut_final)

    cerebro.adddata(data_pe, name='ATM_PE')
    cerebro.adddata(data_ce, name='ATM_CE')

    cerebro.adddata(df_final_lower_pe1, name='LOWER_PE1')
    cerebro.adddata(df_final_lower_ce1, name='LOWER_CE1')
    cerebro.adddata(df_final_lower_pe2, name='LOWER_PE2')
    cerebro.adddata(df_final_lower_ce2, name='LOWER_CE2')
    cerebro.adddata(df_final_lower_pe3, name='LOWER_PE3')
    cerebro.adddata(df_final_lower_ce3, name='LOWER_CE3')
    cerebro.adddata(df_final_upper_pe1, name='UPPER_PE1')
    cerebro.adddata(df_final_upper_ce1, name='UPPER_CE1')
    cerebro.adddata(df_final_upper_pe2, name='UPPER_PE2')
    cerebro.adddata(df_final_upper_ce2, name='UPPER_CE2')
    cerebro.adddata(df_final_upper_pe3, name='UPPER_PE3')
    cerebro.adddata(df_final_upper_ce3, name='UPPER_CE3')
    cerebro.adddata(df_fut_final, name='UNDERLYING')

    cerebro.addstrategy(TestStrategy)
    cerebro.addsizer(bt.sizers.SizerFix, stake=50)
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

    cerebro.broker.setcash(150000.0)
    print("Run start")

    strats = cerebro.run()

    pyfolio = strats[0].analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfolio.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    qs.extend_pandas()
    qs.reports.html(returns, output='backtester/stats.html', download_filename='backtester/stats.html',
                    title='Python Straddle')
    portvalue = cerebro.broker.getvalue()
    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == '__main__':
    straddle_strategy()
