import sqlite3, datetime, sys, json
import backtrader as bt
import pandas as pd
from utils.log import logger_instance
from utils.misc import get_nearest_expiry
import plotly.graph_objects as go
import quantstats as qs

logging = logger_instance


# Strategy Details:
# Simple straddle where we take position at 9:20 and sell at 15:15
# If the total loss of legs is < configured portfolio_loss config then exit all legs
# at 15:15


class TestStrategy(bt.Strategy):
    params = (
        ('portfolio_loss', -2000),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.order_close = None
        self.order1_close = None
        self.order = None
        self.order1 = None
        self.pnl = 0

    def notify_trade(self, trade):
        # Closed positions PNL
        # self.log("Trade executed: {}".format(trade))
        self.pnl += trade.pnl

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

        # skip mondays
        # if self.data0.datetime.datetime().date().weekday() == 0:
        #     return

        # Calculating open positions PNL
        pos = self.getposition(self.data0)
        comminfo = self.broker.getcommissioninfo(self.data0)
        pnl = comminfo.profitandloss(pos.size, pos.price, self.data0.close[0])

        pos1 = self.getposition(self.data1)
        comminfo1 = self.broker.getcommissioninfo(self.data1)
        pnl1 = comminfo1.profitandloss(pos1.size, pos1.price, self.data1.close[0])

        total_pnl = pnl + pnl1
        self.log('position pnl: {} trade pnl: {}, total pnl: {}'.format(total_pnl, self.pnl, total_pnl + self.pnl))

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

            # Below is exit condition based on portfolio loss
            if total_pnl + self.pnl < self.p.portfolio_loss:
                if self.getposition(self.data0):
                    self.order_close = self.buy(self.data0)
                    self.order_close.product_type = self.data0._name
                    self.log('BUY CREATE, {} {}'.format(self.data0.close[0], self.order_close.product_type))

                if self.getposition(self.data1):
                    self.order1_close = self.buy(self.data1)
                    self.order1_close.product_type = self.data1._name
                    self.log('BUY CREATE, {} {}'.format(self.data1.close[0], self.order1_close.product_type))

        if self.data0.datetime.cache/sqllite_cache.pydatetime().time().hour == 15 and self.data0.datetime.datetime().time().minute == 20:
            if self.getposition(self.data0):
                o = self.buy(self.data0)
                o.product_type = self.data0._name
                self.log('BUY CREATE, {} {}'.format(self.data0.close[0], o.product_type))

            if self.getposition(self.data1):
                o1 = self.buy(self.data1)
                o1.product_type = self.data1._name
                self.log('BUY CREATE, {} {}'.format(self.data1.close[0], o1.product_type))
            self.order_close = None
            self.order1_close = None
            self.order = None
            self.order1 = None

        # The below because not able to set self.pnl = 0 in above block
        if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute > 20:
            self.pnl = 0


def main():
    if len(sys.argv) < 2:
        print("Input Format Incorrect : {} {}".format(sys.argv[0], "<filename.json>"))
        exit(1)

    file_name = sys.argv[1]
    with open(file_name) as config_file:
        data = json.load(config_file)

    strike_dbpath = data['strike_dbpath']
    underlying_dbpath = data['underlying_dbpath']
    start_date = data["start_date"]
    end_date = data["end_date"]
    table_name = data["table_name"]
    portfolio_loss = int(data["portfolio_loss"])

    con = sqlite3.connect(strike_dbpath)
    con1 = sqlite3.connect(underlying_dbpath)
    df_final_pe = pd.DataFrame()
    df_final_ce = pd.DataFrame()
    datelist = pd.date_range(start=start_date, end=end_date).tolist()
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
        df_fut = pd.read_sql_query("SELECT * from nifty where date(date) = date(?)",
                                   con1, params=[d], parse_dates=True, index_col='date')
        if df_fut.empty:
            continue
        df_fut.index = pd.to_datetime(df_fut.index)
        # df5_fut = df_fut.resample('5min').apply(ohlc)

        close = df_fut.iloc[1].close
        atm_strike = round(close / 50) * 50  # round to nearest 100
        logging.info("Reading data for {}".format(d))
        logging.info("Picking atm strike {} for expiry {}".format(atm_strike, d1))
        query_string = "SELECT * from {} where strike = ? and date(date) = date(?) and expiry_date = ? and type = ?".format(
            table_name)
        df_opt_pe = pd.read_sql_query(query_string, con, params=[atm_strike, d, d1, 'PE'], parse_dates=True,
                                      index_col='date')
        df_opt_ce = pd.read_sql_query(query_string, con, params=[atm_strike, d, d1, 'CE'], parse_dates=True,
                                      index_col='date')

        df_opt_pe.index = pd.to_datetime(df_opt_pe.index)
        df_opt_pe = df_opt_pe.sort_index()
        # df5_opt_pe = df_opt_pe.resample('5min').apply(ohlc)

        df_opt_ce.index = pd.to_datetime(df_opt_ce.index)
        df_opt_ce = df_opt_ce.sort_index()

        # df5_opt_ce = df_opt_ce.resample('5min').apply(ohlc)
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
    cerebro.addstrategy(TestStrategy, portfolio_loss=portfolio_loss)
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
                    title='Portfolio Fixed SL Based Straddle')
    vix = df1day.open.apply(lambda x: x / 100)
    # fig = go.Figure(data=[
    #     go.Scatter(x=test.index, y=vix, line=dict(color='black', width=1), name="VIX", mode="lines+markers"),
    #     go.Scatter(x=test.index, y=test, line=dict(color='blue', width=1), name="Returns", mode="lines+markers")])
    # fig.show()
    portvalue = cerebro.broker.getvalue()
    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    print("Run finish")


if __name__ == '__main__':
    main()
