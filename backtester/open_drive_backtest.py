import datetime
import pandas as pd
import backtrader as bt
import datetime
from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc


class TestStrategy(bt.Strategy):
    params = (
        ('df1day', ''),
        ('trail_percent', ''),
        ('stop_loss', ''),
        ('take_profit', '')
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.order = None
        self.prevday = self.params.df1day

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, {} Order Ref: {}'.format(order.executed.price, order.ref))
            elif order.issell():
                self.log('SELL EXECUTED, {} Order Ref: {}'.format(order.executed.price, order.ref))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(' Order Canceled/Margin/Rejected - status: {} Order Ref: {} '.format(order.Status[order.status],
                                                                                          order.ref))

    def next(self):
        current_date = self.data0.datetime.datetime().date()
        for i in range(1, 5):
            previous_date = current_date - datetime.timedelta(days=i)
            dfprevday = self.prevday.loc[(self.prevday['date'] == previous_date)]
            if dfprevday.empty:
                continue
            else:
                break
        if i == 4:
            return
            # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.position:
            if self.order.isbuy() and self.data0.close[0] > self.order.executed.price * (1 + self.p.take_profit):
                self.close()
                if self.stop_order.status in [self.stop_order.Submitted, self.stop_order.Accepted]:
                    self.cancel(self.stop_order)

            if self.order.issell() and self.data0.close[0] < self.order.executed.price * (1 - self.p.take_profit):
                self.close()
                if self.stop_order.status in [self.stop_order.Submitted, self.stop_order.Accepted]:
                    self.cancel(self.stop_order)

            if self.data0.datetime.datetime().time().hour == 15 and self.data0.datetime.datetime().time().minute == 15:
                # Close all positions at end of the day
                self.close()
                if self.stop_order.status in [self.stop_order.Submitted, self.stop_order.Accepted]:
                    self.cancel(self.stop_order)

    def next_open(self):
        current_date = self.data0.datetime.datetime().date()
        for i in range(1, 5):
            previous_date = current_date - datetime.timedelta(days=i)
            dfprevday = self.prevday.loc[(self.prevday['date'] == previous_date)]
            if dfprevday.empty:
                continue
            else:
                break
        if i == 4:
            return
            # Check if an order is pending ... if yes, we cannot send a 2nd one

        if not self.position:
            if self.data0.datetime.datetime().time().hour == 9 and self.data0.datetime.datetime().time().minute == 20:
                dfprevday = self.prevday.loc[(self.prevday['date'] == previous_date)]
                if self.data0.open[-1] == self.data0.low[-1] and self.data0.low[-1] > \
                        dfprevday.iloc[0].high and self.data0.open[0] >= self.data0.close[-1]:
                    self.order = self.buy(transmit=False)
                    self.log('BUY CREATE, {}, Order Ref: {}  '.format(self.data0.open[0], self.order.ref))
                    if not self.p.trail_percent:
                        stop_price = self.data0.open[0] * (1.0 - self.p.stop_loss)
                        self.stop_order = self.sell(exectype=bt.Order.Stop, price=stop_price, parent=self.order)
                        self.log('Sell SL Trigger, {}, Order Ref: {}'.format(self.data0.open[0], self.stop_order.ref))
                    else:
                        stop_price = self.data0.open[0] * (1.0 - self.p.stop_loss)
                        self.stop_order = self.sell(exectype=bt.Order.StopTrailLimit, trailpercent=self.p.trail_percent,
                                                    parent=self.order, price=stop_price)
                        self.log('Sell TSL Trigger, {}, Order Ref: {}'.format(self.data0.open[0], self.stop_order.ref))

                else:
                    if self.data0.open[-1] == self.data0.high[-1] and self.data0.high[-1] < \
                            dfprevday.iloc[0].low and self.data0.open[0] <= self.data0.close[-1]:
                        # Keep track of the created order to avoid a 2nd order
                        self.order = self.sell(transmit=False)
                        self.log('SELL CREATE, {}, Order Ref: {}'.format(self.data0.open[0], self.order.ref))
                        if not self.p.trail_percent:
                            stop_price = self.data0.open[0] * (1.0 + self.p.stop_loss)
                            self.stop_order = self.buy(exectype=bt.Order.Stop, price=stop_price, parent=self.order)
                            self.log(
                                'Buy SL Trigger, {}, Order Ref: {}'.format(self.data0.open[0], self.stop_order.ref))
                        else:
                            stop_price = self.data0.open[0] * (1.0 + self.p.stop_loss)
                            self.stop_order = self.buy(exectype=bt.Order.StopTrailLimit,
                                                       trailpercent=self.p.trail_percent,
                                                       parent=self.order, price=stop_price)
                            self.log(
                                'Buy TSL Trigger, {}, Order Ref: {}'.format(self.data0.open[0], self.stop_order.ref))


def generate_data(stock, start_date, end_date):
    # index the date column

    date_range = pd.date_range(start_date, end_date, freq="M")

    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'}
    df5min = pd.DataFrame()
    for datex in date_range:
        d_ref = datex.date()
        s_date = datetime.date(d_ref.year, d_ref.month, 1)
        e_date = datex.date()
        current_5min = fetch_http_ohlc(stock=stock, from_date=s_date, to_date=e_date,
                                       period="5minute")
        df5min_temp = pd.DataFrame(current_5min,
                                   columns=["date", "open", "high", "low", "close", "volume", "oi"])

        df5min = df5min.append(df5min_temp)
    df5min.index = pd.to_datetime(df5min.date)
    df5min = df5min.tz_localize(None)
    df1day = df5min.resample('1D').apply(ohlc)
    df5min.dropna(inplace=True)
    df1day.dropna(inplace=True)
    df1day['date'] = pd.to_datetime(df1day.index).date

    data = bt.feeds.PandasData(dataname=df5min)

    cerebro = bt.Cerebro(cheat_on_open=True)
    cerebro.addstrategy(TestStrategy, df1day=df1day, trail_percent=False, stop_loss=0.01, take_profit=0.02)

    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(100000.0)
    stake = round(100000 / max(df5min.close.array))
    cerebro.addsizer(bt.sizers.SizerFix, stake=stake)
    cerebro.addanalyzer(bt.analyzers.PyFolio)
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    results = cerebro.run()
    strat = results[0]
    pyfoliozer = strat.analyzers.getbyname('pyfolio')
    returns, positions, transactions, gross_lev = pyfoliozer.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    import quantstats
    quantstats.reports.html(returns, output='stats.html', title='BTC Sentiment')
    quantstats.reports.full(returns)
    # quantstats.plots.returns(returns)


    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    return cerebro.broker.getvalue()


if __name__ == '__main__':
    from conf.nifty_stocks import nifty_stock_list

    stock_list = nifty_stock_list
    mtm_list = []
    start_date = datetime.date(2021, 2, 1)
    end_date = datetime.date(2021, 2, 28)
    # stock_list = [
    #     {"instrumenttoken": 3861249, "tradingsymbol": "ADANIPORTS"}
    # ]
    for i in stock_list:
        mtm = generate_data(i, start_date, end_date)
        mtm_list.append(mtm)
    print(mtm_list)
