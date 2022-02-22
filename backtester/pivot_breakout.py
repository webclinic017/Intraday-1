import datetime
import pandas as pd
import backtrader as bt
from conf.global_conf import backtest, source_ohlc_files, source_pivot_calculated_files
from utils.log import logger_instance
from utils.misc import is_multiple_of_interval, between

logging = logger_instance



class TestStrategy(bt.Strategy):
    params = (
        ('stock', ''),
        ('candle_interval', ''),
        ('candle_interval_data', ''),
        ('df1day', ''),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        logging.info('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.day = self.params.df1day
        self.order = None
        self.hunt_val = None
        self.hunt_line = None
        self.hunt_data = None

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

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        advance_datetime = self.data0.datetime.datetime()
        advance_date = self.data0.datetime.datetime().date()
        minute = advance_datetime.minute
        multiple_of_interval = is_multiple_of_interval(minute, self.params.candle_interval)
        real_datetime = advance_datetime - datetime.timedelta(minutes=1)
        real_date = (advance_datetime - datetime.timedelta(minutes=1)).date()
        # check if the candle for interval closed above/below nearby pivots
        keylist = ['S1', 'S2', 'R1',
                   'R2', 'BC', 'TC', 'pivot']  # 'WS1', 'WS2', 'Wpivot', 'MS1', 'MS2', 'MR1', 'MR2', 'Mpivot']
        support_res_touching_list = []
        if multiple_of_interval:
            data = self.params.candle_interval_data.loc[real_datetime]
            for i in keylist:
                val = self.day.loc[str(real_date)][i]
                try:
                    if between(val, data.low, data.close):
                        support_res_touching_list.append((i, val, data))
                except Exception as e:
                    logging.exception("Failed - {}".format(e))
            # proceed only if the candle is touching just 1 SR line
            # of course this is relevant only for pivot and daily pivot , it can touch daily and weekly/monthly
            if len(support_res_touching_list) == 1:
                self.hunt_line = support_res_touching_list[0][0]
                self.hunt_val = support_res_touching_list[0][1]
                self.hunt_data = support_res_touching_list[0][2]

        if not self.position:
            if self.hunt_line and self.dataclose[0] > self.hunt_data.close and self.hunt_data.close > self.hunt_data.open:
                self.entry_point = self.hunt_data.close
                self.log('BUY CREATE, {},{},{}'.format(self.dataclose[0], self.hunt_data.close, self.hunt_val))
                self.order = self.buy()
        else:
            # sell if 10 points of target is reached or 5 points below SR line
            if self.dataclose[0] > (self.hunt_data.close + 50) or self.dataclose[0] < (self.hunt_val - 5):
                self.log('SELL CREATE, {},{},{}'.format(self.dataclose[0], self.hunt_data.close, self.hunt_val))
                self.order = self.sell()
                self.hunt_data = None
                self.hunt_line = None
                self.hunt_val = None

        # below scenarios for long position
        # check if interval candle was bullish, very long wick , discard opportunity
        # check if 1 minute candle closed above the interval candle
        # check the size of interval candle , discard opportunity when interval candle is v big
        # check how far is the high/close from the support, if very far then discard.
        # check the narrow or wide CPR status
        # identify a buffer below support
        # discard opportunity when interval candle is toucing CPR and R1/S1 both (essentially large candle)
        # close at 2 RR or when close to next resistance
        # arrive at a candle distance at which point if 1 minute is not crossing interval candle then start afresh
        # identify  buffer above which its closing
        # discard if next resistance is very close
        # false breakout avoiding -> try to avoid parabolic breakouts, i.e without any consolidation


def generate_data(stock):
    # index the date column
    # TODO: there should be some cleanup here as 1 min file and 1 day file has date in diff formats
    ohlc_filename = source_ohlc_files[stock]
    df1min = pd.read_csv(ohlc_filename, usecols=['date', 'open', 'high', 'low', 'close', 'volume'],
                         index_col=0, parse_dates=True, dayfirst=True)
    df1min = df1min.tz_localize(None)
    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'}

    candle_interval_data = df1min.resample('5min').apply(ohlc)
    # remove NAN values for holidays etc.
    df1min.dropna(inplace=True)
    candle_interval_data.dropna(inplace=True)

    df1day = pd.read_csv(source_pivot_calculated_files[stock],
                         usecols=['date', 'open', 'high', 'low', 'close', 'volume', 'S1', 'S2', 'R1', 'R2', 'BC', 'TC',
                                  'pivot', 'PDH', 'PDL', 'WS1', 'WS2', 'WR1', 'WR2', 'Wpivot', 'MS1', 'MS2', 'MR1',
                                  'MR2', 'Mpivot'],
                         index_col=0, parse_dates=True)

    df1day = df1day.tz_localize(None)
    # df1day.dropna(inplace=True)

    data = bt.feeds.PandasData(dataname=df1min)
    cerebro = bt.Cerebro()
    bt.talib
    candle_interval = 5
    cerebro.addstrategy(TestStrategy, stock=stock, candle_interval=candle_interval,
                        candle_interval_data=candle_interval_data,
                        df1day=df1day)

    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Print out the starting conditions
    logging.info('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    logging.info('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())


def main():
    for stock in backtest["stocklist"]:
        generate_data(stock)


if __name__ == '__main__':
    main()
