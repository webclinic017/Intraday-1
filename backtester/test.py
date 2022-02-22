from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import argparse

import backtrader as bt
import backtrader.feeds as btfeeds
import backtrader.indicators as btind
import backtrader.utils.flushfile
import pandas as pd

from indicator.pivot import df_calculate_pivot_points


class St(bt.Strategy):
    params = (
        ('stock', 'wipro'),
    )

    def __init__(self):
        print("Test")
        print(self.params.stock)


    def next(self):
        print(self.dataclose[0].close)


def runstrat():
    df1min = pd.read_csv('/Users/simon/srv/webapps/bigbasket.com/Intraday/data/stocks/1minute/bnf.csv',
                         usecols=['date', 'open', 'high', 'low', 'close', 'volume'],
                         index_col=0, parse_dates=True)
    # Remove Time Zone data
    df1min = df1min.tz_localize(None)
    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'}

    # resampling 1 minute data into 15min and 1 day using above ohlc logic
    df15min = df1min.resample('15min').apply(ohlc)

    # remove NAN values for holidays etc.
    df15min.dropna(inplace=True)

    cerebro = bt.Cerebro()
    # data_X = bt.feeds.PandasData(dataname=df1min)
    data_Y = bt.feeds.PandasData(dataname=df15min)
    # cerebro.adddata(data_X)
    cerebro.adddata(data_Y)
    cerebro.addstrategy(St, stock='infosys')
    print("Run start")
    cerebro.run()
    print("Run finish")


if __name__ == '__main__':
    runstrat()
