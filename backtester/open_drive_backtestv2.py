import datetime
import pandas as pd
import backtrader as bt
import datetime
from fetcher.kite.http_ohlc_fetcher import fetch_http_ohlc

from backtesting import Strategy, Backtest
import numpy as np


class MyCandlesStrat(Strategy):
    sltr = 5

    def init(self):
        super().init()

    def next(self):
        super().next()
        sltr = self.sltr
        for trade in self.trades:
            if trade.is_long:
                trade.sl = max(trade.sl or -np.inf, self.data.Close[-1] - sltr)
            else:
                trade.sl = min(trade.sl or np.inf, self.data.Close[-1] + sltr)

        # if self.signal1 == 2 and len(self.trades) == 0:  # trades number change!
        #     sl1 = self.data.Close[-1] - sltr
        #     self.buy(sl=sl1)
        # elif self.signal1 == 1 and len(self.trades) == 0:  # trades number change!
        #     sl1 = self.data.Close[-1] + sltr
        #     self.sell(sl=sl1)


def generate_data():
    # index the date column
    current_date = datetime.datetime.now().date()
    start_date = datetime.date(2022, 1, 1)

    stock = {"instrumenttoken": 1346049, "tradingsymbol": "INDUSINDBK"}

    ohlc = {'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'}

    current_5min = fetch_http_ohlc(stock=stock, from_date=start_date, to_date=current_date,
                                   period="5minute")
    df5min = pd.DataFrame(current_5min,
                          columns=["date", "open", "high", "low", "close", "volume", "oi"])

    df5min.index = pd.to_datetime(df5min.date)
    df5min = df5min.tz_localize(None)
    df1day = df5min.resample('1D').apply(ohlc)
    df5min.dropna(inplace=True)
    df1day.dropna(inplace=True)
    df1day['date'] = pd.to_datetime(df1day.index).date

    df5min.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
                  inplace=True)

    df1day.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'},
                  inplace=True)

    bt = Backtest(df5min, MyCandlesStrat, cash=10_000, commission=.000)
    bt._strategy.df1day = df1day
    stat = bt.run()
    stat


if __name__ == '__main__':
    generate_data()
