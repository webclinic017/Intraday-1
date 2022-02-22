import os
import pandas as pd
from dateutil.relativedelta import SU, relativedelta
from indicator.pivot import calculate_pivot_points
from conf.global_conf import source_ohlc_files, source_pivot_calculated_files
from utils.log import logger_instance

logger = logger_instance


def generate_pivot():
    for stock, filename in source_ohlc_files.items():
        if not filename.endswith(".csv"):
            continue
        df1min = pd.read_csv(filename, usecols=['date', 'open', 'high', 'low', 'close', 'volume'],
                             index_col=0, parse_dates=True,dayfirst=True)
        df1min = df1min.tz_localize(None)
        ohlc = {'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'}

        # resampling 1 minute data into 15min and 1 day using above ohlc logic
        df5min = df1min.resample('5min').apply(ohlc)
        dfday = df1min.resample('D').apply(ohlc)
        dfweek = df1min.resample('W').apply(ohlc)
        dfmonth = df1min.resample('M').apply(ohlc)

        # remove NAN values for holidays etc.
        df1min.dropna(inplace=True)
        df5min.dropna(inplace=True)
        dfday.dropna(inplace=True)
        dfweek.dropna(inplace=True)
        dfmonth.dropna(inplace=True)

        generate_monthly_pivot(stock, dfmonth)
        generate_weekly_pivot(stock, dfweek)
        generate_daily_pivot(stock, dfmonth, dfweek, dfday)
        save_pivot_data(stock, dfday)


def load_ohlc_data():
    dirpath = 'data/stocks/1minute'
    for filename in os.listdir(dirpath):
        if filename.endswith(".csv"):
            full_path = os.path.join(dirpath, filename)
            df1min = pd.read_csv(full_path, usecols=['date', 'open', 'high', 'low', 'close', 'volume'],
                                 index_col=0, parse_dates=True)
        yield df1min


def save_pivot_data(stock, dfday):
    full_path = source_pivot_calculated_files[stock]
    dfday.to_csv(full_path)


def generate_daily_pivot(stock, dfmonth, dfweek, dfday):
    logger.info("Calculating Daily Pivot")
    prev_index = None
    for i, row in dfday.iterrows():
        if not prev_index:
            prev_index = i
            prev_row = row
            continue
        pivot_dict = calculate_pivot_points(prev_row)
        dfday.loc[i, 'S1'] = pivot_dict['S1']
        dfday.loc[i, 'S2'] = pivot_dict['S2']
        # dfday.loc[i, 'S3'] = pivot_dict['S3']
        dfday.loc[i, 'R1'] = pivot_dict['R1']
        dfday.loc[i, 'R2'] = pivot_dict['R2']
        # dfday.loc[i, 'R3'] = pivot_dict['R3']
        dfday.loc[i, 'BC'] = pivot_dict['BC']
        dfday.loc[i, 'TC'] = pivot_dict['TC']
        dfday.loc[i, 'pivot'] = pivot_dict['pivot']
        dfday.loc[i, 'PDH'] = prev_row['high']
        dfday.loc[i, 'PDL'] = prev_row['low']
        daily_date = i.date()
        logger.info("Calculating Daily Pivot data  for date :{} for {}".format(daily_date, stock))

        # Weekly Pivot Calculations
        # from current date get date of next Sunday
        weekly_daily_date = daily_date + relativedelta(weekday=SU)
        str_weekly_daily_date = str(weekly_daily_date)
        dfday.loc[i, 'WS1'] = dfweek.loc[str_weekly_daily_date].S1
        dfday.loc[i, 'WS2'] = dfweek.loc[str_weekly_daily_date].S2
        # dfday.loc[i, 'WS3'] = dfweek.loc[str_weekly_daily_date].S3
        dfday.loc[i, 'WR1'] = dfweek.loc[str_weekly_daily_date].R1
        dfday.loc[i, 'WR2'] = dfweek.loc[str_weekly_daily_date].R2
        # dfday.loc[i, 'WR3'] = dfweek.loc[str_weekly_daily_date].R3
        dfday.loc[i, 'Wpivot'] = dfweek.loc[str_weekly_daily_date].pivot

        # Monthly Pivot Calculations
        # from current date get last day of the month
        monthly_daily_date = daily_date + relativedelta(day=31)
        str_monthly_daily_date = str(monthly_daily_date)
        dfday.loc[i, 'MS1'] = dfmonth.loc[str_monthly_daily_date].S1
        dfday.loc[i, 'MS2'] = dfmonth.loc[str_monthly_daily_date].S2
        # dfday.loc[i, 'MS3'] = dfmonth.loc[str_monthly_daily_date].S3
        dfday.loc[i, 'MR1'] = dfmonth.loc[str_monthly_daily_date].R1
        dfday.loc[i, 'MR2'] = dfmonth.loc[str_monthly_daily_date].R2
        # dfday.loc[i, 'MR3'] = dfmonth.loc[str_monthly_daily_date].R3
        dfday.loc[i, 'Mpivot'] = dfmonth.loc[str_monthly_daily_date].pivot

        prev_index = i
        prev_row = row


def generate_weekly_pivot(stock, dfweek):
    logger.info("Calculating Weekly Pivot for {}".format(stock))
    prev_index = None
    for i, row in dfweek.iterrows():
        if not prev_index:
            prev_index = i
            prev_row = row
            continue
        pivot_dict = calculate_pivot_points(prev_row)
        dfweek.loc[i, 'S1'] = pivot_dict['S1']
        dfweek.loc[i, 'S2'] = pivot_dict['S2']
        dfweek.loc[i, 'S3'] = pivot_dict['S3']
        dfweek.loc[i, 'R1'] = pivot_dict['R1']
        dfweek.loc[i, 'R2'] = pivot_dict['R2']
        dfweek.loc[i, 'R3'] = pivot_dict['R3']
        dfweek.loc[i, 'pivot'] = pivot_dict['pivot']
        prev_index = i
        prev_row = row


def generate_monthly_pivot(stock, dfmonth):
    logger.info("Calculating Monthly Pivot for {}".format(stock))
    prev_index = None
    for i, row in dfmonth.iterrows():
        if not prev_index:
            prev_index = i
            prev_row = row
            continue
        pivot_dict = calculate_pivot_points(prev_row)
        dfmonth.loc[i, 'S1'] = pivot_dict['S1']
        dfmonth.loc[i, 'S2'] = pivot_dict['S2']
        dfmonth.loc[i, 'S3'] = pivot_dict['S3']
        dfmonth.loc[i, 'R1'] = pivot_dict['R1']
        dfmonth.loc[i, 'R2'] = pivot_dict['R2']
        dfmonth.loc[i, 'R3'] = pivot_dict['R3']
        dfmonth.loc[i, 'pivot'] = pivot_dict['pivot']
        prev_index = i
        prev_row = row


def main():
    """
    Reads the global config for location of 1 minute  directory and calculates and stores pivot
    data for all the files in that directory in the pivot
    :return:
    """
    generate_pivot()


if __name__ == '__main__':
    main()
