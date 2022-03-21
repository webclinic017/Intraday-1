import re, os, sys, sqlite3
import pandas as pd
import pandas_ta as ta
import datetime
from dateutil.parser import parse
from utils.log import logger_instance
from dateutil.rrule import *

logging = logger_instance

ohlc = {'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'oi': 'sum'}


def splitter(s, index_str):
    '''
    :param s:
    handles below formats
    NIFTY16FEB8100CE
    NIFTY18FEB10800CE.NFO
    NIFTY08APR2115800CE

    :param date:
    2020

    "param index_str:
    NIFTY
    BANKNIFTY

    :return:
     ('10800', 'CE', datetime.date(2020, 2, 18))

    '''

    try:
        t = s.split(index_str)[1]
        u = t.split(".")[0]
        if u[5:7] in ["19", "20", "21"]:
            # Weekly expiry
            v, w = u[:7], u[7:]
            x, y = w[:len(w) - 2], w[-2:]
            z = parse(v, dayfirst=True).date()
        else:
            # Monthly Expiry
            v, w = re.findall('\d*\D+', u)
            x, y = w[:len(w) - 2], w[-2:]
            z = v + '1'  # 1st date of the month
            z = list(rrule(MONTHLY, count=1, byweekday=TH(-1), dtstart=parse(z, yearfirst=True)))[
                0].date()  # last thursday of month
        return x, y, z
    except Exception as e:
        logging.exception("Exception {}, {}".format(t, e))
        return "", "", None


def clean_and_save(file, index_str, con, year):
    try:
        df = pd.read_csv(file, parse_dates=[['Date', 'Time']], dayfirst=True)
        df.drop(df[df['Ticker'].str.contains('OPTIDX_')].index, inplace=True)
        df.drop(df[df['Ticker'].str.contains('FINNIFTY')].index, inplace=True)
        logging.info("Processing file {}".format(file))
        df[['strike', 'type', 'expiry_date']] = df.apply(lambda x: splitter(x['Ticker'], index_str),
                                                         axis=1).tolist()
        df = df.rename(
            columns={"Open Interest": "oi", "Open": "open", "Close": "close", "High": "high", "Volume": "volume",
                     "Date_Time": "date", "Ticker": "ticker", "Low": "low"})
        df = df.drop(['Date_', 'Month', 'Year'], axis=1)
        df.columns = df.columns.str.strip()
        df.index = pd.to_datetime(df.date)
        df.drop(['date'], axis=1, inplace=True)
        df.to_sql('nifty_options_' + year, con, if_exists='append')
    except Exception as e:
        logging.exception("Exception on processing file {}".format(file, e))


def clean_and_save_futures(file, con, year):
    try:
        df = pd.read_csv(file, parse_dates=[['Date', 'Time']], dayfirst=True)
        logging.info("Processing file {}".format(file))
        df = df.rename(
            columns={"Open Interest": "oi", "Open": "open", "Close": "close", "High": "high", "Volume": "volume",
                     "Date_Time": "date", "Ticker": "ticker"})

        df = df.drop(['Date_', 'Month', 'Year'], axis=1)
        df.columns = df.columns.str.strip()
        df.index = pd.to_datetime(df.date)
        df.drop(['date'], axis=1, inplace=True)
        df.to_sql('nifty_futures_' + year, con, if_exists='append')
    except Exception as e:
        logging.exception("Exception on processing file {}".format(file, e))


def get_files(path, pattern):
    file_list = []
    for dirpath, subdirs, files in os.walk(path):
        for x in files:
            if pattern in x:
                x = os.path.join(dirpath, x)
                file_list.append(x)
    return file_list


def main():
    '''
    python data/cleaner.py '/Volumes/HD2/OptionData/2016-21_raw/2016/' '_nifty_futures' 'NIFTY' 2016 'future'
    python data/cleaner.py '/Volumes/HD2/OptionData/2016-21_raw/2016/' '_nifty_options' 'NIFTY' 2016 'option'
    '''
    if len(sys.argv) < 5:
        print("Input Format Incorrect : {} {}".format(sys.argv[0],
                                                      "<path> <pattern> <index_string> <year> <future/option>"))
        exit(1)
    path = sys.argv[1]
    pattern = sys.argv[2]
    index_str = sys.argv[3]
    year = sys.argv[4]
    future_option = sys.argv[5]
    file_list = get_files(path, pattern)
    path = '/Volumes/HD2/OptionData/'
    db_name = path + index_str + str(year) + '.db'
    con = sqlite3.connect(db_name)
    for file in file_list:
        if future_option == "future":
            clean_and_save_futures(file, con, year)
        elif future_option == "option":
            clean_and_save(file, index_str, con, year)
    con.execute("CREATE INDEX ix_nifty_options_expiry_date ON  nifty_options_{}(expiry_date)".format(year))
    con.close()


if __name__ == '__main__':
    main()
