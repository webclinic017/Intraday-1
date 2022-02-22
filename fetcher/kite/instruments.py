import requests, os, datetime
import pandas as pd
from utils.log import logger_instance

logging = logger_instance


def download_instruments_file(force=False):
    "Download the file once day , unless force download is requested"

    yesterday = datetime.datetime.now().date() - datetime.timedelta(days=1)
    time = os.path.getmtime("instruments.csv")
    datex = datetime.datetime.fromtimestamp(float(time)).date()
    if datex > yesterday and not force:
        logging.info("instruments.csv is already latest file")
        return
    instruments_url = "https://api.kite.trade/instruments"
    req = requests.get(instruments_url)
    url_content = req.content
    csv_file = open('instruments.csv', 'wb')

    csv_file.write(url_content)
    csv_file.close()


def read_instruments_file(expiry_date, name):
    df = pd.read_csv("instruments.csv")
    sdf = df.loc[(df['expiry'] == expiry_date) & (df['name'] == name)]
    return sdf
