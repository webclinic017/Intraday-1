import coloredlogs
import logging
import datetime


def get_logger():
    date = datetime.datetime.now().date()
    # logging.basicConfig(level=logging.INFO,
    #                     filename='/Users/simon/srv/webapps/bigbasket.com/Intraday/{}-app.log'.format(date),
    #                     filemode='a', format='%(asctime)s - %(message)s')

    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    rootLogger = logging.getLogger("INTRADAY")
    filename = "log/{}-app.log".format(date)
    fileHandler = logging.FileHandler(filename)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    rootLogger.setLevel(logging.DEBUG)
    coloredlogs.install(logger=rootLogger, level='debug')
    return rootLogger


logger_instance = get_logger()
