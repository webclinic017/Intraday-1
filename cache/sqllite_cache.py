import sqlite3, datetime


class Sqllite(object):

    def init_ltp_db(self):
        dbpath = 'live_price.db'
        self.con = sqlite3.connect(dbpath)
        self.cursor = self.con.cursor()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS price (name text PRIMARY KEY ,ltp integer,time_stamp DATE DEFAULT (datetime('now','localtime')))")
        self.con.commit()

    def get_ltp(self, name, time_window=15):
        d = datetime.datetime.now() - datetime.timedelta(seconds=time_window)
        result = self.cursor.execute("select ltp from price where  name = ? and time_stamp > ?", (name, d))
        result = result.fetchone()
        return result[0] if result else None

    def set_ltp(self, key, price):
        self.cursor.execute(
            "INSERT INTO price (name,ltp) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET ltp= ?, time_stamp=?",
            (key, price, price, datetime.datetime.now()))
        self.con.commit()
