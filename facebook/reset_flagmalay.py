import config
import MySQLdb


class producer:
    def __init__(self):
        self.conn = MySQLdb.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            passwd=config.passwd,
            db=config.db)
        self.connect = self.conn
    def parse(self):
        cur = self.conn.cursor()
        try:
            api = ""
            sql = "UPDATE malaysia SET api = '{}'".format(api)
            cur.execute(sql)
            self.conn.commit()
        except Exception, e:
            print e
        cur.close()

if __name__ == '__main__':
    p = producer()
    p.parse()
