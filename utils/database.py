import sqlalchemy

import utils.logger

class Database(utils.logger.GenericLogger):
    def __init__(self,  url):
        self.url = url
        self.db = None
        super(Database, self).__init__()

    def connect(self):
        self.db = sqlalchemy.create_engine(self.url)

    def write(self, table_name, df, chunk_size = 5000):
        num_rows = len(df)

        for i in range(0, num_rows, chunk_size):
            start = i
            end = min(num_rows, start + chunk_size)
            self.logger.debug("    writing rows %10d - %d" % (start, end))
            df[start:end].to_sql(table_name, self.db, if_exists = 'append')

