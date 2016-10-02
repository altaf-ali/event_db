import pandas as pd
import sqlalchemy
import yurl

import utils.logger

class Database(utils.logger.GenericLogger):
    def __init__(self,  config):
        self.config = config
        self.db = None
        self.readonly_username = None
        super(Database, self).__init__()

    def connect(self):
        url = self.config['url']
        db_writer = self.config['writer']
        if (db_writer):
            parsed_url = yurl.URL(url)
            self.readonly_username = parsed_url.username
            url = str(parsed_url.replace(userinfo=':'.join([db_writer['username'], db_writer['password']])))
        self.logger.debug("  URL = %s" % url)
        self.db = sqlalchemy.create_engine(url)

    def primary_key(self, df):
        return ','.join('"{0}"'.format(i) for i in df.index.names)

    def table_exists(self, table_name):
        return self.db.dialect.has_table(self.db, table_name)

    def verify_table(self, table_name, df):
        if self.table_exists(table_name):
            self.logger.debug("  table %s already exists" % table_name)
        else:
            self.logger.debug("  creating table %s" % table_name)
            df_empty = df.drop(df.index)
            df_empty.to_sql(table_name, self.db)
            self.db.execute('ALTER TABLE %s ADD PRIMARY KEY (%s);' % (table_name, self.primary_key(df)))
            if self.readonly_username:
                with self.db.begin() as conn:
                    conn.execute('GRANT SELECT ON %s TO %s;' % (table_name, self.readonly_username))

    def write(self, table_name, df, chunk_size = 100000):

        # create table if necessary
        self.verify_table(table_name, df)

        df_existing = pd.read_sql('SELECT %s from %s' % (self.primary_key(df), table_name), self.db)
        df_existing = df_existing.set_index(df.index.names)

        df_merged = pd.merge(df, df_existing, how="inner", left_index=True, right_index=True, copy=False)
        df_new = df.drop(df_merged.index)

        num_rows = len(df_new)

        self.logger.debug("  total rows %d, skipping %d, remaining %d" % (len(df), len(df_merged), len(df_new)))

        for i in range(0, num_rows, chunk_size):
            start = i
            end = min(num_rows, start + chunk_size)
            self.logger.debug("    writing rows %10d - %d" % (start, end))
            df_new[start:end].to_sql(table_name, self.db, if_exists = 'append')

