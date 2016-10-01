import pandas as pd
import sqlalchemy

import utils.logger

class Database(utils.logger.GenericLogger):
    def __init__(self,  config):
        self.config = config
        self.db = None
        super(Database, self).__init__()

    def connect(self):
        self.db = sqlalchemy.create_engine(self.config['url'])

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
            readonly_user = self.config['readonly_user']
            if readonly_user:
                with self.db.begin() as conn:
                    conn.execute('GRANT SELECT ON %s TO %s;' % (table_name, self.config['readonly_user']))

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

