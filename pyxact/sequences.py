

class SQLSequence:

    def __init__(self, name, start=1, interval=1, index_type='BIGINT', sql_options=''):
        self._name = name
        self._start = start
        self._interval = interval
        self._index_type = index_type
        self._sql_options = sql_options
        self._nextval_sequence_sql = None
        self._nextval_cached_dialect = None

    def create(self, cursor, dialect):
        for x in self.create_sequence_sql(dialect):
            cursor.execute(x)

    def nextval(self, cursor, dialect):
        for x in self.nextval_sequence_sql(dialect):
            cursor.execute(x)
        return cursor.fetchone()[0]

    def reset(self, cursor, dialect):
        for x in self.reset_sequence_sql(dialect):
            cursor.execute(x)

    def create_sequence_sql(self, dialect):
        return [x.format(name=self._name, start=self._start,
                interval=self._interval,index_type=self._index_type,
                sql_options=self._sql_options)
                for x in dialect.create_sequence_sql]

    def nextval_sequence_sql(self, dialect):
        if not self._nextval_sequence_sql or dialect != self._nextval_cached_dialect:
            self._nextval_sequence_sql = [x.format(name=self._name, start=self._start,
                                          interval=self._interval,index_type=self._index_type)
                                          for x in dialect.nextval_sequence_sql]
        return self._nextval_sequence_sql

    def reset_sequence_sql(self, dialect):
        return [x.format(name=self._name, start=self._start,
                interval=self._interval,index_type=self._index_type)
                for x in dialect.reset_sequence_sql]
