'''This module defines types that represent sequences in a database. These may
be used where it is necessary to acquire values that will be unique within the
database (such as for transaction IDs).'''

class SQLSequence:
    '''SQLSequence defines a basic sequence type. Information about the
    sequence and its current state will be stored in the database. Note that
    this sequence type does not guarantee a 'gapless' sequence.'''

    def __init__(self, name, start=1, interval=1, index_type='BIGINT',
                 sql_options='', sql_name=None):
        self._name = name
        if sql_name:
            self._sql_name = sql_name
        else:
            self._sql_name = name
        self._start = start
        self._interval = interval
        self._index_type = index_type
        self._sql_options = sql_options
        self._nextval_sequence_sql = None
        self._nextval_cached_dialect = None

    @property
    def name(self):
        return self._name

    @property
    def sql_name(self):
        return self._sql_name

    def create(self, cursor, dialect):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        to create the sequence in the database if it does not already exist.
        The dialect parameter allows the function to identify the correct SQL
        commands to issue.'''

        for sql_string in self.create_sequence_sql(dialect):
            cursor.execute(sql_string)

    def nextval(self, cursor, dialect):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        return the next value in the sequence and update the database. The
        dialect parameter allows the function to identify the correct SQL
        commands to issue.'''

        for sql_string in self.nextval_sequence_sql(dialect):
            cursor.execute(sql_string)
        return cursor.fetchone()[0]

    def reset(self, cursor, dialect):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        reset the state of the sequence. The dialect parameter allows the
        function to identify the correct SQL commands to issue.'''

        for sql_string in self.reset_sequence_sql(dialect):
            cursor.execute(sql_string)

    def create_sequence_sql(self, dialect):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        create the sequence if it does not already exist in the database.'''

        return [x.format(name=self._name, start=self._start,
                         interval=self._interval, index_type=self._index_type,
                         sql_options=self._sql_options)
                for x in dialect.create_sequence_sql]

    def nextval_sequence_sql(self, dialect):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        return the next value in the sequence, updating the stored parameters
        as it goes. It should be safe for use by multiple simultaneous database
        users.'''

        if not self._nextval_sequence_sql or dialect != self._nextval_cached_dialect:
            self._nextval_sequence_sql = [x.format(name=self._name, start=self._start,
                                                   interval=self._interval,
                                                   index_type=self._index_type)
                                          for x in dialect.nextval_sequence_sql]
        return self._nextval_sequence_sql

    def reset_sequence_sql(self, dialect):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        return the next value in the sequence, updating the stored parameters
        as it goes. It should be safe for use by multiple simultaneous database
        users.'''

        return [x.format(name=self._name, start=self._start,
                         interval=self._interval, index_type=self._index_type)
                for x in dialect.reset_sequence_sql]
