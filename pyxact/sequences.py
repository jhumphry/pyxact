'''This module defines types that represent sequences in a database. These may
be used where it is necessary to acquire values that will be unique within the
database (such as for transaction IDs).'''

from . import SQLSchemaBase
from . import dialects, fields

class SQLSequence:
    '''SQLSequence defines a basic sequence type. Information about the
    sequence and its current state will be stored in the database. Note that
    this sequence type does not guarantee a 'gapless' sequence.'''

    def __init__(self, name, start=1, interval=1, index_type='BIGINT',
                 sql_options='', sql_name=None, schema=None):
        self.name = name
        if sql_name:
            self.sql_name = sql_name
        else:
            self.sql_name = name

        if schema is None:
            self.schema = None
        elif isinstance(schema, SQLSchemaBase):
            self.schema = schema
            schema.register_sequence(self)
        else:
            raise TypeError('schema must be an instance of pyxact.schemas.SQLSchema')

        self.start = start
        self.interval = interval
        self.index_type = index_type
        self.sql_options = sql_options
        self._nextval_sequence_sql = None
        self._nextval_cached_dialect = None

    def qualified_name(self, dialect=None):
        '''The (possibly schema-qualified) name of the sequence used in SQL.'''

        if self.schema is None:
            return self.sql_name

        return self.schema.qualified_name(self.sql_name)

    def create(self, cursor, dialect=None):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        to create the sequence in the database if it does not already exist.
        The dialect parameter allows the function to identify the correct SQL
        commands to issue.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        for sql_string in self.create_sequence_sql(dialect):
            cursor.execute(sql_string)

    def nextval(self, cursor, dialect=None):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        return the next value in the sequence and update the database. The
        dialect parameter allows the function to identify the correct SQL
        commands to issue.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        for sql_string in self.nextval_sequence_sql(dialect):
            cursor.execute(sql_string)
        return cursor.fetchone()[0]

    def reset(self, cursor, dialect=None):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        reset the state of the sequence. The dialect parameter allows the
        function to identify the correct SQL commands to issue.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        for sql_string in self.reset_sequence_sql(dialect):
            cursor.execute(sql_string)

    def create_sequence_sql(self, dialect=None):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        create the sequence if it does not already exist in the database.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        return [x.format(qualified_name=self.qualified_name(dialect),
                         start=self.start,
                         interval=self.interval,
                         index_type=self.index_type,
                         sql_options=self.sql_options)
                for x in dialect.create_sequence_sql]

    def nextval_sequence_sql(self, dialect=None):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        return the next value in the sequence, updating the stored parameters
        as it goes. It should be safe for use by multiple simultaneous database
        users.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        if not self._nextval_sequence_sql or dialect != self._nextval_cached_dialect:
            self._nextval_sequence_sql = [x.format(qualified_name=self.qualified_name(dialect),
                                                   start=self.start,
                                                   interval=self.interval,
                                                   index_type=self.index_type)
                                          for x in dialect.nextval_sequence_sql]
        return self._nextval_sequence_sql

    def reset_sequence_sql(self, dialect=None):
        '''This function takes a parameter identifying a dialect of SQL and
        returns a list of strings containing the SQL commands necessary to
        return the next value in the sequence, updating the stored parameters
        as it goes. It should be safe for use by multiple simultaneous database
        users.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        return [x.format(qualified_name=self.qualified_name(dialect), start=self.start,
                         interval=self.interval, index_type=self.index_type)
                for x in dialect.reset_sequence_sql]

class SequenceIntField(fields.AbstractIntField):
    '''Represents an integer field in an SQLTransaction that has a link to a
    SQLSequence. It can be retrieved and set as a normal SQLField, but when
    get_new_context is called on the SQLTransaction, it will be updated from
    the next value of the sequence and the name:value pair will be returned as
    part of the context dictionary. Within SQLRecord subclasses, an
    ContextIntField can be used to represent this value. This field type has no
    direct use inside an SQLRecord.'''

    def __init__(self, sequence, **kwargs):
        if not isinstance(sequence, SQLSequence):
            raise TypeError('Sequence provided must be an instance of '
                            'pyxact.sequences.SQLSequence')
        self.sequence = sequence
        super().__init__(py_type=int, sql_type=None,
                         nullable=True, **kwargs)

    def update(self, instance, context, cursor, dialect=None):

        value = self.sequence.nextval(cursor, dialect)
        setattr(instance, self._slot_name, value)
        return value
