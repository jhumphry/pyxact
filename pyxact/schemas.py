'''This module defines schema objects that tie together different pyxact
classes that represent an associated set of database objects. These can be
mapped to database schema in database systems such as PostgreSQL which
implement a flexible schema concept.'''

from . import dialects, records, sequences

class SQLSchema:
    '''This class represents a collection of tables, sequenes etc that are all
    associated. Where the database provides sufficient support, they will all be
    placed in the same SQL schema, and where it does not they will all have the
    same prefix, which will be added automatically.'''

    def __init__(self, name):

        self.name = name
        self.record_types = {}
        self.tables = {}
        self.sequence_types = {}
        self.sequences = {}

    def create_schema(self, cursor, dialect=None):
        '''Execute a suitable CREATE SCEHMA command (in the given
        SQL dialect) that will create the database schema defined by the
        SQLSchema. If the given database dialect (or the default dialect if
        appropriate) does not support the necessary schema features then
        nothing will be done.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        if dialect.schema_support:
            cursor.execute('CREATE SCHEMA IF NOT EXISTS {};'.format(self.name))

    def qualified_name(self, name, dialect=None):
        '''Returns an object name qualified by the schema name. This usually
        means that the schema name will be followed by '.' and then the object
        name, but for database dialects that do not support schemas, the schema
        and object names will be separated with a '_'.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        if not dialect.schema_support:
            return self.name + '_' + name

        return self.name + '.' + name

    def register_table(self, table):
        '''Register an SQLRecord subclass as the definition of a database
        table. Note that in order for this to work, the table must have been
        given a table_name. If the schema class parameter was used on defining
        the subclass then this will have already been called.'''

        if not issubclass(table, records.SQLRecord):
            raise TypeError('Only SQLRecord subclasses can be registered to a schema.')

        if table.table_name is None:
            raise TypeError('Only tables with defined names can be registered to a schema.')

        self.record_types[table.__name__] = table
        self.tables[table.table_name] = table

    def register_sequence(self, sequence):
        '''Register an SQLSequence isntance as the definition of a database
        sequence. If the schema class parameter was used on creating the
        sequence then this will have already been called.'''

        if not isinstance(sequence, sequences.SQLSequence):
            raise TypeError('Only SQLSequence instances can be registered to a schema.')

        self.sequence_types[sequence.name] = sequence
        self.sequences[sequence.sql_name] = sequence

    def create_schema_objects(self, cursor, dialect=None):
        '''Create all of the registered objects in the schema.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        for i in self.record_types.values():
            cursor.execute(i.create_table_sql(dialect))

        for i in self.sequence_types.values():
            i.create(cursor, dialect)
