'''This module defines schema objects that tie together different pyxact
classes that represent an associated set of database objects. These can be
mapped to database schema in database systems such as PostgreSQL which
implement a flexible schema concept.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import enum

from . import SQLSchemaBase
from . import dialects, indexes, sequences, tables, views

class SQLSchema(SQLSchemaBase):
    '''This class represents a collection of tables, sequenes etc that are all
    associated. Where the database provides sufficient support, they will all be
    placed in the same SQL schema, and where it does not they will all have the
    same prefix, which will be added automatically.'''

    def __init__(self, name):
        super().__init__()
        self.name = name

        self.commands_before = []
        self.enums = {}
        self.table_types = {}
        self.tables = {}
        self.view_types = {}
        self.views = {}
        self.sequence_types = {}
        self.sequences = {}
        self.index_types = {}
        self.indexes = {}
        self.commands_after = []

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

    def register_sql_command(self, command, dialect=None, after=True):
        '''Register a string to execute as an SQL command when creating the
        schema. By default they will be run after the other objects are
        created, unless the after parameter is set to false. If a dialect is
        specified, this command will only be run if that database dialect is in
        use.'''

        if after:
            self.commands_after.append((dialect, command))
        else:
            self.commands_before.append((dialect, command))

    def register_table(self, table):
        '''Register an SQLTable subclass as the definition of a database
        table. Note that in order for this to work, the table must have been
        given a table_name. If the schema class parameter was used on defining
        the subclass then this will have already been called.'''

        if not issubclass(table, tables.SQLTable):
            raise TypeError('Only SQLTable subclasses can be registered to a schema.')

        if table._table_name is None:
            raise TypeError('Only tables with defined names can be registered to a schema.')

        self.table_types[table.__name__] = table
        self.tables[table._table_name] = table

    def register_view(self, view):
        '''Register an SQLView subclass as the definition of a database
        table. Note that in order for this to work, the view must have been
        given a view_name. If the schema class parameter was used on defining
        the subclass then this will have already been called.'''

        if not issubclass(view, views.SQLView):
            raise TypeError('Only SQLView subclasses can be registered to a schema.')

        if view._view_name is None:
            raise TypeError('Only views with defined names can be registered to a schema.')

        self.view_types[view.__name__] = view
        self.views[view._view_name] = view

    def register_enum(self, enum_type, sql_name=None):
        '''Register an Enum type to be created in the database schema as the definition of a
        database sequence. '''

        if not issubclass(enum_type, enum.Enum):
            raise TypeError('Only enum.Enum types can be registered to a schema.')

        self.enums[sql_name] = enum_type

    def register_sequence(self, sequence):
        '''Register an SQLSequence isntance as the definition of a database
        sequence. If the schema class parameter was used on creating the
        sequence then this will have already been called.'''

        if not isinstance(sequence, sequences.SQLSequence):
            raise TypeError('Only SQLSequence instances can be registered to a schema.')

        self.sequence_types[sequence.name] = sequence
        self.sequences[sequence.sql_name] = sequence

    def register_index(self, index):
        '''Register an SQLIndex subclass as the definition of a database index.
        If the schema class parameter was used on defining the subclass then
        this will have already been called.'''

        if not isinstance(index, indexes.SQLIndex):
            raise TypeError('Only SQLIndex instances can be registered to a schema.')

        self.index_types[index.name] = index
        self.indexes[index.sql_name] = index

    def _run_commands(self, commands, cursor, dialect):

        for i in commands:
            if i[0] is None or i[0] == dialect:
                if i[0] is None or not dialect.schema_support:
                    command = dialects.convert_schema_sep(i[1], '_')
                else:
                    command = i[1]
                cursor.execute(command)


    def create_schema_objects(self, cursor, dialect=None):
        '''Create all of the registered objects in the schema.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        self._run_commands(self.commands_before, cursor, dialect)

        for i in self.enums.items():
            dialect.create_enum_type(cursor=cursor, py_type=i[1],
                                     sql_name=i[0], sql_schema=self.name)

        for i in self.sequence_types.values():
            i.create(cursor, dialect)

        for i in self.table_types.values():
            cursor.execute(i._create_table_sql(dialect))

        for i in self.view_types.values():
            cursor.execute(i._create_view_sql(dialect))

        for i in self.index_types.values():
            i.create(cursor, dialect)

        self._run_commands(self.commands_after, cursor, dialect)
