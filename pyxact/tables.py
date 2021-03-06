'''This module defines Python types that map to SQL database tables.'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from . import UnconstrainedWhereError, SQLSchemaBase
from . import constraints, dialects, records

INVALID_SQLTABLE_NAMES = None

class SQLTableMetaClass(records.SQLRecordMetaClass):
    '''This is a metaclass that automatically identifies the SQLField and
    SQLConstraint member attributes added to new subclasses and creates
    additional private attributes to help order and access them.'''

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace,
                table_name=None, version=None, schema=None, **kwds):

        mcs.prepare_sqlrecord_namespace(mcs, namespace, INVALID_SQLTABLE_NAMES)
        mcs.prepare_sqltable_namespace(mcs, namespace)

        namespace['_table_name'] = table_name
        namespace['_version'] = version

        if not ((schema is None) or isinstance(schema, SQLSchemaBase)):
            raise TypeError('schema must be an instance of pyxact.schemas.SQLSchema')

        namespace['_schema'] = schema

        new_record_class = type.__new__(mcs, name, bases, namespace)

        if schema is not None:
            schema.register_table(new_record_class)

        return new_record_class

    def prepare_sqltable_namespace(cls, namespace):
        '''This method receives an ordered dictionary of attributes attached to
        the new subclass and checks, indexes and processes them appropriately,
        adding additional items where necessary.'''

        _constraints = dict()

        # Make a list of the SQLConstraints attributes attached to the class and
        # check that the names won't be hiding any methods or attributes on the base class.

        for key, value in namespace.items():
            if isinstance(value, constraints.SQLConstraint):
                if key in INVALID_SQLTABLE_NAMES:
                    raise AttributeError('SQLConstraint {} has the same name as a method'
                                         'or internal attribute'.format(key))
                _constraints[key] = value

            if isinstance(value, type) and issubclass(value, constraints.SQLConstraint):
                raise Warning('An SQLConstraint subclass has been attached as {} rather than an '
                              'instance of the class. This is probably incorrect.'.format(key))

        namespace['_constraints'] = _constraints

        # Now check the ColumnsConstraint for exant columns, fill out the sql_column_names
        # attribute on the constraint (which is not available when the constraint is instantiated,
        # and finally set the sql_reference_names to be the same as the sql_column_names
        # (a 'natural join') if it is None on a ForeignKeyConstraint.

        namespace['_primary_key'] = None

        _fields = namespace['_fields']

        for key, value in _constraints.items():
            if isinstance(value, constraints.ColumnsConstraint):
                sql_column_names = []

                for column_name in value.column_names:
                    if column_name not in _fields:
                        raise AttributeError('SQLConstraint {} references non-existent column {}'
                                             .format(key, column_name))

                    if _fields[column_name].sql_name:
                        sql_column_names.append(_fields[column_name].sql_name)
                    else:
                        sql_column_names.append(column_name)

                value.sql_column_names = tuple(sql_column_names)

                if isinstance(value, constraints.PrimaryKeyConstraint):
                    if namespace['_primary_key']:
                        raise AttributeError('Attempting to have multiple primary keys')
                    namespace['_primary_key'] = value

                if isinstance(value, constraints.ForeignKeyConstraint) and \
                        not value.sql_reference_names:
                    value.sql_reference_names = value.sql_column_names

        return namespace

class SQLTable(records.SQLRecord, metaclass=SQLTableMetaClass):
    '''SQLTable is a subclass of SQLRecord that contains additional information
    needed to map an SQLRecord to a specific table. It also contains methods
    that allow for inserting/updating/selecting records from the table.'''

    def __str__(self):
        result = 'SQL table "' + self._table_name + '"\n'
        result += super().__str__()
        if self._primary_key:
            result += '  Primary key ('
            result += ', '.join(self._primary_key.column_names)
            result += ')\n'
        return result

    @classmethod
    def _qualified_table_name(cls):
        '''The (possibly schema-qualified) name of the table used in SQL.'''

        if cls._table_name is None:
            raise RuntimeError('No table_name was set when the class was created, so it is not '
                               'possible to link the class to a table in the database.')

        if cls._schema is None:
            return cls._table_name

        return cls._schema.qualified_name(cls._table_name)

    def _pk_items(self, context=None):
        '''Returns a tuple containing a list of primary key SQL column names
        and a list of the associated values, using the context dictionary if
        provided.'''

        sql_names = []
        values = []

        for pk_field_name in self._primary_key.column_names:
            field_obj = self._fields[pk_field_name]
            sql_names.append(field_obj.sql_name)
            if context:
                pk_value = field_obj.get_context(self, context)
            else:
                pk_value = field_obj.get(self)
            if pk_value:
                values.append(pk_value)
            else:
                raise UnconstrainedWhereError('Value for primary key column {0} is None.'
                                              .format(pk_field_name))
        return (sql_names, values)

    @classmethod
    def _create_table_sql(cls):
        '''Returns a string containing the CREATE TABLE command that
        will create the table defined by the SQLRecord.'''

        result = 'CREATE TABLE IF NOT EXISTS ' + cls._qualified_table_name() + ' (\n    '
        table_columns = [cls._fields[key].sql_ddl()
                         for key in cls._fields.keys()]
        table_constraints = [cls._constraints[key].sql_ddl()
                             for key in cls._constraints.keys()]
        result += ',\n    '.join(table_columns+table_constraints)
        result += '\n);'
        return result

    @classmethod
    def _truncate_table_sql(cls, cascade=False):
        '''Return an SQL string that will truncate this table.'''

        dialect = dialects.DefaultDialect

        if cascade:
            cmd = dialect.truncate_table_cascade_sql
        else:
            cmd = dialect.truncate_table_sql

        return cmd.format(table_name=cls._qualified_table_name())

    @classmethod
    def _insert_sql_command(cls):
        '''Returns a string containing the parametrised INSERT command
        required to insert data into the SQL table represented by the
        SQLRecord.'''

        dialect = dialects.DefaultDialect

        result = 'INSERT INTO ' + cls._qualified_table_name() + ' ('
        result += cls._column_names_sql()
        result += ') VALUES ('
        if cls._field_count > 0:
            result += dialect.parameter(cls._field_count)
        result += ');'
        return result

    def _insert_sql(self, context=None):
        '''This method constructs an SQL INSERT command and returns a tuple
        containing a suitable string and list of values.'''

        return (self._insert_sql_command(), self._values_sql_repr(context))

    def _update_sql(self, context=None):
        '''This method constructs an SQL UDPATE command and returns a tuple
        containing a suitable string and list of values. It identifies the row
        to be updated using the primary key columns, whose associated SQLField
        attribute values must not be None. If a context dictionary is provided,
        it will be passed to the SQLField attributes and they may use the
        values in it in preference to their existing values.'''

        dialect = dialects.DefaultDialect

        if not self._primary_key:
            raise UnconstrainedWhereError('Only SQLRecord subclasses with a primary key constraint '
                                          'can use the update_sql() method.')

        pk_columns = self._primary_key.column_names
        pk_columns_sql_names, pk_values = self._pk_items(context)
        pk_sql_values = [dialect.sql_repr(x) for x in pk_values]

        update_sql_names = []
        update_values = []

        for field_name, field_obj in self._fields.items():
            if field_name not in pk_columns:
                update_sql_names.append(field_obj.sql_name)
                if context:
                    update_values.append(dialect.sql_repr(field_obj.get_context(self, context)))
                else:
                    update_values.append(dialect.sql_repr(field_obj.get(self)))

        result = 'UPDATE ' + self._qualified_table_name() + ' SET '
        result += dialect.parameter_values(update_sql_names, 1)
        result += ' WHERE '
        result += dialect.parameter_values(pk_columns_sql_names, len(update_sql_names)+1, 'AND')
        result += ';'

        return (result, update_values + pk_sql_values)

    def _delete_sql(self, context=None):
        '''This method constructs an SQL DELETE command and returns a tuple
        containing a suitable string and list of values. It identifies the row
        to be deleted using the primary key columns, whose associated SQLField
        attribute values must not be None. If a context dictionary is provided,
        it will be passed to the SQLField attributes and they may use the
        values in it in preference to their existing values.'''

        dialect = dialects.DefaultDialect

        if not self._primary_key:
            raise UnconstrainedWhereError('Only SQLRecord subclasses with a primary key constraint '
                                          'can use the delete_sql() method.')

        pk_columns_sql_names, pk_values = self._pk_items(context)
        pk_sql_values = [dialect.sql_repr(x) for x in pk_values]

        result = 'DELETE FROM ' + self._qualified_table_name() + ' WHERE '
        result += dialect.parameter_values(pk_columns_sql_names, 1, 'AND')
        result += ';'

        return (result, pk_sql_values)

    @classmethod
    def _simple_select_sql(cls, **kwargs):
        '''Returns a tuple of a string containing the parametrised SELECT command (in the
        given SQL dialect) required to retrieve data from the SQL table
        represented by the SQLRecord, and the values to pass as parameters.
        Only the most basic form of WHERE clause is supported, with exact
        values for columns specified in the form of keyword arguments to the
        method.'''

        dialect = dialects.DefaultDialect

        for field in kwargs:
            if not field in cls._fields:
                raise ValueError('Specified field {0} is not valid'.format(field))

        result = 'SELECT ' + cls._column_names_sql() + ' FROM ' + cls._qualified_table_name()
        if kwargs:
            result += ' WHERE '
            field_sql_names = [cls._fields[field].sql_name for field in kwargs]
            result += dialect.parameter_values(field_sql_names, 1, 'AND')
        result += ';'

        values = [dialect.sql_repr(x) for x in kwargs.values()]

        return (result, values)

    def _pk_select_sql(self, context=None):
        '''This method returns a tuple containg an SQL SELECT statement that
        would retrieve this record based on the primary key and a list of
        values for the primary key columns.'''

        dialect = dialects.DefaultDialect

        if not self._primary_key:
            raise UnconstrainedWhereError('Only SQLRecord subclasses with a primary key constraint '
                                          'can use the pk_select_sql() method.')

        pk_columns_sql_names, pk_values = self._pk_items(context)
        pk_sql_values = [dialect.sql_repr(x) for x in pk_values]

        result = 'SELECT ' + self._column_names_sql()
        result += ' FROM ' + self._qualified_table_name() + ' WHERE '
        result += dialect.parameter_values(pk_columns_sql_names, 1, 'AND')
        result += ';'

        return (result, pk_sql_values)

    @classmethod
    def _context_select_sql(cls, context, allow_unlimited=True):
        '''This method  takes a context dictionary of name:value pairs and identifies those
        SQLFields within the SQLRecord that would use the context values provided by
        any of those names. It then constructs an SQL statement using the column names
        of the identified SQLFields and returns that statement and the list of relevant
        values.'''

        dialect = dialects.DefaultDialect

        result = 'SELECT ' + cls._column_names_sql() + ' FROM ' + cls._qualified_table_name()

        column_sql_names = []
        column_values = []

        # This might be better with a set and intersection operation?

        for field_obj in cls._fields.values():
            field_ctxt = field_obj.context_used
            if field_ctxt in context:
                column_sql_names.append(field_obj.sql_name)
                column_values.append(dialect.sql_repr(context[field_ctxt]))

        if not allow_unlimited and not column_sql_names:
            raise UnconstrainedWhereError('No WHERE clause generated - possible due to '
                                          'missing/misnamed context values?')

        if column_sql_names:
            result += ' WHERE '
            result += dialect.parameter_values(column_sql_names, 1, 'AND')

        result += ';'
        return (result, column_values)

# This constant records all the method and attribute names used in SQLRecord
# and SQLTable so thatthe metaclasses can detect any attempts to overwrite
# them in subclasses.

INVALID_SQLTABLE_NAMES = frozenset(dir(SQLTable))
