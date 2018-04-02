'''This module defines Python types that map to SQL database views.'''

from . import UnconstrainedWhereError, SQLSchemaBase
from . import dialects, records

INVALID_SQLVIEW_NAMES = None

class SQLViewMetaClass(records.SQLRecordMetaClass):
    '''This is a metaclass that automatically identifies the SQLField and SQLConstraint member
    attributes added to new subclasses and creates additional private attributes to help order and
    access them.'''

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, view_name=None, schema=None, query=None, **kwds):

        mcs.prepare_sqlrecord_namespace(mcs, namespace, INVALID_SQLVIEW_NAMES)

        namespace['_view_name'] = view_name

        if not ((schema is None) or isinstance(schema, SQLSchemaBase)):
            raise TypeError('schema must be an instance of pyxact.schemas.SQLSchema')

        namespace['_schema'] = schema

        namespace['_query'] = query

        new_record_class = type.__new__(mcs, name, bases, namespace)

        if schema is not None:
            schema.register_view(new_record_class)

        return new_record_class

class SQLView(records.SQLRecord, metaclass=SQLViewMetaClass):
    '''SQLView is a subclass of SQLRecord that contains additional information needed to map an
    SQLRecord to a specific view. It also contains methods that allow for selecting records from
    the view.'''

    def __str__(self):
        result = 'SQLView "' + self._view_name + '"\n'
        result += super().__str__()
        return result

    @property
    def _view_name(self):
        '''The name of the view used in SQL.'''

        return self._view_name

    @classmethod
    def _qualified_view_name(cls, dialect=None):
        '''The (possibly schema-qualified) name of the view used in SQL.'''

        if cls._schema is None:
            return cls._view_name

        return cls._schema.qualified_name(cls._view_name, dialect)

    @classmethod
    def _create_view_sql(cls, dialect=None):
        '''Returns a string containing the CREATE TABLE command (in the given SQL dialect) that
        will create the table defined by the SQLRecord.'''

        result = 'CREATE VIEW IF NOT EXISTS ' + cls._qualified_view_name(dialect) + ' ('
        result += ', '.join(cls._fields.keys())
        if dialect.schema_support:
            result += ') AS \n' + dialects.convert_schema_sep(cls._query, '.')  + ';'
        else:
            result += ') AS \n' + dialects.convert_schema_sep(cls._query, '_')  + ';'
        return result

    @classmethod
    def _simple_select_sql(cls, dialect=None, **kwargs):
        '''Returns a tuple of a string containing the parametrised SELECT command (in the given SQL
        dialect) required to retrieve data from the SQL View represented by the SQLView, and the
        values to pass as parameters. Only the most basic form of WHERE clause is supported, with
        exact values for columns specified in the form of keyword arguments to the method.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        for field in kwargs:
            if not field in cls._fields:
                raise ValueError('Specified field {0} is not valid'.format(field))

        result = 'SELECT ' + cls._column_names_sql() + ' FROM ' + cls._qualified_view_name(dialect)
        if kwargs:
            result += ' WHERE '
            result += ' AND '.join((cls._fields[field].sql_name+'='+dialect.placeholder
                                    for field in kwargs))
        result += ';'

        values = [dialect.sql_repr(x) for x in kwargs.values()]

        return (result, values)

    @classmethod
    def _context_select_sql(cls, context, dialect=None, allow_unlimited=True):
        '''This method  takes a context dictionary of name:value pairs and identifies those
        SQLFields within the SQLView that would use the context values provided by any of those
        names. It then constructs an SQL statement using the column names of the identified
        SQLFields and returns that statement and the list of relevant values.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        result = 'SELECT ' + cls._column_names_sql() + ' FROM ' + cls._qualified_view_name(dialect)

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
            result += ' AND '.join((column+'='+dialect.placeholder for column in column_sql_names))

        result += ';'
        return (result, column_values)

# This constant records all the method and attribute names used in SQLRecord and SQLTable so that
# the metaclasses can detect any attempts to overwrite them in subclasses.

INVALID_SQLVIEW_NAMES = frozenset(dir(SQLView))
