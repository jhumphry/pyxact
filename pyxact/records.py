'''This module defines Python types that map to SQL database tables.'''

from . import UnconstrainedWhereError
from . import fields, constraints, dialects

class SQLRecordMetaClass(type):
    '''This is a metaclass that automatically identifies the SQLField and
    SQLConstraint member attributes added to new subclasses and creates
    additional private attributes to help order and access them.'''

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, table_name=None, **kwds):

        namespace['_table_name'] = table_name

        slots = []
        _fields = dict()
        _constraints = dict()

        # Make a list of the SQLField and SQLConstraints attributes attached to the class and
        # check that the names won't be hiding any methods or attributes on the base class.

        for key, value in namespace.items():
            if isinstance(value, fields.SQLField):
                if key in INVALID_SQLRECORD_ATTRIBUTE_NAMES:
                    raise AttributeError('SQLField {} has the same name as an SQLRecord method or '
                                         'internal attribute'.format(key))
                slots.append('_'+key)
                _fields[key] = value
            elif isinstance(value, constraints.SQLConstraint):
                if key in INVALID_SQLRECORD_ATTRIBUTE_NAMES:
                    raise AttributeError('SQLConstraint {} has the same name as an SQLRecord method'
                                         'or internal attribute'.format(key))
                _constraints[key] = value

        namespace['__slots__'] = tuple(slots)
        namespace['_fields'] = _fields
        namespace['_field_count'] = len(slots)
        namespace['_constraints'] = _constraints

        # Now check the ColumnsConstraint for exant columns, fill out the sql_column_names
        # attribute on the constraint (which is not available when the constraint is instantiated,
        # check if the ColumnsConstraint might be a suitable superkey (a unique or PK constraint
        # that only references non-nullable columns), and finally fill out the sql_reference_names
        # to be the same as the sql_column_names (a 'natural join') if it is None on a
        # ForeignKeyConstraint.

        for key, value in _constraints.items():
            if isinstance(value, constraints.ColumnsConstraint):
                sql_column_names = []
                possible_superkey = True
                for column_name in value.column_names:
                    if column_name not in _fields:
                        raise AttributeError('SQLConstraint {} references non-existent column {}'
                                             .format(key, column_name))

                    if _fields[column_name].sql_name:
                        sql_column_names.append(_fields[column_name].sql_name)
                    else:
                        sql_column_names.append(column_name)
                    possible_superkey &= not _fields[column_name].nullable
                value.sql_column_names = tuple(sql_column_names)
                if isinstance(value, (constraints.UniqueConstraint,
                                      constraints.PrimaryKeyConstraint)):
                    value.superkey = possible_superkey
                if isinstance(value, constraints.ForeignKeyConstraint) and \
                        not value.sql_reference_names:
                    value.sql_reference_names = value.sql_column_names

        return type.__new__(mcs, name, bases, namespace)

class SQLRecord(metaclass=SQLRecordMetaClass):
    '''SQLRecord maps SQL database tables to Python class types. It is not
    intended for direct use, but as an abstract class to be subclassed.'''

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != self._field_count:
                raise ValueError('{0} values needed to initialise a {1} with a sequence, {2} supplied.'
                                 .format(self._field_count, self.__class__.__name__, len(args)))

            for field, value in zip(self._fields.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self._fields:
                    raise ValueError('{0} is not a valid attribute name for {1}.'
                                     .format(key, self.__class__.__name__))
                setattr(self, key, value)

    def __str__(self):
        result = self.__class__.__name__ + ':\n'
        for key in self._fields.keys():
            result += '- {0} ({1}) = {2}\n'.format(key,
                                                   self._fields[key].__class__.__name__,
                                                   str(getattr(self, key))
                                                  )
        return result

    def copy(self):
        '''Create a deep copy of an instance of an SQLRecord. If normal
        assignment is used, the copies will be shallow and changing the
        attributes on one instance will affect the other.'''

        result = self.__class__()
        for attribute in self.__slots__:
            setattr(result, attribute, getattr(self, attribute))
        return result

    def clear(self):
        '''Set all fields in the SQLRecord to None.'''

        for key in self._fields:
            setattr(self, key, None)

    def get(self, key, context=None):
        '''Get a value stored in an SQLField within this SQLRecord, given a
        context dictionary if appropriate.'''

        return self._fields[key].get_context(self, context)

    def set(self, key, value):
        '''Set a value stored in an SQLField within this SQLRecord.'''

        if key not in self._fields:
            raise ValueError('{0} is not a valid field name.'.format(key))
        setattr(self, key, value)

    def set_values(self, values):
        '''Set all fields within this SQLRecord.'''

        if len(values) != self._field_count:
            raise ValueError('{0} values required, {1} supplied.'
                             .format(self._field_count, len(values)))

        for field, value in zip(self._fields.keys(), values):
            setattr(self, field, value)

    @property
    def table_name(self):
        '''The name of the table used in SQL.'''

        return self._table_name

    @classmethod
    def fields(cls):
        '''Returns a list of SQLField objects in the order they were defined in
        the SQLRecord subclass.'''

        return [cls._fields[key] for key in cls._fields.keys()]

    def values(self, context=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that require one.'''

        return [self.get(key, context) for key in self._fields.keys()]

    def values_sql_repr(self, context=None, dialect=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that require one. The values are in the form required by
        the SQL database adaptor identified by dialect.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        return [dialect.sql_repr(self.get(key, context))
                for key in self._fields.keys()]

    def values_sql_string_unsafe(self, context, dialect=None):
        '''Returns a string containing a comma-separated list of values stored
        in the SQLField attributes of a particular SQLRecord instance. A
        context dictionary can be provided for SQLField types that require one.
        The values are in the form required by the SQL database identified by
        dialect.

        Note that this is not safe, as it is not guaranteed that any
        escaping performed will be sufficient to prevent SQL injection attacks.
        Do not use it with any values supplied by the user or previously stored
        in the database by the user.'''

        result = []
        for key in self._fields.keys():
            value = self._fields[key].get_context(self, context)
            result.append(self._fields[key].sql_string_unsafe(value, dialect))
        return result

    @classmethod
    def items(cls):
        '''Returns a list of tuples of field names and SQLField objects in the
        order they were defined in the SQLRecord subclass.'''

        return [(key, cls._fields[key]) for key in cls._fields.keys()]

    def item_values(self, context=None):
        '''Returns a list of tuples of field names and values stored in the
        SQLField attributes of a particular SQLRecord instance. A context
        dictionary can be provided for SQLField types that require one.'''

        return [(key, self.get(key, context)) for key in self._fields.keys()]

    @classmethod
    def column_names_sql(cls):
        '''Returns a string containing a comma-separated list of column names.'''

        return ', '.join([cls._fields[key].sql_name for key in cls._fields.keys()])

    @classmethod
    def create_table_sql(cls, dialect=None):
        '''Returns a string containing the CREATE TABLE command (in the given
        SQL dialect) that will create the table defined by the SQLRecord.'''

        result = 'CREATE TABLE IF NOT EXISTS ' + cls._table_name + ' (\n    '
        table_columns = [cls._fields[key].sql_ddl(dialect)
                         for key in cls._fields.keys()]
        table_constraints = [cls._constraints[key].sql_ddl(dialect)
                             for key in cls._constraints.keys()]
        result += ',\n    '.join(table_columns+table_constraints)
        result += '\n);'
        return result

    @classmethod
    def insert_sql(cls, context=None, dialect=None):
        '''Returns a string containing the parametrised INSERT command (in the
        given SQL dialect) required to insert data into the SQL table
        represented by the SQLRecord.'''

        if not context:
            context = {}
        if dialect:
            placeholder = dialect.placeholder
        else:
            placeholder = dialects.DefaultDialect.placeholder
        result = 'INSERT INTO ' + cls._table_name + ' ('
        result += cls.column_names_sql()
        result += ') VALUES ('
        if cls._field_count > 0:
            result += (placeholder+', ')*(cls._field_count-1)+placeholder
        result += ');'
        return result

    def insert_sql_unsafe(self, context=None, dialect=None):
        '''Returns a string containing the INSERT command (in the given SQL
        dialect) required to insert the values held by an SQL Record instance
        into the SQL table.

        Note that this is not safe, as it is not guaranteed that any escaping
        performed will be sufficient to prevent SQL injection attacks. Do not
        use it with any values supplied by the user or previously stored in the
        database by the user.'''

        if not context:
            context = {}
        result = 'INSERT INTO ' + self._table_name + ' ('
        result += self.column_names_sql()
        result += ') VALUES ('
        result += ', '.join(self.values_sql_string_unsafe(context, dialect))
        result += ');'
        return result

    @classmethod
    def simple_select_sql(cls, dialect=None, **kwargs):
        '''Returns a tuple of a string containing the parametrised SELECT command (in the
        given SQL dialect) required to retrieve data from the SQL table
        represented by the SQLRecord, and the values to pass as parameters.
        Only the most basic form of WHERE clause is supported, with exact
        values for columns specified in the form of keyword arguments to the
        method.'''

        if dialect:
            placeholder = dialect.placeholder
        else:
            placeholder = dialects.DefaultDialect.placeholder

        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result += ' WHERE '
            i = 1
            for field in kwargs:
                if not field in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(field))
                result += cls._fields[field].sql_name+'='
                result += placeholder
                if i < len(kwargs):
                    result += ' AND '
                i += 1
        result += ';'
        return (result, list(kwargs.values()))

    @classmethod
    def simple_select_sql_unsafe(cls, dialect=None, **kwargs):
        '''Returns a string containing the SELECT command (in the given SQL
        dialect) required to retrieve data from the SQL table represented by
        the SQLRecord. Only the most basic form of WHERE clause is supported,
        with exact values for columns specified in the form of keyword
        arguments to the method.

        Note that this is not safe, as it is not guaranteed that any escaping
        performed will be sufficient to prevent SQL injection attacks. Do not
        use it with any values supplied by the user or previously stored in the
        database by the user.'''

        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result += ' WHERE '
            i = 1
            for field, value in kwargs.items():
                if not field in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(field))
                result += cls._fields[field].sql_name+'='
                result += cls._fields[field].sql_string_unsafe(value)
                if i < len(kwargs):
                    result += ' AND '
                i += 1
        result += ';'
        return result

    @classmethod
    def context_select_sql(cls, context, dialect=None, allow_unlimited=True):
        '''This method  takes a context dictionary of name:value pairs and identifies those
        SQLFields within the SQLRecord that would use the context values provided by
        any of those names. It then constructs an SQL statement using the column names
        of the identified SQLFields and returns that statement and the list of relevant
        values.'''

        if dialect:
            placeholder = dialect.placeholder
        else:
            placeholder = dialects.DefaultDialect.placeholder
        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name

        column_sql_names = []
        column_values = []

        # This might be better with a set and intersection operation?

        for field_obj in cls._fields.values():
            field_ctxt = field_obj.context_used
            if field_ctxt in context:
                column_sql_names.append(field_obj.sql_name)
                column_values.append(context[field_ctxt])

        if not allow_unlimited and not column_sql_names:
            raise UnconstrainedWhereError('No WHERE clause generated - possible due to '
                                          'missing/misnamed context values?')

        if column_sql_names:
            result += ' WHERE '
            i = 1
            for column in column_sql_names:
                result += column+'='+placeholder
                if i < len(column_sql_names):
                    result += ' AND '
                i += 1
        result += ';'
        return (result, column_values)

    def get_context(self):
        '''Returns a dictionary containing all of the (non-None) context values
        that would be used by the fields in the record.'''

        context = {}

        for field_name, field_obj in self._fields.items():
            if field_obj.context_used:
                tmp = getattr(self, field_name)
                if tmp:
                    context[field_obj.context_used] = tmp

        return context

# This constant records all the method and attribute names used in SQLRecord so
# that SQLRecordMetaClass can detect any attempts to overwrite them in subclasses.

INVALID_SQLRECORD_ATTRIBUTE_NAMES = frozenset(dir(SQLRecord))
