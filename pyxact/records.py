'''This module defines Python types that map to SQL database tables.'''

from . import fields, constraints

class SQLRecordMetaClass(type):
    '''This is a metaclass that automatically identifies the SQLField and
    SQLConstraint member attributes added to new subclasses and creates
    additional private attributes to help order and access them.'''

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, table_name=None, **kwds):

        slots = []
        _fields = dict()
        _constraints = dict()

        for k in namespace:
            if isinstance(namespace[k], fields.SQLField):
                slots.append('_'+k)
                _fields[k] = namespace[k]
            elif isinstance(namespace[k], constraints.SQLConstraint):
                _constraints[k] = namespace[k]

        namespace['__slots__'] = tuple(slots)
        namespace['_fields'] = _fields
        namespace['_field_count'] = len(slots)
        namespace['_constraints'] = _constraints
        namespace['_table_name'] = table_name

        return type.__new__(mcs, name, bases, namespace)

class SQLRecord(metaclass=SQLRecordMetaClass):
    '''SQLRecord maps SQL database tables to Python class types. It is not
    intended for direct use, but as an abstract class to be subclassed.'''

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != self._field_count:
                raise ValueError('{0} values required, {1} supplied.'
                                 .format(self._field_count, len(args)))

            for field, value in zip(self._fields.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self._fields:
                    raise ValueError('{0} is not a valid attribute name.'.format(key))
                setattr(self, key, value)

    def copy(self):
        '''Create a deep copy of an instance of an SQLRecord. If normal assignment
        is used, the copies will be shallow and changing the attributes on one instance
        will affect the other.'''

        result = self.__class__()
        for v in self.__slots__:
            setattr(result, v, getattr(self, v))
        return result

    def get(self, key, context=None):
        '''Get a value stored in an SQLField within this SQLRecord, given a
        context dictionary if appropriate.'''

        if context:
            return self._fields[key].get_context(self, context)
        return getattr(self, key)

    def set(self, key, value):
        '''Set a value stored in an SQLField within this SQLRecord.'''

        if key not in self._fields:
            raise ValueError('{0} is not a valid field name.'.format(key))
        setattr(self, key, value)

    @property
    def table_name(self):
        '''The name of the table used in SQL.'''

        return self._table_name

    @classmethod
    def fields(cls):
        '''Returns a list of SQLField objects in the order they were defined in
        the SQLRecord subclass.'''

        return [cls._fields[k] for k in cls._fields.keys()]

    def values(self, context=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that require one.'''

        return [self.get(k, context) for k in self._fields.keys()]

    def values_sql_repr(self, context=None, dialect=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that require one. The values are in the form required by
        the SQL database adaptor identified by dialect.'''

        return [self._fields[k].sql_repr(self.get(k, context), dialect)
                for k in self._fields.keys()]

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
        for k in self._fields.keys():
            value = self._fields[k].get_context(self, context)
            result.append(self._fields[k].sql_string_unsafe(value, dialect))
        return result

    @classmethod
    def items(cls):
        '''Returns a list of tuples of field names and SQLField objects in the
        order they were defined in the SQLRecord subclass.'''

        return [(k, cls._fields[k]) for k in cls._fields.keys()]

    def item_values(self, context=None):
        '''Returns a list of tuples of field names and values stored in the
        SQLField attributes of a particular SQLRecord instance. A context
        dictionary can be provided for SQLField types that require one.'''

        return [(k, self.get(k, context)) for k in self._fields.keys()]

    @classmethod
    def column_names_sql(cls, dialect=None):
        '''Returns a string containing a comma-separated list of column
        names, using a given SQL dialect.'''

        return ', '.join([cls._fields[x].sql_name for x in cls._fields.keys()])

    @classmethod
    def create_table_sql(cls, dialect=None):
        '''Returns a string containing the CREATE TABLE command (in the given
        SQL dialect) that will create the table defined by the SQLRecord.'''

        result = 'CREATE TABLE IF NOT EXISTS ' + cls._table_name + ' (\n    '
        table_columns = [cls._fields[k].sql_ddl(dialect) for k in cls._fields.keys()]
        table_constraints = [cls._constraints[k].sql_ddl(dialect) for k in cls._constraints.keys()]
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
            placeholder = '?'
        result = 'INSERT INTO ' + cls._table_name + ' ('
        result += cls.column_names_sql(dialect)
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
        result += self.column_names_sql(dialect)
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
            placeholder = '?'
        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result += ' WHERE '
            c = 1
            for f in kwargs:
                if not f in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(f))
                result += cls._fields[f].sql_name+'='
                result += placeholder
                if c < len(kwargs):
                    result += ' AND '
                c += 1
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
            c = 1
            for f, v in kwargs.items():
                if not f in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(f))
                result += cls._fields[f].sql_name+'='
                result += cls._fields[f].sql_string_unsafe(v)
                if c < len(kwargs):
                    result += ' AND '
                c += 1
        result += ';'
        return result

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self._fields.keys():
            result += k + ' : ' + str(self._fields[k]) + ', \n'
        result += '}'
        return result
