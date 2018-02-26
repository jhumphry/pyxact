'''This module defines SQLField and subclasses - the class hierarchy that
defines and maps SQL types and values to Python types and values.'''

import decimal
from . import sequences

class SQLField:
    '''SQLField is an abstract class that forms the root of a hierarchy that
    defines the mapping between SQL types and values and their Python
    equivalents.'''

    def __init__(self, py_type=None, sql_name=None, context_used=None,
                 sql_ddl_options='', sql_type=None, nullable=True):
        self._py_type = py_type
        self._sql_name = sql_name
        self._context_used = context_used
        self._sql_ddl_options = sql_ddl_options
        self._sql_type = sql_type
        self._nullable = nullable
        self._name = None
        self._slot_name = None

    def __set_name__(self, owner, name):
        self._name = name
        self._slot_name = '_' + name
        if self._sql_name is None:
            self._sql_name = name

    def __set__(self, instance, value):
        if value is None:
            if self._nullable:
                instance.__setattr__(self._slot_name, None)
            else:
                raise ValueError('''Field '{0}' can not be null.'''.format(self._name))

        elif isinstance(value, self._py_type):
            instance.__setattr__(self._slot_name, value)

        else:
            try:
                instance.__setattr__(self._slot_name, self.convert(value))
            except ValueError as ve:
                raise ValueError('''Field '{0}' cannot be set to value '{1}' of type '{2}.'''
                                 .format(self._name, str(value), str(type(value)))) from ve

    def __get__(self, instance, owner):
        if instance:
            return instance.__getattribute__(self._slot_name)
        return self

    def __str__(self):
        return '{0} ({1} {2})'.format(self.__class__.__name__,
                                      self._sql_name,
                                      self.sql_type())

    def convert(self, value):
        '''The convert method is called if __set__ is passed a value that is
        not of the expected type. Subclasses may (but are not obliged to)
        attempt to convert the provided value into the type expected.'''

        raise ValueError

    def sql_repr(self, value, dialect):
        '''This method returns the value in the form expected by the particular
        database and database adaptor specified by the dialect parameter. It
        exists to handle cases where the database adaptor cannot accept the
        Python type being used - for example while SQL NUMERIC types map quite
        well to Python decimal.Decimal types, the sqlite3 database adaptor does
        not recognise them, so string values must be stored.'''

        return value

    def get_context(self, instance, context):
        '''Given a particular context dictionary, this method attempts to
        retrieve the associated value from a given instance of the class.'''

        return instance.__getattribute__(self._slot_name)

    @property
    def sql_name(self):
        '''This property represents the name used in the database SQL, which
        does not have to be the same as the name of the SQLRecord attribute in
        Python.'''

        return self._sql_name

    @property
    def context_used(self):
        '''This property represents the name of the context value that is used
        when retrieving the value via get_context, or it returns None.'''

        return self._context_used

    @property
    def nullable(self):
        '''This property indicates whether this value can be set to NULL/None.
        Note that the value may still be null if an SQLRecord has been created
        without initialising it.'''

        return self._nullable

    def sql_type(self, dialect=None):
        '''Returns the SQL definition of the data type of the field in the
        appropriate database dialect. It includes parameters (such as NUMERIC
        precision and scale) but not any column constraints such as NOT NULL.'''

        return self._sql_type

    def sql_ddl(self, dialect=None):
        '''Returns the SQL DDL text needed for CREATE TABLE commands'''

        result = self._sql_name + ' ' + self.sql_type(dialect)
        if not self._nullable:
            result += ' NOT NULL'
        if self._sql_ddl_options != '':
            result += ' '+self._sql_ddl_options
        return result

    def sql_string_unsafe(self, value, dialect=None):
        '''This method creates a string from the value for concatenation into
        SQL commands. This is not safe, as it is not guaranteed that any
        escaping performed will be sufficient to prevent SQL injection attacks.
        Do not use it with any values supplied by the user or previously stored
        in the database by the user.'''

        return str(value)


class AbstractIntField(SQLField):
    '''This is the root of the branch of the SQLField class hierarchy that
    represents those SQL types represented in Python by int.'''

    def convert(self, value):
        if isinstance(value, str):
            return int(value)
        raise ValueError

class IntField(AbstractIntField):
    '''Represents an INTEGER field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER", **kwargs)

class SmallIntField(AbstractIntField):
    '''Represents a SMALLINT field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="SMALLINT", **kwargs)

class BigIntField(AbstractIntField):
    '''Represents a BIGINT field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="BIGINT", **kwargs)

class IDIntField(AbstractIntField):
    '''Represents an INTEGER field in a database. When retrieved via
    get_context, the value returned will not be that stored in the SQLRecord
    instance, but will be retrieved from the context dictionary object passed
    in, under the context_name specified. This is intended for use for ID
    fields such as transaction ID, where on insertion a new value will be
    picked from a sequence and then used to relate records in different
    tables.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER",
                         nullable=True, **kwargs)

    def get_context(self, instance, context):
        if self._context_used in context:
            return context[self._context_used]
        raise ValueError('''Required context '{0}' is not provided'''
                         .format(self._context_used))

class SequenceIntField(AbstractIntField):

    def __init__(self, sequence, context_used=None, **kwargs):
        if not isinstance(sequence, sequences.SQLSequence):
            raise ValueError('Sequence provided must be an instance of pyxact.sequences.SQLSequence')
        self._sequence = sequence
        if context_used:
            super().__init__(py_type=int, context_used=context_used,
                             sql_type=self._sequence._index_type,
                             nullable=True, **kwargs)
        else:
            super().__init__(py_type=int, context_used=self._sequence.name,
                             sql_type=self._sequence._index_type,
                             nullable=True, **kwargs)

    @property
    def sequence(self):
        return self._sequence

    def get_context(self, instance, context):
        if self._context_used in context:
            return context[self._context_used]
        raise ValueError('''Required context value '{0}' from sequence '{1}' is not provided'''
                         .format(self._context_used, self._sequence._name))

class RowEnumIntField(AbstractIntField):
    '''Represents an INTEGER field in a database. When retrieved via
    get_context, the value returned will not be that stored in the SQLRecord
    instance, but will be retrieved from the context dictionary object passed
    in, under the context_name specified, and the context dictionary will be
    updated to increment the value. This is intended to enumerate rows where
    multiple rows are being INSERTED into a table at once.'''

    def __init__(self, starting_number=1, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER",
                         nullable=True, **kwargs)
        self._starting_number = starting_number

    def get_context(self, instance, context):
        if self._context_used in context:
            context[self._context_used] += 1
            return context[self._context_used]

        context[self._context_used] = self._starting_number
        return self._starting_number

class NumericField(SQLField):
    '''Represents a NUMERIC field in a database, which maps to decimal.Decimal
    in Python. The scale and precision can be specified.'''

    # TODO: enforce that the decimal.Decimal value stored in Python has the same
    # precision & scale as the SQL NUMERIC

    def __init__(self, precision, scale=0, allow_floats=False, **kwargs):
        super().__init__(py_type=decimal.Decimal, **kwargs)
        self._precision = precision
        self._scale = scale
        self._allow_floats = allow_floats

    def convert(self, value):
        if isinstance(value, (int, str)) or \
            (isinstance(value, float) and self._allow_floats):
            return decimal.Decimal(value)
        raise ValueError

    def sql_repr(self, value, dialect):
        if dialect is None:
            return str(value)
        elif not dialect.native_decimals:
            return str(value)
        return value

    def sql_type(self, dialect=None):
        return 'NUMERIC({0}, {1})'.format(self._precision, self._scale)

class RealField(SQLField):
    '''Represents a REAL field in a database, which maps to float in Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=float, sql_type="REAL", **kwargs)

class BooleanField(SQLField):
    '''Represents a BOOLEAN field in a database, which maps to bool in
    Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=bool, sql_type="BOOLEAN", **kwargs)

    def convert(self, value):
        if isinstance(value, int):
            return bool(value)
        raise ValueError

    def sql_repr(self, value, dialect):
        if dialect is None:
            return 1 if value else 0
        elif not dialect.native_booleans:
            return 1 if value else 0
        return value

class VarCharField(SQLField):
    '''Represents a VARCHAR field in a database, which maps to str in Python.
    The field has a maximum length max_length and it is selectable on
    initialisation whether values longer than this will be silently truncated,
    or will trigger an exception.'''

    def __init__(self, max_length, silent_truncate=False, **kwargs):
        super().__init__(py_type=str, **kwargs)
        self._max_length = max_length
        self._silent_truncate = silent_truncate

    def __set__(self, instance, value):
        if value is None:
            if self._nullable:
                instance.__setattr__(self._slot_name, None)
            else:
                raise ValueError('''Field '{0}' can not be null.'''.format(self._name))

        elif isinstance(value, str):
            if len(value) > self._max_length:
                if self._silent_truncate:
                    instance.__setattr__(self._slot_name, value[0:self._max_length])
                else:
                    raise ValueError('''Field '{0}' can not accept strings longer than {1}.'''
                                     .format(self._name, self._max_length))
            else:
                instance.__setattr__(self._slot_name, value)

        else:
            raise ValueError('''Field '{0}' cannot be set to value '{1}' of type '{2}.'''
                             .format(self._name, str(value), str(type(value))))

    def sql_type(self, dialect=None):
        return 'CHARACTER VARYING({0})'.format(self._max_length)

class CharField(VarCharField):
    '''Represents a VARCHAR field in a database, which maps to str in Python.
    The field has a maximum length max_length and it is selectable on
    initialisation whether values longer than this will be silently truncated,
    or will trigger an exception. Note that values shorter than the maximum
    length are likely to be padded with spaces, but these spaces may not be
    considered significant in SQL expressions.'''

    def sql_type(self, dialect=None):
        return 'CHARACTER({0})'.format(self._max_length)

class TextField(SQLField):
    '''Represents a TEXT field in a database, which maps to str in Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=str, sql_type="TEXT", **kwargs)
