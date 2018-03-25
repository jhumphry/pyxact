'''This module defines SQLField and subclasses - the class hierarchy that
defines and maps SQL types and values to Python types and values.'''

import datetime
import decimal
from . import dialects, ContextRequiredError

class SQLField:
    '''SQLField is an abstract class that forms the root of a hierarchy that
    defines the mapping between SQL types and values and their Python
    equivalents.'''

    def __init__(self, py_type=None, sql_name=None, context_used=None,
                 sql_ddl_options='', sql_type=None, nullable=True):
        self._py_type = py_type
        self.sql_name = sql_name
        self.context_used = context_used
        self._sql_ddl_options = sql_ddl_options
        self._sql_type = sql_type
        self.nullable = nullable
        self._name = None
        self._slot_name = None

    def __set_name__(self, owner, name):
        self._name = name
        self._slot_name = '_' + name
        if self.sql_name is None:
            self.sql_name = name

    def __set__(self, instance, value):
        if value is None:
            if self.nullable:
                instance.__setattr__(self._slot_name, None)
            else:
                raise TypeError('''Field '{0}' can not be null.'''.format(self._name))
        elif self._py_type is not None and isinstance(value, self._py_type):
            instance.__setattr__(self._slot_name, value)
        else:
            try:
                instance.__setattr__(self._slot_name, self.convert(value))
            except TypeError as te_raised:
                raise TypeError('''Field '{0}' cannot be set to value '{1}' of type '{2}.'''
                                .format(self._name, str(value), str(type(value)))) from te_raised

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__getattribute__(self._slot_name)
        return self

    def __str__(self):
        return '{0} ({1} {2})'.format(self.__class__.__name__,
                                      self.sql_name,
                                      self.sql_type())

    def convert(self, value):
        '''The convert method is called if __set__ is passed a value that is
        not of the type (if any) passed into the constructor under the py_type
        parameter. Subclasses may (but are not obliged to) attempt to convert
        the provided value in an appropriate type, and if they cannot they
        should raise TypeError.'''

        raise TypeError

    def get(self, instance):
        '''This method attempts to retrieve the associated value from the given
        instance.'''

        return instance.__getattribute__(self._slot_name)

    def get_context(self, instance, context):
        '''Given a particular context dictionary, this method attempts to
        retrieve the associated value from the given instance. Depending on the
        type of the field, this will either be from the given instance of the
        SQLField subclass or from the value in the context dictionary under the
        name of the field's context_usage parameter. In the latter case the
        value will also be stored in the instance.'''

        return instance.__getattribute__(self._slot_name)

    def update(self, instance, context, cursor, dialect=None):
        '''Given a (possibly partially-completed) context dictionary, a
        database cursor and a database dialect object, this method may retrieve
        data or do some other calculation in order to update the value in the
        SQLField instance. It should also return the value. The default action
        is to simply return the existing value unchanged.'''

        return instance.__getattribute__(self._slot_name)

    def sql_type(self, dialect=None):
        '''Returns the SQL definition of the data type of the field in the
        appropriate database dialect. It includes parameters (such as NUMERIC
        precision and scale) but not any column constraints such as NOT NULL.'''

        return self._sql_type

    def sql_ddl(self, dialect=None):
        '''Returns the SQL DDL text needed for CREATE TABLE commands'''

        result = self.sql_name + ' ' + self.sql_type(dialect)
        if not self.nullable:
            result += ' NOT NULL'
        if self._sql_ddl_options != '':
            result += ' '+self._sql_ddl_options
        return result

class AbstractIntField(SQLField):
    '''This is the root of the branch of the SQLField class hierarchy that
    represents those SQL types represented in Python by int.'''

    def convert(self, value):
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            return int(value)
        raise TypeError

class IntField(AbstractIntField):
    '''Represents an INTEGER field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type='INTEGER', **kwargs)

class SmallIntField(AbstractIntField):
    '''Represents a SMALLINT field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type='SMALLINT', **kwargs)

class BigIntField(AbstractIntField):
    '''Represents a BIGINT field in a database.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type='BIGINT', **kwargs)

class ContextIntField(AbstractIntField):
    '''Represents an INTEGER field in a database. When retrieved via
    get_context, the value returned will not be that stored in the SQLRecord
    instance, but will be retrieved from the context dictionary object passed
    in, under the context_name specified. This is intended for use for ID
    fields such as transaction ID, where on insertion a new value will be
    picked from a sequence and then used to relate records in different
    tables.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type='INTEGER',
                         nullable=True, **kwargs)

    def get_context(self, instance, context):

        if context is None:
            raise ContextRequiredError

        if self.context_used in context:
            setattr(instance, self._slot_name, context[self.context_used])
            return context[self.context_used]
        raise ContextRequiredError('''Required context '{0}' is not provided'''
                                   .format(self.context_used))

class RowEnumIntField(AbstractIntField):
    '''Represents an INTEGER field in a database. When retrieved via
    get_context, the value returned will not be that stored in the SQLRecord
    instance, but will be retrieved from the context dictionary object passed
    in, under the context_name specified, and the context dictionary will be
    updated to increment the value. This is intended to enumerate rows where
    multiple rows are being INSERTED into a table at once.'''

    def __init__(self, starting_number=1, **kwargs):
        super().__init__(py_type=int, sql_type='INTEGER',
                         nullable=True, **kwargs)
        self._starting_number = starting_number

    def get_context(self, instance, context):

        if context is None:
            raise ContextRequiredError

        if self.context_used in context:
            context[self.context_used] += 1
            setattr(instance, self._slot_name, context[self.context_used])
            return context[self.context_used]

        context[self.context_used] = self._starting_number
        setattr(instance, self._slot_name, self._starting_number)
        return self._starting_number

class NumericField(SQLField):
    '''Represents a NUMERIC field in a database, which maps to decimal.Decimal
    in Python. The scale and precision can be specified. Note that NUMERIC in
    SQL represents a fixed-point decimal representation, wherease
    decimal.Decimal in Python is a floating-point decimal representation. This
    field tries to ensure that any unrepresentable values will be caught before
    there is an attempt to write them to the database.'''

    def __init__(self, precision, scale=0,
                 allow_floats=False, inexact_quantize=False, rounding=None,
                 **kwargs):
        super().__init__(py_type=None, **kwargs)
        self.precision = precision
        self.scale = scale
        self.quantization = decimal.Decimal(1).scaleb(-scale)

        self.allow_floats = allow_floats
        self.inexact_quantize = inexact_quantize
        self.rounding = rounding

        traps = [decimal.InvalidOperation]
        if not allow_floats:
            traps.append(decimal.FloatOperation)
        if not inexact_quantize:
            traps.append(decimal.Inexact)

        self.decimal_context = decimal.Context(prec=precision,
                                               rounding=rounding,
                                               traps=traps)

    def convert(self, value):
        if isinstance(value, decimal.Decimal):
            return value.quantize(self.quantization, context=self.decimal_context)
        elif isinstance(value, (int, str)) or \
              (isinstance(value, float) and self.allow_floats):
            return decimal.Decimal(value).quantize(self.quantization, context=self.decimal_context)
        else:
            raise TypeError

    def sql_type(self, dialect=None):
        if (dialect and dialect.store_decimal_as_text) or \
            (not dialect and dialects.DefaultDialect.store_decimal_as_text):
            return 'TEXT'
        return 'NUMERIC({0}, {1})'.format(self.precision, self.scale)

class RealField(SQLField):
    '''Represents a REAL field in a database, which maps to float in Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=float, sql_type='REAL', **kwargs)

class BooleanField(SQLField):
    '''Represents a BOOLEAN field in a database, which maps to bool in
    Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=bool, sql_type='BOOLEAN', **kwargs)

    def convert(self, value):
        if isinstance(value, int):
            return bool(value)
        raise TypeError

class VarCharField(SQLField):
    '''Represents a VARCHAR field in a database, which maps to str in Python.
    The field has a maximum length max_length and it is selectable on
    initialisation whether values longer than this will be silently truncated,
    or will trigger an exception.'''

    def __init__(self, max_length, silent_truncate=False, **kwargs):
        super().__init__(py_type=None, **kwargs)
        self._max_length = max_length
        self._silent_truncate = silent_truncate

    def convert(self, value):
        if isinstance(value, str):
            if len(value) > self._max_length:
                if self._silent_truncate:
                    return value[0:self._max_length]
                else:
                    raise ValueError('''Field '{0}' can not accept strings longer than {1}.'''
                                     .format(self._name, self._max_length))
            else:
                return value

        else:
            raise TypeError

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
        super().__init__(py_type=str, sql_type='TEXT', **kwargs)

class TimestampField(SQLField):
    '''Represents a TIMESTAMP with or without time zone.'''

    def __init__(self, tz=True, **kwargs):
        sql_type = 'TIMESTAMP WITH{} TIME ZONE'.format(('' if tz else 'OUT'))
        super().__init__(py_type=None,
                         sql_type=sql_type,
                         **kwargs)
        self.tz = tz

    def convert(self, value):

        if isinstance(value, datetime.datetime):
            if (value.tzinfo is None and self.tz):
                raise ValueError('''Field '{0}' needs a datetime object with tzinfo.'''
                                 .format(self._name))
            if (value.tzinfo is not None and not self.tz):
                raise ValueError('''Field '{0}' needs a datetime object without tzinfo.'''
                                 .format(self._name))
            return value
        elif isinstance(value, str):
            if self.tz:
                dt_value = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f%z')
            else:
                dt_value = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')
            return dt_value
        else:
            raise TypeError

    def sql_type(self, dialect=None):
        if (dialect and dialect.store_date_time_datetime_as_text) or \
            (not dialect and dialects.DefaultDialect.store_date_time_datetime_as_text):
            return 'TEXT'
        return self._sql_type

class UTCNowTimestampField(TimestampField):
    '''Represents a TIMESTAMP WITHOUT TIME ZONE that represents a UTC
    timestamp. If update() is called on an instance of this class, the stored
    value in the associated SQLRecord or SQLTransaction will be set to the
    current time and date in UTC, according to the local system clock.'''

    def __init__(self, **kwargs):
        super().__init__(tz=False, **kwargs)

    def update(self, instance, context, cursor, dialect=None):
        now_utc = datetime.datetime.utcnow()
        setattr(instance, self._slot_name, now_utc)
        return now_utc

class DateField(SQLField):
    '''Represents a DATE.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=None,
                         sql_type='DATE',
                         **kwargs)

    def convert(self, value):
        if isinstance(value, datetime.date):
            return value
        elif isinstance(value, str):
            return datetime.datetime.strptime(value, '%Y-%m-%d').date()
        else:
            raise TypeError

    def sql_type(self, dialect=None):
        if (dialect and dialect.store_date_time_datetime_as_text) or \
            (not dialect and dialects.DefaultDialect.store_date_time_datetime_as_text):
            return 'TEXT'
        return self._sql_type

class TodayDateField(DateField):
    '''Represents a DATE. If update() is called on this class, the stored value
    will be set to the current date, according to the local system clock.'''

    def update(self, instance, context, cursor, dialect=None):
        today = datetime.date.today()
        setattr(instance, self._slot_name, today)
        return today

class TimeField(SQLField):
    '''Represents a TIME WITHOUT TIME ZONE. Note that while SQL supports the
    idea of a TIME values with time zone information, they are very hard to use
    without the associated date, so are not supported. '''

    def __init__(self, **kwargs):
        super().__init__(py_type=None,
                         sql_type='TIME WITHOUT TIME ZONE',
                         **kwargs)

    def convert(self, value):

        if isinstance(value, datetime.time):
            if value.tzinfo is not None:
                raise ValueError('''Field '{0}' needs a time object without tzinfo.'''
                                 .format(self._name))
            return value
        elif isinstance(value, str):
            dt_value = datetime.datetime.strptime(value, '%H:%M:%S.%f')
            return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()
        else:
            raise TypeError

    def sql_type(self, dialect=None):
        if (dialect and dialect.store_date_time_datetime_as_text) or \
            (not dialect and dialects.DefaultDialect.store_date_time_datetime_as_text):
            return 'TEXT'
        return self._sql_type

class UTCNowTimeField(TimeField):
    '''Represents a TIME WITHOUT TIME ZONE that represents a UTC time. If
    update() is called on an instance of this class, the stored value in the
    associated SQLRecord or SQLTransaction will be set to the current time in
    UTC, according to the local system clock.'''

    def update(self, instance, context, cursor, dialect=None):
        now_utc = datetime.datetime.utcnow().time()
        setattr(instance, self._slot_name, now_utc)
        return now_utc

class BlobField(SQLField):
    '''Represents a BLOB field in a database, which maps to bytes in Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=bytes, sql_type='BLOB', **kwargs)
