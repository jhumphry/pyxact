'''This module defines SQLField and subclasses - the class hierarchy that defines and maps SQL
types and values to Python types and values.'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import datetime
import decimal
from . import dialects, ContextRequiredError

class SQLField:
    '''SQLField is an abstract class that forms the root of a hierarchy that
    defines the mapping between SQL types and values and their Python
    equivalents.'''

    def __init__(self, py_type=None, sql_name=None, context_used=None, query=None,
                 sql_ddl_options='', sql_type=None, nullable=True):
        self.py_type = py_type
        self.sql_name = sql_name
        self.context_used = context_used
        self.query = query
        self._sql_ddl_options = sql_ddl_options
        self._sql_type = sql_type
        self.nullable = nullable
        self.name = None
        self.slot_name = None

    def __set_name__(self, owner, name):
        self.name = name
        self.slot_name = '_' + name
        if self.sql_name is None:
            self.sql_name = name

    def __set__(self, instance, value):
        if value is None:
            if self.nullable:
                instance.__setattr__(self.slot_name, None)
            else:
                raise TypeError('''Field '{0}' can not be null.'''.format(self.name))
        elif self.py_type is not None and isinstance(value, self.py_type):
            instance.__setattr__(self.slot_name, value)
        else:
            try:
                instance.__setattr__(self.slot_name, self.convert(value))
            except TypeError as te_raised:
                raise TypeError('''Field '{0}' cannot be set to value '{1}' of type '{2}.'''
                                .format(self.name, str(value), str(type(value)))) from te_raised

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__getattribute__(self.slot_name)
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

        return instance.__getattribute__(self.slot_name)

    def get_context(self, instance, context):
        '''This method retrieves the appropriate value for a field given an instance of an
        SQLRecord class and a context dictionary. If the field was created with the 'context_used'
        parameter set and a context dictionary has been provided, it will use the value in the
        context dictionary and set the value in the SQLRecord instance to equal it. Otherwise it
        will return the value currently stored in the SQLRecord instance.'''

        if self.context_used is None:
            return instance.__getattribute__(self.slot_name)

        if context is None:
            raise ContextRequiredError

        if self.context_used in context:
            context_value = context[self.context_used]
            self.__set__(instance, context_value)
            return context_value

        raise ContextRequiredError('''Required context '{0}' is not provided'''
                                   .format(self.context_used))

    def refresh(self, instance, context, cursor):
        '''Given a (possibly partially-completed) context dictionary and a database cursor, this
        method may retrieve data or do some other calculation in order to find the value in the
        SQLField instance and also return that value. The refresh method should not modify the
        database in any way or generate new values, so sequences should not be incremented and
        timestamps not updated. The default action where the query parameter was set is to execute
        that query and update and return the single-value result. The default action where no query
        parameter was supplied is to simply return the existing value unchanged.'''

        if self.query is None:
            return instance.__getattribute__(self.slot_name)

        query = self.query()
        query._set_context(context)
        query._execute(cursor)
        value = query._result_singlevalue(cursor)
        self.__set__(instance, value)
        return value

    def update(self, instance, context, cursor):
        '''Given a (possibly partially-completed) context dictionary and a database cursor, this
        method may retrieve data or do some other calculation in order to update the value in the
        SQLField instance and also return the value. Unlike the refresh method, the update method
        may modify the database in some way (such as for sequences) or generate a 'new' value (such
        as for timestamps). The default action is to simply call the refresh method.'''

        return self.refresh(instance, context, cursor)

    def sql_type(self):
        '''Returns the SQL definition of the data type of the field in the
        appropriate database dialect. It includes parameters (such as NUMERIC
        precision and scale) but not any column constraints such as NOT NULL.'''

        return self._sql_type

    def sql_ddl(self):
        '''Returns the SQL DDL text needed for CREATE TABLE commands'''

        result = self.sql_name + ' ' + self.sql_type()
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

class RowEnumIntField(AbstractIntField):
    '''Represents an INTEGER field in a database. When retrieved via get_context, the value
    returned will not be that stored in the SQLRecord instance, but will be retrieved from the
    context dictionary object passed in, under the context_name specified, and the context
    dictionary will be updated to increment the value. This is intended to enumerate rows where
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
            setattr(instance, self.slot_name, context[self.context_used])
            return context[self.context_used]

        context[self.context_used] = self._starting_number
        setattr(instance, self.slot_name, self._starting_number)
        return self._starting_number

class NumericField(SQLField):
    '''Represents a NUMERIC field in a database, which maps to decimal.Decimal in Python. The scale
    and precision can be specified. Note that NUMERIC in SQL represents a fixed-point decimal
    representation, wherease decimal.Decimal in Python is a floating-point decimal representation.
    This field tries to ensure that any unrepresentable values will be caught before there is an
    attempt to write them to the database.'''

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

    def sql_type(self):
        if dialects.DefaultDialect.store_decimal_as_text:
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
                                     .format(self.name, self._max_length))
            else:
                return value

        else:
            raise TypeError

    def sql_type(self):
        return 'CHARACTER VARYING({0})'.format(self._max_length)

class CharField(VarCharField):
    '''Represents a VARCHAR field in a database, which maps to str in Python.
    The field has a maximum length max_length and it is selectable on
    initialisation whether values longer than this will be silently truncated,
    or will trigger an exception. Note that values shorter than the maximum
    length are likely to be padded with spaces, but these spaces may not be
    considered significant in SQL expressions.'''

    def sql_type(self):
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
                                 .format(self.name))
            if (value.tzinfo is not None and not self.tz):
                raise ValueError('''Field '{0}' needs a datetime object without tzinfo.'''
                                 .format(self.name))
            return value
        elif isinstance(value, str):
            if self.tz:
                dt_value = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f%z')
            else:
                dt_value = datetime.datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')
            return dt_value
        else:
            raise TypeError

    def sql_type(self):
        if dialects.DefaultDialect.store_date_time_datetime_as_text:
            return 'TEXT'
        return self._sql_type

class UTCNowTimestampField(TimestampField):
    '''Represents a TIMESTAMP WITHOUT TIME ZONE that represents a UTC
    timestamp. If update() is called on an instance of this class, the stored
    value in the associated SQLRecord or SQLTransaction will be set to the
    current time and date in UTC, according to the local system clock.'''

    def __init__(self, **kwargs):
        super().__init__(tz=False, **kwargs)

    def update(self, instance, context, cursor):
        now_utc = datetime.datetime.utcnow()
        setattr(instance, self.slot_name, now_utc)
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

    def sql_type(self):
        if dialects.DefaultDialect.store_date_time_datetime_as_text:
            return 'TEXT'
        return self._sql_type

class TodayDateField(DateField):
    '''Represents a DATE. If update() is called on this class, the stored value
    will be set to the current date, according to the local system clock.'''

    def update(self, instance, context, cursor):
        today = datetime.date.today()
        setattr(instance, self.slot_name, today)
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
                                 .format(self.name))
            return value
        elif isinstance(value, str):
            return datetime.datetime.strptime(value, '%H:%M:%S.%f').time()
        else:
            raise TypeError

    def sql_type(self):
        if dialects.DefaultDialect.store_date_time_datetime_as_text:
            return 'TEXT'
        return self._sql_type

class UTCNowTimeField(TimeField):
    '''Represents a TIME WITHOUT TIME ZONE that represents a UTC time. If
    update() is called on an instance of this class, the stored value in the
    associated SQLRecord or SQLTransaction will be set to the current time in
    UTC, according to the local system clock.'''

    def update(self, instance, context, cursor):
        now_utc = datetime.datetime.utcnow().time()
        setattr(instance, self.slot_name, now_utc)
        return now_utc

class BlobField(SQLField):
    '''Represents a BLOB field in a database, which maps to bytes in Python.'''

    def __init__(self, **kwargs):
        super().__init__(py_type=bytes, sql_type='BLOB', **kwargs)
