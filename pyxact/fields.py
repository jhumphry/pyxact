'''The definition of classes that represent fields in a record and map between
SQL types and Python types'''

import decimal

class SQLField:

    def __init__(self, py_type=None, sql_name=None, sql_ddl_options='', sql_type=None, nullable=True):
        self._py_type = py_type
        self._sql_name = sql_name
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
            try:
                return instance.__getattribute__(self._slot_name)
            except AttributeError:
                return None
        else:
            return self

    def __str__(self):
        return '{0} ({1} {2})'.format(self.__class__.__name__,
                                      self._sql_name,
                                      self.sql_type())

    def convert(self, value):
        raise ValueError

    def sql_repr(self, value, dialect):
        return value

    def get_context(self, instance, context):
        try:
            return instance.__getattribute__(self._slot_name)
        except AttributeError:
            return None

    @property
    def sql_name(self):
        return self._sql_name

    @property
    def nullable(self):
        return self._nullable

    def sql_type(self, dialect=None):
        return self._sql_type

    def sql_ddl(self, dialect=None):
        result = self._sql_name + ' ' + self.sql_type(dialect)
        if not self._nullable:
            result += ' NOT NULL'
        if self._sql_ddl_options != '':
            result += ' '+self._sql_ddl_options
        return result

    def sql_string_unsafe(self, value, dialect=None):
        return str(value)


class AbstractIntField(SQLField):

    def convert(self, value):
        if isinstance(value, str):
            return int(value)
        raise ValueError

class IntField(AbstractIntField):

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER", **kwargs)

class SmallIntField(AbstractIntField):

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="SMALLINT", **kwargs)

class BigIntField(AbstractIntField):

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="BIGINT", **kwargs)

class IDIntField(AbstractIntField):

    def __init__(self, context_name, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER",
                         nullable=True, **kwargs)
        self._context_name = context_name

    def get_context(self, instance, context):
        if self._context_name in context:
            return context[self._context_name]
        raise ValueError('''Required context '{0}' is not provided'''
                         .format(self._context_name))

class RowEnumIntField(AbstractIntField):

    def __init__(self, context_name, starting_number=1, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER",
                         nullable=True, **kwargs)
        self._context_name = context_name
        self._starting_number = starting_number

    def get_context(self, instance, context):
        if self._context_name in context:
            context[self._context_name] += 1
            return context[self._context_name]

        context[self._context_name] = self._starting_number
        return self._starting_number

class NumericField(SQLField):

    def __init__(self, precision, scale=0, allow_floats=False, **kwargs):
        super().__init__(py_type=decimal.Decimal, **kwargs)
        self._precision = precision
        self._scale = scale
        self._allow_floats = allow_floats

    def convert(self, value):
        if isinstance(value, str) or \
            isinstance(value, int) or \
            (isinstance(value, float) and self._allow_floats):
            return decimal.Decimal(value)
        raise ValueError

    def sql_repr(self, value, dialect):
        if dialect is None:
            return str(value)
        elif not dialect.native_decimals:
            return str(value)
        else:
            return value

    def sql_type(self, dialect=None):
        return 'NUMERIC({0}, {1})'.format(self._precision, self._scale)

class RealField(SQLField):

    def __init__(self, **kwargs):
        super().__init__(py_type=float, sql_type="REAL", **kwargs)

class BooleanField(SQLField):

    def __init__(self, **kwargs):
        super().__init__(py_type=bool, sql_type="BOOLEAN", **kwargs)

    def convert(self, value):
        if isinstance(value, int):
            return bool(value)
        raise ValueError

    def sql_repr(self, value, dialect):
        if dialect is None:
            return (1 if value else 0)
        elif not dialect.native_booleans:
            return (1 if value else 0)
        else:
            return value

class VarCharField(SQLField):

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

    def sql_type(self, dialect=None):
        return 'CHARACTER({0})'.format(self._max_length)

class TextField(SQLField):

    def __init__(self, **kwargs):
        super().__init__(py_type=str, sql_type="TEXT", **kwargs)
