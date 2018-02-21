'''The definition of classes that represent fields in a record and map between
SQL types and Python types'''


class SQLField:


    def __init__(self, py_type=None, sql_type=None, sql_name=None, nullable=True):
        if py_type is None:
            raise ValueError('Field cannot be instantiated without specifying py_type')
        self._py_type = py_type
        if sql_type is None:
            raise ValueError('Field cannot be instantiated without specifying sql_type')
        self._sql_type = sql_type
        self._sql_name = sql_name
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

        elif not isinstance(value, self._py_type):
            raise ValueError('''Field '{0}' can only be set to values of type '{1}'.'''.format(self._name, str(self._py_type)))

        else:
            instance.__setattr__(self._slot_name, value)


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
        if self._nullable:
            return self._sql_type
        else:
            return self._sql_type + ' NOT NULL'

    def sql_string_unsafe(self, value, dialect=None):
        return str(value)


class IntegerField(SQLField):

    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER", **kwargs)

class RowEnumField(SQLField):

    def __init__(self, context_name, starting_number=1, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER",
                         nullable=False, **kwargs)
        self._context_name=context_name
        self._starting_number=starting_number

    def __set__(self, instance, value):
        pass

    def __get__(self, instance, owner):
        if instance:
            return None
        else:
            return self

    def get_context(self, instance, context):
        if self._context_name in context:
            context[self._context_name]+=1
            return context[self._context_name]
        else:
            context[self._context_name]=self._starting_number
            return self._starting_number
