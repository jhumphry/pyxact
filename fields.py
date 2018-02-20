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
        try:
            return instance.__getattribute__(self._slot_name)
        except AttributeError:
            return None

    def __str__(self):
        return '{0} ({1} {2})'.format(self.__class__.__name__,
                                      self._sql_name,
                                      self.sql_type())

    def sql_type(self):
        return self._sql_type


class IntegerField(SQLField):


    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER", **kwargs)
