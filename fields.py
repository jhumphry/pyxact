'''The definition of classes that represent fields in a record and map between
SQL types and Python types'''


class SQLField:


    def __init__(self, py_type=None, sql_type=None, sql_name=None, nullable=True):
        if py_type is None:
            raise ValueError('Field cannot be instantiated without specifying py_type')
        self.__py_type__ = py_type
        if sql_type is None:
            raise ValueError('Field cannot be instantiated without specifying sql_type')
        self.__sqltype__ = sql_type
        self.__sqlname__ = sql_name
        self.__nullable__ = nullable
        self.__name__ = None
        self.__slotname__ = None

    def __set_name__(self, owner, name):
        self.__name__ = name
        self.__slotname__ = '_' + name
        if self.__sqlname__ is None:
            self.__sqlname__ = name

    def __set__(self, instance, value):
        if value is None:
            if self.__nullable__:
                instance.__setattr__(self.__slotname__, None)
            else:
                raise ValueError('''Field '{0}' can not be null.'''.format(self.__name__))

        elif not isinstance(value, self.__py_type__):
            raise ValueError('''Field '{0}' can only be set to values of type '{1}'.'''.format(self.__name__, str(self.__py_type__)))

        else:
            instance.__setattr__(self.__slotname__, value)


    def __get__(self, instance, owner):
        try:
            return instance.__getattribute__(self.__slotname__)
        except AttributeError:
            return None

    def __str__(self):
        return '{0} ({1} {2})'.format(self.__class__.__name__,
                                      self.__sqlname__,
                                      self.sql_type())

    def sql_type(self):
        return self.__sqltype__


class IntegerField(SQLField):


    def __init__(self, **kwargs):
        super().__init__(py_type=int, sql_type="INTEGER", **kwargs)
