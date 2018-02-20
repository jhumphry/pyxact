

import fields

class SQLRecordMetaClass(type):

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, table_name=None, **kwds):

        slots = []
        _fields = dict()

        for k in namespace:
            if isinstance(namespace[k], fields.SQLField):
                slots.append('_'+k)
                _fields[k] = namespace[k]

        namespace['__slots__'] = tuple(slots)
        namespace['__fields__'] = _fields
        namespace['__field_count__'] = len(slots)
        namespace['__tablename__'] = table_name

        return type.__new__(mcs, name, bases, namespace)

class SQLRecord(metaclass=SQLRecordMetaClass):

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != self.__field_count__:
                raise ValueError('{0} values required, {1} supplied.'
                                 .format(self.__field_count__, len(args)))

            for field, value in zip(self.__fields__.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self.__fields__:
                    raise ValueError('{0} is not a valid attribute name.'.format(key))
                setattr(self, key, value)

    def fields(self):
        for k in self.__fields__.keys():
            yield self.__fields__[k]

    def values(self):
        for k in self.__fields__.keys():
            yield self.__getattribute__(self.__fields__[k].__slotname__)

    def items(self):
        for k in self.__fields__.keys():
            yield k, self.__fields__[k]

    def item_values(self):
        for k in self.__fields__.keys():
            yield k, self.__getattribute__(self.__fields__[k].__slotname__)

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self.__fields__.keys():
            result += k + ' : ' + str(self.__fields__[k]) + ', \n'
        result += '}'
        return result
