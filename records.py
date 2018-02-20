

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
        namespace['_fields'] = _fields
        namespace['_field_count'] = len(slots)
        namespace['_table_name'] = table_name

        return type.__new__(mcs, name, bases, namespace)

class SQLRecord(metaclass=SQLRecordMetaClass):

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

    def fields(self):
        for k in self._fields.keys():
            yield self._fields[k]

    def values(self):
        for k in self._fields.keys():
            yield self.__getattribute__(self._fields[k]._slot_name)

    def items(self):
        for k in self._fields.keys():
            yield k, self._fields[k]

    def item_values(self):
        for k in self._fields.keys():
            yield k, self.__getattribute__(self._fields[k]._slot_name)

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self._fields.keys():
            result += k + ' : ' + str(self._fields[k]) + ', \n'
        result += '}'
        return result
