'''This module defines the SQLRecordList class that holds references to multiple
SQLRecord values'''

import collections.abc

from . import records

class SQLRecordListField:

    def __init__(self, field):
        self.field = field

    def __get__(self, instance, owner):
        if instance:
            return self.get(instance)
        return self

    def get(self, instance):
        for record in instance._records:
            yield record.get(self.field)

    def get_context(self, instance, context):
        for record in instance._records:
            yield record.get(self.field, context)

class SQLRecordMetaClass(type):

    def __new__(mcs, name, bases, namespace, record_class=None, **kwds):

        if not issubclass(record_class, records.SQLRecord):
            raise ValueError('record_class parameter must refer to an SQLRecord subclass.')

        for field in record_class._fields:
            namespace[field] = SQLRecordListField(field)

        namespace['_record_class'] = record_class
        namespace['__slots__'] = ('_records',)

        return type.__new__(mcs, name, bases, namespace)

class SQLRecordList(metaclass=SQLRecordMetaClass, record_class=records.SQLRecord):

    def __init__(self, *args):
        self._records = []

        if len(args) == 1:
            if issubclass(args[0], collections.abc.Iterable):
                init_list = args[0]
        elif len(args) > 1:
            init_list = args
        else:
            init_list = []

        if init_list:
            for record in init_list:
                if not isinstance(record, self._record_class):
                    raise ValueError('Value must be an instance of {0}'
                                     .format(str(self._record_class.__name__)))
                self._records.append(record)

    def __len__(self):
        return len(self._records)

    def __getitem__(self, key):
        return self._records[key]

    def __setitem__(self, key, value):
        if not isinstance(value, self._record_class):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_class.__name__)))
        self._records[key] = value

    def __delitem__(self, key):
        del self._records[key]

    def __iter__(self):
        return iter(self._records)

    def __reverse__(self):
        return reversed(self._records)

    def append(self, value):
        if not isinstance(value, self._record_class):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_class.__name__)))
        self._records.append(value)

    def clear(self):
        self._records.clear()

    def copy(self):
        result = self.__class__()
        if self._records:
            result._records.extend((x.copy() for x in self._records))
        return result

    def extend(self, values):
        if all((isinstance(x, self._record_class) for x in values)):
            self._records.extend(values)
        else:
            raise ValueError('Values must be instances of {0}'
                             .format(str(self._record_class.__name__)))

    def insert(self, index, obj):
        if not isinstance(obj, self._record_class):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_class.__name__)))
        self._records.insert(index, obj)

    @property
    def record_class(self):
        return self._record_class

    def values(self, context=None):
        return [x.values(context) for x in self._records]

    def values_sql_repr(self, context=None, dialect=None):
        return [x.values_sql_repr(context, dialect) for x in self._records]
