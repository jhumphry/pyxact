'''This module defines the SQLRecordList class that holds references to multiple
SQLRecord values'''

import collections.abc

from . import records

class SQLRecordListField:
    '''SQLRecordListField is a special descriptor which is created for each of
    the SQLField attributes of the SQLRecord subclass that parametises the
    SQLRecordList. Attempting to retrieve the attribute from an instance of
    SQLRecordList returns a generator function that returns the value of the
    relevant SQLField for each of the SQLRecord contained in the list.'''

    def __init__(self, field):
        self.field = field

    def __get__(self, instance, owner):
        if instance:
            return self.get(instance)
        return self

    def get(self, instance):
        '''Return a generator giving the value of the SQLField for each of the
        underlying SQLRecord in turn.'''

        for record in instance._records:
            yield record.get(self.field)

    def get_context(self, instance, context):
        '''Return a generator giving the value of the SQLField for each of the
        underlying SQLRecord in turn, using the given context dictionary where
        appropriate.'''

        for record in instance._records:
            yield record.get(self.field, context)

class SQLRecordMetaClass(type):
    '''This metaclass ensures that SQLRecordList is only subclassed with a
    valid SQLRecord subclass as the record_type parameter. It also adds
    SQLRecordListField attributes for each of the SQLField attributes on the
    SQLRecord subclass.'''

    def __new__(mcs, name, bases, namespace, record_type=None, **kwds):

        if not issubclass(record_type, records.SQLRecord):
            raise ValueError('record_type parameter must refer to an SQLRecord subclass.')

        for field in record_type._fields:
            namespace[field] = SQLRecordListField(field)

        namespace['_record_type'] = record_type
        namespace['__slots__'] = ('_records',)

        return type.__new__(mcs, name, bases, namespace)

class SQLRecordList(metaclass=SQLRecordMetaClass, record_type=records.SQLRecord):
    '''SQLRecordList subclasses hold ordered lists of SQLRecord subclasses of a
    specified type. They implement many of the methods of the builtin list type
    and allow values to be retrieved in bulk. The metaclass creates attributes
    on the SQLRecordList subclass that match those on the underlying
    SQLRecord.'''

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
                if not isinstance(record, self._record_type):
                    raise ValueError('Value must be an instance of {0}'
                                     .format(str(self._record_type.__name__)))
                self._records.append(record)

    def __len__(self):
        return len(self._records)

    def __getitem__(self, key):
        return self._records[key]

    def __setitem__(self, key, value):
        if not isinstance(value, self._record_type):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_type.__name__)))
        self._records[key] = value

    def __delitem__(self, key):
        del self._records[key]

    def __iter__(self):
        return iter(self._records)

    def __reverse__(self):
        return reversed(self._records)

    def __str__(self):
        result = self.__class__.__name__ + '('
        result += self._record_type.__name__ + '):\n'
        for k in self._record_type._fields.keys():
            result += '- {0} ({1})\n'.format(k,
                                             self._record_type._fields[k].__class__.__name__
                                            )
        return result

    def append(self, value):
        '''Append value to the end of the list of SQLRecords.'''

        if not isinstance(value, self._record_type):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_type.__name__)))
        self._records.append(value)

    def clear(self):
        '''Clear the list of SQLRecords.'''

        self._records.clear()

    def copy(self):
        '''Create a deep-copy of the list by calling SQLRecord.copy() on each
        of the underlying records and adding them to a new SQLRecordList
        instantiation.'''

        result = self.__class__()
        if self._records:
            result._records.extend((x.copy() for x in self._records))
        return result

    def extend(self, values):
        '''Extend the SQLRecordList with the list of records found in values.'''

        if all((isinstance(x, self._record_type) for x in values)):
            self._records.extend(values)
        else:
            raise ValueError('Values must be instances of {0}'
                             .format(str(self._record_type.__name__)))

    def insert(self, index, obj):
        '''Insert SQLRecord obj at index position index.'''

        if not isinstance(obj, self._record_type):
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_type.__name__)))
        self._records.insert(index, obj)

    @property
    def record_type(self):
        '''Return the SQLRecord subclass that this SQLRecordList can contain.'''

        return self._record_type

    def values(self, context=None):
        '''Returns a list of lists of values stored in the SQLField attributes
        of the underlying SQLRecord instances. A context dictionary can be
        provided for SQLField types that require one.'''

        return [x.values(context) for x in self._records]

    def values_sql_repr(self, context=None, dialect=None):
        '''Returns a list of lists of values stored in the SQLField attributes
        of the underlying SQLRecord instances. A context dictionary can be
        provided for SQLField types that require one. The values are in the
        form required by the SQL database adaptor identified by the dialect
        parameter.'''

        return [x.values_sql_repr(context, dialect) for x in self._records]
