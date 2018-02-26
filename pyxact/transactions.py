'''This module defines Python types that map to SQL database tables.'''

from . import fields, records, recordlists

class SQLTransactionField:

    def __init__(self, record_type):

        if not isinstance(record_type, type):
            raise ValueError('record_type parameter must refer to an appropriate subclass.')

        if not issubclass(record_type, (records.SQLRecord,
                                        recordlists.SQLRecordList)):
            raise ValueError('record_type parameter must refer to an appropriate subclass.')

        self._record_type = record_type

    def __get__(self, instance, owner):
        if instance:
            return instance.__getattribute__(self._slot_name)
        return self

    def __set_name__(self, owner, name):
        self._name = name
        self._slot_name = '_' + name

    def __set__(self, instance, value):
        if isinstance(value, self._record_type):
            instance.__setattr__(self._slot_name, value)
        else:
            raise ValueError('Value must be an instance of {0}'
                             .format(str(self._record_type.__name__)))

class SQLTransactionMetaClass(type):

    def __new__(mcs, name, bases, namespace, **kwds):

        slots = []
        _sequences = dict()
        _fields = dict()
        _context_fields = dict()
        _records = dict()
        _recordlists = dict()

        for k in namespace:

            if isinstance(namespace[k], fields.SQLField):
                slots.append('_'+k)
                _context_fields[k] = namespace[k]
                _fields[k] = namespace[k]

            if isinstance(namespace[k], type):
                if issubclass(namespace[k], records.SQLRecord):
                    _records[k] = namespace[k]
                elif issubclass(namespace[k], recordlists.SQLRecordList):
                    _recordlists[k] = namespace[k]

                if issubclass(namespace[k], (records.SQLRecord,
                                             recordlists.SQLRecordList)):
                    slots.append('_'+k)
                    _fields[k] = namespace[k]
                    namespace[k] = SQLTransactionField(namespace[k])

        namespace['__slots__'] = tuple(slots)
        namespace['_fields_count'] = len(_fields)
        namespace['_fields'] = _fields
        namespace['_context_fields'] = _context_fields
        namespace['_records'] = _records
        namespace['_recordlists'] = _recordlists
        namespace['__slots__'] = slots

        return type.__new__(mcs, name, bases, namespace)


class SQLTransaction(metaclass=SQLTransactionMetaClass):

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

    def copy(self):
        result = self.__class__()
        for attr in self.__slots__:
            value = getattr(self, attr)
            if isinstance(value, (records.SQLRecord, recordlists.SQLRecordList)):
                setattr(result, attr, value.copy())
            else:
                setattr(result, attr, value)
        return result

    def get_context(self):
        result = dict()
        for i in self._context_fields:
            result[i] = getattr(self, i)
        return result

    def get_new_context(self, cursor, dialect):
        result = dict()
        for i in self._context_fields:
            if isinstance(self._context_fields[i], fields.SequenceIntField):
                value = self._context_fields[i].sequence().nextval(cursor, dialect)
                setattr(self, i, value)
                result[self._context_fields[i].context_name()] = value
            else:
                result[i] = getattr(self, i)
        return result

    def insert_existing(self, cursor, dialect):
        cursor.execute('BEGIN TRANSACTION;')
        context = self.get_context()

        for record_name in self._records:
            record = getattr(self, '_'+record_name)
            cursor.execute(record.insert_sql(context, dialect),
                           record.values_sql_repr(context, dialect))

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, '_'+recordlist_name)
            cursor.executemany(recordlist.record_class.insert_sql(context, dialect),
                               recordlist.values_sql_repr(context, dialect))

        cursor.execute('COMMIT TRANSACTION;')

    def insert_new(self, cursor, dialect):
        cursor.execute('BEGIN TRANSACTION;')
        context = self.get_new_context(cursor, dialect)

        for record_name in self._records:
            record = getattr(self, '_'+record_name)
            cursor.execute(record.insert_sql(context, dialect),
                           record.values_sql_repr(context, dialect))

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, '_'+recordlist_name)
            cursor.executemany(recordlist.record_class.insert_sql(context, dialect),
                               recordlist.values_sql_repr(context, dialect))

        cursor.execute('COMMIT TRANSACTION;')
