'''This module defines Python types that map to SQL database tables.'''

from . import VerificationError
from . import fields, records, recordlists, dialects, queries

class SQLTransactionField:
    '''SQLTransactionField wraps an SQLRecord or SQLRecordList subclass for
    incorporating into a new SQLTransaction subclass. It ensures that only the
    correct subclass type can be assigned to the attribute.'''

    def __init__(self, record_type):

        if not isinstance(record_type, type):
            raise ValueError('record_type parameter must refer to an appropriate subclass.')

        if not issubclass(record_type, (records.SQLRecord,
                                        recordlists.SQLRecordList)):
            raise ValueError('record_type parameter must refer to an appropriate subclass.')

        self._record_type = record_type
        self._name = None
        self._slot_name = None

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
    '''This metaclass identifies all of the special attributes created in an
    SQLTransaction subclass and creates various internal dictionaries and
    indexes to them.'''

    def __new__(mcs, name, bases, namespace, **kwds):

        slots = []
        _fields = dict()
        _context_fields = dict()
        _records = dict()
        _recordlists = dict()

        for k in namespace:

            if isinstance(namespace[k], fields.SQLField):
                if k in INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES:
                    raise AttributeError('SQLField {} has the same name as an SQLTransaction'
                                         ' method or internal attribute'.format(k))
                slots.append('_'+k)
                _context_fields[k] = namespace[k]
                _fields[k] = namespace[k]

            elif isinstance(namespace[k], SQLTransactionField):
                if k in INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES:
                    raise AttributeError('SQLTransactionField {} has the same name as an '
                                         'SQLTransaction method or internal attribute'.format(k))
                if issubclass(namespace[k]._record_type, records.SQLRecord):
                    _records[k] = namespace[k]
                elif issubclass(namespace[k]._record_type, recordlists.SQLRecordList):
                    _recordlists[k] = namespace[k]

                slots.append('_'+k)
                _fields[k] = namespace[k]

        namespace['__slots__'] = tuple(slots)
        namespace['_fields_count'] = len(_fields)
        namespace['_fields'] = _fields
        namespace['_context_fields'] = _context_fields
        namespace['_records'] = _records
        namespace['_recordlists'] = _recordlists
        namespace['__slots__'] = slots

        return type.__new__(mcs, name, bases, namespace)


class SQLTransaction(metaclass=SQLTransactionMetaClass):
    '''SQLTransaction is an abstract class that can be subclassed to represent
    different types of database transaction. It groups together SQLRecord and
    SQLRecordList subclasses (wrapped in SQLTransactionField) that represent
    rows from different tables in the database that must all be inserted or
    retrieved in a single transaction.

    It can also contain attributes of type SQLField. These are called the
    context fields and represent values (such as transaction numbers) that must
    be consistent accross the appropriate fields within the SQLRecord and
    SQLRecordList.'''


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

    def __str__(self):
        result = self.__class__.__name__ + ':\n'

        for field_name in self._context_fields:
            result += '* {0} ({1}) = {2}\n'.format(field_name,
                                                   self._context_fields[field_name].__class__.__name__,
                                                   str(getattr(self, field_name))
                                                  )

        for record_name in self._records:
            result += '* {0} '.format(record_name)
            result += str(getattr(self, record_name))

        for recordlist_name in self._recordlists:
            result += '* {0} '.format(recordlist_name)
            result += str(getattr(self, recordlist_name))

        return result

    def copy(self):
        '''Create a deep copy of an instance of an SQLTransaction. If normal
        assignment is used, the copies will be shallow and changing the
        attributes on one instance will affect the other.'''

        result = self.__class__()
        for attr in self.__slots__:
            value = getattr(self, attr)
            if isinstance(value, (records.SQLRecord, recordlists.SQLRecordList)):
                setattr(result, attr, value.copy())
            else:
                setattr(result, attr, value)
        return result

    def verify(self):
        '''Return a boolean indicating whether this SQLTransaction meets
        internal consistency requirements (i.e. those that do not require
        database access). If True is returned, the SQLTransaction should be
        suitable for insertion into the database (although the database may
        still reject it based on, for example, foreign key violations). For
        SQLTransaction itself, this method will always return true, but
        subclasses may add conditions appropriate to the domain for which
        SQLTransaction has been subclassed.'''

        return True

    def normalize(self):
        '''Normalize a record that may have fixable inconsistencies. This
        method is called at the end of the context_select method. After a call
        to this method, the verify method should return True if this is at all
        possible to achieve. The precise meaning of 'normalization' is
        dependent on the domain for which SQLTransaction has been subclassed.
        The default implementation for SQLTransaction does nothing.'''

        pass

    def get_context(self):
        '''Return a context dictionary created from the values stored under the
        names of the SQLField objects directly attached as attributes to the
        SQLTransaction. This does not update sequences or perform any database
        access.'''

        result = dict()
        for i in self._context_fields:
            result[i] = getattr(self, i)
        return result

    def get_new_context(self, cursor, dialect=None):
        '''Return a context dictionary created from the SQLField objects
        directly attached as attributes to the SQLTransaction. This method will
        identify SequenceIntFields and QueryFields, and call the database to
        update the values.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        result = dict()
        for field_name, field in self._context_fields.items():
            if isinstance(field, fields.SequenceIntField):
                value = field.sequence.nextval(cursor, dialect)
                setattr(self, field_name, value)
            elif isinstance(field, queries.QueryField):
                query = field.query(**result)
                query.execute(cursor, dialect)
                value = query.result_singlevalue(cursor)
                setattr(self, field_name, value)
            result[field_name] = getattr(self, field_name)
        return result

    def get_context_from_records(self):
        '''This method makes the context dictionary by scanning the SQLRecord
        and SQLTransactionRecord contained in SQLTransactionField attributes
        and working out what context name:value pairs are consistent with them.
        For example if an SQLRecord attached to the SQLTransaction has a
        IDIntField equal to 37 that uses a context value named trans_id, then
        the returned context dictionary should have 37 stored under the name
        trans_id. Does NOT attempt to identify if there are inconsistencies
        between rows.'''

        context = {}

        for record_name in self._records:
            record = getattr(self, record_name)
            context.update(record.get_context())

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, recordlist_name)
            for record in recordlist:
                context.update(record.get_context())

        return context

    def set_context_from_records(self):
        '''This method sets the context fields using the dictionary returned
        by get_context_from_records.'''

        context = self.get_context_from_records()

        for field in self._context_fields:
            if field in context:
                setattr(self, field, context[field])

    def insert_existing(self, cursor, dialect=None):
        '''Insert the contents of the SQLTransaction into the database. This
        method stores only the existing data and will not update any values
        that are linked to sequences in the database.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        cursor.execute('BEGIN TRANSACTION;')
        context = self.get_context()

        if not self.verify():
            raise VerificationError

        for record_name in self._records:
            record = getattr(self, record_name)
            cursor.execute(record.insert_sql(context, dialect),
                           record.values_sql_repr(context, dialect))

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, recordlist_name)
            cursor.executemany(recordlist.record_type.insert_sql(context, dialect),
                               recordlist.values_sql_repr(context, dialect))

        cursor.execute('COMMIT TRANSACTION;')

    def insert_new(self, cursor, dialect=None):
        '''Insert the contents of the SQLTransaction into the database. This
        method will update any values that are linked to sequences or queries
        in the database and then check that the verify method returns True
        before proceeding.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        cursor.execute('BEGIN TRANSACTION;')
        context = self.get_new_context(cursor, dialect)

        if not self.verify():
            raise VerificationError

        for record_name in self._records:
            record = getattr(self, record_name)
            cursor.execute(record.insert_sql(context, dialect),
                           record.values_sql_repr(context, dialect))

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, recordlist_name)
            cursor.executemany(recordlist.record_type.insert_sql(context, dialect),
                               recordlist.values_sql_repr(context, dialect))

        cursor.execute('COMMIT TRANSACTION;')

    def context_select(self, cursor, dialect=None):
        '''This method extracts the values stored in SQLField directly attached
        to the SQLTransaction and stored them in a context dictionary under the
        name of the attribute. It then attempts to use this dictionary to
        retrieve all of the SQLRecord and SQLRecordList objects stored in
        SQLTransactionField attributes. The normalise method is then called,
        followed by the verify method to check that the result meets internal
        consistency requirements.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        cursor.execute('BEGIN TRANSACTION;')
        context = self.get_context()

        for record_name, record_field in self._records.items():
            record_type = record_field._record_type

            record = getattr(self, record_name)
            if record is None:
                record = record_type()
                setattr(self, record_name, record)

            cursor.execute(*record_type.context_select_sql(context,
                                                           dialect,
                                                           allow_unlimited=False))
            nextrow = cursor.fetchone()
            if nextrow:
                record.set_values(nextrow)
            else:
                record.clear()

        for recordlist_name, recordlist_field in self._recordlists.items():
            recordlist_type = recordlist_field._record_type
            record_type = recordlist_type._record_type

            recordlist = getattr(self, recordlist_name)
            if recordlist is None:
                recordlist = recordlist_type()
                setattr(self, recordlist_name, recordlist)

            recordlist.clear()

            cursor.execute(*record_type.context_select_sql(context,
                                                           dialect,
                                                           allow_unlimited=False))
            nextrow = cursor.fetchone()
            while nextrow:
                recordlist.append(record_type(*nextrow))
                nextrow = cursor.fetchone()

        cursor.execute('COMMIT TRANSACTION;')

        self.normalize()

        if not self.verify():
            raise VerificationError

# This constant records all the method and attribute names used in
# SQLTransaction so that SQLTransactionMetaClass can detect any attempts to
# overwrite them in subclasses.

INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES = frozenset(dir(SQLTransaction))
