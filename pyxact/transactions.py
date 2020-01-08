'''This module defines Python types that map to SQL database tables.'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from . import VerificationError
from . import dialects, fields, recordlists, records

class SQLTransactionField:
    '''SQLTransactionField wraps an SQLRecord or SQLRecordList subclass for
    incorporating into a new SQLTransaction subclass. It ensures that only the
    correct subclass type can be assigned to the attribute.'''

    def __init__(self, record_type):

        if not isinstance(record_type, type):
            raise TypeError('record_type parameter must refer to an appropriate subclass.')

        if not issubclass(record_type, (records.SQLRecord,
                                        recordlists.SQLRecordList)):
            raise TypeError('record_type parameter must refer to an appropriate subclass.')

        if issubclass(record_type, recordlists.SQLRecordList) and \
           not issubclass(record_type._record_type, records.SQLRecord):
            raise TypeError('record_type parameter which are SQLRecordList must be able to contain'
                            ' an appropriate subclass.')

        self._record_type = record_type
        self._name = None
        self._slot_name = None

    def __get__(self, instance, owner):
        if instance is not None:
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

INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES = frozenset()

class SQLTransactionMetaClass(type):
    '''This metaclass identifies all of the special attributes created in an
    SQLTransaction subclass and creates various internal dictionaries and
    indexes to them.'''

    def __new__(mcs, name, bases, namespace, version=None, isolation_level=None, **kwds):

        slots = []
        _fields = dict()
        _context_fields = dict()
        _records = dict()
        _recordlists = dict()

        # Inherit any attributes on base classes

        for i in bases:
            if issubclass(i, SQLTransaction):
                slots.extend(i.__slots__)
                _fields.update(i._fields)
                _context_fields.update(i._context_fields)
                _records.update(i._records)
                _recordlists.update(i._recordlists)

        # Check names on attributes and add them to the appropriate internal
        # indices

        for k in namespace:

            if isinstance(namespace[k], (fields.SQLField, SQLTransactionField)) and \
                    k in INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES:
                raise AttributeError('Attribute {} has the same name as an SQLTransaction '
                                     'method or internal attribute'.format(k))

            if isinstance(namespace[k], fields.SQLField):
                slots.append('_'+k)
                _context_fields[k] = namespace[k]
                _fields[k] = namespace[k]

            elif isinstance(namespace[k], SQLTransactionField):
                if issubclass(namespace[k]._record_type, records.SQLRecord):
                    _records[k] = namespace[k]
                elif issubclass(namespace[k]._record_type, recordlists.SQLRecordList):
                    _recordlists[k] = namespace[k]

                slots.append('_'+k)
                _fields[k] = namespace[k]

        namespace['_version'] = version
        namespace['_isolation_level'] = isolation_level
        namespace['__slots__'] = tuple(slots)
        namespace['_field_count'] = len(_fields)
        namespace['_fields'] = _fields
        namespace['_context_fields'] = _context_fields
        namespace['_records'] = _records
        namespace['_recordlists'] = _recordlists
        namespace['__slots__'] = slots

        return type.__new__(mcs, name, bases, namespace)


class SQLTransaction(metaclass=SQLTransactionMetaClass):
    '''SQLTransaction is an abstract class that can be subclassed to represent
    different types of database transaction. It groups together SQLTable and
    SQLRecordList subclasses (wrapped in SQLTransactionField) that represent
    rows from different tables in the database that must all be inserted or
    retrieved in a single transaction.

    It can also contain attributes of type SQLField. These are called the
    context fields and represent values (such as transaction numbers) that must
    be consistent accross the appropriate fields within the SQLTable and
    SQLRecordList.'''


    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        for recordlist_name, recordlist_field in self._recordlists.items():
            recordlist_type = recordlist_field._record_type
            setattr(self, recordlist_name, recordlist_field._record_type())

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
            field_class_name = self._context_fields[field_name].__class__.__name__
            result += '* {0} ({1}) = {2}\n'.format(field_name,
                                                   field_class_name,
                                                   str(getattr(self, field_name))
                                                  )

        for record_name in self._records:
            result += '* {0} '.format(record_name)
            result += str(getattr(self, record_name))

        for recordlist_name in self._recordlists:
            result += '* {0} '.format(recordlist_name)
            result += str(getattr(self, recordlist_name))

        return result

    def _copy(self):
        '''Create a deep copy of an instance of an SQLTransaction. If normal
        assignment is used, the copies will be shallow and changing the
        attributes on one instance will affect the other.'''

        result = self.__class__()
        for attr in self.__slots__:
            value = getattr(self, attr)
            if isinstance(value, (records.SQLRecord, recordlists.SQLRecordList)):
                setattr(result, attr, value._copy())
            else:
                setattr(result, attr, value)
        return result

    def _verify(self):
        '''Indicates whether this SQLTransaction meets internal consistency requirements (i.e.
        those that do not require database access). If True is returned, the SQLTransaction should
        be suitable for insertion into the database (although the database may still reject it
        based on, for example, foreign key violations). For SQLTransaction itself, this method will
        always return True, but subclasses may add conditions appropriate to the domain for which
        SQLTransaction has been subclassed. If False or a string is returned, VerificationError
        will be raised.'''

        return True

    def _post_select_hook(self, context, cursor):
        '''This method is called at the end of the context_select method and indicates if the
        record is consistent with the database. If False or a string is returned, VerificationError
        will be raised. This method is in addition to any internal consistency checks performed by
        the _verify method.

        The use of this hook is dependent on the domain for which SQLTransaction
        has been subclassed. The default implementation for SQLTransaction retrieves all the
        context information from the SQLTable attached to the class and updates the context fields.
        This may be useful where only some of the context fields were necessary to identify the
        records to retrieve, but having the other context fields completed is useful for further
        processing.'''

        context = self._get_context_from_records()

        for field in self._context_fields:
            if field in context:
                setattr(self, field, context[field])

        return True

    def _pre_insert_hook(self, context, cursor):
        '''This method is called by routines that insert a transaction into the database, after a
        context dictionary has been created but before any records have been written. It will
        return a value of True if the transaction can be inserted, or if False or a string is
        returned, VerificationError will be raised. This method is in addition to any internal
        consistency checks performed by the _verify method.

        The use of this hook is dependent on the domain for which SQLTransaction has been
        subclassed. The default implementation returns True.'''

        return True

    def _pre_update_hook(self, context, cursor):
        '''This method is called by routines that update a transaction in the database, after a
        context dictionary has been created but before any records have been written. It will
        return a value of True if the transaction can be updated, or if False or a string is
        returned, VerificationError will be raised. This method is in addition to any internal
        consistency checks performed by the _verify method.

        The use of this hook is dependent on the domain for which SQLTransaction has been
        subclassed. The default implementation returns True.'''

        return True

    def _pre_delete_hook(self, context, cursor):
        '''This method is called shortly before the records associated with the transaction are
        deleted. It will return a value of True if the transaction can be deleted, or if False or a
        string is returned, VerificationError will be raised. This method is in addition to any
        internal consistency checks performed by the _verify method.

        The use of this hook is dependent on the domain for which
        SQLTransaction has been subclassed. The default implementation does nothing.'''

        return True

    def _get_context(self):
        '''Return a context dictionary created from any non-None values stored under the names of
        the SQLField objects directly attached as attributes to the SQLTransaction. This does not
        perform any database access or recalculate any data.'''

        result = {'__name__' : self.__class__.__name__, '__version__' : self.__class__._version}
        for field_name, field in self._context_fields.items():
            tmp = field.get_context(instance=self, context=result)
            if tmp:
                result[field_name] = tmp
        return result

    def _get_refreshed_context(self, cursor):
        '''Return a context dictionary created from any non-None values of the SQLField objects
        directly attached as attributes to the SQLTransaction. This method will call the refresh
        method on each of the SQLField objects.'''

        result = {'__name__' : self.__class__.__name__, '__version__' : self.__class__._version}

        for field_name, field in self._context_fields.items():
            tmp = field.refresh(instance=self, context=result, cursor=cursor)
            if tmp:
                result[field_name] = tmp
        return result

    def _get_updated_context(self, cursor):
        '''Return a context dictionary created from any non-None values of the SQLField objects
        directly attached as attributes to the SQLTransaction. This method will call the update
        method on each of the SQLField objects, so may use the cursor to make changes to the
        database.'''

        result = {'__name__' : self.__class__.__name__, '__version__' : self.__class__._version}

        for field_name, field in self._context_fields.items():
            tmp = field.update(instance=self, context=result, cursor=cursor)
            if tmp:
                result[field_name] = tmp
        return result

    def _get_context_from_records(self):
        '''This method makes the context dictionary by scanning the SQLRecord and SQLRecordList
        contained in SQLTransactionField attributes and working out what context name:value pairs
        are consistent with them. For example if an SQLTable attached to the SQLTransaction has a
        IDIntField equal to 37 that uses a context value named trans_id, then the returned context
        dictionary should have 37 stored under the name trans_id. Does NOT attempt to identify if
        there are inconsistencies between rows.'''

        context = {}

        for record_name in self._records:
            record = getattr(self, record_name)
            context.update(record._context_values_stored())

        for recordlist_name in self._recordlists:
            recordlist = getattr(self, recordlist_name)
            for record in recordlist:
                context.update(record._context_values_stored())

        return context

    def _insert_existing(self, cursor):
        '''Insert the contents of the SQLTransaction into the database. This method stores only the
        existing data and will not update any values that are linked to sequences in the database.
        First SQLTable directly attached to the SQLTransaction are inserted in order of definition
        in the SQLTransaction subclass, followed by SQLTable held in SQLRecordList attached to the
        SQLTransaction, again in the order they were defined.'''

        with dialects.DefaultDialect.begin_transaction(cursor, self._isolation_level):
            context = self._get_context()

            status = self._pre_insert_hook(context, cursor)
            if status!=True:
                if isinstance(status, str):
                    raise VerificationError(status)
                raise VerificationError

            if not self._verify():
                raise VerificationError

            for record_name in self._records:
                record = getattr(self, record_name)
                if hasattr(record, '_insert_sql'):
                    cursor.execute(*record._insert_sql(context))

            for recordlist_name in self._recordlists:
                recordlist = getattr(self, recordlist_name)
                if hasattr(recordlist._record_type, '_insert_sql'):
                    cursor.executemany(recordlist._record_type._insert_sql_command(),
                                       recordlist._values_sql_repr(context))

    def _insert_new(self, cursor):
        '''Insert the contents of the SQLTransaction into the database. This method will update any
        values that are linked to sequences or queries in the database and then check that the
        verify method returns True before proceeding. First SQLTable directly attached to the
        SQLTransaction are inserted in order of definition in the SQLTransaction subclass, followed
        by SQLTable held in SQLRecordList attached to the SQLTransaction, again in the order they
        were defined.'''

        with dialects.DefaultDialect.begin_transaction(cursor, self._isolation_level):
            context = self._get_updated_context(cursor)

            status = self._pre_insert_hook(context, cursor)
            if status!=True:
                if isinstance(status, str):
                    raise VerificationError(status)
                raise VerificationError

            if not self._verify():
                raise VerificationError

            for record_name in self._records:
                record = getattr(self, record_name)
                if hasattr(record, '_insert_sql'):
                    cursor.execute(*record._insert_sql(context))

            for recordlist_name in self._recordlists:
                recordlist = getattr(self, recordlist_name)
                if hasattr(recordlist._record_type, '_insert_sql'):
                    cursor.executemany(recordlist._record_type._insert_sql_command(),
                                       recordlist._values_sql_repr(context))

    def _update(self, cursor):
        '''Insert the contents of the SQLTransaction into the database. This method stores only the
        existing data and will not update any values that are linked to sequences in the database.
        SQLTable held in SQLRecordList attached to the SQLTransaction are deleted first in reverse
        order of definition in the SQLTransaction subclass, followed by SQLTable directly attached
        to the SQLTransaction, again in reverse order of definition.'''

        with dialects.DefaultDialect.begin_transaction(cursor, self._isolation_level):
            context = self._get_context()

            status = self._pre_update_hook(context, cursor)
            if status!=True:
                if isinstance(status, str):
                    raise VerificationError(status)
                raise VerificationError

            if not self._verify():
                raise VerificationError

            for recordlist_name in reversed(list(self._recordlists)):
                recordlist = getattr(self, recordlist_name)
                if hasattr(recordlist._record_type, '_update_sql'):
                    for record in recordlist:
                        cursor.execute(*(record._update_sql(context)))

            for record_name in reversed(list(self._records)):
                record = getattr(self, record_name)
                if hasattr(record, '_update_sql'):
                    cursor.execute(*(record._update_sql(context)))

    def _delete(self, cursor):
        '''Delete the records corresponding to the contents of the SQLTransaction into the
        database. This method assumes sufficient context has been completed to specify the primary
        keys of the records to be deleted. SQLTable held in SQLRecordList attached to the
        SQLTransaction are deleted first in reverse order of definition in the SQLTransaction
        subclass, followed by SQLTable directly attached to the SQLTransaction, again in reverse
        order of definition.'''

        with dialects.DefaultDialect.begin_transaction(cursor, self._isolation_level):
            context = self._get_context()

            status = self._pre_delete_hook(context, cursor)
            if status!=True:
                if isinstance(status, str):
                    raise VerificationError(status)
                raise VerificationError

            if not self._verify():
                raise VerificationError

            # Deletions are done in reverse order compared with insertion to try to minimise the
            # number of constraint violations generated even when the constraint is set to NOT
            # DEFERRABLE.

            for recordlist_name in reversed(list(self._recordlists)):
                recordlist = getattr(self, recordlist_name)
                if hasattr(recordlist._record_type, '_delete_sql'):
                    for record in recordlist:
                        cursor.execute(*(record._delete_sql(context)))

            for record_name in reversed(list(self._records)):
                record = getattr(self, record_name)
                if hasattr(record, '_delete_sql'):
                    cursor.execute(*(record._delete_sql(context)))

    def _context_select(self, cursor, allow_unlimited=False):
        '''This method extracts the values stored in SQLField directly attached to the
        SQLTransaction and stored them in a context dictionary under the name of the attribute. It
        then attempts to use this dictionary to retrieve all of the SQLRecord and SQLRecordList
        objects stored in SQLTransactionField attributes. The post_select_hook method is then
        called, followed by the verify method to check that the result meets internal consistency
        requirements.'''

        with dialects.DefaultDialect.begin_transaction(cursor, self._isolation_level):

            context = self._get_refreshed_context(cursor)

            for record_name, record_field in self._records.items():
                record_type = record_field._record_type

                record = getattr(self, record_name)
                if record is None:
                    record = record_type()
                    setattr(self, record_name, record)

                if hasattr(record_type, '_context_select_sql'):
                    cursor.execute(*record_type._context_select_sql(context,
                                                                    allow_unlimited=allow_unlimited
                                                                   )
                                  )
                    nextrow = cursor.fetchone()
                    if nextrow:
                        record._set_values(nextrow)
                    else:
                        record._clear()
                else:
                    record._clear()


            for recordlist_name, recordlist_field in self._recordlists.items():
                recordlist_type = recordlist_field._record_type
                record_type = recordlist_type._record_type

                recordlist = getattr(self, recordlist_name)
                if recordlist is None:
                    recordlist = recordlist_type()
                    setattr(self, recordlist_name, recordlist)

                recordlist._clear()

                if hasattr(recordlist, '_context_select_sql'):
                    cursor.execute(*recordlist._context_select_sql(context,
                                                                   allow_unlimited=allow_unlimited
                                                                  )
                                  )
                    nextrow = cursor.fetchone()
                    while nextrow:
                        recordlist._append(record_type(*nextrow))
                        nextrow = cursor.fetchone()

                elif hasattr(record_type, '_context_select_sql'):
                    cursor.execute(*record_type._context_select_sql(context,
                                                                    allow_unlimited=allow_unlimited
                                                                   )
                                  )
                    nextrow = cursor.fetchone()
                    while nextrow:
                        recordlist._append(record_type(*nextrow))
                        nextrow = cursor.fetchone()

            status = self._post_select_hook(context, cursor)
            if status!=True:
                if isinstance(status, str):
                    raise VerificationError(status)
                raise VerificationError

        if not self._verify():
            raise VerificationError

# This constant records all the method and attribute names used in
# SQLTransaction so that SQLTransactionMetaClass can detect any attempts to
# overwrite them in subclasses.

INVALID_SQLTRANSACTION_ATTRIBUTE_NAMES = frozenset(dir(SQLTransaction))
