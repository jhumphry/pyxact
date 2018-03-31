'''This module defines Python types that map to SQL database tables.'''

from . import fields, dialects

INVALID_SQLRECORD_NAMES = None
INVALID_SQLTABLE_NAMES = None

class SQLRecordMetaClass(type):
    '''This is a metaclass that automatically identifies the SQLField and
    SQLConstraint member attributes added to new subclasses and creates
    additional private attributes to help order and access them.'''

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, **kwds):

        mcs.prepare_sqlrecord_namespace(mcs, namespace, INVALID_SQLRECORD_NAMES)

        return type.__new__(mcs, name, bases, namespace)

    def prepare_sqlrecord_namespace(cls, namespace, forbidden_names):
        '''This method receives an ordered dictionary of attributes attached to
        the new subclass and checks, indexes and processes them appropriately,
        adding additional items where necessary.'''

        slots = []
        _fields = dict()

        # Make a list of the SQLField attributes attached to the class and check that
        # the names won't be hiding any methods or attributes on the base class.

        for key, value in namespace.items():
            if isinstance(value, fields.SQLField):
                if key in forbidden_names:
                    raise AttributeError('SQLField {} has the same name as a method or '
                                         'internal attribute'.format(key))
                slots.append('_'+key)
                _fields[key] = value
            if isinstance(value, type) and issubclass(value, fields.SQLField):
                raise Warning('An SQLField subclass has been attached as {} rather than an '
                              'instance of the class. This is probably incorrect.'.format(key))

        namespace['__slots__'] = tuple(slots)
        namespace['_fields'] = _fields
        namespace['_field_count'] = len(slots)

        return namespace

class SQLRecord(metaclass=SQLRecordMetaClass):
    '''SQLRecord maps database rows (including those not connected with a
    specific table, for example the output of queries) to a Python class type.
    It is not intended for direct use, but as an abstract class to be
    subclassed.'''

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != self._field_count:
                raise ValueError('{0} values needed to initialise a {1}, {2} supplied.'
                                 .format(self._field_count, self.__class__.__name__, len(args)))

            for field, value in zip(self._fields.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self._fields:
                    raise ValueError('{0} is not a valid attribute name for {1}.'
                                     .format(key, self.__class__.__name__))
                setattr(self, key, value)

    def __str__(self):
        result = 'class ' + self.__class__.__name__ + ':\n'
        for key in self._fields.keys():
            result += '- {0} ({1}) = {2}\n'.format(key,
                                                   self._fields[key].__class__.__name__,
                                                   str(getattr(self, key))
                                                  )
        return result

    def copy(self):
        '''Create a deep copy of an instance of an SQLRecord. If normal
        assignment is used, the copies will be shallow and changing the
        attributes on one instance will affect the other.'''

        result = self.__class__()
        for attribute in self.__slots__:
            setattr(result, attribute, getattr(self, attribute))
        return result

    def clear(self):
        '''Set all fields in the SQLRecord to None.'''

        for key in self._fields:
            setattr(self, key, None)

    def get(self, key, context=None):
        '''Get a value stored in an SQLField within this SQLRecord. If a
        context dictionary is given, it may be used to provide the value, and
        both the context dictionary and the underlying SQLField may be
        updated.'''

        if context:
            return self._fields[key].get_context(self, context)
        return self._fields[key].get(self)

    def set(self, key, value):
        '''Set a value stored in an SQLField within this SQLRecord.'''

        if key not in self._fields:
            raise ValueError('{0} is not a valid field name.'.format(key))
        setattr(self, key, value)

    def set_values(self, values=None, **kwargs):
        '''Set all fields within this SQLRecord.'''
        if values:
            if len(values) != self._field_count:
                raise ValueError('{0} values required, {1} supplied.'
                                 .format(self._field_count, len(values)))

            for field_name, value in zip(self._fields.keys(), values):
                setattr(self, field_name, value)
        elif kwargs:
            for field_name, value in kwargs.items():
                if field_name not in self._fields:
                    raise ValueError('{0} is not a valid field name.'
                                     .format(field_name))
                setattr(self, field_name, value)
        else:
            raise ValueError('Must specify values')

    @classmethod
    def fields(cls):
        '''Returns a iterable of SQLField objects in the order they were
        defined in the SQLRecord subclass.'''

        return cls._fields.values()

    def values(self, context=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that may update and return a value from it, rather than
        the previously stored value.'''

        if context is not None:
            return [field.get_context(self, context) for field in self._fields.values()]

        return [field.get(self) for field in self._fields.values()]

    def values_sql_repr(self, context=None, dialect=None):
        '''Returns a list of values stored in the SQLField attributes of a
        particular SQLRecord instance. A context dictionary can be provided for
        SQLField types that may update and return a value from it, rather than
        the previously stored value. The values are in the form required by the
        SQL database adaptor identified by dialect.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        if context is not None:
            return [dialect.sql_repr(field.get_context(self, context))
                    for field in self._fields.values()]

        return [dialect.sql_repr(field.get(self)) for field in self._fields.values()]

    @classmethod
    def items(cls):
        '''Returns a iterable of tuples of field names and SQLField objects in the
        order they were defined in the SQLRecord subclass.'''

        return cls._fields.items()

    def item_values(self, context=None):
        '''Returns a list of tuples of field names and values stored in the
        SQLField attributes of a particular SQLRecord instance. A context
        dictionary can be provided for SQLField types that may update and
        return a value from it, rather than the previously stored value.'''

        if context is not None:
            return [(key, value.get_context(self, context))
                    for key, value in self._fields.items()]

        return [(key, value.get(self))
                for key, value in self._fields.items()]

    @classmethod
    def column_names_sql(cls):
        '''Returns a string containing a comma-separated list of column names.'''

        return ', '.join([cls._fields[key].sql_name for key in cls._fields.keys()])

    def context_values_stored(self):
        '''Returns a dictionary containing all of the (non-None) context values
        that are stored by context-sensitive fields in the record.'''

        context = {}

        for field_obj in self._fields.values():
            if field_obj.context_used:
                tmp = field_obj.get(self)
                if tmp:
                    context[field_obj.context_used] = tmp

        return context

# This constant records all the method and attribute names used in SQLRecord
# and SQLTable so thatthe metaclasses can detect any attempts to overwrite
# them in subclasses.

INVALID_SQLRECORD_NAMES = frozenset(dir(SQLRecord))
