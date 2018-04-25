'''This module defines Python types that describe SQL queries.'''

import re
from . import dialects, fields, records, recordlists

CONTEXT_PLACEHOLDER_REGEXP = re.compile(r'\{[^\}\.]+\}', re.UNICODE)

INVALID_SQLQUERY_NAMES = None
INVALID_SQLQUERYRESULT_NAMES = None

class SQLQueryMetaClass(type):
    '''This metaclass ensures that SQLQuery is only subclassed with a
    valid SQLRecord subclass as the record_type parameter.'''

    def __new__(mcs, name, bases, namespace, query='', record_type=None, **kwds):

        if record_type:
            if not isinstance(record_type, type):
                raise ValueError('record_type must refer to an SQLRecord subclass.')
            if not issubclass(record_type, records.SQLRecord):
                raise ValueError('record_type must refer to an SQLRecord subclass.')
        namespace['_record_type'] = record_type

        # Now identify the SQLField attached to the new class to form the context for
        # the query
        slots = []
        _context_fields = dict()
        for k in namespace:
            if isinstance(namespace[k], fields.SQLField):
                if k in INVALID_SQLQUERY_NAMES:
                    raise AttributeError('SQLField {} has the same name as an SQLQuery'
                                         ' method or internal attribute'.format(k))
                slots.append('_'+k)
                _context_fields[k] = namespace[k]
        namespace['_context_fields'] = _context_fields
        namespace['__slots__'] = slots

        # Now process the query text to extract placeholders and check they
        # correspond to actual context fields.
        segmented_query = []
        segmented_query_noschema = []
        query_fields = []
        current_pos = 0
        match = CONTEXT_PLACEHOLDER_REGEXP.search(query, pos=current_pos)
        if match:
            while match:
                field = match.group(0)[1:-1]
                if field not in _context_fields:
                    raise AttributeError('Query placeholder {} does not match any of the '
                                         'context fields'.format(field))
                query_fields.append(field)

                query_segment = query[current_pos:match.start()]
                segmented_query.append(dialects.convert_schema_sep(query_segment, '.'))
                segmented_query_noschema.append(dialects.convert_schema_sep(query_segment, '_'))

                current_pos = match.end()
                match = CONTEXT_PLACEHOLDER_REGEXP.search(query, pos=current_pos)

        query_segment = query[current_pos:]
        segmented_query.append(dialects.convert_schema_sep(query_segment, '.'))
        segmented_query_noschema.append(dialects.convert_schema_sep(query_segment, '_'))

        namespace['_query'] = query
        namespace['_segmented_query'] = segmented_query
        namespace['_segmented_query_noschema'] = segmented_query_noschema
        namespace['_query_fields'] = query_fields

        return type.__new__(mcs, name, bases, namespace)

class SQLQuery(metaclass=SQLQueryMetaClass,
               query='',
               record_type=records.SQLRecord):
    '''SQLQuery maps an SQL query to Python class types. It is not intended
    for direct use, but as an abstract class to be subclassed. The query string
    passed to it can have placeholders indicated by text such as {foo}. When
    the query SQL is generated by the query_sql method these will be replaced
    by the placeholder string used by the relevant database adaptor dialect.
    The method query_values_sql_repr will return the extract the values from
    the SQLField class attributes of the same name (here, a SQLField in the
    SQLQuery subclass named 'foo') in the correct order to be passed to the
    database alongside the query text.'''

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != len(self._context_fields):
                raise ValueError('{0} values required, {1} supplied.'
                                 .format(len(self._context_fields), len(args)))

            for field, value in zip(self._context_fields.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self._context_fields:
                    raise ValueError('{0} is not a valid attribute name.'.format(key))
                setattr(self, key, value)

    def _set_context(self, context):
        '''Set the values stored as SQLField objects directly attached as attributes to the
        SQLQuery to the values in the supplied context dictionary if present.'''

        for key in self._context_fields.keys():
            if key in context:
                setattr(self, key, context[key])

    def _get_context(self):
        '''Return a context dictionary created from the values stored under the
        names of the SQLField objects directly attached as attributes to the
        SQLQuery.'''

        result = dict()
        for i in self._context_fields:
            result[i] = getattr(self, i)
        return result

    def _query_values(self):
        '''Return a correctly-ordered list of the values that need to be passed
        to the database to execute the query.'''

        return [getattr(self, i) for i in self._query_fields]

    def _query_values_sql_repr(self, dialect=None):
        '''Return a correctly-ordered list of the values that need to be passed
        to the database to execute the query, using the appropriate SQL adaptor
        dialect.'''

        if dialect is None:
            dialect = dialects.DefaultDialect

        return [dialect.sql_repr(getattr(self, i)) for i in self._query_fields]

    @classmethod
    def _query_sql(cls, dialect=None):
        '''Return the SQL query text using the correct placeholder for the SQL
        dialect in use.'''

        if dialect is None:
            dialect = dialects.DefaultDialect
        if dialect.schema_support:
            return dialect.placeholder.join(cls._segmented_query)
        return dialect.placeholder.join(cls._segmented_query_noschema)

    def _execute(self, cursor, dialect=None):
        '''Execute the query using the cursor.'''

        if dialect is None:
            dialect = dialects.DefaultDialect
        cursor.execute(self._query_sql(dialect), self._query_values_sql_repr())

    @classmethod
    def _result_records(cls, cursor):
        '''Take a database cursor with an executed query and for each row
        returned by this query, yield an instance of the SQLRecord subclass
        specified via record_type when SQLQuery was subclassed.'''

        if not cls._record_type:
            raise RuntimeError('This SQLQuery subclass does not have an associated SQLRecord '
                               'result class specified.')

        next_row = cursor.fetchone()
        while next_row:
            yield cls._record_type(*next_row)
            next_row = cursor.fetchone()

    @classmethod
    def _result_record(cls, cursor):
        '''Take a database cursor with an executed query return an instance of
        the SQLRecord subclass specified via record_type when SQLQuery was
        subclassed. None is returned if there are no more results.'''

        if not cls._record_type:
            raise RuntimeError('This SQLQuery subclass does not have an associated SQLRecord '
                               'result class specified.')

        next_row = cursor.fetchone()
        if next_row:
            return cls._record_type(*next_row)
        return None

    @staticmethod
    def _result_singlevalue(cursor):
        '''Take a database cursor with an executed query that returns a single
        value and return just that value.'''

        result_row = cursor.fetchone()

        if result_row is None:
            raise ValueError('No result returned from query.')

        if len(result_row) != 1:
            raise ValueError('The query did not return one value.')

        return result_row[0]

class SQLQueryResultMetaClass(recordlists.SQLRecordListMetaClass):
    '''This is a metaclass that automatically identifies the SQLField  member attributes added to
    new subclasses and creates additional private attributes to help order and access them.'''

    def __new__(mcs, name, bases, namespace, query, isolation_level=None, **kwds):

        if not issubclass(query, SQLQuery):
            raise TypeError('query must be an instance of pyxact.queries.SQLQuery')

        mcs.prepare_sqlrecordlist_namespace(mcs,
                                            namespace,
                                            INVALID_SQLQUERYRESULT_NAMES,
                                            query._record_type)

        namespace['_query_type'] = query
        namespace['_isolation_level'] = isolation_level

        namespace['__slots__'] = ('_records', '_query')

        return type.__new__(mcs, name, bases, namespace)

class SQLQueryResult(recordlists.SQLRecordList,
                     metaclass=SQLQueryResultMetaClass,
                     query=SQLQuery):
    '''SQLQueryResult is a subclass of SQLRecordList that is linked to an SQLQuery. It has a
    _refresh method that can replace its contents with the results from the SQLQuery.'''

    def __init__(self, *args, **kwds):

        super().__init__(args)
        self._query = self._query_type(**kwds)

    def _refresh(self, cursor, dialect=None):
        '''Clear the existing data, start a new database transaction, call the query associated
        with this SQLQueryResult and stored the returned rows before committing the transaction.'''

        self._records.clear()

        if not dialect:
            dialect = dialects.DefaultDialect

        with dialect.begin_transaction(cursor, self._isolation_level):

            self._query._execute(cursor, dialect)

            result_row = cursor.fetchone()
            while result_row:
                self._records.append(self._record_type(*result_row))
                result_row = cursor.fetchone()

    def _context_select_sql(self, context, dialect=None, allow_unlimited=True):
        '''Set the query context to the given context parameter. Return a tuple of the SQL query
        command to execute and the values to pass as parameters.'''

        self._query._set_context(context)

        return (self._query._query_sql(dialect), self._query._query_values_sql_repr())

    def _set_context(self, context):
        '''Set the values stored as SQLField objects directly attached as attributes to the
        SQLQuery to the values in the supplied context dictionary if present.'''

        self._query._set_context(context)

    def _get_context(self):
        '''Return a context dictionary created from the values stored under the names of the
        SQLField objects directly attached as attributes to the SQLQuery.'''

        return self._query._get_context()

INVALID_SQLQUERY_NAMES = frozenset(dir(SQLQuery))
INVALID_SQLQUERYRESULT_NAMES = frozenset(dir(SQLQueryResult))
