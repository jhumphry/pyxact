'''This module defines hint objects that can be added to a field to assist with displaying a
human-readable field value for a id-type field.'''

# Copyright 2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from . import dialects

class HV():
    '''This is a simple wrapper for strings that are to be passed to SQLField types to indicate that
    the string should be looked up via the SQLField's associated hint, rather than stored directly
    in the field.'''

    __slots__ = ['contents',]

    def __init__(self, contents):
        self.contents = contents

    def __repr__(self):
        return self.contents

class Hint():
    '''This class contains an SQL SELECT query string (without trailing ';') that returns a
    two-column 'key' and 'value' result. This can be used to provide mappings between id columns
    used in the database and human-readable values. It provides methods to find the key (id column
    value) associated with a given value (human-readable value), get the value associated with a
    key, and to list the values.'''

    __slots__ = ['query', 'query_noschema']

    def __init__(self, query):

        self.query = dialects.convert_schema_sep(query, '.')
        self.query_noschema = dialects.convert_schema_sep(query, '_')

    def get(self, key, cursor):
        '''Return the human-readable value associated with the key value, or return None.'''

        dialect = dialects.DefaultDialect

        base_query = (self.query if dialect.schema_support
                      else self.query_noschema)

        query = 'SELECT value FROM (' + base_query + ') WHERE '
        query += dialect.parameter_values(('key',))
        query += ';'

        cursor.execute(query, (dialect.sql_repr(key),))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None

    def find(self, value, cursor):
        '''Find the key value associated with the human-readable value, or return None if this
        fails.'''

        dialect = dialects.DefaultDialect

        base_query = (self.query if dialect.schema_support
                      else self.query_noschema)

        query = 'SELECT key FROM (' + base_query + ') WHERE '
        query += dialect.parameter_values(('value',))
        query += ';'

        cursor.execute(query, (dialect.sql_repr(value),))
        result = cursor.fetchone()
        if result:
            return result[0]
        return None

    def list_values(self, cursor):
        '''A generator expression that yields all of the human-readable values in turn.'''

        dialect = dialects.DefaultDialect

        base_query = (self.query if dialect.schema_support
                      else self.query_noschema)

        query = 'SELECT value FROM (' + base_query + ');'

        cursor.execute(query)

        next_row = cursor.fetchone()
        while next_row:
            yield next_row[0]
            next_row = cursor.fetchone()
