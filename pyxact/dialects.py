'''This module defines classes that are used as singleton objects to define the
various flavours of SQL used by different database adaptors.'''

import datetime
import decimal
import re

SCHEMA_SEPARATOR_REGEXP = re.compile(r'\{([^\}\.]+)\.([^\}\.]+)\}', re.UNICODE)

def convert_schema_sep(sql_text, separator='.'):
    '''Find any instances of '{schema.obj}' in the sql_text parameter and
    return a string using the given separator character 'schema.obj'. This is
    used to emulate SQL schema on databases that don't really support them.'''

    match = SCHEMA_SEPARATOR_REGEXP.search(sql_text)
    result = ''
    current_pos = 0

    if match:
        while match:
            result += sql_text[current_pos:match.start()] + match[1] + separator + match[2]
            current_pos = match.end()
            match = SCHEMA_SEPARATOR_REGEXP.search(sql_text, match.end())
    result += sql_text[current_pos:]
    return result

class SQLDialect:
    '''This is an abstract base class from which concrete dialect classes
    should be derived.'''

    placeholder = '?'

    schema_support = True

    store_decimal_as_text = False
    store_date_time_datetime_as_text = False

    truncate_table_sql = '''TRUNCATE TABLE {table_name};'''

    @classmethod
    def sql_repr(cls, value):
        '''This method returns the value in the form expected by the particular
        database and database adaptor specified by the dialect parameter. It
        exists to handle cases where the database adaptor cannot accept the
        Python type being used - for example while SQL NUMERIC types map quite
        well to Python decimal.Decimal types, the sqlite3 database adaptor does
        not recognise them, so string values must be stored.'''

        return value

class sqliteDialect(SQLDialect):
    '''This class contains information used internally to generate suitable SQL
    for use with the standard library interface to SQLite3, the embedded
    database engine that usually comes supplied with Python.'''

    placeholder = '?' # The placeholder to use for parametised queries

    schema_support = False

    store_decimal_as_text = False
    store_date_time_datetime_as_text = True

    truncate_table_sql = '''DELETE FROM {table_name} WHERE 1=1;'''

    @classmethod
    def sql_repr(cls, value):
        if isinstance(value, bool):
            return 1 if value else 0
        elif isinstance(value, (int, float, str, bytes)) or value is None:
            return value
        elif isinstance(value, decimal.Decimal):
            return str(value)
        elif isinstance(value, datetime.datetime):
            if value.tzinfo:
                return value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            return value.strftime('%Y-%m-%dT%H:%M:%S.%f')
        elif isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        elif isinstance(value, datetime.time):
            return value.strftime('%H:%M:%S.%f')

        raise ValueError('sqlite3 Python module cannot handle type {}'.format(str(type(value))))

    create_sequence_sql = ('''
CREATE TABLE IF NOT EXISTS {qualified_name}    (start {index_type},
                                                interval {index_type},
                                                lastval {index_type},
                                                nextval {index_type});''',
                           '''INSERT INTO {qualified_name} VALUES ({start},{interval},{start},{start});''')
    nextval_sequence_sql = ('''UPDATE {qualified_name} SET lastval=nextval, nextval=nextval+interval;''',
                            '''SELECT lastval FROM {qualified_name};''')
    reset_sequence_sql = ('''UPDATE {qualified_name} SET lastval=start, nextval=start;''',)


# This will be used by routines when no dialect is specified. It is not a
# constant as it is intended that it may be over-ridden by package users

DefaultDialect = sqliteDialect
