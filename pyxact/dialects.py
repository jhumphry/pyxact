'''This module defines classes that are used as singleton objects to define the
various flavours of SQL used by different database adaptors.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import datetime
import decimal
import re

from . import IsolationLevel, FKAction, FKMatch, ConstraintDeferrable

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

class TransactionContext:
    '''This is a small helper context manager class that allows the dialect.transaction method to
    be used in a 'with' statement. A transaction will have been begun, and at the end of the 'with'
    block or when an exception occurs the transaction will be committed or rolled-back as
    appropriate.'''

    def __init__(self, cursor, on_success='COMMIT;',
                 on_exception='ROLLBACK;'):

        self.cursor = cursor
        self.on_success = on_success
        self.on_exception = on_exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            if self.on_success:
                self.cursor.execute(self.on_success)
        else:
            if self.on_exception:
                self.cursor.execute(self.on_exception)
        return False

# These are the default SQL strings to set different characteristics of constraints
FOREIGN_KEY_MATCH_SQL = {FKMatch.SIMPLE : 'MATCH SIMPLE',
                         FKMatch.PARTIAL : 'MATCH PARTIAL',
                         FKMatch.FULL : 'MATCH FULL'}

FOREIGN_KEY_ACTION_SQL = {FKAction.NO_ACTION : 'NO ACTION',
                          FKAction.RESTRICT : 'RESTRICT',
                          FKAction.CASCADE : 'CASCADE',
                          FKAction.SET_NULL : 'SET NULL',
                          FKAction.SET_DEFAULT : 'SET DEFAULT'}

CONSTRAINT_DEFERRABLE_SQL = {ConstraintDeferrable.NOT_DEFERRABLE : 'NOT DEFERRABLE',
                             ConstraintDeferrable.DEFERRABLE_INITIALLY_DEFERRED : 'DEFERRABLE INITIALLY DEFERRED',
                             ConstraintDeferrable.DEFERRABLE_INITIALLY_IMMEDIATE : 'DEFERRABLE INITIALLY IMMEDIATE'}

class SQLDialect:
    '''This is an abstract base class from which concrete dialect classes
    should be derived.'''

    placeholder = '?'

    schema_support = True

    store_decimal_as_text = False
    store_date_time_datetime_as_text = False

    foreign_key_match_sql = FOREIGN_KEY_MATCH_SQL
    foreign_key_action_sql = FOREIGN_KEY_ACTION_SQL
    constraint_deferrable_sql = CONSTRAINT_DEFERRABLE_SQL

    truncate_table_sql = '''TRUNCATE TABLE {table_name};'''

    create_sequence_sql = ('',)
    nextval_sequence_sql = ('',)
    reset_sequence_sql = ('',)

    @classmethod
    def sql_repr(cls, value):
        '''This method returns the value in the form expected by the particular
        database and database adaptor specified by the dialect parameter. It
        exists to handle cases where the database adaptor cannot accept the
        Python type being used - for example while SQL NUMERIC types map quite
        well to Python decimal.Decimal types, the sqlite3 database adaptor does
        not recognise them, so string values must be stored.'''

        return value

    @classmethod
    def begin_transaction(cls, cursor, isolation_level=None):
        '''This method starts a new transaction using the database cursor and the (optional)
        isolation level specified, which should be one of the IsolationLevel enum values. It
        returns a context manager so can be used in a 'with' statement.'''

        raise NotImplementedError

    @classmethod
    def commit_transaction(cls, cursor, isolation_level=None):
        '''This method commits a transaction using the database cursor. The isolation level can be
        specified in order to cover cases where MANUAL_TRANSACTIONS (i.e. no automatic management)
        is desired.'''

        raise NotImplementedError

    @classmethod
    def rollback_transaction(cls, cursor, isolation_level=None):
        '''This method rolls back a transaction using the database cursor. The isolation level can
        be specified in order to cover cases where MANUAL_TRANSACTIONS (i.e. no automatic
        management) is desired.'''

        raise NotImplementedError

class sqliteDialect(SQLDialect):
    '''This class contains information used internally to generate suitable SQL
    for use with the standard library interface to SQLite3, the embedded
    database engine that usually comes supplied with Python.'''

    placeholder = '?' # The placeholder to use for parametised queries

    schema_support = False

    store_decimal_as_text = False
    store_date_time_datetime_as_text = True

    foreign_key_match_sql = FOREIGN_KEY_MATCH_SQL
    foreign_key_action_sql = FOREIGN_KEY_ACTION_SQL
    constraint_deferrable_sql = CONSTRAINT_DEFERRABLE_SQL

    truncate_table_sql = '''DELETE FROM {table_name} WHERE 1=1;'''

    create_sequence_sql = ('''
            CREATE TABLE IF NOT EXISTS {qualified_name}    (start {index_type},
                                                            interval {index_type},
                                                            lastval {index_type},
                                                            nextval {index_type});''',
                           '''INSERT INTO {qualified_name} VALUES ({start},{interval},{start},{start});''')
    nextval_sequence_sql = ('''UPDATE {qualified_name} SET lastval=nextval, nextval=nextval+interval;''',
                            '''SELECT lastval FROM {qualified_name};''')
    reset_sequence_sql = ('''UPDATE {qualified_name} SET lastval=start, nextval=start;''',)

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

        raise TypeError('sqlite3 Python module cannot handle type {}'.format(str(type(value))))

    @classmethod
    def begin_transaction(cls, cursor, isolation_level=None):

        if isolation_level == IsolationLevel.MANUAL_TRANSACTIONS:
            return TransactionContext(cursor, None, None)

        # Note that while SQLite does support READ_UNCOMMITTED, it is a per-session pragma and not
        # per-transaction, which makes it harder to use reliably. We always leave the setting on
        # the default, which is the maximalist SERIALIZABLE isolation level.
        cursor.execute('BEGIN TRANSACTION;')
        return TransactionContext(cursor)

    @classmethod
    def commit_transaction(cls, cursor, isolation_level=None):
        if isolation_level != IsolationLevel.MANUAL_TRANSACTIONS:
            cursor.execute('COMMIT;')

    @classmethod
    def rollback_transaction(cls, cursor, isolation_level=None):
        if isolation_level != IsolationLevel.MANUAL_TRANSACTIONS:
            cursor.execute('ROLLBACK;')

# This will be used by routines when no dialect is specified. It is not a
# constant as it is intended that it may be over-ridden by package users

DefaultDialect = sqliteDialect
