'''This module defines the flavour of SQL used by PostgreSQL and pyscopg2.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import enum

from . import dialects
from . import IsolationLevel

class Psycopg2Dialect(dialects.SQLDialect):
    '''This is a singleton class that defines the variant of SQL supported by PostgreSQL and the
    pyscopg2 database adaptor.'''

    @classmethod
    def parameter(cls, number=1, start=1):
        return '%s, '*(number-1) + '%s'

    @classmethod
    def parameter_values(cls, names: list, start=1, concat=','):
        result = ''
        for name in names[:-1]:
            result += name + '=%s ' + concat + ' '
        return result + names[-1]+'=%s'

    schema_support = True

    store_decimal_as_text = False
    store_date_time_datetime_as_text = False
    enum_support = True

    foreign_key_match_sql = dialects.FOREIGN_KEY_MATCH_SQL
    foreign_key_action_sql = dialects.FOREIGN_KEY_ACTION_SQL
    constraint_deferrable_sql = dialects.CONSTRAINT_DEFERRABLE_SQL

    truncate_table_sql = '''TRUNCATE TABLE {table_name} RESTRICT;'''
    truncate_table_cascade_sql = '''TRUNCATE TABLE {table_name} CASCADE;'''

    create_sequence_sql = ('''CREATE SEQUENCE IF NOT EXISTS {qualified_name} AS {index_type} '''
                           '''START {start}''',)
    nextval_sequence_sql = ('''SELECT nextval('{qualified_name}');''',)
    reset_sequence_sql = ('''ALTER SEQUENCE {qualified_name} RESTART''',)

    create_view_sql = '''CREATE OR REPLACE VIEW'''

    index_specifies_schema = False

    @classmethod
    def sql_repr(cls, value):
        '''This method returns the value in the form expected by the particular database and
        database adaptor specified by the dialect parameter. The psycopg2 adaptor handles most
        Python types transparently, so this function does not have to do anything.'''
        if isinstance(value, enum.Enum):
            return value.name
        return value

    @classmethod
    def begin_transaction(cls, cursor, isolation_level=None):

        if isolation_level == IsolationLevel.MANUAL_TRANSACTIONS:
            return dialects.TransactionContext(cursor, None, None)

        if isolation_level:
            cmd = 'BEGIN TRANSACTION ISOLATION MODE '
            cmd += dialects.ISOLATION_LEVEL_SQL[isolation_level]
            cmd += ';'
        else:
            cmd = 'BEGIN TRANSACTION;'

        return dialects.TransactionContext(cursor, cmd, 'COMMIT;', 'ROLLBACK;')

    @classmethod
    def commit_transaction(cls, cursor, isolation_level=None):
        if isolation_level != IsolationLevel.MANUAL_TRANSACTIONS:
            cursor.execute('COMMIT;')

    @classmethod
    def rollback_transaction(cls, cursor, isolation_level=None):
        if isolation_level != IsolationLevel.MANUAL_TRANSACTIONS:
            cursor.execute('ROLLBACK;')

    @staticmethod
    def create_enum_type(cursor, py_type, sql_name, sql_schema=None):
        '''Create an enum type in the PostgreSQL database for the given py_type under the name
        sql_name in the sql_schema (if given).'''

        if sql_schema:
            qual_sql_name = sql_schema + '.' + sql_name
        else:
            qual_sql_name = sql_name
        enum_values = ', '.join(["'" + x.name + "'" for x in py_type])

        # Using 'DROP TYPE IF EXISTS ... CASCADE' does not work here, because it will silently alter
        # existing tables to remove columns using the enum.

        cursor.execute('''DO $$ BEGIN
                            CREATE TYPE {} AS ENUM ({});
                          EXCEPTION
                            WHEN duplicate_object THEN null;
                          END $$;'''.format(qual_sql_name, enum_values))
