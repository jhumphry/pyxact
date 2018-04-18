# Some common fixtures for pytest tests of pyxact modules

import pytest
import sqlite3

from pyxact import tables, fields, constraints

@pytest.fixture(scope="module")
def sqlitedb():
    # Create an in-memory database to work on
    conn = sqlite3.connect(':memory:')

    # Must be done first if SQLite3 is to enforce foreign key constraints
    conn.execute('PRAGMA foreign_keys = ON;')

    yield conn
    conn.close()

@pytest.fixture()
def sqlitecur(sqlitedb):
    cur = sqlitedb.cursor()
    yield cur
    cur.close()

@pytest.fixture('session')
def sample_table_class():
    class SampleTable(tables.SQLTable, table_name='sample_table'):
        trans_id=fields.IntField(context_used='trans_id')
        flag=fields.BooleanField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)
        narrative=fields.TextField()
        pk=constraints.PrimaryKeyConstraint(column_names=('trans_id'))

    return SampleTable

@pytest.fixture('session')
def sample_table_rows(sample_table_class):

    result = []
    result.append(sample_table_class(1, True, '3.4', 'Line 1'))
    result.append(sample_table_class(2, False, '1.1', 'Line 2'))
    result.append(sample_table_class(3, True, '-10.4', 'Line 3'))
    result.append(sample_table_class(4, False, '2.2', 'Line 4'))

    return result

@pytest.fixture('module')
def sample_table(sqlitedb, sample_table_class):

    sqlitedb.execute(sample_table_class._create_table_sql())
    sqlitedb.execute(sample_table_class._truncate_table_sql())

    # This will raise an OperationalError if the table doesn't exist or the
    # names of the columns are wrong
    sqlitedb.execute('SELECT trans_id, flag, amount, narrative FROM sample_table;')
