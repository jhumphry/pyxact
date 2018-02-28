# Some common fixtures for pytest tests of pyxact modules

import pytest
import sqlite3

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

