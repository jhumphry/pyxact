'''Test SQLTable behaviours that need a sqlite database to demonstrate'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.tables as tables
from pyxact.dialects import sqliteDialect

def test_colliding_field_names():

    # SQLTableMetaClass should not allow subclasses of SQLTable with names that collide with
    # built-in methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord(tables.SQLTable, table_name='sample_table_class'):
            ok_name = fields.IntField()
            _insert_sql = fields.IntField()

    # SQLTableMetaClass should not allow subclasses of SQLTable with names that collide with
    # built-in methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord2(tables.SQLTable, table_name='sample_table_class'):
            ok_name = fields.IntField()
            _schema = fields.IntField()

def test_insert(sample_table, sample_table_class, sample_table_rows, sqlitecur):

    # Ensure the table is empty
    sqlitecur.execute('DELETE FROM sample_table WHERE 1=1')

    # Insert a single row and make sure it can be read back
    sqlitecur.execute(*sample_table_rows[0]._insert_sql())
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (sample_table_rows[0].trans_id,))
    assert sqlitecur.fetchone() == (1, 1, 3.4, 'Line 1')

    # Insert the remaining rows in one go and read them back
    sqlitecur.executemany(sample_table_class._insert_sql_command(),
                          [sample_table_rows[1]._values_sql_repr(),
                           sample_table_rows[2]._values_sql_repr(),
                           sample_table_rows[3]._values_sql_repr()])
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=? OR trans_id=?',
                   (sample_table_rows[2].trans_id, sample_table_rows[3].trans_id))
    assert sqlitecur.fetchone() == (3, 1, -10.4, 'Line 3')
    assert sqlitecur.fetchone() == (4, 0, 2.2, 'Line 4')

    # Check the correct number of rows exist in the table.
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (4,)

def test_update(sample_table, sample_table_class, sample_table_rows, sqlitecur):

    # Ensure the table is empty
    sqlitecur.execute('DELETE FROM sample_table WHERE 1=1')

    # Insert all four records
    for i in sample_table_rows:
        sqlitecur.execute(*i._insert_sql())

    # Pick one record and make sure it is stored correctly
    row = sample_table_rows[2]._copy()
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (row.trans_id,))
    assert sqlitecur.fetchone() == (3, 1, -10.4, 'Line 3')

    # Change a field and update the database
    row.amount = '99.9'
    sqlitecur.execute(*row._update_sql())

    # Check it has been updated
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (row.trans_id,))
    assert sqlitecur.fetchone() == (3, 1, 99.9, 'Line 3')

    # Now check a different row to make sure it hasn't changed
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (1,))
    assert sqlitecur.fetchone() == (1, 1, 3.4, 'Line 1')

    # Check there are still only four rows
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (4,)

def test_delete(sample_table, sample_table_class, sample_table_rows, sqlitecur):

    # Ensure the table is empty
    sqlitecur.execute('DELETE FROM sample_table WHERE 1=1')

    # Insert all four records
    for i in sample_table_rows:
        sqlitecur.execute(*i._insert_sql())

    # Check there are four rows
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (4,)

    # Delete one record
    sqlitecur.execute(*sample_table_rows[2]._delete_sql())

    # Check there are now three rows
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (3,)

    # Check the correct row was deleted
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table WHERE trans_id=3;')
    assert sqlitecur.fetchone() == (0,)
