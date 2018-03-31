# Test SQLTable behaviours that need a sqlite database to demonstrate

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.tables as tables
from pyxact.dialects import sqliteDialect

@pytest.fixture('module')
def sample_table_class(sqlitedb):
    class SampleRecord(tables.SQLTable, table_name='sample_table'):
        trans_id=fields.IntField(context_used='trans_id')
        flag=fields.BooleanField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)
        narrative=fields.TextField()
        pk=constraints.PrimaryKeyConstraint(column_names=('trans_id'))

    return SampleRecord

@pytest.fixture('module')
def sample_table_rows(sample_table_class):

    result = []
    result.append(sample_table_class(1, True, '3.4', 'Line 1'))
    result.append(sample_table_class(2, False, '1.1', 'Line 2'))
    result.append(sample_table_class(3, True, '-10.4', 'Line 3'))
    result.append(sample_table_class(4, False, '2.2', 'Line 4'))

    return result

@pytest.fixture('module')
def sample_table(sqlitedb, sample_table_class):

    sqlitedb.execute(sample_table_class.create_table_sql())

    # This will raise an OperationalError if the table doesn't exist or the
    # names of the columns are wrong
    sqlitedb.execute('SELECT trans_id, flag, amount, narrative FROM sample_table;')

def test_colliding_field_names():

    with pytest.raises(AttributeError, message='SQLTableMetaClass should not allow subclasses of SQLTable'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord(tables.SQLTable, table_name='sample_table_class'):
            ok_name = fields.IntField()
            insert_sql = fields.IntField()

    with pytest.raises(AttributeError, message='SQLTableMetaClass should not allow subclasses of SQLTable'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord2(tables.SQLTable, table_name='sample_table_class'):
            ok_name = fields.IntField()
            _schema = fields.IntField()

def test_insert(sample_table, sample_table_class, sample_table_rows, sqlitecur):

    # Ensure the table is empty
    sqlitecur.execute('DELETE FROM sample_table WHERE 1=1')

    # Insert a single row and make sure it can be read back
    sqlitecur.execute(*sample_table_rows[0].insert_sql())
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (sample_table_rows[0].trans_id,))
    assert sqlitecur.fetchone() == (1, 1, 3.4, 'Line 1')

    # Insert the remaining rows in one go and read them back
    sqlitecur.executemany(sample_table_class.insert_sql_command(),
                          [sample_table_rows[1].values_sql_repr(),
                           sample_table_rows[2].values_sql_repr(),
                           sample_table_rows[3].values_sql_repr()])
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
        sqlitecur.execute(*i.insert_sql())

    # Pick one record and make sure it is stored correctly
    row = sample_table_rows[2].copy()
    sqlitecur.execute('SELECT * FROM sample_table WHERE trans_id=?',
                      (row.trans_id,))
    assert sqlitecur.fetchone() == (3, 1, -10.4, 'Line 3')

    # Change a field and update the database
    row.amount = '99.9'
    sqlitecur.execute(*row.update_sql())

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
        sqlitecur.execute(*i.insert_sql())

    # Check there are four rows
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (4,)

    # Delete one record
    sqlitecur.execute(*sample_table_rows[2].delete_sql())

    # Check there are now three rows
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table;')
    assert sqlitecur.fetchone() == (3,)

    # Check the correct row was deleted
    sqlitecur.execute('SELECT COUNT(*) FROM sample_table WHERE trans_id=3;')
    assert sqlitecur.fetchone() == (0,)