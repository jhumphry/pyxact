# Test SQLView behaviours that need a sqlite database to demonstrate

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.tables as tables
import pyxact.views as views
from pyxact.dialects import sqliteDialect

from test_pyxact_tables import sample_table_class, sample_table, sample_table_rows

@pytest.fixture('module')
def sample_view_class():
    class SampleView(views.SQLView, view_name='sample_view',
                     query='SELECT trans_id AS tid, amount AS amount FROM sample_table'):
        tid=fields.IntField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)

    return SampleView

@pytest.fixture('module')
def sample_view(sqlitedb, sample_view_class, sample_table):

    sqlitedb.execute(sample_view_class._create_view_sql())

    # This will raise an OperationalError if the view doesn't exist or the
    # names of the columns are wrong
    sqlitedb.execute('SELECT tid, amount FROM sample_view;')

def test_colliding_field_names():

    with pytest.raises(AttributeError, message='SQLViewMetaClass should not allow subclasses of SQLView'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord(views.SQLView, view_name='sample_view_fail'):
            ok_name = fields.IntField()
            _create_view_sql = fields.IntField()

    with pytest.raises(AttributeError, message='SQLViewMetaClass should not allow subclasses of SQLView'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord2(views.SQLView, view_name='sample_view'):
            ok_name = fields.IntField()
            _query = fields.IntField()

def test_select(sqlitecur, sample_view, sample_view_class, sample_table, sample_table_rows):

    sqlitecur.execute(*sample_view_class._simple_select_sql())

    assert sqlitecur.fetchone() == None

    for row in sample_table_rows:
        sqlitecur.execute(*row._insert_sql())

    sqlitecur.execute(*sample_view_class._simple_select_sql())

    assert sqlitecur.fetchone() == (1, 3.4)
    assert sqlitecur.fetchone() == (2, 1.1)


