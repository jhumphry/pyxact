# Test pyxact.records

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.records as records
from pyxact.dialects import sqliteDialect

# @pytest.fixture()
# def field_test_seq(sqlitecur):
    # field_test_seq = sequences.SQLSequence(name='field_test_seq')
    # field_test_seq.create(sqlitecur, sqliteDialect)
    # return field_test_seq

@pytest.fixture('module')
def sample_record(sqlitedb):
    class SampleRecord(records.SQLRecord, table_name='sample_record'):
        trans_id=fields.IDIntField(context_used='trans_id')
        flag=fields.BooleanField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)
        narrative=fields.TextField()
        pk=constraints.PrimaryKeyConstraint(sql_column_names=('trans_id'))

    sqlitedb.execute(SampleRecord.create_table_sql())

    return SampleRecord

@pytest.fixture('module')
def linked_record(sqlitedb, sample_record):
    class LinkedRecord(records.SQLRecord, table_name='linked_record'):
        record_id=fields.IDIntField(context_used='record_id')
        trans_id=fields.IDIntField(context_used='trans_id')
        some_data=fields.RealField()
        pk=constraints.PrimaryKeyConstraint(sql_column_names=('record_id',))
        fk=constraints.ForeignKeyConstraint(sql_column_names=('trans_id',),
                                            foreign_table='sample_record')

    sqlitedb.execute(LinkedRecord.create_table_sql())

    return LinkedRecord

def test_table_creation(sqlitecur, sample_record, linked_record):
    sqlitecur.execute('SELECT trans_id, flag, amount, narrative FROM sample_record;')
    assert [x.sql_name for x in sample_record.fields()] == \
                ['trans_id', 'flag', 'amount', 'narrative']

    sqlitecur.execute('SELECT record_id, trans_id, some_data FROM linked_record;')

def test_colliding_field_names():
    with pytest.raises(AttributeError, message='SQLRecordMetaClass should not allow subclasses of SQLRecord'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord(records.SQLRecord, table_name='sample_record'):
            ok_name = fields.IntField()
            get_context = fields.IntField()
