# Test pyxact.records

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.records as records
from pyxact.dialects import sqliteDialect

@pytest.fixture('module')
def sample_record(sqlitedb):
    class SampleRecord(records.SQLRecord, table_name='sample_record'):
        trans_id=fields.ContextIntField(context_used='trans_id')
        flag=fields.BooleanField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)
        narrative=fields.TextField()
        pk=constraints.PrimaryKeyConstraint(column_names=('trans_id'))

    sqlitedb.execute(SampleRecord.create_table_sql())

    return SampleRecord

@pytest.fixture('module')
def linked_record(sqlitedb, sample_record):
    class LinkedRecord(records.SQLRecord, table_name='linked_record'):
        record_id=fields.ContextIntField(context_used='record_id')
        trans_id=fields.ContextIntField(context_used='trans_id')
        some_data=fields.RealField()
        pk=constraints.PrimaryKeyConstraint(column_names=('record_id',))
        fk=constraints.ForeignKeyConstraint(column_names=('trans_id',),
                                            foreign_table='sample_record')

    sqlitedb.execute(LinkedRecord.create_table_sql())

    return LinkedRecord

def test_table_creation(sqlitecur, sample_record):
    sqlitecur.execute('SELECT trans_id, flag, amount, narrative FROM sample_record;')
    assert [x.sql_name for x in sample_record.fields()] == \
                ['trans_id', 'flag', 'amount', 'narrative']

    assert sample_record().table_name == 'sample_record'

def test_colliding_field_names():
    with pytest.raises(AttributeError, message='SQLRecordMetaClass should not allow subclasses of SQLRecord'
                                               ' with names that collide with built-in methods/attributes.'):
        class FailRecord(records.SQLRecord, table_name='sample_record'):
            ok_name = fields.IntField()
            get_context = fields.IntField()

def test_initialization(sample_record):
    r1 = sample_record()
    assert all((x is None for x in r1.values()))

    r2 = sample_record(1, True, 3.0, 'test')
    assert set(r2.item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r3 = sample_record(trans_id=1, amount=3.0, flag=True, narrative='test')
    assert set(r3.item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r4 = sample_record(trans_id=1, amount=3.0, narrative='test')
    assert r4.flag is None

    with pytest.raises(ValueError):
        r5 = sample_record(1, True, 3.0)

    with pytest.raises(ValueError):
        r6 = sample_record(trans_id=1, amount=3.0, nonesuch=True, narrative='test')

def test_copy(sample_record):
    r1 = sample_record(trans_id=1, amount=3.0, flag=True, narrative='test')
    r2 = r1.copy()
    r1.flag = False
    assert r1.flag == False
    assert r2.flag == True

def test_get_set_clear(sample_record):

    r1 = sample_record(trans_id=1, amount=3.0, flag=True, narrative='test')
    r1.clear()
    assert all((x is None for x in r1.values()))

    r1.set_values((1, True, 3.0, 'test'))
    assert set(r1.item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r1.clear()

    r1.set_values(trans_id=1, amount=3.0, flag=True, narrative='test')
    assert set(r1.item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r1.set_values(trans_id=2, narrative='foobar')
    assert set(r1.item_values()) == \
            set((('trans_id', 2), ('flag', True), ('amount', 3.0), ('narrative', 'foobar')))

    r1.set('flag', False)

    assert set(r1.item_values()) == \
            set((('trans_id', 2), ('flag', False), ('amount', 3.0), ('narrative', 'foobar')))

    assert r1.get('trans_id') == 2

    assert r1.get('trans_id', context={'trans_id' : 3}) == 3

    # Using the context dictionary parameter should have updated the stored value for
    # trans_id as well as returning the new value
    assert r1.get('trans_id') == 3

    with pytest.raises(ValueError):
        r1.set('nonesuch', 5) # setting a non-existent attribute

    with pytest.raises(ValueError):
        r1.set_values((1, True, 3.0)) # wrong number of values supplied

    # non-existent attribute
    with pytest.raises(ValueError):
        r1.set_values(trans_id=1, amount=3.0, flag=True, nonesuch='test')

    # additional non-existent attribute
    with pytest.raises(ValueError):
        r1.set_values(trans_id=1, amount=3.0, flag=True, narrative='test', nonesuch=3)

def test_fields_values_items(sample_record):

    r1 = sample_record(trans_id=1, flag=True, amount=3.0, narrative='test')
    field_names = [x._name for x in r1.fields()]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']

    assert r1.values() == [1, True, 3.0, 'test']

    field_names = [x[0] for x in r1.items()]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']

    tmp = r1.item_values()
    field_names = [x[0] for x in tmp]
    field_values = [x[1] for x in tmp]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']
    assert field_values == [1, True, 3.0, 'test']

    ctxt = {'trans_id' : 4}
    tmp = r1.item_values(context=ctxt)
    field_names = [x[0] for x in tmp]
    field_values = [x[1] for x in tmp]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']
    assert field_values == [4, True, 3.0, 'test']

def test_get_context(sample_record):
    r1 = sample_record(trans_id=1, flag=True, amount=3.0, narrative='test')

    assert r1.get_context() == {'trans_id' : 1}

    r1.trans_id = 3

    assert r1.get_context() == {'trans_id' : 3}
