'''Test SQLRecord'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.records as records
from pyxact.dialects import sqliteDialect

@pytest.fixture('module')
def sample_record_class():
    class SampleRecord(records.SQLRecord):
        trans_id=fields.IntField(context_used='trans_id')
        flag=fields.BooleanField()
        amount=fields.NumericField(precision=6, scale=2, allow_floats=True)
        narrative=fields.TextField()

    return SampleRecord

@pytest.fixture('module')
def sample_records(sample_record_class):

    result = []
    result.append(sample_record_class(1, True, '3.4', 'Line 1'))
    result.append(sample_record_class(2, False, '1.1', 'Line 2'))
    result.append(sample_record_class(3, True, '-10.4', 'Line 3'))
    result.append(sample_record_class(4, False, '2.2', 'Line 4'))

    return result

def test_table_creation(sample_record_class):

    assert [x.sql_name for x in sample_record_class._sqlfields()] == \
                ['trans_id', 'flag', 'amount', 'narrative']

def test_colliding_field_names():

    class OKRecord(records.SQLRecord):
            ok_name = fields.IntField()
            insert_sql = fields.IntField()

    # SQLRecordMetaClass should not allow subclasses of SQLRecord with names that collide with
    # built-in methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord(records.SQLRecord):
            ok_name = fields.IntField()
            _context_values_stored = fields.IntField()

    # SQLRecordMetaClass should not allow subclasses of SQLRecord with names that collide with
    # built-in methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord2(records.SQLRecord):
            ok_name = fields.IntField()
            _fields = fields.IntField()

def test_initialization(sample_record_class):
    r1 = sample_record_class()
    assert all((x is None for x in r1._values()))

    r2 = sample_record_class(1, True, 3.0, 'test')
    assert set(r2._item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r3 = sample_record_class(trans_id=1, amount=3.0, flag=True, narrative='test')
    assert set(r3._item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r4 = sample_record_class(trans_id=1, amount=3.0, narrative='test')
    assert r4.flag is None

    with pytest.raises(ValueError):
        r5 = sample_record_class(1, True, 3.0)

    with pytest.raises(ValueError):
        r6 = sample_record_class(trans_id=1, amount=3.0, nonesuch=True, narrative='test')

def test_copy(sample_record_class):
    r1 = sample_record_class(trans_id=1, amount=3.0, flag=True, narrative='test')
    r2 = r1._copy()
    r1.flag = False
    assert r1.flag == False
    assert r2.flag == True

def test_get_set_clear(sample_record_class):

    r1 = sample_record_class(trans_id=1, amount=3.0, flag=True, narrative='test')
    r1._clear()
    assert all((x is None for x in r1._values()))

    r1._set_values((1, True, 3.0, 'test'))
    assert set(r1._item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r1._clear()

    r1._set_values(trans_id=1, amount=3.0, flag=True, narrative='test')
    assert set(r1._item_values()) == \
            set((('trans_id', 1), ('flag', True), ('amount', 3.0), ('narrative', 'test')))

    r1._set_values(trans_id=2, narrative='foobar')
    assert set(r1._item_values()) == \
            set((('trans_id', 2), ('flag', True), ('amount', 3.0), ('narrative', 'foobar')))

    r1._set('flag', False)

    assert set(r1._item_values()) == \
            set((('trans_id', 2), ('flag', False), ('amount', 3.0), ('narrative', 'foobar')))

    assert r1._get('trans_id') == 2

    assert r1._get('trans_id', context={'trans_id' : 3}) == 3

    # Using the context dictionary parameter should have updated the stored value for
    # trans_id as well as returning the new value
    assert r1._get('trans_id') == 3

    with pytest.raises(ValueError):
        r1._set('nonesuch', 5) # setting a non-existent attribute

    with pytest.raises(ValueError):
        r1._set_values((1, True, 3.0)) # wrong number of values supplied

    # non-existent attribute
    with pytest.raises(ValueError):
        r1._set_values(trans_id=1, amount=3.0, flag=True, nonesuch='test')

    # additional non-existent attribute
    with pytest.raises(ValueError):
        r1._set_values(trans_id=1, amount=3.0, flag=True, narrative='test', nonesuch=3)

def test_fields_values_items(sample_record_class):

    r1 = sample_record_class(trans_id=1, flag=True, amount=3.0, narrative='test')
    field_names = [x.name for x in r1._sqlfields()]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']

    assert r1._values() == [1, True, 3.0, 'test']

    field_names = [x[0] for x in r1._items()]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']

    tmp = r1._item_values()
    field_names = [x[0] for x in tmp]
    field_values = [x[1] for x in tmp]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']
    assert field_values == [1, True, 3.0, 'test']

    ctxt = {'trans_id' : 4}
    tmp = r1._item_values(context=ctxt)
    field_names = [x[0] for x in tmp]
    field_values = [x[1] for x in tmp]
    assert field_names == ['trans_id', 'flag', 'amount', 'narrative']
    assert field_values == [4, True, 3.0, 'test']

def test_context_values_stored(sample_record_class):
    r1 = sample_record_class(trans_id=1, flag=True, amount=3.0, narrative='test')

    assert r1._context_values_stored() == {'trans_id' : 1}

    r1.trans_id = 3

    assert r1._context_values_stored() == {'trans_id' : 3}


