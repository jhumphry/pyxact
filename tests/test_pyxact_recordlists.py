# Test pyxact.recordlists

import pytest
import pyxact.fields as fields
import pyxact.records as records
import pyxact.recordlists as recordlists
from pyxact.dialects import sqliteDialect

class SimpleRecord(records.SQLRecord):
    foo=fields.IntField()
    bar=fields.TextField()
    baz=fields.IntField()

simple_records = [SimpleRecord(1, 'line1', 2),
                  SimpleRecord(3, 'line2', 4),
                  SimpleRecord(5, 'line3', 6),
                  SimpleRecord(7, 'line4', 8)]

class SimpleRecordList(recordlists.SQLRecordList, record_type=SimpleRecord):
    pass

def test_recordlist_metaclass():
    assert SimpleRecordList()._record_type == SimpleRecord
    assert hasattr(SimpleRecordList, 'foo')
    assert hasattr(SimpleRecordList, 'bar')
    assert hasattr(SimpleRecordList, 'baz')

def test_colliding_field_names():

    # This will be OK as an SQLRecord subclass but will collide with a method
    # when turned into an SQLRecord
    class CollidingFields(records.SQLRecord):
        _append=fields.IntField()
    with pytest.raises(AttributeError, message='Should reject a record_type that has a name '
                                               'that collides with an SQLRecord method/attribute.'):
        class CollidingFieldsLists(recordlists.SQLRecordList, record_type=CollidingFields):
            pass

    class CollidingFields2(records.SQLRecord):
        _records=fields.IntField()
    with pytest.raises(AttributeError, message='Should reject a record_type that has a name '
                                               'that collides with an SQLRecord method/attribute.'):
        class CollidingFieldsLists(recordlists.SQLRecordList, record_type=CollidingFields2):
            pass

def test_recordlist_creation():
    rl1 = SimpleRecordList()
    rl1._extend(simple_records)
    assert list(rl1.foo) == [1, 3, 5, 7]

    rl2 = SimpleRecordList(simple_records)
    assert list(rl2.baz) == [2, 4, 6, 8]

    rl3 = SimpleRecordList(*simple_records)
    assert list(rl3.bar) == ['line1', 'line2', 'line3', 'line4']

    rl4 = SimpleRecordList()
    for i in simple_records:
        rl4._append(i)
    assert list(rl4.foo) == [1, 3, 5, 7]

def test_recordlist_clear_copy():
    rl1 = SimpleRecordList([i._copy() for i in simple_records])
    assert len(rl1) == 4
    rl2 = rl1
    rl3 = rl1._copy()

    assert rl2[0].foo == 1
    rl1[0].foo = 99
    assert rl2[0].foo == 99
    assert rl3[0].foo == 1

    rl3._clear()
    assert len(rl3) == 0
    assert len(rl1) == 4

    with pytest.raises(IndexError):
        assert rl3[0].foo == 1

def test_recordlist_itermethods():

    rl1 = SimpleRecordList(simple_records)

    tmp = []
    for i in rl1:
        tmp.append(i.foo)
    assert tmp == [1, 3, 5, 7]

    tmp = []
    for i in reversed(rl1):
        tmp.append(i.foo)
    assert tmp == [7, 5, 3, 1]

def test_recordlist_getsetdeleteinsert():

    rl1 = SimpleRecordList([i._copy() for i in simple_records])
    assert list(rl1.foo) == [1, 3, 5, 7]
    assert len(rl1) == 4
    del rl1[1]
    assert list(rl1.foo) == [1, 5, 7]
    assert len(rl1) == 3

    rl1._insert(0, rl1[2]._copy()) # Don't insert another reference to the same obj!
    assert len(rl1) == 4
    assert list(rl1.foo) == [7, 1, 5, 7]

def test_recordlist_values():
    rl1 = SimpleRecordList([i._copy() for i in simple_records])

    expected_values = [[1, 'line1', 2],
                       [3, 'line2', 4],
                       [5, 'line3', 6],
                       [7, 'line4', 8]]

    assert rl1._values() == expected_values

    # In this case the sqlite3 does not require translation of values
    assert rl1._values_sql_repr(sqliteDialect) == expected_values
