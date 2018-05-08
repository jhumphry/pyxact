'''Test pyxact.transactions'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.tables as tables
import pyxact.transactions as transactions
import pyxact.constraints as constraints
from pyxact.dialects import sqliteDialect

class SpecialTextField(fields.TextField):

    def get_context(self, instance, context):
        return 'get_context'

    def refresh(self, instance, context, cursor, dialect=None):
        return 'refresh'

    def update(self, instance, context, cursor, dialect=None):
        return 'update'

@pytest.fixture('session')
def sample_special_table_class():
    class SampleSpecialTable(tables.SQLTable, table_name='sample_special_table'):
        trans_id=fields.IntField(context_used='trans_id')
        value=fields.IntField()
        narrative=fields.TextField(context_used='special_text')
        pk=constraints.PrimaryKeyConstraint(column_names=('trans_id'))

    return SampleSpecialTable

@pytest.fixture('module')
def sample_special_table(sqlitedb, sample_special_table_class):

    sqlitedb.execute(sample_special_table_class._create_table_sql())
    sqlitedb.execute(sample_special_table_class._truncate_table_sql())

    # This will raise an OperationalError if the table doesn't exist or the
    # names of the columns are wrong
    sqlitedb.execute('SELECT trans_id, narrative FROM sample_special_table;')
    sqlitedb.commit()

@pytest.fixture('session')
def sample_transaction_class(sample_special_table_class, sample_view_class):

    class SampleTransaction(transactions.SQLTransaction):
        trans_id = fields.IntField()
        special_text = SpecialTextField()
        view = transactions.SQLTransactionField(sample_view_class)
        data = transactions.SQLTransactionField(sample_special_table_class)

    return SampleTransaction

def test_colliding_field_names(sample_table_class, sample_transaction_class):

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _insert_new = fields.IntField()

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord2(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _isolation_level = transactions.SQLTransactionField(sample_table_class)

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord2(sample_transaction_class):
            ok_name = fields.IntField()
            _isolation_level = transactions.SQLTransactionField(sample_table_class)

def test_init(sample_special_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class()
    assert tmp1.trans_id is None

    tmp2 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_special_table_class(1, 99, 'Line 1')
                                    )

    with pytest.raises(TypeError, message='SQLTransaction should require positional attributes to be provided in a '
                                           'type-compliant order.'):
        tmp3 = sample_transaction_class(sample_view_class(),
                                        'foo',
                                        1,
                                        sample_special_table_class(1, 99, 'Line 1')
                                        )

    with pytest.raises(ValueError, message='SQLTransaction should reject init by positional args if the wrong number are specified.'):
        tmp4 = sample_transaction_class(1,
                                        'foo',
                                        sample_view_class()
                                        )

    tmp5 = sample_transaction_class(trans_id=1,
                                    special_text='foo',
                                    view=sample_view_class(),
                                    data=sample_special_table_class(1, 99, 'Line 1')
                                    )

    with pytest.raises(ValueError, message='SQLTransaction should reject init by incorrectly named args.'):
        tmp6 = sample_transaction_class(trans_id=1,
                                        special_text='foo',
                                        views=sample_view_class(),
                                        data=sample_special_table_class(1, 99, 'Line 1')
                                        )

def test_copy(sample_special_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_special_table_class(2, 99, 'Line 1')
                                    )
    tmp2 = tmp1
    tmp3 = tmp1._copy()

    tmp2.trans_id = 999
    assert tmp1.trans_id == 999
    assert tmp3.trans_id == 1

    tmp1.data.trans_id = 888
    assert tmp2.data.trans_id == 888
    assert tmp3.data.trans_id == 2

def test_get_context(sample_special_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_special_table_class(2, 99, 'Line 1')
                                    )
    assert tmp1.special_text == 'foo'

    ctxt = tmp1._get_context()
    assert ctxt['trans_id'] == 1
    assert ctxt['special_text'] == 'get_context'

    ctxt = tmp1._get_refreshed_context(None) # the database cursor will not actually be used...
    assert ctxt['trans_id'] == 1
    assert ctxt['special_text'] == 'refresh'

    ctxt = tmp1._get_updated_context(None) # the database cursor will not actually be used...
    assert ctxt['trans_id'] == 1
    assert ctxt['special_text'] == 'update'

    ctxt = tmp1._get_context_from_records()
    assert ctxt['trans_id'] == 2

    tmp1.trans_id = None
    ctxt = tmp1._get_context()
    assert 'trans_id' not in ctxt

def test_insert(sample_special_table_class, sample_special_table, sample_transaction_class, sqlitecur):

    tmp = sample_transaction_class()
    tmp.trans_id = 42
    tmp.special_text = 'Special Text'
    tmp.data = sample_special_table_class(None, 99, 'Narrative')

    sqlitecur.execute('DELETE FROM sample_special_table WHERE 1=1;')
    sqlitecur.execute('COMMIT;')
    tmp._insert_existing(sqlitecur)

    assert sqlitecur.execute('SELECT COUNT(*) FROM sample_special_table;').fetchone() == (1, )
    assert sqlitecur.execute('SELECT trans_id FROM sample_special_table;').fetchone() == (42, )
    assert sqlitecur.execute('SELECT narrative FROM sample_special_table;').fetchone() == ('get_context', )

    tmp.trans_id = 43
    tmp._insert_new(sqlitecur)
    assert sqlitecur.execute('SELECT COUNT(*) FROM sample_special_table;').fetchone() == (2, )
    assert sqlitecur.execute('SELECT MAX(trans_id) FROM sample_special_table;').fetchone() == (43, )
    assert sqlitecur.execute('SELECT narrative FROM sample_special_table WHERE trans_id=43;').fetchone() == ('update', )

def test_update(sample_special_table_class, sample_special_table, sample_transaction_class, sqlitecur):

    tmp = sample_transaction_class()
    tmp.trans_id = 42
    tmp.special_text = 'Special Text'
    tmp.data = sample_special_table_class(None, 99, 'Narrative')

    sqlitecur.execute('DELETE FROM sample_special_table WHERE 1=1;')
    sqlitecur.execute('COMMIT;')
    tmp._insert_existing(sqlitecur)

    assert sqlitecur.execute('SELECT COUNT(*) FROM sample_special_table;').fetchone() == (1, )
    assert sqlitecur.execute('SELECT trans_id FROM sample_special_table;').fetchone() == (42, )
    assert sqlitecur.execute('SELECT value FROM sample_special_table;').fetchone() == (99, )
    assert sqlitecur.execute('SELECT narrative FROM sample_special_table;').fetchone() == ('get_context', )

    tmp.data.value = 88
    tmp._update(sqlitecur)
    assert sqlitecur.execute('SELECT COUNT(*) FROM sample_special_table;').fetchone() == (1, )
    assert sqlitecur.execute('SELECT MAX(trans_id) FROM sample_special_table;').fetchone() == (42, )
    assert sqlitecur.execute('SELECT value FROM sample_special_table;').fetchone() == (88, )
    assert sqlitecur.execute('SELECT narrative FROM sample_special_table;').fetchone() == ('get_context', )
