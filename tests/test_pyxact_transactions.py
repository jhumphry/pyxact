'''Test pyxact.transactions'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.transactions as transactions
from pyxact.dialects import sqliteDialect

class SpecialTextField(fields.TextField):

    def get_context(self, instance, context):
        return 'get_context'

    def refresh(self, instance, context, cursor, dialect=None):
        return 'refresh'

    def update(self, instance, context, cursor, dialect=None):
        return 'update'

@pytest.fixture('session')
def sample_transaction_class(sample_table_class, sample_view_class):

    class SampleTransaction(transactions.SQLTransaction):
        trans_id = fields.IntField()
        special_text = SpecialTextField()
        view = transactions.SQLTransactionField(sample_view_class)
        data = transactions.SQLTransactionField(sample_table_class)

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

def test_init(sample_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class()
    assert tmp1.trans_id is None

    tmp2 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_table_class(1, True, '3.4', 'Line 1')
                                    )

    with pytest.raises(TypeError, message='SQLTransaction should require positional attributes to be provided in a '
                                           'type-compliant order.'):
        tmp3 = sample_transaction_class(sample_view_class(),
                                        'foo',
                                        1,
                                        sample_table_class(1, True, '3.4', 'Line 1')
                                        )

    with pytest.raises(ValueError, message='SQLTransaction should reject init by positional args if the wrong number are specified.'):
        tmp4 = sample_transaction_class(1,
                                        'foo',
                                        sample_view_class()
                                        )

    tmp5 = sample_transaction_class(trans_id=1,
                                    special_text='foo',
                                    view=sample_view_class(),
                                    data=sample_table_class(1, True, '3.4', 'Line 1')
                                    )

    with pytest.raises(ValueError, message='SQLTransaction should reject init by incorrectly named args.'):
        tmp6 = sample_transaction_class(trans_id=1,
                                        special_text='foo',
                                        views=sample_view_class(),
                                        data=sample_table_class(1, True, '3.4', 'Line 1')
                                        )

def test_copy(sample_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_table_class(2, True, '3.4', 'Line 1')
                                    )
    tmp2 = tmp1
    tmp3 = tmp1._copy()

    tmp2.trans_id = 999
    assert tmp1.trans_id == 999
    assert tmp3.trans_id == 1

    tmp1.data.trans_id = 888
    assert tmp2.data.trans_id == 888
    assert tmp3.data.trans_id == 2

def test_get_context(sample_table_class, sample_view_class, sample_transaction_class):

    tmp1 = sample_transaction_class(1,
                                    'foo',
                                    sample_view_class(),
                                    sample_table_class(2, True, '3.4', 'Line 1')
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
