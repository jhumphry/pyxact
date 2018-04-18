# Test pyxact.transactions

import pytest
import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.sequences as sequences
import pyxact.tables as tables
import pyxact.transactions as transactions
from pyxact.dialects import sqliteDialect

@pytest.fixture('session')
def sample_transaction_class(sample_table_class, sample_view_class):

    class SampleTransaction(transactions.SQLTransaction):
        trans_id = sequences.SequenceIntField(sequence=tid_seq)
        view = transactions.SQLTransactionField(sample_view_class)
        transaction = transactions.SQLTransactionField(sample_table_class)

def test_colliding_field_names(sample_table_class):

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _insert_new = fields.IntField()

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _insert_new = transactions.SQLTransactionField(sample_table_class)

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord2(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _isolation_level = fields.IntField()

    with pytest.raises(AttributeError, message='SQLTransactionMetaClass should not allow subclasses of SQLTransaction'
                                               ' with attribute names that collide with built-in methods/attributes.'):
        class FailRecord2(transactions.SQLTransaction):
            ok_name = fields.IntField()
            _isolation_level = transactions.SQLTransactionField(sample_table_class)

