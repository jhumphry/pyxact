# A basic usage example for pyxact

import sqlite3
from decimal import Decimal as D

import pyxact.fields as fields
import pyxact.constraints as constraints
import pyxact.records as records
import pyxact.recordlists as recordlists
import pyxact.sequences as sequences
import pyxact.transactions as transactions
from pyxact.dialects import sqliteDialect

# Create an in-memory database to work on
conn = sqlite3.connect(':memory:')

# Must be done first if SQLite3 is to enforce foreign key constraints
conn.execute('PRAGMA foreign_keys = ON;')

cursor = conn.cursor()
cursor.execute('BEGIN TRANSACTION;')

# Create a persistent sequence
trans_id_seq = sequences.SQLSequence(name='trans_id_seq')

# Creation can require more than one SQL statement
trans_id_seq.create(cursor, sqliteDialect)

class TransactionRecord(records.SQLRecord, table_name='transactions'):
    trans_id = fields.IDIntField(context_used='trans_id')
    created_by = fields.CharField(max_length=3)
    trans_reversed = fields.BooleanField()
    narrative = fields.TextField()
    pk = constraints.PrimaryKeyConstraint(sql_column_names=('trans_id'))

cursor.execute(TransactionRecord.create_table_sql(sqliteDialect))

class JournalRecord(records.SQLRecord, table_name='journals'):
    trans_id = fields.IDIntField(context_used='trans_id')
    row_id = fields.RowEnumIntField(context_used='row_id', starting_number=1)
    account = fields.IntField()
    amount = fields.NumericField(precision=8, scale=6, allow_floats=True)
    cons_pk = constraints.PrimaryKeyConstraint(sql_column_names=('trans_id', 'row_id'))
    cons_fk = constraints.ForeignKeyConstraint(sql_column_names=('trans_id',),
                                               foreign_table='transactions')

cursor.execute(JournalRecord.create_table_sql(sqliteDialect))

cursor.execute('COMMIT TRANSACTION;')

class JournalList(recordlists.SQLRecordList, record_class=JournalRecord):
    pass

class AccountingTransaction(transactions.SQLTransaction):
    trans_id = fields.SequenceIntField(sequence=trans_id_seq)
    trans_details = transactions.SQLTransactionField(TransactionRecord)
    journal_list = transactions.SQLTransactionField(JournalList)

# Note we do not supply the trans_id as we do not know it at this time
sample_transaction = TransactionRecord(created_by='ABC',
                                       trans_reversed=False,
                                       narrative='Example usage of pyxact')

sample_journals = JournalList(JournalRecord(None, None, 1000, D('10.5')),
                              JournalRecord(None, None, 1001, D('-5.5')),
                              JournalRecord(None, None, 1002, D('-5.0'))
                             )

assert sum(sample_journals.amount) == 0

test_trans = AccountingTransaction(trans_details = sample_transaction,
                                   journal_list = sample_journals)

test_trans.insert_new(cursor, sqliteDialect)

cursor.execute(*JournalRecord.simple_select_sql(sqliteDialect, trans_id=1, row_id=2))
values = cursor.fetchone()
journal_retrieved = JournalRecord(*values)
