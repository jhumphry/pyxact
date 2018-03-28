'''A simple usage example for pyxact, showing the creation of an set of database
tables and the insertion and retrieval of linked records.'''

import sqlite3
from decimal import Decimal as D

from pyxact import constraints, dialects, fields, queries, records
from pyxact import recordlists, schemas, sequences, tables, transactions, views

# Create a schema to hold our database objects. SQLite does not really support
# schema, but this will automatically be worked around - objects will be renamed
# 'schema_object'

accounting = schemas.SQLSchema('accounting')

# Create a persistent sequence for the schema we are going to build. Most
# databases have the concept of implicit sequences/identity columns, but it can
# then be hard to refer to the underlying sequence in a consistent manner. We
# prefer explict sequences here.

trans_id_seq = sequences.SQLSequence(name='trans_id_seq', schema=accounting)

# Now start building up a schema by subclassing records.SQLRecord, adding
# SQLField class attributes to represent the columns and SQLConstraint class
# attributes to represent table constraints.

class TransactionRecord(tables.SQLTable, table_name='transactions', schema=accounting):
    trans_id = fields.ContextIntField(context_used='trans_id') # Context dicts are explained later
    created_by = fields.CharField(max_length=3)
    trans_reversed = fields.BooleanField()
    narrative = fields.TextField()
    cons_pk = constraints.PrimaryKeyConstraint(column_names=('trans_id'))

class JournalRecord(tables.SQLTable, table_name='journals', schema=accounting):
    trans_id = fields.ContextIntField(context_used='trans_id')
    row_id = fields.RowEnumIntField(context_used='row_id', starting_number=1)
    account = fields.IntField()
    amount = fields.NumericField(precision=8, scale=2, allow_floats=True)
    cons_pk = constraints.PrimaryKeyConstraint(column_names=('trans_id', 'row_id'))
    cons_fk = constraints.ForeignKeyConstraint(column_names=('trans_id',),
                                               foreign_table='transactions',
                                               foreign_schema=accounting)

# Creating a simple view joining the two tables

SIMPLEVIEW_QUERY = '''
SELECT t.trans_id, t.created_by, t.trans_reversed, t.narrative, j.row_id, j.account, j.amount
FROM accounting_transactions AS t JOIN accounting_journals AS j ON t.trans_id = j.trans_id
'''

class SimpleView(views.SQLView,
                 view_name='simple_view',
                 query=SIMPLEVIEW_QUERY,
                 schema=accounting):
    trans_id = fields.ContextIntField(context_used='trans_id')
    created_by = fields.CharField(max_length=3)
    trans_reversed = fields.BooleanField()
    narrative = fields.TextField()
    row_id = fields.RowEnumIntField(context_used='row_id', starting_number=1)
    account = fields.IntField()
    amount = fields.NumericField(precision=8, scale=2, allow_floats=True)

# Create an in-memory sqlite3 database to work on

conn = sqlite3.connect(':memory:')
conn.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support

cursor = conn.cursor()

# Create the 'accounting' schema and the objects defined above

cursor.execute('BEGIN TRANSACTION;')

accounting.create_schema(cursor=cursor, dialect=dialects.sqliteDialect)
accounting.create_schema_objects(cursor=cursor, dialect=dialects.sqliteDialect)

cursor.execute('COMMIT TRANSACTION;')

# JournalList is created to be a list-type class that can hold JournalRecord. No
# attributes need to be created explicitly, but descriptors will be added for
# each of the SQLField on the underlying JournalRecord. These descriptors will
# return generators giving the value for that SQLField for each record in turn.

class JournalList(recordlists.SQLRecordList, record_type=JournalRecord):
    pass

# AccountingTransaction ties together a single TransactionRecord, and JournalList
# holding a variable number of JournalRecord. The trans_id SQLField is a 'context
# field'. When the AccountingTransaction is inserted into the database, it will
# update itself from the associated sequence and store the value in a 'context
# dictionary' under the name 'trans_id'. This context dictionary will be passed
# to the rows, and when the rows' values are extracted the IDIntField on both the
# TransactionRecord and the JournalRecord will pick up the value from the context
# dictionary rather than any value manually assigned to the field. This ensures
# the value is consistent for the whole AcccountingTransaction and is a fresh
# value from the sequence.

# AccountingTransaction has a domain-specific verify method that checks for
# internal consistency - in this case checking that the accounting journal
# balances.

class AccountingTransaction(transactions.SQLTransaction):
    trans_id = sequences.SequenceIntField(sequence=trans_id_seq)
    trans_details = transactions.SQLTransactionField(TransactionRecord)
    journal_list = transactions.SQLTransactionField(JournalList)

    def verify(self):
        return super().verify() and sum(self.journal_list.amount)==D(0)

# Now we create an instance of an AccountingTransaction and insert it.
# Note we do not need to supply the trans_id as we do not know it at this time.
# row_id can also be ignored as they will automatically enumerate themselves.

sample_transaction = TransactionRecord(created_by='ABC',
                                       trans_reversed=False,
                                       narrative='Example usage of pyxact')

sample_journals = JournalList(JournalRecord(None, None, 1000, D('10.5')),
                              JournalRecord(None, None, 1001, D('-5.5')),
                              JournalRecord(None, None, 1002, D('-5.0'))
                             )

assert sum(sample_journals.amount) == 0

test_trans = AccountingTransaction(trans_details=sample_transaction,
                                   journal_list=sample_journals)

test_trans.insert_new(cursor)

# We can assign new values to the attributes of the AccountingTransaction
# instance and re-insert it. the trans_id context field will pick up a suitable
# new value from the sequence it is linked to.

test_trans.trans_details.created_by = 'DEF'
test_trans.journal_list[2].account = 1003
test_trans.insert_new(cursor)

# It is possible to read existing AccountingTransactions back into Python. Simply
# set up the context fields appropriately and call context_select to fill in the
# rest of the data.

new_trans = AccountingTransaction(trans_id=2)
new_trans.context_select(cursor)

assert new_trans.journal_list[2].account == 1003
assert new_trans.trans_details.narrative == 'Example usage of pyxact'

# It is possible to subclass AccountingTransaction and change the normalize()
# method which normalizes the data after it has been read in. It will inherit
# all of the SQLField, SQLRecord and SQLRecordList attributes from the base
# class

class ReverseTransaction(AccountingTransaction):

    def post_select_hook(self, context, cursor, dialect):
        super().post_select_hook(context, cursor, dialect)
        for i in self.journal_list:
            i.amount = -i.amount
        self.trans_details.trans_reversed = True

rev_trans = ReverseTransaction(trans_id=1)
rev_trans.context_select(cursor)

# rev_trans will be normalized at the end of the context_select - i.e. the sign
# of the amounts will have been flipped

rev_trans.insert_new(cursor)

# This usage of SQLQuery shows a very simple usage case with no parameters

class TransactionCountQuery(queries.SQLQuery,
                            query='''SELECT COUNT(*) FROM accounting_transactions;'''):
    pass

trans_count_query = TransactionCountQuery()
trans_count_query.execute(cursor)

assert trans_count_query.result_singlevalue(cursor) == 3

# Complex queries can also be handled by subclassing SQLQuery. Here we first
# write the query, define an SQLRecord subclass and a matching SQLRecordList
# subclass to handle the result. Note that it is not strictly necessary to make
# an SQLRecord subclass if you are happy to fetch and process the data yourself -
# the SQLRecord approach does allow for some type checking.

# Any instances of '{foo}' in the text of the SQL query passed to the SQLQuery
# will be replaced with the contents of a suitably named SQLField (i.e. 'foo')
# added to the SQLQuery as a context field. Records can be retrieved with the
# result_records method (which returns a generator that gives each result in
# turn), the result_recordlist method (which downloads and returns all results at
# once) or the result_singlevalue method which assumes the SQL query will return
# a single value.

# Any instances of '{schema.obj}' will be replaced with the SQL standard
# 'schema.obj' or the work-around 'schema_obj' depending on whether the database
# in use supports SQL schema sufficiently - sqlite3 does not, for example.

JOURNAL_ROW_COUNT_QUERY = '''
SELECT transactions.created_by, transactions.trans_id, COUNT(*) AS row_count
FROM {accounting.journals} AS journals
JOIN {accounting.transactions} as transactions ON journals.trans_id = transactions.trans_id
WHERE transactions.created_by LIKE {created_by}
GROUP BY transactions.created_by, transactions.trans_id;
'''

class JournalRowCountResult(records.SQLRecord):
    created_by = fields.CharField(max_length=3)
    trans_id = fields.IntField()
    row_count = fields.IntField()

class JournalRowCountResultList(recordlists.SQLRecordList, record_type=JournalRowCountResult):
    pass

class JournalRowCountQuery(queries.SQLQuery,
                           query=JOURNAL_ROW_COUNT_QUERY,
                           record_type=JournalRowCountResult,
                           recordlist_type=JournalRowCountResultList):
    created_by = fields.CharField(max_length=3)

test_query = JournalRowCountQuery(created_by='%')
test_query.execute(cursor)

print('\nQuery result:\n')

for i in test_query.result_records(cursor):
    print(i)
