'''An example of defining a simple database schema using pyxact.'''

import sqlite3
from decimal import Decimal as D

from pyxact import constraints, dialects, fields, indexes, queries, records
from pyxact import recordlists, schemas, sequences, tables, transactions, views

# These examples are going to sketch out a very simplistic version of an accounting application. All
# of the objects will be held in a schema called 'accounting'. In some databases this will map to an
# SQL schema in the database. In others, such as SQLite, where schema support is more limited it
# will be used as a prefix for the names of the objects, producing names such as 'accounting_object'.

accounting = schemas.SQLSchema('accounting')

# Now we need to create a persistent sequence for the schema we are going to build. Most databases
# have the concept of implicit sequences/identity columns, but it can then be hard to refer to the
# underlying sequence in a consistent manner. It is easier to use explicit sequences. The sequence
# may create a one row table of the same name to hold the state of the sequence. The use of the
# 'schema' parameter means that the sequence will be automatically registered in the 'accounting'
# schema object.

tid_seq = sequences.SQLSequence(name='tid_seq', schema=accounting)

# Tables in an SQL database are represented by subclasses of 'tables.SQLTable'. Class attributes
# which are subclasses of 'fields.SQLField' and 'constraints.SQLConstraint' represent columns and
# constraints in the respective SQL table. The 'tables.SQLTable' class uses Python metaclass magic to
# transform the created class, indexing and cross-referencing the fields and constraints. The
# 'table_name' parameter gives the SQL table name, and the use of the optional 'schema' parameter
# means that the table is automatically registered in the 'accounting' schema.

class TransactionTable(tables.SQLTable, table_name='transactions', schema=accounting):
    tid = fields.IntField(context_used='tid')
    creator = fields.CharField(max_length=3)
    creation_ts = fields.TimestampField(tz=False, context_used='creation_ts')
    t_rev = fields.BooleanField(sql_name='t_rev')
    narrative = fields.TextField()
    cons_pk = constraints.PrimaryKeyConstraint(column_names=('tid'))

# It should be relatively simple to see what is going on here if you are familiar with SQL. An
# instance of TransactionTable represents a row in the SQL table 'transactions'. It has attributes
# for 'tid', 'creator', 't_rev' etc. Unusually for Python, these are strongly typed and will only
# accept certain types of values. In addition, you can not dynamically create additional attributes
# on an instance of TransactionTable - only the four specified in the class definition above.

# There are a few things to explain. The 'tid' field operates slightly differently from the other
# SQLField instances because it has 'context_used' set and so it is sensitive to what is called a
# context dictionary. When the values of an instance of TransactionTable are retrieved, a context
# dictionary can be passed and the IntField will expect to be able to update itself from a value in
# that dictionary (in this case, under the name 'tid') and to return that value rather than any value
# previously stored. This is used where multiple rows need to have a consistent value - for example a
# transaction ID number. The context dictionary carries that state from one SQLTable instance to the
# next.

# All fields take an optional 'sql_name' parameter. This allows the name used in the SQL database to
# be different from that in Python. This is useful in cases where there is a name collision between
# the database and one of the provided methods or attributes of SQLTable.

# The 'constraints.PrimaryKeyConstraint' defines a primary key for the table. The 'column_names'
# parameter should be passed a sequence of names of SQLField attributes that have been added to the
# SQLTable subclass.

class JournalTable(tables.SQLTable, table_name='journals', schema=accounting):
    tid = fields.IntField(context_used='tid')
    row_id = fields.RowEnumIntField(context_used='row_id', starting_number=1)
    account = fields.IntField()
    amount = fields.NumericField(precision=8, scale=2, allow_floats=True)
    cons_pk = constraints.PrimaryKeyConstraint(column_names=('tid', 'row_id'))
    cons_fk = constraints.ForeignKeyConstraint(column_names=('tid',),
                                               foreign_table='transactions',
                                               foreign_schema=accounting)

# This table shows a few more features. The 'fields.RowEnumIntField' is another context-sensitive
# field. It retrieves (or creates) a counter in the context dictionary (if passed) and increments it.
# This means that if multiple rows (i.e. multiple instances of JournalTable) are inserted, they will
# each be given a unique row number, regardless of any previous value in the field.

# The 'fields.NumericField' represents a NUMERIC column in SQL. The 'allow_floats' parameter
# specifies whether fields of this sort can be initialised from a float, which can be inaccurate and
# cause data loss. To make it easier to work with SQLite (which does not support NUMERIC very well)
# it has been permitted in these examples.

# 'constraints.ForeignKeyConstraint' represents a foreign key constraint. In this case the
# 'sql_reference_names' parameter has not been used, so an assumption is made that the names of the
# columns in the foreign table are the same as those in the current table. Note that where
# 'sql_reference_names' is given explicitly, the names used in SQL must be given and not the Python
# names where the two differ.

class JournalList(recordlists.SQLRecordList, record_type=JournalTable):
    pass

# JournalList is a subclass of 'recordlists.SQLRecordList' which creates a special list-type class
# that can hold a sequence of JournalRecord. No attributes need to be created explicitly. Descriptor
# attributes will be added automatically for each of the SQLField on the underlying JournalRecord.
# When these descriptors on an instance of JournalList are read, they will return generator functions
# which yield the value for that SQLField for each record in turn.

journals_account_idx = indexes.SQLIndex(name='journals_account_idx',
                                        table=JournalTable,
                                        column_exprs=(indexes.IndexColumn('account', None, None),),
                                        unique=False,
                                        schema=accounting)

# Creating indexes on columns can also be done from Python. The key here is column_exprs, which takes
# a sequence of a mixture of 'fields.IndexColumn' or 'fields.IndexExpr', two 'namedtuple' types. In
# this case we are happy to accept the default collation and direction of the index on the 'account'
# column.

# Note that it is also possible to define completely custom SQL commands to create objects in a
# schema using the 'schemas.SQLSchema.register_sql_command' method. This can be used if the built-in
# facilities are insufficient.

SIMPLEVIEW_QUERY = '''
SELECT t.tid, t.creator, t.creation_ts, t.t_rev, t.narrative, j.row_id, j.account, j.amount
FROM {accounting.transactions} AS t JOIN {accounting.journals} AS j ON t.tid = j.tid
'''

# This string defines an SQL query that we are going to make an SQL view from. Note that table
# names are specified surrounded by braces. This allows pyxact to re-write the table names as
# 'accounting_transactions' (etc.) if the database in use does not handle user-defined schema.

class SimpleView(views.SQLView, view_name='simple_view', query=SIMPLEVIEW_QUERY, schema=accounting):
    tid = fields.IntField()
    creator = fields.CharField(max_length=3)
    creation_ts = fields.TimestampField(tz=False)
    t_rev = fields.BooleanField()
    narrative = fields.TextField()
    row_id = fields.IntField()
    account = fields.IntField()
    amount = fields.NumericField(precision=8, scale=2, allow_floats=True)

# This looks superficially similar to an SQLTable. Once again, class attributes which are SQLField
# are used to define the names and types of the values that are expected to be returned from the
# view. As the view is not writable, we do not bother using the context-sensitive field types.

class AccountingTransaction(transactions.SQLTransaction):
    tid = sequences.SequenceIntField(sequence=tid_seq)
    creation_ts = fields.UTCNowTimestampField()
    transaction = transactions.SQLTransactionField(TransactionTable)
    journal_list = transactions.SQLTransactionField(JournalList)

    def verify(self):
        return super().verify() and sum(self.journal_list.amount)==0

# AccountingTransaction ties together a single TransactionTable, a JournalList holding a variable
# number of JournalTable, and a number of 'context fields'. When the 'insert_new' method is called on
# an 'AccountingTransaction' instance, these fields will have their 'update' methods called, which in
# the case of a 'SequenceIntField' will get the next value from the sequence and in the case of a
# 'UTCNowTimestampField' will get the current time as a UTC timestamp. A context dictionary will be
# formed will these values under the names 'tid' and 'creation_ts' respectively and this dictionary
# will be used when getting the values of each of the TransactionTable and JournalTable instances
# prior to inserting them into the SQL database. This means that all of the records will have the
# same 'tid' and 'creation_ts' values, and the RowEnumIntField of the JournalTable records will have
# enumerated them.

# AccountingTransaction has been given an optional domain-specific verify method that checks for
# internal consistency - in this case checking that the accounting journal balances and is valid for
# double-entry book-keeping.


sample_transaction = TransactionTable(creator='ABC',
                                      t_rev=False,
                                      narrative='Example usage of pyxact')

sample_journals = JournalList(JournalTable(None, None, 1000, D('10.5')),
                              JournalTable(None, None, 1001, D('-5.5')),
                              JournalTable(None, None, 1002, D('-5.0'))
                             )

test_transaction1 = AccountingTransaction(transaction=sample_transaction,
                                          journal_list=sample_journals)


# Now we create an instance of an AccountingTransaction. Note that we do not specify values for
# certain fields where we expect them to be completed from the context dictionary, or automatically
# completed.

test_transaction2 = test_transaction1.copy()

test_transaction2.transaction.creator = 'DEF'
test_transaction2.journal_list[2].account = 1003

# Note that when we want to duplicate an SQLTransaction it is necessary to use the 'copy' method to
# ensure a deep copy is made - otherwise the values stored in one may just be aliases of the values
# stored in the other.

def create_example_schema(cursor, dialect=dialects.sqliteDialect):
    '''Create the example 'accounting' schema in the database using the cursor and the database
    dialect supplied. The dialect is an object that defines the peculiarities of a particular
    database and its Python database adaptor. Most methods will use the variable
    dialects.DefaultDialect if no dialect parameter is set. By default this is set for SQLite.'''

    cursor.execute('BEGIN TRANSACTION;')

    accounting.create_schema(cursor=cursor, dialect=dialect)
    accounting.create_schema_objects(cursor=cursor, dialect=dialect)

    cursor.execute('COMMIT TRANSACTION;')

def populate_example_schema(cursor, dialect=dialects.sqliteDialect):
    '''Add some sample data to the example 'accounting' schema.'''

    # Note that SQLTransactions issue 'BEGIN TRANSACTION' and 'COMMIT TRANSACTION' themselves

    test_transaction1.insert_new(cursor, dialect)
    test_transaction2.insert_new(cursor, dialect)

if __name__ == '__main__':
    conn = sqlite3.connect(':memory:')
    conn.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support

    cursor = conn.cursor()
    create_example_schema(cursor)
    populate_example_schema(cursor)

    # It is possible to read existing AccountingTransactions back into Python. Simply set up the
    # context fields appropriately and call context_select to fill in the rest of the data.

    new_trans = AccountingTransaction(tid=2)
    new_trans.context_select(cursor)

    assert new_trans.journal_list[2].account == 1003
    assert new_trans.transaction.narrative == 'Example usage of pyxact'

