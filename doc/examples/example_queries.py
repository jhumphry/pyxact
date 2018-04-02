'''An example of using SQL queries under pyxact.'''

import sqlite3

from pyxact import dialects, fields, queries, records, recordlists

import example_schema

# This module relies on the 'accounting' schema created and populated in the example_schema.py
# file. It will define various queries and demonstrate their use.

class TransactionCountQuery(queries.SQLQuery,
                            query='''SELECT COUNT(*) FROM accounting_transactions;'''):
    pass

# This is the simplest sort of query. It has no parameters, and because it is only going to return a
# single value, there is no particular need to provide an SQLRecord type for the return values.

JOURNAL_ROW_COUNT_QUERY = '''
SELECT transactions.creator, transactions.tid, COUNT(*) AS row_count
FROM {accounting.journals} AS journals
JOIN {accounting.transactions} as transactions ON journals.tid = transactions.tid
WHERE transactions.creator LIKE {creator_pattern}
GROUP BY transactions.creator, transactions.tid;
'''

# This is a more complicated SQL query. It attempts to get a list of transactions and the number
# of journal entry lines each one involves for a transactions with a 'creator' that match the
# pattern provided.

# Any instances of '{foo}' in the text of the SQL query passed to the SQLQuery will be replaced with
# the contents of a suitably named SQLField (i.e. 'foo') added to the SQLQuery as a context field.
# Records can be retrieved with the result_records method (which returns a generator that gives each
# result in turn), the result_recordlist method (which downloads and returns all results at once) or
# the result_singlevalue method which assumes the SQL query will return a single value.

# Any instances of '{schema.obj}' will be replaced with the SQL standard 'schema.obj' or the
# work-around 'schema_obj' depending on whether the database in use supports SQL schema sufficiently
# - sqlite3 does not, for example.

class JournalRowCountResult(records.SQLRecord):
    creator = fields.CharField(max_length=3)
    tid = fields.IntField()
    row_count = fields.IntField()

class JournalRowCountResultList(recordlists.SQLRecordList, record_type=JournalRowCountResult):
    pass

# Complex queries can also be handled by subclassing SQLQuery. Here we first define an SQLRecord
# subclass and a matching SQLRecordList subclass to handle the result. SQLRecord is the parent class
# of SQLTable which represents a named, typed tuple of values- it does not support SQLConstraint and
# does not have table-specific parameters such as a 'table_name' or 'schema'.

# Note that it is not strictly necessary to make an SQLRecord subclass if you are happy to fetch and
# process the data yourself - the SQLRecord approach does allow for some type checking and named
# fields.

class JournalRowCountQuery(queries.SQLQuery,
                           query=JOURNAL_ROW_COUNT_QUERY,
                           record_type=JournalRowCountResult,
                           recordlist_type=JournalRowCountResultList):
    creator_pattern = fields.CharField(max_length=3)

# The SQLQuery has a single context field. On instances of SQLQuery, the value this contains will
# used to parametise the query.

if __name__ == '__main__':
    conn = sqlite3.connect(':memory:')
    conn.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor)
    example_schema.populate_example_schema(cursor)

    # First we are going to count the number of transactions in the example schema...
    transaction_count_query = TransactionCountQuery()
    transaction_count_query._execute(cursor)
    assert transaction_count_query._result_singlevalue(cursor) == 2

    # Now we are going to count the number of journal rows for all transactions created by all
    # creators (all creators are selected as the 'created_by' pattern is the SQL wildcard '%').
    journal_row_count_query = JournalRowCountQuery(creator_pattern='%')
    journal_row_count_query._execute(cursor)

    print('\nTransactions and associated journal entries:\n')

    for i in journal_row_count_query._result_records(cursor):
        print(i)

    # SQLQuery instances have a result_records generator method that returns one SQLRecord for each
    # row returned by the cursor, assuming this follows the use of the execute method to actually
    # call the query. The SQLRecord types have a suitable __str__ method defined, so they will print
    # their contents susefully, if somewhat verbosely.
