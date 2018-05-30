'''An example of using SQL queries under pyxact.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import sys

from pyxact import dialects, fields, loggingdb, queries, records, recordlists, transactions

import example_schema, utils

# This module relies on the 'accounting' schema created and populated in the example_schema.py
# file. It will define various queries and demonstrate their use.

class TransactionCountQuery(queries.SQLQuery,
                            query='SELECT COUNT(*) FROM {accounting.transactions};'):
    pass

# This is the simplest sort of query. It has no parameters, and because it is only going to return a
# single value, there is no particular need to provide an SQLRecord type for the return values.

# Any instances of '{schema.obj}' will be replaced with the SQL standard 'schema.obj' or the
# work-around 'schema_obj' depending on whether the database in use supports SQL schema sufficiently
# - sqlite3 does not, for example.

class JournalRowCount(records.SQLRecord):
    creator = fields.CharField(max_length=3)
    tid = fields.IntField()
    row_count = fields.IntField()

# Complex queries can also be handled by subclassing SQLQuery. First define an SQLRecord subclass to
# handle the result. SQLRecord is the parent class of SQLTable which represents a named, typed tuple
# of values - it does not support SQLConstraint and does not have table-specific parameters such as
# a 'table_name' or 'schema'.

# Note that it is not strictly necessary to make an SQLRecord subclass if you are happy to fetch and
# process the data yourself - the SQLRecord approach does allow for some type checking and named
# fields.

JOURNAL_ROW_COUNT_QUERY_BY_CREATOR = '''
SELECT transactions.creator, transactions.tid, COUNT(*) AS row_count
FROM {accounting.journals} AS journals
JOIN {accounting.transactions} AS transactions ON journals.tid = transactions.tid
WHERE transactions.creator LIKE {creator_pattern}
GROUP BY transactions.creator, transactions.tid;
'''

# This is the text of  more complicated SQL query. It attempts to get a list of transactions and the
# number of journal entry lines each one involves for a transactions with a 'creator' that match the
# pattern provided.

# Any instances of '{foo}' in the text of the SQL query passed to the SQLQuery will be replaced with
# the contents of a suitably named SQLField (i.e. 'foo') added to the SQLQuery as a context field.
# Records can be retrieved with the result_records method (which returns a generator that gives each
# result in turn), the result_recordlist method (which downloads and returns all results at once) or
# the result_singlevalue method which assumes the SQL query will return a single value.

class JournalRowCountQueryCreator(queries.SQLQuery,
                                  query=JOURNAL_ROW_COUNT_QUERY_BY_CREATOR,
                                  record_type=JournalRowCount):
    creator_pattern = fields.CharField(max_length=3)

# The SQLQuery subclass has a single context field. On instances of SQLQuery, the value this
# contains will be used to parametise the query.

class JournalRowCountResultCreator(queries.SQLQueryResult, query=JournalRowCountQueryCreator):
    pass

# The SQLQueryResult is a specialised subclass of SQLRecordList that is linked to an SQLQuery and
# has a _refresh method that clears and then repopulates the SQLRecordList using the query.

JOURNAL_ROW_COUNT_QUERY_BY_TID = '''
SELECT transactions.creator, transactions.tid, COUNT(*) AS row_count
FROM {accounting.journals} AS journals
JOIN {accounting.transactions} AS transactions ON journals.tid = transactions.tid
WHERE transactions.tid = {tid}
GROUP BY transactions.creator, transactions.tid;
'''
class JournalRowCountQueryTID(queries.SQLQuery,
                              query=JOURNAL_ROW_COUNT_QUERY_BY_TID,
                              record_type=JournalRowCount):
    tid = fields.IntField()

class JournalRowCountResultTID(queries.SQLQueryResult, query=JournalRowCountQueryTID):
    pass

# This is another example of a parametised query. Like the previous query, it fetches row count by
# journal information, but this time the journal is specified by the 'tid' column rather than by
# the 'creator' column.

class QueryTransaction(transactions.SQLTransaction):
    tid = fields.IntField()
    total_transaction_count = fields.IntField(query=TransactionCountQuery)
    row_count = transactions.SQLTransactionField(JournalRowCountResultTID)

# The QueryTransaction class combines two queries. When the tid context field is set and the
# _context_select_sql method is called on the transaction, the queries will be updated and the
# appropriate records from the view will be retrieved. (Note that in this case the
# TransactionCountQuery doesn't actually depend on the tid set, but the JournalRowCountResultQueryTID
# underneath the JournalRowCountResultTID does). The purpose of being able to do this is that all of
# the queries (and any view retrieval) will happen in a single transaction, so consistent results
# will be obtained even where there are concurrent writes to the database.

if __name__ == '__main__':

    conn = utils.process_command_line('Demonstrate usage of pyxact with simple queries')

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor)
    example_schema.populate_example_schema(cursor)

    # First we are going to count the number of transactions in the example schema...
    transaction_count_query = TransactionCountQuery()
    transaction_count_query._execute(cursor)
    assert transaction_count_query._result_singlevalue(cursor) == 2

    # Now we are going to count the number of journal rows for all transactions created by all
    # creators (all creators are selected as the 'created_by' pattern is the SQL wildcard '%').
    journal_row_count_query_creator = JournalRowCountResultCreator(creator_pattern='%')
    journal_row_count_query_creator._refresh(cursor)

    print('\nTransactions and associated journal entries:\n')

    for i in journal_row_count_query_creator:
        print(i)

    # SQLQuery instances have a result_records generator method that returns one SQLRecord for each
    # row returned by the cursor, assuming this follows the use of the execute method to actually
    # call the query. The SQLRecord types have a suitable __str__ method defined, so they will print
    # their contents usefully, if somewhat verbosely.

    query_transaction = QueryTransaction(tid=2)
    query_transaction._context_select(cursor)

    print('JournalRowCount for transaction 2:\n')

    for i in query_transaction.row_count:
        print(i)
