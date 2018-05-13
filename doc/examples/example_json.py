'''An example of using JSON serialization.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import json
import sqlite3
import sys

from pyxact import serialize_json
from pyxact import loggingdb
import example_schema

record_json = json.dumps(example_schema.test_transaction1.transaction,
                         cls=serialize_json.PyxactEncoder,
                         indent=4)
recordlist_json = json.dumps(example_schema.test_transaction1.journal_list,
                             cls=serialize_json.PyxactEncoder,
                             indent=4)
transaction_json = json.dumps(example_schema.test_transaction1,
                              cls=serialize_json.PyxactEncoder,
                              indent=4)

# PyxactEncoder is a customised JSON encoder class that knows how to serialise SQLRecord,
# SQLRecordList and SQLTransaction types into JSON.

custom_decoder = serialize_json.PyxactDecoder()
custom_decoder.register_sqlschema(example_schema.accounting)
custom_decoder.register_sqlrecordlist(example_schema.JournalList)
custom_decoder.register_sqltransaction(example_schema.AccountingTransaction)

# PyxactDecoder can turn the output of PyxactEncoder back into Python objects, provided the relevant
# classes have been registered with it.

if __name__ == '__main__':

    # You can see what SQL commands are being issued by specifying a log file name on the command
    # line, or you can specify STDOUT to get them printed out on the console.
    if len(sys.argv) == 1:
        conn = sqlite3.connect(':memory:')
    elif sys.argv[1].upper() == 'STDOUT':
        conn = loggingdb.Connection(inner_connection=sqlite3.connect(':memory:'))
    else:
        log_file = open(sys.argv[1], 'a')
        conn = loggingdb.Connection(inner_connection=sqlite3.connect(':memory:'),
                                    log_file=log_file)

    conn.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor)
    example_schema.populate_example_schema(cursor)
