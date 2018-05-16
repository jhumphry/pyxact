'''An example of using pyxact with PostgreSQL and pyscopg2.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import argparse
import os

import psycopg2

from pyxact import postgresql
from pyxact import loggingdb

import example_schema

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Demonstrate pyxact with PostgreSQL')

    parser.add_argument('--log', help='Dump SQL commands to a file before executing them',
                        nargs='?', metavar='LOG FILE', default='STDOUT',
                        type=argparse.FileType('a'))
    parser.add_argument('--database',
                        help='PostgreSQL database to use (default pyxact)',
                        action='store', default='pyxact')
    parser.add_argument('--user', help='PostgreSQL user for upload',
                        action='store',
                        default=os.environ.get('USER', 'postgres'))
    parser.add_argument('--password', help='PostgreSQL user password',
                        action='store', default='')
    parser.add_argument('--host', help='PostgreSQL host (if using TCP/IP)',
                        action='store', default=None)
    parser.add_argument('--port', help='PostgreSQL port (if required)',
                        action='store', type=int, default=5432)
    args = parser.parse_args()

    if args.host:
        connection = psycopg2.connect(database=args.database,
                                      user=args.user,
                                      password=args.password,
                                      host=args.host,
                                      port=args.post)
    else:
        connection = psycopg2.connect(database=args.database,
                                      user=args.user,
                                      password=args.password)

    if args.log:
        if args.log == 'STDOUT':
            conn = loggingdb.Connection(inner_connection=connection)
        else:
            conn = loggingdb.Connection(inner_connection=connection, log_file=args.log)
    else:
        conn = connection

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor, dialect=postgresql.PostgreSQLDialect)
    example_schema.populate_example_schema(cursor, dialect=postgresql.PostgreSQLDialect)

    # It is possible to read existing AccountingTransactions back into Python. Simply set up the
    # context fields appropriately and call context_select to fill in the rest of the data.

    new_trans = example_schema.AccountingTransaction(tid=2)
    new_trans._context_select(cursor, dialect=postgresql.PostgreSQLDialect)

    assert new_trans.journal_list[2].account == 1003
    assert new_trans.transaction.narrative == 'Example usage of pyxact'
