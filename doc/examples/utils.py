'''Utility functions for the examples.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import argparse
import os
import sys
import sqlite3

try:
    import psycopg2
    POSTGRESQL_AVAILABLE = True
except:
    POSTGRESQL_AVAILABLE = False

from pyxact import dialects, loggingdb, loggingdb
import pyxact.psycopg2

DATABASE_USED = None

def process_command_line(description='Demonstrate pyxact'):
    '''Process the command line arguments and return a functioning DB-API connection'''

    global DATABASE_USED

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('--log', help='Dump SQL commands to a file before executing them',
                        action='store', default=None)
    parser.add_argument('--postgresql', help='Whether to use PostgreSQL instead of SQLite',
                        action='store_true')
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

    if args.postgresql:
        if not POSTGRESQL_AVAILABLE:
            raise RuntimeError('PostgreSQL support not available')

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
        dialects.DefaultDialect = pyxact.psycopg2.Psycopg2Dialect
        # By changing DefaultDialect we change the default SQL dialect used whenever no specific
        # dialect parameter is passed to a relevant pyxact method.
        DATABASE_USED = 'PostgreSQL'

    else:
        connection = sqlite3.connect(':memory:')
        connection.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support
        dialects.DefaultDialect = dialects.sqliteDialect
        DATABASE_USED = 'SQLite'

    if args.log:
        if args.log == 'STDOUT':
            conn = loggingdb.Connection(inner_connection=connection)
        else:
            conn = loggingdb.Connection(inner_connection=connection, log_file=open(args.log, 'a'))
    else:
        conn = connection
    # pyxact.loggingdb is a facade that can save the SQL commands being executing and the parameters
    # used to a file for use in debugging.

    return conn
