'''A DB API connection and cursor facade that records the SQL commands and queries being passed
through it.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import sys

class Cursor:
    '''A  database cursor facade that implements a subset of DB-API methods and outputs information
    on the requests to a file or stdout.'''

    def __init__(self, inner_cursor, log_file=sys.stdout):
        self.inner_cursor = inner_cursor
        self.log_file = log_file
        self.context = None

    def __enter__(self):
        self.context = self.inner_cursor.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.context.__exit__(exc_type, exc_val, exc_tb)

    def execute(self, sql, params=None):
        '''Log a request to execute some SQL with given parameters'''

        self.log_file.write("Executed SQL: '{}' with params '{}'\n"
                            .format(sql, repr(params)))
        if params:
            self.inner_cursor.execute(sql, params)
        else:
            self.inner_cursor.execute(sql)

    def executemany(self, sql, params=None):
        '''Log a request to execute some SQL with multiple sets of parameters'''

        self.log_file.write("Executed SQL: '{}' with params:\n"
                            .format(sql))
        for i in params:
            self.log_file.write(repr(i))
            self.log_file.write('\n')
        self.log_file.write('\n')
        self.inner_cursor.executemany(sql, params)

    def fetchone(self):
        '''Log a request to return a single result row'''

        result = self.inner_cursor.fetchone()
        self.log_file.write("Fetched a row:\n")
        self.log_file.write(str(result))
        self.log_file.write("\n")
        return result

    def fetchmany(self, size):
        '''Log a request to return many result rows'''

        result = self.inner_cursor.fetchmany(size)
        self.log_file.write("Fetched {} rows\n".format(len(result)))
        return result

    def fetchall(self):
        '''Log a request to return a result'''

        result = self.inner_cursor.fetchall()
        self.log_file.write("Fetched {} rows\n".format(len(result)))
        return result

    def copy_from(self, file, table, sep='\t', null='\\N', size=8192, columns=None):
        '''Log a request to execute a COPY command to upload bulk data. This is a
        Postgresql/psycopg-specific command'''

        self.log_file.write("Executed a COPY from file '{}' to table: '{}'"
                            " with params {}\n"
                            .format(file.name,
                                    table,
                                    repr((sep, null, size, columns)))
                           )
        self.inner_cursor.copy_from(self, file, table, sep, null, size, columns)

    def copy_expert(self, sql, file, size=8192):
        '''Log a request to execute a COPY command to upload bulk data. This is a
        Postgresql/psycopg-specific command'''

        self.log_file.write("Executed a COPY from file '{}' using SQL: '{}'"
                            " with size '{}'\n"
                            .format(file.name,
                                    sql,
                                    repr(size))
                           )
        self.inner_cursor.copy_expert(sql, file, size)

    def close(self):
        '''Close the dummy database cursor object. Does not close the associated output file.'''

        self.inner_cursor.close()


class Connection:
    '''A database connection facade that implements a subset of DB-API methods and outputs
    information on the requests to a file or stdout.'''

    def __init__(self, inner_connection, log_file=sys.stdout):
        self.inner_connection = inner_connection
        self.log_file = log_file
        self.log_file.write("***New Log Started***\n\n")
        self._autocommit = False

    def set_autocommit(self, value):
        '''Log an attempt to change the autocommit mode of the mock database connection object'''

        self._autocommit = value
        self.log_file.write("Set autocommit status to: {}\n".format(value))
        self.log_file.flush()
        self.inner_connection.set_autocommit(value)

    autocommit = property(fset=set_autocommit)

    def cursor(self):
        '''Create a dummy cursor which uses the same output file.'''

        return Cursor(self.inner_connection.cursor(), self.log_file)

    def commit(self):
        '''Log a request to commit a transaction.'''

        self.log_file.write("Committed transaction\n")
        self.log_file.flush()
        self.inner_connection.commit()

    def close(self):
        '''Close the database facade. Also closes the file associated with the object unless that
        is sys.stdout.'''

        self.log_file.write("Closing connection\n\n")
        self.log_file.flush()
        if self.log_file != sys.stdout:
            self.log_file.close()
        self.inner_connection.close()

    def execute(self, sql, params=None):
        '''Log a request to execute some SQL with given parameters'''

        self.log_file.write("Executed SQL: '{}' with params '{}'\n"
                            .format(sql, repr(params)))
        if params:
            self.inner_connection.execute(sql, params)
        else:
            self.inner_connection.execute(sql)
