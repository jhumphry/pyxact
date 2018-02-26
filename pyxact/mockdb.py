''' mockdb.py - a mock DB API connection and cursor definition'''

import sys


class Cursor:
    '''A dummy database cursor object that implements a subset of DB-API
    methods and outputs the requests to a file or stdout.'''

    def __init__(self, log_file=sys.stdout):
        self.log_file = log_file

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False  # Don't suppress exceptions

    def execute(self, sql, params=None):
        '''Log a request to execute some SQL with given parameters'''

        self.log_file.write("Executed SQL: '{}' with params '{}'\n"
                            .format(sql, repr(params)))

    def executemany(self, sql, params=None):
        '''Log a request to execute some SQL with multiple sets of parameters'''

        self.log_file.write("Executed SQL: '{}' with params:\n"
                            .format(sql))
        for i in params:
            self.log_file.write(repr(i))

    def copy_from(self, file, table, sep='\t',
                  null='\\N', size=8192, columns=None):
        '''Log a request to execute a COPY command to upload bulk data. This
        is a Postgresql-specific command'''

        self.log_file.write("Executed a COPY from file '{}' to table: '{}'"
                            " with params {}\n"
                            .format(file.name,
                                    table,
                                    repr((sep, null, size, columns)))
                            )

    def copy_expert(self, sql, file, size=8192):
        '''Log a request to execute a COPY command to upload bulk data. This
        is a Postgresql-specific command'''

        self.log_file.write("Executed a COPY from file '{}' using SQL: '{}'"
                            " with size '{}'\n"
                            .format(file.name,
                                    sql,
                                    repr(size))
                            )

    def close(self):
        '''Close the dummy database cursor object. Does not close the
        associated output file.'''

        pass


class Connection:
    '''A dummy database object that implements a subset of DB-API methods and
    outputs the requests to a file or stdout.'''

    def __init__(self, log_file=sys.stdout):
        self.log_file = log_file
        self._autocommit = False

    def set_autocommit(self, value):
        '''Log an attempt to change the autocommit mode of the mock database
        connection object'''

        self._autocommit = value
        self.log_file.write("Set autocommit status to: {}\n".format(value))
        self.log_file.flush()

    autocommit = property(fset=set_autocommit)

    def cursor(self):
        '''Create a dummy cursor which uses the same output file.'''

        return Cursor(self.log_file)

    def commit(self):
        '''Log a request to commit a transaction.'''

        self.log_file.write("Committed transaction\n")
        self.log_file.flush()

    def close(self):
        '''Close the dummy database object. Closes the file associated with
        the object unless that is sys.stdout.'''

        self.log_file.write("Closing connection\n")
        self.log_file.flush()
        if self.log_file != sys.stdout:
            self.log_file.close()
