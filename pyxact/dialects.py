'''This module defines classes that are used as singleton objects to define the
various flavours of SQL used by different database adaptors.'''

class SQLDialect:
    '''This is an abstract base class from which concrete dialect classes
    should be derived.'''

    pass

class sqliteDialect(SQLDialect):
    '''This class contains information used internally to generate suitable SQL
    for use with the standard library interface to SQLite3, the embedded
    database engine that usually comes supplied with Python.'''

    placeholder = '?' # The placeholder to use for parametised queries
    native_decimals = False # Whether the adaptor supports decimals natively
                            # otherwise they must be converted to strings
    native_booleans = False # Whether the adaptor supports booleans, otherwise
                            # they must be converted to 0/1 integers

    create_sequence_sql = ('''
CREATE TABLE IF NOT EXISTS {name}    (start {index_type},
                                      interval {index_type},
                                      lastval {index_type},
                                      nextval {index_type});''',
                           '''INSERT INTO {name} VALUES ({start},{interval},{start},{start});''')
    nextval_sequence_sql = ('''UPDATE {name} SET lastval=nextval, nextval=nextval+interval;''',
                            '''SELECT lastval FROM {name};''')
    reset_sequence_sql = ('''UPDATE {name} SET lastval=start, nextval=start;''',)
