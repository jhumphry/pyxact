'''pyxact is a package that provides a Python model of SQL tables, transactions
and queries. It is designed for those who have a clear idea of the  database
schema they wish to use, but who want to make it easier to keep the Python and
SQL DDL code in sync and to avoid lots of duplicate boilerplate code. Schema can
be modelled in Python and the SQL generated from the models.

pyxact uses metaclasses, descriptors and __slots__ extensively to create
classes that have type-enforced attributes and have a fixed list of attributes
frozen at the point of class definition. Python 3.6+ is currently required for
correct operation.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from enum import Enum

class IsolationLevel(Enum):
    '''This enumeration is used to specify the isolation level used for database transactions.
    MANUAL_TRANSACTIONS implies that no automatic management of transactions is desired. The other
    values indicate that the isolation guarantees should be at least equivalent to the relevant
    guarantee in the SQL standard.'''

    MANUAL_TRANSACTIONS = -1
    READ_UNCOMMITTED = 1
    READ_COMMITTED = 2
    REPEATABLE_READ = 3
    SERIALIZABLE = 4

class PyxactError(Exception):
    '''Base class for exceptions defined by pyxact.'''

    pass

class VerificationError(PyxactError):
    '''Transaction failed internal consistency checks at a point when it was
    required to pass.'''

    pass

class UnconstrainedWhereError(PyxactError):
    '''The WHERE clause of an SQL statement would not impose any constraints.'''

    pass

class ContextRequiredError(PyxactError):
    '''An SQLField requires a context dictionary to be passed in order to
    retrieve values.'''

    pass

class SQLSchemaBase:
    '''This serves as a base class for pyxact.schemas.SQLSchema. It is used in
    the modules of pyxact to check if an SQLSchema has been passed in as a
    parameter, without needing to import the 'schemas' module itself, which
    could create circular dependencies. It is not intended for end-user use.'''

    pass
