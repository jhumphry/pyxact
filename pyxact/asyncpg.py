'''This module defines the flavour of SQL used by PostgreSQL and asyncpg.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import enum

from . import dialects
from . import IsolationLevel
from . import psycopg2

class AsyncpgDialect(psycopg2.Psycopg2Dialect):
    '''This is a singleton class that defines the variant of SQL supported by PostgreSQL and the
    asyncpg database adaptor.'''

    @classmethod
    def parameter(cls, number=1, start=1):
        result = ''
        for i in range(start, start+number-1):
            result += '$' + str(i) + ', '
        return result + '$'+ str(start+number-1)

    @classmethod
    def parameter_values(cls, names: list, start=1, concat=','):
        result = ''
        i = start
        for name in names[:-1]:
            result += name + '=$' + str(i) + ' ' + concat + ' '
            i += 1
        return result + names[-1]+'=$' + str(i)
