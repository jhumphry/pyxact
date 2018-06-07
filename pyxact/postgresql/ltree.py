'''This module defines Python objects that map to PostgreSQL ltree etc types. The ltree type
represents labels of data stored in a hierarchical tree-like structure, and the lquery type allows
various sorts of queries. These types are found in one of the standard contrib modules in
PostgreSQL, so must first be enabled with 'CREATE EXTENSION ltree'. The Python types do not attempt
to implement all the behaviour of the PostgreSQL type.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import re

from .. import fields

VALID_LTREE_REGEXP = re.compile(r'[\w]+(\.[\w]+)*', re.UNICODE)

class LTree(str):
    '''LTree represents labels of data stored in a hierarchical tree-like structure.'''

    def __init__(self, value=''):

        if not isinstance(value, str):
            raise TypeError('LTree can only be initialised from strings or string-like objects')

        if not isinstance(value, LTree) and not VALID_LTREE_REGEXP.fullmatch(value):
            raise ValueError('Not a valid LTree string')

        str.__init__(value)

    def path(self):
        '''Return the labels that make up the path'''

        return self.split('.')

    def __add__(self, value):
        '''Concatenate two ltree (or one ltree and a string value that meets the requirements to be
        an ltree'''

        if isinstance(value, LTree) or VALID_LTREE_REGEXP.fullmatch(value):
            return LTree(str(self)+'.'+str(value))
        raise ValueError('Only other LTree or suitable string values can be added to an LTree')

class LTreeField(fields.SQLField):
    '''Represents a ltree field in a PostgreSQL database. By setting 'store_as_text' to True, it is
    possible to store the values in other databases, but obviously it will not be possible to use
    the relevant operators in queries.'''

    def __init__(self, store_as_text=False, **kwargs):

        if store_as_text:
            super().__init__(py_type=None, sql_type='TEXT', **kwargs)
        super().__init__(py_type=None, sql_type='LTREE', **kwargs)

    def convert(self, value):
        if isinstance(value, LTree):
            return value
        elif isinstance(value, str):
            return LTree(value)
        else:
            raise TypeError
