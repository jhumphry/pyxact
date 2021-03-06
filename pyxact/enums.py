'''This module defines an SQLField subclass that maps enum.Enum types.'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from . import dialects, fields

class EnumField(fields.SQLField):
    '''Abstract type representing an Enum type in SQL. To use, derive a subclass and set the
    enum_type class attribute to the Python enumeration type, sql_type to the name of the equivalent
    SQL type and (if necessary) fallback_sql_type to the name of a suitable integer type for
    databases that don't support enumerations directly.'''

    enum_type = None
    enum_sql = ''
    fallback_sql_type = 'SMALLINT'
    schema = None

    def convert(self, value):
        if isinstance(value, self.enum_type):
            return value
        if isinstance(value, int):
            return self.enum_type(value)
        if isinstance(value, str):
            return self.enum_type[value]
        raise TypeError

    def sql_type(self):

        if dialects.DefaultDialect.enum_support:
            if self.schema:
                return self.schema.qualified_name(self.enum_sql)
            return self.enum_sql
        return self.fallback_sql_type
