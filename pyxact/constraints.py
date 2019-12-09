'''This module defines classes that represent tables constraints in SQL.'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from . import SQLSchemaBase, FKMatch, FKAction, ConstraintDeferrable, dialects

class SQLConstraint:
    '''This abstract base class is the root of the class hierarchy for table
    constraints.'''

    def __init__(self, sql_name=None):
        self.sql_name = sql_name
        self.name = None

    def __set_name__(self, owner, name):
        self.name = name
        if self.sql_name is None:
            self.sql_name = name

    def sql_ddl(self):
        '''This method returns a string containing suitable SQL DDL
        instructions (in the specified database dialect) that can be inserted
        into a CREATE or ALTER TABLE command to apply the required table
        constraint.'''

        raise NotImplementedError

class CustomConstraint(SQLConstraint):
    '''Due to the very varied capabilities of table constraints in different
    databases, and the varied syntax for how they are to be specified, it may
    not always be possible to use a specific SQLConstraint subclass to achieve
    the desired effect. The CustomConstraint class takes a string of pre-formed
    SQL to form the text of the constraint. The class user is responsible for
    ensuring portability between databases, if desired.'''

    def __init__(self, constraint_sql, **kwargs):
        super().__init__(**kwargs)
        self.constraint_sql = constraint_sql

    def sql_ddl(self):
        return 'CONSTRAINT '+self.sql_name+' '+self.constraint_sql

class CheckConstraint(SQLConstraint):
    '''Check constraints take an SQL expression and ensure that holds for every
    row in the table. The class user is responsible for ensuring portability
    between databases, if desired.'''

    def __init__(self, check_sql, **kwargs):
        super().__init__(**kwargs)
        self.check_sql = check_sql

    def sql_ddl(self):
        return 'CONSTRAINT '+self.sql_name+' CHECK ('+self.check_sql+')'

class ColumnsConstraint(SQLConstraint):
    '''ColumnsConstraint is an abstract subclass of SQLConstraint which factors
    out the commonalities between UNIQUE constraints and PRIMARY KEY
    constraints.'''

    def __init__(self, column_names, sql_options='', **kwargs):
        super().__init__(**kwargs)
        if isinstance(column_names, str):
            self.column_names = (column_names,)
        else:
            self.column_names = column_names
        self.sql_column_names = None # Will be filled out by SQLRecordMetaClass
        self.sql_options = sql_options

    def sql_ddl(self):
        raise NotImplementedError

class UniqueConstraint(ColumnsConstraint):
    '''This class is used to create UNIQUE constraints. Depending on the
    database, this make automatically create an index covering the columns.'''

    def sql_ddl(self):
        result = 'CONSTRAINT' + self.sql_name + ' UNIQUE ('
        result += ', '.join(self.sql_column_names)
        result += ') ' + self.sql_options
        return result

class PrimaryKeyConstraint(ColumnsConstraint):
    '''This class is used to create PRIMARY KEY constraints. Depending on the
    database, this make automatically create an index covering the columns.'''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def sql_ddl(self):
        result = 'CONSTRAINT ' + self.sql_name + ' PRIMARY KEY ('
        result += ', '.join(self.sql_column_names)
        result += ') ' + self.sql_options
        return result

class ForeignKeyConstraint(ColumnsConstraint):
    '''This class is used to create FOREIGN KEY constraints. Depending on the
    database, this make require the referenced columns to be indexed. If
    sql_reference_names is None then it is assumed the referenced columns have
    the same names as the columns in the current table.'''

    def __init__(self, foreign_table, foreign_schema=None, sql_reference_names=None,
                 match=FKMatch.SIMPLE, on_update=FKAction.NO_ACTION, on_delete=FKAction.NO_ACTION,
                 deferrable=ConstraintDeferrable.DEFERRABLE_INITIALLY_DEFERRED, **kwargs):

        super().__init__(**kwargs)

        self.foreign_table = foreign_table
        self.match = match

        # Note that by default operations that affect FK constraints in other tables will not be
        # permitted (as is usual in SQL) but that FK constraints will be deferred by default to the
        # end of the transaction. This allows SQLTransaction to delete all rows in all tables
        # associated with the transaction without having to first do a topological sort to work out
        # what ordering of deletions will avoid spurious FK constraint errors.
        self.on_update = on_update
        self.on_delete = on_delete
        self.deferrable = deferrable

        # Note that we cannot type-check the foreign_schema to avoid circular dependencies
        if foreign_schema is None:
            self.foreign_schema = None
        elif isinstance(foreign_schema, SQLSchemaBase):
            self.foreign_schema = foreign_schema
        else:
            raise TypeError('foreign_schema must be an instance of pyxact.schemas.SQLSchema')

        if isinstance(sql_reference_names, str):
            self.sql_reference_names = (sql_reference_names,)
        else:
            self.sql_reference_names = sql_reference_names
        # If sql_reference_names is None it will be over-ridden by the sql_column_names
        # in SQLRecordMetaClass

    def sql_ddl(self):

        dialect = dialects.DefaultDialect

        if self.foreign_schema is None:
            foreign_table = self.foreign_table
        else:
            foreign_table = self.foreign_schema.qualified_name(self.foreign_table)

        result = 'CONSTRAINT ' + self.sql_name + ' FOREIGN KEY ('
        result += ', '.join(self.sql_column_names)
        result += ') REFERENCES ' + foreign_table + ' ('
        result += ', '.join(self.sql_reference_names)
        result += ') '
        result += dialect.foreign_key_match_sql[self.match] + ' '
        result += 'ON DELETE ' + dialect.foreign_key_action_sql[self.on_delete] + ' '
        result += 'ON UPDATE ' + dialect.foreign_key_action_sql[self.on_update] + ' '
        result += dialect.constraint_deferrable_sql[self.deferrable] + ' '
        result += self.sql_options
        return result
