'''This module defines classes that represent tables constraints in SQL.'''


class SQLConstraint:
    '''This abstract base class is the root of the class hierarchy for table
    constraints.'''

    def __init__(self, sql_name=None):
        self._sql_name = sql_name
        self._name = None

    def __set_name__(self, owner, name):
        self._name = name
        if self._sql_name is None:
            self._sql_name = name

    def sql_ddl(self, dialect=None):
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
        self._constraint_sql = constraint_sql

    def sql_ddl(self, dialect=None):
        return 'CONSTRAINT '+self._sql_name+' '+self._constraint_sql

class CheckConstraint(SQLConstraint):
    '''Check constraints take an SQL expression and ensure that holds for every
    row in the table. The class user is responsible for ensuring portability
    between databases, if desired.'''

    def __init__(self, check_sql, **kwargs):
        super().__init__(**kwargs)
        self._check_sql = check_sql

    def sql_ddl(self, dialect=None):
        return 'CONSTRAINT '+self._sql_name+' CHECK ('+self._check_sql+')'

class ColumnsConstraint(SQLConstraint):
    '''ColumnsConstraint is an abstract subclass of SQLConstraint which factors
    out the commonalities between UNIQUE constraints and PRIMARY KEY
    constraints.'''

    def __init__(self, sql_column_names, sql_options='', **kwargs):
        super().__init__(**kwargs)
        if isinstance(sql_column_names, str):
            self._sql_column_names = (sql_column_names,)
        else:
            self._sql_column_names = sql_column_names
        self._sql_options = sql_options

class UniqueConstraint(ColumnsConstraint):
    '''This class is used to create UNIQUE constraints. Depending on the
    database, this make automatically create an index covering the columns.'''

    def sql_ddl(self, dialect=None):
        result = 'CONSTRAINT' + self._sql_name + ' UNIQUE ('
        result += ', '.join(self._sql_column_names)
        result += ') ' + self._sql_options
        return result

class PrimaryKeyConstraint(ColumnsConstraint):
    '''This class is used to create PRIMARY KEY constraints. Depending on the
    database, this make automatically create an index covering the columns.'''

    def sql_ddl(self, dialect=None):
        result = 'CONSTRAINT ' + self._sql_name + ' PRIMARY KEY ('
        result += ', '.join(self._sql_column_names)
        result += ') ' + self._sql_options
        return result

class ForeignKeyConstraint(SQLConstraint):
    '''This class is used to create FOREIGN KEY constraints. Depending on the
    database, this make require the referenced columns to be indexed.'''

    def __init__(self, sql_column_names, foreign_table, sql_reference_names,
                 sql_options='', **kwargs):
        super().__init__(**kwargs)
        if isinstance(sql_column_names, str):
            self._sql_column_names = (sql_column_names,)
        else:
            self._sql_column_names = sql_column_names
        self._foreign_table = foreign_table
        if isinstance(sql_reference_names, str):
            self._sql_reference_names = (sql_reference_names,)
        else:
            self._sql_reference_names = sql_reference_names
        self._sql_options = sql_options

    def sql_ddl(self, dialect=None):
        result = 'CONSTRAINT ' + self._sql_name + ' FOREIGN KEY ('
        result += ', '.join(self._sql_column_names)
        result += ') REFERENCES '+self._foreign_table + ' ('
        result += ', '.join(self._sql_reference_names)
        result += ') ' + self._sql_options
        return result
