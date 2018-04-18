'''This module defines types that represent indexes in a database.'''

from collections import namedtuple

from . import SQLSchemaBase
from . import dialects, tables

IndexColumn = namedtuple('Column', ['column', 'collation', 'direction'])
IndexExpr = namedtuple('Expr', ['expr'])

class SQLIndex:
    '''SLQIndex is a representation of a database index'''

    def __init__(self, name, table, column_exprs, unique=False,
                 where_clause=None, sql_name=None, schema=None):
        self.name = name

        self.unique = unique

        if not isinstance(table, type) or not issubclass(table, tables.SQLTable):
            raise TypeError('Must provide an SQLTable to index')
        self.table = table

        if len(column_exprs) < 1:
            raise ValueError('At least one column or expression must be specified')

        self.column_exprs = []

        for i in column_exprs:
            if not isinstance(i, (IndexColumn, IndexExpr)):
                raise TypeError('Only IndexColumn or IndexExpr objects can be used with SQLIndex')

            if isinstance(i, IndexColumn) and i.column not in table._fields:
                raise ValueError('Column {} is not in {}'.format(i.column, table.__name__))

            self.column_exprs.append(i)

        self.where_clause = where_clause

        if sql_name:
            self.sql_name = sql_name
        else:
            self.sql_name = name

        if schema is None:
            self.schema = None
        elif isinstance(schema, SQLSchemaBase):
            self.schema = schema
            schema.register_index(self)
        else:
            raise TypeError('schema must be an instance of pyxact.schemas.SQLSchema')

    def qualified_name(self, dialect=None):
        '''The (possibly schema-qualified) name of the index used in SQL.'''

        if self.schema is None:
            return self.sql_name

        return self.schema.qualified_name(self.sql_name, dialect)

    def create(self, cursor, dialect=None):
        '''This function takes a DB-API 2.0 cursor and runs the necessary code
        to create the index in the database if it does not already exist.
        The dialect parameter allows the function to identify the correct SQL
        commands to issue.'''

        if not dialect:
            dialect = dialects.DefaultDialect

        result = 'CREATE '
        if self.unique:
            result += 'UNIQUE '
        result += 'INDEX IF NOT EXISTS '
        result += self.qualified_name(dialect)
        result += ' ON ' + self.table._qualified_table_name(dialect)

        column_exprs_clauses = []
        for i in self.column_exprs:
            if isinstance(i, IndexColumn):
                tmp = self.table._fields[i.column].sql_name
                if i.collation:
                    tmp += ' COLLATION ' + i.collation
                if i.direction:
                    tmp += i.direction + ' '
                column_exprs_clauses.append(tmp)
            else:
                column_exprs_clauses.append(i.expr)

        result += ' (' + ', '.join(column_exprs_clauses) + ')'
        if self.where_clause:
            result += ' WHERE ' + self.where_clause

        cursor.execute(result)
