


class SQLConstraint:

    def __init__(self, sql_name=None):
        self._sql_name = sql_name
        self._name = None

    def __set_name__(self, owner, name):
        self._name = name
        if self._sql_name is None:
            self._sql_name = name

    def sql_ddl(self, dialect=None):
        raise NotImplementedError

class CustomConstraint(SQLConstraint):

     def __init__(self, constraint_sql, **kwargs):
        super().__init__(**kwargs)
        self._constraint_sql = constraint_sql

     def sql_ddl(self, dialect=None):
         return 'CONSTRAINT '+self._sql_name+' '+self._constraint_sql+' '

class ColumnsConstraint(SQLConstraint):

    def __init__(self, sql_column_names, sql_options='', **kwargs):
        super().__init__(**kwargs)
        self._sql_column_names = sql_column_names
        self._sql_options = sql_options

class UniqueConstraint(ColumnsConstraint):

     def sql_ddl(self, dialect=None):
         result = 'CONSTRAINT' + self._sql_name + ' UNIQUE ('
         result += ', '.join(self._sql_column_names)
         result += ') ' + self.sql_options
         return result

class PrimaryKeyConstraint(ColumnsConstraint):

     def sql_ddl(self, dialect=None):
         result = 'CONSTRAINT ' + self._sql_name + ' PRIMARY KEY ('
         result += ', '.join(self._sql_column_names)
         result += ') ' + self._sql_options
         return result
