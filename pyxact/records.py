

from . import fields

class SQLRecordMetaClass(type):

    # Note - needs Python 3.6+ in order for the namespace dict to be ordered by
    # default

    def __new__(mcs, name, bases, namespace, table_name=None, **kwds):

        slots = []
        _fields = dict()

        for k in namespace:
            if isinstance(namespace[k], fields.SQLField):
                slots.append('_'+k)
                _fields[k] = namespace[k]

        namespace['__slots__'] = tuple(slots)
        namespace['_fields'] = _fields
        namespace['_field_count'] = len(slots)
        namespace['_table_name'] = table_name

        return type.__new__(mcs, name, bases, namespace)

class SQLRecord(metaclass=SQLRecordMetaClass):

    def __init__(self, *args, **kwargs):

        for i in self.__slots__:
            setattr(self, i, None)

        if args:
            if len(args) != self._field_count:
                raise ValueError('{0} values required, {1} supplied.'
                                 .format(self._field_count, len(args)))

            for field, value in zip(self._fields.keys(), args):
                setattr(self, field, value)

        elif kwargs:
            for key, value in kwargs.items():
                if key not in self._fields:
                    raise ValueError('{0} is not a valid attribute name.'.format(key))
                setattr(self, key, value)

    def copy(self):
        result = self.__class__()
        for v in self.__slots__:
            setattr(result, v, getattr(self, v))
        return result

    def get(self, key):
        if key not in self._fields:
            raise ValueError('{0} is not a valid field name.'.format(key))
        return getattr(self, key)

    def set(self, key, value):
        if key not in self._fields:
            raise ValueError('{0} is not a valid field name.'.format(key))
        setattr(self, key, value)

    @property
    def table_name(self):
        return self._table_name

    @classmethod
    def fields(cls):
        return [cls._fields[k] for k in cls._fields.keys()]

    def values(self, context=None):
        return [self._fields[k].get_context(self, context) for k in self._fields.keys()]

    def values_sql_string_unsafe(self, context, dialect=None):
        result = []
        for k in self._fields.keys():
            value = self._fields[k].get_context(self, context)
            result.append(self._fields[k].sql_string_unsafe(value, dialect))
        return result

    @classmethod
    def items(cls):
        return [(k, cls._fields[k]) for k in cls._fields.keys()]

    def item_values(self):
        return [(k, self.get(k)) for k in self._fields.keys()]

    @classmethod
    def column_names_sql(cls, dialect=None):
        return ', '.join([cls._fields[x].sql_name for x in cls._fields.keys()])

    @classmethod
    def create_table_sql(cls, dialect=None):
        result = 'CREATE TABLE IF NOT EXISTS ' + cls._table_name + ' (\n    '
        columns = [cls._fields[k]._sql_name + ' ' + cls._fields[k].sql_type()
                   for k in cls._fields.keys()]
        result += ',\n    '.join(columns)
        result += '\n);'
        return result

    @classmethod
    def insert_sql(cls, context=None, dialect=None):
        if not context:
            context = {}
        if dialect:
            placeholder = dialect.placeholder
        else:
            placeholder = '?'
        result = 'INSERT INTO ' + cls._table_name + ' ('
        result += cls.column_names_sql(dialect)
        result += ') VALUES ('
        if cls._field_count > 0:
            result += (placeholder+', ')*(cls._field_count-1)+placeholder
        result += ');'
        return result

    def insert_sql_unsafe(self, context=None, dialect=None):
        if not context:
            context = {}
        result = 'INSERT INTO ' + self._table_name + ' ('
        result += self.column_names_sql(dialect)
        result += ') VALUES ('
        result += ', '.join(self.values_sql_string_unsafe(context, dialect))
        result += ');'
        return result

    @classmethod
    def simple_select_sql(cls, dialect=None, **kwargs):
        if dialect:
            placeholder = dialect.placeholder
        else:
            placeholder = '?'
        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result += ' WHERE '
            c = 1
            for f, v in kwargs.items():
                if not f in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(f))
                result += cls._fields[f].sql_name+'='
                result += placeholder
                if c < len(kwargs):
                    result += ' AND '
                c += 1
        result += ';'
        return (result, list(kwargs.values()))

    @classmethod
    def simple_select_sql_unsafe(cls, dialect=None, **kwargs):
        result = 'SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result += ' WHERE '
            c = 1
            for f, v in kwargs.items():
                if not f in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(f))
                result += cls._fields[f].sql_name+'='
                result += cls._fields[f].sql_string_unsafe(v)
                if c < len(kwargs):
                    result += ' AND '
                c += 1
        result += ';'
        return result

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self._fields.keys():
            result += k + ' : ' + str(self._fields[k]) + ', \n'
        result += '}'
        return result
