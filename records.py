

import fields

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
        namespace['_column_names'] = None # Not yet known

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
    def fields(self):
        for k in self._fields.keys():
            yield self._fields[k]

    def values(self):
        for k in self._fields.keys():
            yield self.get(k)

    @classmethod
    def items(self):
        for k in self._fields.keys():
            yield k, self._fields[k]

    def item_values(self):
        for k in self._fields.keys():
            yield k, self.get(k)

    @classmethod
    def column_names_sql(cls, dialect=None):
        if cls._column_names:
            return cls._column_names
        else:
            if cls._field_count==0:
                pass
            else:
                result=''
                c=1
                for k in cls._fields.keys():
                    result += cls._fields[k]._sql_name
                    if c < cls._field_count:
                        result += ', '
                    c+=1
            cls._column_names = result
            return result

    def values_sql(self, context, dialect=None):
        result='('
        if self._field_count==0:
            pass
        else:
            c=1
            for k in self._fields.keys():
                value = self._fields[k].get_context(self, context)
                result += self._fields[k].sql_string(value, dialect)
                if c < self._field_count:
                    result += ', '
                c+=1
        result+=')'
        return result

    @classmethod
    def create_table_sql(cls, dialect=None):
        result='CREATE TABLE IF NOT EXISTS ' + cls._table_name + ' (\n    '
        if cls._field_count==0:
            pass
        else:
            c=1
            for k in cls._fields.keys():
                result += cls._fields[k]._sql_name + ' ' + \
                          cls._fields[k].sql_type()
                if c == cls._field_count:
                    result += '\n'
                else:
                    result += ',\n    '
                c+=1
        result += ');'
        return result

    def insert_sql(self, context=None, dialect=None):
        if context==None:
            context={}
        result='INSERT INTO ' + self._table_name + ' ('
        result+=self.column_names_sql(dialect)
        result+=') VALUES '
        result+=self.values_sql(context, dialect)
        result+=';'
        return result

    @classmethod
    def simple_select_sql(cls, **kwargs):
        result='SELECT ' + cls.column_names_sql() + ' FROM ' + cls._table_name
        if kwargs:
            result+=' WHERE '
            c=1
            for f,v in kwargs.items():
                if not f in cls._fields:
                    raise ValueError('Specified field {0} is not valid'.format(f))
                result+=cls._fields[f].sql_name+'='
                result+=cls._fields[f].sql_string(v)
                if c<len(kwargs):
                    result+=' AND '
                c+=1
        result+=';'
        return result

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self._fields.keys():
            result += k + ' : ' + str(self._fields[k]) + ', \n'
        result += '}'
        return result
