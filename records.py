

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

    def fields(self):
        for k in self._fields.keys():
            yield self._fields[k]

    def values(self):
        for k in self._fields.keys():
            yield self.__getattribute__(self._fields[k]._slot_name)

    def items(self):
        for k in self._fields.keys():
            yield k, self._fields[k]

    def item_values(self):
        for k in self._fields.keys():
            yield k, self.__getattribute__(self._fields[k]._slot_name)

    @classmethod
    def column_names_sql(cls, dialect=None):
        if cls._column_names:
            return cls._column_names
        else:
            result='('
            if cls._field_count==0:
                pass
            else:
                c=1
                for k in cls._fields.keys():
                    result += cls._fields[k]._sql_name
                    if c < cls._field_count:
                        result += ', '
                    c+=1
            result+=')'
            cls._column_names = result
            return result

    def values_sql(self, dialect=None):
        result='('
        if self._field_count==0:
            pass
        else:
            c=1
            for k in self._fields.keys():
                value = self.__getattribute__(self._fields[k]._slot_name)
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

    def insert_sql(self, dialect=None):
        result='INSERT INTO ' + self._table_name + ' '
        result+=self.column_names_sql(dialect)
        result+=' VALUES '
        result+=self.values_sql(dialect)
        result+=';'
        return result

    def __str__(self):
        result = self.__class__.__name__ + ' with fields {\n'
        for k in self._fields.keys():
            result += k + ' : ' + str(self._fields[k]) + ', \n'
        result += '}'
        return result
