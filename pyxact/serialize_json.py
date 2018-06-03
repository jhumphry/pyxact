'''This module defines custom JSON encoders/decoders for pyxact types.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import datetime
import decimal
import json

from . import records
from . import recordlists
from . import transactions

class PyxactEncoder(json.JSONEncoder):
    '''This class extends json.JSONEncoder to handle the encoding of pyxact SQLRecord,
    SQLRecordList and SQLTransaction values. Instances of these classes (or their descendants) will
    be turned into JSON objects with the values of the instance stored under the name of the
    appropriate attribute. Additional keys are added to enable the JSON object to be identified as
    a SQLRecord, SQLRecordList or SQLTransaction, and for the relevant subclass to be
    identified.'''

    @classmethod
    def serialize_sqlfield_value(cls, value):
        '''This method turns values into suitable types for the parent JSONEncoder class to encode.
        The serialisation of values stores compatible types as their JSON equivalents. Decimal
        numbers are stored as strings to make interpretation as binary floats more difficult. Dates
        and times are stored as text in ISO format. In both cases, the relevant SQLField know how
        to accept strings and recover the typed value.'''

        if isinstance(value, (bool, int, float, str, bytes)) or value is None:
            return value
        elif isinstance(value, decimal.Decimal):
            return str(value)
        elif isinstance(value, datetime.datetime):
            if value.tzinfo:
                return value.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
            return value.strftime('%Y-%m-%dT%H:%M:%S.%f')
        elif isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        elif isinstance(value, datetime.time):
            return value.strftime('%H:%M:%S.%f')
        raise TypeError('Unable to serialize type {} for JSON'.format(str(type(value))))

    @classmethod
    def serialize_sqlrecord(cls, obj, details=True):
        '''This method turns an SQLRecord subclass into a dictionary that the parent JSONEncoder
        class knows how to turn into a JSON object. Additional attributes are added to identify the
        JSON object as a SQLRecord, and to identify the schema (if any) associated with it.'''

        result = dict()
        if details:
            result['__SQLRecord__'] = obj.__class__.__name__
            if hasattr(obj, '_schema') and obj._schema is not None:
                result['__SQLSchema__'] = obj._schema.name
        for field_name in obj._fields:
            result[field_name] = cls.serialize_sqlfield_value(getattr(obj, field_name))
        return result

    @classmethod
    def serialize_sqlrecordlist(cls, obj):
        '''This method turns an SQLRecordList subclass into a dictionary that the parent
        JSONEncoder class knows how to turn into a JSON object. Additional attributes are added to
        identify the JSON object as a SQLRecordList, to record the underlying SQLRecord type and to
        identify the schema (if any) associated with it. Values in the SQLRecord are stored under
        'values'.'''

        result = dict()
        result['__SQLRecordList__'] = obj.__class__.__name__
        result['__SQLRecord_type__'] = obj._record_type.__name__
        if hasattr(obj._record_type, '_schema'):
            result['__SQLSchema__'] = obj._record_type._schema.name
        result['values'] = [cls.serialize_sqlrecord(x, details=False) for x in obj]
        return result

    @classmethod
    def serialize_sqltransaction(cls, obj):
        '''This method turns an SQLTransaction subclass into a dictionary that the parent
        JSONEncoder class knows how to turn into a JSON object. An additional attribute is added to
        identify the JSON object as a SQLTransaction.'''

        result = dict()
        result['__SQLTransaction__'] = obj.__class__.__name__
        for field_name in obj._context_fields:
            result[field_name] = cls.serialize_sqlfield_value(getattr(obj, field_name))
        for record_name in obj._records:
            result[record_name] = cls.serialize_sqlrecord(getattr(obj, record_name))
        for recordlist_name in obj._recordlists:
            result[recordlist_name] = cls.serialize_sqlrecordlist(getattr(obj, recordlist_name))
        return result

    def default(self, o): # pylint: disable=E0202
        if isinstance(o, records.SQLRecord):
            return self.serialize_sqlrecord(o)
        if isinstance(o, recordlists.SQLRecordList):
            return self.serialize_sqlrecordlist(o)
        if isinstance(o, transactions.SQLTransaction):
            return self.serialize_sqltransaction(o)

        return json.JSONEncoder.default(self, o)

class PyxactDecoder():
    '''This class allows the creation of JSON decoders that can identify pyxact SQLRecord,
    SQLRecordList and SQLTransaction objects and return an instance of the appropriate subclass.
    This relies on the subclasses being registered with the instance before the decode method is
    called. It also supports the registration of SQLSchema, which leads to all of the SQLTable in
    that schema being recognised.'''

    def __init__(self, *args, **kwargs):
        self.sqlschemas = dict()
        self.sqlrecords = dict()
        self.sqlrecordlists = dict()
        self.sqltransactions = dict()
        super().__init__(*args, **kwargs)

    def register_sqlschema(self, schema):
        '''Register an SQLSchema instance.'''

        self.sqlschemas[schema.name] = schema

    def register_sqlrecord(self, record):
        '''Register an SQLRecord subclass (including SQLTable subclasses).'''

        self.sqlrecords[record.__name__] = record

    def register_sqlrecordlist(self, recordlist):
        '''Register an SQLRecordList subclass.'''

        self.sqlrecordlists[recordlist.__name__] = recordlist

    def register_sqltransaction(self, transaction):
        '''Register an SQLTransaction subclass.'''

        self.sqltransactions[transaction.__name__] = transaction

    def decode(self, string):
        '''Decode a string from JSON. If it looks like an SQLRecord, SQLRecordList or
        SQLTransaction object, return an instance of the appropriate subclass, If the appropriate
        subclass has not been registered an error will be raised.'''

        obj = json.JSONDecoder().decode(string)
        if '__SQLTransaction__' in obj:
            return self.decode_sqltransaction(obj)
        elif '__SQLRecordList__' in obj:
            return self.decode_sqlrecordlist(obj)
        elif '__SQLRecord__' in obj:
            return self.decode_sqlrecord(obj)
        return obj

    def decode_sqlrecord(self, obj):
        '''Take a dict returned from json.JSONDecoder and try to turn it into an SQLRecord subclass
        instance, by checking for the subclass name under the registered SQLSchema or under the
        directly registered SQLRecord.'''

        if '__SQLSchema__' in obj:
            schema = self.sqlschemas[obj.pop('__SQLSchema__')]
            record_type = schema.table_types[obj.pop('__SQLRecord__')]
        else:
            record_type = self.sqlrecords[obj.pop('__SQLRecord__')]

        return record_type(**obj)

    def decode_sqlrecordlist(self, obj):
        '''Take a dict returned from json.JSONDecoder and try to turn it into an SQLRecordList
        subclass instance. Both the SQLRecordList and the underlying SQLRecord type must have been
        registered.'''

        recordlist_type = self.sqlrecordlists[obj.pop('__SQLRecordList__')]

        if '__SQLSchema__' in obj:
            schema = self.sqlschemas[obj.pop('__SQLSchema__')]
            record_type = schema.table_types[obj.pop('__SQLRecord_type__')]
        else:
            record_type = self.sqlrecords[obj.pop('__SQLRecord_type__')]

        result = recordlist_type()
        for value in obj['values']:
            result._append(record_type(**value))
        return result

    def decode_sqltransaction(self, obj):
        '''Take a dict returned from json.JSONDecoder and try to turn it into an SQLTransaction
        subclass instance. All of the underlying SQLRecord and SQLRecordList subclasses must have
        been registered first.'''

        transaction_type = self.sqltransactions[obj.pop('__SQLTransaction__')]

        result = transaction_type()

        for key, value in obj.items():
            if isinstance(value, dict):
                if '__SQLRecordList__' in value:
                    setattr(result, key, self.decode_sqlrecordlist(value))
                elif '__SQLRecord__' in value:
                    setattr(result, key, self.decode_sqlrecord(value))
                else:
                    raise ValueError('Cannot decode unknown type of object in JSON provided')
            else:
                setattr(result, key, value)

        return result
