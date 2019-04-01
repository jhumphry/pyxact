'''Test pyxact.fields'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from decimal import Decimal, Inexact, InvalidOperation
import datetime

import pytest
from pyxact import ContextRequiredError
import pyxact.fields as fields
import pyxact.sequences as sequences
from pyxact.dialects import sqliteDialect

@pytest.fixture()
def field_test_seq(sqlitecur):
    field_test_seq = sequences.SQLSequence(name='field_test_seq')
    field_test_seq.create(sqlitecur, sqliteDialect)
    return field_test_seq

@pytest.fixture()
def holder_class(field_test_seq):

    # This is deliberately not created as an SQLRecord to help decouple the
    # SQLField tests and the SQLRecord tests

    class Holder:
        int_field=fields.IntField(nullable=False)
        int_field_nullable=fields.IntField(nullable=True)
        int_field_sqlname=fields.IntField(sql_name='not_int_field_sqlname')
        smallint_field=fields.SmallIntField()
        bigint_field=fields.BigIntField()
        context_int_field=fields.IntField(context_used='int_context')
        sequence_int_field=sequences.SequenceIntField(sequence=field_test_seq)
        row_enum_int_field=fields.RowEnumIntField(context_used='row_context')
        numeric_field=fields.NumericField(precision=6, scale=2)
        numeric_field_from_floats=fields.NumericField(precision=6, scale=2, allow_floats=True)
        numeric_field_inexact_quantize=fields.NumericField(precision=6, scale=2, inexact_quantize=True)
        real_field=fields.RealField()
        boolean_field=fields.BooleanField()
        text_field=fields.TextField()
        varchar_field=fields.VarCharField(max_length=5)
        varchar_field_truncate=fields.VarCharField(max_length=5, silent_truncate=True)
        char_field=fields.CharField(max_length=3)
        timestamp_field=fields.TimestampField(tz=True)
        timestamp_notz_field=fields.TimestampField(tz=False)
        utcnowtimestamp_field=fields.UTCNowTimestampField()
        time_field=fields.TimeField()
        utcnowtime_field=fields.UTCNowTimeField()
        date_field=fields.DateField()
        todaydate_field=fields.TodayDateField()
        blob_field=fields.BlobField()

    return Holder

@pytest.fixture()
def holder(holder_class):
    return holder_class()

@pytest.fixture()
def context():
    return {'int_context' : 42, 'row_context' : 7}

def test_int(context, holder, holder_class):
    holder.int_field = 2
    assert holder.int_field == 2

    holder.int_field = 3
    assert holder.int_field == 3

    assert holder_class.int_field.get_context(holder, context) == 3

    holder.int_field = '4'
    assert holder.int_field == 4

    # IntField should not have accepted a float in a string value
    with pytest.raises(ValueError):
        holder.int_field = '1.2'

    # SQLField should not have accepted a None value.
    with pytest.raises(TypeError):
        holder.int_field = None

    holder.int_field_nullable = None
    assert holder.int_field_nullable is None

    assert holder_class.int_field.sql_name == 'int_field'
    assert holder_class.int_field_sqlname.sql_name == 'not_int_field_sqlname'

    assert holder_class.int_field.sql_type(sqliteDialect) == 'INTEGER'
    assert holder_class.smallint_field.sql_type(sqliteDialect) == 'SMALLINT'
    assert holder_class.bigint_field.sql_type(sqliteDialect) == 'BIGINT'

def test_contextintfield(context, holder, holder_class):

    # Can be set manually
    holder.context_int_field = 33
    assert holder.context_int_field == 33
    assert holder_class.context_int_field.get(holder) == 33

    # Retrieving from context dictionary provided
    assert holder_class.context_int_field.get_context(holder, context) == 42

    # Retrieving a value from a context dictionary should have over-ridden the
    # value stored manually...
    assert holder.context_int_field == 42

    null_context = {}

    # IntField.get_context should complain if required context is missing
    with pytest.raises(ContextRequiredError):
        holder_class.context_int_field.get_context(holder, null_context)

    silly_context = {'int_context' : 'not_a_number', 'row_context' : 7}

    # IntField.get_context should complain if it is given a context value of the wrong type
    with pytest.raises(ValueError):
        holder_class.context_int_field.get_context(holder, silly_context)

def test_sequenceintfield(holder_class):

    assert isinstance(holder_class.sequence_int_field.sequence, sequences.SQLSequence)

def test_rowenumintfield(context, holder, holder_class):

    holder.row_enum_int_field = 33
    assert holder.row_enum_int_field == 33

    # context fixture is initialised with 7 for row_context so the next
    # row number will be 8

    assert holder_class.row_enum_int_field.get_context(holder, context) == 8
    assert holder.row_enum_int_field == 8

    assert holder_class.row_enum_int_field.get_context(holder, context) == 9
    assert holder_class.row_enum_int_field.get_context(holder, context) == 10
    assert holder.row_enum_int_field == 10

    null_context = {}

    assert holder_class.row_enum_int_field.get_context(holder, null_context) == 1
    assert holder.row_enum_int_field == 1


def test_numericfield(holder):

    holder.numeric_field = Decimal('1.23')
    assert holder.numeric_field == Decimal('1.23')

    # NumericField should not have accepted a float
    with pytest.raises(TypeError):
        holder.numeric_field = 1.25

    # NumericField should not have accepted a decimal with inexact quantization
    with pytest.raises(Inexact):
        holder.numeric_field = Decimal('1.234')

    # NumericField should not have accepted a decimal too large for the quantization/precision
    with pytest.raises(InvalidOperation):
        holder.numeric_field = Decimal('12345')

    holder.numeric_field_from_floats = 1.5
    assert holder.numeric_field_from_floats == 1.5

    holder.numeric_field_from_floats = '2.75'
    assert holder.numeric_field_from_floats == 2.75

    holder.numeric_field_from_floats = 5
    assert holder.numeric_field_from_floats == 5

    holder.numeric_field_inexact_quantize = Decimal('1.23')
    assert holder.numeric_field_inexact_quantize == Decimal('1.23')

    holder.numeric_field_inexact_quantize = Decimal('1.234')
    assert holder.numeric_field_inexact_quantize == Decimal('1.23')

    # NumericField should not have accepted a decimal too large for the quantization/precision
    with pytest.raises(InvalidOperation):
        holder.numeric_field_inexact_quantize = Decimal('12345')

def test_realfield(holder, holder_class):
    # If extending this test func, make sure any floating-point constants used
    # are actually exactly representable in binary floating-point
    # (i.e. not 1.1 or similar)

    holder.real_field = 1.5
    assert holder.real_field == 1.5

    holder.real_field = 3.0
    assert holder.real_field == 3.0

def test_booleanfield(holder, holder_class):
    holder.boolean_field = True
    assert holder.boolean_field

    holder.boolean_field = False
    assert not holder.boolean_field

    holder.boolean_field = 1
    assert holder.boolean_field

def test_textfield(holder):
    holder.text_field = "Test"
    assert holder.text_field == "Test"

    holder.text_field = "Lorem Ipsum"
    assert holder.text_field == "Lorem Ipsum"

def test_char_varcharfield(holder):
    holder.varchar_field = "Test"
    assert holder.varchar_field == "Test"

    holder.char_field = "ABC"
    assert holder.char_field == "ABC"

    # VarChar field with silent_truncate=False should reject long string
    with pytest.raises(ValueError):
        holder.varchar_field = "Lorem Ipsum"

    holder.varchar_field_truncate = "Lorem Ipsum"

def test_datetime(holder, holder_class):
    # timestamp_field=fields.TimestampField(tz=True)
    # timestamp_notz_field=fields.TimestampField(tz=False)
    # utcnowtimestamp_field=fields.UTCNowTimestampField()

    holder_class.utcnowtimestamp_field.update(holder, None, None, None)
    timestamp1 = holder.utcnowtimestamp_field
    holder_class.utcnowtimestamp_field.update(holder, None, None, None)
    timestamp2 = holder.utcnowtimestamp_field
    assert timestamp2 > timestamp1

    holder.timestamp_notz_field = timestamp1
    assert holder.timestamp_notz_field == timestamp1

    # A timestamp field that requires a time zone should have rejected an input value without one.
    with pytest.raises(ValueError):
        holder.timestamp_field = timestamp2

    holder.timestamp_field = '2010-06-30T09:30:52.6541+0100'

    # A timestamp field that requires a time zone should have rejected an input value without one.
    with pytest.raises(ValueError):
        holder.timestamp_field = '2010-06-30T09:30:52.5434'


    holder.timestamp_notz_field = '2010-06-30T09:30:52.5434'

    # A timestamp field that does not require a time zone should have rejected an input value with
    # one.
    with pytest.raises(ValueError):
        holder.timestamp_notz_field = '2010-06-30T09:30:52.5434+0530'


def test_date_time(holder, holder_class):
    # time_field=fields.TimeField()
    # utcnowtime_field=fields.UTCNowTimeField()
    # date_field=fields.DateField()
    # todaydate_field=fields.TodayDateField()

    holder_class.utcnowtime_field.update(holder, None, None, None)
    time1 = holder.utcnowtime_field
    holder.time_field = time1
    holder_class.todaydate_field.update(holder, None, None, None)
    date1 = holder.todaydate_field
    holder.date_field = date1

    holder_class.utcnowtime_field.update(holder, None, None, None)
    time2 = holder.utcnowtime_field
    holder_class.todaydate_field.update(holder, None, None, None)
    date2 = holder.todaydate_field

    # Don't forget people running the test over midnight!
    # (we are ignoring people for whom the test takes more than 24 hours...
    assert (date2 == date1 and time2 > time1) or (date2 > date1 and time2 < time1)

    holder.time_field = '13:45:54.123456'

    # TimeField should reject invalid time strings.
    with pytest.raises(ValueError):
        holder.time_field = 'Hello, World!'

    # TimeField should reject impossible times
    with pytest.raises(ValueError):
        holder.time_field = '25:45:54.12344'

    holder.date_field = '1956-08-14'

    # DateField should reject invalid date strings.
    with pytest.raises(ValueError):
        holder.date_field = 'Hello, World!'

    # DateField should reject impossible dates.
    with pytest.raises(ValueError):
        holder.date_field = '1999-06-31'

def test_blobfield(holder):
    holder.blob_field = b'Test\0Binary\0'
    assert holder.blob_field == b'Test\0Binary\0'

    holder.blob_field = b'\0Field!'
    assert holder.blob_field == b'\0Field!'

