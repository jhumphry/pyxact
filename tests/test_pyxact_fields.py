# Test pyxact.fields

from decimal import Decimal, Inexact, InvalidOperation

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
        context_int_field=fields.ContextIntField(context_used='int_context')
        sequence_int_field=fields.SequenceIntField(sequence=field_test_seq)
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

    return Holder

@pytest.fixture()
def holder(holder_class):
    print("New holder")
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

    with pytest.raises(ValueError, message='IntField should not have accepted a float in a string value'):
        holder.int_field = '1.2'

    with pytest.raises(ValueError, message='SQLField should not have accepted a None value'):
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

    with pytest.raises(ContextRequiredError, message='ContextIntField.get_Context should complain if required context is missing'):
        holder_class.context_int_field.get_context(holder, null_context)

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

    with pytest.raises(ValueError, message='NumericField should not have accepted a float'):
        holder.numeric_field = 1.25

    with pytest.raises(Inexact, message='NumericField should not have accepted a decimal with inexact quantization'):
        holder.numeric_field = Decimal('1.234')

    with pytest.raises(InvalidOperation, message='NumericField should not have accepted a decimal too large for the quantization/precision'):
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

    with pytest.raises(InvalidOperation, message='NumericField should not have accepted a decimal too large for the quantization/precision'):
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

    with pytest.raises(ValueError,
                        message='VarChar field with silent_truncate=False should reject long string'):
        holder.varchar_field = "Lorem Ipsum"

    holder.varchar_field_truncate = "Lorem Ipsum"
