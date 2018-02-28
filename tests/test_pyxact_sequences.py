# Test pyxact.sequences

import pytest
import pyxact.sequences as sequences
from pyxact.dialects import sqliteDialect

@pytest.fixture()
def simple_seq(sqlitecur):
    simple_seq = sequences.SQLSequence(name='simple_seq')
    simple_seq.create(sqlitecur, sqliteDialect)
    return simple_seq

@pytest.fixture()
def jump_seq(sqlitecur):
    jump_seq = sequences.SQLSequence(name='jump_seq', start=2, interval=3,
                                     index_type='SMALLINT', sql_name='jump_seq_sql_name')
    for sql_command in jump_seq.create_sequence_sql(sqliteDialect):
        sqlitecur.execute(sql_command)
    return jump_seq

def test_creation(sqlitecur, simple_seq, jump_seq):
    assert simple_seq.name == 'simple_seq'
    assert simple_seq.sql_name == 'simple_seq'
    assert simple_seq.index_type == 'BIGINT'

    assert jump_seq.name == 'jump_seq'
    assert jump_seq.sql_name == 'jump_seq_sql_name'
    assert jump_seq.index_type == 'SMALLINT'

def test_nextval_reset(sqlitecur, simple_seq, jump_seq):
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 1
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 2
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 3
    simple_seq.reset(sqlitecur, sqliteDialect)
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 1
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 2
    assert simple_seq.nextval(sqlitecur, sqliteDialect) == 3

    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 2
    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 5
    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 8
    jump_seq.reset(sqlitecur, sqliteDialect)
    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 2
    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 5
    assert jump_seq.nextval(sqlitecur, sqliteDialect) == 8
