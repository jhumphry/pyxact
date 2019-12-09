'''Test pyxact.sequences'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.sequences as sequences

@pytest.fixture()
def simple_seq(sqlitecur):
    simple_seq = sequences.SQLSequence(name='simple_seq')
    simple_seq.create(sqlitecur)
    return simple_seq

@pytest.fixture()
def jump_seq(sqlitecur):
    jump_seq = sequences.SQLSequence(name='jump_seq', start=2, interval=3,
                                     index_type='SMALLINT', sql_name='jump_seq_sql_name')
    for sql_command in jump_seq.create_sequence_sql():
        sqlitecur.execute(sql_command)
    return jump_seq

def test_creation(sqlitecur, simple_seq, jump_seq):
    assert simple_seq.name == 'simple_seq'
    assert simple_seq.start == 1
    assert simple_seq.interval == 1
    assert simple_seq.sql_name == 'simple_seq'
    assert simple_seq.index_type == 'BIGINT'

    assert jump_seq.name == 'jump_seq'
    assert jump_seq.start == 2
    assert jump_seq.interval == 3
    assert jump_seq.sql_name == 'jump_seq_sql_name'
    assert jump_seq.index_type == 'SMALLINT'

def test_nextval_reset(sqlitecur, simple_seq, jump_seq):
    assert simple_seq.nextval(sqlitecur) == 1
    assert simple_seq.nextval(sqlitecur) == 2
    assert simple_seq.nextval(sqlitecur) == 3
    simple_seq.reset(sqlitecur)
    assert simple_seq.nextval(sqlitecur) == 1
    assert simple_seq.nextval(sqlitecur) == 2
    assert simple_seq.nextval(sqlitecur) == 3

    assert jump_seq.nextval(sqlitecur) == 2
    assert jump_seq.nextval(sqlitecur) == 5
    assert jump_seq.nextval(sqlitecur) == 8
    jump_seq.reset(sqlitecur)
    assert jump_seq.nextval(sqlitecur) == 2
    assert jump_seq.nextval(sqlitecur) == 5
    assert jump_seq.nextval(sqlitecur) == 8

def test_valid(simple_seq, jump_seq):

    assert simple_seq.valid(1)
    assert simple_seq.valid(999)
    assert not simple_seq.valid(0)
    assert not simple_seq.valid(-1)

    assert jump_seq.valid(2)
    assert jump_seq.valid(5)
    assert jump_seq.valid(8)
    assert not jump_seq.valid(1)
    assert not jump_seq.valid(-5)
