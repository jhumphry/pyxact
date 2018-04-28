'''Test pyxact.indexes'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.indexes as indexes
from pyxact.dialects import sqliteDialect

import test_pyxact_tables

def test_index_creation(sqlitecur, sample_table_class):

    with pytest.raises(TypeError):
        tmp = indexes.SQLIndex('test_index', None, None)

    with pytest.raises(TypeError):
        tmp = indexes.SQLIndex('test_index', sample_table_class, None)


    sample_index = indexes.SQLIndex(name='test_index',
                                    table=sample_table_class,
                                    column_exprs=(indexes.IndexColumn('narrative', None, None),))

    assert sample_index.sql_name == 'test_index'
    assert sample_index.qualified_name() == 'test_index' # No schema set

    sqlitecur.execute(sample_table_class._create_table_sql())
    sample_index.create(sqlitecur)

    sample_index2 = indexes.SQLIndex(name='test_index',
                                table=sample_table_class,
                                column_exprs=(indexes.IndexColumn('narrative', None, None),),
                                sql_name='other_name')

    assert sample_index2.sql_name == 'other_name'
    assert sample_index2.qualified_name() == 'other_name' # No schema set

def test_indexcolumn(sample_table_class):

    with pytest.raises(TypeError):
        tmp = indexes.SQLIndex('test_index', sample_table_class, ('narrative',))

    with pytest.raises(ValueError):
        tmp = indexes.SQLIndex('test_index', sample_table_class, (indexes.IndexColumn('no_such', None, None),))

    sample_index = indexes.SQLIndex(name='test_index',
                                    table=sample_table_class,
                                    column_exprs=(indexes.IndexColumn('narrative', None, None),))

    sample_index_2col = indexes.SQLIndex(name='test_index',
                                         table=sample_table_class,
                                         column_exprs=(indexes.IndexColumn('narrative', None, None),
                                                       indexes.IndexColumn('flag', None, None)
                                                       )
                                         )

def test_exprcolumn(sample_table_class):

    with pytest.raises(TypeError):
        tmp = indexes.SQLIndex('test_index', sample_table_class, ('UPPER(narrative)',))

    tmp = indexes.SQLIndex('test_index', sample_table_class, (indexes.IndexExpr('UPPER(narrative)'),))


