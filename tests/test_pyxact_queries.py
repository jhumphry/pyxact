'''Test pyxact.queries'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.records as records
import pyxact.recordlists as recordlists
import pyxact.queries as queries
from pyxact.dialects import sqliteDialect

class SingleIntRow(records.SQLRecord):
    answer=fields.IntField()

class StaticQuery(queries.SQLQuery,
                  query='SELECT 2+2 AS answer;',
                  record_type=SingleIntRow):
    pass

class StaticQueryResult(queries.SQLQueryResult, query=StaticQuery):
    pass

def test_static_query(sqlitecur):

    static_query=StaticQuery()

    static_query._execute(sqlitecur, sqliteDialect)
    assert static_query._result_singlevalue(sqlitecur) == 4

    static_query._execute(sqlitecur, sqliteDialect)
    result = static_query._result_record(sqlitecur)
    assert result.answer == 4

    static_query._execute(sqlitecur, sqliteDialect)
    result_list = list(static_query._result_records(sqlitecur))
    assert len(result_list) == 1
    assert result_list[0].answer == 4

    result_recordlist = StaticQueryResult()
    result_recordlist._refresh(sqlitecur, sqliteDialect)
    assert len(result_recordlist) == 1
    assert result_recordlist[0].answer == 4

######

class SimpleQuery(queries.SQLQuery,
                  query='SELECT {alpha}+{beta} AS answer;',
                  record_type=SingleIntRow):
    alpha=fields.IntField()
    beta=fields.IntField()

def test_simple_query(sqlitecur):

    simple_query=SimpleQuery()

    simple_query.alpha=2
    simple_query.beta=3
    simple_query._execute(sqlitecur, sqliteDialect)
    assert simple_query._result_singlevalue(sqlitecur) == 5

    simple_query.beta=-4
    simple_query._execute(sqlitecur, sqliteDialect)
    assert simple_query._result_singlevalue(sqlitecur) == -2

    assert simple_query._query_values() == [2, -4]

######

class MultiValueQuery(queries.SQLQuery,
                      query='VALUES (1),(2),(3),(4)',
                      record_type=SingleIntRow):
    pass

class MultiValueQueryResult(queries.SQLQueryResult, query=MultiValueQuery):
    pass

def test_multivalue_query(sqlitecur):

    mv_query=MultiValueQuery()

    mv_query._execute(sqlitecur, sqliteDialect)
    assert mv_query._result_singlevalue(sqlitecur) == 1
    assert mv_query._result_singlevalue(sqlitecur) == 2
    assert mv_query._result_singlevalue(sqlitecur) == 3
    assert mv_query._result_singlevalue(sqlitecur) == 4

    with pytest.raises(ValueError, message='Should not have returned another result'):
        assert mv_query._result_singlevalue(sqlitecur) == 5

    mv_query._execute(sqlitecur, sqliteDialect)
    for i in range(1, 5):
        result = mv_query._result_record(sqlitecur)
        assert result.answer == i

    mv_query._execute(sqlitecur, sqliteDialect)
    result_list = list(mv_query._result_records(sqlitecur))
    assert len(result_list) == 4
    assert result_list[2].answer == 3

    result_recordlist = MultiValueQueryResult()
    result_recordlist._refresh(sqlitecur, sqliteDialect)
    assert len(result_recordlist) == 4
    assert result_recordlist[3].answer == 4

######

class Holder:
    query_field = fields.IntField(query=SimpleQuery)

def test_queryintfield(sqlitecur):

    test_holder = Holder()
    test_holder.query_field = 6
    assert test_holder.query_field == 6

    context = {'alpha' : 6, 'beta' : 7}

    Holder.query_field.update(test_holder, context, sqlitecur, sqliteDialect)

    assert test_holder.query_field == 13

######

complex_query_text = '''
SELECT x AS x, x*{alpha} AS x_alpha, y AS y, y*{beta} AS y_beta
FROM (SELECT 1 AS x, 2 AS y UNION VALUES (3, 4), (5,6));'''

class ComplexQueryRecord(records.SQLRecord):
    x=fields.IntField()
    x_alpha=fields.IntField()
    y=fields.IntField()
    y_beta=fields.IntField()

class ComplexQuery(queries.SQLQuery,
                   query=complex_query_text,
                   record_type=ComplexQueryRecord):
    alpha=fields.IntField()
    beta=fields.IntField()

class ComplexQueryResult(queries.SQLQueryResult, query=ComplexQuery):
    pass

def test_complexquery(sqlitecur):

    cq = ComplexQueryResult()

    cq._query.alpha = 2
    cq._query.beta = 3
    cq._refresh(sqlitecur)

    assert list(cq.x) == [1, 3, 5]
    assert list(cq.x_alpha) == [2, 6, 10]
    assert list(cq.y) == [2, 4, 6]
    assert list(cq.y_beta) == [6, 12, 18]

    cq._query.beta=4
    cq._refresh(sqlitecur)

    assert list(cq.x) == [1, 3, 5]
    assert list(cq.x_alpha) == [2, 6, 10]
    assert list(cq.y) == [2, 4, 6]
    assert list(cq.y_beta) == [8, 16, 24]

    assert cq._get_context() == {'alpha' : 2, 'beta' : 4}

    cq._set_context({'alpha' : -1, 'beta' : -2})
    cq._refresh(sqlitecur)

    assert list(cq.x) == [1, 3, 5]
    assert list(cq.x_alpha) == [-1, -3, -5]
    assert list(cq.y) == [2, 4, 6]
    assert list(cq.y_beta) == [-4, -8, -12]

    sqlitecur.execute(*cq._context_select_sql({'alpha' : 2, 'beta' : 1}))
    nextrow = sqlitecur.fetchone()
    assert nextrow == (1 , 2, 2, 2)

