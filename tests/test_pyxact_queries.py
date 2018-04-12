# Test pyxact.queries

import pytest
import pyxact.fields as fields
import pyxact.records as records
import pyxact.recordlists as recordlists
import pyxact.queries as queries
from pyxact.dialects import sqliteDialect

class SingleIntRow(records.SQLRecord):
    answer=fields.IntField()

class SingleIntRowLists(recordlists.SQLRecordList, record_type=SingleIntRow):
    pass

class StaticQuery(queries.SQLQuery,
                  query='SELECT 2+2 AS answer;',
                  record_type=SingleIntRow,
                  recordlist_type=SingleIntRowLists):
    pass

class StaticQueryResult(queries.SQLQueryResult, query=StaticQuery):
    pass

class SimpleQuery(queries.SQLQuery,
                  query='SELECT {alpha}+{beta} AS answer;',
                  record_type=SingleIntRow):
    alpha=fields.IntField()
    beta=fields.IntField()

class MultiValueQuery(queries.SQLQuery,
                      query='VALUES (1),(2),(3),(4)',
                      record_type=SingleIntRow,
                      recordlist_type=SingleIntRowLists):
    pass

class MultiValueQueryResult(queries.SQLQueryResult, query=MultiValueQuery):
    pass

class Holder:
    query_field = fields.IntField(query=SimpleQuery)

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

def test_queryintfield(sqlitecur):

    test_holder = Holder()
    test_holder.query_field = 6
    assert test_holder.query_field == 6

    context = {'alpha' : 6, 'beta' : 7}

    Holder.query_field.update(test_holder, context, sqlitecur, sqliteDialect)

    assert test_holder.query_field == 13
