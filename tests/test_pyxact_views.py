'''Test SQLView behaviours that need a sqlite database to demonstrate'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.fields as fields
import pyxact.views as views
from pyxact.dialects import sqliteDialect

def test_colliding_field_names():

    #SQLViewMetaClass should not allow subclasses of SQLView with names that collide with built-in
    # methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord(views.SQLView, view_name='sample_view_fail'):
            ok_name = fields.IntField()
            _create_view_sql = fields.IntField()

    # SQLViewMetaClass should not allow subclasses of SQLView with names that collide with built-in
    # methods/attributes.
    with pytest.raises(AttributeError):
        class FailRecord2(views.SQLView, view_name='sample_view'):
            ok_name = fields.IntField()
            _query = fields.IntField()

def test_select(sqlitecur, sample_view, sample_view_class, sample_table, sample_table_rows):

    sqlitecur.execute(*sample_view_class._simple_select_sql())

    assert sqlitecur.fetchone() == None

    for row in sample_table_rows:
        sqlitecur.execute(*row._insert_sql())

    sqlitecur.execute(*sample_view_class._simple_select_sql())

    assert sqlitecur.fetchone() == (1, 3.4)
    assert sqlitecur.fetchone() == (2, 1.1)


