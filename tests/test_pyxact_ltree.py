'''Test pyxact.postgresql.ltree'''

# Copyright 2018-2019, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.postgresql.ltree as ltree

@pytest.fixture()
def ltree_holder_class():

    class LTreeHolder:
        lt = ltree.LTreeField()
        lt_text = ltree.LTreeField(store_as_text=True)

    return LTreeHolder

def test_ltree_type():

    lt1 = ltree.LTree('alpha')
    lt2 = ltree.LTree('beta.gamma')
    lt3 = ltree.LTree(lt2)

    assert lt2 == lt3

    lt4 = ltree.LTree('this.is.a.unīcodé.tęst')

    # LTree should only take a string-like object.
    with pytest.raises(TypeError):
        tmp = ltree.LTree(3)

    # LTree should only accept valid ltree paths
    with pytest.raises(ValueError):
        tmp = ltree.LTree('delta epsilon')

    assert lt1+lt2 == 'alpha.beta.gamma'
    assert lt1+lt2 == ltree.LTree('alpha.beta.gamma')

    assert lt1.path() == ['alpha', ]
    assert lt2.path() == ['beta', 'gamma']

def test_ltree_field(ltree_holder_class):

    lthc = ltree_holder_class
    lth = lthc()

    assert lthc.lt.sql_type() == 'LTREE'
    assert lthc.lt_text.sql_type() == 'TEXT'

    lth.lt = 'alpha'
    lth.lt = 'beta.gamma'
    lth.lt = ltree.LTree('delta.epsilon')
    lth.lt_text = 'alpha'
    lth.lt_text = 'beta.gamma'
    lth.lt_text = ltree.LTree('delta.epsilon')

    # LTreeField should only take a string-like object
    with pytest.raises(TypeError):
        lth.lt = 3

    # LTreeField should only accept valid ltree paths
    with pytest.raises(ValueError):
        lth.lt = ltree.LTree('delta epsilon')

