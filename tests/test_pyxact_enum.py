'''Test pyxact.enums'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import enum

import pytest
import pyxact.enums as enums
from pyxact.dialects import sqliteDialect

class TrafficLight(enum.Enum):
    RED=1
    AMBER=2
    GREEN=3

class Colours(enum.Enum):
    RED=1
    AMBER=2
    GREEN=3

@pytest.fixture()
def enum_field_class():

    class TrafficLightField(enums.EnumField):
        enum_type=TrafficLight
        enum_sql='trafficlight'

    return TrafficLightField

@pytest.fixture()
def enum_field_holder_class(enum_field_class):

    class EnumFieldHolder:
        tl1=enum_field_class()
        tl2=enum_field_class()

    return EnumFieldHolder

def test_ltree_field(enum_field_holder_class):

    ehc = enum_field_holder_class

    class DialectWithEnums:
        enum_support = True

    class DialectWithoutEnums:
        enum_support = False


    assert ehc.tl1.sql_type(dialect=DialectWithEnums) == 'trafficlight'
    assert ehc.tl1.sql_type(dialect=DialectWithoutEnums) == 'SMALLINT'

    eh = ehc()

    eh.tl1 = TrafficLight.RED
    eh.tl2 = TrafficLight.GREEN

    with pytest.raises(TypeError, message='EnumField should not take a valid value of a different '
                                          'Enum class to that it was created for.'):
        eh.tl1 = Colours.RED

    eh.tl1 = 2
    assert eh.tl1 == TrafficLight.AMBER

    with pytest.raises(ValueError, message='EnumField should only take a valid int values of the '
                                           'correct underlying Enum subclass.'):
        eh.tl1 = 99

    eh.tl2 = 'RED'
    assert eh.tl2 == TrafficLight.RED

    with pytest.raises(KeyError, message='EnumField should only take a valid str values of the '
                                           'correct underlying Enum subclass.'):
        eh.tl1 = 'PURPLE'
