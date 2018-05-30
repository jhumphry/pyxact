'''An example of using Enum support.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import enum
import sys

import pyxact.enums, pyxact.postgresql
import pyxact.fields as fields
import pyxact.tables as tables
import example_schema, utils

class TrafficLight(enum.Enum):
    RED=1
    AMBER=2
    GREEN=3

class TrafficLightField(pyxact.enums.EnumField):
    enum_type=TrafficLight
    enum_sql='trafficlight'

class EnumTestTable(tables.SQLTable, table_name='enum_test'):
    tid = fields.IntField(context_used='tid')
    traffic_light = TrafficLightField()

if __name__ == '__main__':

    conn = utils.process_command_line('Demonstrate usage of pyxact JSON serialisation')

    cursor = conn.cursor()
    # example_schema.create_example_schema(cursor)
    # example_schema.populate_example_schema(cursor)

    if utils.DATABASE_USED == 'PostgreSQL':
        cursor.execute('DROP TYPE IF EXISTS trafficlight;')
        pyxact.postgresql.create_enum_type(cursor, TrafficLight, 'trafficlight')
        cursor.execute('COMMIT;')
