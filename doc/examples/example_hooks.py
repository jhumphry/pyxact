'''An example of using SQLTransaction hooks.'''

import sqlite3

import example_schema

class ReverseTransaction(example_schema.AccountingTransaction):

    def _post_select_hook(self, context, cursor, dialect):
        super()._post_select_hook(context, cursor, dialect)
        for i in self.journal_list:
            i.amount = -i.amount
        self.transaction.t_rev = True

# It is possible to subclass AccountingTransaction from the example_schema module and change the
# post_select_hook() method which normalizes the data after it has been read in. In this case, it
# flips the sign of all of the journal amounts. It will inherit all of the SQLField, SQLTable and
# SQLRecordList attributes from the base class.

if __name__ == '__main__':
    conn = sqlite3.connect(':memory:')
    conn.execute('PRAGMA foreign_keys = ON;') # We need SQLite foreign key support

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor)
    example_schema.populate_example_schema(cursor)

    # Now we are going to select the first transaction created by populate_example_schema
    rev_trans = ReverseTransaction(tid=1)
    rev_trans._context_select(cursor)

    # rev_trans will have been normalized at the end of the context_select - i.e. the sign of the
    # amounts in the journals will have been flipped

    rev_trans._insert_new(cursor)

    # As insert_new rather than insert_existing has been used, updated values for tid and creation_ts
    # will have been used and written back into rev_trans

    original_trans = example_schema.AccountingTransaction(tid=1)
    original_trans._context_select(cursor)

    # rev_trans could be used directly, but this shows that the new data has hit the database
    new_trans = example_schema.AccountingTransaction(tid=rev_trans.tid)
    new_trans._context_select(cursor)

    print('***Original transaction***\n')
    print(original_trans)

    print('***Reversed transaction***\n')
    print(new_trans)
