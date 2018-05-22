'''An example of using SQLTransaction hooks.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import decimal
import random
import sys

from pyxact import fields, queries, records, transactions
from pyxact import loggingdb
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

SUM_ACCOUNT_QUERY = '''
SELECT SUM(amount)
FROM {accounting.journals}
WHERE account = {account};'''

class SumAccountQuery(queries.SQLQuery, query=SUM_ACCOUNT_QUERY):
    account = fields.IntField()

class SetAccountTotal(example_schema.AccountingTransaction):
    account = fields.IntField()
    desired_sum = fields.NumericField(precision=8, scale=2, allow_floats=True)
    current_sum = fields.NumericField(precision=8, scale=2, allow_floats=True,
                                      inexact_quantize=True, query=SumAccountQuery)

    def _pre_insert_hook(self, context, cursor, dialect):
        super()._pre_insert_hook(context, cursor, dialect)

        if self.transaction is None:
            self.transaction = example_schema.TransactionTable()
        if self.journal_list is None:
            self.journal_list = example_schema.JournalList()

        self.transaction.narrative = 'Adjusting account {} to {}'.format(self.account,
                                                                         self.desired_sum)
        self.transaction.creator = 'SYS'
        self.transaction.t_rev = False
        posting_needed = self.desired_sum - self.current_sum

        self.journal_list._clear()
        self.journal_list._append(example_schema.JournalTable(account=self.account, amount=posting_needed))
        self.journal_list._append(example_schema.JournalTable(account=9999, amount=-posting_needed))

# This more complicated subclass of AccountingTransaction shows how a hook can be used to
# automatically regenerate some parts of the transaction based on other parts. Here the journals are
# generated based on context fields provided by the user and a query refreshed from the database,
# along with the timestamp, tid and row_id fields that are set up to be automatically completed by
# the standard operation of AccountingTransaction

# Note that the current_sum field has 'inexact_quantize' activated. SQLite does not really support
# NUMERIC. It stores values as an integer, binary float or text as necessary to avoid losing ,
# precision but it cannot do decimal fixed-point arithmetic, so uses binary floating-point which
# will not always exactly quantize down to two decimal places. The 'inexact_quantize' parameter
# tells the NumericField to silently discard the excess decimal places without complaining.

def generate_transactions(cursor, n=100):
    '''Add random transactions to the example schema'''

    trans_details = example_schema.TransactionTable(creator='AAA',
                                                    t_rev=False)
    trans_journals = example_schema.JournalList(example_schema.JournalTable(),
                                                example_schema.JournalTable(),
                                               )
    tmp_transaction = example_schema.AccountingTransaction(transaction=trans_details,
                                                           journal_list=trans_journals)
    for i in range(0,n):
        tmp_transaction.transaction.narrative='Random transaction '+str(i)
        x = random.randint(-500, 500)
        z = decimal.Decimal(x).scaleb(-2)
        tmp_transaction.journal_list[0].amount = z
        tmp_transaction.journal_list[0].account = random.randint(1000,1020)
        tmp_transaction.journal_list[1].amount = -z
        tmp_transaction.journal_list[1].account = random.randint(1000,1020)
        tmp_transaction._insert_new(cursor)

if __name__ == '__main__':

    conn = example_schema.process_command_line('Demonstrate pyxact transactions with hooks')

    cursor = conn.cursor()
    example_schema.create_example_schema(cursor)
    example_schema.populate_example_schema(cursor)

    # Now we are going to select the first transaction created by populate_example_schema
    rev_trans = ReverseTransaction(tid=1)
    rev_trans._context_select(cursor)

    # rev_trans will have been normalized at the end of the context_select - i.e. the sign of the
    # amounts in the journals will have been flipped

    rev_trans._insert_new(cursor)

    # As insert_new rather than insert_existing has been used, updated values for tid and
    # creation_ts will have been used and written back into rev_trans

    original_trans = example_schema.AccountingTransaction(tid=1)
    original_trans._context_select(cursor)

    # rev_trans could be used directly, but this shows that the new data has hit the database
    new_trans = example_schema.AccountingTransaction(tid=rev_trans.tid)
    new_trans._context_select(cursor)

    print('***Original transaction***\n')
    print(original_trans)

    print('***Reversed transaction***\n')
    print(new_trans)

    print('\nInserting 100 random transactions\n')
    generate_transactions(cursor=cursor, n=100)

    account_1010 = SumAccountQuery(account=1010)
    account_1010._execute(cursor)
    print('Account 1010 has total value: {}'.format(account_1010._result_singlevalue(cursor)))
    print('(any spurious decimal places are due to the lack of support for true decimal arithmetic '
          'on NUMERIC data types in SQLite)\n')

    print('Adjusting to value 3.14\n')
    set_account_1010 = SetAccountTotal(account=1010,
                                       desired_sum=decimal.Decimal('3.14'))
    set_account_1010._insert_new(cursor)

    account_1010._execute(cursor)
    print('Account 1010 has total value: {}\n'.format(account_1010._result_singlevalue(cursor)))

