'''Test pyxact.dialects'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

import pytest
import pyxact.dialects as dialects

def test_convert_schema_sep():

    assert dialects.convert_schema_sep('{alpha.beta}') == 'alpha.beta'
    assert dialects.convert_schema_sep('{alpha.beta}','_') == 'alpha_beta'
    assert dialects.convert_schema_sep('Prefix {alpha.beta} suffix') == 'Prefix alpha.beta suffix'
    assert dialects.convert_schema_sep('Prefix {alpha.beta} middle {gamma.elipson} suffix') == \
                                       'Prefix alpha.beta middle gamma.elipson suffix'
    assert dialects.convert_schema_sep('{alpha.beta}{gamma.elipson}') == 'alpha.betagamma.elipson'
    assert dialects.convert_schema_sep('{alpha_beta}') == '{alpha_beta}'
    assert dialects.convert_schema_sep('{.}') == '{.}'
