#!/bin/sh

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

# This is a place to collect various tests and checks that should always pass
# Intended to be run with the working directory in the root of the project

echo "Checking for Python files lacking an SPDX identifier:"
grep -r -L --include='*.py' --exclude-dir=tmp --exclude-dir=venv 'SPDX\-License\-Identifier'
echo

echo "Pytest results:"
pytest -q tests
