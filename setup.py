'''The setuptools script to manage the pyxact package.'''

# Copyright 2018, James Humphry
# This work is released under the ISC license - see LICENSE for details
# SPDX-License-Identifier: ISC

from setuptools import setup, find_packages
setup(
    name="pyxact",
    version="0.0.1",
    packages=find_packages(),

    python_requires='>=3.6',

    author="James Humphry",
    description="This project explores transaction-focussed interfacing with SQL databases.",
    license="ISC",
    keywords="database",
    url="https://github.com/jhumphry/pyxact",
)
