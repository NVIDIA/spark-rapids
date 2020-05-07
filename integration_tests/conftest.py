# Copyright (c) 2020, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

_incompat = False

def is_incompat():
    return _incompat

_ignore_order = False

def is_order_ignored():
    return _ignore_order

_allow_any_non_gpu = False

def is_allowing_any_non_gpu():
    return _allow_any_non_gpu

def pytest_runtest_setup(item):
    global _ignore_order
    if item.get_closest_marker('ignore_order'):
        _ignore_order = True
    else:
        _ignore_order = False

    global _incompat
    if item.get_closest_marker('incompat'):
        _incompat = True
    else:
        _incompat = False

    global _allow_any_non_gpu
    if item.get_closest_marker('allow_any_non_gpu'):
        _allow_any_non_gpu = True
    else:
        _allow_any_non_gpu = False

def pytest_collection_modifyitems(config, items):
    for item in items:
        extras = []
        if item.get_closest_marker('ignore_order'):
            extras.append('IGNORE_ORDER')
        if item.get_closest_marker('incompat'):
            extras.append('INCOMPAT')
        if item.get_closest_marker('allow_any_non_gpu'):
            extras.append('ALLOW_ANY_NON_GPU')

        if extras:
            # This is not ideal because we are reaching into an internal value
            item._nodeid = item.nodeid + '[' + ', '.join(extras) + ']'

def pytest_addoption(parser):
    """Pytest hook to define command line options for pytest"""
    parser.addoption(
        "--tpcxbb_format", action="store", default="parquet", help="format of TPCXbb data"
    )
    parser.addoption(
        "--tpcxbb_path", action="store", default=None, help="path to TPCXbb data"
    )
    parser.addoption(
        "--tpch_format", action="store", default="parquet", help="format of TPCH data"
    )
    parser.addoption(
        "--tpch_path", action="store", default=None, help="path to TPCH data"
    )
    parser.addoption(
        "--mortgage_format", action="store", default="parquet", help="format of Mortgage data"
    )
    parser.addoption(
        "--mortgage_path", action="store", default=None, help="path to Mortgage data"
    )
