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

_approximate_float_args = None

def get_float_check():
    if not _approximate_float_args is None:
        return lambda lhs,rhs: lhs == pytest.approx(rhs, **_approximate_float_args)
    else:
        return lambda lhs,rhs: lhs == rhs

_incompat = False

def is_incompat():
    return _incompat

_ignore_order = False

def is_order_ignored():
    return _ignore_order

_allow_any_non_gpu = False
_non_gpu_allowed = []

def is_allowing_any_non_gpu():
    return _allow_any_non_gpu

def get_non_gpu_allowed():
    return _non_gpu_allowed

_limit = -1

def get_limit():
    return _limit

def _get_limit_from_mark(mark):
    if mark.args:
        return mark.args[0]
    else:
        return mark.kwargs.get('num_rows', 100000)

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

    global _approximate_float_args
    app_f = item.get_closest_marker('approximate_float')
    if app_f:
        _approximate_float_args = app_f.kwargs
    else:
        _approximate_float_args = None

    global _allow_any_non_gpu
    global _non_gpu_allowed
    non_gpu = item.get_closest_marker('allow_non_gpu')
    if non_gpu:
        if non_gpu.kwargs and non_gpu.kwargs['any']:
            _allow_any_non_gpu = True
            _non_gpu_allowed = []
        elif non_gpu.args:
            _allow_any_non_gpu = False
            _non_gpu_allowed = non_gpu.args
        else:
            pytest.warn('allow_non_gpu marker without anything allowed')
            _allow_any_non_gpu = False
            _non_gpu_allowed = []
    else:
        _allow_any_non_gpu = False
        _non_gpu_allowed = []

    global _limit
    limit_mrk = item.get_closest_marker('limit')
    if limit_mrk:
        _limit = _get_limit_from_mark(limit_mrk)
    else:
        _limit = -1


def pytest_collection_modifyitems(config, items):
    for item in items:
        extras = []
        if item.get_closest_marker('ignore_order'):
            extras.append('IGNORE_ORDER')
        if item.get_closest_marker('incompat'):
            extras.append('INCOMPAT')
        app_f = item.get_closest_marker('approximate_float')
        if app_f:
            if app_f.kwargs:
                extras.append('APPROXIMATE_FLOAT(' + str(app_f.kwargs) + ')')
            else:
                extras.append('APPROXIMATE_FLOAT')
        non_gpu = item.get_closest_marker('allow_non_gpu')
        if non_gpu:
            if non_gpu.kwargs and non_gpu.kwargs['any']:
                extras.append('ALLOW_NON_GPU(ANY)')
            elif non_gpu.args:
                extras.append('ALLOW_NON_GPU(' + ','.join(non_gpu.args) + ')')

        limit_mrk = item.get_closest_marker('limit')
        if limit_mrk:
            extras.append('LIMIT({})'.format(_get_limit_from_mark(limit_mrk)))

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
