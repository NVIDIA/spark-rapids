# Copyright (c) 2021-2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from hashing_test import xxhash_gens
from marks import ignore_order


@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', xxhash_gens, ids=idfn)
def test_hllpp_groupby(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, [("c1", int_gen), ("c2", data_gen)]),
        "tab",
        "select c1, APPROX_COUNT_DISTINCT(c2) from tab group by c1")


@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', xxhash_gens, ids=idfn)
def test_hllpp_reduction(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, data_gen),
        "tab",
        "select APPROX_COUNT_DISTINCT(a) from tab")

# precision = Math.ceil(2.0d * Math.log(1.106d / relativeSD) / Math.log(2.0d)).toInt
_relativeSD = [
    0.3,   #  precision 4
    0.25,  #  precision 5
    0.15,  #  precision 6
    0.1,   #  precision 7
    0.08,  #  precision 8
    0.05,  #  precision 9
    0.04,  #  precision 10
    0.03,  #  precision 11
    0.02,  #  precision 12
    0.015, #  precision 13
    0.01,  #  precision 14
    # 0.008, #  precision 15 Refer to bug: https://github.com/NVIDIA/spark-rapids/issues/12347
    # 0.005, #  precision 16 Refer to bug: https://github.com/NVIDIA/spark-rapids/issues/12347
    # 0.004, #  precision 17 Refer to bug: https://github.com/NVIDIA/spark-rapids/issues/12347
    # 0.003, #  precision 18 Refer to bug: https://github.com/NVIDIA/spark-rapids/issues/12347
]

@ignore_order(local=True)
@pytest.mark.parametrize('relativeSD', _relativeSD, ids=idfn)
def test_hllpp_precisions_reduce(relativeSD):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: unary_op_df(spark, int_gen),
        "tab",
        f"select APPROX_COUNT_DISTINCT(a, {relativeSD}) from tab")

@ignore_order(local=True)
@pytest.mark.parametrize('relativeSD', _relativeSD, ids=idfn)
def test_hllpp_precisions_groupby(relativeSD):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, [("c1", int_gen), ("c2", int_gen)]),
        "tab",
        f"select c1, APPROX_COUNT_DISTINCT(c2, {relativeSD}) from tab group by c1")
