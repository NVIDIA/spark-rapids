# Copyright (c) 2021-2024, NVIDIA CORPORATION.
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

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
def test_hllpp_groupby(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, [("c1", int_gen), ("c2", data_gen)]),
        "tab",
        "select c1, APPROX_COUNT_DISTINCT(c2) from tab group by c1")

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
def test_hllpp_reduction(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : unary_op_df(spark, data_gen),
        "tab",
        "select APPROX_COUNT_DISTINCT(a) from tab")