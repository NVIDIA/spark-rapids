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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import ignore_order

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_union(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).union(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_union_by_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).unionByName(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('num_parts', [1, 10, 100, 1000, 2000], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
def test_coalesce_df(num_parts, length):
    #This should change eventually to be more than just the basic gens
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen_list, length=length).coalesce(num_parts))

@pytest.mark.parametrize('num_parts', [1, 10, 100, 1000, 2000], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
def test_repartion_df(num_parts, length):
    #This should change eventually to be more than just the basic gens
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen_list, length=length).repartition(num_parts),
            conf = allow_negative_scale_of_decimal_conf)
