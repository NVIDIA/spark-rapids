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
from pyspark.sql.types import *
import pyspark.sql.functions as f

def unary_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([('a', gen)], nullable=False), length=length, seed=seed)

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

@pytest.mark.parametrize('data_gen,delim', [(mk_str_gen('([ABC]{0,3}_?){0,7}'), '_'),
    (mk_str_gen('([MNP_]{0,3}\.?){0,5}'), '.'),
    (mk_str_gen('([123]{0,3}\^?){0,5}'), '^')], ids=idfn)
def test_substring_index(data_gen,delim):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.substring_index(f.col('a'), delim, 1),
                f.substring_index(f.col('a'), delim, 3),
                f.substring_index(f.col('a'), delim, 0),
                f.substring_index(f.col('a'), delim, -1),
                f.substring_index(f.col('a'), delim, -4)))


