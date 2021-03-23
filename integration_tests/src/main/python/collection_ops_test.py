# Copyright (c) 2021, NVIDIA CORPORATION.
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

@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('size_of_null', ['true', 'false'], ids=idfn)
def test_size_of_array(data_gen, size_of_null):
    gen = ArrayGen(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr('size(a)'),
            conf={'spark.sql.legacy.sizeOfNull': size_of_null})

@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
@pytest.mark.parametrize('size_of_null', ['true', 'false'], ids=idfn)
def test_size_of_map(data_gen, size_of_null):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr('size(a)'),
            conf={'spark.sql.legacy.sizeOfNull': size_of_null})
