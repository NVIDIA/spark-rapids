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
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f

@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_project_alias(data_gen):
    dec = Decimal('123123123123123123123123123.456')
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).select(
            f.col('a').alias('col1'),
            f.col('b').alias('col2'),
            f.lit(dec)))

@pytest.mark.parametrize('data_gen', [double_gen], ids=idfn)
def test_project_float(data_gen):
    data_type = data_gen.data_type
    dec = float(123123123123123123123123123.456)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : debug_df(binary_op_df(spark, data_gen, length=2).select(
            f.col('a').cast(DecimalGen(precision=20, scale=3).data_type))))


