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
from marks import incompat
from pyspark.sql.types import *
import pyspark.sql.functions as f

@pytest.mark.parametrize('data_gen', [StructGen([["first", boolean_gen], ["second", byte_gen], ["third", float_gen]]),
    StructGen([["first", short_gen], ["second", int_gen], ["third", long_gen]]),
    StructGen([["first", double_gen], ["second", date_gen], ["third", timestamp_gen]]),
    StructGen([["first", string_gen], ["second", ArrayGen(byte_gen)], ["third", simple_string_to_string_map_gen]])], ids=idfn)
def test_struct_get_item(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a.first',
                'a.second',
                'a.third'))
