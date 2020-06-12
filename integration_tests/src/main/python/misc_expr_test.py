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
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f

def test_mono_id():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, short_gen).select(
                f.col('a'),
                f.monotonically_increasing_id()))

def test_part_id():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, short_gen).select(
                f.col('a'),
                f.spark_partition_id()))
