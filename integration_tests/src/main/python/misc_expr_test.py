# Copyright (c) 2020-2024, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_400

def test_mono_id():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, short_gen, num_slices=8).select(
                f.col('a'),
                f.monotonically_increasing_id()))

def test_part_id():
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, short_gen, num_slices=8).select(
                f.col('a'),
                f.spark_partition_id()))


@pytest.mark.skipif(condition=not is_before_spark_400(),
                    reason="raise_error() not currently implemented for Spark 4.0. "
                           "See https://github.com/NVIDIA/spark-rapids/issues/10107.")
def test_raise_error():
    data_gen = ShortGen(nullable=False, min_val=0, max_val=20, special_cases=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen, num_slices=2).select(
            f.when(f.col('a') > 30, f.raise_error("unexpected"))))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.range(0).select(f.raise_error(f.col("id"))))

    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, null_gen, length=2, num_slices=1).select(
                f.raise_error(f.col('a'))).collect(),
        conf={},
        error_message="java.lang.RuntimeException")

    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, short_gen, length=2, num_slices=1).select(
                f.raise_error(f.lit("unexpected"))).collect(),
        conf={},
        error_message="java.lang.RuntimeException: unexpected")
