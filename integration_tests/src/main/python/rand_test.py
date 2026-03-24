# Copyright (c) 2024-2025, NVIDIA CORPORATION.
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
from marks import *
from spark_session import is_before_spark_351

import pyspark.sql.functions as f


@ignore_order(local=True)
def test_group_agg_with_rand():
    # GPU and CPU produce the same grouping rows but in different orders after Shuffle,
    # while the rand() always generates the same sequence. Then CPU and GPU will produce
    # different final rows after aggregation. See as below:
    # GPU output:
    #     +---+-------------------+
    #     |  a|             random|
    #     +---+-------------------+
    #     |  3|  0.619189370225301|
    #     |  5| 0.5096018842446481|
    #     |  2| 0.8325259388871524|
    #     |  4|0.26322809041172357|
    #     |  1| 0.6702867696264135|
    #     +---+-------------------+
    # CPU output:
    #     +---+-------------------+
    #     |  a|             random|
    #     +---+-------------------+
    #     |  1|  0.619189370225301|
    #     |  2| 0.5096018842446481|
    #     |  3| 0.8325259388871524|
    #     |  4|0.26322809041172357|
    #     |  5| 0.6702867696264135|
    #     +---+-------------------+
    # To make the output comparable, here builds a generator to generate only one group.
    const_int_gen = IntegerGen(nullable=False, min_val=1, max_val=1, special_cases=[])

    def test(spark):
        return unary_op_df(spark, const_int_gen, num_slices=1).groupby('a').agg(f.rand(42))
    assert_gpu_and_cpu_are_equal_collect(test)


@ignore_order(local=True)
def test_project_with_rand():
    # To make the output comparable, here build a generator to generate only one value.
    # Not sure if Project could have the same order issue as groupBy, but still just in case.
    const_int_gen = IntegerGen(nullable=False, min_val=1, max_val=1, special_cases=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, const_int_gen, num_slices=1).select('a', f.rand(42))
    )


@ignore_order(local=True)
def test_filter_with_rand():
    const_int_gen = IntegerGen(nullable=False, min_val=1, max_val=1, special_cases=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, const_int_gen, num_slices=1).filter(f.rand(42) > 0.5)
    )

# See https://github.com/apache/spark/commit/9c0b803ba124a6e70762aec1e5559b0d66529f4d
@ignore_order(local=True)
@pytest.mark.skipif(is_before_spark_351(),
                    reason='Generate supports nondeterministic inputs from Spark 3.5.1')
def test_generate_with_rand():
    const_int_gen = IntegerGen(nullable=False, min_val=1, max_val=1, special_cases=[])
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, const_int_gen, num_slices=1).select(
            f.explode(f.array(f.rand(42))))
    )
