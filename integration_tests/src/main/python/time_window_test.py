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
from datetime import datetime
from marks import ignore_order, allow_non_gpu
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

# do it over a day so we have more chance of overlapping values
_restricted_start = datetime(2020, 1, 1, tzinfo=timezone.utc)
_restricted_end = datetime(2020, 1, 2, tzinfo=timezone.utc)
_restricted_ts_gen = TimestampGen(start=_restricted_start, end=_restricted_end)

# Once we support grouping by a struct (even single level) this should go away
# https://github.com/NVIDIA/spark-rapids/issues/2877
# Shuffle falls back to CPU because it is in between two CPU hash/sort aggregates
@allow_non_gpu('HashAggregateExec', 'SortAggregateExec', 'AggregateExpression', 'Max', 'Alias', 'ShuffleExchangeExec', 'HashPartitioning')
@pytest.mark.parametrize('data_gen', integral_gens + [string_gen], ids=idfn)
@ignore_order
def test_grouped_tumbling_window(data_gen):
    row_gen = StructGen([['ts', _restricted_ts_gen],['data', data_gen]], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).groupBy(f.window('ts', '5 hour')).agg(f.max("data").alias("max_data")))

# Warning. On Sliding windows is it easy to make lots of overlapping windows. This can make the Spark code generation
# have some real problems and even crash some times when trying to JIT it. This problem only happens on the CPU
# so be careful.

# Once we support grouping by a struct (even single level) this should go away
# https://github.com/NVIDIA/spark-rapids/issues/2877
# Shuffle falls back to CPU because it is in between two CPU hash/sort aggregates
@allow_non_gpu('HashAggregateExec', 'SortAggregateExec', 'AggregateExpression', 'Max', 'Alias', 'ShuffleExchangeExec', 'HashPartitioning')
@pytest.mark.parametrize('data_gen', integral_gens + [string_gen], ids=idfn)
@ignore_order
def test_grouped_sliding_window(data_gen):
    row_gen = StructGen([['ts', _restricted_ts_gen],['data', data_gen]], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).groupBy(f.window('ts', '5 hour', '1 hour')).agg(f.max("data").alias("max_data")))

# Having arrays allows us to verify that expand exec in this case works with arrays too
# Once we support grouping by a struct (even single level) this should go away
# https://github.com/NVIDIA/spark-rapids/issues/2877
# Shuffle falls back to CPU because it is in between two CPU hash/sort aggregates
@allow_non_gpu('HashAggregateExec', 'SortAggregateExec', 'AggregateExpression', 'GetArrayItem', 'Literal', 'Max', 'Alias', 'ShuffleExchangeExec', 'HashPartitioning')
@pytest.mark.parametrize('data_gen', integral_gens + [string_gen], ids=idfn)
@ignore_order
def test_grouped_sliding_window_array(data_gen):
    row_gen = StructGen([['ts', _restricted_ts_gen],['data', ArrayGen(data_gen)]], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).groupBy(f.window('ts', '5 hour', '1 hour')).agg(f.max(f.col("data")[3]).alias("max_data")))

@pytest.mark.parametrize('data_gen', integral_gens + [string_gen], ids=idfn)
@ignore_order
def test_tumbling_window(data_gen):
    row_gen = StructGen([['ts', _restricted_ts_gen],['data', data_gen]], nullable=False)
    w = Window.partitionBy(f.window('ts', '5 hour'))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).withColumn('rolling_max', f.max("data").over(w)))

@pytest.mark.parametrize('data_gen', integral_gens + [string_gen], ids=idfn)
@ignore_order
def test_sliding_window(data_gen):
    row_gen = StructGen([['ts', _restricted_ts_gen],['data', data_gen]], nullable=False)
    w = Window.partitionBy(f.window('ts', '5 hour', '1 hour'))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).withColumn('rolling_max', f.max("data").over(w)))

# This allows us to verify that GpuExpandExec works with all of the various types.
@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens + array_gens_sample + map_gens_sample, ids=idfn)
# This includes an expand and we produce a different order than the CPU does. Sort locally to allow sorting of all types
@ignore_order(local=True)
def test_just_window(data_gen):
    row_gen = StructGen([['ts', timestamp_gen],['data', data_gen]], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, row_gen).withColumn('time_bucket', f.window('ts', '5 hour', '1 hour')),
            conf = allow_negative_scale_of_decimal_conf)


