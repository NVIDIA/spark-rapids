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
from marks import *
from spark_session import with_cpu_session

_grpkey_longs_with_no_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length=20)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

_grpkey_longs_with_timestamps = [
    ('a', RepeatSeqGen(LongGen(), length=2048)),
    ('b', DateGen(nullable=False, start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

_grpkey_longs_with_nullable_timestamps = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', DateGen(nullable=(True, 5.0), start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]


@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_no_nulls,
                                      _grpkey_longs_with_nulls,
                                      _grpkey_longs_with_timestamps,
                                      _grpkey_longs_with_nullable_timestamps], ids=idfn)
def test_window_aggs_for_rows(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=2048))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' sum(c) over '
            '   (partition by a order by b,c asc rows between 1 preceding and 1 following) as sum_c_asc, '
            ' max(c) over '
            '   (partition by a order by b desc, c desc rows between 2 preceding and 1 following) as max_c_desc, '
            ' min(c) over '
            '   (partition by a order by b,c rows between 2 preceding and current row) as min_c_asc, '
            ' count(1) over '
            '   (partition by a order by b,c rows between UNBOUNDED preceding and UNBOUNDED following) as count_1, '
            ' row_number() over '
            '   (partition by a order by b,c rows between UNBOUNDED preceding and CURRENT ROW) as row_num '
            'from window_agg_table '))


# Test for RANGE queries, with timestamp order-by expressions.
# Non-timestamp order-by columns are currently unsupported for RANGE queries.
# See https://github.com/NVIDIA/spark-rapids/issues/216
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_timestamps,
                                      _grpkey_longs_with_nullable_timestamps], ids=idfn)
def test_window_aggs_for_ranges(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=2048))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' sum(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between interval 1 day preceding and interval 1 day following) as sum_c_asc, '
            ' max(c) over '
            '   (partition by a order by cast(b as timestamp) desc '
            '       range between interval 2 days preceding and interval 1 days following) as max_c_desc, '
            ' min(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between interval 2 days preceding and current row) as min_c_asc, '
            ' count(1) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between  CURRENT ROW and UNBOUNDED following) as count_1_asc, '
            ' sum(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between UNBOUNDED preceding and CURRENT ROW) as sum_c_unbounded, '
            ' max(c) over '
            '   (partition by a order by cast(b as timestamp) asc  '
            '       range between UNBOUNDED preceding and UNBOUNDED following) as max_c_unbounded '
            'from window_agg_table'))


@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over non-timestamp columns "
                          "(https://github.com/NVIDIA/spark-rapids/issues/216)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_timestamps], ids=idfn)
def test_window_aggs_for_ranges_of_dates(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=2048))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' sum(c) over '
            '   (partition by a order by b asc  '
            '       range between 1 preceding and 1 following) as sum_c_asc '
            'from window_agg_table'))


@pytest.mark.xfail(reason="[BUG] `COUNT(x)` should not count null values of `x` "
                          "(https://github.com/NVIDIA/spark-rapids/issues/218)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_no_nulls], ids=idfn)
def test_window_aggs_for_rows_count_non_null(data_gen):
    df = with_cpu_session(
        lambda spark : gen_df(spark, data_gen, length=2048))
    df.createOrReplaceTempView("window_agg_table")
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql(
            'select '
            ' count(c) over '
            '   (partition by a order by b,c '
            '       rows between UNBOUNDED preceding and UNBOUNDED following) as count_non_null '
            'from window_agg_table '))