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

from spark_session import is_before_spark_310
from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.sql.functions as f

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

_grpkey_longs_with_decimals = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', DecimalGen(precision=18, scale=3, nullable=False)),
    ('c', IntegerGen())]

_grpkey_longs_with_nullable_decimals = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length=20)),
    ('b', DecimalGen(precision=18, scale=10, nullable=True)),
    ('c', IntegerGen())]

_grpkey_decimals_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length=20)),
    ('b', IntegerGen()),
    # the max decimal precision supported by sum operation is 8
    ('c', DecimalGen(precision=8, scale=3, nullable=True))]


@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_no_nulls,
                                      _grpkey_longs_with_nulls,
                                      _grpkey_longs_with_timestamps,
                                      _grpkey_longs_with_nullable_timestamps,
                                      _grpkey_longs_with_decimals,
                                      _grpkey_longs_with_nullable_decimals,
                                      _grpkey_decimals_with_nulls], ids=idfn)
def test_window_aggs_for_rows(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(c) over '
        '   (partition by a order by b,c asc rows between 1 preceding and 1 following) as sum_c_asc, '
        ' max(c) over '
        '   (partition by a order by b desc, c desc rows between 2 preceding and 1 following) as max_c_desc, '
        ' min(c) over '
        '   (partition by a order by b,c rows between 2 preceding and current row) as min_c_asc, '
        ' count(1) over '
        '   (partition by a order by b,c rows between UNBOUNDED preceding and UNBOUNDED following) as count_1, '
        ' count(c) over '
        '   (partition by a order by b,c rows between UNBOUNDED preceding and UNBOUNDED following) as count_c, '
        ' row_number() over '
        '   (partition by a order by b,c rows between UNBOUNDED preceding and CURRENT ROW) as row_num '
        'from window_agg_table ')


part_and_order_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=1)]

lead_lag_data_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=3)]

def meta_idfn(meta):
    def tmp(something):
        return meta + idfn(something)
    return tmp

@pytest.mark.xfail(condition=not(is_before_spark_310()), reason='https://github.com/NVIDIA/spark-rapids/issues/999')
@ignore_order
@approximate_float
@pytest.mark.parametrize('c_gen', lead_lag_data_gens, ids=idfn)
@pytest.mark.parametrize('b_gen', part_and_order_gens, ids=meta_idfn('orderBy:'))
@pytest.mark.parametrize('a_gen', part_and_order_gens, ids=meta_idfn('partBy:'))
def test_multi_types_window_aggs_for_rows_lead_lag(a_gen, b_gen, c_gen):
    data_gen = [
            ('a', RepeatSeqGen(a_gen, length=20)),
            ('b', b_gen),
            ('c', c_gen)]
    # By default for many operations a range of unbounded to unbounded is used
    # This will not work until https://github.com/NVIDIA/spark-rapids/issues/216
    # is fixed.

    # Ordering needs to include c because with nulls and especially on booleans
    # it is possible to get a different ordering when it is ambiguous.
    baseWindowSpec = Window.partitionBy('a').orderBy('b', 'c')
    inclusiveWindowSpec = baseWindowSpec.rowsBetween(-10, 100)

    defaultVal = gen_scalar_value(c_gen, force_no_nulls=False)

    def do_it(spark):
        return gen_df(spark, data_gen, length=2048) \
                .withColumn('inc_count_1', f.count('*').over(inclusiveWindowSpec)) \
                .withColumn('inc_count_c', f.count('c').over(inclusiveWindowSpec)) \
                .withColumn('inc_max_c', f.max('c').over(inclusiveWindowSpec)) \
                .withColumn('inc_min_c', f.min('c').over(inclusiveWindowSpec)) \
                .withColumn('lead_5_c', f.lead('c', 5).over(baseWindowSpec)) \
                .withColumn('lead_def_c', f.lead('c', 2, defaultVal).over(baseWindowSpec)) \
                .withColumn('lag_1_c', f.lag('c', 1).over(baseWindowSpec)) \
                .withColumn('lag_def_c', f.lag('c', 4, defaultVal).over(baseWindowSpec)) \
                .withColumn('row_num', f.row_number().over(baseWindowSpec))
    assert_gpu_and_cpu_are_equal_collect(do_it, conf={'spark.rapids.sql.hasNans': 'false'})

# lead and lag don't currently work for string columns, so redo the tests, but just for strings
# without lead and lag
@ignore_order
@approximate_float
@pytest.mark.parametrize('c_gen', [string_gen], ids=idfn)
@pytest.mark.parametrize('b_gen', part_and_order_gens, ids=meta_idfn('orderBy:'))
@pytest.mark.parametrize('a_gen', part_and_order_gens, ids=meta_idfn('partBy:'))
def test_multi_types_window_aggs_for_rows(a_gen, b_gen, c_gen):
    data_gen = [
            ('a', RepeatSeqGen(a_gen, length=20)),
            ('b', b_gen),
            ('c', c_gen)]
    # By default for many operations a range of unbounded to unbounded is used
    # This will not work until https://github.com/NVIDIA/spark-rapids/issues/216
    # is fixed.

    # Ordering needs to include c because with nulls and especially on booleans
    # it is possible to get a different ordering when it is ambiguous
    baseWindowSpec = Window.partitionBy('a').orderBy('b', 'c')
    inclusiveWindowSpec = baseWindowSpec.rowsBetween(-10, 100)

    def do_it(spark):
        return gen_df(spark, data_gen, length=2048) \
                .withColumn('inc_count_1', f.count('*').over(inclusiveWindowSpec)) \
                .withColumn('inc_count_c', f.count('c').over(inclusiveWindowSpec)) \
                .withColumn('inc_max_c', f.max('c').over(inclusiveWindowSpec)) \
                .withColumn('inc_min_c', f.min('c').over(inclusiveWindowSpec)) \
                .withColumn('row_num', f.row_number().over(baseWindowSpec))
    assert_gpu_and_cpu_are_equal_collect(do_it, conf={'spark.rapids.sql.hasNans': 'false'})


# Test for RANGE queries, with timestamp order-by expressions.
# Non-timestamp order-by columns are currently unsupported for RANGE queries.
# See https://github.com/NVIDIA/spark-rapids/issues/216
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_timestamps,
                                      pytest.param(_grpkey_longs_with_nullable_timestamps)],
                                      ids=idfn)
def test_window_aggs_for_ranges(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
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
        ' count(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_c_asc, '
        ' sum(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as sum_c_unbounded, '
        ' max(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between UNBOUNDED preceding and UNBOUNDED following) as max_c_unbounded '
        'from window_agg_table')

@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over non-timestamp columns "
                          "(https://github.com/NVIDIA/spark-rapids/issues/216)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_timestamps], ids=idfn)
def test_window_aggs_for_ranges_of_dates(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(c) over '
        '   (partition by a order by b asc  '
        '       range between 1 preceding and 1 following) as sum_c_asc '
        'from window_agg_table'
    )
