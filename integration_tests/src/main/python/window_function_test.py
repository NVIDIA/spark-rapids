# Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

_grpkey_byte_with_nulls = [
    ('a', IntegerGen()),
    ('b', ByteGen(nullable=True, min_val=-98, max_val=98, special_cases=[]))]

_grpkey_short_with_nulls = [
    ('a', IntegerGen()),
    ('b', ShortGen(nullable=True, min_val=-32700, max_val=32700, special_cases=[]))]

_grpkey_int_with_nulls = [
    ('a', IntegerGen()),
    ('b', IntegerGen(nullable=True, min_val=-2147483000, max_val=2147483000, special_cases=[]))]

_grpkey_long_with_nulls = [
    ('a', IntegerGen()),
    ('b', LongGen(nullable=True, min_val=-9223372036854775000, max_val=9223372036854775000, special_cases=[]))]

_grpkey_byte_with_nulls_with_overflow = [
    ('a', IntegerGen()),
    ('b', ByteGen(nullable=True))]

_grpkey_short_with_nulls_with_overflow = [
    ('a', IntegerGen()),
    ('b', ShortGen(nullable=True))]

_grpkey_int_with_nulls_with_overflow = [
    ('a', IntegerGen()),
    ('b', IntegerGen(nullable=True))]

_grpkey_long_with_nulls_with_overflow = [
    ('a', IntegerGen()),
    ('b', LongGen(nullable=True))]


@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over order by byte column overflow "
                          "(https://github.com/NVIDIA/spark-rapids/pull/2020#issuecomment-838127070)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_byte_with_nulls_with_overflow], ids=idfn)
def test_window_aggs_for_ranges_numeric_byte_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '      range between 127 preceding and 127 following) as sum_c_asc, '
        'from window_agg_table',
        conf={'spark.rapids.sql.window.range.byte.enabled': True})


@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over order by short column overflow "
                          "(https://github.com/NVIDIA/spark-rapids/pull/2020#issuecomment-838127070)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_short_with_nulls_with_overflow], ids=idfn)
def test_window_aggs_for_ranges_numeric_short_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '      range between 32767 preceding and 32767 following) as sum_c_asc, '
        'from window_agg_table',
        conf={'spark.rapids.sql.window.range.short.enabled': True})


@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over order by int column overflow "
                          "(https://github.com/NVIDIA/spark-rapids/pull/2020#issuecomment-838127070)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_int_with_nulls_with_overflow], ids=idfn)
def test_window_aggs_for_ranges_numeric_int_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '      range between 2147483647 preceding and 2147483647 following) as sum_c_asc, '
        'from window_agg_table')


@pytest.mark.xfail(reason="[UNSUPPORTED] Ranges over order by long column overflow "
                          "(https://github.com/NVIDIA/spark-rapids/pull/2020#issuecomment-838127070)")
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_long_with_nulls_with_overflow], ids=idfn)
def test_window_aggs_for_ranges_numeric_long_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '      range between 9223372036854775807 preceding and 9223372036854775807 following) as sum_c_asc, '
        'from window_agg_table')


@ignore_order
@pytest.mark.parametrize('data_gen', [
                                      _grpkey_byte_with_nulls,
                                      _grpkey_short_with_nulls,
                                      _grpkey_int_with_nulls,
                                      _grpkey_long_with_nulls
                                    ], ids=idfn)
def test_window_aggs_for_range_numeric(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '      range between 1 preceding and 3 following) as sum_c_asc, '
        ' avg(b) over '
        '   (partition by a order by b asc  '
        '       range between 1 preceding and 3 following) as avg_b_asc, '
        ' max(b) over '
        '   (partition by a order by b asc '
        '       range between 1 preceding and 3 following) as max_b_desc, '
        ' min(b) over '
        '   (partition by a order by b asc  '
        '       range between 1 preceding and 3 following) as min_b_asc, '
        ' count(1) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_1_asc, '
        ' count(b) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_b_asc, '
        ' avg(b) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as avg_b_unbounded, '
        ' sum(b) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as sum_b_unbounded, '
        ' max(b) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and UNBOUNDED following) as max_b_unbounded '
        'from window_agg_table ',
        conf={'spark.rapids.sql.window.range.byte.enabled': True,
              'spark.rapids.sql.window.range.short.enabled': True})


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
        ' avg(c) over '
        '   (partition by a order by b,c rows between UNBOUNDED preceding and UNBOUNDED following) as avg_c, '
        ' row_number() over '
        '   (partition by a order by b,c rows between UNBOUNDED preceding and CURRENT ROW) as row_num '
        'from window_agg_table ',
        conf = {'spark.rapids.sql.castFloatToDecimal.enabled': True})


part_and_order_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=1)]

lead_lag_data_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=3)]

def meta_idfn(meta):
    def tmp(something):
        return meta + idfn(something)
    return tmp

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
        ' avg(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between interval 1 day preceding and interval 1 day following) as avg_c_asc, '
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
        ' avg(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as avg_c_unbounded, '
        ' sum(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as sum_c_unbounded, '
        ' max(c) over '
        '   (partition by a order by cast(b as timestamp) asc  '
        '       range between UNBOUNDED preceding and UNBOUNDED following) as max_c_unbounded '
        'from window_agg_table',
        conf = {'spark.rapids.sql.castFloatToDecimal.enabled': True})

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

_gen_data_for_collect = [
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', IntegerGen()),
    ('c_int', IntegerGen()),
    ('c_long', LongGen()),
    ('c_time', DateGen()),
    ('c_string', StringGen()),
    ('c_float', FloatGen()),
    ('c_decimal', DecimalGen(precision=8, scale=3)),
    ('c_struct', StructGen(children=[
        ['child_int', IntegerGen()],
        ['child_time', DateGen()],
        ['child_string', StringGen()],
        ['child_decimal', DecimalGen(precision=8, scale=3)]]))]


# SortExec does not support array type, so sort the result locally.
@ignore_order(local=True)
def test_window_aggs_for_rows_collect_list():
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, _gen_data_for_collect),
        "window_collect_table",
        '''
        select
          collect_list(c_int) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_int,
          collect_list(c_long) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_long,
          collect_list(c_time) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_time,
          collect_list(c_string) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_string,
          collect_list(c_float) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_float,
          collect_list(c_decimal) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_decimal,
          collect_list(c_struct) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_struct
        from window_collect_table
        ''')
