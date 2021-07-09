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
import math
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import *
from pyspark.sql.types import *
from pyspark.sql.types import NumericType
from pyspark.sql.window import Window
import pyspark.sql.functions as f

def meta_idfn(meta):
    def tmp(something):
        return meta + idfn(something)
    return tmp

_grpkey_longs_with_no_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length=20)),
    ('b', IntegerGen()),
    ('c', IntegerGen())]

_grpkey_longs_with_dates = [
    ('a', RepeatSeqGen(LongGen(), length=2048)),
    ('b', DateGen(nullable=False, start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

_grpkey_longs_with_nullable_dates = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', DateGen(nullable=(True, 5.0), start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

_grpkey_longs_with_timestamps = [
    ('a', RepeatSeqGen(LongGen(), length=2048)),
    ('b', TimestampGen(nullable=False)),
    ('c', IntegerGen())]

_grpkey_longs_with_nullable_timestamps = [
    ('a', RepeatSeqGen(LongGen(nullable=False), length=20)),
    ('b', TimestampGen(nullable=(True, 5.0))),
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
    ('a', RepeatSeqGen(int_gen, length=20)),
    # restrict the values generated by min_val/max_val not to be overflow when calculating
    ('b', ByteGen(nullable=True, min_val=-98, max_val=98, special_cases=[])),
    ('c', IntegerGen())]

_grpkey_short_with_nulls = [
    ('a', RepeatSeqGen(int_gen, length=20)),
    # restrict the values generated by min_val/max_val not to be overflow when calculating
    ('b', ShortGen(nullable=True, min_val=-32700, max_val=32700, special_cases=[])),
    ('c', IntegerGen())]

_grpkey_int_with_nulls = [
    ('a', RepeatSeqGen(int_gen, length=20)),
    # restrict the values generated by min_val/max_val not to be overflow when calculating
    ('b', IntegerGen(nullable=True, min_val=-2147483000, max_val=2147483000, special_cases=[])),
    ('c', IntegerGen())]

_grpkey_long_with_nulls = [
    ('a', RepeatSeqGen(int_gen, length=20)),
    # restrict the values generated by min_val/max_val not to be overflow when calculating
    ('b', LongGen(nullable=True, min_val=-9223372036854775000, max_val=9223372036854775000, special_cases=[])),
    ('c', IntegerGen())]

_grpkey_date_with_nulls = [
    ('a', RepeatSeqGen(int_gen, length=20)),
    ('b', DateGen(nullable=(True, 5.0), start=date(year=2020, month=1, day=1), end=date(year=2020, month=12, day=31))),
    ('c', IntegerGen())]

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

part_and_order_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=1)]

running_part_and_order_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        string_gen, byte_gen, timestamp_gen, DecimalGen(precision=18, scale=1)]

lead_lag_data_gens = [long_gen, DoubleGen(no_nans=True, special_cases=[]),
        boolean_gen, timestamp_gen, DecimalGen(precision=18, scale=3)]


all_basic_gens_no_nans = [byte_gen, short_gen, int_gen, long_gen, 
        FloatGen(no_nans=True, special_cases=[]), DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen]

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

# In a distributed setup the order of the partitions returend might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
@pytest.mark.parametrize('data_gen', [
                                      _grpkey_byte_with_nulls,
                                      _grpkey_short_with_nulls,
                                      _grpkey_int_with_nulls,
                                      _grpkey_long_with_nulls,
                                      _grpkey_date_with_nulls,
                                    ], ids=idfn)
def test_window_aggs_for_range_numeric_date(data_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.window.range.byte.enabled': True,
            'spark.rapids.sql.window.range.short.enabled': True}
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        'window_agg_table',
        'select '
        ' sum(c) over '
        '   (partition by a order by b asc  '
        '      range between 1 preceding and 3 following) as sum_c_asc, '
        ' avg(c) over '
        '   (partition by a order by b asc  '
        '       range between 1 preceding and 3 following) as avg_b_asc, '
        ' max(c) over '
        '   (partition by a order by b asc '
        '       range between 1 preceding and 3 following) as max_b_desc, '
        ' min(c) over '
        '   (partition by a order by b asc  '
        '       range between 1 preceding and 3 following) as min_b_asc, '
        ' count(1) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_1_asc, '
        ' count(c) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_b_asc, '
        ' avg(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as avg_b_unbounded, '
        ' sum(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as sum_b_unbounded, '
        ' max(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and UNBOUNDED following) as max_b_unbounded '
        'from window_agg_table ',
        conf = conf)

# In a distributed setup the order of the partitions returend might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_no_nulls,
                                      _grpkey_longs_with_nulls,
                                      _grpkey_longs_with_dates,
                                      _grpkey_longs_with_nullable_dates,
                                      _grpkey_longs_with_decimals,
                                      _grpkey_longs_with_nullable_decimals,
                                      _grpkey_decimals_with_nulls], ids=idfn)
def test_window_aggs_for_rows(data_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.castFloatToDecimal.enabled': True}
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
        conf = conf)


# This is for aggregations that work with a running window optimization. They don't need to be batched
# specially, but it only works if all of the aggregations can support this.
# the order returned should be consistent because the data ends up in a single task (no partitioning)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches 
@pytest.mark.parametrize('b_gen', all_basic_gens_no_nans + [decimal_gen_scale_precision], ids=meta_idfn('data:'))
def test_window_running_no_part(b_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.hasNans': False,
            'spark.rapids.sql.castFloatToDecimal.enabled': True}
    query_parts = ['row_number() over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as row_num',
            'count(b) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as count_col',
            'min(b) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as min_col',
            'max(b) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as max_col']
    if isinstance(b_gen.data_type, NumericType) and not isinstance(b_gen, FloatGen) and not isinstance(b_gen, DoubleGen):
        query_parts.append('sum(b) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as sum_col')

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : two_col_df(spark, LongRangeGen(), b_gen, length=1024 * 14),
        "window_agg_table",
        'select ' +
        ', '.join(query_parts) +
        ' from window_agg_table ',
        validate_execs_in_gpu_plan = ['GpuRunningWindowExec'],
        conf = conf)

# Test that we can so a running window sum on floats and doubles.  This becomes problematic because we do the agg in parallel
# which means that the result can switch back and forth from Inf to not Inf depending on the order of aggregations.
# We test this by limiting the range of the values in the sum to never hit Inf, and by using abs so we don't have
# positive and negative values that interfere with each other.
# the order returned should be consistent because the data ends up in a single task (no partitioning)
@approximate_float
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches 
def test_running_float_sum_no_part(batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.variableFloatAgg.enabled': True,
            'spark.rapids.sql.castFloatToDecimal.enabled': True}
    query_parts = ['a', 
            'sum(cast(b as double)) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as shrt_dbl_sum',
            'sum(abs(dbl)) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as dbl_sum',
            'sum(cast(b as float)) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as shrt_flt_sum',
            'sum(abs(flt)) over (order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as flt_sum']

    gen = StructGen([('a', LongRangeGen()),('b', short_gen),('flt', float_gen),('dbl', double_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, gen, length=1024 * 14),
        "window_agg_table",
        'select ' +
        ', '.join(query_parts) +
        ' from window_agg_table ',
        validate_execs_in_gpu_plan = ['GpuRunningWindowExec'],
        conf = conf)

# This is for aggregations that work with a running window optimization. They don't need to be batched
# specially, but it only works if all of the aggregations can support this.
# In a distributed setup the order of the partitions returned might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
@pytest.mark.parametrize('b_gen, c_gen', [(long_gen, x) for x in running_part_and_order_gens] +
        [(x, long_gen) for x in all_basic_gens_no_nans + [decimal_gen_scale_precision]], ids=idfn)
def test_window_running(b_gen, c_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.hasNans': False,
            'spark.rapids.sql.variableFloatAgg.enabled': True,
            'spark.rapids.sql.castFloatToDecimal.enabled': True}
    query_parts = ['row_number() over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as row_num',
            'count(c) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as count_col',
            'min(c) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as min_col',
            'max(c) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as max_col']

    # Decimal precision can grow too large. Float and Double can get odd results for Inf/-Inf because of ordering
    if isinstance(c_gen.data_type, NumericType) and (not isinstance(c_gen, FloatGen)) and (not isinstance(c_gen, DoubleGen)) and (not isinstance(c_gen, DecimalGen)):
        query_parts.append('sum(c) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as sum_col')

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : three_col_df(spark, LongRangeGen(), RepeatSeqGen(b_gen, length=100), c_gen, length=1024 * 14),
        "window_agg_table",
        'select ' +
        ', '.join(query_parts) +
        ' from window_agg_table ',
        validate_execs_in_gpu_plan = ['GpuRunningWindowExec'],
        conf = conf)

# Test that we can so a running window sum on floats and doubles and decimal. This becomes problematic because we do the agg in parallel
# which means that the result can switch back and forth from Inf to not Inf depending on the order of aggregations.
# We test this by limiting the range of the values in the sum to never hit Inf, and by using abs so we don't have
# positive and negative values that interfere with each other.
# decimal is problematic if the precision is so high it falls back to the CPU.
# In a distributed setup the order of the partitions returned might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
def test_window_running_float_decimal_sum(batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.variableFloatAgg.enabled': True,
            'spark.rapids.sql.castFloatToDecimal.enabled': True}
    # TODO need a way to insert NaNs...
    query_parts = ['b', 'a', 
            'sum(cast(c as double)) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as dbl_sum',
            'sum(abs(dbl)) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as dbl_sum',
            'sum(cast(c as float)) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as flt_sum',
            'sum(abs(flt)) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as flt_sum',
            'sum(cast(c as Decimal(6,1))) over (partition by b order by a rows between UNBOUNDED PRECEDING AND CURRENT ROW) as dec_sum']

    gen = StructGen([('a', LongRangeGen()),('b', RepeatSeqGen(int_gen, length=1000)),('c', short_gen),('flt', float_gen),('dbl', double_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, gen, length=1024 * 14),
        "window_agg_table",
        'select ' +
        ', '.join(query_parts) +
        ' from window_agg_table ',
        validate_execs_in_gpu_plan = ['GpuRunningWindowExec'],
        conf = conf)

# In a distributed setup the order of the partitions returned might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@approximate_float
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
@pytest.mark.parametrize('c_gen', lead_lag_data_gens, ids=idfn)
@pytest.mark.parametrize('a_b_gen', part_and_order_gens, ids=meta_idfn('partAndOrderBy:'))
def test_multi_types_window_aggs_for_rows_lead_lag(a_b_gen, c_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size,
            'spark.rapids.sql.hasNans': False}
    data_gen = [
            ('a', RepeatSeqGen(a_b_gen, length=20)),
            ('b', a_b_gen),
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
    assert_gpu_and_cpu_are_equal_collect(do_it, conf = conf)


lead_lag_array_data_gens =\
    [ArrayGen(sub_gen, max_length=10) for sub_gen in lead_lag_data_gens] + \
    [ArrayGen(ArrayGen(sub_gen, max_length=10), max_length=10) for sub_gen in lead_lag_data_gens] + \
    [ArrayGen(ArrayGen(ArrayGen(sub_gen, max_length=10), max_length=10), max_length=10) \
        for sub_gen in lead_lag_data_gens]

@ignore_order(local=True)
@pytest.mark.parametrize('batch_size', ['1000', '1g'], ids=idfn) # set the batch size so we can test multiple stream batches
@pytest.mark.parametrize('d_gen', lead_lag_array_data_gens, ids=meta_idfn('agg:'))
@pytest.mark.parametrize('c_gen', [LongRangeGen()], ids=meta_idfn('orderBy:'))
@pytest.mark.parametrize('b_gen', [long_gen], ids=meta_idfn('orderBy:'))
@pytest.mark.parametrize('a_gen', [long_gen], ids=meta_idfn('partBy:'))
def test_window_aggs_for_rows_lead_lag_on_arrays(a_gen, b_gen, c_gen, d_gen, batch_size):
    conf = {'spark.rapids.sql.batchSizeBytes': batch_size}
    data_gen = [
            ('a', RepeatSeqGen(a_gen, length=20)),
            ('b', b_gen),
            ('c', c_gen),
            ('d', d_gen),
            ('d_default', d_gen)]

    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        '''
        SELECT
            LEAD(d, 5) OVER (PARTITION by a ORDER BY b,c) lead_d_5,
            LEAD(d, 2, d_default) OVER (PARTITION by a ORDER BY b,c) lead_d_2_default,
            LAG(d, 5) OVER (PARTITION by a ORDER BY b,c) lag_d_5,
            LAG(d, 2, d_default) OVER (PARTITION by a ORDER BY b,c) lag_d_2_default
        FROM window_agg_table
        ''',
        conf = conf)


# lead and lag don't currently work for string columns, so redo the tests, but just for strings
# without lead and lag
# In a distributed setup the order of the partitions returned might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@approximate_float
@pytest.mark.parametrize('c_gen', [string_gen], ids=idfn)
@pytest.mark.parametrize('a_b_gen', part_and_order_gens, ids=meta_idfn('partAndOrderBy:'))
def test_multi_types_window_aggs_for_rows(a_b_gen, c_gen):
    data_gen = [
            ('a', RepeatSeqGen(a_b_gen, length=20)),
            ('b', a_b_gen),
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
# In a distributed setup the order of the partitions returned might be different, so we must ignore the order
# but small batch sizes can make sort very slow, so do the final order by locally
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [_grpkey_longs_with_timestamps,
                                      pytest.param(_grpkey_longs_with_nullable_timestamps)],
                                      ids=idfn)
def test_window_aggs_for_ranges_timestamps(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, data_gen, length=2048),
        "window_agg_table",
        'select '
        ' sum(c) over '
        '   (partition by a order by b asc  '
        '       range between interval 1 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND preceding '
        '             and interval 1 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND following) as sum_c_asc, '
        ' avg(c) over '
        '   (partition by a order by b asc  '
        '       range between interval 1 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND preceding '
        '            and interval 1 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND following) as avg_c_asc, '
        ' max(c) over '
        '   (partition by a order by b desc '
        '       range between interval 2 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND preceding '
        '            and interval 1 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND following) as max_c_desc, '
        ' min(c) over '
        '   (partition by a order by b asc  '
        '       range between interval 2 DAY 5 HOUR 3 MINUTE 2 SECOND 1 MILLISECOND 5 MICROSECOND preceding '
        '            and current row) as min_c_asc, '
        ' count(1) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_1_asc, '
        ' count(c) over '
        '   (partition by a order by b asc  '
        '       range between  CURRENT ROW and UNBOUNDED following) as count_c_asc, '
        ' avg(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as avg_c_unbounded, '
        ' sum(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and CURRENT ROW) as sum_c_unbounded, '
        ' max(c) over '
        '   (partition by a order by b asc  '
        '       range between UNBOUNDED preceding and UNBOUNDED following) as max_c_unbounded '
        'from window_agg_table',
        conf = {'spark.rapids.sql.castFloatToDecimal.enabled': True})


_gen_data_for_collect_list = [
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', IntegerGen()),
    ('c_bool', BooleanGen()),
    ('c_short', ShortGen()),
    ('c_int', IntegerGen()),
    ('c_long', LongGen()),
    ('c_date', DateGen()),
    ('c_ts', TimestampGen()),
    ('c_byte', ByteGen()),
    ('c_string', StringGen()),
    ('c_float', FloatGen()),
    ('c_double', DoubleGen()),
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
        lambda spark : gen_df(spark, _gen_data_for_collect_list),
        "window_collect_table",
        '''
        select
          collect_list(c_bool) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_bool,
          collect_list(c_short) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_short,
          collect_list(c_int) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_int,
          collect_list(c_long) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_long,
          collect_list(c_date) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_date,
          collect_list(c_ts) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_ts,
          collect_list(c_byte) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_byte,
          collect_list(c_string) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_string,
          collect_list(c_float) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_float,
          collect_list(c_double) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_double,
          collect_list(c_decimal) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_decimal,
          collect_list(c_struct) over
            (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as collect_struct
        from window_collect_table
        ''')


# SortExec does not support array type, so sort the result locally.
@ignore_order(local=True)
# This test is more directed at Databricks and their running window optimization instead of ours
# this is why we do not validate that we inserted in a GpuRunningWindowExec, yet.
def test_running_window_function_exec_for_all_aggs():
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, _gen_data_for_collect_list),
        "window_collect_table",
        '''
        select
          sum(c_int) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as sum_int,
          min(c_long) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as min_long,
          max(c_date) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as max_date,
          count(1) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as count_1,
          count(*) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as count_star,
          row_number() over
            (partition by a order by b,c_int) as row_num,
          collect_list(c_float) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as collect_float,
          collect_list(c_decimal) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as collect_decimal,
          collect_list(c_struct) over
            (partition by a order by b,c_int rows between UNBOUNDED PRECEDING AND CURRENT ROW) as collect_struct
        from window_collect_table
        ''')

# Generates some repeated values to test the deduplication of GpuCollectSet.
# And GpuCollectSet does not yet support struct type.
_gen_data_for_collect_set = [
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', LongRangeGen()),
    ('c_bool', RepeatSeqGen(BooleanGen(), length=15)),
    ('c_int', RepeatSeqGen(IntegerGen(), length=15)),
    ('c_long', RepeatSeqGen(LongGen(), length=15)),
    ('c_short', RepeatSeqGen(ShortGen(), length=15)),
    ('c_date', RepeatSeqGen(DateGen(), length=15)),
    ('c_timestamp', RepeatSeqGen(TimestampGen(), length=15)),
    ('c_byte', RepeatSeqGen(ByteGen(), length=15)),
    ('c_string', RepeatSeqGen(StringGen(), length=15)),
    ('c_float', RepeatSeqGen(FloatGen(), length=15)),
    ('c_double', RepeatSeqGen(DoubleGen(), length=15)),
    ('c_decimal', RepeatSeqGen(DecimalGen(precision=8, scale=3), length=15)),
    # case to verify the NAN_UNEQUAL strategy
    ('c_fp_nan', RepeatSeqGen(FloatGen().with_special_case(math.nan, 200.0), length=5)),
]


# SortExec does not support array type, so sort the result locally.
@ignore_order(local=True)
def test_window_aggs_for_rows_collect_set():
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark: gen_df(spark, _gen_data_for_collect_set),
        "window_collect_table",
        '''
        select a, b,
            sort_array(cc_bool),
            sort_array(cc_int),
            sort_array(cc_long),
            sort_array(cc_short),
            sort_array(cc_date),
            sort_array(cc_ts),
            sort_array(cc_byte),
            sort_array(cc_str),
            sort_array(cc_float),
            sort_array(cc_double),
            sort_array(cc_decimal),
            sort_array(cc_fp_nan)
        from (
            select a, b,
              collect_set(c_bool) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_bool,
              collect_set(c_int) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_int,
              collect_set(c_long) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_long,
              collect_set(c_short) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_short,
              collect_set(c_date) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_date,
              collect_set(c_timestamp) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_ts,
              collect_set(c_byte) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_byte,
              collect_set(c_string) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_str,
              collect_set(c_float) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_float,
              collect_set(c_double) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_double,
              collect_set(c_decimal) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_decimal,
              collect_set(c_fp_nan) over
                (partition by a order by b,c_int rows between CURRENT ROW and UNBOUNDED FOLLOWING) as cc_fp_nan
            from window_collect_table
        ) t
        ''')
