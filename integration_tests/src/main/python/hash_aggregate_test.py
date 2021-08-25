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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql,\
    assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_sql_with_capture,\
    assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from functools import reduce
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import is_before_spark_311, with_cpu_session

_no_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.hasNans': 'false',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'
                      }

_no_nans_float_smallbatch_conf = _no_nans_float_conf.copy()
_no_nans_float_smallbatch_conf.update(
    {'spark.rapids.sql.batchSizeBytes' : '1000'})

_no_nans_float_conf_partial = _no_nans_float_conf.copy()
_no_nans_float_conf_partial.update(
    {'spark.rapids.sql.hashAgg.replaceMode': 'partial'})

_no_nans_float_conf_final = _no_nans_float_conf.copy()
_no_nans_float_conf_final.update({'spark.rapids.sql.hashAgg.replaceMode': 'final'})

_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                    'spark.rapids.sql.hasNans': 'true',
                    'spark.rapids.sql.castStringToFloat.enabled': 'true'
                   }

_nans_float_conf_partial = _nans_float_conf.copy()
_nans_float_conf_partial.update(
    {'spark.rapids.sql.hashAgg.replaceMode': 'partial'})

_nans_float_conf_final = _nans_float_conf.copy()
_nans_float_conf_final.update({'spark.rapids.sql.hashAgg.replaceMode': 'final'})

# The input lists or schemas that are used by StructGen.

# grouping longs with nulls
_longs_with_nulls = [('a', LongGen()), ('b', IntegerGen()), ('c', LongGen())]
# grouping longs with no nulls
_longs_with_no_nulls = [
    ('a', LongGen(nullable=False)),
    ('b', IntegerGen(nullable=False)),
    ('c', LongGen(nullable=False))]
# grouping longs with nulls present
_grpkey_longs_with_nulls = [
    ('a', RepeatSeqGen(LongGen(nullable=(True, 10.0)), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping doubles with nulls present
_grpkey_dbls_with_nulls = [
    ('a', RepeatSeqGen(DoubleGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping floats with nulls present
_grpkey_floats_with_nulls = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0), special_cases=[]), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping strings with nulls present
_grpkey_strings_with_nulls = [
    ('a', RepeatSeqGen(StringGen(pattern='[0-9]{0,30}'), length= 20)),
    ('b', IntegerGen()),
    ('c', LongGen())]
# grouping strings with nulls present, and null value
_grpkey_strings_with_extra_nulls = [
    ('a', RepeatSeqGen(StringGen(pattern='[0-9]{0,30}'), length= 20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping NullType
_grpkey_nulls = [
    ('a', NullGen()),
    ('b', IntegerGen()),
    ('c', LongGen())]

# grouping floats with other columns containing nans and nulls
_grpkey_floats_with_nulls_and_nans = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0)), length= 20)),
    ('b', FloatGen(nullable=(True, 10.0), special_cases=[(float('nan'), 10.0)])),
    ('c', LongGen())]

_nan_zero_float_special_cases = [
    (float('nan'),  5.0),
    (NEG_FLOAT_NAN_MIN_VALUE, 5.0),
    (NEG_FLOAT_NAN_MAX_VALUE, 5.0),
    (POS_FLOAT_NAN_MIN_VALUE, 5.0),
    (POS_FLOAT_NAN_MAX_VALUE, 5.0),
    (float('0.0'),  5.0),
    (float('-0.0'), 5.0),
]

_grpkey_floats_with_nan_zero_grouping_keys = [
    ('a', RepeatSeqGen(FloatGen(nullable=(True, 10.0), special_cases=_nan_zero_float_special_cases), length=50)),
    ('b', IntegerGen(nullable=(True, 10.0))),
    ('c', LongGen())]

_nan_zero_double_special_cases = [
    (float('nan'),  5.0),
    (NEG_DOUBLE_NAN_MIN_VALUE, 5.0),
    (NEG_DOUBLE_NAN_MAX_VALUE, 5.0),
    (POS_DOUBLE_NAN_MIN_VALUE, 5.0),
    (POS_DOUBLE_NAN_MAX_VALUE, 5.0),
    (float('0.0'),  5.0),
    (float('-0.0'), 5.0),
]

_grpkey_doubles_with_nan_zero_grouping_keys = [
    ('a', RepeatSeqGen(DoubleGen(nullable=(True, 10.0), special_cases=_nan_zero_double_special_cases), length=50)),
    ('b', FloatGen(nullable=(True, 10.0))),
    ('c', LongGen())]

# Schema for xfail cases
struct_gens_xfail = [
    _grpkey_floats_with_nulls_and_nans
]

# List of schemas with no NaNs
_init_list_no_nans = [
    _longs_with_nulls,
    _longs_with_no_nulls,
    _grpkey_longs_with_nulls,
    _grpkey_dbls_with_nulls,
    _grpkey_floats_with_nulls,
    _grpkey_strings_with_nulls,
    _grpkey_nulls,
    _grpkey_strings_with_extra_nulls]

# List of schemas with NaNs included
_init_list_with_nans_and_no_nans = [
    _longs_with_nulls,
    _longs_with_no_nulls,
    _grpkey_longs_with_nulls,
    _grpkey_dbls_with_nulls,
    _grpkey_floats_with_nulls,
    _grpkey_strings_with_nulls,
    _grpkey_floats_with_nulls_and_nans]


def get_params(init_list, marked_params=[]):
    """
    A method to build the test inputs along with their passed in markers to allow testing
    specific params with their relevant markers. Right now it is used to parametrize _confs with
    allow_non_gpu which allows some operators to be enabled.
    However, this can be used with any list of params to the test.
    :arg init_list list of param values to be tested
    :arg marked_params A list of tuples of (params, list of pytest markers)
    Look at params_markers_for_confs as an example.
    """
    list = init_list.copy()
    for index in range(0, len(list)):
        for test_case, marks in marked_params:
            if list[index] == test_case:
                list[index] = pytest.param(list[index], marks=marks)
    return list


# Run these tests with in 3 modes, all on the GPU, only partial aggregates on GPU and
# only final aggregates on the GPU with conf for spark.rapids.sql.hasNans set to false/true
_confs = [_no_nans_float_conf, _no_nans_float_smallbatch_conf, _no_nans_float_conf_final, _no_nans_float_conf_partial]
_confs_with_nans = [_nans_float_conf, _nans_float_conf_partial, _nans_float_conf_final]

# Pytest marker for list of operators allowed to run on the CPU,
# esp. useful in partial and final only modes.
# but this ends up allowing close to everything being off the GPU so I am not sure how
# useful this really is
_excluded_operators_marker = pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression', 'UnscaledValue', 'MakeDecimal',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'KnownFloatingPointNormalized', 'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec', 'Coalesce', 'IsNull', 'EqualNullSafe',
    'PivotFirst', 'GetArrayItem', 'ShuffleExchangeExec', 'HashPartitioning')

params_markers_for_confs = [
    (_no_nans_float_conf_partial, [_excluded_operators_marker]),
    (_no_nans_float_conf_final, [_excluded_operators_marker])
]

params_markers_for_confs_nans = [
    (_nans_float_conf_partial, [_excluded_operators_marker]),
    (_nans_float_conf_final, [_excluded_operators_marker]),
    (_nans_float_conf, [_excluded_operators_marker])
]

_grpkey_small_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=7, scale=3, nullable=(True, 10.0)), length=50)),
    ('b', DecimalGen(precision=5, scale=2)),
    ('c', DecimalGen(precision=8, scale=3))]

_init_list_no_nans_with_decimal = _init_list_no_nans + [
    _grpkey_small_decimals]

@shuffle_test
@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans_with_decimal, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_sum(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b')),
        conf=conf
    )


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_avg(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.avg('b')),
        conf=conf
    )

@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_hash_grpby_avg_nulls(data_gen, conf, ansi_enabled):
    conf.update({'spark.sql.ansi.enabled': ansi_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a')
          .agg(f.avg('c')),
        conf=conf
    )

@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_hash_reduction_avg_nulls(data_gen, conf, ansi_enabled):
    conf.update({'spark.sql.ansi.enabled': ansi_enabled})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
          .agg(f.avg('c')),
        conf=conf
    )

# tracks https://github.com/NVIDIA/spark-rapids/issues/154
@approximate_float
@ignore_order
@incompat
@pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'KnownFloatingPointNormalized', 'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec')
@pytest.mark.parametrize('data_gen', [
    StructGen(children=[('a', int_gen), ('b', int_gen)],nullable=False,
        special_cases=[((None, None), 400.0), ((None, -1542301795), 100.0)])], ids=idfn)
def test_hash_avg_nulls_partial_only(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=2).agg(f.avg('b')),
        conf=_no_nans_float_conf_partial
    )


@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans_with_decimal, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_pivot(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b')
            .agg(f.sum('c')),
        conf=conf)

@approximate_float
@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'PivotFirst', 'AggregateExpression', 'Alias', 'GetArrayItem',
        'Literal', 'ShuffleExchangeExec', 'HashPartitioning', 'KnownFloatingPointNormalized',
        'NormalizeNaNAndZero')
@incompat
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nulls_and_nans], ids=idfn)
def test_hash_pivot_groupby_nan_fallback(data_gen):
    # collect values to pivot in previous, to avoid this preparation job being captured
    def fetch_pivot_values(spark):
        max_values = spark._jsparkSession.sessionState().conf().dataFramePivotMaxValues()
        df = gen_df(spark, data_gen, length=100)
        df = df.select('b').distinct().limit(max_values + 1).sort('b')
        return [row[0] for row in df.collect()]

    pivot_values = with_cpu_session(fetch_pivot_values)

    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b', pivot_values)
            .agg(f.sum('c')),
        "PivotFirst",
        conf=_nans_float_conf)


@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs_with_nans, params_markers_for_confs_nans), ids=idfn)
def test_hash_grpby_pivot_without_nans(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b')
            .agg(f.sum('c')),
        conf=conf)


@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_multiple_grpby_pivot(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a','b')
            .pivot('b')
            .agg(f.sum('c'), f.max('c')),
        conf=conf)

@approximate_float
@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'PivotFirst', 'AggregateExpression', 'Alias', 'GetArrayItem',
        'Literal', 'ShuffleExchangeExec')
@incompat
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nulls_and_nans], ids=idfn)
def test_hash_pivot_reduction_nan_fallback(data_gen):
    # collect values to pivot in previous, to avoid this preparation job being captured
    def fetch_pivot_values(spark):
        max_values = spark._jsparkSession.sessionState().conf().dataFramePivotMaxValues()
        df = gen_df(spark, data_gen, length=100)
        df = df.select('b').distinct().limit(max_values + 1).sort('b')
        return [row[0] for row in df.collect()]

    pivot_values = with_cpu_session(fetch_pivot_values)

    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby()
            .pivot('b', pivot_values)
            .agg(f.sum('c')),
        "PivotFirst",
        conf=_nans_float_conf)

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs_with_nans, params_markers_for_confs_nans), ids=idfn)
def test_hash_reduction_pivot_without_nans(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby()
            .pivot('b')
            .agg(f.sum('c')),
        conf=conf)

_repeat_agg_column_for_collect_op = [
    RepeatSeqGen(BooleanGen(), length=15),
    RepeatSeqGen(IntegerGen(), length=15),
    RepeatSeqGen(LongGen(), length=15),
    RepeatSeqGen(ShortGen(), length=15),
    RepeatSeqGen(DateGen(), length=15),
    RepeatSeqGen(TimestampGen(), length=15),
    RepeatSeqGen(ByteGen(), length=15),
    RepeatSeqGen(StringGen(), length=15),
    RepeatSeqGen(FloatGen(), length=15),
    RepeatSeqGen(DoubleGen(), length=15),
    RepeatSeqGen(DecimalGen(precision=8, scale=3), length=15),
    # case to verify the NAN_UNEQUAL strategy
    RepeatSeqGen(FloatGen().with_special_case(math.nan, 200.0), length=5),
]

_gen_data_for_collect_op = [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen),
    ('c', LongRangeGen())] for value_gen in _repeat_agg_column_for_collect_op
]

_repeat_agg_column_for_collect_list_op = [
        RepeatSeqGen(ArrayGen(int_gen), length=15),
        RepeatSeqGen(all_basic_struct_gen, length=15),
        RepeatSeqGen(simple_string_to_string_map_gen, length=15)
]

_gen_data_for_collect_list_op = _gen_data_for_collect_op + [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen),
    ('c', LongRangeGen())] for value_gen in _repeat_agg_column_for_collect_list_op
]

# to avoid ordering issues with collect_list we do it all in a single task
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
@pytest.mark.parametrize('use_obj_hash_agg', [True, False], ids=idfn)
def test_hash_groupby_collect_list(data_gen, use_obj_hash_agg):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).coalesce(1)
            .groupby('a')
            .agg(f.collect_list('b')),
        conf={'spark.sql.execution.useObjectHashAggregateExec': str(use_obj_hash_agg).lower(),
            'spark.sql.shuffle.partitons': '1'})

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_collect_set(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_set('b')), f.count('b')))

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_collect_with_single_distinct(data_gen):
    # test collect_ops with other distinct aggregations
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')),
                 f.sort_array(f.collect_set('b')),
                 f.countDistinct('c'),
                 f.count('c')))

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_single_distinct_collect(data_gen):
    # test distinct collect
    sql = """select a,
                    sort_array(collect_list(distinct b)),
                    sort_array(collect_set(distinct b))
            from tbl group by a"""
    assert_gpu_and_cpu_are_equal_sql(
        df_fun=lambda spark: gen_df(spark, data_gen, length=100),
        table_name="tbl", sql=sql)

    # test distinct collect with nonDistinct aggregations
    sql = """select a,
                    sort_array(collect_list(distinct b)),
                    sort_array(collect_set(b)),
                    count(distinct b),
                    count(c)
            from tbl group by a"""
    assert_gpu_and_cpu_are_equal_sql(
        df_fun=lambda spark: gen_df(spark, data_gen, length=100),
        table_name="tbl", sql=sql)

@approximate_float
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_collect_with_multi_distinct(data_gen):
    def spark_fn(spark_session):
        return gen_df(spark_session, data_gen, length=100).groupby('a').agg(
            f.sort_array(f.collect_list('b')),
            f.sort_array(f.collect_set('b')),
            f.countDistinct('b'),
            f.countDistinct('c'))
    assert_gpu_and_cpu_are_equal_collect(spark_fn)

@approximate_float
@ignore_order(local=True)
@allow_non_gpu('ObjectHashAggregateExec', 'ShuffleExchangeExec',
               'HashPartitioning', 'SortArray', 'Alias', 'Literal',
               'Count', 'CollectList', 'CollectSet', 'AggregateExpression')
@incompat
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_op, ids=idfn)
@pytest.mark.parametrize('conf', [_nans_float_conf_partial, _nans_float_conf_final], ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['true', 'false'], ids=idfn)
def test_hash_groupby_collect_partial_replace_fallback(data_gen, conf, aqe_enabled):
    local_conf = conf.copy()
    local_conf.update({'spark.sql.adaptive.enabled': aqe_enabled})
    # test without Distinct
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')), f.sort_array(f.collect_set('b'))),
        exist_classes='CollectList,CollectSet',
        non_exist_classes='GpuCollectList,GpuCollectSet',
        conf=local_conf)
    # test with single Distinct
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')),
                 f.sort_array(f.collect_set('b')),
                 f.countDistinct('c'),
                 f.count('c')),
        exist_classes='CollectList,CollectSet',
        non_exist_classes='GpuCollectList,GpuCollectSet',
        conf=local_conf)

@ignore_order(local=True)
@allow_non_gpu('ObjectHashAggregateExec', 'ShuffleExchangeExec', 'HashAggregateExec',
               'HashPartitioning', 'SortArray', 'Alias', 'Literal',
               'CollectList', 'CollectSet', 'Max', 'AggregateExpression')
@pytest.mark.parametrize('conf', [_nans_float_conf_final, _nans_float_conf_partial], ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['true', 'false'], ids=idfn)
def test_hash_groupby_collect_partial_replace_fallback_with_other_agg(conf, aqe_enabled):
    # This test is to ensure "associated fallback" will not affect another Aggregate plans.
    local_conf = conf.copy()
    local_conf.update({'spark.sql.adaptive.enabled': aqe_enabled})

    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: gen_df(spark, [('k1', RepeatSeqGen(LongGen(), length=20)),
                                     ('k2', RepeatSeqGen(LongGen(), length=20)),
                                     ('v', LongRangeGen())], length=100),
        exist_classes='GpuMax,Max,CollectList,CollectSet',
        non_exist_classes='GpuObjectHashAggregateExec,GpuCollectList,GpuCollectSet',
        table_name='table',
        sql="""
    select k1,
        sort_array(collect_set(k2)),
        sort_array(collect_list(max_v))
    from
        (select k1, k2,
            max(v) as max_v
        from table group by k1, k2
        )t
    group by k1""",
        conf=local_conf)

@ignore_order(local=True)
@allow_non_gpu('ObjectHashAggregateExec', 'ShuffleExchangeExec',
               'HashAggregateExec', 'HashPartitioning',
               'ApproximatePercentile', 'Alias', 'Literal', 'AggregateExpression')
def test_hash_groupby_typed_imperative_agg_without_gpu_implementation_fallback():
    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(LongGen(), length=20)),
                                     ('v', LongRangeGen())], length=100),
        exist_classes='ApproximatePercentile,ObjectHashAggregateExec',
        non_exist_classes='GpuApproximatePercentile,GpuObjectHashAggregateExec',
        table_name='table',
        sql="""select k,
        approx_percentile(v, array(0.25, 0.5, 0.75)) from table group by k""")

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_multiple_mode_query(data_gen, conf):
    print_params(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.count('a'),
                 f.avg('b'),
                 f.avg('a'),
                 f.countDistinct('b'),
                 f.sum('a'),
                 f.min('a'),
                 f.max('a'),
                 f.sumDistinct('b'),
                 f.countDistinct('c')
                ), conf=conf)


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs),
    ids=idfn)
def test_hash_multiple_mode_query_avg_distincts(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .selectExpr('avg(distinct a)', 'avg(distinct b)','avg(distinct c)'),
        conf=conf)


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
@pytest.mark.parametrize('parameterless', ['true', pytest.param('false', marks=pytest.mark.xfail(
    condition=not is_before_spark_311(), reason="parameterless count not supported by default in Spark 3.1+"))])
def test_hash_query_multiple_distincts_with_non_distinct(data_gen, conf, parameterless):
    conf.update({'spark.sql.legacy.allowParameterlessCount': parameterless})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select avg(a),' +
        'avg(distinct b),' +
        'avg(distinct c),' +
        'sum(distinct a),' +
        'count(distinct b),' +
        'count(a),' +
        'count(),' +
        'sum(a),' +
        'min(a),'+
        'max(a) from hash_agg_table group by a',
        conf)


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs),
                         ids=idfn)
@pytest.mark.parametrize('parameterless', ['true', pytest.param('false', marks=pytest.mark.xfail(
    condition=not is_before_spark_311(), reason="parameterless count not supported by default in Spark 3.1+"))])
def test_hash_query_max_with_multiple_distincts(data_gen, conf, parameterless):
    conf.update({'spark.sql.legacy.allowParameterlessCount': parameterless})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select max(c),' +
        'sum(distinct a),' +
        'count(),' +
        'count(distinct b) from hash_agg_table group by a',
        conf)

@ignore_order
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_count_with_filter(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .selectExpr('count(a) filter (where c > 50)'),
        conf=conf)


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_multiple_filters(data_gen, conf):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select count(a) filter (where c > 50),' +
        'count(b) filter (where c > 100),' +
        'avg(b) filter (where b > 20),' +
        'min(a), max(b) filter (where c > 250) from hash_agg_table group by a',
        conf)


@ignore_order
@allow_non_gpu('HashAggregateExec', 'AggregateExpression', 'AttributeReference', 'Alias', 'Max',
               'KnownFloatingPointNormalized', 'NormalizeNaNAndZero', 'ShuffleExchangeExec',
               'HashPartitioning')
@pytest.mark.parametrize('data_gen', struct_gens_xfail, ids=idfn)
def test_hash_query_max_nan_fallback(data_gen):
    print_params(data_gen)
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.max('b')),
        "Max")

@approximate_float
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nan_zero_grouping_keys,
                                      _grpkey_doubles_with_nan_zero_grouping_keys], ids=idfn)
@pytest.mark.parametrize('parameterless', ['true', pytest.param('false', marks=pytest.mark.xfail(
    condition=not is_before_spark_311(), reason="parameterless count not supported by default in Spark 3.1+"))])
def test_hash_agg_with_nan_keys(data_gen, parameterless):
    _no_nans_float_conf.update({'spark.sql.legacy.allowParameterlessCount': parameterless})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1024),
        "hash_agg_table",
        'select a, '
        'count(*) as count_stars, '
        'count() as count_parameterless, '
        'count(b) as count_bees, '
        'sum(b) as sum_of_bees, '
        'max(c) as max_seas, '
        'min(c) as min_seas, '
        'count(distinct c) as count_distinct_cees, '
        'avg(c) as average_seas '
        'from hash_agg_table group by a',
        _no_nans_float_conf)


@approximate_float
@ignore_order
@pytest.mark.parametrize('data_gen', [ _grpkey_doubles_with_nan_zero_grouping_keys], ids=idfn)
def test_count_distinct_with_nan_floats(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1024),
        "hash_agg_table",
        'select a, count(distinct b) as count_distinct_bees from hash_agg_table group by a',
        _no_nans_float_conf)

# TODO: Literal tests

# REDUCTIONS

non_nan_all_basic_gens = [byte_gen, short_gen, int_gen, long_gen,
        # nans and -0.0 cannot work because of nan support in min/max, -0.0 == 0.0 in cudf for distinct and
        # Spark fixed ordering of 0.0 and -0.0 in Spark 3.1 in the ordering
        FloatGen(no_nans=True, special_cases=[]), DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, date_gen, timestamp_gen]

_nested_gens = array_gens_sample + struct_gens_sample + map_gens_sample

@pytest.mark.parametrize('data_gen', decimal_gens, ids=idfn)
def test_first_last_reductions_extra_types(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # Coalesce and sort are to make sure that first and last, which are non-deterministic
            # become deterministic
            lambda spark : unary_op_df(spark, data_gen)\
                    .coalesce(1).selectExpr(
                'first(a)',
                'last(a)'),
            conf = allow_negative_scale_of_decimal_conf)

# TODO: https://github.com/NVIDIA/spark-rapids/issues/3221
@allow_non_gpu('HashAggregateExec', 'SortAggregateExec',
               'ShuffleExchangeExec', 'HashPartitioning',
               'AggregateExpression', 'Alias', 'First', 'Last')
@pytest.mark.parametrize('data_gen', _nested_gens, ids=idfn)
def test_first_last_reductions_nested_types_fallback(data_gen):
    assert_cpu_and_gpu_are_equal_collect_with_capture(
            lambda spark: unary_op_df(spark, data_gen, num_slices=1)\
                    .selectExpr('first(a)', 'last(a)', 'first(a, True)', 'last(a, True)'),
            exist_classes='First,Last',
            non_exist_classes='GpuFirst,GpuLast',
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', non_nan_all_basic_gens, ids=idfn)
@pytest.mark.parametrize('parameterless', ['true', pytest.param('false', marks=pytest.mark.xfail(
    condition=not is_before_spark_311(), reason="parameterless count not supported by default in Spark 3.1+"))])
def test_generic_reductions(data_gen, parameterless):
    _no_nans_float_conf.update({'spark.sql.legacy.allowParameterlessCount': parameterless})
    assert_gpu_and_cpu_are_equal_collect(
            # Coalesce and sort are to make sure that first and last, which are non-deterministic
            # become deterministic
            lambda spark : unary_op_df(spark, data_gen)\
                    .coalesce(1).selectExpr(
                'min(a)',
                'max(a)',
                'first(a)',
                'last(a)',
                'count(a)',
                'count()',
                'count(1)'),
            conf = _no_nans_float_conf)

@pytest.mark.parametrize('data_gen', non_nan_all_basic_gens, ids=idfn)
@pytest.mark.parametrize('parameterless', ['true', pytest.param('false', marks=pytest.mark.xfail(
    condition=not is_before_spark_311(), reason="parameterless count not supported by default in Spark 3.1+"))])
def test_count(data_gen, parameterless):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen) \
            .selectExpr(
            'count(a)',
            'count()',
            'count()',
            'count(1)'),
        conf = {'spark.sql.legacy.allowParameterlessCount': parameterless})

@pytest.mark.parametrize('data_gen', non_nan_all_basic_gens, ids=idfn)
def test_distinct_count_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'count(DISTINCT a)'))

@pytest.mark.xfail(condition=is_before_spark_311(),
        reason='Spark fixed distinct count of NaNs in 3.1')
@pytest.mark.parametrize('data_gen', [float_gen, double_gen], ids=idfn)
def test_distinct_float_count_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'count(DISTINCT a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_arithmetic_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'sum(a)',
                'avg(a)'),
            conf = _no_nans_float_conf)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen + _nested_gens, ids=idfn)
def test_groupby_first_last(data_gen):
    gen_fn = [('a', RepeatSeqGen(LongGen(), length=20)), ('b', data_gen)]
    agg_fn = lambda df: df.groupBy('a').agg(
        f.first('b'), f.last('b'), f.first('b', True), f.last('b', True))
    assert_gpu_and_cpu_are_equal_collect(
        # First and last are not deterministic when they are run in a real distributed setup.
        # We set parallelism 1 to prevent nondeterministic results because of distributed setup.
        lambda spark: agg_fn(gen_df(spark, gen_fn, num_slices=1)),
        conf=allow_negative_scale_of_decimal_conf)

@ignore_order
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
@pytest.mark.parametrize('count_func', [f.count, f.countDistinct])
def test_agg_count(data_gen, count_func):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, [('a', data_gen), ('b', data_gen)],
                              length=1024).groupBy('a').agg(count_func("b")))

def subquery_create_temp_views(spark, expr):
    t1 = "select * from values (1,2) as t1(a,b)"
    t2 = "select * from values (3,4) as t2(c,d)"
    spark.sql(t1).createOrReplaceTempView("t1")
    spark.sql(t2).createOrReplaceTempView("t2")
    return spark.sql(expr)

# Adding these tests as they were added in SPARK-31620, and were shown to break in
# SPARK-32031, but our GPU hash aggregate does not seem to exhibit the same failure.
# The tests are being added more as a sanity check.
# Adaptive is being turned on and off so we invoke re-optimization at the logical plan level.
@pytest.mark.parametrize('adaptive', ["true", "false"])
@pytest.mark.parametrize('expr', [
  "select sum(if(c > (select a from t1), d, 0)) as csum from t2",
  "select c, sum(if(c > (select a from t1), d, 0)) as csum from t2 group by c",
  "select avg(distinct(d)), sum(distinct(if(c > (select a from t1), d, 0))) as csum " +
    "from t2 group by c",
  "select sum(distinct(if(c > (select sum(distinct(a)) from t1), d, 0))) as csum " +
    "from t2 group by c"
])
def test_subquery_in_agg(adaptive, expr):
    assert_gpu_and_cpu_are_equal_collect(
      lambda spark: subquery_create_temp_views(spark, expr),
        conf = {"spark.sql.adaptive.enabled" : adaptive})


# TODO support multi-level structs https://github.com/NVIDIA/spark-rapids/issues/2438
def assert_single_level_struct(df):
    first_level_dt = df.schema['a'].dataType
    second_level_dt = first_level_dt['aa'].dataType
    assert isinstance(first_level_dt, StructType)
    assert isinstance(second_level_dt, IntegerType)

# Prevent value deduplication bug across rows and struct columns
# https://github.com/apache/spark/pull/31778 by injecting
# extra literal columns
#
def workaround_dedupe_by_value(df, num_cols):
    col_id_rng = range(0, num_cols)
    return reduce(lambda df, i: df.withColumn(f"fake_col_{i}", f.lit(i)), col_id_rng, df)


@allow_non_gpu(any = True)
@pytest.mark.parametrize('key_data_gen', [
    StructGen([
        ('aa', IntegerGen(min_val=0, max_val=9)),
    ], nullable=False),
    StructGen([
        ('aa', IntegerGen(min_val=0, max_val=4)),
        ('ab', IntegerGen(min_val=5, max_val=9)),
    ], nullable=False),
], ids=idfn)
@ignore_order(local=True)
def test_struct_groupby_count(key_data_gen):
    def group_by_count(spark):
        df = two_col_df(spark, key_data_gen, IntegerGen())
        assert_single_level_struct(df)
        return workaround_dedupe_by_value(df.groupBy(df.a).count(), 3)
    assert_gpu_and_cpu_are_equal_collect(group_by_count)


@pytest.mark.parametrize('cast_struct_tostring', ['LEGACY', 'SPARK311+'])
@pytest.mark.parametrize('key_data_gen', [
    StructGen([
        ('aa', IntegerGen(min_val=0, max_val=9)),
    ], nullable=False),
    StructGen([
        ('aa', IntegerGen(min_val=0, max_val=4)),
        ('ab', IntegerGen(min_val=5, max_val=9)),
    ], nullable=False)
], ids=idfn)
@ignore_order(local=True)
def test_struct_cast_groupby_count(cast_struct_tostring, key_data_gen):
    def _group_by_struct_or_cast(spark):
        df = two_col_df(spark, key_data_gen, IntegerGen())
        assert_single_level_struct(df)
        return df.groupBy(df.a.cast(StringType())).count()
    assert_gpu_and_cpu_are_equal_collect(_group_by_struct_or_cast, {
        'spark.sql.legacy.castComplexTypesToString.enabled': cast_struct_tostring == 'LEGACY'
    })


@allow_non_gpu(any = True)
@pytest.mark.parametrize('key_data_gen', [
    StructGen([
        ('a', StructGen([
            ('aa', IntegerGen(min_val=0, max_val=9))
        ]))], nullable=False),
    StructGen([
        ('a', StructGen([
            ('aa', IntegerGen(min_val=0, max_val=4)),
            ('ab', IntegerGen(min_val=5, max_val=9)),
        ]))], nullable=False),
], ids=idfn)
@ignore_order(local=True)
def test_struct_count_distinct(key_data_gen):
    def _count_distinct_by_struct(spark):
        df = gen_df(spark, key_data_gen)
        assert_single_level_struct(df)
        return df.agg(f.countDistinct(df.a))
    assert_gpu_and_cpu_are_equal_collect(_count_distinct_by_struct)


@pytest.mark.parametrize('cast_struct_tostring', ['LEGACY', 'SPARK311+'])
@pytest.mark.parametrize('key_data_gen', [
    StructGen([
        ('a', StructGen([
            ('aa', IntegerGen(min_val=0, max_val=9))
        ]))], nullable=False),
    StructGen([
        ('a', StructGen([
            ('aa', IntegerGen(min_val=0, max_val=4)),
            ('ab', IntegerGen(min_val=5, max_val=9)),
        ]))], nullable=False),
], ids=idfn)
@ignore_order(local=True)
def test_struct_count_distinct_cast(cast_struct_tostring, key_data_gen):
    def _count_distinct_by_struct(spark):
        df = gen_df(spark, key_data_gen)
        assert_single_level_struct(df)
        return df.agg(f.countDistinct(df.a.cast(StringType())))
    assert_gpu_and_cpu_are_equal_collect(_count_distinct_by_struct, {
        'spark.sql.legacy.castComplexTypesToString.enabled': cast_struct_tostring == 'LEGACY'
    })

@ignore_order(local=True)
def test_reduction_nested_struct():
    def do_it(spark):
        df = unary_op_df(spark, StructGen([('aa', StructGen([('aaa', IntegerGen(min_val=0, max_val=4))]))]))
        return df.agg(f.sum(df.a.aa.aaa))
    assert_gpu_and_cpu_are_equal_collect(do_it)

@ignore_order(local=True)
def test_reduction_nested_array():
    def do_it(spark):
        df = unary_op_df(spark, ArrayGen(StructGen([('aa', IntegerGen(min_val=0, max_val=4))])))
        return df.agg(f.sum(df.a[1].aa))
    assert_gpu_and_cpu_are_equal_collect(do_it)

# The map here is a child not a top level, because we only support GetMapValue on String to String maps.
@ignore_order(local=True)
def test_reduction_nested_map():
    def do_it(spark):
        df = unary_op_df(spark, ArrayGen(MapGen(StringGen('a{1,5}', nullable=False), StringGen('[ab]{1,5}'))))
        return df.agg(f.min(df.a[1]["a"]))
    assert_gpu_and_cpu_are_equal_collect(do_it)

@ignore_order(local=True)
def test_agg_nested_struct():
    def do_it(spark):
        df = two_col_df(spark, StringGen('k{1,5}'), StructGen([('aa', StructGen([('aaa', IntegerGen(min_val=0, max_val=4))]))]))
        return df.groupBy('a').agg(f.sum(df.b.aa.aaa))
    assert_gpu_and_cpu_are_equal_collect(do_it)

@ignore_order(local=True)
def test_agg_nested_array():
    def do_it(spark):
        df = two_col_df(spark, StringGen('k{1,5}'), ArrayGen(StructGen([('aa', IntegerGen(min_val=0, max_val=4))])))
        return df.groupBy('a').agg(f.sum(df.b[1].aa))
    assert_gpu_and_cpu_are_equal_collect(do_it)

# The map here is a child not a top level, because we only support GetMapValue on String to String maps.
@ignore_order(local=True)
def test_agg_nested_map():
    def do_it(spark):
        df = two_col_df(spark, StringGen('k{1,5}'), ArrayGen(MapGen(StringGen('a{1,5}', nullable=False), StringGen('[ab]{1,5}'))))
        return df.groupBy('a').agg(f.min(df.b[1]["a"]))
    assert_gpu_and_cpu_are_equal_collect(do_it)
