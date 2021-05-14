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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_fallback_collect
from conftest import get_non_gpu_allowed
from data_gen import *
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import with_spark_session, is_before_spark_311

_no_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.hasNans': 'false',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'
                      }

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
_confs = [_no_nans_float_conf, _no_nans_float_conf_final, _no_nans_float_conf_partial]
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
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b')
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
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby()
            .pivot('b')
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
# TODO: First and Last tests

# REDUCTIONS

non_nan_all_basic_gens = [byte_gen, short_gen, int_gen, long_gen,
        # nans and -0.0 cannot work because of nan support in min/max, -0.0 == 0.0 in cudf for distinct and
        # Spark fixed ordering of 0.0 and -0.0 in Spark 3.1 in the ordering
        FloatGen(no_nans=True, special_cases=[]), DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, date_gen, timestamp_gen]


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
def test_struct_groupby_count(key_data_gen):
    def group_by_count(spark):
        df = two_col_df(spark, key_data_gen, IntegerGen())
        return df.groupBy(df.a).count()
    assert_gpu_and_cpu_are_equal_collect(group_by_count)


@pytest.mark.parametrize('cast_struct_tostring', ['LEGACY', 'SPARK311+'])
@pytest.mark.parametrize('key_data_gen', [
    StructGen([
        ('a', IntegerGen(min_val=0, max_val=9)),
    ], nullable=False),
    StructGen([
        ('a', IntegerGen(min_val=0, max_val=4)),
        ('b', IntegerGen(min_val=5, max_val=9)),
    ], nullable=False)
], ids=idfn)
@ignore_order(local=True)
def test_struct_cast_groupby_count(cast_struct_tostring, key_data_gen):
    def _group_by_struct_or_cast(spark):
        df = two_col_df(spark, key_data_gen, IntegerGen())
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
        return df.agg(f.countDistinct(df.a.cast(StringType())))
    assert_gpu_and_cpu_are_equal_collect(_count_distinct_by_struct, {
        'spark.sql.legacy.castComplexTypesToString.enabled': cast_struct_tostring == 'LEGACY'
    })
