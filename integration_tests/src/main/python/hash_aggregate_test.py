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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql
from conftest import is_databricks_runtime
from data_gen import *
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import with_spark_session, is_spark_300

_no_nans_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.hasNans': 'false',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'
                      }

_no_nans_float_conf_partial = _no_nans_float_conf.copy()
_no_nans_float_conf_partial.update(
    {'spark.rapids.sql.hashAgg.replaceMode': 'partial'})

_no_nans_float_conf_final = _no_nans_float_conf.copy()
_no_nans_float_conf_final.update({'spark.rapids.sql.hashAgg.replaceMode': 'final'})

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
# only final aggregates on the GPU
_confs = [_no_nans_float_conf, _no_nans_float_conf_final, _no_nans_float_conf_partial]

# Pytest marker for list of operators allowed to run on the CPU,
# esp. useful in partial and final only modes.
_excluded_operators_marker = pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'KnownFloatingPointNormalized', 'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec', 'Coalesce')

params_markers_for_confs = [
    (_no_nans_float_conf_partial, [_excluded_operators_marker]),
    (_no_nans_float_conf_final, [_excluded_operators_marker])
]


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
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
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs),
                         ids=idfn)
def test_hash_query_multiple_distincts_with_non_distinct(data_gen, conf):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select avg(a),' +
        'avg(distinct b),' +
        'avg(distinct c),' +
        'sum(distinct a),' +
        'count(distinct b),' +
        'count(a),' +
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
def test_hash_query_max_with_multiple_distincts(data_gen, conf):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select max(c),' +
        'sum(distinct a),' +
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
               'KnownFloatingPointNormalized', 'NormalizeNaNAndZero')
@pytest.mark.parametrize('data_gen', struct_gens_xfail, ids=idfn)
def test_hash_query_max_bug(data_gen):
    print_params(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.max('b')))


@approximate_float
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nan_zero_grouping_keys,
                                      _grpkey_doubles_with_nan_zero_grouping_keys], ids=idfn)
def test_hash_agg_with_nan_keys(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1024),
        "hash_agg_table",
        'select a, '
        'count(*) as count_stars, ' 
        'count(b) as count_bees, '
        'sum(b) as sum_of_bees, '
        'max(c) as max_seas, '
        'min(c) as min_seas, '
        'count(distinct c) as count_distinct_cees, '
        'avg(c) as average_seas '
        'from hash_agg_table group by a',
        _no_nans_float_conf)


@pytest.mark.xfail(
    condition=with_spark_session(lambda spark : is_spark_300()),
    reason="[SPARK-32038][SQL] NormalizeFloatingNumbers should also work on distinct aggregate "
           "(https://github.com/apache/spark/pull/28876) "
           "Fixed in later Apache Spark releases.")
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
        # https://github.com/NVIDIA/spark-rapids/issues/84 in the ordering
        FloatGen(no_nans=True, special_cases=[]), DoubleGen(no_nans=True, special_cases=[]),
        string_gen, boolean_gen, date_gen, timestamp_gen]


@pytest.mark.parametrize('data_gen', non_nan_all_basic_gens, ids=idfn)
def test_generic_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # Coalesce and sort are to make sure that first and last, which are non-deterministic
            # become deterministic
            lambda spark : binary_op_df(spark, data_gen)\
                    .coalesce(1)\
                    .sortWithinPartitions('b').selectExpr(
                'min(a)',
                'max(a)',
                'first(a)',
                'last(a)',
                'count(a)',
                'count(1)'),
            conf = _no_nans_float_conf)

@pytest.mark.parametrize('data_gen', non_nan_all_basic_gens, ids=idfn)
def test_distinct_count_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'count(DISTINCT a)'))

@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/837')
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

