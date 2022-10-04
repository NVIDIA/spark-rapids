# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_row_counts_equal,\
    assert_gpu_and_cpu_are_equal_sql,\
    assert_gpu_fallback_collect, assert_cpu_and_gpu_are_equal_sql_with_capture,\
    assert_cpu_and_gpu_are_equal_collect_with_capture, run_with_cpu, run_with_cpu_and_gpu
from conftest import is_databricks_runtime
from data_gen import *
from functools import reduce
from pyspark.sql.types import *
from marks import *
import pyspark.sql.functions as f
from spark_session import is_databricks104_or_later, with_cpu_session

pytestmark = pytest.mark.nightly_resource_consuming_test

_float_conf = {'spark.rapids.sql.variableFloatAgg.enabled': 'true',
                       'spark.rapids.sql.castStringToFloat.enabled': 'true'
                      }

_float_smallbatch_conf = copy_and_update(_float_conf,
        {'spark.rapids.sql.batchSizeBytes' : '250'})

_float_conf_partial = copy_and_update(_float_conf,
        {'spark.rapids.sql.hashAgg.replaceMode': 'partial'})

_float_conf_final = copy_and_update(_float_conf,
        {'spark.rapids.sql.hashAgg.replaceMode': 'final'})

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
# grouping single-level structs
_grpkey_structs_with_non_nested_children = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', StringGen(pattern='[0-9]{0,30}')],
        ['ac', DecimalGen()]]), length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping multiple-level structs
_grpkey_nested_structs = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', StringGen(pattern='[0-9]{0,30}')],
        ['ac', StructGen([['aca', LongGen()],
                          ['acb', BooleanGen()],
                          ['acc', StructGen([['acca', StringGen()]])]])]]),
        length=20)),
    ('b', IntegerGen()),
    ('c', NullGen())]
# grouping multiple-level structs with arrays in children
_grpkey_nested_structs_with_array_child = [
    ('a', RepeatSeqGen(StructGen([
        ['aa', IntegerGen()],
        ['ab', ArrayGen(IntegerGen())],
        ['ac', ArrayGen(StructGen([['aca', LongGen()]]))]]),
        length=20)),
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

# grouping decimals with nulls
_decimals_with_nulls = [('a', DecimalGen()), ('b', DecimalGen()), ('c', DecimalGen())]

# grouping decimals with no nulls
_decimals_with_no_nulls = [
    ('a', DecimalGen(nullable=False)),
    ('b', DecimalGen(nullable=False)),
    ('c', DecimalGen(nullable=False))]

_init_list_with_nans_and_no_nans_with_decimals = _init_list_with_nans_and_no_nans + [
    _decimals_with_nulls, _decimals_with_no_nulls]

# Used to test ANSI-mode fallback
_no_overflow_ansi_gens = [
    ByteGen(min_val = 1, max_val = 10, special_cases=[]),
    ShortGen(min_val = 1, max_val = 100, special_cases=[]),
    IntegerGen(min_val = 1, max_val = 1000, special_cases=[]),
    LongGen(min_val = 1, max_val = 3000, special_cases=[])]

_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)
_decimal_gen_36_neg5 = DecimalGen(precision=36, scale=-5)
_decimal_gen_38_10 = DecimalGen(precision=38, scale=10)


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


# Run these tests with in 4 modes, all on the GPU
_confs = [_float_conf, _float_smallbatch_conf, _float_conf_final, _float_conf_partial]

# Pytest marker for list of operators allowed to run on the CPU,
# esp. useful in partial and final only modes.
# but this ends up allowing close to everything being off the GPU so I am not sure how
# useful this really is
_excluded_operators_marker = pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression', 'UnscaledValue', 'MakeDecimal',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'StddevPop', 'StddevSamp', 'VariancePop', 'VarianceSamp',
    'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec', 'Coalesce', 'IsNull', 'EqualNullSafe',
    'PivotFirst', 'GetArrayItem', 'ShuffleExchangeExec', 'HashPartitioning')

params_markers_for_confs = [
    (_float_conf_partial, [_excluded_operators_marker]),
    (_float_conf_final, [_excluded_operators_marker]),
    (_float_conf, [_excluded_operators_marker])
]

_grpkey_small_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=7, scale=3, nullable=(True, 10.0)), length=50)),
    ('b', DecimalGen(precision=5, scale=2)),
    ('c', DecimalGen(precision=8, scale=3))]

_grpkey_big_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=32, scale=10, nullable=(True, 10.0)), length=50)),
    ('b', DecimalGen(precision=20, scale=2)),
    ('c', DecimalGen(precision=36, scale=5))]

_grpkey_short_mid_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', decimal_gen_64bit),
    ('c', decimal_gen_64bit)]

_grpkey_short_big_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', decimal_gen_128bit),
    ('c', decimal_gen_128bit)]

# NOTE on older versions of Spark decimal 38 causes the CPU to crash
#  instead of detect overflows, we have versions of this for both
#  36 and 38 so we can get some coverage for old versions and full
# coverage for newer versions
_grpkey_short_very_big_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_36_5),
    ('c', _decimal_gen_36_5)]

_grpkey_short_very_big_neg_scale_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_36_neg5),
    ('c', _decimal_gen_36_neg5)]

# Only use negative values to avoid the potential to hover around an overflow
# as values are added and subtracted during the sum. Non-deterministic ordering
# of values from shuffle cannot guarantee overflow calculation is predictable
# when the sum can move in both directions as new partitions are aggregated.
_decimal_gen_sum_38_0 = DecimalGen(precision=38, scale=0, avoid_positive_values=True)
_decimal_gen_sum_38_neg10 = DecimalGen(precision=38, scale=-10, avoid_positive_values=True)

_grpkey_short_sum_full_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_sum_38_0),
    ('c', _decimal_gen_sum_38_0)]

_grpkey_short_sum_full_neg_scale_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _decimal_gen_sum_38_neg10),
    ('c', _decimal_gen_sum_38_neg10)]


_init_list_no_nans_with_decimal = _init_list_no_nans + [
    _grpkey_small_decimals]

_init_list_no_nans_with_decimalbig = _init_list_no_nans + [
    _grpkey_small_decimals, _grpkey_big_decimals, _grpkey_short_mid_decimals,
    _grpkey_short_big_decimals, _grpkey_short_very_big_decimals, 
    _grpkey_short_very_big_neg_scale_decimals]

_init_list_with_nans_and_no_nans_with_decimalbig = _init_list_with_nans_and_no_nans + [
    _grpkey_small_decimals, _grpkey_big_decimals, _grpkey_short_mid_decimals,
    _grpkey_short_big_decimals, _grpkey_short_very_big_decimals, 
    _grpkey_short_very_big_neg_scale_decimals]


#Any smaller precision takes way too long to process on the CPU
# or results in using too much memory on the GPU
@nightly_gpu_mem_consuming_case
@pytest.mark.parametrize('precision', [38, 37, 36, 35, 34, 33, 32, 31], ids=idfn)
def test_hash_reduction_decimal_overflow_sum(precision):
    constant = '9' * precision
    count = pow(10, 38 - precision)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.range(count)\
                .selectExpr("CAST('{}' as Decimal({}, 0)) as a".format(constant, precision))\
                .selectExpr("SUM(a)"),
        # This is set to 128m because of a number of other bugs that compound to having us
        # run out of memory in some setups. These should not happen in production, because
        # we really are just doing a really bad job at multiplying to get this result so
        # some optimizations are conspiring against us.
        conf = {'spark.rapids.sql.batchSizeBytes': '128m'})

@pytest.mark.parametrize('data_gen', [_longs_with_nulls], ids=idfn)
def test_hash_grpby_sum_count_action(data_gen):
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b'))
    )

@pytest.mark.parametrize('data_gen', [_longs_with_nulls], ids=idfn)
def test_hash_reduction_sum_count_action(data_gen):
    assert_gpu_and_cpu_row_counts_equal(
        lambda spark: gen_df(spark, data_gen, length=100).agg(f.sum('b'))
    )

# Make sure that we can do computation in the group by columns
@ignore_order
def test_computation_in_grpby_columns():
    conf = {'spark.rapids.sql.batchSizeBytes' : '250'}
    data_gen = [
            ('a', RepeatSeqGen(StringGen('a{1,20}'), length=50)),
            ('b', short_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen).groupby(f.substring(f.col('a'), 2, 10)).agg(f.sum('b')),
        conf = conf)

@shuffle_test
@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans_with_decimalbig, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_sum(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b')),
        conf = conf)

@shuffle_test
@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', [_grpkey_short_sum_full_decimals, _grpkey_short_sum_full_neg_scale_decimals], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_sum_full_decimal(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a').agg(f.sum('b')),
        conf = conf)

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens + [DecimalGen(precision=36, scale=5)], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_sum(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen, length=100).selectExpr("SUM(a)"),
        conf = conf)

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens + [
    DecimalGen(precision=38, scale=0), DecimalGen(precision=38, scale=-10)], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_sum_full_decimal(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen, length=100).selectExpr("SUM(a)"),
        conf = conf)

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans + [_grpkey_short_mid_decimals, 
    _grpkey_short_big_decimals, _grpkey_short_very_big_decimals, _grpkey_short_sum_full_decimals], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_avg(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=200).groupby('a').agg(f.avg('b')),
        conf=conf
    )

# tracks https://github.com/NVIDIA/spark-rapids/issues/154
@approximate_float
@ignore_order
@incompat
@pytest.mark.allow_non_gpu(
    'HashAggregateExec', 'AggregateExpression',
    'AttributeReference', 'Alias', 'Sum', 'Count', 'Max', 'Min', 'Average', 'Cast',
    'NormalizeNaNAndZero', 'GreaterThan', 'Literal', 'If',
    'EqualTo', 'First', 'SortAggregateExec')
@pytest.mark.parametrize('data_gen', [
    StructGen(children=[('a', int_gen), ('b', int_gen)],nullable=False,
        special_cases=[((None, None), 400.0), ((None, -1542301795), 100.0)])], ids=idfn)
@pytest.mark.xfail(condition=is_databricks104_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/4963')
def test_hash_avg_nulls_partial_only(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=2).agg(f.avg('b')),
        conf=_float_conf_partial
    )

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans_with_decimalbig, ids=idfn)
def test_intersectAll(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, data_gen, length=100).intersectAll(gen_df(spark, data_gen, length=100)))

@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans_with_decimalbig, ids=idfn)
def test_exceptAll(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : gen_df(spark, data_gen, length=100).exceptAll(gen_df(spark, data_gen, length=100).filter('a != b')))

# Spark fails to sort some decimal values due to overflow when calculating the sorting prefix.
# See https://issues.apache.org/jira/browse/SPARK-40129
# Since pivot orders by value, avoid generating these extreme values for this test.
_pivot_gen_128bit = DecimalGen(precision=20, scale=2, special_cases=[])
_pivot_big_decimals = [
    ('a', RepeatSeqGen(DecimalGen(precision=32, scale=10, nullable=(True, 10.0)), length=50)),
    ('b', _pivot_gen_128bit),
    ('c', DecimalGen(precision=36, scale=5))]
_pivot_short_big_decimals = [
    ('a', RepeatSeqGen(short_gen, length=50)),
    ('b', _pivot_gen_128bit),
    ('c', decimal_gen_128bit)]

_pivot_gens_with_decimals = _init_list_with_nans_and_no_nans + [
    _grpkey_small_decimals, _pivot_big_decimals, _grpkey_short_mid_decimals,
    _pivot_short_big_decimals, _grpkey_short_very_big_decimals,
    _grpkey_short_very_big_neg_scale_decimals]
@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _pivot_gens_with_decimals, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_pivot(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b')
            .agg(f.sum('c')),
        conf = conf)

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
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
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans, ids=idfn)
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
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_pivot_without_nans(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby()
            .pivot('b')
            .agg(f.sum('c')),
        conf=conf)

@approximate_float
@ignore_order(local=True)
@incompat
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_pivot_with_nans(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby()
            .pivot('b')
            .agg(f.sum('c')),
        conf=conf)

@approximate_float
@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'PivotFirst', 'AggregateExpression', 'Alias', 'GetArrayItem',
        'Literal', 'ShuffleExchangeExec', 'HashPartitioning', 'NormalizeNaNAndZero')
@incompat
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nulls_and_nans], ids=idfn)
def test_hash_pivot_groupby_duplicates_fallback(data_gen):
    # PivotFirst will not work on the GPU when pivot_values has duplicates
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .pivot('b', ['10.0', '10.0'])
            .agg(f.sum('c')),
        "PivotFirst",
        conf=_float_conf)

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

_full_repeat_agg_column_for_collect_op = [
    RepeatSeqGen(_decimal_gen_38_10, length=15)
]

_gen_data_for_collect_op = [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen),
    ('c', LongRangeGen())] for value_gen in _repeat_agg_column_for_collect_op]

_full_gen_data_for_collect_op = _gen_data_for_collect_op + [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen),
    ('c', LongRangeGen())] for value_gen in _full_repeat_agg_column_for_collect_op]

_repeat_agg_column_for_collect_list_op = [
        RepeatSeqGen(ArrayGen(int_gen), length=15),
        RepeatSeqGen(all_basic_struct_gen, length=15),
        RepeatSeqGen(StructGen([['c0', all_basic_struct_gen]]), length=15),
        RepeatSeqGen(simple_string_to_string_map_gen, length=15)]

_gen_data_for_collect_list_op = _full_gen_data_for_collect_op + [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen)] for value_gen in _repeat_agg_column_for_collect_list_op]

_repeat_agg_column_for_collect_set_op = [
    RepeatSeqGen(all_basic_struct_gen, length=15),
    RepeatSeqGen(StructGen([
        ['c0', all_basic_struct_gen], ['c1', int_gen]]), length=15)]

# data generating for collect_set based-nested Struct[Array] types
_repeat_agg_column_for_collect_set_op_nested = [
    RepeatSeqGen(struct_array_gen_no_nans, length=15),
    RepeatSeqGen(StructGen([
        ['c0', struct_array_gen_no_nans], ['c1', int_gen]]), length=15),
    RepeatSeqGen(ArrayGen(all_basic_struct_gen_no_nan), length=15)]

_array_of_array_gen = [RepeatSeqGen(ArrayGen(sub_gen), length=15) for sub_gen in single_level_array_gens_no_nan]

_gen_data_for_collect_set_op = [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen)] for value_gen in _repeat_agg_column_for_collect_set_op]

_gen_data_for_collect_set_op_nested = [[
    ('a', RepeatSeqGen(LongGen(), length=20)),
    ('b', value_gen)] for value_gen in _repeat_agg_column_for_collect_set_op_nested + _array_of_array_gen]

_all_basic_gens_with_all_nans_cases = all_basic_gens + [SetValuesGen(t, [math.nan, None]) for t in [FloatType(), DoubleType()]]

# very simple test for just a count on decimals 128 values until we can support more with them
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_decimal128_count_reduction(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('count(a)'))

# very simple test for just a count on decimals 128 values until we can support more with them
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_decimal128_count_group_by(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, byte_gen, data_gen)
            .groupby('a')
            .agg(f.count('b')))

# very simple test for just a min/max on decimals 128 values until we can support more with them
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_decimal128_min_max_reduction(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('min(a)', 'max(a)'))

# very simple test for just a min/max on decimals 128 values until we can support more with them
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', [decimal_gen_128bit], ids=idfn)
def test_decimal128_min_max_group_by(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, byte_gen, data_gen)
            .groupby('a')
            .agg(f.min('b'), f.max('b')))

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _all_basic_gens_with_all_nans_cases, ids=idfn)
def test_min_max_group_by(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, byte_gen, data_gen)
            .groupby('a')
            .agg(f.min('b'), f.max('b')))

# to avoid ordering issues with collect_list we do it all in a single task
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_list_op, ids=idfn)
@pytest.mark.parametrize('use_obj_hash_agg', [True, False], ids=idfn)
def test_hash_groupby_collect_list(data_gen, use_obj_hash_agg):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).coalesce(1)
            .groupby('a')
            .agg(f.collect_list('b')),
        conf={'spark.sql.execution.useObjectHashAggregateExec': str(use_obj_hash_agg).lower(),
            'spark.sql.shuffle.partitions': '1'})

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _full_gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_collect_set(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_set('b')), f.count('b')))

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_set_op, ids=idfn)
def test_hash_groupby_collect_set_on_nested_type(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_set('b'))))


# Note, using sort_array() on the CPU, because sort_array() does not yet
# support sorting certain nested/arbitrary types on the GPU
# See https://github.com/NVIDIA/spark-rapids/issues/3715
# and https://github.com/rapidsai/cudf/issues/11222
@ignore_order(local=True)
@allow_non_gpu("ProjectExec", "SortArray")
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_set_op_nested, ids=idfn)
def test_hash_groupby_collect_set_on_nested_array_type(data_gen):
    conf = copy_and_update(_float_conf, {
        "spark.rapids.sql.castFloatToString.enabled": "true",
        "spark.rapids.sql.castDecimalToString.enabled": "true",
        "spark.rapids.sql.expression.SortArray": "false"
    })

    def do_it(spark):
        df = gen_df(spark, data_gen, length=100)\
            .groupby('a')\
            .agg(f.collect_set('b').alias("collect_set"))
        # pull out the rdd and schema and create a new dataframe to run SortArray
        # to handle Spark 3.3.0+ optimization that moves SortArray from ProjectExec
        # to ObjectHashAggregateExec
        return spark.createDataFrame(df.rdd, schema=df.schema)\
            .selectExpr("sort_array(collect_set)")
        
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)


@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _full_gen_data_for_collect_op, ids=idfn)
def test_hash_reduction_collect_set(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .agg(f.sort_array(f.collect_set('b')), f.count('b')))

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_set_op, ids=idfn)
def test_hash_reduction_collect_set_on_nested_type(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .agg(f.sort_array(f.collect_set('b'))))


# Note, using sort_array() on the CPU, because sort_array() does not yet
# support sorting certain nested/arbitrary types on the GPU
# See https://github.com/NVIDIA/spark-rapids/issues/3715
# and https://github.com/rapidsai/cudf/issues/11222
@ignore_order(local=True)
@allow_non_gpu("ProjectExec", "SortArray")
@pytest.mark.parametrize('data_gen', _gen_data_for_collect_set_op_nested, ids=idfn)
def test_hash_reduction_collect_set_on_nested_array_type(data_gen):
    conf = copy_and_update(_float_conf, {
        "spark.rapids.sql.castFloatToString.enabled": "true",
        "spark.rapids.sql.castDecimalToString.enabled": "true",
        "spark.rapids.sql.expression.SortArray": "false"
    })

    def do_it(spark):
        df = gen_df(spark, data_gen, length=100)\
            .agg(f.collect_set('b').alias("collect_set"))
        # pull out the rdd and schema and create a new dataframe to run SortArray
        # to handle Spark 3.3.0+ optimization that moves SortArray from ProjectExec
        # to ObjectHashAggregateExec
        return spark.createDataFrame(df.rdd, schema=df.schema)\
            .selectExpr("sort_array(collect_set)")
        
    assert_gpu_and_cpu_are_equal_collect(do_it, conf=conf)

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _full_gen_data_for_collect_op, ids=idfn)
def test_hash_groupby_collect_with_single_distinct(data_gen):
    # test collect_ops with other distinct aggregations
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')),
                 f.sort_array(f.collect_set('b')),
                 f.countDistinct('c'),
                 f.count('c')))

@ignore_order(local=True)
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

_replace_modes_non_distinct = [
    # Spark: GPU(Final) -> CPU(Partial)
    # Databricks runtime: GPU(Complete)
    'final|complete',
    # Spark: CPU(Final) -> GPU(Partial)
    # Databricks runtime: CPU(Complete)
    'partial',
]
@ignore_order(local=True)
@allow_non_gpu('ObjectHashAggregateExec', 'SortAggregateExec',
               'ShuffleExchangeExec', 'HashPartitioning', 'SortExec',
               'SortArray', 'Alias', 'Literal', 'Count', 'CollectList', 'CollectSet',
               'GpuToCpuCollectBufferTransition', 'CpuToGpuCollectBufferTransition',
               'AggregateExpression')
@pytest.mark.parametrize('data_gen', _full_gen_data_for_collect_op, ids=idfn)
@pytest.mark.parametrize('replace_mode', _replace_modes_non_distinct, ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
@pytest.mark.parametrize('use_obj_hash_agg', ['false', 'true'], ids=idfn)
def test_hash_groupby_collect_partial_replace_fallback(data_gen,
                                                       replace_mode,
                                                       aqe_enabled,
                                                       use_obj_hash_agg):
    conf = {'spark.rapids.sql.hashAgg.replaceMode': replace_mode,
            'spark.sql.adaptive.enabled': aqe_enabled,
            'spark.sql.execution.useObjectHashAggregateExec': use_obj_hash_agg}

    cpu_clz, gpu_clz = ['CollectList', 'CollectSet'], ['GpuCollectList', 'GpuCollectSet']
    exist_clz, non_exist_clz = [], []
    # For aggregations without distinct, Databricks runtime removes the partial Aggregate stage (
    # map-side combine). There only exists an AggregateExec in Databricks runtimes. So, we need to
    # set the expected exist_classes according to runtime.
    if is_databricks_runtime():
        if replace_mode == 'partial':
            exist_clz, non_exist_clz = cpu_clz, gpu_clz
        else:
            exist_clz, non_exist_clz = gpu_clz, cpu_clz
    else:
        exist_clz = cpu_clz + gpu_clz

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')), f.sort_array(f.collect_set('b'))),
        exist_classes=','.join(exist_clz),
        non_exist_classes=','.join(non_exist_clz),
        conf=conf)

_replace_modes_single_distinct = [
    # Spark: CPU -> CPU -> GPU(PartialMerge) -> GPU(Partial)
    # Databricks runtime: CPU(Final and Complete) -> GPU(PartialMerge)
    'partial|partialMerge',
    # Spark: GPU(Final) -> GPU(PartialMerge&Partial) -> CPU(PartialMerge) -> CPU(Partial)
    # Databricks runtime: GPU(Final&Complete) -> CPU(PartialMerge)
    'final|partialMerge&partial|final&complete',
]
@ignore_order(local=True)
@allow_non_gpu('ObjectHashAggregateExec', 'SortAggregateExec',
               'ShuffleExchangeExec', 'HashPartitioning', 'SortExec',
               'SortArray', 'Alias', 'Literal', 'Count', 'CollectList', 'CollectSet',
               'GpuToCpuCollectBufferTransition', 'CpuToGpuCollectBufferTransition',
               'AggregateExpression')
@pytest.mark.parametrize('data_gen', _full_gen_data_for_collect_op, ids=idfn)
@pytest.mark.parametrize('replace_mode', _replace_modes_single_distinct, ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
@pytest.mark.parametrize('use_obj_hash_agg', ['false', 'true'], ids=idfn)
@pytest.mark.xfail(condition=is_databricks104_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/4963')
def test_hash_groupby_collect_partial_replace_with_distinct_fallback(data_gen,
                                                                     replace_mode,
                                                                     aqe_enabled,
                                                                     use_obj_hash_agg):
    conf = {'spark.rapids.sql.hashAgg.replaceMode': replace_mode,
            'spark.sql.adaptive.enabled': aqe_enabled,
            'spark.sql.execution.useObjectHashAggregateExec': use_obj_hash_agg}
    # test with single Distinct
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, data_gen, length=100)
            .groupby('a')
            .agg(f.sort_array(f.collect_list('b')),
                 f.sort_array(f.collect_set('b')),
                 f.countDistinct('c')),
        exist_classes='CollectList,CollectSet,GpuCollectList,GpuCollectSet',
        conf=conf)

    # test with Distinct Collect
    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark: gen_df(spark, data_gen, length=100),
        table_name='table',
        exist_classes='CollectSet,GpuCollectSet,Count,GpuCount',
        sql="""
    select a,
        sort_array(collect_list(distinct c)),
        sort_array(collect_set(b)),
        count(c)
    from table
    group by a""",
        conf=conf)

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
def test_hash_query_multiple_distincts_with_non_distinct(data_gen, conf):
    local_conf = copy_and_update(conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
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
        conf=local_conf)


@approximate_float
@ignore_order
@incompat
@pytest.mark.parametrize('data_gen', _init_list_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_query_max_with_multiple_distincts(data_gen, conf):
    local_conf = copy_and_update(conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=100),
        "hash_agg_table",
        'select max(c),' +
        'sum(distinct a),' +
        'count(),' +
        'count(distinct b) from hash_agg_table group by a',
        conf=local_conf)

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
@pytest.mark.parametrize('data_gen', _init_list_no_nans + [_grpkey_short_mid_decimals, _grpkey_short_big_decimals], ids=idfn)
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

@approximate_float
@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_floats_with_nan_zero_grouping_keys,
                                      _grpkey_doubles_with_nan_zero_grouping_keys], ids=idfn)
def test_hash_agg_with_nan_keys(data_gen):
    local_conf = copy_and_update(_float_conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
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
        local_conf)

@ignore_order
@pytest.mark.parametrize('data_gen',  [_grpkey_structs_with_non_nested_children,
                                       _grpkey_nested_structs], ids=idfn)
def test_hash_agg_with_struct_keys(data_gen):
    local_conf = copy_and_update(_float_conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
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
        conf=local_conf)

@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'Avg', 'Count', 'Max', 'Min', 'Sum', 'Average',
               'Cast', 'Literal', 'Alias', 'AggregateExpression',
               'ShuffleExchangeExec', 'HashPartitioning')
@pytest.mark.parametrize('data_gen',  [_grpkey_nested_structs_with_array_child], ids=idfn)
def test_hash_agg_with_struct_of_array_fallback(data_gen):
    local_conf = copy_and_update(_float_conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
    assert_cpu_and_gpu_are_equal_sql_with_capture(
        lambda spark : gen_df(spark, data_gen, length=100),
        'select a, '
        'count(*) as count_stars, '
        'count() as count_parameterless, '
        'count(b) as count_bees, '
        'sum(b) as sum_of_bees, '
        'max(c) as max_seas, '
        'min(c) as min_seas, '
        'avg(c) as average_seas '
        'from hash_agg_table group by a',
        "hash_agg_table",
        exist_classes='HashAggregateExec',
        non_exist_classes='GpuHashAggregateExec',
        conf=local_conf)


@approximate_float
@ignore_order
@pytest.mark.parametrize('data_gen', [ _grpkey_doubles_with_nan_zero_grouping_keys], ids=idfn)
def test_count_distinct_with_nan_floats(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1024),
        "hash_agg_table",
        'select a, count(distinct b) as count_distinct_bees from hash_agg_table group by a',
        _float_conf)

# TODO: Literal tests

# REDUCTIONS

_nested_gens = array_gens_sample + struct_gens_sample + map_gens_sample

@pytest.mark.parametrize('data_gen', decimal_gens, ids=idfn)
def test_first_last_reductions_decimal_types(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        # Coalesce and sort are to make sure that first and last, which are non-deterministic
        # become deterministic
        lambda spark: unary_op_df(spark, data_gen).coalesce(1).selectExpr(
            'first(a)', 'last(a)', 'first(a, true)', 'last(a, true)'))

@pytest.mark.parametrize('data_gen', _nested_gens, ids=idfn)
def test_first_last_reductions_nested_types(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        # Coalesce and sort are to make sure that first and last, which are non-deterministic
        # become deterministic
        lambda spark: unary_op_df(spark, data_gen).coalesce(1).selectExpr(
            'first(a)', 'last(a)', 'first(a, true)', 'last(a, true)'))

@pytest.mark.parametrize('data_gen', _all_basic_gens_with_all_nans_cases, ids=idfn)
def test_generic_reductions(data_gen):
    local_conf = copy_and_update(_float_conf, {'spark.sql.legacy.allowParameterlessCount': 'true'})
    assert_gpu_and_cpu_are_equal_collect(
        # Coalesce and sort are to make sure that first and last, which are non-deterministic
        # become deterministic
        lambda spark : unary_op_df(spark, data_gen) \
            .coalesce(1).selectExpr(
            'min(a)',
            'max(a)',
            'first(a)',
            'last(a)',
            'count(a)',
            'count()',
            'count(1)'),
        conf=local_conf)

@pytest.mark.parametrize('data_gen', all_gen + _nested_gens, ids=idfn)
def test_count(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen) \
            .selectExpr(
            'count(a)',
            'count()',
            'count()',
            'count(1)'),
        conf = {'spark.sql.legacy.allowParameterlessCount': 'true'})

@pytest.mark.parametrize('data_gen', all_basic_gens, ids=idfn)
def test_distinct_count_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'count(DISTINCT a)'))

@pytest.mark.parametrize('data_gen', [float_gen, double_gen], ids=idfn)
def test_distinct_float_count_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'count(DISTINCT a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', numeric_gens + [decimal_gen_64bit, decimal_gen_128bit], ids=idfn)
def test_arithmetic_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'sum(a)',
                'avg(a)'),
            conf = _float_conf)

@pytest.mark.parametrize('data_gen',
                         all_basic_gens + decimal_gens + _nested_gens,
                         ids=idfn)
def test_collect_list_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        # coalescing because collect_list is not deterministic
        lambda spark: unary_op_df(spark, data_gen).coalesce(1).selectExpr('collect_list(a)'),
        conf=_float_conf)

_no_neg_zero_all_basic_gens = [byte_gen, short_gen, int_gen, long_gen,
        # -0.0 cannot work because of -0.0 == 0.0 in cudf for distinct and
        # Spark fixed ordering of 0.0 and -0.0 in Spark 3.1 in the ordering
        FloatGen(special_cases=[FLOAT_MIN, FLOAT_MAX, 0.0, 1.0, -1.0]), DoubleGen(special_cases=[]),
        string_gen, boolean_gen, date_gen, timestamp_gen]

_struct_only_nested_gens = [all_basic_struct_gen,
                            StructGen([['child0', byte_gen], ['child1', all_basic_struct_gen]]),
                            StructGen([])]
@pytest.mark.parametrize('data_gen',
                         _no_neg_zero_all_basic_gens + decimal_gens + _struct_only_nested_gens,
                         ids=idfn)
def test_collect_set_reductions(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('sort_array(collect_set(a))'),
        conf=_float_conf)

def test_collect_empty():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("select collect_list(null)"))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: spark.sql("select collect_set(null)"))

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen + _nested_gens, ids=idfn)
def test_groupby_first_last(data_gen):
    gen_fn = [('a', RepeatSeqGen(LongGen(), length=20)), ('b', data_gen)]
    agg_fn = lambda df: df.groupBy('a').agg(
        f.first('b'), f.last('b'), f.first('b', True), f.last('b', True))
    assert_gpu_and_cpu_are_equal_collect(
        # First and last are not deterministic when they are run in a real distributed setup.
        # We set parallelism 1 to prevent nondeterministic results because of distributed setup.
        lambda spark: agg_fn(gen_df(spark, gen_fn, num_slices=1)))

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen + _struct_only_nested_gens, ids=idfn)
def test_sorted_groupby_first_last(data_gen):
    gen_fn = [('a', RepeatSeqGen(LongGen(), length=20)), ('b', data_gen)]
    # sort by more than the group by columns to be sure that first/last don't remove the ordering
    agg_fn = lambda df: df.orderBy('a', 'b').groupBy('a').agg(
        f.first('b'), f.last('b'), f.first('b', True), f.last('b', True))
    assert_gpu_and_cpu_are_equal_collect(
        # First and last are not deterministic when they are run in a real distributed setup.
        # We set parallelism and partitions to 1 to prevent nondeterministic results because
        # of distributed setups.
        lambda spark: agg_fn(gen_df(spark, gen_fn, num_slices=1)),
        conf = {'spark.sql.shuffle.partitions': '1'})

# Spark has a sorting bug with decimals, see https://issues.apache.org/jira/browse/SPARK-40129.
# Have pytest do the sorting rather than Spark as a workaround.
@ignore_order(local=True)
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

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_reduction(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('v', DoubleGen())], length=100),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf, reduction = True)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_reduction_single_row(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('v', DoubleGen())], length=1),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf, reduction = True)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_reduction_no_rows(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('v', DoubleGen())], length=0),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf, reduction = True)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_byte(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', ByteGen())], length=100),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_byte_scalar(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', ByteGen())], length=100),
        0.5, conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_long_repeated_keys(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(LongGen(), length=20)),
                                     ('v', LongRangeGen())], length=100),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_long(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', LongRangeGen())], length=100),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_long_single(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', LongRangeGen())], length=100),
        0.5, conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_double(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', DoubleGen())], length=100),
        [0.05, 0.25, 0.5, 0.75, 0.95], conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
def test_hash_groupby_approx_percentile_double_single(aqe_enabled):
    conf = {'spark.sql.adaptive.enabled': aqe_enabled}
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', StringGen(nullable=False)),
                                     ('v', DoubleGen())], length=100),
        0.05, conf)

@incompat
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
@ignore_order(local=True)
@allow_non_gpu('TakeOrderedAndProjectExec', 'Alias', 'Cast', 'ObjectHashAggregateExec', 'AggregateExpression',
    'ApproximatePercentile', 'Literal', 'ShuffleExchangeExec', 'HashPartitioning', 'CollectLimitExec')
def test_hash_groupby_approx_percentile_partial_fallback_to_cpu(aqe_enabled):
    conf = {
        'spark.rapids.sql.hashAgg.replaceMode': 'partial',
        'spark.sql.adaptive.enabled': aqe_enabled
    }

    def approx_percentile_query(spark):
        df = gen_df(spark, [('k', StringGen(nullable=False)),
                            ('v', DoubleGen())], length=100)
        df.createOrReplaceTempView("t")
        return spark.sql("select k, approx_percentile(v, array(0.1, 0.2)) from t group by k")

    assert_gpu_fallback_collect(lambda spark: approx_percentile_query(spark), 'ApproximatePercentile', conf)

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal32():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(6, 2))]),
        [0.05, 0.25, 0.5, 0.75, 0.95])

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal32_single():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(6, 2))]),
        0.05)

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal64():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(10, 9))]),
        [0.05, 0.25, 0.5, 0.75, 0.95])

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal64_single():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(10, 9))]),
        0.05)

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal128():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(19, 18))]),
        [0.05, 0.25, 0.5, 0.75, 0.95])

@incompat
@ignore_order(local=True)
def test_hash_groupby_approx_percentile_decimal128_single():
    compare_percentile_approx(
        lambda spark: gen_df(spark, [('k', RepeatSeqGen(ByteGen(nullable=False), length=2)),
                                     ('v', DecimalGen(19, 18))]),
        0.05)

# The percentile approx tests differ from other tests because we do not expect the CPU and GPU to produce the same
# results due to the different algorithms being used. Instead we compute an exact percentile on the CPU and then
# compute approximate percentiles on CPU and GPU and assert that the GPU numbers are accurate within some percentage
# of the CPU numbers
def compare_percentile_approx(df_fun, percentiles, conf = {}, reduction = False):

    # create SQL statements for exact and approx percentiles
    p_exact_sql = create_percentile_sql("percentile", percentiles, reduction)
    p_approx_sql = create_percentile_sql("approx_percentile", percentiles, reduction)

    def run_exact(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView("t")
        return spark.sql(p_exact_sql)

    def run_approx(spark):
        df = df_fun(spark)
        df.createOrReplaceTempView("t")
        return spark.sql(p_approx_sql)

    # run exact percentile on CPU
    exact = run_with_cpu(run_exact, 'COLLECT', conf)

    # run approx_percentile on CPU and GPU
    approx_cpu, approx_gpu = run_with_cpu_and_gpu(run_approx, 'COLLECT', conf)

    assert len(exact) == len(approx_cpu)
    assert len(exact) == len(approx_gpu)

    for i in range(len(exact)):
        cpu_exact_result = exact[i]
        cpu_approx_result = approx_cpu[i]
        gpu_approx_result = approx_gpu[i]

        # assert that keys match
        if not reduction:
            assert cpu_exact_result['k'] == cpu_approx_result['k']
            assert cpu_exact_result['k'] == gpu_approx_result['k']

        # extract the percentile result column
        exact_percentile = cpu_exact_result['the_percentile']
        cpu_approx_percentile = cpu_approx_result['the_percentile']
        gpu_approx_percentile = gpu_approx_result['the_percentile']

        if exact_percentile is None:
            assert cpu_approx_percentile is None
            assert gpu_approx_percentile is None
        else:
            assert cpu_approx_percentile is not None
            assert gpu_approx_percentile is not None
            if isinstance(exact_percentile, list):
                for j in range(len(exact_percentile)):
                    assert cpu_approx_percentile[j] is not None
                    assert gpu_approx_percentile[j] is not None
                    gpu_delta = abs(float(gpu_approx_percentile[j]) - float(exact_percentile[j]))
                    cpu_delta = abs(float(cpu_approx_percentile[j]) - float(exact_percentile[j]))
                    if gpu_delta > cpu_delta:
                        # GPU is less accurate so make sure we are within some tolerance
                        if gpu_delta == 0:
                            assert abs(gpu_delta / cpu_delta) - 1 < 0.001
                        else:
                            assert abs(cpu_delta / gpu_delta) - 1 < 0.001
            else:
                gpu_delta = abs(float(gpu_approx_percentile) - float(exact_percentile))
                cpu_delta = abs(float(cpu_approx_percentile) - float(exact_percentile))
                if gpu_delta > cpu_delta:
                    # GPU is less accurate so make sure we are within some tolerance
                    if gpu_delta == 0:
                        assert abs(gpu_delta / cpu_delta) - 1 < 0.001
                    else:
                        assert abs(cpu_delta / gpu_delta) - 1 < 0.001

def create_percentile_sql(func_name, percentiles, reduction):
    if reduction:
        if isinstance(percentiles, list):
            return """select {}(v, array({})) as the_percentile from t""".format(
                func_name, ",".join(str(i) for i in percentiles))
        else:
            return """select {}(v, {}) as the_percentile from t""".format(
                func_name, percentiles)
    else:
        if isinstance(percentiles, list):
            return """select k, {}(v, array({})) as the_percentile from t group by k order by k""".format(
                func_name, ",".join(str(i) for i in percentiles))
        else:
            return """select k, {}(v, {}) as the_percentile from t group by k order by k""".format(
                func_name, percentiles)


@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_avg_nulls(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a')
          .agg(f.avg('c')),
        conf=conf
    )

@ignore_order
@allow_non_gpu('HashAggregateExec', 'Alias', 'AggregateExpression', 'Cast',
  'HashPartitioning', 'ShuffleExchangeExec', 'Average')
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_grpby_avg_nulls_ansi(data_gen, conf):
    local_conf = copy_and_update(conf, {'spark.sql.ansi.enabled': 'true'})
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100).groupby('a')
          .agg(f.avg('c')),
        'Average',
        conf=local_conf
    )

@ignore_order
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_avg_nulls(data_gen, conf):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
          .agg(f.avg('c')),
        conf=conf
    )

@ignore_order
@allow_non_gpu('HashAggregateExec', 'Alias', 'AggregateExpression', 'Cast',
  'HashPartitioning', 'ShuffleExchangeExec', 'Average')
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_hash_reduction_avg_nulls_ansi(data_gen, conf):
    local_conf = copy_and_update(conf, {'spark.sql.ansi.enabled': 'true'})
    assert_gpu_fallback_collect(
        lambda spark: gen_df(spark, data_gen, length=100)
          .agg(f.avg('c')),
        'Average',
        conf=local_conf
    )


@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'Alias', 'AggregateExpression', 'Cast',
  'HashPartitioning', 'ShuffleExchangeExec', 'Sum')
@pytest.mark.parametrize('data_gen', _no_overflow_ansi_gens, ids=idfn)
def test_sum_fallback_when_ansi_enabled(data_gen):
    def do_it(spark):
        df = gen_df(spark, [('a', data_gen), ('b', data_gen)], length=100)
        return df.groupBy('a').agg(f.sum("b"))

    assert_gpu_fallback_collect(do_it, 'Sum',
        conf={'spark.sql.ansi.enabled': 'true'})


@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'Alias', 'AggregateExpression', 'Cast',
  'HashPartitioning', 'ShuffleExchangeExec', 'Average')
@pytest.mark.parametrize('data_gen', _no_overflow_ansi_gens, ids=idfn)
def test_avg_fallback_when_ansi_enabled(data_gen):
    def do_it(spark):
        df = gen_df(spark, [('a', data_gen), ('b', data_gen)], length=100)
        return df.groupBy('a').agg(f.avg("b"))

    assert_gpu_fallback_collect(do_it, 'Average',
        conf={'spark.sql.ansi.enabled': 'true'})


@ignore_order(local=True)
@allow_non_gpu('HashAggregateExec', 'Alias', 'AggregateExpression',
  'HashPartitioning', 'ShuffleExchangeExec', 'Count', 'Literal')
@pytest.mark.parametrize('data_gen', _no_overflow_ansi_gens, ids=idfn)
def test_count_fallback_when_ansi_enabled(data_gen):
    def do_it(spark):
        df = gen_df(spark, [('a', data_gen), ('b', data_gen)], length=100)
        return df.groupBy('a').agg(f.count("b"), f.count("*"))

    assert_gpu_fallback_collect(do_it, 'Count',
        conf={'spark.sql.ansi.enabled': 'true'})

@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', _no_overflow_ansi_gens, ids=idfn)
def test_no_fallback_when_ansi_enabled(data_gen):
    def do_it(spark):
        df = gen_df(spark, [('a', data_gen), ('b', data_gen)], length=100)
        # coalescing because first/last are not deterministic
        df = df.coalesce(1).orderBy("a", "b")
        return df.groupBy('a').agg(f.first("b"), f.last("b"), f.min("b"), f.max("b"))

    assert_gpu_and_cpu_are_equal_collect(do_it,
        conf={'spark.sql.ansi.enabled': 'true'})


# Tests for standard deviation and variance aggregations.
@ignore_order(local=True)
@approximate_float
@incompat
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans_with_decimals, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
def test_groupby_std_variance(data_gen, conf):
    local_conf = copy_and_update(conf, {
        'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1000),
        "data_table",
        'select ' +
        'stddev(b),' +
        'stddev_pop(b),' +
        'stddev_samp(b),' +
        'variance(b),' +
        'var_pop(b),' +
        'var_samp(b)' +
        ' from data_table group by a',
        conf=local_conf)


@ignore_order(local=True)
@approximate_float
@incompat
@pytest.mark.parametrize('data_gen', [_grpkey_strings_with_extra_nulls], ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
def test_groupby_std_variance_nulls(data_gen, conf, ansi_enabled):
    local_conf = copy_and_update(conf, {'spark.sql.ansi.enabled': ansi_enabled})
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, data_gen, length=1000),
        "data_table",
        'select ' +
        'stddev(c),' +
        'stddev_pop(c),' +
        'stddev_samp(c),' +
        'variance(c),' +
        'var_pop(c),' +
        'var_samp(c)' +
        ' from data_table group by a',
        conf=local_conf)


@ignore_order(local=True)
@approximate_float
@allow_non_gpu('NormalizeNaNAndZero',
               'HashAggregateExec', 'SortAggregateExec',
               'Cast',
               'ShuffleExchangeExec', 'HashPartitioning', 'SortExec',
               'StddevPop', 'StddevSamp', 'VariancePop', 'VarianceSamp',
               'SortArray', 'Alias', 'Literal', 'Count',
               'GpuToCpuCollectBufferTransition', 'CpuToGpuCollectBufferTransition',
               'AggregateExpression')
@pytest.mark.parametrize('data_gen', _init_list_with_nans_and_no_nans, ids=idfn)
@pytest.mark.parametrize('conf', get_params(_confs, params_markers_for_confs), ids=idfn)
@pytest.mark.parametrize('replace_mode', _replace_modes_non_distinct, ids=idfn)
@pytest.mark.parametrize('aqe_enabled', ['false', 'true'], ids=idfn)
@pytest.mark.xfail(condition=is_databricks104_or_later(), reason='https://github.com/NVIDIA/spark-rapids/issues/4963')
def test_groupby_std_variance_partial_replace_fallback(data_gen,
                                                       conf,
                                                       replace_mode,
                                                       aqe_enabled):
    local_conf = copy_and_update(conf, {'spark.rapids.sql.hashAgg.replaceMode': replace_mode,
                                        'spark.sql.adaptive.enabled': aqe_enabled})

    exist_clz = ['StddevPop', 'StddevSamp', 'VariancePop', 'VarianceSamp',
                 'GpuStddevPop', 'GpuStddevSamp', 'GpuVariancePop', 'GpuVarianceSamp']
    non_exist_clz = []

    assert_cpu_and_gpu_are_equal_collect_with_capture(
        lambda spark: gen_df(spark, data_gen, length=1000)
            .groupby('a')
            .agg(
                f.stddev('b'),
                f.stddev_pop('b'),
                f.stddev_samp('b'),
                f.variance('b'),
                f.var_pop('b'),
                f.var_samp('b')
            ),
        exist_classes=','.join(exist_clz),
        non_exist_classes=','.join(non_exist_clz),
        conf=local_conf)

#
# test min max on single level structure
#
gens_for_max_min = [byte_gen, short_gen, int_gen, long_gen,
    float_gen, double_gen,
    string_gen, boolean_gen,
    date_gen, timestamp_gen,
    DecimalGen(precision=12, scale=2),
    DecimalGen(precision=36, scale=5),
    null_gen]
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen',  gens_for_max_min, ids=idfn)
def test_min_max_for_single_level_struct(data_gen):
    df_gen = [
        ('a', StructGen([
                ('aa', data_gen),
                ('ab', data_gen)])),
        ('b', RepeatSeqGen(IntegerGen(), length=20))]

    # test max
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, df_gen),
        "hash_agg_table",
        'select b, max(a) from hash_agg_table group by b',
        _float_conf)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, df_gen),
        "hash_agg_table",
        'select max(a) from hash_agg_table',
        _float_conf)

    # test min
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, df_gen, length=1024),
        "hash_agg_table",
        'select b, min(a) from hash_agg_table group by b',
        _float_conf)
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : gen_df(spark, df_gen, length=1024),
        "hash_agg_table",
        'select min(a) from hash_agg_table',
        _float_conf)
