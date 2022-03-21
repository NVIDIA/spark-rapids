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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error, assert_gpu_fallback_collect
from data_gen import *
from spark_session import is_before_spark_311, is_before_spark_330, is_databricks104_or_later
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from pyspark.sql.functions import array_contains, col, isnan, element_at

# max_val is a little larger than the default max size(20) of ArrayGen
# so we can get the out-of-bound indices.
array_index_gen = IntegerGen(min_val=-25, max_val=25, special_cases=[None])
array_neg_index_gen = IntegerGen(min_val=-25, max_val=-1, special_cases=[None])
array_out_index_gen = IntegerGen(min_val=25, max_val=100, special_cases=[None])
array_zero_index_gen = IntegerGen(min_val=0, max_val=0, special_cases=[])
array_no_zero_index_gen = IntegerGen(min_val=1, max_val=25,
    special_cases=[(-25, 100), (-20, 100), (-10, 100), (-4, 100), (-3, 100), (-2, 100), (-1, 100), (None, 100)])

array_all_null_gen = ArrayGen(int_gen, all_null=True)
array_item_test_gens = array_gens_sample + [array_all_null_gen,
    ArrayGen(MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen(), max_length=10), max_length=10)]

# Merged "test_nested_array_item" with this one since arrays as literals is supported
@pytest.mark.parametrize('data_gen', array_item_test_gens, ids=idfn)
def test_array_item(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, array_index_gen).selectExpr(
            'a[b]',
            'a[0]',
            'a[1]',
            'a[null]',
            'a[3]',
            'a[50]',
            'a[-1]'))


# No need to test this for multiple data types for array. Only one is enough
@pytest.mark.skipif(is_before_spark_330(), reason="'strictIndexOperator' is introduced from Spark 3.3.0")
@pytest.mark.parametrize('strict_index_enabled', [True, False])
@pytest.mark.parametrize('index', [-2, 100, array_neg_index_gen, array_out_index_gen], ids=idfn)
def test_array_item_with_strict_index(strict_index_enabled, index):
    message = "SparkArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_df = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(col('a')[index])
    else:
        test_df = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr('a[b]')

    test_conf=copy_and_update(
        ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': strict_index_enabled})

    if strict_index_enabled:
        assert_gpu_and_cpu_error(
            lambda spark: test_df(spark).collect(),
            conf=test_conf,
            error_message=message)
    else:
        assert_gpu_and_cpu_are_equal_collect(
            test_df,
            conf=test_conf)


# No need to test this for multiple data types for array. Only one is enough, but with two kinds of invalid index.
@pytest.mark.skipif(is_before_spark_311() or not is_before_spark_330(),
                    reason="Only in Spark [3.1.1, 3.3.0) with ANSI mode, it throws exceptions for invalid index")
@pytest.mark.parametrize('index', [-2, 100, array_neg_index_gen, array_out_index_gen], ids=idfn)
def test_array_item_ansi_fail_invalid_index(index):
    message = "SparkArrayIndexOutOfBoundsException" if is_databricks104_or_later() else "java.lang.ArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(col('a')[index]).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr('a[b]').collect()
    assert_gpu_and_cpu_error(
        test_func,
        conf=ansi_enabled_conf,
        error_message=message)


@pytest.mark.skipif(not is_before_spark_311(),
                    reason="Only before Spark 3.1.1 with ANSI mode, it returns nulls for invalid index")
def test_array_item_ansi_not_fail_invalid_index():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, ArrayGen(int_gen), array_neg_index_gen, array_out_index_gen).selectExpr(
            'a[100]',
            'a[-2]',
            'a[b]',
            'a[c]'),
        conf=ansi_enabled_conf)


def test_array_item_ansi_not_fail_all_null_data():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, array_all_null_gen, array_neg_index_gen, array_out_index_gen).selectExpr(
            'a[100]',
            'a[-2]',
            'a[b]',
            'a[c]'),
        conf=ansi_enabled_conf)


@pytest.mark.parametrize('data_gen', all_basic_gens + [
                         decimal_gen_32bit, decimal_gen_64bit, decimal_gen_128bit,
                         StructGen([['child0', StructGen([['child01', IntegerGen()]])], ['child1', string_gen], ['child2', float_gen]], nullable=False),
                         StructGen([['child0', byte_gen], ['child1', string_gen], ['child2', float_gen]], nullable=False)], ids=idfn)
def test_make_array(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'array(null)',
                'array(a, b)',
                'array(b, a, null, {}, {})'.format(s1, s2),
                'array(array(b, a, null, {}, {}), array(a), array(null))'.format(s1, s2)))


@pytest.mark.parametrize('data_gen', single_level_array_gens, ids=idfn)
def test_orderby_array_unique(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [ArrayGen(ArrayGen(short_gen, max_length=10), max_length=10),
                                      ArrayGen(ArrayGen(string_gen, max_length=10), max_length=10)], ids=idfn)
def test_orderby_array_of_arrays(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
    lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [ArrayGen(StructGen([['child0', byte_gen],
                                                          ['child1', string_gen],
                                                          ['child2', float_gen]]))], ids=idfn)
def test_orderby_array_of_structs(data_gen):
    assert_gpu_and_cpu_are_equal_sql(
        lambda spark : append_unique_int_col_to_df(spark, unary_op_df(spark, data_gen)),
        'array_table',
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int')


@pytest.mark.parametrize('data_gen', [byte_gen, short_gen, int_gen, long_gen,
                                      FloatGen(no_nans=True), DoubleGen(no_nans=True),
                                      string_gen, boolean_gen, date_gen, timestamp_gen], ids=idfn)
def test_array_contains(data_gen):
    arr_gen = ArrayGen(data_gen)
    lit = gen_scalar(data_gen, force_no_nulls=True)

    def get_input(spark):
        return two_col_df(spark, arr_gen, data_gen)

    assert_gpu_and_cpu_are_equal_collect(lambda spark: get_input(spark).select(
                                            array_contains(col('a'), lit.cast(data_gen.data_type)),
                                            array_contains(col('a'), col('b')),
                                            array_contains(col('a'), col('a')[5])
                                         ), no_nans_conf)


# Test array_contains() with a literal key that is extracted from the input array of doubles
# that does contain NaNs. Note that the config is still set to indicate that the input has NaNs
# but we verify that the plan is on the GPU despite that if the value being looked up is not a NaN.
@pytest.mark.parametrize('data_gen', [double_gen], ids=idfn)
def test_array_contains_for_nans(data_gen):
    arr_gen = ArrayGen(data_gen)

    def main_df(spark):
        df = three_col_df(spark, arr_gen, data_gen, arr_gen)
        chk_val = df.select(col('a')[0].alias('t')).filter(~isnan(col('t'))).collect()[0][0]
        return df.select(array_contains(col('a'), chk_val))
    assert_gpu_and_cpu_are_equal_collect(main_df)


@pytest.mark.parametrize('data_gen', array_item_test_gens, ids=idfn)
def test_array_element_at(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, data_gen, array_no_zero_index_gen).selectExpr(
            'element_at(a, cast(NULL as int))',
            'element_at(a, 1)',
            'element_at(a, 30)',
            'element_at(a, -1)',
            'element_at(a, -30)',
            'element_at(a, b)'))


# No need tests for multiple data types for list data. Only one is enough.
@pytest.mark.skipif(is_before_spark_311(),
                    reason="Only for Spark 3.1.1+ with ANSI mode, it throws an exception for invalid index")
@pytest.mark.parametrize('index', [100, array_out_index_gen], ids=idfn)
def test_array_element_at_ansi_fail_invalid_index(index):
    message = "ArrayIndexOutOfBoundsException" if is_before_spark_330() else "SparkArrayIndexOutOfBoundsException"
    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(
            element_at(col('a'), index)).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr(
            'element_at(a, b)').collect()
    # For 3.3.0+ strictIndexOperator should not affect element_at
    test_conf=copy_and_update(ansi_enabled_conf, {'spark.sql.ansi.strictIndexOperator': 'false'})
    assert_gpu_and_cpu_error(
        test_func,
        conf=test_conf,
        error_message=message)


@pytest.mark.skipif(not is_before_spark_311(),
                    reason="Only before Spark 3.1.1 with ANSI mode, it returns nulls for invalid index")
def test_array_element_at_ansi_not_fail_invalid_index():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, ArrayGen(int_gen), array_out_index_gen).selectExpr(
            'element_at(a, 100)',
            'element_at(a, b)'),
        conf=ansi_enabled_conf)


def test_array_element_at_ansi_not_fail_all_null_data():
    # No exception when zero index but all the array rows are null
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, array_all_null_gen, array_zero_index_gen, array_out_index_gen).selectExpr(
            'element_at(a, 0)',
            'element_at(a, b)',
            'element_at(a, c)'),
        conf=ansi_enabled_conf)


@pytest.mark.parametrize('index', [0, array_zero_index_gen], ids=idfn)
@pytest.mark.parametrize('ansi_enabled', [False, True], ids=idfn)
def test_array_element_at_zero_index_fail(index, ansi_enabled):
    message = "SQL array indices start at 1"
    if isinstance(index, int):
        test_func = lambda spark: unary_op_df(spark, ArrayGen(int_gen)).select(
            element_at(col('a'), index)).collect()
    else:
        test_func = lambda spark: two_col_df(spark, ArrayGen(int_gen), index).selectExpr(
            'element_at(a, b)').collect()
    assert_gpu_and_cpu_error(
        test_func,
        conf={'spark.sql.ansi.enabled':ansi_enabled},
        error_message=message)


@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_transform(data_gen):
    def do_it(spark):
        columns = ['a', 'b',
                'transform(a, item -> item) as ident',
                'transform(a, item -> null) as n',
                'transform(a, item -> 1) as one',
                'transform(a, (item, index) -> index) as indexed',
                'transform(a, item -> b) as b_val',
                'transform(a, (item, index) -> index - b) as math_on_index']
        element_type = data_gen.data_type.elementType
        # decimal types can grow too large so we are avoiding those here for now
        if isinstance(element_type, IntegralType):
            columns.extend(['transform(a, item -> item + 1) as add',
                'transform(a, item -> item + item) as mul',
                'transform(a, (item, index) -> item + index + b) as all_add'])

        if isinstance(element_type, StringType):
            columns.extend(['transform(a, entry -> concat(entry, "-test")) as con'])

        if isinstance(element_type, ArrayType):
            columns.extend(['transform(a, entry -> transform(entry, sub_entry -> 1)) as sub_one',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index)) as index_as_sub_entry',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index + sub_index)) as index_add_sub_index',
                'transform(a, (entry, index) -> transform(entry, (sub_entry, sub_index) -> index + sub_index + b)) as add_indexes_and_value'])

        return two_col_df(spark, data_gen, byte_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it)

# TODO add back in string_gen when https://github.com/rapidsai/cudf/issues/9156 is fixed
array_min_max_gens_no_nan = [byte_gen, short_gen, int_gen, long_gen, FloatGen(no_nans=True), DoubleGen(no_nans=True),
        string_gen, boolean_gen, date_gen, timestamp_gen, null_gen] + decimal_gens

@pytest.mark.parametrize('data_gen', array_min_max_gens_no_nan, ids=idfn)
def test_array_min(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, ArrayGen(data_gen)).selectExpr(
                'array_min(a)'),
            conf=no_nans_conf)


@pytest.mark.parametrize('data_gen', decimal_gens, ids=idfn)
def test_array_concat_decimal(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : debug_df(unary_op_df(spark, ArrayGen(data_gen)).selectExpr(
            'concat(a, a)')),
        conf=no_nans_conf)


@pytest.mark.parametrize('data_gen', array_min_max_gens_no_nan, ids=idfn)
def test_array_max(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, ArrayGen(data_gen)).selectExpr(
                'array_max(a)'),
            conf=no_nans_conf)

# We add in several types of processing for foldable functions because the output
# can be different types.
@pytest.mark.parametrize('query', [
    'sequence(1, 5) as s',
    'array(1, 2, 3) as a',
    'array(sequence(1, 5), sequence(2, 7)) as a_a',
    'array(map(1, "a", 2, "b")) as a_m',
    'array(map_from_arrays(sequence(1, 2), array("1", "2"))) as a_m',
    'array(struct(1 as a, 2 as b), struct(3 as a, 4 as b)) as a_s',
    'array(struct(1 as a, sequence(1, 5) as b), struct(3 as a, sequence(2, 7) as b)) as a_s_a',
    'array(array(struct(1 as a, 2 as b), struct(3 as a, 4 as b))) as a_a_s'], ids=idfn)
def test_sql_array_scalars(query):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.sql('SELECT {}'.format(query)))


@pytest.mark.parametrize('data_gen', all_basic_gens + nested_gens_sample, ids=idfn)
def test_get_array_struct_fields(data_gen):
    array_struct_gen = ArrayGen(
        StructGen([['child0', data_gen], ['child1', int_gen]]),
        max_length=6)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, array_struct_gen).selectExpr('a.child0'))

@pytest.mark.parametrize('data_gen', [ArrayGen(string_gen), ArrayGen(int_gen)])
@pytest.mark.parametrize('threeVL', [
    pytest.param(False, id='3VL:off'),
    pytest.param(True, id='3VL:on'),
])
def test_array_exists(data_gen, threeVL):
    def do_it(spark):
        columns = ['a']
        element_type = data_gen.data_type.elementType
        if isinstance(element_type, IntegralType):
            columns.extend([
                'exists(a, item -> item % 2 = 0) as exists_even',
                'exists(a, item -> item < 0) as exists_negative',
                'exists(a, item -> item >= 0) as exists_non_negative'
            ])

        if isinstance(element_type, StringType):
            columns.extend(['exists(a, entry -> length(entry) > 5) as exists_longer_than_5'])

        return unary_op_df(spark, data_gen).selectExpr(columns)

    assert_gpu_and_cpu_are_equal_collect(do_it, conf= {
        'spark.sql.legacy.followThreeValuedLogicInArrayExists' : threeVL,
    })