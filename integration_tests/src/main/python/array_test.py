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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_sql, assert_gpu_and_cpu_error
from data_gen import *
from functools import reduce
from spark_session import is_before_spark_311
from marks import allow_non_gpu
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from pyspark.sql.functions import array_contains, col, first, isnan, lit, element_at

# Once we support arrays as literals then we can support a[null] and
# negative indexes for all array gens. When that happens
# test_nested_array_index should go away and this should test with
# array_gens_sample instead
@pytest.mark.parametrize('data_gen', single_level_array_gens, ids=idfn)
def test_array_index(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a[0]',
                'a[1]',
                'a[null]',
                'a[3]',
                'a[50]',
                'a[-1]'),
            conf=allow_negative_scale_of_decimal_conf)

# Once we support arrays as literals then we can support a[null] for
# all array gens. See test_array_index for more info
@pytest.mark.parametrize('data_gen', nested_array_gens_sample, ids=idfn)
def test_nested_array_index(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'a[0]',
                'a[1]',
                'a[3]',
                'a[50]'))


@pytest.mark.parametrize('data_gen', all_basic_gens + [decimal_gen_default, decimal_gen_scale_precision], ids=idfn)
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
        'select array_table.a, array_table.uniq_int from array_table order by uniq_int',
        conf=allow_negative_scale_of_decimal_conf)


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
    assert_gpu_and_cpu_are_equal_collect(lambda spark: two_col_df(
        spark, arr_gen, data_gen).select(array_contains(col('a'), lit.cast(data_gen.data_type)),
                                         array_contains(col('a'), col('b')),
                                         array_contains(col('a'), col('a')[5])), no_nans_conf)


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

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, array index throws on out of range indexes")
@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_get_array_item_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(lambda spark: unary_op_df(
        spark, data_gen).select(col('a')[100]).collect(),
                               conf={'spark.sql.ansi.enabled':True,
                                     'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                               error_message='java.lang.ArrayIndexOutOfBoundsException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if index is out of range")
@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_get_array_item_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(
        spark, data_gen).select(col('a')[100]),
                               conf={'spark.sql.ansi.enabled':True,
                               'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_element_at(data_gen):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(
        spark, data_gen).select(element_at(col('a'), 1),
                               element_at(col('a'), -1)),
                               conf={'spark.sql.ansi.enabled':False,
                                     'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

@pytest.mark.skipif(is_before_spark_311(), reason="Only in Spark 3.1.1 + ANSI mode, array index throws on out of range indexes")
@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_element_at_ansi_fail(data_gen):
    assert_gpu_and_cpu_error(lambda spark: unary_op_df(
        spark, data_gen).select(element_at(col('a'), 100)).collect(),
                               conf={'spark.sql.ansi.enabled':True,
                                     'spark.sql.legacy.allowNegativeScaleOfDecimal': True},
                               error_message='java.lang.ArrayIndexOutOfBoundsException')

@pytest.mark.skipif(not is_before_spark_311(), reason="For Spark before 3.1.1 + ANSI mode, null will be returned instead of an exception if index is out of range")
@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_element_at_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(
        spark, data_gen).select(element_at(col('a'), 100)),
                               conf={'spark.sql.ansi.enabled':True,
                               'spark.sql.legacy.allowNegativeScaleOfDecimal': True})

# This corner case is for both Spark 3.0.x and 3.1.x
# CPU version will return `null` for null[100], not throwing an exception
@pytest.mark.parametrize('data_gen', [ArrayGen(null_gen,all_null=True)], ids=idfn)
def test_array_element_at_all_null_ansi_not_fail(data_gen):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(
        spark, data_gen).select(element_at(col('a'), 100)),
                               conf={'spark.sql.ansi.enabled':True,
                               'spark.sql.legacy.allowNegativeScaleOfDecimal': True})


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

    assert_gpu_and_cpu_are_equal_collect(do_it, 
            conf=allow_negative_scale_of_decimal_conf)

