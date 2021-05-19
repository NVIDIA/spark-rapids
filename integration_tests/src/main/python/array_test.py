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
from marks import allow_non_gpu
from pyspark.sql.types import *
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
                'array(a, b)',
                'array(b, a, null, {}, {})'.format(s1, s2)))


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

@pytest.mark.parametrize('data_gen', array_gens_sample, ids=idfn)
def test_array_element_at(data_gen):
    assert_gpu_and_cpu_are_equal_collect(lambda spark: unary_op_df(
        spark, data_gen).select(element_at(col('a'), 1),
                               element_at(col('a'), -1)),
                               conf={'spark.sql.ansi.enabled':False,
                                     'spark.sql.legacy.allowNegativeScaleOfDecimal': True})


@pytest.mark.parametrize('child_gen', [
    float_gen,
    double_gen,
    int_gen
], ids=idfn)
@pytest.mark.parametrize('child_to_type', [
    FloatType(),
    DoubleType(),
    IntegerType(),
], ids=idfn)
def test_array_cast_recursive(child_gen, child_to_type):
    def cast_func(spark):
        df = two_col_df(spark, int_gen, ArrayGen(child_gen))
        res = df.select(df.b.cast(ArrayType(child_to_type)))
        return res
    assert_gpu_and_cpu_are_equal_collect(cast_func)


@allow_non_gpu(any=True)
def test_array_cast_fallback():
    def cast_float_to_double(spark):
        df = two_col_df(spark, int_gen, ArrayGen(int_gen))
        res = df.select(df.b.cast(ArrayType(StringType())))
        return res
    assert_gpu_and_cpu_are_equal_collect(cast_float_to_double)