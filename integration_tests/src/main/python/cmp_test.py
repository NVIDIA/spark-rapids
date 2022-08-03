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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from spark_session import with_cpu_session, is_before_spark_330
from pyspark.sql.types import *
import pyspark.sql.functions as f

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_eq(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') == s1,
                s2 == f.col('b'),
                f.lit(None).cast(data_type) == f.col('a'),
                f.col('b') == f.lit(None).cast(data_type),
                f.col('a') == f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_eq_for_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') == s1,
                s2 == f.col('b'),
                f.lit(None).cast(data_type) == f.col('a'),
                f.col('b') == f.lit(None).cast(data_type),
                f.col('a') == f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)
    

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
def test_eq_ns(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a').eqNullSafe(s1),
                s2.eqNullSafe(f.col('b')),
                f.lit(None).cast(data_type).eqNullSafe(f.col('a')),
                f.col('b').eqNullSafe(f.lit(None).cast(data_type)),
                f.col('a').eqNullSafe(f.col('b'))))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_eq_ns_for_interval():
    data_gen = DayTimeIntervalGen()
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).select(
            f.col('a').eqNullSafe(s1),
            s2.eqNullSafe(f.col('b')),
            f.lit(None).cast(data_type).eqNullSafe(f.col('a')),
            f.col('b').eqNullSafe(f.lit(None).cast(data_type)),
            f.col('a').eqNullSafe(f.col('b'))))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_ne(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') != s1,
                s2 != f.col('b'),
                f.lit(None).cast(data_type) != f.col('a'),
                f.col('b') != f.lit(None).cast(data_type),
                f.col('a') != f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_ne_for_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') != s1,
                s2 != f.col('b'),
                f.lit(None).cast(data_type) != f.col('a'),
                f.col('b') != f.lit(None).cast(data_type),
                f.col('a') != f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_lt(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') < s1,
                s2 < f.col('b'),
                f.lit(None).cast(data_type) < f.col('a'),
                f.col('b') < f.lit(None).cast(data_type),
                f.col('a') < f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_lt_for_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') < s1,
                s2 < f.col('b'),
                f.lit(None).cast(data_type) < f.col('a'),
                f.col('b') < f.lit(None).cast(data_type),
                f.col('a') < f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_lte(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') <= s1,
                s2 <= f.col('b'),
                f.lit(None).cast(data_type) <= f.col('a'),
                f.col('b') <= f.lit(None).cast(data_type),
                f.col('a') <= f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_lte_for_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') <= s1,
                s2 <= f.col('b'),
                f.lit(None).cast(data_type) <= f.col('a'),
                f.col('b') <= f.lit(None).cast(data_type),
                f.col('a') <= f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)


@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_gt(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') > s1,
                s2 > f.col('b'),
                f.lit(None).cast(data_type) > f.col('a'),
                f.col('b') > f.lit(None).cast(data_type),
                f.col('a') > f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_gt_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') > s1,
                s2 > f.col('b'),
                f.lit(None).cast(data_type) > f.col('a'),
                f.col('b') > f.lit(None).cast(data_type),
                f.col('a') > f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', orderable_gens + struct_gens_sample_with_decimal128_no_list, ids=idfn)
def test_gte(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') >= s1,
                s2 >= f.col('b'),
                f.lit(None).cast(data_type) >= f.col('a'),
                f.col('b') >= f.lit(None).cast(data_type),
                f.col('a') >= f.col('b')))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_gte_for_interval():
    def test_func(data_gen):
        (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
        data_type = data_gen.data_type
        assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') >= s1,
                s2 >= f.col('b'),
                f.lit(None).cast(data_type) >= f.col('a'),
                f.col('b') >= f.lit(None).cast(data_type),
                f.col('a') >= f.col('b')))
    # DayTimeIntervalType not supported inside Structs -- issue #6184
    # data_gens = [DayTimeIntervalGen(),
    # StructGen([['child0', StructGen([['child2', DayTimeIntervalGen()]])], ['child1', short_gen]])]
    data_gens = [DayTimeIntervalGen()]
    for data_gen in data_gens:
        test_func(data_gen)

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_isnull(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnull(f.col('a'))))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_isnull_for_interval():
    data_gen = DayTimeIntervalGen()
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).select(
            f.isnull(f.col('a'))))

@pytest.mark.parametrize('data_gen', [FloatGen(), DoubleGen()], ids=idfn)
def test_isnan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnan(f.col('a'))))

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_dropna_any(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna())

@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_dropna_all(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna(how='all'))

#dropna is really a filter along with a test for null, but lets do an explicit filter test too
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_filter(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, BooleanGen(), data_gen, data_gen).filter(f.col('a')))

# coalesce batch happens after a filter, but only if something else happens on the GPU after that
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen + array_gens_sample + struct_gens_sample + map_gens_sample, ids=idfn)
def test_filter_with_project(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, BooleanGen(), data_gen).filter(f.col('a')).selectExpr('*', 'a as a2'))

@pytest.mark.parametrize('expr', [f.lit(True), f.lit(False), f.lit(None).cast('boolean')], ids=idfn)
def test_filter_with_lit(expr):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, LongGen()).filter(expr))

# Spark supports two different versions of 'IN', and it depends on the spark.sql.optimizer.inSetConversionThreshold conf
# This is to test entries under that value.
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
def test_in(data_gen):
    # nulls are not supported for in on the GPU yet
    num_entries = int(with_cpu_session(lambda spark: spark.conf.get('spark.sql.optimizer.inSetConversionThreshold'))) - 1
    # we have to make the scalars in a session so negative scales in decimals are supported
    scalars = with_cpu_session(lambda spark: list(gen_scalars(data_gen, num_entries, force_no_nulls=not isinstance(data_gen, NullGen))))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').isin(scalars)))

# Spark supports two different versions of 'IN', and it depends on the spark.sql.optimizer.inSetConversionThreshold conf
# This is to test entries over that value.
@pytest.mark.parametrize('data_gen', eq_gens_with_decimal_gen, ids=idfn)
def test_in_set(data_gen):
    # nulls are not supported for in on the GPU yet
    num_entries = int(with_cpu_session(lambda spark: spark.conf.get('spark.sql.optimizer.inSetConversionThreshold'))) + 1
    # we have to make the scalars in a session so negative scales in decimals are supported
    scalars = with_cpu_session(lambda spark: list(gen_scalars(data_gen, num_entries, force_no_nulls=not isinstance(data_gen, NullGen))))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').isin(scalars)))
