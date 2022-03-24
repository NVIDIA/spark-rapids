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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu
from pyspark.sql.types import *
import pyspark.sql.functions as f

orderable_not_null_gen = [ByteGen(nullable=False), ShortGen(nullable=False), IntegerGen(nullable=False),
        LongGen(nullable=False), FloatGen(nullable=False), DoubleGen(nullable=False), BooleanGen(nullable=False),
        TimestampGen(nullable=False), DateGen(nullable=False), StringGen(nullable=False),
        DecimalGen(precision=7, scale=3, nullable=False), DecimalGen(precision=12, scale=2, nullable=False),
        DecimalGen(precision=20, scale=2, nullable=False)]

@allow_non_gpu('SortExec', 'ShuffleExchangeExec', 'RangePartitioning', 'SortOrder')
@pytest.mark.parametrize('data_gen', [StringGen(nullable=False)], ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').cast(BinaryType())], ids=idfn)
def test_sort_binary_fallback(data_gen, order):
    assert_gpu_fallback_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order),
            "SortExec")

@allow_non_gpu('ProjectExec', 'ShuffleExchangeExec', 'RangePartitioning')
@pytest.mark.parametrize('data_gen', [StringGen(nullable=False)], ids=idfn)
def test_sort_nonbinary_carry_binary(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen)
                .withColumn("binary_string", f.col("a").cast(BinaryType()))
                .orderBy(f.col('a')))

@pytest.mark.parametrize('data_gen', orderable_gens + orderable_not_null_gen, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_orderby(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order))

@pytest.mark.parametrize('shuffle_parts', [
    pytest.param(1),
    pytest.param(200)
])
@pytest.mark.parametrize('stable_sort', ['STABLE', 'OUTOFCORE'])
@pytest.mark.parametrize('data_gen', [
    pytest.param(all_basic_struct_gen),
    pytest.param(StructGen([['child0', decimal_gen_128bit]])),
    pytest.param(StructGen([['child0', all_basic_struct_gen]])),
    pytest.param(ArrayGen(string_gen),
        marks=pytest.mark.xfail(reason="arrays are not supported")),
    pytest.param(MapGen(StringGen(pattern='key_[0-9]', nullable=False), simple_string_to_string_map_gen),
        marks=pytest.mark.xfail(reason="maps are not supported")),
], ids=idfn)
@pytest.mark.parametrize('order', [
    pytest.param(f.col('a').asc()),
    pytest.param(f.col('a').asc_nulls_first()),
    pytest.param(f.col('a').asc_nulls_last(),
        marks=pytest.mark.xfail(reason='opposite null order not supported')),
    pytest.param(f.col('a').desc()),
    pytest.param(f.col('a').desc_nulls_first(),
        marks=pytest.mark.xfail(reason='opposite null order not supported')),
    pytest.param(f.col('a').desc_nulls_last()),
], ids=idfn)
def test_single_nested_orderby_plain(data_gen, order, shuffle_parts, stable_sort):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order),
            conf = {
                'spark.sql.shuffle.partitions': shuffle_parts,
                'spark.rapids.sql.stableSort.enabled': stable_sort == 'STABLE'
            })

# only default null ordering for direction is supported for nested types
@allow_non_gpu('SortExec', 'ShuffleExchangeExec', 'RangePartitioning', 'SortOrder')
@pytest.mark.parametrize('data_gen', [
    pytest.param(all_basic_struct_gen),
    pytest.param(StructGen([['child0', all_basic_struct_gen]])),
], ids=idfn)
@pytest.mark.parametrize('order', [
    pytest.param(f.col('a').asc_nulls_last()),
    pytest.param(f.col('a').desc_nulls_first()),
], ids=idfn)
def test_single_nested_orderby_fallback_for_nullorder(data_gen, order):
    assert_gpu_fallback_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order),
            "SortExec")

# SPARK CPU itself has issue with negative scale for take ordered and project
orderable_without_neg_decimal = [n for n in (orderable_gens + orderable_not_null_gen) if not (isinstance(n, DecimalGen) and n.scale < 0)]
@pytest.mark.parametrize('data_gen', orderable_without_neg_decimal, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_orderby_with_limit(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).orderBy(order).limit(100))

@pytest.mark.parametrize('order,data_gen', [
    pytest.param(f.col('a').asc(), all_basic_struct_gen),
    pytest.param(f.col('a').asc_nulls_first(), all_basic_struct_gen),
    pytest.param(f.col('a').desc(), all_basic_struct_gen),
    pytest.param(f.col('a').desc_nulls_last(), all_basic_struct_gen)
], ids=idfn)
def test_single_nested_orderby_with_limit(data_gen, order):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).orderBy(order).limit(100),
        conf = {
            'spark.rapids.allowCpuRangePartitioning': False
        })

@allow_non_gpu('TakeOrderedAndProjectExec', 'SortOrder')
@pytest.mark.parametrize('order,data_gen', [
    pytest.param(f.col('a').asc_nulls_last(), all_basic_struct_gen),
    pytest.param(f.col('a').desc_nulls_first(), all_basic_struct_gen),
    pytest.param(f.col('a').asc(), ArrayGen(string_gen)),
    pytest.param(f.col('a').asc_nulls_last(), ArrayGen(string_gen)),
    pytest.param(f.col('a').desc(), ArrayGen(string_gen)),
    pytest.param(f.col('a').desc_nulls_first(), ArrayGen(string_gen))
], ids=idfn)
def test_single_nested_orderby_with_limit_fallback(data_gen, order):
    assert_gpu_fallback_collect(
        lambda spark : unary_op_df(spark, data_gen).orderBy(order).limit(100),
        "TakeOrderedAndProjectExec",
        conf = {
            'spark.rapids.allowCpuRangePartitioning': False
        })

@pytest.mark.parametrize('data_gen', orderable_gens + orderable_not_null_gen, ids=idfn)
@pytest.mark.parametrize('order', [f.col('a').asc(), f.col('a').asc_nulls_last(), f.col('a').desc(), f.col('a').desc_nulls_first()], ids=idfn)
def test_single_sort_in_part(data_gen, order):
    # We set `num_slices` to handle https://github.com/NVIDIA/spark-rapids/issues/2477
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, num_slices=12).sortWithinPartitions(order))

@pytest.mark.parametrize('data_gen', [
    pytest.param(all_basic_struct_gen),
    pytest.param(struct_gen_decimal128),
    pytest.param(StructGen([['child0', all_basic_struct_gen]])),
    pytest.param(StructGen([['child0', struct_gen_decimal128]])),
], ids=idfn)
@pytest.mark.parametrize('order', [
    pytest.param(f.col('a').asc()),
    pytest.param(f.col('a').asc_nulls_first()),
    pytest.param(f.col('a').asc_nulls_last(),
                 marks=pytest.mark.xfail(reason='opposite null order not supported')),
    pytest.param(f.col('a').desc()),
    pytest.param(f.col('a').desc_nulls_first(),
                 marks=pytest.mark.xfail(reason='opposite null order not supported')),
    pytest.param(f.col('a').desc_nulls_last()),
], ids=idfn)
@pytest.mark.parametrize('stable_sort', ['STABLE', 'OUTOFCORE'], ids=idfn)
def test_single_nested_sort_in_part(data_gen, order, stable_sort):
    sort_conf = {'spark.rapids.sql.stableSort.enabled': stable_sort == 'STABLE'}
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen, num_slices=12).sortWithinPartitions(order),
        conf=sort_conf)

orderable_gens_sort = [byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen,
        boolean_gen, timestamp_gen, date_gen, string_gen, null_gen, StructGen([('child0', long_gen)])] + decimal_gens
@pytest.mark.parametrize('data_gen', orderable_gens_sort, ids=idfn)
def test_multi_orderby(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).orderBy(f.col('a'), f.col('b').desc()))

# SPARK CPU itself has issue with negative scale for take ordered and project
orderable_gens_sort_without_neg_decimal = [n for n in orderable_gens_sort if not (isinstance(n, DecimalGen) and n.scale < 0)]
@pytest.mark.parametrize('data_gen', orderable_gens_sort_without_neg_decimal, ids=idfn)
def test_multi_orderby_with_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).orderBy(f.col('a'), f.col('b').desc()).limit(100))

# We added in a partitioning optimization to take_ordered_and_project
# This should trigger it.
@pytest.mark.parametrize('data_gen', orderable_gens_sort_without_neg_decimal, ids=idfn)
def test_multi_orderby_with_limit_single_part(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).coalesce(1).orderBy(f.col('a'), f.col('b').desc()).limit(100))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_orderby_with_processing(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # avoid ambiguity in the order by statement for floating point by including a as a backup ordering column
            lambda spark : unary_op_df(spark, data_gen).orderBy(f.lit(100) - f.col('a'), f.col('a')))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_orderby_with_processing_and_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            # avoid ambiguity in the order by statement for floating point by including a as a backup ordering column
            lambda spark : unary_op_df(spark, data_gen).orderBy(f.lit(100) - f.col('a'), f.col('a')).limit(100))


# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [StructGen([('child0', long_gen)])], ids=idfn)
def test_single_nested_orderby_with_processing_and_limit(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        # avoid ambiguity in the order by statement for floating point by including a as a backup ordering column
        lambda spark : unary_op_df(spark, data_gen)\
            .orderBy(f.struct(f.lit(100) - f.col('a.child0')), f.col('a'))\
            .limit(100))

# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [byte_gen, long_gen, float_gen], ids=idfn)
def test_single_orderby_with_skew(data_gen):
    # When doing range partitioning the upstream data is sampled to try and get the bounds for cutoffs.
    # If the data comes back with skewed partitions then those partitions will be resampled for more data.
    # This is to try and trigger it to happen.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen)\
                    .selectExpr('a', 'random(1) > 0.5 as b')\
                    .repartition(f.col('b'))\
                    .orderBy(f.col('a'))\
                    .selectExpr('a'))


# We are not trying all possibilities, just doing a few with numbers so the query works.
@pytest.mark.parametrize('data_gen', [all_basic_struct_gen, StructGen([['child0', all_basic_struct_gen]])], ids=idfn)
@pytest.mark.parametrize('stable_sort', ['STABLE', 'OUTOFCORE'], ids=idfn)
def test_single_nested_orderby_with_skew(data_gen, stable_sort):
    sort_conf = {'spark.rapids.sql.stableSort.enabled': stable_sort == 'STABLE'}
    # When doing range partitioning the upstream data is sampled to try and get the bounds for cutoffs.
    # If the data comes back with skewed partitions then those partitions will be resampled for more data.
    # This is to try and trigger it to happen.
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen) \
            .selectExpr('a', 'random(1) > 0.5 as b') \
            .repartition(f.col('b')) \
            .orderBy(f.col('a')) \
            .selectExpr('a'),
        conf=sort_conf)


# This is primarily to test the out of core sort with multiple batches. For this we set the data size to
# be relatively large (1 MiB across all tasks) and the target size to be small (16 KiB). This means we
# should see around 64 batches of data. So this is the most valid if there are less than 64 tasks
# in the cluster, but it should still work even then.
@pytest.mark.parametrize('data_gen', [long_gen, StructGen([('child0', long_gen)])], ids=idfn)
@pytest.mark.parametrize('stable_sort', ['STABLE', 'OUTOFCORE'], ids=idfn)
def test_large_orderby(data_gen, stable_sort):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen, length=1024*128)\
                    .orderBy(f.col('a')),
            conf={
                'spark.rapids.sql.batchSizeBytes': '16384',
                'spark.rapids.sql.stableSort.enabled': stable_sort == 'STABLE'
            })

# This is similar to test_large_orderby, but here we want to test some types
# that are not being sorted on, but are going along with it
@pytest.mark.parametrize('data_gen', [byte_gen,
    string_gen,
    float_gen,
    date_gen,
    timestamp_gen,
    decimal_gen_64bit,
    decimal_gen_128bit,
    StructGen([('child1', byte_gen)]),
    simple_string_to_string_map_gen,
    ArrayGen(byte_gen, max_length=5)], ids=idfn)
@pytest.mark.order(2)
def test_large_orderby_nested_ridealong(data_gen):
    # We use a LongRangeGen to avoid duplicate keys that can cause ambiguity in the sort
    #  results, especially on distributed clusters.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, LongRangeGen(), data_gen, length=1024*127)\
                    .orderBy(f.col('a').desc()),
            conf = {'spark.rapids.sql.batchSizeBytes': '16384'})

@pytest.mark.parametrize('data_gen', [byte_gen,
    string_gen,
    float_gen,
    date_gen,
    timestamp_gen,
    decimal_gen_64bit,
    decimal_gen_128bit,
    StructGen([('child1', byte_gen)]),
    simple_string_to_string_map_gen,
    ArrayGen(byte_gen, max_length=5),
    ArrayGen(decimal_gen_128bit, max_length=5)], ids=idfn)
@pytest.mark.order(2)
def test_orderby_nested_ridealong_limit(data_gen):
    # We use a LongRangeGen to avoid duplicate keys that can cause ambiguity in the sort
    #  results, especially on distributed clusters.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, LongRangeGen(), data_gen)\
                    .orderBy(f.col('a').desc()).limit(100))
