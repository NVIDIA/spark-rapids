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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from spark_session import is_before_spark_311
from data_gen import *
from marks import ignore_order, allow_non_gpu
import pyspark.sql.functions as f

nested_scalar_mark=pytest.mark.xfail(reason="https://github.com/NVIDIA/spark-rapids/issues/1459")
@pytest.mark.parametrize('data_gen', [pytest.param((StructGen([['child0', DecimalGen(7, 2)]]),
                                                    StructGen([['child1', IntegerGen()]])), marks=nested_scalar_mark),
                                      (StructGen([['child0', DecimalGen(7, 2)]], nullable=False),
                                       StructGen([['child1', IntegerGen()]], nullable=False))], ids=idfn)
@pytest.mark.skipif(is_before_spark_311(), reason="This is supported only in Spark 3.1.1+")
# This tests the union of DF of structs with different types of cols as long as the struct itself
# isn't null. This is a limitation in cudf because we don't support nested types as literals
def test_union_struct_missing_children(data_gen):
    left_gen, right_gen = data_gen
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, left_gen).unionByName(binary_op_df(
            spark, right_gen), True))

@pytest.mark.parametrize('data_gen', all_gen + [all_basic_struct_gen, StructGen([['child0', DecimalGen(7, 2)]])], ids=idfn)
# This tests union of two DFs of two cols each. The types of the left col and right col is the same
def test_union(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).union(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('data_gen', all_gen + [pytest.param(all_basic_struct_gen, marks=nested_scalar_mark),
                                                pytest.param(StructGen([[ 'child0', DecimalGen(7, 2)]], nullable=False), marks=nested_scalar_mark)])
@pytest.mark.skipif(is_before_spark_311(), reason="This is supported only in Spark 3.1.1+")
# This tests the union of two DFs of structs with missing child column names. The missing child
# column will be replaced by nulls in the output DF. This is a feature added in 3.1+
def test_union_by_missing_col_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).withColumnRenamed("a", "x")
                                .unionByName(binary_op_df(spark, data_gen).withColumnRenamed("a", "y"), True))

@pytest.mark.parametrize('data_gen', all_gen + [all_basic_struct_gen, StructGen([['child0', DecimalGen(7, 2)]])], ids=idfn)
def test_union_by_name(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).unionByName(binary_op_df(spark, data_gen)))

@pytest.mark.parametrize('num_parts', [1, 10, 100, 1000, 2000], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
def test_coalesce_df(num_parts, length):
    #This should change eventually to be more than just the basic gens
    gen_list = [('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens)]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen_list, length=length).coalesce(num_parts))

@pytest.mark.parametrize('data_gen', [
    pytest.param([('_c' + str(i), gen) for i, gen in enumerate(all_basic_gens + decimal_gens)]),
    pytest.param([('s', StructGen([['child0', all_basic_struct_gen]]))]),
    pytest.param([('a', ArrayGen(string_gen))]),
], ids=idfn)
@pytest.mark.parametrize('num_parts', [1, 10, 2345], ids=idfn)
@pytest.mark.parametrize('length', [0, 2048, 4096], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
def test_repartition_df(data_gen, num_parts, length):
    from pyspark.sql.functions import lit
    assert_gpu_and_cpu_are_equal_collect(
            # Add a computed column to avoid shuffle being optimized back to a CPU shuffle
            lambda spark : gen_df(spark, data_gen, length=length).withColumn('x', lit(1)).repartition(num_parts),
            # disable sort before shuffle so round robin works for arrays
            conf = {'spark.sql.execution.sortBeforeRepartition': 'false',
                'spark.sql.legacy.allowNegativeScaleOfDecimal': 'true'})

@allow_non_gpu('ShuffleExchangeExec', 'RoundRobinPartitioning')
@pytest.mark.parametrize('data_gen', [[('a', ArrayGen(string_gen))],
    [('a', StructGen([
      ('a_1', StructGen([
        ('a_1_1', int_gen),
        ('a_1_2', float_gen),
        ('a_1_3', double_gen)
      ])),
      ('b_1', long_gen)
    ]))],
    [('a', simple_string_to_string_map_gen)]], ids=idfn)
@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
def test_round_robin_sort_fallback(data_gen):
    from pyspark.sql.functions import lit
    assert_gpu_fallback_collect(
            # Add a computed column to avoid shuffle being optimized back to a CPU shuffle like in test_repartition_df
            lambda spark : gen_df(spark, data_gen).withColumn('x', lit(1)).repartition(13),
            'ShuffleExchangeExec')

@ignore_order(local=True) # To avoid extra data shuffle by 'sort on Spark' for this repartition test.
@pytest.mark.parametrize('num_parts', [1, 2, 10, 17, 19, 32], ids=idfn)
@pytest.mark.parametrize('gen', [
    ([('a', boolean_gen)], ['a']), 
    ([('a', byte_gen)], ['a']), 
    ([('a', short_gen)], ['a']),
    ([('a', int_gen)], ['a']),
    ([('a', long_gen)], ['a']),
    ([('a', float_gen)], ['a']),
    ([('a', double_gen)], ['a']),
    ([('a', timestamp_gen)], ['a']),
    ([('a', date_gen)], ['a']),
    ([('a', decimal_gen_default)], ['a']),
    ([('a', decimal_gen_neg_scale)], ['a']),
    ([('a', decimal_gen_scale_precision)], ['a']),
    ([('a', decimal_gen_same_scale_precision)], ['a']),
    ([('a', decimal_gen_64bit)], ['a']),
    ([('a', string_gen)], ['a']),
    ([('a', null_gen)], ['a']),
    ([('a', StructGen([('c0', boolean_gen), ('c1', StructGen([('c1_0', byte_gen), ('c1_1', string_gen), ('c1_2', boolean_gen)]))]))], ['a']), 
    ([('a', long_gen), ('b', StructGen([('b1', long_gen)]))], ['a']),
    ([('a', long_gen), ('b', ArrayGen(long_gen, max_length=2))], ['a']),
    ([('a', byte_gen)], [f.col('a') - 5]), 
    ([('a', long_gen)], [f.col('a') + 15]), 
    ([('a', byte_gen), ('b', boolean_gen)], ['a', 'b']),
    ([('a', short_gen), ('b', string_gen)], ['a', 'b']),
    ([('a', int_gen), ('b', byte_gen)], ['a', 'b']),
    ([('a', long_gen), ('b', null_gen)], ['a', 'b']),
    ([('a', byte_gen), ('b', boolean_gen), ('c', short_gen)], ['a', 'b', 'c']),
    ([('a', float_gen), ('b', double_gen), ('c', short_gen)], ['a', 'b', 'c']),
    ([('a', timestamp_gen), ('b', date_gen), ('c', int_gen)], ['a', 'b', 'c']),
    ([('a', short_gen), ('b', string_gen), ('c', int_gen)], ['a', 'b', 'c']),
    ([('a', decimal_gen_default), ('b', decimal_gen_64bit), ('c', decimal_gen_scale_precision)], ['a', 'b', 'c']),
    ], ids=idfn)
def test_hash_repartition_exact(gen, num_parts):
    data_gen = gen[0]
    part_on = gen[1]
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen, length=1024)\
                    .repartition(num_parts, *part_on)\
                    .withColumn('id', f.spark_partition_id())\
                    .withColumn('hashed', f.hash(*part_on))\
                    .selectExpr('*', 'pmod(hashed, {})'.format(num_parts)),
            conf = allow_negative_scale_of_decimal_conf)
