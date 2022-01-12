# Copyright (c) 2021-2022, NVIDIA CORPORATION.
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
from pyspark.sql.types import *
from spark_session import with_cpu_session
from string_test import mk_str_gen
import pyspark.sql.functions as f

nested_gens = [ArrayGen(LongGen()), ArrayGen(decimal_gen_38_10),
               StructGen([("a", LongGen()), ("b", decimal_gen_38_10)]),
               MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen())]
# additional test for NonNull Array because of https://github.com/rapidsai/cudf/pull/8181
non_nested_array_gens = [ArrayGen(sub_gen, nullable=nullable)
                         for nullable in [True, False]
                         for sub_gen in all_gen + [null_gen]]
non_nested_array_gens_dec128 = [ArrayGen(sub_gen, nullable=nullable)
                                for nullable in [True, False]
                                for sub_gen in all_gen + [null_gen] + decimal_128_gens_no_neg]

@pytest.mark.parametrize('data_gen', non_nested_array_gens, ids=idfn)
def test_concat_list(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr('concat(a)'))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).selectExpr('concat(a, b)'))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: three_col_df(spark, data_gen, data_gen, data_gen
                                   ).selectExpr('concat(a, b, c)'))

def test_empty_concat_list():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, ArrayGen(LongGen())).selectExpr('concat()'))

@pytest.mark.parametrize('data_gen', non_nested_array_gens, ids=idfn)
def test_concat_list_with_lit(data_gen):
    array_lit = gen_scalar(data_gen)
    array_lit2 = gen_scalar(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).select(
            f.concat(f.col('a'),
                     f.col('b'),
                     f.lit(array_lit).cast(data_gen.data_type))))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).select(
            f.concat(f.lit(array_lit).cast(data_gen.data_type),
                     f.col('a'),
                     f.lit(array_lit2).cast(data_gen.data_type))))

    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: binary_op_df(spark, data_gen).select(
            f.concat(f.lit(array_lit).cast(data_gen.data_type),
                     f.lit(array_lit2).cast(data_gen.data_type))))

def test_concat_string():
    gen = mk_str_gen('.{0,5}')
    (s1, s2) = gen_scalars(gen, 2, force_no_nulls=True)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: binary_op_df(spark, gen).select(
                f.concat(),
                f.concat(f.col('a')),
                f.concat(s1),
                f.concat(f.col('a'), f.col('b')),
                f.concat(f.col('a'), f.col('b'), f.col('a')),
                f.concat(s1, f.col('b')),
                f.concat(f.col('a'), s2),
                f.concat(f.lit(None).cast('string'), f.col('b')),
                f.concat(f.col('a'), f.lit(None).cast('string')),
                f.concat(f.lit(''), f.col('b')),
                f.concat(f.col('a'), f.lit(''))))

@pytest.mark.parametrize('data_gen', all_gen + decimal_128_gens_no_neg + nested_gens, ids=idfn)
@pytest.mark.parametrize('size_of_null', ['true', 'false'], ids=idfn)
def test_size_of_array(data_gen, size_of_null):
    gen = ArrayGen(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, gen).selectExpr('size(a)'),
            conf={'spark.sql.legacy.sizeOfNull': size_of_null})

@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
@pytest.mark.parametrize('size_of_null', ['true', 'false'], ids=idfn)
def test_size_of_map(data_gen, size_of_null):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr('size(a)'),
            conf={'spark.sql.legacy.sizeOfNull': size_of_null})

@pytest.mark.parametrize('data_gen', non_nested_array_gens_dec128, ids=idfn)
@pytest.mark.parametrize('is_ascending', [True, False], ids=idfn)
def test_sort_array(data_gen, is_ascending):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen).select(
            f.sort_array(f.col('a'), is_ascending)))

@pytest.mark.parametrize('data_gen', non_nested_array_gens_dec128, ids=idfn)
@pytest.mark.parametrize('is_ascending', [True, False], ids=idfn)
def test_sort_array_lit(data_gen, is_ascending):
    array_lit = gen_scalar(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, data_gen, length=10).select(
            f.sort_array(f.lit(array_lit), is_ascending)))

# We must restrict the length of sequence, since we may suffer the exception
# "Too long sequence: 2147483745. Should be <= 2147483632" or OOM.
sequence_integral_gens = [
    ByteGen(nullable=False, min_val=-20, max_val=20, special_cases=[]),
    ShortGen(nullable=False, min_val=-20, max_val=20, special_cases=[]),
    IntegerGen(nullable=False, min_val=-20, max_val=20, special_cases=[]),
    LongGen(nullable=False, min_val=-20, max_val=20, special_cases=[])
]

@pytest.mark.parametrize('data_gen', sequence_integral_gens, ids=idfn)
def test_sequence_without_step(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark :
        three_col_df(spark, data_gen, data_gen, data_gen)
            .selectExpr("sequence(a, b)",
                        "sequence(a, 0)",
                        "sequence(0, b)"))

# This function is to generate the correct sequence data according to below limitations.
#      (step > num.zero && start <= stop)
#        || (step < num.zero && start >= stop)
#        || (step == num.zero && start == stop)
def get_sequence_data(data_gen, length=2048):
    rand = random.Random(0)
    data_gen.start(rand)
    list = []
    for index in range(length):
        start = data_gen.gen()
        stop = data_gen.gen()
        step = data_gen.gen()
        # decide the direction of step
        if start < stop:
            step = abs(step) + 1
        elif start == stop:
            step = 0
        else:
            step = -(abs(step) + 1)
        list.append(tuple([start, stop, step]))
    # add special case
    list.append(tuple([2, 2, 0]))
    return list

def get_sequence_df(spark, data, data_type):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize(data),
        StructType([StructField('a', data_type), StructField('b', data_type), StructField('c', data_type)]))

# test below case
# (2, -1, -1)
# (2, 5, 2)
# (2, 2, 0)
@pytest.mark.parametrize('data_gen', sequence_integral_gens, ids=idfn)
def test_sequence_with_step_case1(data_gen):
    data = get_sequence_data(data_gen)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark :
        get_sequence_df(spark, data, data_gen.data_type)
            .selectExpr("sequence(a, b, c)"))

sequence_three_cols_integral_gens = [
    (ByteGen(nullable=False, min_val=-10, max_val=10, special_cases=[]),
     ByteGen(nullable=False, min_val=30, max_val=50, special_cases=[]),
     ByteGen(nullable=False, min_val=1, max_val=10, special_cases=[])),
    (ShortGen(nullable=False, min_val=-10, max_val=10, special_cases=[]),
     ShortGen(nullable=False, min_val=30, max_val=50, special_cases=[]),
     ShortGen(nullable=False, min_val=1, max_val=10, special_cases=[])),
    (IntegerGen(nullable=False, min_val=-10, max_val=10, special_cases=[]),
     IntegerGen(nullable=False, min_val=30, max_val=50, special_cases=[]),
     IntegerGen(nullable=False, min_val=1, max_val=10, special_cases=[])),
    (LongGen(nullable=False, min_val=-10, max_val=10, special_cases=[-10, 10]),
     LongGen(nullable=False, min_val=30, max_val=50, special_cases=[30, 50]),
     LongGen(nullable=False, min_val=1, max_val=10, special_cases=[1, 10])),
]

# Test the scalar case for the data start < stop and step > 0
@pytest.mark.parametrize('start_gen,stop_gen,step_gen', sequence_three_cols_integral_gens, ids=idfn)
def test_sequence_with_step_case2(start_gen, stop_gen, step_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark :
        three_col_df(spark, start_gen, stop_gen, step_gen)
            .selectExpr("sequence(a, b, c)",
                        "sequence(a, b, 2)",
                        "sequence(a, 20, c)",
                        "sequence(a, 20, 2)",
                        "sequence(0, b, c)",
                        "sequence(0, 4, c)",
                        "sequence(0, b, 3)"),)
