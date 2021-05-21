# Copyright (c) 2021, NVIDIA CORPORATION.
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
from pyspark.sql.types import *
from string_test import mk_str_gen
import pyspark.sql.functions as f

nested_gens = [ArrayGen(LongGen()),
               StructGen([("a", LongGen())]),
               MapGen(StringGen(pattern='key_[0-9]', nullable=False), StringGen())]
# additional test for NonNull Array because of https://github.com/rapidsai/cudf/pull/8181
non_nested_array_gens = [ArrayGen(sub_gen, nullable=nullable)
                         for nullable in [True, False] for sub_gen in all_gen + [null_gen]]

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

@pytest.mark.parametrize('data_gen', all_gen + nested_gens, ids=idfn)
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
