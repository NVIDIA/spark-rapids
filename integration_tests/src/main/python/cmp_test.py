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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_are_equal_iterator
from data_gen import *
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f

def two_col_df(spark, a_gen, b_gen, length=2048, seed=0):
    gen = StructGen([('a', a_gen),('b', b_gen)], nullable=False)
    return gen_df(spark, gen, length=length, seed=seed)

def binary_op_df(spark, gen, length=2048, seed=0):
    return two_col_df(spark, gen, gen, length=length, seed=seed)

def unary_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([('a', gen)], nullable=False), length=length, seed=seed)

orderable_gens = [ByteGen(), ShortGen(), IntegerGen(), LongGen(), FloatGen(), DoubleGen(), StringGen(), BooleanGen(), DateGen(), TimestampGen()]
cmp_gens = [ByteGen(), ShortGen(), IntegerGen(), LongGen(), FloatGen(), DoubleGen(), StringGen(), BooleanGen(), DateGen(), TimestampGen()]

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_eq(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') == s1,
                s2 == f.col('b'),
                f.lit(None).cast(data_type) == f.col('a'),
                f.col('b') == f.lit(None).cast(data_type),
                f.col('a') == f.col('b')))

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_ne(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') != s1,
                s2 != f.col('b'),
                f.lit(None).cast(data_type) != f.col('a'),
                f.col('b') != f.lit(None).cast(data_type),
                f.col('a') != f.col('b')))

@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_lt(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') < s1,
                s2 < f.col('b'),
                f.lit(None).cast(data_type) < f.col('a'),
                f.col('b') < f.lit(None).cast(data_type),
                f.col('a') < f.col('b')))

@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_lte(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') <= s1,
                s2 <= f.col('b'),
                f.lit(None).cast(data_type) <= f.col('a'),
                f.col('b') <= f.lit(None).cast(data_type),
                f.col('a') <= f.col('b')))

@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_gt(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') > s1,
                s2 > f.col('b'),
                f.lit(None).cast(data_type) > f.col('a'),
                f.col('b') > f.lit(None).cast(data_type),
                f.col('a') > f.col('b')))

@pytest.mark.parametrize('data_gen', orderable_gens, ids=idfn)
def test_gte(data_gen):
    (s1, s2) = gen_scalars(data_gen, 2, force_no_nulls=True)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') >= s1,
                s2 >= f.col('b'),
                f.lit(None).cast(data_type) >= f.col('a'),
                f.col('b') >= f.lit(None).cast(data_type),
                f.col('a') >= f.col('b')))

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_isnull(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnull(f.col('a'))))

@pytest.mark.parametrize('data_gen', [FloatGen(), DoubleGen()], ids=idfn)
def test_isnan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(
                f.isnan(f.col('a'))))

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_dropna_any(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna())

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_dropna_all(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).dropna(how='all'))

@pytest.mark.parametrize('data_gen', cmp_gens, ids=idfn)
def test_in(data_gen):
    # nulls are not supported for in on the GPU yet
    scalars = list(gen_scalars(data_gen, 5, force_no_nulls=True))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).select(f.col('a').isin(scalars)))

