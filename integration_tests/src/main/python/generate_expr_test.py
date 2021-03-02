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

from asserts import assert_gpu_and_cpu_are_equal_collect
from data_gen import *
from marks import ignore_order
from pyspark.sql.types import *
import pyspark.sql.functions as f
from spark_session import with_cpu_session

def four_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([
        ('a', gen),
        ('b', gen),
        ('c', gen),
        ('d', gen)], nullable=False), length=length, seed=seed)

"""
below tests are temporarily disabled because explode_position has not been supported in cuDF-nightly  

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_makearray(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).selectExpr('posexplode(array(b, c, d))', 'a'))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_litarray(data_gen):
    array_lit = gen_scalar(ArrayGen(data_gen, min_length=3, max_length=3, nullable=False))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).select(f.col('a'), f.col('b'), f.col('c'), 
                f.posexplode(array_lit)))
"""

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_makearray(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).selectExpr('a', 'explode(array(b, c, d))'))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_litarray(data_gen):
    array_lit = gen_scalar(ArrayGen(data_gen, min_length=3, max_length=3, nullable=False))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : four_op_df(spark, data_gen).select(f.col('a'), f.col('b'), f.col('c'), 
                f.explode(array_lit)))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
def test_explode_split_string():
    str_gen = StringGen('([ABC]{0,3}_?){0,7}').with_special_case('').with_special_pattern('.{0,10}')
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, int_gen, str_gen).selectExpr('a', 'explode(split(b, "_"))'))
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, int_gen, str_gen).selectExpr(
                'a', 'explode(split(b, "_")) as c').selectExpr(
                'a', 'explode(split(c, "A"))'))

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
def test_explode_from_parquet(spark_tmp_path):
    data_path = spark_tmp_path + '/PARQUET_DATA'
    data_gen = [int_gen, ArrayGen(long_gen)]
    with_cpu_session(
            lambda spark: two_col_df(spark, *data_gen).write.parquet(data_path))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.parquet(data_path).selectExpr('a', 'explode(b)'))
    # test with nested array
    data_path1 = spark_tmp_path + '/PARQUET_DATA1'
    data_gen1 = [int_gen, ArrayGen(ArrayGen(long_gen))]
    with_cpu_session(
            lambda spark: two_col_df(spark, *data_gen1).write.parquet(data_path1))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.read.parquet(data_path1).selectExpr(
            'a', 'explode(b) as c').selectExpr('a', 'explode(c)'))
