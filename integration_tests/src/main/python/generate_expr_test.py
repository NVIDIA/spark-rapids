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

def four_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([
        ('a', gen),
        ('b', gen),
        ('c', gen),
        ('d', gen)], nullable=False), length=length, seed=seed)

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

# use a small `spark.rapids.sql.batchSizeBytes` to enforce input batches splitting up during explode
conf_to_enforce_split_input = {'spark.rapids.sql.batchSizeBytes': '8192'}

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_nested_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'explode(b) as c').selectExpr('a', 'explode(c)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_outer_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'explode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_explode_outer_nested_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'explode_outer(b) as c').selectExpr('a', 'explode_outer(c)'),
        conf=conf_to_enforce_split_input)

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

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_nested_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'posexplode(b) as (pos, c)').selectExpr('a', 'pos', 'posexplode(c)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_outer_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr('a', 'posexplode_outer(b)'),
        conf=conf_to_enforce_split_input)

#sort locally because of https://github.com/NVIDIA/spark-rapids/issues/84
# After 3.1.0 is the min spark version we can drop this
@ignore_order(local=True)
@pytest.mark.parametrize('data_gen', all_gen, ids=idfn)
def test_posexplode_nested_outer_array_data(spark_tmp_path, data_gen):
    data_gen = [int_gen, ArrayGen(ArrayGen(data_gen))]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, *data_gen).selectExpr(
            'a', 'posexplode_outer(b) as (pos, c)').selectExpr(
            'a', 'pos', 'posexplode_outer(c)'),
        conf=conf_to_enforce_split_input)
