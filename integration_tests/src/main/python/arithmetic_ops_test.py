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
from data_gen import random_df_from_schema
from pyspark.sql.types import *
import pyspark.sql.functions as f

def binary_op_df(spark, data_type):
    schema = StructType([
        StructField("a", data_type),
        StructField("b", data_type)])
    return random_df_from_schema(spark, schema)

def to_cast_string(spark_type):
    if isinstance(spark_type, ByteType):
        return "BYTE"
    elif isinstance(spark_type, ShortType):
        return "SHORT"
    elif isinstance(spark_type, IntegerType):
        return "INT"
    elif isinstance(spark_type, LongType):
        return "LONG"
    elif isinstance(spark_type, FloatType):
        return "FLOAT"
    elif isinstance(spark_type, DoubleType):
        return "DOUBLE"
    else:
        raise RuntimeError("CAST TO TYPE {} NOT SUPPORTED YET".format(spark_type))


def idfn(val):
    return str(val)

numeric_types = [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType()]
integral_types = [ByteType(), ShortType(), IntegerType(), LongType()]

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_scalar_addition(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_addition(data_type):
    # Using iterator for now just so some test covers this path
    # this should really only be used for tests that produce a lot of data
    assert_gpu_and_cpu_are_equal_iterator(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a + b'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_scalar_subtraction(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_subtraction(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a - b'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_scalar_multiplication(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).select(
                f.col('a') * f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) * f.col('b'),
                f.lit(None).cast(data_type) * f.col('a'),
                f.col('b') * f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_multiplication(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a * b'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_scalar_division(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).select(
                f.col('a') / f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) / f.col('b'),
                f.lit(None).cast(data_type) / f.col('a'),
                f.col('b') / f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_division(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a / b'))

@pytest.mark.parametrize('data_type', integral_types, ids=idfn)
def test_scalar_int_division(data_type):
    string_type = to_cast_string(data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr(
                'a DIV cast(100 as {})'.format(string_type),
                'cast(-12 as {}) DIV b'.format(string_type),
                'null DIV a',
                'b DIV null'))

@pytest.mark.parametrize('data_type', integral_types, ids=idfn)
def test_columnar_int_division(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a DIV b'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_scalar_mod(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).select(
                f.col('a') % f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) % f.col('b'),
                f.lit(None).cast(data_type) % f.col('a'),
                f.col('b') % f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_mod(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_type).selectExpr('a % b'))


