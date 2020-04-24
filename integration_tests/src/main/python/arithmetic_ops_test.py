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

def two_col_df(spark, a_data_type, b_data_type, length=2048, seed=0):
    schema = StructType([
        StructField("a", a_data_type),
        StructField("b", b_data_type)])
    return random_df_from_schema(spark, schema, length=length, seed=seed)

def binary_op_df(spark, data_type, length=2048, seed=0):
    return two_col_df(spark, data_type, data_type, length=length, seed=seed)

def unary_op_df(spark, data_type, length=2048, seed=0):
    schema = StructType([
        StructField("a", data_type)])
    return random_df_from_schema(spark, schema, length=length, seed=seed)

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


def debug_df(df):
    print("COLLECTED\n{}".format(df.collect()))
    return df

def idfn(val):
    return str(val)

numeric_types = [ByteType(), ShortType(), IntegerType(), LongType(), FloatType(), DoubleType()]
integral_types = [ByteType(), ShortType(), IntegerType(), LongType()]
# A lot of mathematical expressions only support a double as input
# by parametrizing even for a single param for the test it makes the tests consistent
double_types = [DoubleType()]
double_n_long_types = [DoubleType(), LongType()]
int_n_long_types = [IntegerType(), LongType()]

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
                'cast(null as {}) DIV a'.format(string_type),
                'b DIV cast(null as {})'.format(string_type)))

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

@pytest.mark.parametrize('data_type', double_types, ids=idfn)
def test_columnar_signum(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('signum(a)'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_unary_minus(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('-a'))

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_unary_positive(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('+a'))

@pytest.mark.parametrize('data_type', numeric_types, ids=idfn)
def test_columnar_abs(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('abs(a)'))

@pytest.mark.parametrize('data_type', double_types, ids=idfn)
def test_columnar_asin(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('asin(a)'))

@pytest.mark.parametrize('data_type', double_types, ids=idfn)
def test_columnar_sqrt(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('sqrt(a)'))

@pytest.mark.parametrize('data_type', double_n_long_types, ids=idfn)
def test_columnar_floor(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('floor(a)'))

@pytest.mark.parametrize('data_type', double_n_long_types, ids=idfn)
def test_columnar_ceil(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('ceil(a)'))

@pytest.mark.parametrize('data_type', double_types, ids=idfn)
def test_columnar_rint(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_type).selectExpr('rint(a)'))


@pytest.mark.parametrize('data_type', int_n_long_types, ids=idfn)
def test_scalar_shift_left(data_type):
    string_type = to_cast_string(data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftLeft exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr(
                'shiftleft(a, cast(12 as INT))',
                'shiftleft(cast(-12 as {}), b)'.format(string_type),
                'shiftleft(cast(null as {}), b)'.format(string_type),
                'shiftleft(a, cast(null as INT))'))

@pytest.mark.parametrize('data_type', int_n_long_types, ids=idfn)
def test_columnar_shift_left(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr('shiftleft(a, b)'))

@pytest.mark.parametrize('data_type', int_n_long_types, ids=idfn)
def test_scalar_shift_right(data_type):
    string_type = to_cast_string(data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRight exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr(
                'shiftright(a, cast(12 as INT))',
                'shiftright(cast(-12 as {}), b)'.format(string_type),
                'shiftright(cast(null as {}), b)'.format(string_type),
                'shiftright(a, cast(null as INT))'))

@pytest.mark.parametrize('data_type', int_n_long_types, ids=idfn)
def test_columnar_shift_right(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr('shiftright(a, b)'))


#TODO switch back to int_n_long_types when shiftrightunsigned is fixed
@pytest.mark.parametrize('data_type', [pytest.param(LongType(), marks=pytest.mark.xfail), IntegerType()], ids=idfn)
def test_scalar_shift_right_unsigned(data_type):
    string_type = to_cast_string(data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRightUnsigned exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr(
                'shiftrightunsigned(a, cast(12 as INT))',
                'shiftrightunsigned(cast(-12 as {}), b)'.format(string_type),
                'shiftrightunsigned(cast(null as {}), b)'.format(string_type),
                'shiftrightunsigned(a, cast(null as INT))'))

@pytest.mark.parametrize('data_type', int_n_long_types, ids=idfn)
def test_columnar_shift_right_unsigned(data_type):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_type, IntegerType()).selectExpr('shiftrightunsigned(a, b)'))

