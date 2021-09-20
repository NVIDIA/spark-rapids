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

from asserts import assert_cpu_and_gpu_are_equal_collect_with_capture
from data_gen import *
from marks import approximate_float
from spark_session import with_cpu_session
import pyspark.sql.functions as f

# Each descriptor contains a list of data generators and a corresponding boolean
# indicating whether that data type is supported by the AST
ast_integral_descrs = [
    (byte_gen, False),  # AST implicitly upcasts to INT32, need AST cast to support
    (short_gen, False), # AST implicitly upcasts to INT32, need AST cast to support
    (int_gen, True),
    (long_gen, True)
]

ast_arithmetic_descrs = ast_integral_descrs + [(float_gen, True), (double_gen, True)]

# cudf AST cannot support comparing floating point until it is expressive enough to handle NaNs
# cudf AST does not support strings yet
ast_comparable_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, False),
    (double_gen, False),
    (timestamp_gen, True),
    (string_gen, False)
]

ast_boolean_descr = [(boolean_gen, True)]
ast_double_descr = [(double_gen, True)]

def assert_gpu_ast(is_supported, func, conf={}):
    exist = "GpuProjectAstExec"
    non_exist = "GpuProjectExec"
    if not is_supported:
        exist = "GpuProjectExec"
        non_exist = "GpuProjectAstExec"
    ast_conf = conf.copy()
    ast_conf.update({"spark.rapids.sql.projectAstEnabled": "true"})
    assert_cpu_and_gpu_are_equal_collect_with_capture(
        func,
        exist_classes=exist,
        non_exist_classes=non_exist,
        conf=ast_conf)

def assert_unary_ast(data_descr, func, conf={}):
    (data_gen, is_supported) = data_descr
    assert_gpu_ast(is_supported, lambda spark: func(unary_op_df(spark, data_gen)), conf=conf)

def assert_binary_ast(data_descr, func, conf={}):
    (data_gen, is_supported) = data_descr
    assert_gpu_ast(is_supported, lambda spark: func(binary_op_df(spark, data_gen)), conf=conf)

@pytest.mark.parametrize('data_gen', [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, timestamp_gen], ids=idfn)
def test_literal(spark_tmp_path, data_gen):
    # Write data to Parquet so Spark generates a plan using just the count of the data.
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    # AST does not support null literals until https://github.com/rapidsai/cudf/pull/9117
    scalar = gen_scalar(data_gen, force_no_nulls=True)
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).select(scalar))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_not(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('~a'))

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_descr', [
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True)], ids=idfn)
def test_unary_positive(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('+a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
def test_unary_minus(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('-a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
def test_abs(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('abs(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cbrt(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cbrt(a)'))

@pytest.mark.parametrize('data_descr', ast_boolean_descr, ids=idfn)
def test_not(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('!a'))

@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_rint(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('rint(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sqrt(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sqrt(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sin(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sin(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cos(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cos(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_tan(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('tan(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cot(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cot(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_sinh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('sinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_cosh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('cosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_tanh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('tanh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_asin(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asin(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_acos(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acos(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_atan(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('atan(a)'))

# AST is not expressive enough to support the ASINH Spark emulation expression
@approximate_float
@pytest.mark.parametrize('data_descr', [(double_gen, False)], ids=idfn)
def test_asinh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_acosh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_atanh(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('atanh(a)'))

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because Spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_descr', [(DoubleGen(min_exp=-20, max_exp=20), True)], ids=idfn)
def test_asinh_improved(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('asinh(a)'),
        conf={'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because Spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_descr', [(DoubleGen(min_exp=-20, max_exp=20), True)], ids=idfn)
def test_acosh_improved(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('acosh(a)'),
        conf={'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_exp(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('exp(a)'))

@approximate_float
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_expm1(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('expm1(a)'))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_eq(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') == s1,
            s2 == f.col('b'),
            f.col('a') == f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_ne(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') != s1,
            s2 != f.col('b'),
            f.col('a') != f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lt(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') < s1,
            s2 < f.col('b'),
            f.col('a') < f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lte(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') <= s1,
            s2 <= f.col('b'),
            f.col('a') <= f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gt(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') > s1,
            s2 > f.col('b'),
            f.col('a') > f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gte(data_descr):
    (s1, s2) = gen_scalars(data_descr[0], 2)
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') >= s1,
            s2 >= f.col('b'),
            f.col('a') >= f.col('b')))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_and(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseAND(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseAND(f.col('b')),
            f.col('a').bitwiseAND(f.col('b'))))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_or(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseOR(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseOR(f.col('b')),
            f.col('a').bitwiseOR(f.col('b'))))

@pytest.mark.parametrize('data_descr', ast_integral_descrs, ids=idfn)
def test_bitwise_xor(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a').bitwiseXOR(f.lit(100).cast(data_type)),
            f.lit(-12).cast(data_type).bitwiseXOR(f.col('b')),
            f.col('a').bitwiseXOR(f.col('b'))))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
def test_addition(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') + f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) + f.col('b'),
            f.col('a') + f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
def test_subtraction(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') - f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) - f.col('b'),
            f.col('a') - f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
def test_multiplication(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') * f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) * f.col('b'),
            f.col('a') * f.col('b')))

@approximate_float
def test_scalar_pow():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a pow
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    assert_gpu_ast(is_supported=True,
        func=lambda spark: gen_df(spark, data_gen).selectExpr(
            'pow(a, 7.0)',
            'pow(-12.0, b)'))

@approximate_float
def test_scalar_pow_fallback():
    # AST null literals not supported until https://github.com/rapidsai/cudf/issues/8831 is fixed
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    assert_gpu_ast(is_supported=False,
        func=lambda spark: gen_df(spark, data_gen).selectExpr(
            'pow(cast(null as DOUBLE), a)',
            'pow(b, cast(null as DOUBLE))'))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_columnar_pow(data_descr):
    assert_binary_ast(data_descr, lambda df: df.selectExpr('pow(a, b)'))
