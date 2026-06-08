# Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

import os

import pytest

from asserts import (
    assert_cpu_and_gpu_are_equal_collect_with_capture,
    assert_gpu_and_cpu_are_equal_collect,
    assert_gpu_and_cpu_error)
from data_gen import *
from marks import approximate_float, datagen_overrides, ignore_order, disable_ansi_mode
from spark_session import with_cpu_session, is_before_spark_330
import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType, IntegerType, LongType

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
ast_comparable_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, False),
    (double_gen, False),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, True)
]

ast_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, True)
]

ast_boolean_descr = [(boolean_gen, True)]
ast_double_descr = [(double_gen, True)]

_project_ast_enabled_conf = {"spark.rapids.sql.projectAstEnabled": "true"}
_jit_ast_enabled_conf = {
    "spark.rapids.sql.projectAstAnsiArithmeticEnabled": "true",
    "spark.executorEnv.LIBCUDF_JIT_ENABLED": "1"}
_ansi_jit_ast_enabled_conf = copy_and_update(ansi_enabled_conf, _jit_ast_enabled_conf)
_requires_libcudf_jit = pytest.mark.skipif(
    os.environ.get("LIBCUDF_JIT_ENABLED") != "1",
    reason="ANSI JIT AST requires LIBCUDF_JIT_ENABLED=1 before libcudf initialization")

def assert_gpu_ast(is_supported, func, conf={}):
    exist = "GpuProjectAstExec"
    non_exist = "GpuProjectExec"
    if not is_supported:
        exist = "GpuProjectExec"
        non_exist = "GpuProjectAstExec"
    ast_conf = copy_and_update(conf, _project_ast_enabled_conf)
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

@pytest.mark.parametrize('data_gen', [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, timestamp_gen, date_gen], ids=idfn)
def test_literal(spark_tmp_path, data_gen):
    # Write data to Parquet so Spark generates a plan using just the count of the data.
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    scalar = with_cpu_session(lambda spark: gen_scalar(data_gen, force_no_nulls=True))
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).select(scalar))

@pytest.mark.parametrize('data_gen', [boolean_gen, byte_gen, short_gen, int_gen, long_gen, float_gen, double_gen, timestamp_gen, date_gen], ids=idfn)
def test_null_literal(spark_tmp_path, data_gen):
    # Write data to Parquet so Spark generates a plan using just the count of the data.
    data_path = spark_tmp_path + '/AST_TEST_DATA'
    with_cpu_session(lambda spark: gen_df(spark, [("a", IntegerGen())]).write.parquet(data_path))
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: spark.read.parquet(data_path).select(f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_descr', ast_descrs, ids=idfn)
def test_isnull(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('isnull(a)'))

@pytest.mark.parametrize('data_descr', ast_descrs, ids=idfn)
def test_isnotnull(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('isnotnull(a)'))

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

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_unary_positive_for_daytime_interval():
    data_descr = (DayTimeIntervalGen(), True)
    assert_unary_ast(data_descr, lambda df: df.selectExpr('+a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_unary_minus(data_descr):
    assert_unary_ast(data_descr, lambda df: df.selectExpr('-a'))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
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
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') == s1,
            s2 == f.col('b'),
            f.col('a') == f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_ne(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') != s1,
            s2 != f.col('b'),
            f.col('a') != f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lt(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') < s1,
            s2 < f.col('b'),
            f.col('a') < f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_lte(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') <= s1,
            s2 <= f.col('b'),
            f.col('a') <= f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gt(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') > s1,
            s2 > f.col('b'),
            f.col('a') > f.col('b')))

@pytest.mark.parametrize('data_descr', ast_comparable_descrs, ids=idfn)
def test_gte(data_descr):
    (s1, s2) = with_cpu_session(lambda spark: gen_scalars(data_descr[0], 2))
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

_ast_coalesce_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (float_gen, True),
    (double_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_coalesce_descrs, ids=idfn)
def test_jit_coalesce(data_descr):
    data_gen, is_supported = data_descr
    scalar = with_cpu_session(
        lambda spark: gen_scalar(data_gen, force_no_nulls=True))
    gen = StructGen([
        ('a', data_gen.copy_special_case(None, weight=1000.0)),
        ('b', data_gen.copy_special_case(None, weight=1000.0)),
        ('c', data_gen.copy_special_case(None, weight=1000.0))],
        nullable=False)
    assert_gpu_ast(is_supported,
        lambda spark: gen_df(spark, gen).select(
            f.coalesce(f.col('a'), f.col('b')),
            f.coalesce(f.col('a'), f.col('b'), f.col('c'), scalar)),
        conf=_ansi_jit_ast_enabled_conf)

_ast_nullify_if_descrs = _ast_coalesce_descrs

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_nullify_if_descrs, ids=idfn)
def test_jit_if_nullify(data_descr):
    data_gen, is_supported = data_descr
    data_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            'if(isnull(a), cast(null as {}), b)'.format(data_type),
            'if(isnotnull(a), b, cast(null as {}))'.format(data_type),
            'if(cast(null as BOOLEAN), cast(null as {}), b)'.format(data_type)),
        conf=_ansi_jit_ast_enabled_conf)

_ast_if_else_descrs = _ast_coalesce_descrs

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_if_else_descrs, ids=idfn)
def test_jit_if_else(data_descr):
    data_gen, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            'if(isnull(a), b, a)',
            'if(isnotnull(a), a, b)',
            'if(cast(null as BOOLEAN), a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

_ast_nullif_descrs = [
    (boolean_gen, True),
    (byte_gen, True),
    (short_gen, True),
    (int_gen, True),
    (long_gen, True),
    (timestamp_gen, True),
    (date_gen, True),
    (string_gen, False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_nullif_descrs, ids=idfn)
def test_jit_nullif(data_descr):
    data_gen, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: binary_op_df(spark, data_gen).selectExpr('nullif(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_if_nullify_complex_branch_falls_back():
    data_gen = IntegerGen(min_val=-100, max_val=100, special_cases=[])
    assert_gpu_ast(False,
        lambda spark: binary_op_df(spark, data_gen).selectExpr(
            'if(isnull(a), cast(null as INT), a + cast(1 as INT))'),
        conf=_ansi_jit_ast_enabled_conf)

_ast_decimal_cast_descrs = [
    (DecimalGen(7, 2, special_cases=[]), DecimalType(9, 4), True),
    (DecimalGen(7, 2, special_cases=[]), DecimalType(18, 4), True),
    (DecimalGen(18, 2, special_cases=[]), DecimalType(30, 4), True),
    (DecimalGen(7, 4, special_cases=[]), DecimalType(9, 2), False)
]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_decimal_cast_descrs, ids=idfn)
def test_jit_decimal_cast(data_descr):
    data_gen, to_type, is_supported = data_descr
    assert_gpu_ast(is_supported,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(to_type)),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_decimal_cast_precision_check_ansi_disabled():
    data_gen = DecimalGen(18, 2)
    assert_gpu_ast(True,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(DecimalType(9, 2))),
        conf=_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_jit_decimal_cast_scale_up_precision_check_ansi_disabled():
    data_gen = DecimalGen(18, 0)
    assert_gpu_ast(False,
        lambda spark: unary_op_df(spark, data_gen).select(f.col('a').cast(DecimalType(18, 2))),
        conf=_jit_ast_enabled_conf)

_ast_shift_descrs = [(int_gen, True), (long_gen, True)]
_ast_shift_amount_gen = IntegerGen(
    min_val=-80,
    max_val=80,
    special_cases=[-65, -64, -63, -33, -32, -31, -1, 0, 1, 31, 32, 33, 63, 64, 65])

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_shift_descrs, ids=idfn)
def test_jit_shift_left(data_descr):
    data_gen, is_supported = data_descr
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: two_col_df(spark, data_gen, _ast_shift_amount_gen).selectExpr(
            'shiftleft(a, cast(12 as INT))',
            'shiftleft(a, cast(40 as INT))',
            'shiftleft(a, cast(-1 as INT))',
            'shiftleft(cast(-12 as {}), b)'.format(string_type),
            'shiftleft(cast(null as {}), b)'.format(string_type),
            'shiftleft(a, cast(null as INT))',
            'shiftleft(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_descr', _ast_shift_descrs, ids=idfn)
def test_jit_shift_right(data_descr):
    data_gen, is_supported = data_descr
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_ast(is_supported,
        lambda spark: two_col_df(spark, data_gen, _ast_shift_amount_gen).selectExpr(
            'shiftright(a, cast(12 as INT))',
            'shiftright(a, cast(40 as INT))',
            'shiftright(a, cast(-1 as INT))',
            'shiftright(cast(-12 as {}), b)'.format(string_type),
            'shiftright(cast(null as {}), b)'.format(string_type),
            'shiftright(a, cast(null as INT))',
            'shiftright(a, b)'),
        conf=_ansi_jit_ast_enabled_conf)

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_addition(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') + f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) + f.col('b'),
            f.col('a') + f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_subtraction(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') - f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) - f.col('b'),
            f.col('a') - f.col('b')))

@pytest.mark.parametrize('data_descr', ast_arithmetic_descrs, ids=idfn)
@disable_ansi_mode
def test_multiplication(data_descr):
    data_type = data_descr[0].data_type
    assert_binary_ast(data_descr,
        lambda df: df.select(
            f.col('a') * f.lit(100).cast(data_type),
            f.lit(-12).cast(data_type) * f.col('b'),
            f.col('a') * f.col('b')))

# Each descriptor contains a list of data generators and a corresponding boolean
# indicating whether that data type is supported by the AST
# all the below desc are not supported by the AST because ANSI mode is on
_ast_integral_desc_list_for_ansi_on = [
    (ByteGen(min_val=-11, max_val=11, special_cases=[]), False),  # 11 * 11 < 127 (Byte.MaxValue)
    (ShortGen(min_val=-181, max_val=181, special_cases=[]), False), # 181 * 181 < 32767 (Short.MaxValue)
    (IntegerGen(min_val=-46340, max_val=46340, special_cases=[]), False) , # 46340 * 46340 < 2147483647 (Int.MaxValue)
    (LongGen(min_val=-3037000499, max_val=3037000499, special_cases=[]), False)] # 3037000499 * 3037000499 < 9223372036854775807(Long.MaxValue)
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_on, ids=idfn)
def test_multiplication_for_integer_ansi_on(data_desc):
    data_type = data_desc[0].data_type
    assert_binary_ast(data_desc,
                      lambda df: df.select(f.col('a') * f.col('b')),
                      conf=ansi_enabled_conf)

_ast_integral_desc_list_for_ansi_jit_on = [
    (ByteGen(min_val=-11, max_val=11, special_cases=[]), False),
    (ShortGen(min_val=-181, max_val=181, special_cases=[]), False),
    (IntegerGen(min_val=-46340, max_val=46340, special_cases=[]), True),
    (LongGen(min_val=-3037000499, max_val=3037000499, special_cases=[]), True)]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_arithmetic_for_integer_ansi_on(data_desc):
    assert_binary_ast(data_desc,
                      lambda df: df.select(
                          f.col('a') + f.col('b'),
                          f.col('a') - f.col('b'),
                          f.col('a') * f.col('b')),
                      conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_integral_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_unary_arithmetic_for_integer_ansi_on(data_desc):
    assert_unary_ast(data_desc,
                     lambda df: df.select(-f.col('a'), f.abs(f.col('a'))),
                     conf=_ansi_jit_ast_enabled_conf)

_ast_div_mod_desc_list_for_ansi_jit_on = [
    (IntegerGen(min_val=-1000, max_val=1000, special_cases=[]),
        SetValuesGen(IntegerType(), [-13, -7, -3, -1, 1, 3, 7, 13, None])),
    (LongGen(min_val=-1000000, max_val=1000000, special_cases=[]),
        SetValuesGen(LongType(), [-13, -7, -3, -1, 1, 3, 7, 13, None]))]

@_requires_libcudf_jit
@pytest.mark.parametrize('data_desc', _ast_div_mod_desc_list_for_ansi_jit_on, ids=idfn)
def test_ansi_jit_integral_div_mod_for_integer_ansi_on(data_desc):
    lhs_gen, rhs_gen = data_desc
    assert_gpu_ast(True,
                   lambda spark: two_col_df(spark, lhs_gen, rhs_gen).selectExpr(
                       'a DIV b',
                       'a % b'),
                   conf=_ansi_jit_ast_enabled_conf)

@_requires_libcudf_jit
def test_ansi_jit_integral_mod_sign_for_integer_ansi_on():
    assert_gpu_ast(True,
                   lambda spark: spark.createDataFrame(
                       spark.sparkContext.parallelize(
                           [(-5, 3), (5, -3), (-5, -3), (5, 3), (None, 3), (5, None)] * 8,
                           1),
                       'a INT, b INT').selectExpr('a % b'),
                   conf=_ansi_jit_ast_enabled_conf)

def _collect_with_retry_oom_disabled(spark, df_fun):
    spark.conf.set("spark.rapids.sql.test.injectRetryOOM", "false")
    return df_fun(spark).collect()

@_requires_libcudf_jit
def test_ansi_jit_integral_div_by_zero_errors():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    assert_gpu_and_cpu_error(
        lambda spark: _collect_with_retry_oom_disabled(
            spark,
            lambda spark: two_col_df(
                spark,
                LongGen(nullable=False, min_val=-100, max_val=100, special_cases=[]),
                SetValuesGen(LongType(), [0]),
                length=8,
                num_slices=1).selectExpr('a DIV b')),
        ast_conf,
        'Division by zero')

@_requires_libcudf_jit
def test_ansi_jit_integral_div_overflow_errors():
    ast_conf = copy_and_update(
        _ansi_jit_ast_enabled_conf,
        _project_ast_enabled_conf,
        {"spark.rapids.sql.test.injectRetryOOM": "false"})
    assert_gpu_and_cpu_error(
        lambda spark: _collect_with_retry_oom_disabled(
            spark,
            lambda spark: spark.createDataFrame(
                spark.sparkContext.parallelize([(LONG_MIN, -1)] * 8, 1),
                'a LONG, b LONG').selectExpr('a DIV b')),
        ast_conf,
        'Overflow')

@approximate_float
def test_scalar_pow():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a pow
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    assert_gpu_ast(is_supported=True,
        func=lambda spark: gen_df(spark, data_gen).selectExpr(
            'pow(a, 7.0)',
            'pow(-12.0, b)'))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
@pytest.mark.parametrize('data_descr', ast_double_descr, ids=idfn)
def test_columnar_pow(data_descr):
    assert_binary_ast(data_descr, lambda df: df.selectExpr('pow(a, b)'))

@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_and(data_gen):
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
        func=lambda spark: binary_op_df(spark, data_gen).select(
            f.col('a') & f.lit(True),
            f.lit(False) & f.col('b'),
            f.col('a') & f.col('b')))

@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_or(data_gen):
    data_type = data_gen.data_type
    assert_gpu_ast(is_supported=True,
                   func=lambda spark: binary_op_df(spark, data_gen).select(
                       f.col('a') | f.lit(True),
                       f.lit(False) | f.col('b'),
                       f.col('a') | f.col('b')))

@ignore_order
@disable_ansi_mode
def test_multi_tier_ast():
    assert_gpu_ast(
        is_supported=True,
        # repartition is here to avoid Spark simplifying the expression
        func=lambda spark: spark.range(10).withColumn("x", f.col("id")).repartition(1)\
            .selectExpr("x", "(id < x) == (id < (id + x))"))


# MUST NOT use GPU AST when project refers to string type(non-fixed-width),
# or cudf::compute_column will throw error: Invalid, non-fixed-width type
# ANSI mode is disabled here due to an overflow issue with integer multiplication on Spark 4.0.0.
@disable_ansi_mode
@ignore_order(local=True)
def test_refer_to_non_fixed_width_column():
    gens = [('col_int', int_gen), ('col_string', string_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gens).selectExpr("col_int * col_int", "col_string"),
        conf=_project_ast_enabled_conf)
