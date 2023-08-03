# Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

from logging import exception
import pytest

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error, assert_gpu_fallback_collect, assert_gpu_and_cpu_are_equal_sql
from data_gen import *
from marks import ignore_order, incompat, approximate_float, allow_non_gpu
from pyspark.sql.types import *
from pyspark.sql.types import IntegralType
from spark_session import *
import pyspark.sql.functions as f
import pyspark.sql.utils
from datetime import timedelta

# No overflow gens here because we just focus on verifying the fallback to CPU when
# enabling ANSI mode. But overflows will fail the tests because CPU runs raise
# exceptions.
_no_overflow_multiply_gens_for_fallback = [
    ByteGen(min_val = 1, max_val = 10, special_cases=[]),
    ShortGen(min_val = 1, max_val = 100, special_cases=[]),
    IntegerGen(min_val = 1, max_val = 1000, special_cases=[]),
    LongGen(min_val = 1, max_val = 3000, special_cases=[])]


_no_overflow_multiply_gens = _no_overflow_multiply_gens_for_fallback + [
    DecimalGen(10, 0),
    DecimalGen(19, 0)]

_decimal_gen_7_7 = DecimalGen(precision=7, scale=7)
_decimal_gen_18_0 = DecimalGen(precision=18, scale=0)
_decimal_gen_18_3 = DecimalGen(precision=18, scale=3)
_decimal_gen_30_2 = DecimalGen(precision=30, scale=2)
_decimal_gen_36_5 = DecimalGen(precision=36, scale=5)
_decimal_gen_36_neg5 = DecimalGen(precision=36, scale=-5)
_decimal_gen_38_0 = DecimalGen(precision=38, scale=0)
_decimal_gen_38_10 = DecimalGen(precision=38, scale=10)
_decimal_gen_38_neg10 = DecimalGen(precision=38, scale=-10)

_arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 = [
    decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0, decimal_gen_128bit,
    _decimal_gen_30_2, _decimal_gen_36_5, _decimal_gen_38_10
]

_arith_data_gens_diff_precision_scale_and_no_neg_scale = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 + [_decimal_gen_38_0]

_arith_decimal_gens_no_neg_scale = _arith_data_gens_diff_precision_scale_and_no_neg_scale + [_decimal_gen_7_7]

_arith_decimal_gens = _arith_decimal_gens_no_neg_scale + [
    decimal_gen_32bit_neg_scale, _decimal_gen_36_neg5, _decimal_gen_38_neg10
]

_arith_data_gens = numeric_gens + _arith_decimal_gens

_arith_data_gens_no_neg_scale = numeric_gens + _arith_decimal_gens_no_neg_scale

_arith_decimal_gens_no_neg_scale_38_0_overflow = \
    _arith_data_gens_diff_precision_scale_and_no_neg_scale_no_38_0 + [
        _decimal_gen_7_7,
        pytest.param(_decimal_gen_38_0, marks=pytest.mark.skipif(
            is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+'))]

def _get_overflow_df(spark, data, data_type, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([data]),
        StructType([StructField('a', data_type)])
    ).selectExpr(expr)

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_addition(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type),
                f.col('a') + f.col('b')))

# If it will not overflow for multiply it is good for add too
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens, ids=idfn)
def test_addition_ansi_no_overflow(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type),
                f.col('a') + f.col('b')),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_subtraction(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type),
                f.col('a') - f.col('b')))

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
@pytest.mark.parametrize('addOrSub', ['+', '-'])
def test_addition_subtraction_mixed(lhs, rhs, addOrSub):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"a {addOrSub} b")
    )

# If it will not overflow for multiply it is good for subtract too
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens, ids=idfn)
def test_subtraction_ansi_no_overflow(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type),
                f.col('a') - f.col('b')),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', numeric_gens + [
    decimal_gen_32bit_neg_scale, decimal_gen_32bit, _decimal_gen_7_7,
    DecimalGen(precision=8, scale=8), decimal_gen_64bit, _decimal_gen_18_3,
    _decimal_gen_38_10,
    _decimal_gen_38_neg10
    ], ids=idfn)
def test_multiplication(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a'), f.col('b'),
                f.col('a') * f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) * f.col('b'),
                f.lit(None).cast(data_type) * f.col('a'),
                f.col('b') * f.lit(None).cast(data_type),
                f.col('a') * f.col('b')
                ))

@allow_non_gpu('ProjectExec', 'Alias', 'Multiply', 'Cast')
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback, ids=idfn)
def test_multiplication_fallback_when_ansi_enabled(data_gen):
    assert_gpu_fallback_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.col('b')),
            'Multiply',
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [float_gen, double_gen, decimal_gen_32bit, DecimalGen(19, 0)], ids=idfn)
def test_multiplication_ansi_enabled(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.lit(100).cast(data_type),
                f.col('a') * f.col('b')),
            conf=ansi_enabled_conf)

def test_multiplication_ansi_overflow():
    exception_str = 'ArithmeticException'
    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, DecimalGen(38, 0)).selectExpr("a * " + "9"*38 + " as ret").collect(),
        ansi_enabled_conf,
        exception_str)

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
def test_multiplication_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') * f.col('b')))

@approximate_float # we should get the perfectly correct answer for floats except when casting a decimal to a float in some corner cases.
@pytest.mark.parametrize('lhs', [float_gen, double_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3)], ids=idfn)
def test_float_multiplication_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') * f.col('b')),
            conf={'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})

@pytest.mark.parametrize('data_gen', [double_gen, decimal_gen_32bit_neg_scale, DecimalGen(6, 3),
 DecimalGen(5, 5), DecimalGen(6, 0), DecimalGen(7, 4), DecimalGen(15, 0), DecimalGen(18, 0),
 DecimalGen(17, 2), DecimalGen(16, 4), DecimalGen(38, 21), DecimalGen(21, 17), DecimalGen(3, -2)], ids=idfn)
def test_division(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') / f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) / f.col('b'),
                f.lit(None).cast(data_type) / f.col('a'),
                f.col('b') / f.lit(None).cast(data_type),
                f.col('a') / f.col('b')))

@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(4, 1), DecimalGen(5, 0), DecimalGen(5, 1), DecimalGen(10, 5)], ids=idfn)
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(1, -2), DecimalGen(16, 1)], ids=idfn)
def test_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a'), f.col('b'),
                f.col('a') / f.col('b')))

# Spark has some problems with some decimal operations where it can try to generate a type that is invalid (scale > precision) which results in an error
# instead of increasing the precision. So we have a second test that deals with a few of these use cases
@pytest.mark.parametrize('rhs', [DecimalGen(30, 10), DecimalGen(28, 18)], ids=idfn)
@pytest.mark.parametrize('lhs', [DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
def test_division_mixed_larger_dec(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a'), f.col('b'),
                f.col('a') / f.col('b')))

def test_special_decimal_division():
    for precision in range(1, 39):
        for scale in range(-3, precision + 1):
            print("PRECISION " + str(precision) + " SCALE " + str(scale))
            data_gen = DecimalGen(precision, scale)
            assert_gpu_and_cpu_are_equal_collect(
                    lambda spark : two_col_df(spark, data_gen, data_gen).select(
                        f.col('a') / f.col('b')))

@approximate_float # we should get the perfectly correct answer for floats except when casting a decimal to a float in some corner cases.
@pytest.mark.parametrize('rhs', [float_gen, double_gen], ids=idfn)
@pytest.mark.parametrize('lhs', [DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(1, -2), DecimalGen(16, 1)], ids=idfn)
def test_float_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') / f.col('b')),
            conf={'spark.rapids.sql.castDecimalToFloat.enabled': 'true'})

@pytest.mark.parametrize('data_gen', integral_gens + [
    decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_7_7, _decimal_gen_18_3, _decimal_gen_30_2,
    _decimal_gen_36_5, _decimal_gen_38_0], ids=idfn)
def test_int_division(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a DIV cast(100 as {})'.format(string_type),
                'cast(-12 as {}) DIV b'.format(string_type),
                'cast(null as {}) DIV a'.format(string_type),
                'b DIV cast(null as {})'.format(string_type),
                'a DIV b'))

@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(5, 4), DecimalGen(3, -2), _decimal_gen_30_2], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(13, 2), DecimalGen(6, 3), _decimal_gen_38_0,
                                 pytest.param(_decimal_gen_36_neg5, marks=pytest.mark.skipif(not is_before_spark_340() or is_databricks113_or_later(), reason='SPARK-41207'))], ids=idfn)
def test_int_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).selectExpr(
                'a DIV b'))

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_mod(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') % f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) % f.col('b'),
                f.lit(None).cast(data_type) % f.col('a'),
                f.col('b') % f.lit(None).cast(data_type),
                f.col('a') % f.col('b')))

# pmod currently falls back for Decimal(precision=38)
# https://github.com/NVIDIA/spark-rapids/issues/6336
# only testing numeric_gens because of https://github.com/NVIDIA/spark-rapids/issues/7553
_pmod_gens = numeric_gens
test_pmod_fallback_decimal_gens = [ decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0, decimal_gen_128bit,
                              _decimal_gen_30_2, _decimal_gen_36_5,
                              DecimalGen(precision=37, scale=0), DecimalGen(precision=37, scale=10),
                              _decimal_gen_7_7]

@pytest.mark.parametrize('data_gen', _pmod_gens, ids=idfn)
def test_pmod(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'pmod(a, cast(100 as {}))'.format(string_type),
                'pmod(cast(-12 as {}), b)'.format(string_type),
                'pmod(cast(null as {}), a)'.format(string_type),
                'pmod(b, cast(null as {}))'.format(string_type),
                'pmod(a, b)'))


@allow_non_gpu("ProjectExec", "Pmod")
@pytest.mark.parametrize('data_gen', test_pmod_fallback_decimal_gens + [_decimal_gen_38_0, _decimal_gen_38_10], ids=idfn)
def test_pmod_fallback(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_fallback_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'pmod(a, cast(100 as {}))'.format(string_type),
            'pmod(cast(-12 as {}), b)'.format(string_type),
            'pmod(cast(null as {}), a)'.format(string_type),
            'pmod(b, cast(null as {}))'.format(string_type),
            'pmod(a, b)'),
        "Pmod")

# test pmod(Long.MinValue, -1) = 0 and Long.MinValue % -1 = 0, should not throw
def test_mod_pmod_long_min_value():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : spark.createDataFrame([(-9223372036854775808,)], ["a"]).selectExpr(
            'pmod(a, -1L)',
            'a % -1L'),
        ansi_enabled_conf)

# pmod currently falls back for Decimal(precision=38)
# https://github.com/NVIDIA/spark-rapids/issues/6336
@pytest.mark.xfail(reason='Decimals type disabled https://github.com/NVIDIA/spark-rapids/issues/7553')
@pytest.mark.parametrize('data_gen', [decimal_gen_32bit, decimal_gen_64bit, _decimal_gen_18_0,
                                      decimal_gen_128bit, _decimal_gen_30_2, _decimal_gen_36_5], ids=idfn)
@pytest.mark.parametrize('overflow_exp', [
    'pmod(a, cast(0 as {}))',
    'pmod(cast(-12 as {}), cast(0 as {}))',
    'a % (cast(0 as {}))',
    'cast(-12 as {}) % cast(0 as {})'], ids=idfn)
def test_mod_pmod_by_zero(data_gen, overflow_exp):
    string_type = to_cast_string(data_gen.data_type)
    if is_before_spark_320():
        exception_str = 'java.lang.ArithmeticException: divide by zero'
    elif is_before_spark_330():
        exception_str = 'SparkArithmeticException: divide by zero'
    elif is_before_spark_340() and not is_databricks113_or_later():
        exception_str = 'SparkArithmeticException: Division by zero'
    else:
        exception_str = 'SparkArithmeticException: [DIVIDE_BY_ZERO] Division by zero'

    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            overflow_exp.format(string_type, string_type)).collect(),
        ansi_enabled_conf,
        exception_str)

def test_cast_neg_to_decimal_err():
    # -12 cannot be represented as decimal(7,7)
    data_gen = _decimal_gen_7_7
    if is_before_spark_322():
        exception_content = "Decimal(compact,-120000000,20,0}) cannot be represented as Decimal(7, 7)"
    elif is_databricks113_or_later() or not is_before_spark_340():
        exception_content = "[NUMERIC_VALUE_OUT_OF_RANGE] -12 cannot be represented as Decimal(7, 7)"
    else:
        exception_content = "Decimal(compact, -120000000, 20, 0) cannot be represented as Decimal(7, 7)"

    if is_before_spark_330() and not is_databricks104_or_later():
            exception_type = "java.lang.ArithmeticException: "
    elif not is_before_spark_340():
        exception_type = "pyspark.errors.exceptions.captured.ArithmeticException: "
    else:
        exception_type = "org.apache.spark.SparkArithmeticException: "

    assert_gpu_and_cpu_error(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'cast(-12 as {})'.format(to_cast_string(data_gen.data_type))).collect(),
        ansi_enabled_conf,
        exception_type + exception_content)

@pytest.mark.parametrize('data_gen', _pmod_gens, ids=idfn)
def test_mod_pmod_by_zero_not_ansi(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'pmod(a, cast(0 as {}))'.format(string_type),
            'pmod(cast(-12 as {}), cast(0 as {}))'.format(string_type, string_type)),
        {'spark.sql.ansi.enabled': 'false'})
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr(
            'a % (cast(0 as {}))'.format(string_type),
            'cast(-12 as {}) % cast(0 as {})'.format(string_type, string_type)),
        {'spark.sql.ansi.enabled': 'false'})

@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 5),
    DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2),
    DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen, DecimalGen(6, 3),
    DecimalGen(10, -2), DecimalGen(15, 3), DecimalGen(30, 12), DecimalGen(3, -3),
    DecimalGen(27, 7), DecimalGen(20, -3)], ids=idfn)
def test_mod_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"a % b"))

# @pytest.mark.skipif(not is_databricks113_or_later() and not is_spark_340_or_later(), reason="https://github.com/NVIDIA/spark-rapids/issues/8330")
@pytest.mark.parametrize('lhs', [DecimalGen(38,0), DecimalGen(37,2), DecimalGen(38,5), DecimalGen(38,-10), DecimalGen(38,7)], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(27,7), DecimalGen(30,10), DecimalGen(38,1), DecimalGen(36,0), DecimalGen(28,-7)], ids=idfn)
def test_mod_mixed_decimal128(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr("a", "b", f"a % b"))

# Split into 4 tests to permute https://github.com/NVIDIA/spark-rapids/issues/7553 failures
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
def test_pmod_mixed_numeric(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"))

@allow_non_gpu("ProjectExec", "Pmod")
@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3),
    DecimalGen(4, 2), DecimalGen(3, -2), DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)
    ], ids=idfn)
@pytest.mark.parametrize('rhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
def test_pmod_mixed_decimal_lhs(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"),
        "Pmod")

@allow_non_gpu("ProjectExec", "Pmod")
@pytest.mark.parametrize('lhs', [byte_gen, short_gen, int_gen, long_gen], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3),
    DecimalGen(30, 12), DecimalGen(3, -3), DecimalGen(27, 7), DecimalGen(20, -3)
    ], ids=idfn)
def test_pmod_mixed_decimal_rhs(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"),
        "Pmod")

@allow_non_gpu("ProjectExec", "Pmod")
@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3),
    DecimalGen(4, 2), DecimalGen(3, -2), DecimalGen(16, 7), DecimalGen(19, 0), DecimalGen(30, 10)
    ], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3), DecimalGen(10, -2), DecimalGen(15, 3),
    DecimalGen(30, 12), DecimalGen(3, -3), DecimalGen(27, 7), DecimalGen(20, -3)
    ], ids=idfn)
def test_pmod_mixed_decimal(lhs, rhs):
    assert_gpu_fallback_collect(
        lambda spark : two_col_df(spark, lhs, rhs).selectExpr(f"pmod(a, b)"),
        "Pmod")

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_signum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('signum(a)'))

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_unary_minus(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'))

@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens + [float_gen, double_gen] + _arith_decimal_gens, ids=idfn)
def test_unary_minus_ansi_no_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'),
            conf=ansi_enabled_conf)

@pytest.mark.parametrize('data_type,value', [
    (LongType(), LONG_MIN),
    (IntegerType(), INT_MIN),
    (ShortType(), SHORT_MIN),
    (ByteType(), BYTE_MIN)], ids=idfn)
def test_unary_minus_ansi_overflow(data_type, value):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: {name of the data type} overflow.
    GPU: One or more rows overflow for {name of the operation} operation.
    """
    assert_gpu_and_cpu_error(
            df_fun=lambda spark: _get_overflow_df(spark, [value], data_type, '-a').collect(),
            conf=ansi_enabled_conf,
            error_message='java.lang.ArithmeticException' if is_before_spark_330() else \
                          'org.apache.spark.SparkArithmeticException')

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_unary_positive(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : unary_op_df(spark, data_gen).selectExpr('+a'))

@pytest.mark.parametrize('data_gen', _arith_data_gens, ids=idfn)
def test_abs(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'))

# ANSI is ignored for abs prior to 3.2.0, but still okay to test it a little more.
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens + [float_gen, double_gen] + _arith_decimal_gens, ids=idfn)
def test_abs_ansi_no_overflow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'),
            conf=ansi_enabled_conf)

# Only run this test for Spark v3.2.0 and later to verify abs will
# throw exceptions for overflow when ANSI mode is enabled.
@pytest.mark.skipif(is_before_spark_320(), reason='SPARK-33275')
@pytest.mark.parametrize('data_type,value', [
    (LongType(), LONG_MIN),
    (IntegerType(), INT_MIN),
    (ShortType(), SHORT_MIN),
    (ByteType(), BYTE_MIN)], ids=idfn)
def test_abs_ansi_overflow(data_type, value):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: {name of the data type} overflow.
    GPU: One or more rows overflow for abs operation.
    """
    assert_gpu_and_cpu_error(
            df_fun=lambda spark: _get_overflow_df(spark, [value], data_type, 'abs(a)').collect(),
            conf=ansi_enabled_conf,
            error_message='java.lang.ArithmeticException' if is_before_spark_330() else \
                          'org.apache.spark.SparkArithmeticException')

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sqrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sqrt(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_hypot(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark : binary_op_df(spark, data_gen).selectExpr(
            'hypot(a, b)',
        ))

@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale + [DecimalGen(30, 15)], ids=idfn)
def test_floor(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a)'))

@pytest.mark.skipif(is_before_spark_330(), reason='scale parameter in Floor function is not supported before Spark 3.3.0')
@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale, ids=idfn)
def test_floor_scale_zero(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a, 0)'),
            conf={'spark.rapids.sql.castFloatToDecimal.enabled':'true'})

@pytest.mark.skipif(is_before_spark_330(), reason='scale parameter in Floor function is not supported before Spark 3.3.0')
@allow_non_gpu('ProjectExec')
@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale_38_0_overflow, ids=idfn)
def test_floor_scale_nonzero(data_gen):
    assert_gpu_fallback_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a, -1)'), 'RoundFloor')

@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale + [DecimalGen(30, 15)], ids=idfn)
def test_ceil(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a)'))

@pytest.mark.skipif(is_before_spark_330(), reason='scale parameter in Ceil function is not supported before Spark 3.3.0')
@pytest.mark.parametrize('data_gen', double_n_long_gens + _arith_decimal_gens_no_neg_scale, ids=idfn)
def test_ceil_scale_zero(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a, 0)'),
            conf={'spark.rapids.sql.castFloatToDecimal.enabled':'true'})

@pytest.mark.parametrize('data_gen', [_decimal_gen_36_neg5, _decimal_gen_38_neg10], ids=idfn)
def test_floor_ceil_overflow(data_gen):
    exception_type = "java.lang.ArithmeticException" if is_before_spark_330() and not is_databricks104_or_later() \
        else "SparkArithmeticException"
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('floor(a)').collect(),
        conf={},
        error_message=exception_type)
    assert_gpu_and_cpu_error(
        lambda spark: unary_op_df(spark, data_gen).selectExpr('ceil(a)').collect(),
        conf={},
        error_message=exception_type)

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_rint(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('rint(a)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_left(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftLeft exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftleft(a, cast(12 as INT))',
                'shiftleft(cast(-12 as {}), b)'.format(string_type),
                'shiftleft(cast(null as {}), b)'.format(string_type),
                'shiftleft(a, cast(null as INT))',
                'shiftleft(a, b)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_right(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRight exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftright(a, cast(12 as INT))',
                'shiftright(cast(-12 as {}), b)'.format(string_type),
                'shiftright(cast(null as {}), b)'.format(string_type),
                'shiftright(a, cast(null as INT))',
                'shiftright(a, b)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_shift_right_unsigned(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRightUnsigned exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftrightunsigned(a, cast(12 as INT))',
                'shiftrightunsigned(cast(-12 as {}), b)'.format(string_type),
                'shiftrightunsigned(cast(null as {}), b)'.format(string_type),
                'shiftrightunsigned(a, cast(null as INT))',
                'shiftrightunsigned(a, b)'))

_arith_data_gens_for_round = numeric_gens + _arith_decimal_gens_no_neg_scale_38_0_overflow + [
    decimal_gen_32bit_neg_scale,
    DecimalGen(precision=15, scale=-8),
    DecimalGen(precision=30, scale=-5),
    pytest.param(_decimal_gen_36_neg5, marks=pytest.mark.skipif(
        is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+')),
    pytest.param(_decimal_gen_38_neg10, marks=pytest.mark.skipif(
        is_spark_330_or_later(), reason='This case overflows in Spark 3.3.0+'))
]

@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', _arith_data_gens_for_round, ids=idfn)
def test_decimal_bround(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'bround(a)',
                'bround(1.234, 2)',
                'bround(a, -1)',
                'bround(a, 1)',
                'bround(a, 2)',
                'bround(a, 10)'))

@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', _arith_data_gens_for_round, ids=idfn)
def test_decimal_round(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'round(a)',
                'round(1.234, 2)',
                'round(a, -1)',
                'round(a, 1)',
                'round(a, 2)',
                'round(a, 10)'))


@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', [int_gen], ids=idfn)
def test_illegal_args_round(data_gen):
    def check_analysis_exception(spark, sql_text):
        try:
            gen_df(spark, [("a", data_gen), ("b", int_gen)], length=10).selectExpr(sql_text)
            raise Exception("round/bround should not plan with invalid arguments %s" % sql_text)
        except pyspark.sql.utils.AnalysisException as e:
            pass

    def doit(spark):
        check_analysis_exception(spark, "round(1.2345, b)")
        check_analysis_exception(spark, "round(a, b)")
        check_analysis_exception(spark, "bround(1.2345, b)")
        check_analysis_exception(spark, "bround(a, b)")

    with_cpu_session(lambda spark: doit(spark))
    with_gpu_session(lambda spark: doit(spark))


@incompat
@approximate_float
def test_non_decimal_round_overflow():
    gen = StructGen([('byte_c', byte_gen), ('short_c', short_gen),
                     ('int_c', int_gen), ('long_c', long_gen),
                     ('float_c', float_gen), ('double_c', double_gen)], nullable=False)
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen).selectExpr(
            'round(byte_c, -2)', 'round(byte_c, -3)',
            'round(short_c, -4)', 'round(short_c, -5)',
            'round(int_c, -9)', 'round(int_c, -10)',
            'round(long_c, -19)', 'round(long_c, -20)',
            'round(float_c, -38)', 'round(float_c, -39)',
            'round(float_c, 38)', 'round(float_c, 39)',
            'round(double_c, -308)', 'round(double_c, -309)',
            'round(double_c, 308)', 'round(double_c, 309)',
            'bround(byte_c, -2)', 'bround(byte_c, -3)',
            'bround(short_c, -4)', 'bround(short_c, -5)',
            'bround(int_c, -9)', 'bround(int_c, -10)',
            'bround(long_c, -19)', 'bround(long_c, -20)',
            'bround(float_c, -38)', 'bround(float_c, -39)',
            'bround(float_c, 38)', 'bround(float_c, 39)',
            'bround(double_c, -308)', 'bround(double_c, -309)',
            'bround(double_c, 308)', 'bround(double_c, 309)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cbrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cbrt(a)'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_and(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a & cast(100 as {})'.format(string_type),
                'cast(-12 as {}) & b'.format(string_type),
                'cast(null as {}) & a'.format(string_type),
                'b & cast(null as {})'.format(string_type),
                'a & b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_or(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a | cast(100 as {})'.format(string_type),
                'cast(-12 as {}) | b'.format(string_type),
                'cast(null as {}) | a'.format(string_type),
                'b | cast(null as {})'.format(string_type),
                'a | b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_xor(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a ^ cast(100 as {})'.format(string_type),
                'cast(-12 as {}) ^ b'.format(string_type),
                'cast(null as {}) ^ a'.format(string_type),
                'b ^ cast(null as {})'.format(string_type),
                'a ^ b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_bit_not(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('~a'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_radians(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('radians(a)'))

# Spark's degrees will overflow on large values in jdk 8 or below
@approximate_float
@pytest.mark.skipif(get_java_major_version() <= 8, reason="requires jdk 9 or higher")
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_degrees(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('degrees(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', [float_gen], ids=idfn)
def test_degrees_small(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('degrees(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cos(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_acos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acos(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cosh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cosh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_acosh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acosh(a)'))

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_gen', [DoubleGen(min_exp=-20, max_exp=20)], ids=idfn)
def test_columnar_acosh_improved(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acosh(a)'),
            {'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sin(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sinh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sinh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asinh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asinh(a)'))

# The default approximate is 1e-6 or 1 in a million
# in some cases we need to adjust this because the algorithm is different
@approximate_float(rel=1e-4, abs=1e-12)
# Because spark will overflow on large exponents drop to something well below
# what it fails at, note this is binary exponent, not base 10
@pytest.mark.parametrize('data_gen', [DoubleGen(min_exp=-20, max_exp=20)], ids=idfn)
def test_columnar_asinh_improved(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asinh(a)'),
            {'spark.rapids.sql.improvedFloatOps.enabled': 'true'})

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_tan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('tan(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_atan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atan(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_atanh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atanh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_tanh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('tanh(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_cot(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cot(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_exp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('exp(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_expm1(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('expm1(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log1p(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log1p(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log2(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log2(a)'))

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_log10(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log10(a)'))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
def test_logarithm():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a log
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    string_type = 'DOUBLE'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'log(a, cast(100 as {}))'.format(string_type),
                'log(cast(-12 as {}), b)'.format(string_type),
                'log(cast(null as {}), b)'.format(string_type),
                'log(a, cast(null as {}))'.format(string_type),
                'log(a, b)'))

@approximate_float
def test_scalar_pow():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a pow
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    string_type = 'DOUBLE'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'pow(a, cast(7 as {}))'.format(string_type),
                'pow(cast(-12 as {}), b)'.format(string_type),
                'pow(cast(null as {}), a)'.format(string_type),
                'pow(b, cast(null as {}))'.format(string_type)))

@approximate_float
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/89')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_pow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('pow(a, b)'))

@pytest.mark.parametrize('data_gen', all_basic_gens + _arith_decimal_gens, ids=idfn)
def test_least(data_gen):
    num_cols = 20
    s1 = gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=100.0))
        for x in range(0, num_cols)], nullable=False)

    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.least(*command_args)))

@pytest.mark.parametrize('data_gen', all_basic_gens + _arith_decimal_gens, ids=idfn)
def test_greatest(data_gen):
    num_cols = 20
    s1 = gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=100.0))
        for x in range(0, num_cols)], nullable=False)
    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.greatest(*command_args)))


def _test_div_by_zero(ansi_mode, expr, is_lit=False):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_mode == 'ansi'}
    data_gen = lambda spark: two_col_df(spark, IntegerGen(), IntegerGen(min_val=0, max_val=0), length=1)
    div_by_zero_func = lambda spark: data_gen(spark).selectExpr(expr)
    if is_before_spark_320():
        err_message = 'java.lang.ArithmeticException: divide by zero'
    elif is_before_spark_330():
        err_message = 'SparkArithmeticException: divide by zero'
    elif is_before_spark_340() and not is_databricks113_or_later():
        err_message = 'SparkArithmeticException: Division by zero'
    else:
        exception_type = 'SparkArithmeticException: ' \
            if not is_lit else "pyspark.errors.exceptions.captured.ArithmeticException: "
        err_message = exception_type + "[DIVIDE_BY_ZERO] Division by zero"

    if ansi_mode == 'ansi':
        assert_gpu_and_cpu_error(df_fun=lambda spark: div_by_zero_func(spark).collect(),
                                 conf=ansi_conf,
                                 error_message=err_message)
    else:
        assert_gpu_and_cpu_are_equal_collect(div_by_zero_func, ansi_conf)


@pytest.mark.parametrize('expr', ['a/0', 'a/b'])
@pytest.mark.parametrize('ansi', [True, False])
def test_div_by_zero(expr, ansi):
    _test_div_by_zero(ansi_mode=ansi, expr=expr)

# We want to test literals separate from expressions because Spark 3.4 throws different exceptions
@pytest.mark.parametrize('ansi', [True, False])
def test_div_by_zero_literal(ansi):
    _test_div_by_zero(ansi_mode=ansi, expr='1/0', is_lit=True)

def _get_div_overflow_df(spark, expr):
    return spark.createDataFrame(
        [(LONG_MIN, -1)],
        ['a', 'b']
    ).selectExpr(expr)

def _div_overflow_exception_when(expr, ansi_enabled, is_lit=False):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_enabled}
    err_exp = 'java.lang.ArithmeticException' if is_before_spark_330() else \
        'org.apache.spark.SparkArithmeticException' \
            if not is_lit or not is_spark_340_or_later() else "pyspark.errors.exceptions.captured.ArithmeticException"
    err_mess = ': Overflow in integral divide' \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        ': [ARITHMETIC_OVERFLOW] Overflow in integral divide'
    if ansi_enabled:
        assert_gpu_and_cpu_error(
            df_fun=lambda spark: _get_div_overflow_df(spark, expr).collect(),
            conf=ansi_conf,
            error_message=err_exp + err_mess)
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_div_overflow_df(spark, expr),
            conf=ansi_conf)

# Only run this test for Spark v3.2.0 and later to verify IntegralDivide will
# throw exceptions for overflow when ANSI mode is enabled.
@pytest.mark.skipif(is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['a DIV CAST(-1 AS INT)', 'a DIV b'])
@pytest.mark.parametrize('ansi_enabled', [False, True])
def test_div_overflow_exception_when_ansi(expr, ansi_enabled):
    _div_overflow_exception_when(expr, ansi_enabled)

# Only run this test for Spark v3.2.0 and later to verify IntegralDivide will
# throw exceptions for overflow when ANSI mode is enabled.
# We have split this test from test_div_overflow_exception_when_ansi because Spark 3.4
# throws a different exception for literals
@pytest.mark.skipif(is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['CAST(-9223372036854775808L as LONG) DIV -1'])
@pytest.mark.parametrize('ansi_enabled', [False, True])
def test_div_overflow_exception_when_ansi_literal(expr, ansi_enabled):
    _div_overflow_exception_when(expr, ansi_enabled, is_lit=True)

# Only run this test before Spark v3.2.0 to verify IntegralDivide will NOT
# throw exceptions for overflow even ANSI mode is enabled.
@pytest.mark.skipif(not is_before_spark_320(), reason='https://github.com/apache/spark/pull/32260')
@pytest.mark.parametrize('expr', ['CAST(-9223372036854775808L as LONG) DIV -1', 'a DIV CAST(-1 AS INT)', 'a DIV b'])
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_div_overflow_no_exception_when_ansi(expr, ansi_enabled):
    assert_gpu_and_cpu_are_equal_collect(
        func=lambda spark: _get_div_overflow_df(spark, expr),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

_data_type_expr_for_add_overflow = [
    ([127], ByteType(), 'a + 1Y'),
    ([-128], ByteType(), '-1Y + a'),
    ([32767], ShortType(), 'a + 1S'),
    ([-32768], ShortType(), '-1S + a'),
    ([2147483647], IntegerType(), 'a + 1'),
    ([-2147483648], IntegerType(), '-1 + a'),
    ([9223372036854775807],  LongType(), 'a + 1L'),
    ([-9223372036854775808], LongType(), '-1L + a'),
    ([3.4028235E38],  FloatType(), 'a + a'),
    ([-3.4028235E38], FloatType(), 'a + a'),
    ([1.7976931348623157E308],  DoubleType(), 'a + a'),
    ([-1.7976931348623157E308], DoubleType(), 'a + a'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a + -1'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a + a'),
    ([Decimal('9' * 38)], DecimalType(38,0), 'a + 1'),
    ([Decimal('9' * 38)], DecimalType(38,0), 'a + 1')]

@pytest.mark.parametrize('data,tp,expr', _data_type_expr_for_add_overflow, ids=idfn)
def test_add_overflow_with_ansi_enabled(data, tp, expr):
    if isinstance(tp, IntegralType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='java.lang.ArithmeticException' if is_before_spark_330() else 'SparkArithmeticException')
    elif isinstance(tp, DecimalType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_overflow_df(spark, data, tp, expr),
            conf=ansi_enabled_conf)


_data_type_expr_for_sub_overflow = [
    ([-128], ByteType(), 'a - 1Y'),
    ([-32768], ShortType(), 'a -1S'),
    ([-2147483648], IntegerType(), 'a - 1'),
    ([-9223372036854775808], LongType(), 'a - 1L'),
    ([-3.4028235E38], FloatType(), 'a - cast(1.0 as float)'),
    ([-1.7976931348623157E308], DoubleType(), 'a - 1.0'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a - 1'),
    ([Decimal('-' + '9' * 38)], DecimalType(38,0), 'a - (-a)')
    ]

@pytest.mark.parametrize('data,tp,expr', _data_type_expr_for_sub_overflow, ids=idfn)
def test_subtraction_overflow_with_ansi_enabled(data, tp, expr):
    if isinstance(tp, IntegralType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='java.lang.ArithmeticException' if is_before_spark_330() else 'SparkArithmeticException')
    elif isinstance(tp, DecimalType):
        assert_gpu_and_cpu_error(
            lambda spark: _get_overflow_df(spark, data, tp, expr).collect(),
            conf=ansi_enabled_conf,
            error_message='')
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: _get_overflow_df(spark, data, tp, expr),
            conf=ansi_enabled_conf)


@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_unary_minus_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DAY_TIME_GEN_NO_OVER_FLOW).selectExpr('-a'),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_unary_minus_ansi_overflow_day_time_interval(ansi_enabled):
    """
    We don't check the error messages because they are different on CPU and GPU.
    CPU: long overflow.
    GPU: One or more rows overflow for minus operation.
    """
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df(spark, [timedelta(microseconds=LONG_MIN)], DayTimeIntervalType(), '-a').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='SparkArithmeticException')

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_abs_ansi_no_overflow_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DAY_TIME_GEN_NO_OVER_FLOW).selectExpr('abs(a)'),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_abs_ansi_overflow_day_time_interval(ansi_enabled):
    """
    Check the error message only when ANSI mode is false because they are different on CPU and GPU.
    CPU: long overflow.
    GPU: One or more rows overflow for abs operation.
    """
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df(spark, [timedelta(microseconds=LONG_MIN)], DayTimeIntervalType(), 'abs(a)').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='' if ansi_enabled else 'SparkArithmeticException')

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_addition_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, DAY_TIME_GEN_NO_OVER_FLOW, DAY_TIME_GEN_NO_OVER_FLOW).select(
            f.col('a') + f.col('b')),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_add_overflow_with_ansi_enabled_day_time_interval(ansi_enabled):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.createDataFrame(
            SparkContext.getOrCreate().parallelize([(timedelta(microseconds=LONG_MAX), timedelta(microseconds=10)),]),
            StructType([StructField('a', DayTimeIntervalType()), StructField('b', DayTimeIntervalType())])
        ).selectExpr('a + b').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='SparkArithmeticException')

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_subtraction_day_time_interval(ansi_enabled):
    DAY_TIME_GEN_NO_OVER_FLOW = DayTimeIntervalGen(min_value=timedelta(days=-2000*365), max_value=timedelta(days=3000*365))
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: two_col_df(spark, DAY_TIME_GEN_NO_OVER_FLOW, DAY_TIME_GEN_NO_OVER_FLOW).select(
            f.col('a') - f.col('b')),
        conf={'spark.sql.ansi.enabled': ansi_enabled})

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('ansi_enabled', ['false', 'true'])
def test_subtraction_overflow_with_ansi_enabled_day_time_interval(ansi_enabled):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: spark.createDataFrame(
            SparkContext.getOrCreate().parallelize([(timedelta(microseconds=LONG_MIN), timedelta(microseconds=10)),]),
            StructType([StructField('a', DayTimeIntervalType()), StructField('b', DayTimeIntervalType())])
        ).selectExpr('a - b').collect(),
        conf={'spark.sql.ansi.enabled': ansi_enabled},
        error_message='SparkArithmeticException')

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
def test_unary_positive_day_time_interval():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, DayTimeIntervalGen()).selectExpr('+a'))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=-3, max_exp=5, special_cases=[0.0])], ids=idfn)
def test_day_time_interval_multiply_number(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-20 * 86400), max_value=timedelta(seconds=20 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 * _c2"))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=0, max_exp=5, special_cases=[])], ids=idfn)
def test_day_time_interval_division_number_no_overflow1(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-5000 * 365 * 86400), max_value=timedelta(seconds=5000 * 365 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        # avoid dividing by 0
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 / case when _c2 = 0 then cast(1 as {}) else _c2 end".format(to_cast_string(data_gen.data_type))))

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_gen', _no_overflow_multiply_gens_for_fallback + [DoubleGen(min_exp=-5, max_exp=0, special_cases=[])], ids=idfn)
def test_day_time_interval_division_number_no_overflow2(data_gen):
    gen_list = [('_c1', DayTimeIntervalGen(min_value=timedelta(seconds=-20 * 86400), max_value=timedelta(seconds=20 * 86400))),
                ('_c2', data_gen)]
    assert_gpu_and_cpu_are_equal_collect(
        # avoid dividing by 0
        lambda spark: gen_df(spark, gen_list).selectExpr("_c1 / case when _c2 = 0 then cast(1 as {}) else _c2 end".format(to_cast_string(data_gen.data_type))))

def _get_overflow_df_1col(spark, data_type, value, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([value]),
        StructType([
            StructField('a', data_type)
        ])
    ).selectExpr(expr)

def _get_overflow_df_2cols(spark, data_types, values, expr):
    return spark.createDataFrame(
        SparkContext.getOrCreate().parallelize([values]),
        StructType([
            StructField('a', data_types[0]),
            StructField('b', data_types[1])
        ])
    ).selectExpr(expr)

# test interval division overflow, such as interval / 0, Long.MinValue / -1 ...
@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_type,value_pair', [
    (LongType(), [MIN_DAY_TIME_INTERVAL, -1]),
    (IntegerType(), [timedelta(microseconds=LONG_MIN), -1])
], ids=idfn)
def test_day_time_interval_division_overflow(data_type, value_pair):
    exception_message = "SparkArithmeticException: Overflow in integral divide." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [ARITHMETIC_OVERFLOW] Overflow in integral divide."
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_type,value_pair', [
    (FloatType(), [MAX_DAY_TIME_INTERVAL, 0.1]),
    (DoubleType(), [MAX_DAY_TIME_INTERVAL, 0.1]),
    (FloatType(), [MIN_DAY_TIME_INTERVAL, 0.1]),
    (DoubleType(), [MIN_DAY_TIME_INTERVAL, 0.1]),
], ids=idfn)
def test_day_time_interval_division_round_overflow(data_type, value_pair):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message='java.lang.ArithmeticException')

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_type,value_pair', [
    (ByteType(), [timedelta(seconds=1), 0]),
    (ShortType(), [timedelta(seconds=1), 0]),
    (IntegerType(), [timedelta(seconds=1), 0]),
    (LongType(), [timedelta(seconds=1), 0]),
    (FloatType(), [timedelta(seconds=1), 0.0]),
    (FloatType(), [timedelta(seconds=1), -0.0]),
    (DoubleType(), [timedelta(seconds=1), 0.0]),
    (DoubleType(), [timedelta(seconds=1), -0.0]),
    (FloatType(), [timedelta(seconds=0), 0.0]),   # 0 / 0 = NaN
    (DoubleType(), [timedelta(seconds=0), 0.0]),  # 0 / 0 = NaN
], ids=idfn)
def test_day_time_interval_divided_by_zero(data_type, value_pair):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero"
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('zero_literal', ['0', '0.0f', '-0.0f'], ids=idfn)
def test_day_time_interval_divided_by_zero_scalar(zero_literal):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero."
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_1col(spark, DayTimeIntervalType(), [timedelta(seconds=1)], 'a / ' + zero_literal).collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_type,value', [
    (ByteType(), 0),
    (ShortType(), 0),
    (IntegerType(), 0),
    (LongType(), 0),
    (FloatType(), 0.0),
    (FloatType(), -0.0),
    (DoubleType(), 0.0),
    (DoubleType(), -0.0),
], ids=idfn)
def test_day_time_interval_scalar_divided_by_zero(data_type, value):
    exception_message = "SparkArithmeticException: Division by zero." \
        if is_before_spark_340() and not is_databricks113_or_later() else \
        "SparkArithmeticException: [INTERVAL_DIVIDED_BY_ZERO] Division by zero."
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_1col(spark, data_type, [value], 'INTERVAL 1 SECOND / a').collect(),
        conf={},
        error_message=exception_message)

@pytest.mark.skipif(is_before_spark_330(), reason='DayTimeInterval is not supported before Pyspark 3.3.0')
@pytest.mark.parametrize('data_type,value_pair', [
    (FloatType(), [timedelta(seconds=1), float('NaN')]),
    (DoubleType(), [timedelta(seconds=1), float('NaN')]),
], ids=idfn)
def test_day_time_interval_division_nan(data_type, value_pair):
    assert_gpu_and_cpu_error(
        df_fun=lambda spark: _get_overflow_df_2cols(spark, [DayTimeIntervalType(), data_type], value_pair, 'a / b').collect(),
        conf={},
        error_message='java.lang.ArithmeticException')
