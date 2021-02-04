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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from marks import incompat, approximate_float
from pyspark.sql.types import *
from spark_session import with_cpu_session, with_gpu_session, with_spark_session, is_before_spark_310
import pyspark.sql.functions as f

decimal_gens_not_max_prec = [decimal_gen_neg_scale, decimal_gen_scale_precision,
        decimal_gen_same_scale_precision, decimal_gen_64bit]

@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens_not_max_prec, ids=idfn)
def test_addition(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type),
                f.col('a') + f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens_not_max_prec, ids=idfn)
def test_subtraction(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type),
                f.col('a') - f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', numeric_gens +
        [decimal_gen_neg_scale, decimal_gen_scale_precision, decimal_gen_same_scale_precision, DecimalGen(8, 8)], ids=idfn)
def test_multiplication(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) * f.col('b'),
                f.lit(None).cast(data_type) * f.col('a'),
                f.col('b') * f.lit(None).cast(data_type),
                f.col('a') * f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(6, 4), DecimalGen(5, 4), DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(3, -2)], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(6, 3)], ids=idfn)
def test_multiplication_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') * f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', [double_gen, decimal_gen_neg_scale, DecimalGen(6, 3), DecimalGen(5, 5), DecimalGen(6, 0)], ids=idfn)
def test_division(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') / f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) / f.col('b'),
                f.lit(None).cast(data_type) / f.col('a'),
                f.col('b') / f.lit(None).cast(data_type),
                f.col('a') / f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('lhs', [DecimalGen(5, 3), DecimalGen(4, 2), DecimalGen(1, -2)], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(4, 1)], ids=idfn)
def test_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).select(
                f.col('a') / f.col('b')),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', integral_gens +  [decimal_gen_default, decimal_gen_scale_precision,
        decimal_gen_same_scale_precision, decimal_gen_64bit], ids=idfn)
def test_int_division(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a DIV cast(100 as {})'.format(string_type),
                'cast(-12 as {}) DIV b'.format(string_type),
                'cast(null as {}) DIV a'.format(string_type),
                'b DIV cast(null as {})'.format(string_type),
                'a DIV b'))

@pytest.mark.parametrize('lhs', [DecimalGen(6, 5), DecimalGen(5, 4), DecimalGen(3, -2)], ids=idfn)
@pytest.mark.parametrize('rhs', [DecimalGen(13, 2), DecimalGen(6, 3)], ids=idfn)
def test_int_division_mixed(lhs, rhs):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, lhs, rhs).selectExpr(
                'a DIV b'),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_mod(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') % f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) % f.col('b'),
                f.lit(None).cast(data_type) % f.col('a'),
                f.col('b') % f.lit(None).cast(data_type),
                f.col('a') % f.col('b')))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_pmod(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'pmod(a, cast(100 as {}))'.format(string_type),
                'pmod(cast(-12 as {}), b)'.format(string_type),
                'pmod(cast(null as {}), a)'.format(string_type),
                'pmod(b, cast(null as {}))'.format(string_type),
                'pmod(a, b)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_signum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('signum(a)'))

@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens, ids=idfn)
def test_unary_minus(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'),
            conf=allow_negative_scale_of_decimal_conf)

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens, ids=idfn)
def test_unary_positive(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('+a'),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', numeric_gens + decimal_gens, ids=idfn)
def test_abs(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'),
            conf=allow_negative_scale_of_decimal_conf)

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_sqrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sqrt(a)'))

@pytest.mark.parametrize('data_gen', double_n_long_gens + decimal_gens, ids=idfn)
def test_floor(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a)'),
            conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', double_n_long_gens + decimal_gens, ids=idfn)
def test_ceil(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a)'),
            conf=allow_negative_scale_of_decimal_conf)

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

@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', round_gens, ids=idfn)
def test_decimal_bround(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'bround(a)',
                'bround(a, -1)',
                'bround(a, 1)',
                'bround(a, 2)',
                'bround(a, 10)'),
                conf=allow_negative_scale_of_decimal_conf)

@incompat
@approximate_float
@pytest.mark.parametrize('data_gen', round_gens, ids=idfn)
def test_decimal_round(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark: unary_op_df(spark, data_gen).selectExpr(
                'round(a)',
                'round(a, -1)',
                'round(a, 1)',
                'round(a, 2)',
                'round(a, 10)'),
               conf=allow_negative_scale_of_decimal_conf)

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

@approximate_float
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
@pytest.mark.xfail(reason='https://github.com/NVIDIA/spark-rapids/issues/109')
def test_degrees(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('degrees(a)'))

# Once https://github.com/NVIDIA/spark-rapids/issues/109 is fixed this can be removed
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

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
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
                f.least(*command_args)), conf=allow_negative_scale_of_decimal_conf)

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_gens, ids=idfn)
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
                f.greatest(*command_args)), conf=allow_negative_scale_of_decimal_conf)


def _test_div_by_zero(ansi_mode, expr):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_mode == 'ansi'}
    data_gen = lambda spark: two_col_df(spark, IntegerGen(), IntegerGen(min_val=0, max_val=0), length=1)
    div_by_zero_func = lambda spark: data_gen(spark).selectExpr(expr)

    if ansi_mode == 'ansi':
        assert_gpu_and_cpu_error(df_fun=lambda spark: div_by_zero_func(spark).collect(),
                                 conf=ansi_conf,
                                 error_message='java.lang.ArithmeticException: divide by zero')
    else:
        assert_gpu_and_cpu_are_equal_collect(div_by_zero_func, ansi_conf)


@pytest.mark.parametrize('expr', ['1/0', 'a/0', 'a/b'])
@pytest.mark.xfail(condition=is_before_spark_310(), reason='https://github.com/apache/spark/pull/29882')
def test_div_by_zero_ansi(expr):
    _test_div_by_zero(ansi_mode='ansi', expr=expr)

@pytest.mark.parametrize('expr', ['1/0', 'a/0', 'a/b'])
def test_div_by_zero_nonansi(expr):
    _test_div_by_zero(ansi_mode='nonAnsi', expr=expr)
