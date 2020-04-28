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
from pyspark.sql.types import *
import pyspark.sql.functions as f

def two_col_df(spark, a_gen, b_gen, length=2048, seed=0):
    gen = StructGen([('a', a_gen),('b', b_gen)])
    return gen_df(spark, gen, length=length, seed=seed)

def binary_op_df(spark, gen, length=2048, seed=0):
    return two_col_df(spark, gen, gen, length=length, seed=seed)

def unary_op_df(spark, gen, length=2048, seed=0):
    return gen_df(spark, StructGen([('a', gen)]), length=length, seed=seed)

def to_cast_string(spark_type):
    if isinstance(spark_type, ByteType):
        return 'BYTE'
    elif isinstance(spark_type, ShortType):
        return 'SHORT'
    elif isinstance(spark_type, IntegerType):
        return 'INT'
    elif isinstance(spark_type, LongType):
        return 'LONG'
    elif isinstance(spark_type, FloatType):
        return 'FLOAT'
    elif isinstance(spark_type, DoubleType):
        return 'DOUBLE'
    else:
        raise RuntimeError('CAST TO TYPE {} NOT SUPPORTED YET'.format(spark_type))

def debug_df(df):
    print('COLLECTED\n{}'.format(df.collect()))
    return df

def idfn(val):
    return str(val)

numeric_gens = [ByteGen(), ShortGen(), IntegerGen(), LongGen(), FloatGen(), DoubleGen()]
integral_gens = [ByteGen(), ShortGen(), IntegerGen(), LongGen()]
# A lot of mathematical expressions only support a double as input
# by parametrizing even for a single param for the test it makes the tests consistent
double_gens = [DoubleGen()]
double_n_long_gens = [DoubleGen(), LongGen()]
int_n_long_gens = [IntegerGen(), LongGen()]

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_scalar_addition(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') + f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) + f.col('b'),
                f.lit(None).cast(data_type) + f.col('a'),
                f.col('b') + f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_addition(data_gen):
    # Using iterator for now just so some test covers this path
    # this should really only be used for tests that produce a lot of data
    assert_gpu_and_cpu_are_equal_iterator(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a + b'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_scalar_subtraction(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') - f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) - f.col('b'),
                f.lit(None).cast(data_type) - f.col('a'),
                f.col('b') - f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_subtraction(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a - b'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_scalar_multiplication(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') * f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) * f.col('b'),
                f.lit(None).cast(data_type) * f.col('a'),
                f.col('b') * f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_multiplication(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a * b'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_scalar_division(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') / f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) / f.col('b'),
                f.lit(None).cast(data_type) / f.col('a'),
                f.col('b') / f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_division(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a / b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_scalar_int_division(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a DIV cast(100 as {})'.format(string_type),
                'cast(-12 as {}) DIV b'.format(string_type),
                'cast(null as {}) DIV a'.format(string_type),
                'b DIV cast(null as {})'.format(string_type)))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_columnar_int_division(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a DIV b'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_scalar_mod(data_gen):
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') % f.lit(100).cast(data_type),
                f.lit(-12).cast(data_type) % f.col('b'),
                f.lit(None).cast(data_type) % f.col('a'),
                f.col('b') % f.lit(None).cast(data_type)))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_mod(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a % b'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_signum(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('signum(a)'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_unary_minus(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('-a'))

# This just ends up being a pass through.  There is no good way to force
# a unary positive into a plan, because it gets optimized out, but this
# verifies that we can handle it.
@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_unary_positive(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('+a'))

@pytest.mark.parametrize('data_gen', numeric_gens, ids=idfn)
def test_columnar_abs(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('abs(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_sqrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sqrt(a)'))

@pytest.mark.parametrize('data_gen', double_n_long_gens, ids=idfn)
def test_columnar_floor(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('floor(a)'))

@pytest.mark.parametrize('data_gen', double_n_long_gens, ids=idfn)
def test_columnar_ceil(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('ceil(a)'))

@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_rint(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('rint(a)'))


@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_scalar_shift_left(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftLeft exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftleft(a, cast(12 as INT))',
                'shiftleft(cast(-12 as {}), b)'.format(string_type),
                'shiftleft(cast(null as {}), b)'.format(string_type),
                'shiftleft(a, cast(null as INT))'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_columnar_shift_left(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr('shiftleft(a, b)'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_scalar_shift_right(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRight exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftright(a, cast(12 as INT))',
                'shiftright(cast(-12 as {}), b)'.format(string_type),
                'shiftright(cast(null as {}), b)'.format(string_type),
                'shiftright(a, cast(null as INT))'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_columnar_shift_right(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr('shiftright(a, b)'))

@pytest.mark.parametrize('data_gen', [pytest.param(LongGen(), marks=pytest.mark.xfail(reason='https://github.com/rapidsai/cudf/issues/5015')), IntegerGen()], ids=idfn)
def test_scalar_shift_right_unsigned(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            # The version of shiftRightUnsigned exposed to dataFrame does not take a column for num bits
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr(
                'shiftrightunsigned(a, cast(12 as INT))',
                'shiftrightunsigned(cast(-12 as {}), b)'.format(string_type),
                'shiftrightunsigned(cast(null as {}), b)'.format(string_type),
                'shiftrightunsigned(a, cast(null as INT))'))

@pytest.mark.parametrize('data_gen', int_n_long_gens, ids=idfn)
def test_columnar_shift_right_unsigned(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : two_col_df(spark, data_gen, IntegerGen()).selectExpr('shiftrightunsigned(a, b)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_cbrt(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cbrt(a)'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_scalar_bit_and(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a & cast(100 as {})'.format(string_type),
                'cast(-12 as {}) & b'.format(string_type),
                'cast(null as {}) & a'.format(string_type),
                'b & cast(null as {})'.format(string_type)))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_columnar_bit_and(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a & b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_scalar_bit_or(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a | cast(100 as {})'.format(string_type),
                'cast(-12 as {}) | b'.format(string_type),
                'cast(null as {}) | a'.format(string_type),
                'b | cast(null as {})'.format(string_type)))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_columnar_bit_or(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a | b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_scalar_bit_xor(data_gen):
    string_type = to_cast_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'a ^ cast(100 as {})'.format(string_type),
                'cast(-12 as {}) ^ b'.format(string_type),
                'cast(null as {}) ^ a'.format(string_type),
                'b ^ cast(null as {})'.format(string_type)))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_columnar_bit_xor(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('a ^ b'))

@pytest.mark.parametrize('data_gen', integral_gens, ids=idfn)
def test_columnar_bit_not(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('~a'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_radians(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('radians(a)'))


@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_cos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cos(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_acos(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acos(a)'))

@pytest.mark.xfail(reason='SPAR-1124')
@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_acosh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('acosh(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_sin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('sin(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_asin(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asin(a)'))

@pytest.mark.xfail(reason='SPAR-1124')
@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_asinh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('asinh(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_tan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('tan(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_atan(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atan(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_atanh(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('atanh(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_cot(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('cot(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_exp(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('exp(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_expm1(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('expm1(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_log(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_log1p(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log1p(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_log2(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log2(a)'))

@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_log10(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr('log10(a)'))

@pytest.mark.xfail(reason='SPAR-1125')
@pytest.mark.usefixtures('incompat')
def test_scalar_logarithm():
    # For the 'b' field include a lot more values that we would expect customers to use as a part of a log
    data_gen = [('a', DoubleGen()),('b', DoubleGen().with_special_case(lambda rand: float(rand.randint(-16, 16)), weight=100.0))]
    string_type = 'DOUBLE'
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, data_gen).selectExpr(
                'log(a, cast(100 as {}))'.format(string_type),
                'log(cast(-12 as {}), b)'.format(string_type),
                'log(cast(null as {}), b)'.format(string_type),
                'log(a, cast(null as {}))'.format(string_type)))

@pytest.mark.xfail(reason='SPAR-1125')
@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_logarithm(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('log(a, b)'))

@pytest.mark.usefixtures('incompat')
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

@pytest.mark.xfail(reason='SPAR-1125')
@pytest.mark.usefixtures('incompat')
@pytest.mark.parametrize('data_gen', double_gens, ids=idfn)
def test_columnar_pow(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr('pow(a, b)'))
