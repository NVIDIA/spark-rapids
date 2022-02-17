# Copyright (c) 2020-2022, NVIDIA CORPORATION.
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
from pyspark.sql.types import *
import pyspark.sql.functions as f

def mk_str_gen(pattern):
    return StringGen(pattern).with_special_case('').with_special_pattern('.{0,10}')

all_gens = all_gen + [NullGen()]
all_nested_gens = array_gens_sample + struct_gens_sample + map_gens_sample
all_nested_gens_nonempty_struct = array_gens_sample + nonempty_struct_gens_sample

# Create dedicated data gens of nested type for 'if' tests here with two exclusions:
#   1) Excludes the nested 'NullGen' because it seems to be impossible to convert the
#      'NullType' to a SQL type string. But the top level NullGen is handled specially
#      in 'gen_scalars_for_sql'.
#   2) Excludes the empty struct gen 'Struct()' because it leads to an error as below
#      in both cpu and gpu runs.
#      E: java.lang.AssertionError: assertion failed: each serializer expression should contain\
#         at least one `BoundReference`
if_array_gens_sample = [ArrayGen(sub_gen) for sub_gen in all_gen] + nested_array_gens_sample
if_struct_gen = StructGen([['child'+str(ind), sub_gen] for ind, sub_gen in enumerate(all_gen)])
if_struct_gens_sample = [if_struct_gen,
        StructGen([['child0', byte_gen], ['child1', if_struct_gen]]),
        StructGen([['child0', ArrayGen(short_gen)], ['child1', double_gen]])]
if_nested_gens = if_array_gens_sample + if_struct_gens_sample

@pytest.mark.parametrize('data_gen', all_gens + if_nested_gens + decimal_128_gens_no_neg, ids=idfn)
def test_if_else(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, boolean_gen, data_gen, data_gen).selectExpr(
                'IF(TRUE, b, c)',
                'IF(TRUE, {}, {})'.format(s1, null_lit),
                'IF(FALSE, {}, {})'.format(s1, null_lit),
                'IF(a, b, c)',
                'IF(a, {}, c)'.format(s1),
                'IF(a, b, {})'.format(s2),
                'IF(a, {}, {})'.format(s1, s2),
                'IF(a, b, {})'.format(null_lit),
                'IF(a, {}, c)'.format(null_lit)))

# Maps scalars are not really supported by Spark from python without jumping through a lot of hoops
# so for now we are going to skip them
@pytest.mark.parametrize('data_gen', map_gens_sample, ids=idfn)
def test_if_else_map(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, boolean_gen, data_gen, data_gen).selectExpr(
                'IF(TRUE, b, c)',
                'IF(a, b, c)'))

@pytest.mark.order(1) # at the head of xdist worker queue if pytest-order is installed
@pytest.mark.parametrize('data_gen', all_gens + all_nested_gens + single_array_gens_sample_with_decimal128 + decimal_128_gens, ids=idfn)
def test_case_when(data_gen):
    num_cmps = 20
    s1 = gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen))
    # we want lots of false
    bool_gen = BooleanGen().with_special_case(False, weight=1000.0)
    gen_cols = [('_b' + str(x), bool_gen) for x in range(0, num_cmps)]
    gen_cols = gen_cols + [('_c' + str(x), data_gen) for x in range(0, num_cmps)]
    gen = StructGen(gen_cols, nullable=False)
    command = f.when(f.col('_b0'), f.col('_c0'))
    for x in range(1, num_cmps):
        command = command.when(f.col('_b'+ str(x)), f.col('_c' + str(x)))
    command = command.otherwise(s1)
    data_type = data_gen.data_type
    # `command` covers the case of (column, scalar) for values, so the following 3 ones
    # are for
    #    (scalar, scalar)  -> the default `otherwise` is a scalar.
    #    (column, column)
    #    (scalar, column)
    # in sequence.
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(command,
                f.when(f.col('_b0'), s1),
                f.when(f.col('_b0'), f.col('_c0')).otherwise(f.col('_c1')),
                f.when(f.col('_b0'), s1).otherwise(f.col('_c0')),
                f.when(f.col('_b0'), s1).when(f.lit(False), f.col('_c0')),
                f.when(f.col('_b0'), s1).when(f.lit(True), f.col('_c0')),
                f.when(f.col('_b0'), f.lit(None).cast(data_type)).otherwise(f.col('_c0')),
                f.when(f.lit(False), f.col('_c0'))))

@pytest.mark.parametrize('data_gen', [float_gen, double_gen], ids=idfn)
def test_nanvl(data_gen):
    s1 = gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen))
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.nanvl(f.col('a'), f.col('b')),
                f.nanvl(f.col('a'), s1.cast(data_type)),
                f.nanvl(f.lit(None).cast(data_type), f.col('b')),
                f.nanvl(f.lit(float('nan')).cast(data_type), f.col('b'))))

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_128_gens_no_neg, ids=idfn)
def test_nvl(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'nvl(a, b)',
                'nvl(a, {})'.format(s2),
                'nvl({}, b)'.format(s1),
                'nvl({}, b)'.format(null_lit),
                'nvl(a, {})'.format(null_lit)))

#nvl is translated into a 2 param version of coalesce
# Exclude the empty struct gen 'Struct()' because it leads to an error as below
# in both cpu and gpu runs.
#      E: java.lang.AssertionError: assertion failed: each serializer expression should contain\
#         at least one `BoundReference`
@pytest.mark.parametrize('data_gen', all_gens + all_nested_gens_nonempty_struct + decimal_128_gens + single_array_gens_sample_with_decimal128, ids=idfn)
def test_coalesce(data_gen):
    num_cols = 20
    s1 = gen_scalar(data_gen, force_no_nulls=not isinstance(data_gen, NullGen))
    # we want lots of nulls
    gen = StructGen([('_c' + str(x), data_gen.copy_special_case(None, weight=1000.0)) 
        for x in range(0, num_cols)], nullable=False)
    command_args = [f.col('_c' + str(x)) for x in range(0, num_cols)]
    command_args.append(s1)
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : gen_df(spark, gen).select(
                f.coalesce(*command_args)))

def test_coalesce_constant_output():
    # Coalesce can allow a constant value as output. Technically Spark should mark this
    # as foldable and turn it into a constant, but it does not, so make sure our code
    # can deal with it.  (This means something like + will get two constant scalar values)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : spark.range(1, 100).selectExpr("4 + coalesce(5, id) as nine"))

@pytest.mark.parametrize('data_gen', all_basic_gens + decimal_128_gens_no_neg, ids=idfn)
def test_nvl2(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : three_col_df(spark, data_gen, data_gen, data_gen).selectExpr(
                'nvl2(a, b, c)',
                'nvl2(a, b, {})'.format(s2),
                'nvl2({}, b, c)'.format(s1),
                'nvl2({}, b, c)'.format(null_lit),
                'nvl2(a, {}, c)'.format(null_lit)))

@pytest.mark.parametrize('data_gen', eq_gens + decimal_128_gens_no_neg, ids=idfn)
def test_nullif(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'nullif(a, b)',
                'nullif(a, {})'.format(s2),
                'nullif({}, b)'.format(s1),
                'nullif({}, b)'.format(null_lit),
                'nullif(a, {})'.format(null_lit)))

@pytest.mark.parametrize('data_gen', eq_gens + decimal_128_gens_no_neg, ids=idfn)
def test_ifnull(data_gen):
    (s1, s2) = gen_scalars_for_sql(data_gen, 2, force_no_nulls=not isinstance(data_gen, NullGen))
    null_lit = get_null_lit_string(data_gen.data_type)
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).selectExpr(
                'ifnull(a, b)',
                'ifnull(a, {})'.format(s2),
                'ifnull({}, b)'.format(s1),
                'ifnull({}, b)'.format(null_lit),
                'ifnull(a, {})'.format(null_lit)))

@pytest.mark.parametrize('data_gen', [IntegerGen().with_special_case(2147483647)], ids=idfn)
def test_conditional_with_side_effects_col_col(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a < 2147483647, a + 1, a)'),
            conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [IntegerGen().with_special_case(2147483647)], ids=idfn)
def test_conditional_with_side_effects_col_scalar(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a < 2147483647, a + 1, 2147483647)',
                'IF(a >= 2147483646, 2147483647, a + 1)'),
            conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [mk_str_gen('[0-9]{1,20}')], ids=idfn)
def test_conditional_with_side_effects_cast(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'IF(a RLIKE "^[0-9]{1,5}\\z", CAST(a AS INT), 0)'),
            conf = ansi_enabled_conf)

@pytest.mark.parametrize('data_gen', [mk_str_gen('[0-9]{1,9}')], ids=idfn)
def test_conditional_with_side_effects_case_when(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : unary_op_df(spark, data_gen).selectExpr(
                'CASE \
                WHEN a RLIKE "^[0-9]{1,3}\\z" THEN CAST(a AS INT) \
                WHEN a RLIKE "^[0-9]{4,6}\\z" THEN CAST(a AS INT) + 123 \
                ELSE -1 END'),
                conf = ansi_enabled_conf)
