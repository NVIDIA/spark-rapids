# Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_fallback_collect
from data_gen import *
from marks import allow_non_gpu, disable_ansi_mode, ignore_order


@ignore_order(local=True)
def test_tiered_project_with_complex_transform():
    confs = {"spark.rapids.sql.tiered.project.enabled": "true"}
    def do_project(spark):
        df = spark.createDataFrame(
            [
                (1, "a", [(0, "z"), (1, "y")]),
                (2, "b", [(2, "x")])
            ],
            "a int, b string, c array<struct<x: int, y: string>>").repartition(2)
        return df.selectExpr(
            "transform(c, (v, i) -> named_struct('x', c[i].x, 'y', c[i].y)) AS t")
    assert_gpu_and_cpu_are_equal_collect(do_project, conf=confs)


# --- ArrayAggregate tests ---
#
# The decomposer accepts lambdas of the form `(acc, x) -> op(acc, g(x))` with an identity
# finish, where `op` is one of SUM/PRODUCT/MAX/MIN/ALL/ANY. Other shapes fall back to CPU.


# Happy path for each supported numeric op. Product uses a narrow range to keep the test
# output numerically tame (GPU and CPU both wrap consistently, but small numbers make the
# test easier to read when debugging a failure).
@pytest.mark.parametrize('lambda_sql, init_sql, gen_max', [
    ('(acc, x) -> acc + CAST(x as BIGINT)', '0L', 100),
    ('(acc, x) -> acc * CAST(x as BIGINT)', '1L', 3),
    ('(acc, x) -> greatest(acc, CAST(x as BIGINT))', '-9223372036854775808L', 100),
    ('(acc, x) -> least(acc, CAST(x as BIGINT))', '9223372036854775807L', 100),
], ids=['sum', 'product', 'max', 'min'])
@disable_ansi_mode
def test_array_aggregate_numeric_ops(lambda_sql, init_sql, gen_max):
    gen = IntegerGen(min_val=-gen_max, max_val=gen_max)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Same ops exercised on the native element type (no Cast in the lambda body), so the
# identityScalar / combineWithZero paths for Int / Long are hit directly. Covers the
# INCLUDE-policy null-element propagation for SUM on a nullable element type too.
@pytest.mark.parametrize('gen, lambda_sql, init_sql', [
    (IntegerGen(min_val=-100, max_val=100), '(acc, x) -> acc + x', '0'),
    (LongGen(min_val=-100, max_val=100), '(acc, x) -> acc + x', '0L'),
    (IntegerGen(min_val=-100, max_val=100),
        '(acc, x) -> greatest(acc, x)', 'CAST(-9999 as INT)'),
    (LongGen(min_val=-100, max_val=100),
        '(acc, x) -> least(acc, x)', '9223372036854775807L'),
], ids=['int-sum', 'long-sum', 'int-max', 'long-min'])
@disable_ansi_mode
def test_array_aggregate_native_integer_ops(gen, lambda_sql, init_sql):
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Happy path for the boolean ops. Elements must be non-null because cuDF's segmented ALL/
# ANY with INCLUDE nulls don't match Spark's AND/OR 3VL for mixed null+bool (specifically,
# `false AND null = false` short-circuit; `true OR null = true`). The tag-time guard falls
# back to CPU when the element type is nullable, so here we use a non-nullable BooleanGen.
@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc AND x', 'true'),
    ('(acc, x) -> acc OR x', 'false'),
], ids=['all', 'any'])
@disable_ansi_mode
def test_array_aggregate_boolean_ops(lambda_sql, init_sql):
    non_null_bool = BooleanGen(nullable=False)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(non_null_bool, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# When array elements may contain nulls, ALL/ANY must fall back to CPU (cuDF's INCLUDE-
# nulls semantics don't match Spark's AND/OR 3VL).
@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc AND x', 'true'),
    ('(acc, x) -> acc OR x', 'false'),
], ids=['all', 'any'])
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_boolean_ops_nullable_elements_fallback(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(boolean_gen, max_length=8)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


# Count-if pattern (structural twin of the client's real workload).
@disable_ansi_mode
def test_array_aggregate_count_if_int():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            'aggregate(a, 0, (acc, x) -> acc + CASE WHEN x > 0 THEN 1 ELSE 0 END) as pos_cnt',
            'aggregate(a, 0L, (acc, x) -> acc + CAST(CASE WHEN x IS NULL THEN 1 ELSE 0 END as BIGINT)) as null_cnt'))


# Client's actual pattern (simplified): filter + aggregate with split / GetArrayItem / IN.
@disable_ansi_mode
def test_array_aggregate_client_pattern():
    field_gen = StringGen('[a-z]{2}')
    def do_it(spark):
        df = unary_op_df(spark, ArrayGen(field_gen, max_length=5))
        return df.selectExpr("""
            aggregate(
              filter(transform(a, x -> concat_ws('    ', x, x, x, x, x)), z -> z != ''),
              0L,
              (acc, z) -> acc + CAST(CASE WHEN (
                size(split(z, '    ', -1)) > 2
                AND split(z, '    ', -1)[2] IN ('aa', 'bb')
                AND NOT split(z, '    ', -1)[1] IN ('xx', 'yy')
              ) THEN 1 ELSE 0 END as BIGINT),
              id -> id
            ) as client_cnt""")
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Non-zero init: result should include the init.
@disable_ansi_mode
def test_array_aggregate_non_zero_init():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=10)).selectExpr(
            'aggregate(a, 100L, (acc, x) -> acc + CAST(x as BIGINT)) as sum_with_init'))


# null array -> null, empty array -> finish(init) = init.
@disable_ansi_mode
def test_array_aggregate_null_array():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, all_null=True)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT)) as should_be_null'))


@disable_ansi_mode
def test_array_aggregate_empty_array():
    def do_it(spark):
        return spark.createDataFrame(
            [([1, 2, 3],), ([],), ([7],), ([],)],
            'a array<int>').selectExpr(
                'aggregate(a, 42L, (acc, x) -> acc + CAST(x as BIGINT)) as sum_with_empty')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Lambda body references an outer attribute — exercises boundIntermediate plumbing.
@disable_ansi_mode
def test_array_aggregate_lambda_refs_outer_column():
    def do_it(spark):
        return two_col_df(spark, ArrayGen(int_gen, max_length=10), int_gen).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x + b as BIGINT)) as sum_with_outer')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# zero is an outer column, not a literal.
@disable_ansi_mode
def test_array_aggregate_zero_is_outer_column():
    def do_it(spark):
        return two_col_df(spark, ArrayGen(int_gen, max_length=10), long_gen).selectExpr(
            'aggregate(a, b, (acc, x) -> acc + CAST(x as BIGINT)) as sum_from_col')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# array<struct>: accumulate over a struct field.
@disable_ansi_mode
def test_array_aggregate_over_struct_field():
    def do_it(spark):
        elem_gen = StructGen([['i', int_gen]], nullable=False)
        return unary_op_df(spark, ArrayGen(elem_gen, max_length=10)).selectExpr(
            'aggregate(a, 0L, (acc, s) -> acc + CAST(s.i as BIGINT)) as sum_field')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Deeper g body without acc references (x * 2 + 1).
@disable_ansi_mode
def test_array_aggregate_deeper_g_body():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=10)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x * 2 + 1 as BIGINT)) as sum_poly'))


# Long-overflow wrap-around matches between Spark SUM and cudf SUM in non-ANSI mode.
@disable_ansi_mode
def test_array_aggregate_long_overflow_wraps():
    def do_it(spark):
        big = LongGen(min_val=9223372036854775000, max_val=9223372036854775700, nullable=False)
        return unary_op_df(spark, ArrayGen(big, min_length=5, max_length=15)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + x) as wrapped_sum')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Decimal SUM: zero must be widened to DECIMAL(38,2) (Spark's cap) with the element Cast to
# match so that merge.dataType == zero.dataType (Spark's checkInputDataTypes).
@disable_ansi_mode
def test_array_aggregate_decimal_sum():
    decimal_gen = DecimalGen(precision=10, scale=2)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(decimal_gen, max_length=8)).selectExpr(
            'aggregate(a, CAST(0 as DECIMAL(38,2)), '
            '(acc, x) -> acc + CAST(x as DECIMAL(38,2))) as dec_sum')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Shapes the decomposer rejects must fall back to CPU. Covered: non-associative op
# (Subtract, Divide), variadic op with wrong arity (Greatest with 3 children), and a lambda
# whose g sub-expression references the accumulator.
@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> acc - CAST(x as BIGINT)', '0L'),
    ('(acc, x) -> CAST(acc / CAST(x + 1 as BIGINT) as BIGINT)', '1L'),
    ('(acc, x) -> greatest(acc, CAST(x as BIGINT), CAST(x * 2 as BIGINT))', '-999L'),
    ('(acc, x) -> acc + acc * CAST(x as BIGINT)', '0L'),
], ids=['subtract', 'divide', 'greatest-3ary', 'g-refs-acc'])
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_fallback_shapes(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')


# Non-identity finish is kept as its own test because its SQL shape (4-arg aggregate with
# a separate finish lambda) differs from the merge-only fallbacks above.
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_non_identity_finish_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT), acc -> acc * 2) as doubled'),
        'ArrayAggregate')


# MAX / MIN on float/double arrays must fall back: cuDF's segmented max/min follow IEEE 754
# where NaN is absorbed (`fmax(NaN, x) = x`), while Spark's `Greatest`/`Least` propagate NaN
# via `Double.compare`. Rather than paper over this for now we restrict ExtremumOp to
# integral types and fall back on float/double.
@pytest.mark.parametrize('lambda_sql, init_sql', [
    ('(acc, x) -> greatest(acc, x)', 'CAST("-Infinity" as DOUBLE)'),
    ('(acc, x) -> least(acc, x)', 'CAST("Infinity" as DOUBLE)'),
], ids=['max', 'min'])
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_double_extremum_falls_back(lambda_sql, init_sql):
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(double_gen, max_length=5)).selectExpr(
            f'aggregate(a, {init_sql}, {lambda_sql}) as res'),
        'ArrayAggregate')
