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
# Covers the decomposable SUM path: lambdas of the form `(acc, x) -> acc + g(x)` with an
# identity finish. Non-decomposable shapes must fall back to CPU.

# Simple: sum elements of an array<int> with zero = 0. Accumulator type is promoted to long
# via the CAST in g, matching the zero's LongType.
@disable_ansi_mode
def test_array_aggregate_sum_int_to_long():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT)) as sum'))


# Count-if pattern: sum of CASE WHEN predicate THEN 1 ELSE 0 END.
# This is the structural twin of the client's real workload.
@disable_ansi_mode
def test_array_aggregate_count_if_int():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=15)).selectExpr(
            'aggregate(a, 0, (acc, x) -> acc + CASE WHEN x > 0 THEN 1 ELSE 0 END) as pos_cnt',
            'aggregate(a, 0L, (acc, x) -> acc + CAST(CASE WHEN x IS NULL THEN 1 ELSE 0 END as BIGINT)) as null_cnt'))


# Client's actual pattern (simplified to two fields): filter + aggregate with split / GetArrayItem / IN.
# The string data is synthesized with 4-space separators so each element has enough columns.
@disable_ansi_mode
def test_array_aggregate_client_pattern():
    # Generate strings like "aa    bb    cc    dd    ee" so the split yields >2 pieces.
    field_gen = StringGen('[a-z]{2}')
    # Build strings via concat_ws in SQL; use a simple array of ~5 strings.
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


# Null and empty arrays. Spark semantics: null array -> null, empty array -> finish(init) = init.
@disable_ansi_mode
def test_array_aggregate_null_array():
    assert_gpu_and_cpu_are_equal_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, all_null=True)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT)) as should_be_null'))


@disable_ansi_mode
def test_array_aggregate_empty_array():
    def do_it(spark):
        # Array column with some empty arrays interspersed.
        return spark.createDataFrame(
            [([1, 2, 3],), ([],), ([7],), ([],)],
            'a array<int>').selectExpr(
                'aggregate(a, 42L, (acc, x) -> acc + CAST(x as BIGINT)) as sum_with_empty')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Non-decomposable lambda must fall back to CPU. `acc - x` is not associative / not in whitelist.
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_subtract_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc - CAST(x as BIGINT)) as diff'),
        'ArrayAggregate')


# Non-identity finish must fall back.
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_non_identity_finish_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + CAST(x as BIGINT), acc -> acc * 2) as doubled'),
        'ArrayAggregate')


# g that references the accumulator must fall back.
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_g_references_acc_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + acc * CAST(x as BIGINT)) as recur'),
        'ArrayAggregate')


# Multiplicative accumulator (not in the SUM whitelist) must fall back.
@disable_ansi_mode
@allow_non_gpu('ProjectExec')
def test_array_aggregate_product_falls_back():
    assert_gpu_fallback_collect(
        lambda spark: unary_op_df(spark, ArrayGen(int_gen, max_length=5)).selectExpr(
            'aggregate(a, 1L, (acc, x) -> acc * CAST(x as BIGINT)) as prod'),
        'ArrayAggregate')


# Lambda body references an outer attribute ("b") — exercises boundIntermediate plumbing.
@disable_ansi_mode
def test_array_aggregate_lambda_refs_outer_column():
    def do_it(spark):
        return two_col_df(spark, ArrayGen(int_gen, max_length=10), int_gen).selectExpr(
            # g(x) = (x + b) — b is a closed-over outer column, not the acc.
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


# Long-overflow wrap-around: in non-ANSI mode both Spark SUM and cudf SUM wrap silently.
@disable_ansi_mode
def test_array_aggregate_long_overflow_wraps():
    def do_it(spark):
        big = LongGen(min_val=9223372036854775000, max_val=9223372036854775700, nullable=False)
        return unary_op_df(spark, ArrayGen(big, min_length=5, max_length=15)).selectExpr(
            'aggregate(a, 0L, (acc, x) -> acc + x) as wrapped_sum')
    assert_gpu_and_cpu_are_equal_collect(do_it)


# Decimal SUM: Spark's ArrayAggregate requires merge.dataType == zero.dataType exactly. For
# DECIMAL(10,2) + DECIMAL(10,2) the result is DECIMAL(11,2), which fails analysis against a
# DECIMAL(10,2) zero. The working pattern is to widen zero to DECIMAL(38,2) (Spark's cap)
# and Cast the element to match, so `acc + Cast(x, DECIMAL(38,2))` stays at DECIMAL(38,2).
@disable_ansi_mode
def test_array_aggregate_decimal_sum():
    decimal_gen = DecimalGen(precision=10, scale=2)
    def do_it(spark):
        return unary_op_df(spark, ArrayGen(decimal_gen, max_length=8)).selectExpr(
            'aggregate(a, CAST(0 as DECIMAL(38,2)), '
            '(acc, x) -> acc + CAST(x as DECIMAL(38,2))) as dec_sum')
    assert_gpu_and_cpu_are_equal_collect(do_it)
