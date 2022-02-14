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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_gpu_and_cpu_error
from data_gen import *
from marks import incompat, approximate_float
from pyspark.sql.types import *
import pyspark.sql.functions as f

@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_and(data_gen, ansi_enabled):
    ansi_conf = {'spark.sql.ansi.enabled': ansi_enabled}
    data_type = data_gen.data_type
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') & f.lit(True),
                f.lit(False) & f.col('b'),
                f.lit(None).cast(data_type) & f.col('a'),
                f.col('b') & f.lit(None).cast(data_type),
                f.col('a') & f.col('b')),
                conf=ansi_conf)

@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_or(data_gen, ansi_enabled):
    data_type = data_gen.data_type
    ansi_conf = {'spark.sql.ansi.enabled': ansi_enabled}
    assert_gpu_and_cpu_are_equal_collect(
            lambda spark : binary_op_df(spark, data_gen).select(
                f.col('a') | f.lit(True),
                f.lit(False) | f.col('b'),
                f.lit(None).cast(data_type) | f.col('a'),
                f.col('b') | f.lit(None).cast(data_type),
                f.col('a') | f.col('b')),
                conf=ansi_conf)

@pytest.mark.parametrize('data_gen', boolean_gens, ids=idfn)
def test_not(data_gen):
    assert_gpu_and_cpu_are_equal_collect(
                lambda spark : unary_op_df(spark, data_gen).selectExpr('!a'))

# AND/OR on the CPU in Spark will process the LHS unconditionally. But they will only
# process the RHS if they cannot figure out the result from just the LHS.
# Tests the GPU short-circuits the predicates without throwing Exception in ANSI mode.
@pytest.mark.parametrize('logic_op', ['AND'])
@pytest.mark.parametrize('ansi_enabled', ['true', 'false'])
@pytest.mark.parametrize('lhs_predicate', [True, False])
def test_logical_with_side_effect(ansi_enabled, logic_op, lhs_predicate):
    def do_it(spark, lhs_operand, op):
        return spark.createDataFrame(
            [(lhs_operand, INT_MAX), (True, 1), (True, -5)],
            ['a', 'b']
        ).selectExpr('a {} (CAST(b as INT) + 2) > 0'.format(op))
    ansi_conf = {'spark.sql.ansi.enabled': ansi_enabled}
    bypass_op_map = {'AND': False, 'OR': True}
    expect_error = ansi_enabled == 'true' and bypass_op_map[logic_op] != lhs_predicate
    if ansi_enabled == 'true' and expect_error:
        assert_gpu_and_cpu_error(
            df_fun=lambda spark: do_it(spark, lhs_predicate, logic_op).collect(),
            conf=ansi_conf,
            error_message="java.lang.ArithmeticException")
    else:
        assert_gpu_and_cpu_are_equal_collect(
            func=lambda spark: do_it(spark, lhs_predicate, logic_op),
            conf=ansi_conf)