# Copyright (c) 2025, NVIDIA CORPORATION.
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

from asserts import assert_gpu_and_cpu_are_equal_collect, assert_cpu_and_gpu_are_equal_collect_with_capture, assert_gpu_fallback_collect
from marks import allow_non_gpu
from data_gen import *
from marks import ignore_order
from spark_session import is_before_spark_330, with_cpu_session, with_gpu_session


# Helper function to create config that forces specific expressions to CPU bridge
def create_cpu_bridge_fallback_conf(disabled_gpu_expressions, codegen_enabled=True,
                                    disallowed_bridge_expressions=[]):
    """Create config that enables CPU bridge and disables specific GPU expressions"""
    conf = {
        'spark.rapids.sql.expression.cpuBridge.enabled': True,
        'spark.rapids.sql.expression.cpuBridge.codegenEnabled': codegen_enabled
    }
    # Disable specific GPU expressions to force CPU bridge fallback
    for expr_name in disabled_gpu_expressions:
        conf[f'spark.rapids.sql.expression.{expr_name}'] = False

    if disallowed_bridge_expressions:
        conf['spark.rapids.sql.expression.cpuBridge.disallowList'] = ','.join(disallowed_bridge_expressions)
    return conf

@pytest.mark.parametrize('codegen_enabled', [True, False], ids=['codegen_on', 'codegen_off'])
def test_cpu_bridge_add_fallback(codegen_enabled):
    """Test CPU bridge when Add expression is forced to fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1024)
        # This Add will be forced to use CPU bridge due to config
        return df.selectExpr("a", "b", "a + a as s1", "b * b as p1")
    
    # Force Add to fall back to CPU bridge
    conf = create_cpu_bridge_fallback_conf(['Add'], codegen_enabled=codegen_enabled)
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@pytest.mark.parametrize('codegen_enabled', [True, False], ids=['codegen_on', 'codegen_off'])
def test_cpu_bridge_multiply_fallback(codegen_enabled):
    """Test CPU bridge when Multiply expression is forced to fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', double_gen)], length=1024)
        # This Multiply will be forced to use CPU bridge due to config
        return df.selectExpr("a", "b", "a + a as s1", "b * b as p1")
    
    # Force Multiply to fall back to CPU bridge
    conf = create_cpu_bridge_fallback_conf(['Multiply'], codegen_enabled=codegen_enabled)
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@pytest.mark.parametrize('codegen_enabled', [True, False], ids=['codegen_on', 'codegen_off'])
def test_cpu_bridge_complex_expression_tree(codegen_enabled):
    """Test CPU bridge with complex expression trees containing multiple fallbacks"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen), ('c', int_gen)], length=1000)
        return df.selectExpr(
            "a", "b", "c",
            # Complex expression mixing CPU bridge (Add) and GPU (Multiply) operations
            "a + (b * c) as mixed",
            "case when a > 0 then a + b + 5 else a + c + 2 end as conditional"
        )
    
    # Force Add to CPU bridge, keep other expressions on GPU
    conf = create_cpu_bridge_fallback_conf(['Add'], codegen_enabled=codegen_enabled)
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)


@pytest.mark.parametrize('codegen_enabled', [True, False], ids=['codegen_on', 'codegen_off'])
def test_cpu_bridge_higher_order_function_fallback(codegen_enabled):
    """Test CPU bridge with higher order functions where inner expressions fall back to CPU"""
    def test_func(spark):
        df = gen_df(spark, [('arr', ArrayGen(int_gen, min_length=3, max_length=5))], length=1000)
        return df.selectExpr(
            "arr",
            # transform where the lambda contains Add (forced to CPU bridge)
            "transform(arr, x -> x + 1) as arr_plus_one",
            # filter where the lambda contains Add (forced to CPU bridge) 
            "filter(arr, x -> x + 2 > 5) as filtered_arr",
            # exists where the lambda contains Add (forced to CPU bridge)
            "exists(arr, x -> (x + 3) > 10) as has_large_element",
            "transform(arr, x -> (x + 2) * 3) as transformed"
        )
    
    # Force Add to CPU bridge - this will affect expressions inside the lambda functions
    conf = create_cpu_bridge_fallback_conf(['Add'], codegen_enabled=codegen_enabled)
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

def test_cpu_bridge_nondeterministic_works_next_to_bridge():
    """Test mixed scenario: some expressions use CPU bridge, others stay on GPU"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1000)
        # Add should use CPU bridge, rand(42) should stay on GPU - this should work fine
        return df.selectExpr("a", "b", "a + b as sum", "rand(42) * 100 as scaled_random")
    
    # Force Add to CPU bridge, rand() stays on GPU - should work with mixed execution
    conf = create_cpu_bridge_fallback_conf(['Add'])
    
    # This should succeed with mixed GPU/CPU bridge execution, not fall back entirely
    assert_gpu_and_cpu_are_equal_collect(test_func, conf=conf)

# ==============================================================================
# NEGATIVE TEST CASES - Verify expressions that should NOT use CPU bridge
# These should cause full CPU fallback instead of using the bridge
# ==============================================================================

@allow_non_gpu('ProjectExec')
def test_cpu_bridge_rand_disabled_fallback():
    """Test that when rand() is disabled via config, ProjectExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', int_gen)], length=1000)
        # rand(42) with seed for deterministic results - disabled via config should cause full CPU fallback
        return df.selectExpr("a", "b", "a + b as sum", "rand(42) as random_val")
    
    # Enable CPU bridge but disable rand() - should cause full ProjectExec fallback
    conf = create_cpu_bridge_fallback_conf(['Rand'])
    
    # Verify that ProjectExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'ProjectExec', conf=conf)


@allow_non_gpu('HashAggregateExec', 'ShuffleExchangeExec')
@ignore_order(local=True)
def test_cpu_bridge_aggregation_sum_disabled_fallback():
    """Test that when sum() is disabled via config, HashAggregateExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', string_gen)], length=2000)
        # sum() disabled via config should cause full HashAggregateExec fallback, not bridge
        return df.groupBy('b').agg(f.sum('a').alias('total'))
    
    # Enable CPU bridge but disable sum() - should cause full HashAggregateExec fallback
    conf = create_cpu_bridge_fallback_conf(['Sum'])
    
    # Verify that HashAggregateExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'HashAggregateExec', conf=conf)


@allow_non_gpu('WindowExec')
def test_cpu_bridge_window_lag_disabled_fallback():
    """Test that when lag() is disabled via config, WindowExec falls back to CPU entirely"""
    def test_func(spark):
        df = gen_df(spark, [('a', int_gen), ('b', string_gen)], length=1000)
        # lag() disabled via config should cause full WindowExec fallback, not bridge
        return df.selectExpr(
            "a", "b",
            "lag(a, 1) over (partition by b order by a) as prev_a",
            "row_number() over (partition by b order by a) as row_num"
        )
    
    # Enable CPU bridge but disable lag() - should cause full WindowExec fallback
    conf = create_cpu_bridge_fallback_conf(['Lag'])
    
    # Verify that WindowExec falls back to CPU (doesn't use GPU or bridge)
    assert_gpu_fallback_collect(test_func, 'WindowExec', conf=conf)

@allow_non_gpu('ProjectExec')
def test_disallowed_bridge_fallback():
    """Test that when an expression is not on the GPU and is disallowed to use
    the cpu_bridge that it is honored"""
    conf = create_cpu_bridge_fallback_conf(['Add'],
            disallowed_bridge_expressions=['org.apache.spark.sql.catalyst.expressions.Add'])
    assert_gpu_fallback_collect(lambda spark: binary_op_df(spark, byte_gen).selectExpr("a + b"),
                                'ProjectExec', conf=conf)