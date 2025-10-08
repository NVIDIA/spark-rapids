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

"""
Integration tests for assertInTests utility from Python/pytest.

This test suite verifies that the assertInTests utility correctly detects
when running under pytest and executes assertions appropriately.
"""

import os
import pytest
from pyspark.sql import SparkSession
from spark_session import with_gpu_session, with_cpu_session


def test_assertInTests_detects_test_environment():
    """
    Test that assertInTests can detect it's running in the integration test environment.
    This works because run_pyspark_from_build.sh sets com.nvidia.spark.rapids.runningTests=true
    """
    def _test_detection(spark):
        # Get the Scala helper class
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Test that a true assertion executes and passes
        assert_executed = helper.testAssertion(True, "This assertion should execute in tests")
        assert assert_executed, "Assertion body should have been executed in test environment"
        
        return True
    
    # Run on GPU
    with_gpu_session(_test_detection)
    
    # Run on CPU too (to verify it works regardless of GPU/CPU mode)
    with_cpu_session(_test_detection)


def test_assertInTests_catches_false_conditions():
    """
    Test that assertInTests throws AssertionError when condition is false.
    """
    def _test_false_condition(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # This should throw an AssertionError
        try:
            helper.testAssertion(False, "This should fail")
            # If we get here, the assertion didn't fire (bad!)
            return False
        except Exception as e:
            # Check it's an assertion error
            assert "AssertionError" in str(type(e)) or "assertion failed" in str(e).lower(), \
                f"Expected AssertionError but got: {type(e)} - {e}"
            return True
    
    result = with_gpu_session(_test_false_condition)
    assert result, "AssertionError should have been thrown for false condition"


def test_assertInTests_iterator_pattern():
    """
    Test that assertInTests works with the iterator pattern (side effects).
    """
    def _test_iterator(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Create a test array
        test_array = [1, 2, 3, 4, 5]
        
        # Convert Python list to Java array
        java_array = spark.sparkContext._gateway.new_array(
            spark.sparkContext._jvm.int, len(test_array))
        for i, val in enumerate(test_array):
            java_array[i] = val
        
        # Call the helper which uses assertInTests with iterator.hasNext
        remaining = helper.testIteratorPattern(java_array)
        
        # All elements should still be in the iterator
        # (assertInTests should check hasNext but not consume elements)
        assert remaining == len(test_array), \
            f"Expected {len(test_array)} elements but got {remaining}"
        
        return True
    
    with_gpu_session(_test_iterator)


def test_assertInTests_expensive_operation():
    """
    Test that assertInTests executes expensive operations in test mode.
    """
    def _test_expensive_op(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Call the expensive operation test
        call_count = helper.testExpensiveOperation()
        
        # The expensive operation should have been called in test mode
        assert call_count == 1, \
            f"Expensive operation should have been called once, but was called {call_count} times"
        
        return True
    
    with_gpu_session(_test_expensive_op)


def test_print_environment_diagnostics():
    """
    Print diagnostic information about test environment detection.
    This helps debug detection issues across different environments.
    """
    def _print_diagnostics(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Get diagnostic info from JVM
        info = helper.getTestEnvironmentInfo()
        
        print("\n" + "="*70)
        print("AssertInTests Detection Diagnostics")
        print("="*70)
        
        print("\nJVM System Properties (Primary Detection Methods):")
        for key in ['com.nvidia.spark.rapids.runningTests', 'spark.testing', 
                    'spark.test.home', 'surefire.test.class.path', 
                    'maven.home', 'bloop.owner']:
            if key in info:
                print(f"  {key} = {info[key]}")
        
        print("\nClasspath Analysis:")
        for key in ['classpath.contains.test-classes', 'classpath.contains.scalatest',
                    'classpath.contains.bloop']:
            if key in info:
                print(f"  {key} = {info[key]}")
        
        print("\nStack Trace Analysis:")
        if 'stack.contains.test.framework' in info:
            print(f"  Contains test framework = {info['stack.contains.test.framework']}")
        
        print("\n" + "="*70)
        print("Key Detection: com.nvidia.spark.rapids.runningTests system property")
        print("="*70 + "\n")
        
        return True
    
    with_gpu_session(_print_diagnostics)


def test_assertInTests_works_on_driver():
    """
    Test that assertInTests works on the driver (simplified from executor test).
    Testing on executors is complex due to Spark serialization constraints.
    """
    def _test_on_driver(spark):
        helper = spark.sparkContext._jvm.com.nvidia.spark.rapids.tests.AssertInTestsHelper
        
        # Test assertion execution
        assert_executed = helper.testAssertion(True, "Assertion on driver")
        
        # Verify assertion ran
        assert assert_executed, "Assertion should have executed on driver"
        
        return True
    
    with_gpu_session(_test_on_driver)

