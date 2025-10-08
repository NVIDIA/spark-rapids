/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests

import com.nvidia.spark.rapids.AssertUtils.assertInTests

/**
 * Helper class to test assertInTests detection from Python integration tests.
 * This class is called from Python/pytest to verify that assertions are
 * properly detected and executed when running under pytest.
 */
object AssertInTestsHelper {
  
  /**
   * Tests that assertInTests properly detects the pytest environment.
   * This method will throw an AssertionError if the condition is false
   * and assertions are enabled (which they should be in tests).
   * 
   * @param condition The condition to assert
   * @param message The assertion message
   * @return true if the assertion passed, false if it was skipped (shouldn't happen in tests)
   */
  def testAssertion(condition: Boolean, message: String): Boolean = {
    var assertionExecuted = false
    
    // Use a by-name parameter to detect if the assertion body runs
    assertInTests({
      assertionExecuted = true
      condition
    }, message)
    
    assertionExecuted
  }
  
  /**
   * Tests the iterator pattern with assertInTests.
   * Returns the number of elements remaining in the iterator.
   * The assertion should check hasNext but not consume elements.
   */
  def testIteratorPattern(elements: Array[Int]): Int = {
    val iter = elements.iterator
    
    // This should check hasNext in test mode but not consume the iterator
    assertInTests(iter.hasNext, "Iterator should have elements")
    
    // Count remaining elements
    iter.size
  }
  
  /**
   * Tests an expensive operation pattern.
   * Returns the number of times the expensive operation was called.
   */
  def testExpensiveOperation(): Int = {
    var callCount = 0
    
    def expensiveOp(): Boolean = {
      callCount += 1
      // Simulate expensive work
      Thread.sleep(1)
      true
    }
    
    // This should call the expensive operation in test mode
    assertInTests(expensiveOp(), "Expensive operation should succeed")
    
    callCount
  }
  
  /**
   * Returns diagnostic information about test environment detection.
   */
  def getTestEnvironmentInfo(): java.util.Map[String, String] = {
    val info = new java.util.HashMap[String, String]()
    
    // Primary detection method for integration tests
    info.put("com.nvidia.spark.rapids.runningTests", 
      String.valueOf(System.getProperty("com.nvidia.spark.rapids.runningTests")))
    
    // Other system properties
    info.put("spark.testing", String.valueOf(System.getProperty("spark.testing")))
    info.put("spark.test.home", String.valueOf(sys.props.get("spark.test.home")))
    info.put("surefire.test.class.path", 
      if (sys.props.contains("surefire.test.class.path")) "SET" else "NOT SET")
    info.put("maven.home", String.valueOf(sys.props.get("maven.home")))
    info.put("bloop.owner", String.valueOf(sys.props.get("bloop.owner")))
    
    // Classpath analysis
    val cp = sys.props.get("java.class.path").getOrElse("")
    info.put("classpath.contains.test-classes", String.valueOf(cp.contains("test-classes")))
    info.put("classpath.contains.scalatest", String.valueOf(cp.contains("scalatest")))
    info.put("classpath.contains.bloop", String.valueOf(cp.contains("bloop")))
    
    // Stack trace
    val stackTrace = Thread.currentThread().getStackTrace
    val hasTestFramework = stackTrace.exists { element =>
      val className = element.getClassName
      className.contains("scalatest") ||
      className.contains("junit") ||
      className.contains("bloop")
    }
    info.put("stack.contains.test.framework", String.valueOf(hasTestFramework))
    
    info
  }
}

