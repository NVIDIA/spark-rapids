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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.AssertUtils.assertInTests
import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite to verify that AssertUtils correctly detects test environments
 * across different test runners (Maven, Bloop, IDEs, etc.)
 */
class AssertUtilsVerificationSuite extends AnyFunSuite {

  test("assertInTests should execute in test environment") {
    // This should NOT throw an assertion error when running as a test
    var executed = false
    assertInTests({
      executed = true
      true
    }, "This assertion should execute in test mode")
    
    // Verify the assertion body was actually executed
    assert(executed, "assertInTests body should have been executed in test environment")
  }

  test("assertInTests should catch false conditions") {
    // This SHOULD throw an assertion error
    assertThrows[AssertionError] {
      assertInTests(false, "This should throw")
    }
  }

  test("assertInTests with message should include message in error") {
    val caught = intercept[AssertionError] {
      assertInTests(false, "custom error message")
    }
    assert(caught.getMessage.contains("custom error message"))
  }

  test("print diagnostic information about test environment detection") {
    // Print information about which detection methods fired
    println("\n=== AssertUtils Test Environment Detection Diagnostics ===")
    
    // Check system properties
    println("\nSystem Properties:")
    println(s"  spark.testing = ${System.getProperty("spark.testing")}")
    println(s"  spark.test.home = ${sys.props.get("spark.test.home")}")
    println("  surefire.test.class.path = " + 
      s"${sys.props.get("surefire.test.class.path").map(_ => "SET").getOrElse("NOT SET")}")
    println(s"  maven.home = ${sys.props.get("maven.home")}")
    println(s"  bloop.owner = ${sys.props.get("bloop.owner")}")
    
    // Check environment variables
    println("\nEnvironment Variables:")
    println(s"  PYTEST_CURRENT_TEST = ${sys.env.get("PYTEST_CURRENT_TEST")}")
    
    // Check classpath
    println("\nClasspath Analysis:")
    val cp = sys.props.get("java.class.path").getOrElse("")
    println(s"  Contains 'bloop': ${cp.contains("bloop")}")
    println(s"  Contains 'test-classes': ${cp.contains("test-classes")}")
    println(s"  Contains 'scalatest': ${cp.contains("scalatest")}")
    println(s"  Contains 'junit': ${cp.contains("junit")}")
    
    // Check stack trace
    println("\nStack Trace Analysis:")
    val stackTrace = Thread.currentThread().getStackTrace
    val testFrameworkClasses = stackTrace.filter { element =>
      val className = element.getClassName
      className.contains("scalatest") ||
      className.contains("junit") ||
      className.contains("surefire") ||
      className.contains("bloop") ||
      className.contains("metals") ||
      className.contains("Suite")
    }
    
    if (testFrameworkClasses.isEmpty) {
      println("  No test framework classes found in stack trace")
    } else {
      println("  Test framework classes in stack trace:")
      testFrameworkClasses.take(5).foreach { element =>
        println(s"    ${element.getClassName}.${element.getMethodName}")
      }
    }
    
    println("\n=== End Diagnostics ===\n")
  }

  test("verify iterator assertion pattern works") {
    // Simulate the problematic iterator pattern
    val iter = Iterator(1, 2, 3)
    
    // This should NOT consume the iterator in production,
    // but SHOULD verify it has elements in test mode
    assertInTests(iter.hasNext, "Iterator should have elements")
    
    // Iterator should still have all elements
    assert(iter.next() == 1)
    assert(iter.next() == 2)
    assert(iter.next() == 3)
  }

  test("verify expensive operation pattern works") {
    var expensiveOpCount = 0
    
    def expensiveOperation(): Boolean = {
      expensiveOpCount += 1
      true
    }
    
    // This should call the expensive operation in tests
    assertInTests(expensiveOperation())
    
    // Verify it was called
    assert(expensiveOpCount == 1, "Expensive operation should be called in test mode")
  }

  test("verify collection traversal pattern works") {
    val largeCollection = (1 to 1000).toSeq
    
    // This should traverse the collection in tests
    assertInTests(largeCollection.forall(_ > 0), "All elements should be positive")
    
    // No exception thrown means it worked
  }
}

