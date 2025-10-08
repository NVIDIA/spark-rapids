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

/**
 * Utilities for assertions that should only run during tests.
 * 
 * This helps avoid performance overhead and side effects from directly using the
 * `assert` function in scala, since it is not elided in the production code by default.
 */
object AssertUtils {

  /**
   * Determines if we are currently running in a test environment.
   * This checks for common test framework indicators, short-circuiting on first match.
   * Checks are ordered from most likely / cheapest to least likely / most expensive.
   */
  private lazy val isTestEnvironment: Boolean = {
    // Most common: Integration test marker (set by integration_tests/run_pyspark_from_build.sh)
    // This is explicitly passed to both driver and executor JVMs
    def isRapidsTest: Boolean = 
      System.getProperty("com.nvidia.spark.rapids.runningTests") == "true"
    
    // Maven Surefire (unit tests)
    def isMavenTest: Boolean = 
      sys.props.contains("surefire.test.class.path") ||
      sys.props.contains("maven.home")
    
    // Spark test properties
    def isSparkTest: Boolean = 
      System.getProperty("spark.testing") == "1" ||
      sys.props.contains("spark.test.home")

    // Bloop-specific properties (for IDE testing)
    def isBloop: Boolean = sys.props.contains("bloop.owner")
    
    // IDE/Bloop builds: Check classpath (moderately cheap)
    def hasTestClasspath: Boolean = sys.props.get("java.class.path").exists { cp =>
      cp.contains("test-classes") ||  // Maven/Gradle test output
      cp.contains("scalatest") ||      // ScalaTest on classpath
      cp.contains("junit") ||          // JUnit on classpath
      cp.contains("bloop")             // Bloop in classpath
    }
    
    // Most expensive: Stack trace inspection (only if all else fails)
    def hasTestRunner: Boolean = try {
      val stackTrace = Thread.currentThread().getStackTrace
      stackTrace.exists { element =>
        val className = element.getClassName
        className.contains("org.scalatest") ||
        className.contains("org.junit") ||
        className.contains("surefire") ||
        className.contains("bloop") ||        // Bloop test runner
        className.contains("metals") ||       // Metals extension
        className.contains("Suite")
      }
    } catch {
      case _: Exception => false
    }
    
    // Short-circuit on first true: most likely checks first
    // isRapidsTest catches Python integration tests
    isRapidsTest || isMavenTest || isSparkTest || isBloop || hasTestClasspath || hasTestRunner
  }

  /**
   * Asserts a condition only when running in a test environment.
   * 
   * This is useful for assertions that:
   * - Have side effects (e.g., calling iterator.hasNext)
   * - Are expensive (e.g., query.resolved, plan canonicalization)
   * - Should not impact production performance
   * 
   * The condition parameter is by-name (=> Boolean), so it is only evaluated
   * if we are actually in a test environment.
   * 
   * @param condition The condition to assert (not evaluated in production)
   * @param message Optional message to display on assertion failure
   * 
   * Example usage:
   * {{{
   *   // Instead of:
   *   assert(!buildIter.hasNext, "build side should have a single batch")
   *   
   *   // Use:
   *   assertInTests(!buildIter.hasNext, "build side should have a single batch")
   * }}}
   */
  def assertInTests(condition: => Boolean, message: => String = ""): Unit = {
    if (isTestEnvironment) {
      if (!condition) {
        if (message.nonEmpty) {
          throw new AssertionError(s"assertion failed: $message")
        } else {
          throw new AssertionError("assertion failed")
        }
      }
    }
  }

  /**
   * Alternative form that takes only a condition without a message.
   */
  def assertInTests(condition: => Boolean): Unit = {
    assertInTests(condition, "")
  }
}

