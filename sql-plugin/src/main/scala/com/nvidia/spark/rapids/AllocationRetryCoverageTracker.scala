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

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging

/**
 * Memory allocation kind for retry coverage tracking.
 */
object AllocationKind extends Enumeration {
  type AllocationKind = Value
  val HOST, DEVICE = Value
}

/**
 * Utility object to track memory allocations that are not covered by retry methods.
 * 
 * This is a debugging tool to help ensure all memory allocation code paths are
 * protected by withRetry/withRetryNoSplit blocks, as required by the retry-OOM
 * handling mechanism in spark-rapids.
 * 
 * Tracking support:
 * - HOST memory: via HostAlloc hooks
 * - DEVICE memory: via RMM debug mode (onAllocated callback)
 * 
 * This feature is DISABLED by default. To enable, set environment variable:
 *   export SPARK_RAPIDS_RETRY_COVERAGE_TRACKING=true
 * 
 * For integration tests:
 *   SPARK_RAPIDS_RETRY_COVERAGE_TRACKING=true ./integration_tests/run_pyspark_from_build.sh
 * 
 * When enabled:
 * - Checks memory allocations' call stacks for retry-related methods
 * - Logs uncovered allocations to /tmp/uncovered_allocations.csv
 * - Also logs warnings via Spark logging
 * 
 * Note: Enabling this feature turns on RMM debug mode which may impact performance.
 * Only use for debugging/testing purposes.
 * 
 * See: https://github.com/NVIDIA/spark-rapids/issues/13672
 */
object AllocationRetryCoverageTracker extends Logging {
  import AllocationKind._

  // Package prefixes for spark-rapids code where we look for "Retry" in method names
  private val SPARK_RAPIDS_PACKAGE_PREFIXES = Seq(
    "com.nvidia.spark.rapids",
    "org.apache.spark.sql.rapids"
  )

  // Default output file path for uncovered allocations - use /tmp for easy access
  private val DEFAULT_OUTPUT_PATH = "/tmp/uncovered_allocations.csv"

  // Environment variable to enable tracking
  private val ENV_VAR_NAME = "SPARK_RAPIDS_RETRY_COVERAGE_TRACKING"

  // Check environment variable - this works reliably across all processes
  val ENABLED: Boolean = {
    val envValue = System.getenv(ENV_VAR_NAME)
    envValue != null && envValue.equalsIgnoreCase("true")
  }

  @volatile private var headerWritten: Boolean = false

  // Track unique call stacks we've already logged to avoid duplicates
  private val loggedStacks = ConcurrentHashMap.newKeySet[String]()

  // Lock for thread-safe file writing
  private val writeLock = new Object()

  /**
   * Ensure header is written (thread-safe, lazy initialization).
   */
  private def ensureHeaderWritten(): Unit = {
    if (!headerWritten) {
      writeLock.synchronized {
        if (!headerWritten) {
          logWarning(s"Retry coverage tracking ACTIVE. Output: $DEFAULT_OUTPUT_PATH")
          writeToFileInternal("kind,call_stack", append = false)
          headerWritten = true
        }
      }
    }
  }

  /**
   * Check if a memory allocation is covered by a retry method.
   * If not covered and tracking is enabled, log to the output file.
   * 
   * @param kind The kind of memory allocation (HOST or DEVICE)
   */
  def checkAllocation(kind: AllocationKind): Unit = {
    if (!ENABLED) {
      return
    }

    // Ensure header is written on first check
    ensureHeaderWritten()

    val stackTrace = Thread.currentThread().getStackTrace
    
    // Check if any retry-related code is in the call stack.
    // We look for any class or method containing "Retry" in its name within spark-rapids packages.
    // This catches:
    // - Core retry methods: withRetry, withRetryNoSplit, withRestoreOnRetry
    // - Wrapper functions: concatenateAndMergeWithRetry, projectAndCloseWithRetrySingleBatch, etc.
    // - Retry framework internals: RmmRapidsRetryIterator$AutoCloseableAttemptSpliterator.next
    // Note: We exclude AllocationRetryCoverageTracker itself since it's always in the stack
    val hasCoverage = stackTrace.exists { element =>
      val className = element.getClassName
      val methodName = element.getMethodName
      SPARK_RAPIDS_PACKAGE_PREFIXES.exists(className.startsWith) && 
        !className.contains("AllocationRetryCoverageTracker") &&
        (className.contains("Retry") || methodName.contains("Retry"))
    }

    if (!hasCoverage) {
      // Filter to only spark-rapids related frames for cleaner output
      val relevantStack = stackTrace
        .filter { element =>
          val className = element.getClassName
          className.startsWith("com.nvidia.spark.rapids") ||
            className.startsWith("org.apache.spark.sql.rapids") ||
            className.startsWith("ai.rapids.cudf")
        }
        .map(_.toString)
        .mkString(" -> ")

      // Use the filtered stack as the key to avoid duplicate logging
      val stackKey = s"$kind:$relevantStack"
      
      // Only log if we haven't seen this exact stack before
      if (!loggedStacks.contains(stackKey)) {
        if (loggedStacks.add(stackKey)) {
          // Escape the stack trace for CSV (replace quotes and wrap in quotes)
          val escapedStack = "\"" + relevantStack.replace("\"", "\"\"") + "\""
          writeToFile(s"$kind,$escapedStack", append = true)
          logWarning(s"Uncovered $kind allocation #${loggedStacks.size()}. Stack: $relevantStack")
        }
      }
    }
  }

  /**
   * Check host memory allocation coverage.
   */
  def checkHostAllocation(): Unit = {
    checkAllocation(HOST)
  }

  /**
   * Check device (GPU) memory allocation coverage.
   */
  def checkDeviceAllocation(): Unit = {
    checkAllocation(DEVICE)
  }

  /**
   * Write a line to the output CSV file (internal, assumes lock is NOT held).
   */
  private def writeToFileInternal(line: String, append: Boolean): Unit = {
    var writer: PrintWriter = null
    try {
      writer = new PrintWriter(new BufferedWriter(new FileWriter(DEFAULT_OUTPUT_PATH, append)))
      writer.println(line)
    } catch {
      case e: Exception =>
        logError(s"Failed to write to retry coverage tracking file: $DEFAULT_OUTPUT_PATH", e)
    } finally {
      if (writer != null) {
        writer.close()
      }
    }
  }

  /**
   * Write a line to the output CSV file (thread-safe).
   */
  private def writeToFile(line: String, append: Boolean): Unit = {
    writeLock.synchronized {
      writeToFileInternal(line, append)
    }
  }

  /**
   * Get the number of unique uncovered allocation patterns logged.
   */
  def getUncoveredCount: Int = loggedStacks.size()

  /**
   * Clear the logged stacks (useful for testing).
   */
  def clearLoggedStacks(): Unit = {
    loggedStacks.clear()
  }

  /**
   * Check if tracking is currently enabled.
   */
  def isEnabled: Boolean = ENABLED

  /**
   * Reset the tracker state (useful for testing).
   * Note: Cannot change ENABLED as it's read from env var at class load time.
   */
  def reset(): Unit = synchronized {
    headerWritten = false
    loggedStacks.clear()
  }
}

