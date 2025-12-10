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
 * When enabled via spark.rapids.sql.test.retryCoverageTracking.enabled, this tracker
 * will check every memory allocation's call stack for retry-related methods. If an
 * allocation happens without any retry method in the call stack, it logs the allocation
 * info (kind and call stack) to a CSV file.
 * 
 * See: https://github.com/NVIDIA/spark-rapids/issues/13672
 */
object AllocationRetryCoverageTracker extends Logging {
  import AllocationKind._

  // Retry-related method names to look for in the call stack
  // These methods indicate the allocation is properly covered by retry logic
  private val RETRY_METHOD_NAMES = Set(
    "withRetry",
    "withRetryNoSplit",
    "withRestoreOnRetry"
  )

  // Classes that contain retry methods
  private val RETRY_CLASS_PREFIXES = Seq(
    "com.nvidia.spark.rapids.RmmRapidsRetryIterator",
    "com.nvidia.spark.rapids.RmmRapidsRetryIterator$"
  )

  // Track whether tracking is enabled (set on first use from config)
  @volatile private var enabled: Boolean = false
  @volatile private var outputPath: String = "uncovered_allocations.csv"
  @volatile private var initialized: Boolean = false

  // Track unique call stacks we've already logged to avoid duplicates
  private val loggedStacks = ConcurrentHashMap.newKeySet[String]()

  // Lock for thread-safe file writing
  private val writeLock = new Object()

  /**
   * Initialize the tracker with configuration.
   * Should be called during plugin initialization.
   */
  def initialize(conf: RapidsConf): Unit = synchronized {
    if (!initialized) {
      enabled = conf.isRetryCoverageTrackingEnabled
      outputPath = conf.retryCoverageTrackingOutputPath
      initialized = true
      if (enabled) {
        logInfo(s"Retry coverage tracking enabled. Uncovered allocations will be logged to: " +
          s"$outputPath")
        // Write CSV header
        writeToFile("kind,call_stack", append = false)
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
    if (!enabled) {
      return
    }

    val stackTrace = Thread.currentThread().getStackTrace
    
    // Check if any retry method is in the call stack
    val hasCoverage = stackTrace.exists { element =>
      val className = element.getClassName
      val methodName = element.getMethodName
      
      // Check if this is a retry-related method in the retry iterator classes
      RETRY_CLASS_PREFIXES.exists(prefix => className.startsWith(prefix)) &&
        RETRY_METHOD_NAMES.contains(methodName)
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
          logWarning(s"Uncovered $kind memory allocation detected. Call stack: $relevantStack")
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
   * Write a line to the output CSV file.
   */
  private def writeToFile(line: String, append: Boolean): Unit = {
    writeLock.synchronized {
      var writer: PrintWriter = null
      try {
        writer = new PrintWriter(new BufferedWriter(new FileWriter(outputPath, append)))
        writer.println(line)
      } catch {
        case e: Exception =>
          logError(s"Failed to write to retry coverage tracking file: $outputPath", e)
      } finally {
        if (writer != null) {
          writer.close()
        }
      }
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
  def isEnabled: Boolean = enabled

  /**
   * Reset the tracker (useful for testing).
   */
  def reset(): Unit = synchronized {
    enabled = false
    outputPath = "uncovered_allocations.csv"
    initialized = false
    loggedStacks.clear()
  }
}

