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

import scala.collection.mutable.ArrayBuffer

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Integration test suite for RAPIDS Shuffle Manager.
 * Tests buffer fallback behavior under different configurations:
 * - Verifies SpillablePartialFileHandle memory-to-disk fallback
 * - Tests different partialFileBufferMaxSize settings
 * - Validates forced file-only mode for finalMergeWriter
 */
class RapidsShuffleIntegrationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  
  /**
   * Capture log messages during operation and check for buffer behavior indicators.
   * 
   * @param operation The operation to execute while capturing logs
   * @return Tuple of (hasExpansion, hasSpill, hasForcedFileOnly)
   *         - hasExpansion: Whether buffer expansion occurred
   *         - hasSpill: Whether buffer spilled to disk
   *         - hasForcedFileOnly: Whether forced file-only mode was used
   */
  private def checkLogsForBufferBehavior(operation: => Unit): 
      (Boolean, Boolean, Boolean) = {
    val logMessages = new ArrayBuffer[String]()
    
    val spillableLogger = org.apache.log4j.Logger.getLogger(
      "com.nvidia.spark.rapids.spill.SpillablePartialFileHandle")
    val writerLogger = org.apache.log4j.Logger.getLogger(
      "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleMapOutputWriter")
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    
    val origSpillableLevel = spillableLogger.getLevel
    val origWriterLevel = writerLogger.getLevel
    
    // Enable DEBUG level to capture "Expanded buffer from" messages
    spillableLogger.setLevel(org.apache.log4j.Level.DEBUG)
    writerLogger.setLevel(org.apache.log4j.Level.DEBUG)
    
    val appender = new org.apache.log4j.AppenderSkeleton {
      override def append(event: org.apache.log4j.spi.LoggingEvent): Unit = {
        logMessages += event.getRenderedMessage
      }
      override def close(): Unit = {}
      override def requiresLayout(): Boolean = false
    }
    
    rootLogger.addAppender(appender)
    try {
      operation
    } finally {
      rootLogger.removeAppender(appender)
      if (origSpillableLevel != null) spillableLogger.setLevel(origSpillableLevel)
      if (origWriterLevel != null) writerLogger.setLevel(origWriterLevel)
    }
    
    val hasExpansion = logMessages.exists(msg => 
      msg.contains("Expanded buffer from"))
    
    val hasSpill = logMessages.exists(msg =>
      msg.contains("Spilled buffer to"))
    
    val hasForcedFileOnly = logMessages.exists(msg =>
      msg.contains("Using forced file-only mode"))
    
    (hasExpansion, hasSpill, hasForcedFileOnly)
  }
  
  /**
   * Create SparkSession with custom partialFileBufferMaxSize.
   * Required because partialFileBufferMaxSize is .startupOnly() config.
   * 
   * @param maxBufferSize The max buffer size (e.g., "1m", "6m")
   */
  private def createSessionWithBufferConfig(maxBufferSize: String): Unit = {
    if (spark != null) {
      spark.stop()
    }
    SparkSession.clearActiveSession()
    
    val shimVersion = ShimLoader.getShimVersion
    val shuffleManagerClass = s"com.nvidia.spark.rapids.spark" +
      s"${shimVersion.toString.replace(".", "")}." +
      "RapidsShuffleManager"
    
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.test.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleDataIO")
      .set("spark.rapids.memory.host.partialFileBufferInitialSize", "1m")
      .set("spark.rapids.memory.host.partialFileBufferMaxSize", maxBufferSize)
      .set("spark.rapids.sql.batchSizeBytes", "5m")
    
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    
    // Clean up any existing session before each test
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  override def afterEach(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterEach()
    }
  }

  test("shuffle with join - 1MB max buffer triggers fallback") {
    // Test verifying buffer fallback with small max buffer size
    // Setup: 1MB max buffer, 1MB initial buffer, 5MB GPU batch size
    // Expected: Serialized data (~4MB) exceeds 1MB limit, triggers spill to disk
    createSessionWithBufferConfig("1m")
    
    val (hasExpansion, hasSpill, hasForcedFileOnly) = checkLogsForBufferBehavior {
      val df1 = spark.range(0, 3000000, 1, 3)
        .selectExpr(
          "id as key",
          "id * 2 as value1",
          "concat('left_', cast(id as string)) as left_str"
        )
      
      val df2 = spark.range(0, 30000000, 1, 3)
        .selectExpr(
          "id as key",
          "id * 3 as value2",
          "concat('right_', cast(id as string)) as right_str"
        )
      
      val result = df1.join(df2, "key")
        .selectExpr("key", "value1", "value2")
        .collect()
      
      assert(result.length == 3000000, "Should have 3M joined rows")
    }
    
    // Verify: buffer cannot expand beyond 1MB, must spill to disk
    assert(!hasExpansion && hasSpill && hasForcedFileOnly,
      s"Expected NO expansion and spill with 1MB max buffer. " +
      s"expansion=$hasExpansion, spill=$hasSpill, forcedFileOnly=$hasForcedFileOnly")
  }

  test("shuffle with join - 6MB max buffer avoids fallback") {
    // Test verifying buffer stays in memory with sufficient max buffer size
    // Setup: 6MB max buffer, 1MB initial buffer, 5MB GPU batch size
    // Expected: Serialized data (~4MB) fits within 6MB limit via buffer expansion
    createSessionWithBufferConfig("6m")
    
    val (hasExpansion, hasSpill, hasForcedFileOnly) = checkLogsForBufferBehavior {
      val df1 = spark.range(0, 3000000, 1, 3)
        .selectExpr(
          "id as key",
          "id * 2 as value1",
          "concat('left_', cast(id as string)) as left_str"
        )
      
      val df2 = spark.range(0, 30000000, 1, 3)
        .selectExpr(
          "id as key",
          "id * 3 as value2",
          "concat('right_', cast(id as string)) as right_str"
        )
      
      val result = df1.join(df2, "key")
        .selectExpr("key", "value1", "value2")
        .collect()
      
      assert(result.length == 3000000, "Should have 3M joined rows")
    }
    
    // Verify: buffer expands to accommodate data, no spill to disk
    assert(hasExpansion && !hasSpill && hasForcedFileOnly,
      s"Expected expansion, NO spill, forced file-only with 6MB max buffer. " +
      s"expansion=$hasExpansion, spill=$hasSpill, forcedFileOnly=$hasForcedFileOnly")
  }
}

