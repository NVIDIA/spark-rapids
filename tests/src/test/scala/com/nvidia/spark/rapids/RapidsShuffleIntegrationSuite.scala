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
 * Tests the complete shuffle pipeline including:
 * - SpillablePartialFileHandle for memory management
 * - RapidsLocalDiskShuffleMapOutputWriter for writing
 * - Multi-batch scenarios
 * - Various shuffle operations (groupBy, join, distinct, etc.)
 */
class RapidsShuffleIntegrationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val testData = ArrayBuffer[String]()
  
  // Helper to capture and check for buffer fallback indicators in logs
  private def checkLogsForBufferBehavior(operation: => Unit): 
      (Boolean, Boolean, Boolean) = {
    // Create a custom log appender to capture log messages
    val logMessages = new ArrayBuffer[String]()
    
    // Get the specific logger for SpillablePartialFileHandle to capture DEBUG logs
    val spillableLogger = org.apache.log4j.Logger.getLogger(
      "com.nvidia.spark.rapids.spill.SpillablePartialFileHandle")
    val writerLogger = org.apache.log4j.Logger.getLogger(
      "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleMapOutputWriter")
    val rootLogger = org.apache.log4j.Logger.getRootLogger
    
    // Save original log levels
    val origSpillableLevel = spillableLogger.getLevel
    val origWriterLevel = writerLogger.getLevel
    
    // Set to DEBUG to capture "Expanded buffer from" messages
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
      // Restore original log levels
      if (origSpillableLevel != null) spillableLogger.setLevel(origSpillableLevel)
      if (origWriterLevel != null) writerLogger.setLevel(origWriterLevel)
    }
    
    // Check for specific log patterns
    val hasExpansion = logMessages.exists(msg => 
      msg.contains("Expanded buffer from"))
    
    val hasSpill = logMessages.exists(msg =>
      msg.contains("Spilled buffer to"))
    
    val hasForcedFileOnly = logMessages.exists(msg =>
      msg.contains("Using forced file-only mode"))
    
    (hasExpansion, hasSpill, hasForcedFileOnly)
  }
  
  // Helper to recreate SparkSession with custom buffer config
  private def recreateSessionWithBufferConfig(maxBufferSize: String): Unit = {
    spark.stop()
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
    
    // Clean up any existing session
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    
    // Get current shim version to construct the correct shuffle manager class name
    val shimVersion = ShimLoader.getShimVersion
    val shuffleManagerClass = s"com.nvidia.spark.rapids.spark" +
      s"${shimVersion.toString.replace(".", "")}." +
      "RapidsShuffleManager"
    
    // Create Spark session with RAPIDS shuffle manager
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("RapidsShuffleIntegrationTest")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.test.enabled", "true")
      .set("spark.shuffle.manager", shuffleManagerClass)
      .set("spark.shuffle.sort.io.plugin.class",
        "org.apache.spark.shuffle.sort.io.RapidsLocalDiskShuffleDataIO")
      // This makes it easy to trigger buffer expansion and memory->file transition
      .set("spark.rapids.memory.host.partialFileBufferInitialSize", "1m")
      .set("spark.rapids.memory.host.partialFileBufferMaxSize", "20m")
      // Use smaller batch sizes to trigger multi-batch scenarios
      .set("spark.rapids.sql.batchSizeBytes", "5m")

    spark = SparkSession.builder().config(conf).getOrCreate()
    
    // Register cleanup
    testData += "session_created"
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
      testData.clear()
      super.afterEach()
    }
  }

  test("basic shuffle with groupBy aggregation - small dataset") {
    // Small dataset to verify basic functionality
    // Each partition should produce multiple batches (batchSize=1MB)
    val df = spark.range(0, 500000, 1, 20)
      .selectExpr(
        "id",
        "id % 100 as key",
        "id * 2 as value",
        "concat('data_', cast(id as string), '_suffix') as str_col"
      )
    
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.sum("value").alias("sum_value"),
        org.apache.spark.sql.functions.count("*").alias("count")
      )
      .collect()
    
    assert(result.length == 100, "Should have 100 groups")
    
    // Verify a sample result
    val group0 = result.find(_.getAs[Long]("key") == 0)
    assert(group0.isDefined, "Should find group 0")
    assert(group0.get.getAs[Long]("count") == 5000, "Group 0 should have 5000 rows")
  }

  test("shuffle with join operation - medium dataset") {
    // Medium sized join: 1M rows PER partition × 3 partitions = 3M rows per side
    // Each partition writes >> 1MB, ensuring multi-batch shuffle per partition
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
    
    // Verify join correctness
    val row0 = result.find(_.getAs[Long]("key") == 0)
    assert(row0.isDefined)
    assert(row0.get.getAs[Long]("value1") == 0)
    assert(row0.get.getAs[Long]("value2") == 0)
    
    val row100 = result.find(_.getAs[Long]("key") == 100)
    assert(row100.isDefined)
    assert(row100.get.getAs[Long]("value1") == 200)
    assert(row100.get.getAs[Long]("value2") == 300)
  }

  test("shuffle with distinct operation") {
    // Create data with duplicates
    val df = spark.range(0, 10000, 1, 10)
      .selectExpr("id % 100 as value")
    
    val result = df.distinct().collect()
    
    assert(result.length == 100, "Should have 100 distinct values")
    
    val values = result.map(_.getAs[Long]("value")).sorted
    val expected = (0L until 100L).toArray
    assert(values.sameElements(expected))
  }

  test("shuffle with sort operation") {
    val df = spark.range(0, 1000, 1, 10)
      .selectExpr("id", "(1000 - id) as reverse_id")
      .repartition(5)
    
    val result = df.orderBy("reverse_id").collect()
    
    assert(result.length == 1000, "Should have 1000 rows")
    
    // Verify sort order
    assert(result.head.getAs[Long]("id") == 999)
    assert(result.last.getAs[Long]("id") == 0)
    
    // Check that all values are in descending order by reverse_id
    val reverseIds = result.map(_.getAs[Long]("reverse_id"))
    assert(reverseIds.sameElements(reverseIds.sorted))
  }

  test("large shuffle to trigger buffer expansion and memory pressure") {
    // Large dataset designed to:
    // 1. Trigger multi-batch per partition (each partition writes >> 1MB)
    // 2. Multiple concurrent writes to create memory pressure
    // 3. Potentially trigger buffer expansion or memory->file switch
    // 1M rows PER partition × 40 partitions = 40M rows total
    val df = spark.range(0, 40000000, 1, 40)
      .selectExpr(
        "id",
        "id % 20 as key",  // Only 20 groups to concentrate data
        "id * 2 as value1",
        "id * 3 as value2",
        "concat('prefix_', cast(id as string), '_middle_', " +
          "cast(id * 2 as string), '_suffix') as large_str1",
        "concat('data_col_', cast(id as string), '_extra_padding_', " +
          "cast(id % 1000 as string)) as large_str2",
        "cast(rand() * 1000 as bigint) as random_value"
      )
    
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.sum("random_value").alias("sum_random"),
        org.apache.spark.sql.functions.count("*").alias("count"),
        org.apache.spark.sql.functions.max("large_str1").alias("max_str")
      )
      .collect()
    
    assert(result.length == 20, "Should have 20 groups")
    
    // Each group should have 2M rows (40M / 20)
    result.foreach { row =>
      assert(row.getAs[Long]("count") == 2000000,
        s"Group ${row.getAs[Long]("key")} should have 2M rows")
    }
  }

  test("complex multi-stage shuffle with large intermediate data") {
    // Multiple shuffle stages with large intermediate data
    // 1M rows PER partition × 30 partitions = 30M rows per dataframe
    // Each partition writes >> 1MB per stage
    val df1 = spark.range(0, 30000000, 1, 30)
      .selectExpr(
        "id",
        "id % 100 as group1",
        "id * 2 as value1",
        "concat('stage1_', cast(id as string)) as str1"
      )
    
    val df2 = spark.range(0, 30000000, 1, 30)
      .selectExpr(
        "id",
        "id % 100 as group2",
        "id * 3 as value2",
        "concat('stage2_', cast(id as string)) as str2"
      )
    
    // Multiple shuffle stages - each should produce significant shuffle data
    val result = df1
      .groupBy("group1")
      .agg(org.apache.spark.sql.functions.sum("value1").alias("sum1"))
      .join(
        df2.groupBy("group2")
          .agg(org.apache.spark.sql.functions.sum("value2").alias("sum2")),
        df1("group1") === df2("group2")
      )
      .collect()
    
    assert(result.length == 100, "Should have 100 joined groups")
  }

  test("shuffle with wide schema and many string columns") {
    // Wide schema with many string columns
    // 1M rows PER partition × 25 partitions = 25M rows total
    // Wide schema ensures each partition writes >> 1MB
    val df = spark.range(0, 25000000, 1, 25)
      .selectExpr(
        "id",
        "id % 50 as key",
        "concat('col1_', cast(id as string), '_padding_data') as str_col1",
        "concat('col2_', cast(id * 2 as string), '_more_padding') as str_col2",
        "concat('col3_', cast(id * 3 as string), '_extra_data') as str_col3",
        "concat('col4_', cast(id % 1000 as string), '_suffix') as str_col4",
        "cast(rand() * 100 as int) as int_value",
        "cast(rand() * 1000 as bigint) as long_value"
      )
    
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("count"),
        org.apache.spark.sql.functions.max("str_col1").alias("max_str"),
        org.apache.spark.sql.functions.avg("int_value").alias("avg_int"),
        org.apache.spark.sql.functions.sum("long_value").alias("sum_long")
      )
      .orderBy("key")
      .collect()
    
    assert(result.length == 50, "Should have 50 groups")
    
    // Verify each group has 500K rows (25M / 50)
    result.foreach { row =>
      assert(row.getAs[Long]("count") == 500000,
        s"Group ${row.getAs[Long]("key")} should have 500K rows")
    }
  }

  test("shuffle with repartition to different partition counts") {
    val df = spark.range(0, 10000, 1, 20)
      .selectExpr("id", "id % 10 as key")
    
    // Repartition down
    val repartitioned = df.repartition(5, df("key"))
    
    val result = repartitioned
      .groupBy("key")
      .count()
      .collect()
    
    assert(result.length == 10, "Should have 10 groups")
    result.foreach { row =>
      assert(row.getAs[Long]("count") == 1000,
        s"Group ${row.getAs[Long]("key")} should have 1000 rows")
    }
  }

  test("verify shuffle manager is RAPIDS") {
    val shuffleManager = spark.sparkContext.getConf
      .get("spark.shuffle.manager", "")
    
    assert(shuffleManager.contains("RapidsShuffleManager"),
      s"Expected RAPIDS shuffle manager, but got: $shuffleManager")
  }

  test("extreme shuffle to force memory-to-file transition") {
    // EXTREME test case to trigger memory->file transition on MAPPER side:
    // Strategy: Create large partial files that exceed buffer limits
    // - 10M rows PER mapper partition × 10 partitions = 100M rows total
    // - Very wide schema with multiple large string columns
    // - Config: initialBufferSize=10MB, maxBufferSize=50MB
    // - Each mapper's partial file will quickly exceed 10MB initial buffer
    // - Buffer expansion triggers when writing >> 10MB:
    //   * Tries to expand: 10MB → 20MB → 40MB → would need 80MB
    //   * But 80MB > 50MB max limit → spillBufferToFileAndSwitch()
    //   * OR cumulative memory pressure from 10 concurrent writes > 50% threshold
    val df = spark.range(0, 100000000, 1, 10)
      .selectExpr(
        "id",
        "id % 100 as key",
        // Multiple large string columns to increase shuffle write size
        "concat('very_long_string_column_1_with_id_', cast(id as string), " +
          "'_and_extra_padding_data_here_', cast(id * 2 as string)) as str1",
        "concat('very_long_string_column_2_with_id_', cast(id as string), " +
          "'_more_padding_information_', cast(id * 3 as string)) as str2",
        "concat('very_long_string_column_3_with_id_', cast(id as string), " +
          "'_additional_data_content_', cast(id * 4 as string)) as str3",
        "concat('very_long_string_column_4_with_id_', cast(id as string), " +
          "'_extra_suffix_padding_', cast(id * 5 as string)) as str4",
        "id * 2 as value1",
        "id * 3 as value2",
        "cast(rand() * 1000000 as bigint) as random_large_value"
      )
    
    // Force evaluation with aggregation
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("count"),
        org.apache.spark.sql.functions.sum("random_large_value").alias("sum_random"),
        org.apache.spark.sql.functions.max("str1").alias("max_str")
      )
      .collect()
    
    assert(result.length == 100, "Should have 100 groups")
    
    // Each group should have 1M rows (100M / 100)
    result.foreach { row =>
      assert(row.getAs[Long]("count") == 1000000,
        s"Group ${row.getAs[Long]("key")} should have 1M rows")
    }
  }

  test("shuffle with null values and larger dataset") {
    // Generate nulls by using conditional expression
    // Larger dataset to ensure multi-batch shuffle
    val df = spark.range(0, 500000, 1, 20)
      .selectExpr(
        "case when id % 3 = 0 then null else id % 100 end as key",
        "concat('value_', cast(id as string)) as value",
        "id * 2 as numeric_value"
      )
    
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("count"),
        org.apache.spark.sql.functions.sum("numeric_value").alias("sum_val")
      )
      .collect()
    
    // Should have 100 groups + 1 null group = 101
    assert(result.length == 101, "Should have 101 groups including null")
    
    val nullGroup = result.find(r => r.isNullAt(r.fieldIndex("key")))
    assert(nullGroup.isDefined, "Should have null group")
    // Every 3rd row is null, so ~166K rows should be null
    val nullCount = nullGroup.get.getAs[Long]("count")
    assert(nullCount > 166000 && nullCount < 167000,
      s"Null group should have ~166K rows, got $nullCount")
  }

  test("shuffle correctness with heavily skewed data distribution") {
    // Heavily skewed distribution: key 0 gets 90% of data
    // 1M rows PER partition × 30 partitions = 30M rows total
    // Key 0 receives ~27M rows = massive single partition shuffle write
    val df = spark.range(0, 30000000, 1, 30)
      .selectExpr(
        // 90% go to key 0, rest distributed among keys 10-19 (10 keys)
        "case when id % 10 < 9 then 0 else cast((id % 10 - 8) * 10 + " +
          "(id % 100) / 10 as long) end as key",
        "id as value",
        "concat('skewed_data_', cast(id as string)) as str_col"
      )
    
    val result = df.groupBy("key")
      .agg(
        org.apache.spark.sql.functions.count("*").alias("count"),
        org.apache.spark.sql.functions.sum("value").alias("sum_value")
      )
      .orderBy("key")
      .collect()
    
    // Should have 11 keys total: key 0 and keys 10-19
    assert(result.length == 11, s"Should have 11 keys, got ${result.length}")
    
    // Key 0 should have ~27M rows (90% of 30M)
    val key0 = result.find(_.getAs[Long]("key") == 0)
    assert(key0.isDefined, "Key 0 should exist")
    val key0Count = key0.get.getAs[Long]("count")
    assert(key0Count == 27000000,
      s"Key 0 should have exactly 27M rows (90%), got $key0Count")
    
    // Each other key (10-19) should have ~300K rows (10% / 10 = 1% each)
    val otherKeys = result.filter(_.getAs[Long]("key") != 0)
    assert(otherKeys.length == 10, s"Should have 10 non-zero keys, got ${otherKeys.length}")
    otherKeys.foreach { row =>
      val key = row.getAs[Long]("key")
      val count = row.getAs[Long]("count")
      assert(key >= 10 && key <= 19, s"Other keys should be in range 10-19, got $key")
      assert(count == 300000,
        s"Key $key should have exactly 300K rows (1%), got $count")
    }
  }

  test("shuffle with join - 1MB max buffer triggers fallback") {
    recreateSessionWithBufferConfig("1m")
    
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
    
    // With 1MB max buffer and ~4MB data, buffer cannot expand enough
    // Should directly spill to disk without expansion
    assert(!hasExpansion && hasSpill && hasForcedFileOnly,
      s"Expected NO expansion and spill with 1MB max buffer. " +
      s"expansion=$hasExpansion, spill=$hasSpill, forcedFileOnly=$hasForcedFileOnly")
  }

  test("shuffle with join - 6MB max buffer avoids fallback") {
    recreateSessionWithBufferConfig("6m")
    
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
    
    // With 6MB max buffer, should see expansion but NOT spill
    // Data should stay in memory after expansion
    assert(hasExpansion && !hasSpill && hasForcedFileOnly,
      s"Expected expansion, NO spill, forced file-only with 6MB max buffer. " +
      s"expansion=$hasExpansion, spill=$hasSpill, forcedFileOnly=$hasForcedFileOnly")
  }
}

