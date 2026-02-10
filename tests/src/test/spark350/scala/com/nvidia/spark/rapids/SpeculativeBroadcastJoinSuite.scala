/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "400"}
{"spark": "410"}
{"spark": "420"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.rapids.execution.GpuBroadcastHashJoinExec

/**
 * Test suite for Speculative Broadcast Join optimization.
 *
 * This optimization allows the query planner to speculatively add shuffle to the build side
 * of a join and make a runtime decision (during AQE) whether to use BroadcastHashJoin or
 * ShuffledHashJoin based on actual shuffled data size.
 */
class SpeculativeBroadcastJoinSuite extends SparkQueryCompareTestSuite {

  private def baseConf: SparkConf = new SparkConf()
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")  // Disable auto broadcast

  /**
   * Creates test parquet files and returns paths.
   * Large table: ~5MB, Small table: ~500KB
   */
  private def withTestParquetTables(spark: SparkSession)(
      testFn: (String, String) => Unit): Unit = {
    val tempDir = Files.createTempDirectory("spec_bcast_test").toFile
    val largePath = new File(tempDir, "large_table").getAbsolutePath
    val smallPath = new File(tempDir, "small_table").getAbsolutePath

    try {
      // Generate and write large table parquet (~5MB)
      val largeDF = spark.range(0, 500000).toDF("key")
        .withColumn("value", (rand() * 1000).cast("int"))
        .withColumn("category", (col("key") % 100).cast("string"))
      largeDF.write.parquet(largePath)

      // Generate and write small table parquet (~500KB)
      val smallDF = spark.range(0, 50000).toDF("key")
        .withColumn("filter_col", (col("key") % 10).cast("int"))
        .withColumn("info", concat(lit("info_"), col("key")))
      smallDF.write.parquet(smallPath)

      // Refresh to ensure stats are available
      spark.catalog.refreshByPath(largePath)
      spark.catalog.refreshByPath(smallPath)

      testFn(largePath, smallPath)
    } finally {
      // Cleanup
      deleteRecursively(tempDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  /**
   * Case 1: SpeculativeBroadcast triggers and converts to BroadcastHashJoin.
   *
   * When:
   * - Large side meets largeSideMinSize threshold
   * - Small side has selective filters
   * - Actual shuffled size of build side < targetThreshold
   *
   * Expected: Final plan uses GpuBroadcastHashJoinExec (large side avoids shuffle)
   */
  test("speculative broadcast triggers and uses BroadcastHashJoin") {
    val conf = baseConf
      .set("spark.rapids.sql.speculativeBroadcast.enabled", "true")
      .set("spark.rapids.sql.speculativeBroadcast.targetThreshold", "10MB")
      .set("spark.rapids.sql.speculativeBroadcast.largeSideMinSize", "1MB")

    withGpuSparkSession(spark => {
      withTestParquetTables(spark) { (largePath, smallPath) =>
        spark.read.parquet(largePath).createOrReplaceTempView("large_table")
        spark.read.parquet(smallPath).createOrReplaceTempView("small_table")

        val query = """
          SELECT l.key, l.category, s.info
          FROM large_table l
          JOIN small_table s ON l.key = s.key
          WHERE s.filter_col = 5
        """

        val df = spark.sql(query)
        df.collect()  // Execute to get final adaptive plan

        val plan = df.queryExecution.executedPlan

        // Verify BroadcastHashJoin is used
        val bhjOps = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(bhjOps.nonEmpty,
          s"Expected GpuBroadcastHashJoinExec but not found in plan:\n$plan")

        // Verify no SortMergeJoin (which would mean both sides shuffled)
        val smjOps = PlanUtils.findOperators(plan, _.isInstanceOf[SortMergeJoinExec])
        assert(smjOps.isEmpty,
          "Unexpected SortMergeJoinExec found - large side should avoid shuffle")
      }
    }, conf)
  }

  /**
   * Case 2: SpeculativeBroadcast does NOT trigger due to largeSideMinSize threshold.
   *
   * When:
   * - Large side does NOT meet largeSideMinSize threshold (threshold set very high)
   *
   * Expected: No SpeculativeBroadcast, uses regular SortMergeJoin or ShuffledHashJoin
   */
  test("speculative broadcast not triggered when largeSideMinSize too large") {
    val conf = baseConf
      .set("spark.rapids.sql.speculativeBroadcast.enabled", "true")
      .set("spark.rapids.sql.speculativeBroadcast.targetThreshold", "10MB")
      .set("spark.rapids.sql.speculativeBroadcast.largeSideMinSize", "100GB")  // Very large!

    withGpuSparkSession(spark => {
      withTestParquetTables(spark) { (largePath, smallPath) =>
        spark.read.parquet(largePath).createOrReplaceTempView("large_table")
        spark.read.parquet(smallPath).createOrReplaceTempView("small_table")

        val query = """
          SELECT l.key, l.category, s.info
          FROM large_table l
          JOIN small_table s ON l.key = s.key
          WHERE s.filter_col = 5
        """

        val df = spark.sql(query)
        df.collect()

        val plan = df.queryExecution.executedPlan

        // Should NOT use BroadcastHashJoin (mechanism didn't trigger)
        val bhjOps = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(bhjOps.isEmpty,
          s"Unexpected GpuBroadcastHashJoinExec - mechanism should not trigger:\n$plan")

        // Should use SortMergeJoin or ShuffledHashJoin (both sides shuffle)
        val smjOps = PlanUtils.findOperators(plan, _.isInstanceOf[SortMergeJoinExec])
        val shjOps = PlanUtils.findOperators(plan,
          p => p.isInstanceOf[GpuShuffledSymmetricHashJoinExec] ||
               p.isInstanceOf[GpuShuffledHashJoinExec])

        assert(smjOps.nonEmpty || shjOps.nonEmpty,
          s"Expected SortMergeJoin or ShuffledHashJoin when mechanism doesn't trigger:\n$plan")
      }
    }, conf)
  }

  /**
   * Case 3: SpeculativeBroadcast triggers but falls back to ShuffledHashJoin.
   *
   * When:
   * - SpeculativeBroadcast candidate detection passes
   * - But actual shuffled size of build side >= targetThreshold
   *
   * Expected: Falls back to GpuShuffledHashJoin (both sides shuffle)
   */
  test("speculative broadcast fallback to ShuffledHashJoin when build side too large") {
    val conf = baseConf
      .set("spark.rapids.sql.speculativeBroadcast.enabled", "true")
      .set("spark.rapids.sql.speculativeBroadcast.targetThreshold", "10KB")  // Very small!
      .set("spark.rapids.sql.speculativeBroadcast.largeSideMinSize", "1MB")

    withGpuSparkSession(spark => {
      withTestParquetTables(spark) { (largePath, smallPath) =>
        spark.read.parquet(largePath).createOrReplaceTempView("large_table")
        spark.read.parquet(smallPath).createOrReplaceTempView("small_table")

        val query = """
          SELECT l.key, l.category, s.info
          FROM large_table l
          JOIN small_table s ON l.key = s.key
          WHERE s.filter_col = 5
        """

        val df = spark.sql(query)
        df.collect()

        val plan = df.queryExecution.executedPlan

        // Should NOT use BroadcastHashJoin (fallback due to size)
        val bhjOps = PlanUtils.findOperators(plan, _.isInstanceOf[GpuBroadcastHashJoinExec])
        assert(bhjOps.isEmpty,
          s"Unexpected GpuBroadcastHashJoinExec - should fallback to ShuffledHashJoin:\n$plan")

        // Should use ShuffledHashJoin (fallback)
        val shjOps = PlanUtils.findOperators(plan,
          p => p.isInstanceOf[GpuShuffledSymmetricHashJoinExec] ||
               p.isInstanceOf[GpuShuffledHashJoinExec])
        assert(shjOps.nonEmpty,
          s"Expected GpuShuffledHashJoin after fallback:\n$plan")
      }
    }, conf)
  }
}
