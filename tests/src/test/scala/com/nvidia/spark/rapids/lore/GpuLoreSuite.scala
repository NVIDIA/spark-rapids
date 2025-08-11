/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.lore

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, GpuColumnarToRowExec, RapidsConf, ShimLoader, SparkQueryCompareTestSuite}
import com.nvidia.spark.rapids.Arm.withResource
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{functions, DataFrame, SparkSession}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.internal.SQLConf

class GpuLoreSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir with Logging {
  /**
   * Check if the current version should be skipped for GpuInsertIntoHiveTable test
   */
  private def skipIfUnsupportedVersion(testName: String): Unit = {
    val version = ShimLoader.getShimVersion
    val unsupportedVersions = GpuLore.getGpuWriteFilesUnsupportedVersions

    // Skip this test if the current version is in unsupported versions
    assume(!unsupportedVersions.contains(version),
      s"Skipping $testName for version $version as it's in the unsupported versions list")
  }

  // Reusable Derby cleanup function
  private def cleanupDerbyFiles(metastoreDir: String = "metastore_db"): Unit = {
    val derbyFiles = Seq("derby.log", "derbyout.log", "derbyerr.log")
    derbyFiles.foreach { fileName =>
      val file = new java.io.File(fileName)
      if (file.exists()) {
        try {
          file.delete()
        } catch {
          case _: Exception => // Ignore cleanup failures
        }
      }
    }

    val metadataDb = new java.io.File(metastoreDir)
    if (metadataDb.exists()) {
      import java.nio.file.{Files, Paths}
      import java.util.Comparator
      try {
        if (metadataDb.isDirectory) {
          Files.walk(Paths.get(metastoreDir))
            .sorted(Comparator.reverseOrder())
            .forEach(Files.deleteIfExists(_))
        } else {
          metadataDb.delete()
        }
      } catch {
        case _: Exception =>
          try {
            import sys.process._
            s"rm -rf $metastoreDir".!
          } catch {
            case _: Exception => // Ignore cleanup failures
          }
      }
    }
  }

  test("Aggregate") {
    // The max value and number of values make it so that SUM will not overflow, so ANIS is good
    doTestReplay("10[*]") { spark =>
      spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))
    }
  }

  test("Broadcast join") {
    // The max value and number of values make it so that SUM will not overflow, so ANIS is good
    doTestReplay("32[*]") { spark =>
      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("Subquery Filter") {
    doTestReplay("13[*]") { spark =>
      spark.range(0, 100, 1, 10)
        .createTempView("df1")

      spark.range(50, 1000, 1, 10)
        .createTempView("df2")

      spark.sql("select * from df1 where id > (select max(id) from df2)")
    }
  }

  test("Subquery in projection") {
    doTestReplay("11[*]") { spark =>
      spark.sql(
        """
          |CREATE TEMPORARY VIEW t1
          |AS SELECT * FROM VALUES
          |(1, "a"),
          |(2, "a"),
          |(3, "a") t(id, value)
          |""".stripMargin)

      spark.sql(
        """
          |SELECT *, (SELECT COUNT(*) FROM t1) FROM t1
          |""".stripMargin)
    }
  }

  test("No broadcast join") {
    // The max value and number of values make it so that SUM will not overflow, so ANIS is good
    doTestReplay("30[*]") { spark =>
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("AQE broadcast") {
    // The max value and number of values make it so that SUM will not overflow, so ANIS is good
    doTestReplay("93[*]") { spark =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      val df1 = spark.range(0, 1000, 1, 10)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      val df2 = spark.range(0, 1000, 1, 10)
        .selectExpr("(id % 10 + 5) as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("count"))

      df1.join(df2, Seq("key"))
    }
  }

  test("AQE Exchange") {
    // The max value and number of values make it so that SUM will not overflow, so ANIS is good
    doTestReplay("28[*]") { spark =>
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))
    }
  }

  test("Partition only") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[0 2]")

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      val res = df.collect().length
      println(s"Length of original: $res")


      val restoredRes = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-3"), spark))
        .executeCollect()
        .length

      assert(20 == restoredRes)
    }
  }

  test("Non-empty lore dump path") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[*]")

      //Create a file in the root path
      val path = new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/test")
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      withResource(fs.create(path, true)) { _ =>
      }

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      assertThrows[IllegalArgumentException] {
        df.collect()
      }
    }
  }

  test("Skip dumping plan") {
    withGpuSparkSession{ spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "3[*]")
      spark.conf.set(RapidsConf.LORE_SKIP_DUMPING_PLAN.key, "true")

      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")

      assert(1000 == df.collect().length)

      val lorePath = new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-3")
      val planDataFile = GpuLore.pathOfRootPlanMeta(lorePath)
      val fs = planDataFile.getFileSystem(spark.sparkContext.hadoopConfiguration)

      assert(!fs.exists(planDataFile))
    }
  }

  test("GpuShuffledSymmetricHashJoin with SerializedTableColumn") {
    doTestReplay("56[*]") { spark =>
      // Disable broadcast join, force hash join
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")

      // Create larger tables to ensure shuffle
      val df1 = spark.range(0, 1000, 1, 100)
      .selectExpr("id % 10 as key", "id as value")
    val df2 = spark.range(0, 1000, 1, 100)
      .selectExpr("id % 10 as key", "id as value")
      // Join with equality condition to trigger hash join
      df1.join(df2, Seq("key"))
    }
  }

  test("GpuShuffledSymmetricHashJoin with in Kudo mode") {
    doTestReplay("56[*]") { spark =>
      // Disable broadcast join, force hash join
      spark.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      spark.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "true")
      // in Kudo mode
      spark.conf.set(RapidsConf.SHUFFLE_KUDO_SERIALIZER_ENABLED.key, "true")

      // Create larger tables to ensure shuffle
      val df1 = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id as value")
      val df2 = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id as value")
      // Join with equality condition to trigger hash join
      df1.join(df2, Seq("key"))
    }
  }

  test("GpuInsertIntoHiveTable with LoRE dump and replay") {
    /*
    This test is different from previous ones as it's a HiveInsert operation.
    It will produce 0 output rows while writing data to a Hive Table.
    So the validation should be comparing the Hive Table content between the first run and the
    replay run.
    */

    // Skip this test for versions that are in unsupportedHiveInsertVersions
    skipIfUnsupportedVersion("GpuInsertIntoHiveTable with LoRE dump and replay")
    // Clean up before test starts
    cleanupDerbyFiles()
    // Create unique metastore location for this test run
    val uniqueMetastoreDir = s"metastore_db_${System.currentTimeMillis()}"
    try {
      val loreDumpIds = "5[*]" // Fixed LORE ID since unsupported versions are skipped
      val loreId = OutputLoreId.parse(loreDumpIds).head._1

      // Use a single SparkSession for both operations with unique metastore
      withGpuHiveSparkSession({ spark =>
        // Configure unique metastore location
        spark.conf.set("javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$uniqueMetastoreDir;create=true")

        // First execution - capture the original data written to Hive table
        val originalDataCount = 100
        // Configure LORE for the first execution
        spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
        spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, loreDumpIds)

        // Clean up any existing table and its associated directory
        spark.sql("DROP TABLE IF EXISTS test_hive_table")
        spark.sql("""
          CREATE TABLE test_hive_table (
            id INT,
            name STRING
          )
          STORED AS textfile
        """)

        // Create test data and insert into Hive table - this should trigger LoRE dumping
        val df = spark.range(0, originalDataCount, 1, 10)
          .selectExpr("id as id", "concat('name_', id) as name")
        df.write
          .mode("overwrite")
          .insertInto("test_hive_table")
        // Disable LORE for the replay phase
        spark.conf.unset(RapidsConf.LORE_DUMP_PATH.key)
        spark.conf.unset(RapidsConf.LORE_DUMP_IDS.key)
        // Clean up and recreate table for replay
        spark.sql("DROP TABLE IF EXISTS test_hive_table")
        spark.sql("""
          CREATE TABLE test_hive_table (
            id INT,
            name STRING
          )
          STORED AS textfile
        """)

        // Execute the LoRE replay
        // LoRE replay is not a fully-completed Query execution, but partial,
        // no query executionId is set. Mean while,
        // HiveInsertInto will trigger GpuFileFormatWriter, which will check the executionId,
        // if the executionId is not set, it will throw an exception.
        // So we need to set a valid executionId here.
        val executionId = 9999L
        spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
        try {
          GpuColumnarToRowExec(GpuLore.restoreGpuExec(
            new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-$loreId"),
            spark))
            .executeCollect()
        } finally {
          spark.sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
        }
        // TODO: remove, reason:
        // Spark 400 enables ANSI mode by default which will fallback the shuffleExec
        spark.conf.set(RapidsConf.TEST_CONF.key, "false")
        // Read the data written by replay and count it
        val replayDataCount = spark.sql("SELECT * FROM test_hive_table").count()
        // Verify the results
        assert(originalDataCount == replayDataCount,
          s"Original data count ($originalDataCount) != Replay data count ($replayDataCount)")
      })
    } finally {
      // Clean up warehouse directory to avoid conflicts in future test runs
      val possiblePaths = Seq(
        "tests/spark-warehouse/test_hive_table",
        "spark-warehouse/test_hive_table"
      )
      possiblePaths.foreach { pathStr =>
        val warehouseDir = new java.io.File(pathStr)
        if (warehouseDir.exists()) {
          import java.nio.file.{Files, Paths}
          import java.util.Comparator
          try {
            Files.walk(Paths.get(warehouseDir.getPath))
              .sorted(Comparator.reverseOrder())
              .forEach(Files.deleteIfExists(_))
          } catch {
            case _: Exception => // Ignore cleanup failures
          }
        }
      }
      // Clean up Derby files and metastore directories
      cleanupDerbyFiles()
      cleanupDerbyFiles(uniqueMetastoreDir)
    }
  }

  test("LORE dump should throw exception for GpuDataWritingCommandExec on unsupported versions") {
    // Only run this test for versions that are in unsupported versions
    val currentShimVersion = ShimLoader.getShimVersion
    val unsupportedVersions = GpuLore.getGpuWriteFilesUnsupportedVersions

    // Skip this test if the current version is NOT in unsupported versions
    assume(unsupportedVersions.contains(currentShimVersion),
      s"Skipping test for version $currentShimVersion as it's not in the unsupported versions list")

    // Clean up before test starts
    cleanupDerbyFiles()

    try {
      withGpuHiveSparkSession { spark =>
        // Configure LORE for testing
        spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
        spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "7[*]")

        // Clean up any existing table
        spark.sql("DROP TABLE IF EXISTS test_hive_table")
        spark.sql("""
          CREATE TABLE test_hive_table (
            id INT,
            name STRING
          )
          STORED AS textfile
        """)

        // Create test data
        val df = spark.range(0, 10, 1, 2)
          .selectExpr("id as id", "concat('name_', id) as name")

        // This should throw an exception if the current version is in the unsupported list
        val exception = intercept[UnsupportedOperationException] {
          df.write
            .mode("overwrite")
            .insertInto("test_hive_table")
        }

        // Verify the exception message contains the expected information
        assert(exception.getMessage.contains("LORE dump is not supported for" +
          " GpuDataWritingCommandExec"))
        assert(exception.getMessage.contains("Unsupported versions:"))
      }
    } finally {
      // Clean up Derby files after test completes
      cleanupDerbyFiles()
    }
  }

  test("Parquet original schema names") {
    withGpuSparkSession { spark =>
      // Enable LORE dump with original schema names for parquet
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, "10[*]")
      spark.conf.set(RapidsConf.LORE_PARQUET_USE_ORIGINAL_NAMES.key, "true")

      // Build an aggregate plan (same as Aggregate test)
      val df = spark.range(0, 1000, 1, 100)
        .selectExpr("id % 10 as key", "id % 100 as value")
        .groupBy("key")
        .agg(functions.sum("value").as("total"))

      // Trigger execution to produce LORE dump
      val _ = df.collect().length

      // Inspect dumped parquet under loreId-10/input-0
      val inputRoot = new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-10/input-0")
      val fs = inputRoot.getFileSystem(spark.sparkContext.hadoopConfiguration)

      // Read expected names from rdd.meta
      val metaPath = new Path(inputRoot, "rdd.meta")
      val meta = GpuLore.loadObject[LoreRDDMeta](metaPath, spark.sparkContext.hadoopConfiguration)
      val expectedNames = meta.attrs.map(_.name)

      // Find a parquet batch file in one partition folder and verify column names
      val partDir = fs.listStatus(inputRoot).map(_.getPath)
        .find(p => p.getName.startsWith("partition-"))
        .get
      val parquetFile = fs.listStatus(partDir).map(_.getPath)
        .find(p => p.getName.endsWith(".parquet")).get

      val directDf = spark.read.parquet(parquetFile.toString)
      assert(directDf.columns.toSeq == expectedNames)
    }
  }

  private def doTestReplay(loreDumpIds: String)(dfFunc: SparkSession => DataFrame) = {
    val loreId = OutputLoreId.parse(loreDumpIds).head._1
    withGpuSparkSession { spark =>
      spark.conf.set(RapidsConf.LORE_DUMP_PATH.key, TEST_FILES_ROOT.getAbsolutePath)
      spark.conf.set(RapidsConf.LORE_DUMP_IDS.key, loreDumpIds)

      val df = dfFunc(spark)

      val expectedLength = df.collect().length

      val restoredResultLength = GpuColumnarToRowExec(GpuLore.restoreGpuExec(
        new Path(s"${TEST_FILES_ROOT.getAbsolutePath}/loreId-$loreId"),
        spark))
        .executeCollect()
        .length

      assert(expectedLength == restoredResultLength)
    }
  }
}
