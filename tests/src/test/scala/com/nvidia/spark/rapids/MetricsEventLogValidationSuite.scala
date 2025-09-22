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

import java.io.File

import scala.collection.mutable
import scala.io.Source

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class MetricsEventLogValidationSuite extends AnyFunSuite with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val tempDir = new File(System.getProperty("java.io.tmpdir"), "metrics-eventlog-test")
  private val eventLogDir = new File(tempDir, "eventlogs")

  override def beforeEach(): Unit = {
    // Clean up temp directories
    if (tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
    tempDir.mkdirs()
    eventLogDir.mkdirs()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MetricsEventLogValidation")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.rapids.sql.enabled", "true")
      .set("spark.rapids.sql.batchSizeBytes", "65536")
      .set("spark.eventLog.enabled", "true")
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set("spark.eventLog.dir", eventLogDir.getAbsolutePath)
      .set("spark.eventLog.rolling.enabled", "false") // Keep single file for easier parsing
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  override def afterEach(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }

    if (tempDir.exists()) {
      org.apache.commons.io.FileUtils.deleteDirectory(tempDir)
    }
  }

  case class MetricRecord(name: String, value: Long, stage: Option[Int] = None)
  case class TaskTimeRecord(taskId: Long, executionTime: Long, stage: Option[Int] = None)

  private def parseEventLogs(): (List[MetricRecord], List[TaskTimeRecord]) = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val metrics = mutable.ListBuffer[MetricRecord]()
    val taskTimes = mutable.ListBuffer[TaskTimeRecord]()
    
    val eventLogFiles = eventLogDir.listFiles()
      .filter(f => f.getName.endsWith(".inprogress") || !f.getName.contains("_tmp_"))
      .toList
    
    eventLogFiles.foreach { file =>
      try {
        val lines = Source.fromFile(file, "UTF-8").getLines().toList
        
        lines.foreach { line =>
          try {
            val json = parse(line)
            val eventType = (json \ "Event").extractOpt[String]
            
            eventType match {
              case Some("SparkListenerTaskEnd") =>
                val stageId = (json \ "Stage ID").extractOpt[Int]
                val taskInfo = (json \ "Task Info")
                
                // Extract task execution time from Task Metrics
                val taskId = (taskInfo \ "Task ID").extractOpt[Long]
                val taskMetrics = (json \ "Task Metrics")
                // https://github.com/apache/spark/blob/450b415028c3b00f3a002126cd11318d3932e28f/
                // core/src/main/scala/org/apache/spark/ui/jobs/StagePage.scala#L151
                val executorRunTime = (taskMetrics \ "Executor Run Time").extractOpt[Long]
                
                (taskId, executorRunTime) match {
                  case (Some(tId), Some(runTime)) =>
                    taskTimes += TaskTimeRecord(tId, runTime, stageId)
                  case _ => // Skip if timing data is incomplete
                }
                
                // Extract operator time metrics
                val accumulables = (taskInfo \ "Accumulables").extract[List[JObject]]
                accumulables.foreach { acc =>

                  // refer org.apache.spark.scheduler.AccumulableInfo
                  val name = (acc \ "Name").extractOpt[String]
                  val value = (acc \ "Update").extractOpt[String]
                  
                  (name, value) match {
                    case (Some(n), Some(v)) if n.equals("op time") => {
                      metrics += MetricRecord(n, v.toLong, stageId)
                    }
                    case _ => // Ignore other metrics
                  }
                }
                
              case _ => // Ignore other events
            }
          } catch {
            case _: Exception => // Skip malformed lines
          }
        }
      } catch {
        case e: Exception =>
          println(s"Warning: Could not parse event log ${file.getName}: ${e.getMessage}")
      }
    }
    
    (metrics.toList, taskTimes.toList)
  }

  test("operator time metrics are recorded in event logs with OpTimeTracking enabled") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Enable OpTimeTracking
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")

    val numRows = 5000000L
    val numTasks = 8

    // Run query that should generate operator time metrics
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 20 as group_key",
        "rand() * 100 as value"
      )
      .groupBy("group_key")
      .agg(
        count("*").as("count"),
        sum("value").as("sum_value"),
        avg("value").as("avg_value")
      )
      .filter($"count" > 1000)

    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")

    // Parse event logs to find metrics and task times
    val (metrics, taskTimes) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

    assert(operatorTimeMetrics.nonEmpty,
      s"Should find operator time metrics in event logs. " +
        s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")

    assert(taskTimes.nonEmpty,
      s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")

    // Calculate total operator time (in nanoseconds)
    val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

    // Calculate total task execution time
    // (Executor Run Time in milliseconds, convert to nanoseconds)
    val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum

    // Verify metric values are reasonable (> 0)
    operatorTimeMetrics.foreach { metric =>
      assert(metric.value > 0, s"operator time metric ${metric.name} " +
        s"should have positive value, got ${metric.value}")
    }

    taskTimes.foreach { taskTime =>
      assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
        s"should be positive, got ${taskTime.executionTime}")
    }

    println(s"Found ${operatorTimeMetrics.length} operator time metrics in event logs")
    println(s"Found ${taskTimes.length} executor run time records in event logs")
    println(f"Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
    println(f"Total executor run time: ${totalTaskExecutionTime / 1000000.0}%.2f ms")

    // Verify that operator time is within expected range of executor run time
    // Operator time should be between 80% and 100% of executor run time
    val minExpectedOperatorTime = totalTaskExecutionTime * 0.8
    val maxExpectedOperatorTime = totalTaskExecutionTime
    val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble

    println(f"Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% of executor run time")
    println(f"Expected range: 80.0%% - 100.0%% of executor run time")

    assert(totalOperatorTime >= minExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should be at least 80%% " +
        f"of total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was only ${operatorTimeRatio * 100.0}%.1f%%")

    assert(totalOperatorTime <= maxExpectedOperatorTime,
      f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should not exceed " +
        f"total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
        f"but was ${operatorTimeRatio * 100.0}%.1f%%")

    operatorTimeMetrics.foreach { m =>
      println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms (stage ${m.stage.getOrElse("unknown")})")
    }
  }

  test("operator time metrics are less when c2r and r2c happened") {
    val sparkSession = spark
    import sparkSession.implicits._
      
    // Enable OpTimeTracking
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")

    try {
      spark.conf.set("spark.rapids.sql.exec.HashAggregateExec", "false")

      val numRows = 5000000L
      val numTasks = 8

      // Run query that should generate operator time metrics
      val resultDF = spark.range(0, numRows, 1, numTasks)
        .selectExpr(
          "id",
          "id % 20 as group_key",
          "rand() * 100 as value"
        )
        .groupBy("group_key")
        .agg(
          count("*").as("count"),
          sum("value").as("sum_value"),
          avg("value").as("avg_value")
        )
        .filter($"count" > 1000)

      val results = resultDF.collect()
      assert(results.length > 0, "Query should produce results")

      // Parse event logs to find metrics and task times
      val (metrics, taskTimes) = parseEventLogs()
      val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

      assert(operatorTimeMetrics.nonEmpty,
        s"Should find operator time metrics in event logs. " +
          s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")

      assert(taskTimes.nonEmpty,
        s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")

      // Calculate total operator time (in nanoseconds)
      val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

      // Calculate total task execution time
      // (Executor Run Time in milliseconds, convert to nanoseconds)
      val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum

      // Verify metric values are reasonable (> 0)
      operatorTimeMetrics.foreach { metric =>
        assert(metric.value > 0, s"operator time metric ${metric.name} " +
          s"should have positive value, got ${metric.value}")
      }

      taskTimes.foreach { taskTime =>
        assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
          s"should be positive, got ${taskTime.executionTime}")
      }

      println(s"Found ${operatorTimeMetrics.length} operator time metrics in event logs")
      println(s"Found ${taskTimes.length} executor run time records in event logs")
      println(f"Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
      println(f"Total executor run time: ${totalTaskExecutionTime / 1000000.0}%.2f ms")

      // Verify that operator time is within expected range of executor run time
      // Operator time should be between 80% and 100% of executor run time
      val minExpectedOperatorTime = totalTaskExecutionTime * 0.1
      val maxExpectedOperatorTime = totalTaskExecutionTime * 0.8
      val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble

      println(f"Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% of executor run time")
      println(f"Expected range: 80.0%% - 100.0%% of executor run time")

      assert(totalOperatorTime >= minExpectedOperatorTime,
        f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should be at least 80%% " +
          f"of total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
          f"but was only ${operatorTimeRatio * 100.0}%.1f%%")

      assert(totalOperatorTime <= maxExpectedOperatorTime,
        f"Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) should not exceed " +
          f"total executor run time (${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
          f"but was ${operatorTimeRatio * 100.0}%.1f%%")

      operatorTimeMetrics.foreach { m =>
        println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms " +
          f"(stage ${m.stage.getOrElse("unknown")})")
      }
      println("Test completed successfully.")
    } finally {
      spark.conf.set("spark.rapids.sql.exec.HashAggregateExec", "true")
    }
  }

  test("operator time metrics are reasonable for parquet write jobs") {
    val sparkSession = spark
    import sparkSession.implicits._
    
    // Save original configurations
    val originalOpTimeTracking =
      spark.conf.getOption("spark.rapids.sql.exec.opTimeTrackingRDD.enabled")
    val originalSlowfsImpl = spark.conf.getOption("fs.slowfs.impl")

    try {
      // Enable OpTimeTracking for this test
      spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "true")
      
      // Configure slow filesystem for testing
      spark.conf.set("fs.slowfs.impl", "com.nvidia.spark.rapids.SlowFileSystem")

      val numRows = 5000000L
      val numTasks = 8
      val parquetOutputPath = "slowfs://" + new File(tempDir, "test_parquet").getAbsolutePath
    
      // Create test data for parquet write
      val testDataDF = spark.range(0, numRows, 1, numTasks)
        .selectExpr(
          "id",
          "id % 100 as category", 
          "rand() * 1000 as price",
          "cast(rand() * 10000 as int) as quantity"
        )
        .groupBy("category")
        .agg(
          count("*").as("total_count"),
          sum("price").as("total_price"),
          avg("quantity").as("avg_quantity")
        )
        .filter($"total_count" > 5)
    
      // Write to slow filesystem Parquet format with repartitioning
      // and take significant time due to filesystem delays
      testDataDF
        .repartition(50)  // Repartition to 50 partitions to amplify write time
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(parquetOutputPath)
    
      // Verify the output file exists (check the actual file path, not the slowfs path)
      val actualOutputPath = new File(tempDir, "test_parquet")
      assert(actualOutputPath.exists() && actualOutputPath.listFiles().nonEmpty,
        "Parquet output files should be created")
      
      // Parse event logs to find metrics and task times
      val (metrics, taskTimes) = parseEventLogs()
      val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

      assert(operatorTimeMetrics.nonEmpty, 
        s"Should find operator time metrics for parquet write job. " +
          s"Found ${metrics.length} total metrics: ${metrics.map(_.name).distinct}")
      
      assert(taskTimes.nonEmpty, 
        s"Should find executor run times in event logs. Found ${taskTimes.length} tasks")
      
      // Calculate total operator time (in nanoseconds)
      val totalOperatorTime = operatorTimeMetrics.map(_.value).sum

      // Calculate total task execution time
      // (Executor Run Time in milliseconds, convert to nanoseconds)
      val totalTaskExecutionTime = taskTimes.map(_.executionTime * 1000000L).sum
      
      // Verify metric values are reasonable (> 0)
      operatorTimeMetrics.foreach { metric =>
        assert(metric.value > 0, s"operator time metric ${metric.name} " +
          s"should have positive value, got ${metric.value}")
      }
      
      taskTimes.foreach { taskTime =>
        assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
          s"should be positive, got ${taskTime.executionTime}")
      }
    
      println(s"Parquet write job: Found ${operatorTimeMetrics.length} operator time metrics")
      println(s"Parquet write job: Found ${taskTimes.length} executor run time records")
      println(f"Parquet write job: Total operator time: ${totalOperatorTime / 1000000.0}%.2f ms")
      println(f"Parquet write job: Total executor run time: " +
        f"${totalTaskExecutionTime / 1000000.0}%.2f ms")
    
    val minExpectedOperatorTime = totalTaskExecutionTime * 0.7
    val maxExpectedOperatorTime = totalTaskExecutionTime
    val operatorTimeRatio = totalOperatorTime.toDouble / totalTaskExecutionTime.toDouble
    
    println(f"Parquet write job: Operator time ratio: ${operatorTimeRatio * 100.0}%.1f%% " +
      "of executor run time")
    println(f"Parquet write job: Expected range: 70.0%% - 100.0%% of executor run time")
    
    assert(totalOperatorTime >= minExpectedOperatorTime, 
      f"Parquet write job: Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) " +
      f"should be at least 70%% of total executor run time " +
      f"(${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
      f"but was only ${operatorTimeRatio * 100.0}%.1f%%")
    
    assert(totalOperatorTime <= maxExpectedOperatorTime, 
      f"Parquet write job: Total operator time (${totalOperatorTime / 1000000.0}%.2f ms) " +
      f"should not exceed total executor run time " +
      f"(${totalTaskExecutionTime / 1000000.0}%.2f ms), " +
      f"but was ${operatorTimeRatio * 100.0}%.1f%%")
    
    // Assert stage 5 (parquet write stage) operator time accounts for > 20% of total
    val stage5Metrics = operatorTimeMetrics.filter(_.stage.contains(5))
    val stage5OperatorTime = stage5Metrics.map(_.value).sum
    val stage5Ratio = if (totalOperatorTime > 0) {
      stage5OperatorTime.toDouble / totalOperatorTime.toDouble
    } else {
      0.0
    }
    
    println(f"Parquet write job: Stage 5 operator time: " +
      f"${stage5OperatorTime / 1000000.0}%.2f ms")
    println(f"Parquet write job: Stage 5 ratio: ${stage5Ratio * 100.0}%.1f%% " +
      "of total operator time")
    
    assert(stage5Metrics.nonEmpty, 
      "Should find operator time metrics for stage 5 (parquet write stage)")
    
    assert(stage5Ratio > 0.2,
      f"Stage 5 (parquet write stage) operator time should account for more than 20%% " +
      f"of total operator time, but was only ${stage5Ratio * 100.0}%.1f%% " +
      f"(${stage5OperatorTime / 1000000.0}%.2f ms out of " +
      f"${totalOperatorTime / 1000000.0}%.2f ms)")
      
      operatorTimeMetrics.foreach { m =>
        println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms " +
          f"(stage ${m.stage.getOrElse("unknown")})")
      }
      println("Parquet write job: Test completed successfully.")

    } finally {
      // Restore original configurations
      originalOpTimeTracking match {
        case Some(value) => spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", value)
        case None => spark.conf.unset("spark.rapids.sql.exec.opTimeTrackingRDD.enabled")
      }
      originalSlowfsImpl match {
        case Some(value) => spark.conf.set("fs.slowfs.impl", value)
        case None => spark.conf.unset("fs.slowfs.impl")
      }
    }
  }

  test("no operator time metrics when OpTimeTracking is disabled") {
    val sparkSession = spark
    import sparkSession.implicits._
      
    // Disable OpTimeTracking for this test
    spark.conf.set("spark.rapids.sql.exec.opTimeTrackingRDD.enabled", "false")
    
    val numRows = 2000000L
    val numTasks = 4
    
    // Run a query that would normally generate operator time metrics
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 50 as bucket",
        "rand() * 500 as score"
      )
      .groupBy("bucket")
      .agg(
        count("*").as("record_count"),
        sum("score").as("total_score"),
        max("score").as("max_score"),
        min("score").as("min_score")
      )
      .filter($"record_count" > 10000)
      .orderBy($"total_score".desc)
    
    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")
    
    // Parse event logs to find metrics and task times
    val (metrics, taskTimes) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("op time"))

    println(s"OpTimeTracking disabled: Found ${metrics.length} total metrics")
    println(s"OpTimeTracking disabled: Found ${operatorTimeMetrics.length} operator time metrics")
    println(s"OpTimeTracking disabled: Found ${taskTimes.length} executor run time records")
    
    // When OpTimeTracking is disabled, there should be no operator time metrics
    assert(operatorTimeMetrics.isEmpty, 
      s"Should not find any operator time metrics when OpTimeTracking is disabled. " +
        s"Found ${operatorTimeMetrics.length} operator time metrics: " +
        s"${operatorTimeMetrics.map(m => s"${m.name}=${m.value}")}")
    
    // But we should still have task execution times
    assert(taskTimes.nonEmpty, 
      s"Should still find executor run times even when OpTimeTracking is disabled. " +
        s"Found ${taskTimes.length} tasks")
    
    // Verify task execution times are reasonable
    taskTimes.foreach { taskTime =>
      assert(taskTime.executionTime > 0, s"task ${taskTime.taskId} executor run time " +
        s"should be positive, got ${taskTime.executionTime}")
    }
    
    val totalTaskExecutionTime = taskTimes.map(_.executionTime).sum
    println(f"OpTimeTracking disabled: Total executor run time: " +
      f"${totalTaskExecutionTime}%.2f ms")
    
    // Verify that we executed a meaningful workload (total execution time > 100ms)
    assert(totalTaskExecutionTime > 100, 
      s"Total task execution time should be substantial to validate the test, " +
        s"got ${totalTaskExecutionTime} ms")
  }

}
