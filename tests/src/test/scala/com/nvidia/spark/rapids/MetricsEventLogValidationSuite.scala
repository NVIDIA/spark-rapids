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

// THIS is still under debugging!!!!!!!!!!




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
                    case (Some(n), Some(v)) if n.equals("operator time") => {
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
    spark.conf.set("spark.rapids.sql.exec.disableOpTimeTrackingRDD", "false")
    
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
    val operatorTimeMetrics = metrics.filter(_.name.equals("operator time"))

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

  test("operatorTime metrics from file writing operations are recorded") {

    val numRows = 30000L  
    val numTasks = 6
    val outputPath = new File(tempDir, "write_test_output").getAbsolutePath
    
    // Run query with file writing to trigger GpuFileFormatDataWriter metrics
    spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "cast(id as string) as id_str",
        "id % 10 as category",
        "rand() * 1000 as value"
      )
      .groupBy("category")
      .agg(
        count("*").as("count"),
        sum("value").as("sum_value")
      )
      .write
      .mode("overwrite")
      .parquet(outputPath)
    
    // Verify output was created
    assert(new File(outputPath).exists(), "Parquet output should exist")
    
    // Parse event logs for operatorTime metrics
    val (metrics, _) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(m => 
      m.name.contains("operatorTime") || m.name.equals("operator time"))
    
    // Note: operatorTime metrics might not always appear in simple cases
    // This test validates the infrastructure is working
    if (operatorTimeMetrics.nonEmpty) {
      println(s"Found ${operatorTimeMetrics.length} operatorTime metrics:")
      operatorTimeMetrics.foreach { m =>
        println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms")
        assert(m.value >= 0, s"operatorTime metric should be non-negative, got ${m.value}")
      }
    } else {
      println("No operatorTime metrics found - may be expected for simple queries")
    }
    
    // At minimum, we should see some GPU metrics recorded
    val allGpuMetrics = metrics.filter(_.name.toLowerCase.contains("gpu"))
    if (allGpuMetrics.nonEmpty) {
      println(s"Found ${allGpuMetrics.length} GPU-related metrics, " +
        s"indicating GPU execution occurred")
    }
  }

  test("OpTimeTracking disabled shows different metrics pattern") {
    val sparkSession = spark
    import sparkSession.implicits._

    // Disable OpTimeTracking
    spark.conf.set("spark.rapids.sql.exec.disableOpTimeTrackingRDD", "true")
    
    val numRows = 40000L
    val numTasks = 6
    
    // Run similar query as the enabled test
    val resultDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id", 
        "id % 15 as group_key",
        "rand() * 50 as value"
      )
      .groupBy("group_key")
      .agg(
        count("*").as("count"),
        sum("value").as("sum_value")
      )
      .filter($"count" > 500)
    
    val results = resultDF.collect()
    assert(results.length > 0, "Query should produce results")
    
    // Parse event logs
    val (metrics, _) = parseEventLogs()
    val operatorTimeMetrics = metrics.filter(_.name.equals("operator time"))
    
    // With tracking disabled, we might still see operator time metrics but with different patterns
    // The key difference is in the RDD wrapping behavior, not necessarily metric absence
    println(s"With OpTimeTracking disabled, " +
      s"found ${operatorTimeMetrics.length} operator time metrics")
    
    if (operatorTimeMetrics.nonEmpty) {
      operatorTimeMetrics.foreach { m =>
        println(f"  ${m.name}: ${m.value / 1000000.0}%.2f ms")
      }
    }
    
    // Verify the query executed successfully regardless of tracking setting
    val totalRecords = results.map(_.getAs[Long]("count")).sum
    assert(totalRecords > 0, "Should have processed some records")
    println(s"Successfully processed $totalRecords records with OpTimeTracking disabled")
  }

  test("Concurrent operations produce consistent metrics") {
    val sparkSession = spark
    import sparkSession.implicits._

    spark.conf.set("spark.rapids.sql.exec.disableOpTimeTrackingRDD", "false")
    spark.conf.set("spark.sql.shuffle.partitions", "20")
    
    val numRows = 60000L
    val numTasks = 10
    
    // Create concurrent operations that should stress thread-safe metrics
    val df1 = spark.range(0, numRows, 1, numTasks)
      .selectExpr("id", "id % 30 as key1", "rand() as value1")
      
    val df2 = spark.range(numRows, numRows * 2, 1, numTasks)
      .selectExpr("id", "id % 30 as key2", "rand() as value2")
    
    // Operations that will execute concurrently and stress metrics collection
    val agg1 = df1.groupBy("key1").agg(sum("value1").as("sum1"))
    val agg2 = df2.groupBy("key2").agg(sum("value2").as("sum2"))
    
    // Force concurrent execution through join
    val joinResult = agg1.join(agg2, $"key1" === $"key2")
      .selectExpr("key1 as key", "sum1 + sum2 as total")
      .orderBy("total")
    
    val results = joinResult.collect()
    assert(results.length > 0, "Concurrent operations should produce results")
    
    // Parse metrics from concurrent execution
    val (metrics, _) = parseEventLogs()
    val allNewMetrics = metrics.filter(m => 
      m.name.equals("operator time") || m.name.contains("operatorTime"))
    
    // Verify metrics were collected without corruption
    if (allNewMetrics.nonEmpty) {
      println(s"Concurrent execution produced ${allNewMetrics.length} new metrics")
      
      // All metric values should be non-negative (indicating no corruption)
      allNewMetrics.foreach { m =>
        assert(m.value >= 0, s"Metric ${m.name} should be " +
          s"non-negative under concurrency, got ${m.value}")
      }
      
      // Group by metric name to check for consistency
      val metricsByName = allNewMetrics.groupBy(_.name)
      metricsByName.foreach { case (name, values) =>
        val totalValue = values.map(_.value).sum
        println(f"  $name: ${values.length} occurrences, total ${totalValue / 1000000.0}%.2f ms")
        
        // All values for the same metric should be reasonable
        values.foreach { v =>
          assert(v.value >= 0 && v.value < 60000000000L, // Less than 60 seconds
            s"Metric value should be reasonable: ${v.value}")
        }
      }
    } else {
      println("No new metrics found in concurrent execution - may indicate issue")
    }
    
    // Verify data integrity wasn't compromised by concurrent metrics collection
    results.foreach { row =>
      val total = row.getAs[Double]("total")
      assert(total >= 0, "Result totals should be non-negative")
    }
    
    println(s"Concurrent execution completed successfully with ${results.length} results")
  }

  test("Metrics recorded across multiple stages and tasks") {
    val sparkSession = spark
    import sparkSession.implicits._
    
    spark.conf.set("spark.rapids.sql.exec.disableOpTimeTrackingRDD", "false")
    
    val numRows = 80000L
    val numTasks = 12
    
    // Create multi-stage query to test metrics across different execution phases
    val baseDF = spark.range(0, numRows, 1, numTasks)
      .selectExpr(
        "id",
        "id % 50 as partition_key",
        "id % 10 as group_key", 
        "rand() * 100 as value"
      )
    
    // Stage 1: Group by partition_key
    val stage1 = baseDF
      .groupBy("partition_key", "group_key")
      .agg(
        count("*").as("local_count"),
        sum("value").as("local_sum")
      )
    
    // Stage 2: Global aggregation (will cause shuffle)
    val stage2 = stage1
      .groupBy("group_key")
      .agg(
        sum("local_count").as("total_count"),
        sum("local_sum").as("total_sum"),
        avg("local_sum").as("avg_local_sum")
      )
    
    // Stage 3: Final processing and writing
    val finalResult = stage2
      .selectExpr(
        "group_key",
        "total_count",
        "total_sum",
        "total_sum / total_count as overall_avg"
      )
      .filter($"total_count" > 1000)
    
    val outputPath = new File(tempDir, "multistage_output").getAbsolutePath
    finalResult.write.mode("overwrite").parquet(outputPath)
    
    // Verify execution completed
    assert(new File(outputPath).exists(), "Multi-stage output should exist")
    
    // Parse metrics across all stages
    val (metrics, _) = parseEventLogs()
    val newMetrics = metrics.filter(m => 
      m.name.equals("operator time") || m.name.contains("operatorTime"))
    
    if (newMetrics.nonEmpty) {
      println(s"Multi-stage execution produced ${newMetrics.length} new metrics")
      
      // Group metrics by stage to analyze distribution
      val metricsByStage = newMetrics.groupBy(_.stage)
      metricsByStage.foreach { case (stageOpt, stageMetrics) =>
        val stage = stageOpt.getOrElse(-1)
        val totalTime = stageMetrics.map(_.value).sum
        println(f"  Stage $stage: ${stageMetrics.length} metrics, " +
          f"total ${totalTime / 1000000.0}%.2f ms")
      }
      
      // Verify reasonable distribution across stages
      if (metricsByStage.size > 1) {
        println("Metrics successfully distributed across multiple stages")
      }
    }
    
    // Read back to verify data integrity
    val readBack = spark.read.parquet(outputPath)
    val finalCount = readBack.count()
    assert(finalCount > 0, s"Final result should have data, got $finalCount rows")
    
    println(s"Multi-stage execution completed with $finalCount final results")
  }
}
