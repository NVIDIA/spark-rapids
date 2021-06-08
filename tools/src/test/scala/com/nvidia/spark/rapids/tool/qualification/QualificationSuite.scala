/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.tool.qualification

import java.io.File
import java.util.concurrent.TimeUnit.NANOSECONDS

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{SparkSession, TrampolineUtil}

class QualificationSuite extends FunSuite with BeforeAndAfterEach with Logging {

  private var sparkSession: SparkSession = _

  private val expRoot = ToolTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String) = {
    Seq(true, false).foreach { hasExecCpu =>
      TrampolineUtil.withTempDir { outpath =>
        val resultExpectation = new File(expRoot, expectFileName)
        val outputArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val allArgs = hasExecCpu match  {
          case true => outputArgs
          case false => outputArgs ++ Array("--no-exec-cpu-percent")
        }
        val appArgs = new QualificationArgs(allArgs ++ eventLogs)

        val (exit, dfQualOpt) =
          QualificationMain.mainInternal(sparkSession, appArgs, writeOutput=false,
            dropTempViews=true)
        assert(exit == 0)
        val dfExpectOrig =
          ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
        val dfExpect = if (hasExecCpu) dfExpectOrig else dfExpectOrig.drop("executorCPURatio")
        assert(dfQualOpt.isDefined)
        ToolTestUtils.compareDataFrames(dfQualOpt.get, dfExpect)
      }
    }
  }

  test("skip malformed json eventlog") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val badEventLog = s"$profileLogDir/malformed_json_eventlog"
    val logFiles = Array(s"$logDir/nds_q86_test", badEventLog)
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv")
  }

  test("test udf event logs") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv")
  }

  test("test missing sql end") {
    val logFiles = Array(s"$logDir/join_missing_sql_end")
    runQualificationTest(logFiles, "qual_test_missing_sql_end_expectation.csv")
  }

  test("test truncated log file 1") {
    val logFiles = Array(s"$logDir/truncated_eventlog")
    runQualificationTest(logFiles, "truncated_1_end_expectation.csv")
  }

  test("test nds q86 test") {
    val logFiles = Array(s"$logDir/nds_q86_test")
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv")
  }

  test("sql metric agg") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val listener = new ToolTestListener
      val eventLog = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
        spark.sparkContext.addSparkListener(listener)
        import spark.implicits._
        val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
        testData.createOrReplaceTempView("t1")
        testData.createOrReplaceTempView("t2")
        spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
          "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
          "GROUP BY a ORDER BY a")
      }
      assert(listener.completedStages.length == 5)

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>

        // create new session for tool to use
        val spark2 = SparkSession
          .builder()
          .master("local[*]")
          .appName("Rapids Spark Profiling Tool Unit Tests")
          .getOrCreate()

        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, _) =
          QualificationMain.mainInternal(spark2, appArgs, writeOutput = false,
            dropTempViews = false)
        assert(exit == 0)

        val df = spark2.table("sqlAggMetricsDF")

        def fieldIndex(name: String) = df.schema.fieldIndex(name)

        val rows = df.collect()
        assert(rows.length === 1)
        val collect = rows.head
        assert(collect.getString(fieldIndex("description")).startsWith("collect"))

        // parse results from listener
        val executorCpuTime = listener.executorCpuTime
        val executorRunTime = listener.completedStages
          .map(_.stageInfo.taskMetrics.executorRunTime).sum

        // compare metrics from event log with metrics from listener
        assert(collect.getLong(fieldIndex("executorCPUTime")) === executorCpuTime)
        assert(collect.getLong(fieldIndex("executorRunTime")) === executorRunTime)
      }
    }
  }
}

class ToolTestListener extends SparkListener {
  val completedStages = new ListBuffer[SparkListenerStageCompleted]()
  var executorCpuTime = 0L

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    executorCpuTime += NANOSECONDS.toMillis(taskEnd.taskMetrics.executorCpuTime)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    completedStages.append(stageCompleted)
  }
}
