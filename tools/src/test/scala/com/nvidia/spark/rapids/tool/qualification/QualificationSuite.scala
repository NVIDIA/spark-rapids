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

import scala.collection.mutable.ListBuffer
import scala.io.Source

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.ToolUtils
import org.apache.spark.sql.rapids.tool.qualification.QualificationSummaryInfo
import org.apache.spark.sql.types._

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

  val schema = new StructType()
    .add("App Name", StringType, true)
    .add("App ID", StringType, true)
    .add("Score", DoubleType, true)
    .add("Potential Problems", StringType, true)
    .add("SQL Dataframe Duration", LongType, true)
    .add("SQL Dataframe Task Duration", LongType, true)
    .add("App Duration", LongType, true)
    .add("Executor CPU Time Percent", DoubleType, true)
    .add("App Duration Estimated", BooleanType, true)
    .add("SQL Duration with Potential Problems", LongType, true)
    .add("SQL Ids with Failures", StringType, true)
    .add("Read Score Percent", IntegerType, true)
    .add("Read File Format Score", DoubleType, true)
    .add("Unsupported Read File Formats and Types", StringType, true)

  def readExpectedFile(expected: File): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath(),
      Some(schema))
  }

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String,
      shouldReturnEmpty: Boolean = false) = {
    TrampolineUtil.withTempDir { outpath =>
      val resultExpectation = new File(expRoot, expectFileName)
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())

      val appArgs = new QualificationArgs(allArgs ++ eventLogs)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      val spark2 = sparkSession
      import spark2.implicits._
      val dfTmp = appSum.toDF.drop("readFileFormats")
      val dfQual = sparkSession.createDataFrame(dfTmp.rdd, schema)
      if (shouldReturnEmpty) {
        assert(appSum.head.sqlDataFrameDuration == 0.0)
      } else {
        val dfExpect = readExpectedFile(resultExpectation)
        assert(!dfQual.isEmpty)
        ToolTestUtils.compareDataFrames(dfQual, dfExpect)
      }
    }
  }

  test("test order asc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "asc")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 4)
      assert(appSum.head.appId.equals("local-1622043423018"))

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 4))
        // skip the 3 header lines
        val firstRow = lines(3)
        assert(firstRow.contains("local-1621955976602"))
      } finally {
        inputSource.close()
      }
    }
  }

  test("test order desc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "desc")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 4)
      assert(appSum.head.appId.equals("local-1622043423018"))

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 4))
        // skip the 3 header lines
        val firstRow = lines(3)
        assert(firstRow.contains("local-1622043423018"))
      } finally {
        inputSource.close()
      }
    }
  }

  test("test limit desc") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    var appSum: Seq[QualificationSummaryInfo] = Seq()
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "desc",
        "-n",
        "2")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, sum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines
        // 4 lines of header and footer, limit is 2
        assert(lines.size == (4 + 2))
      } finally {
        inputSource.close()
      }
    }
  }

  test("test datasource read format included") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv1.zstd")
    var appSum: Seq[QualificationSummaryInfo] = Seq()
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--report-read-schema")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, sum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.csv"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toSeq
        // 1 for header, 1 for values
        assert(lines.size == 2)
        assert(lines.head.contains("Read Schema"))
        assert(lines(1).contains("loan399"))
      } finally {
        inputSource.close()
      }
    }
  }

  test("skip malformed json eventlog") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val badEventLog = s"$profileLogDir/malformed_json_eventlog.zstd"
    val logFiles = Array(s"$logDir/nds_q86_test", badEventLog)
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv")
  }

  test("spark2 eventlog") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val log = s"$profileLogDir/spark2-eventlog.zstd"
    runQualificationTest(Array(log), "spark2_expectation.csv")
  }

  test("test udf event logs") {
    val logFiles = Array(
      s"$logDir/dataset_eventlog",
      s"$logDir/dsAndDf_eventlog.zstd",
      s"$logDir/udf_dataset_eventlog",
      s"$logDir/udf_func_eventlog"
    )
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv")
  }

  test("test missing sql end") {
    val logFiles = Array(s"$logDir/join_missing_sql_end")
    runQualificationTest(logFiles, "qual_test_missing_sql_end_expectation.csv")
  }

  test("test eventlog with no jobs") {
    val logFiles = Array(s"$logDir/empty_eventlog")
    runQualificationTest(logFiles, "", shouldReturnEmpty=true)
  }

  test("test eventlog with rdd only jobs") {
    val logFiles = Array(s"$logDir/rdd_only_eventlog")
    runQualificationTest(logFiles, "", shouldReturnEmpty=true)
  }

  test("test truncated log file 1") {
    val logFiles = Array(s"$logDir/truncated_eventlog")
    runQualificationTest(logFiles, "truncated_1_end_expectation.csv")
  }

  test("test nds q86 test") {
    val logFiles = Array(s"$logDir/nds_q86_test")
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv")
  }

  // event log rolling creates files under a directory
  test("test event log rolling") {
    val logFiles = Array(s"$logDir/eventlog_v2_local-1623876083964")
    runQualificationTest(logFiles, "directory_test_expectation.csv")
  }

  // these test files were setup to simulator the directory structure
  // when running on Databricks and are not really from there
  test("test db event log rolling") {
    val logFiles = Array(s"$logDir/db_sim_eventlog")
    runQualificationTest(logFiles, "db_sim_test_expectation.csv")
  }

  test("test nds q86 with failure test") {
    val logFiles = Array(s"$logDir/nds_q86_fail_test")
    runQualificationTest(logFiles, "nds_q86_fail_test_expectation.csv")
  }

  test("test read datasource v1") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv1.zstd")
    runQualificationTest(logFiles, "read_dsv1_expectation.csv")
  }

  test("test read datasource v2") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv2.zstd")
    runQualificationTest(logFiles, "read_dsv2_expectation.csv")
  }

  test("test dsv1 complex and decimal") {
    val logFiles = Array(s"$logDir/complex_dec_eventlog.zstd")
    runQualificationTest(logFiles, "complex_dec_expectation.csv")
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
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo) =
          QualificationMain.mainInternal(appArgs)
        assert(exit == 0)

        // parse results from listener
        val executorCpuTime = listener.executorCpuTime
        val executorRunTime = listener.completedStages
          .map(_.stageInfo.taskMetrics.executorRunTime).sum

        val listenerCpuTimePercent =
          ToolUtils.calculateDurationPercent(executorCpuTime, executorRunTime)

        // compare metrics from event log with metrics from listener
        assert(sumInfo.head.executorCpuTimePercent === listenerCpuTimePercent)
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
