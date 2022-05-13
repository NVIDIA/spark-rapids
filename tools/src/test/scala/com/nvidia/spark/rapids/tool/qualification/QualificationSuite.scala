/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
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

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.rapids.tool.{AppBase, AppFilterImpl, ToolUtils}
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
    .add("Unsupported Write Data Format", StringType, true)
    .add("Complex Types", StringType, true)
    .add("Nested Complex Types", StringType, true)
    .add("Longest SQL Duration", LongType, true)
    .add("NONSQL Task Duration Plus Overhead", LongType, true)
    .add("Estimated Duration", DoubleType, true)
    .add("Unsupported Duration", LongType, true)
    .add("Speedup Duration", LongType, true)
    .add("Speedup Factor", DoubleType, true)
    .add("Total Speedup", DoubleType, true)
    .add("Recommendation", StringType, true)

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
      assert(appSum.head.appId.equals("local-1623281204390"))

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
      assert(appSum.head.appId.equals("local-1623281204390"))

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.log"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 4))
        // skip the 3 header lines
        val firstRow = lines(3)
        assert(firstRow.contains("local-1623281204390"))
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

  test("test skip gpu event logs") {
    val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")
    val logFiles = Array(s"$qualLogDir/gpu_eventlog")
    var appSum: Seq[QualificationSummaryInfo] = Seq()
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      assert(appSum.size == 0)

      val filename = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output.csv"
      val inputSource = Source.fromFile(filename)
      try {
        val lines = inputSource.getLines.toSeq
        // 1 for header, Event log not parsed since it is from GPU run.
        assert(lines.size == 1)
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

  test("test appName filter") {
    val appName = "Spark shell"
    val appArgs = new QualificationArgs(Array(
      "--application-name",
      appName,
      s"$logDir/rdd_only_eventlog",
      s"$logDir/empty_eventlog",
      s"$logDir/udf_func_eventlog"
    ))

    val (eventLogInfo, _) = EventLogPathProcessor.processAllPaths(
      appArgs.filterCriteria.toOption, appArgs.matchEventLogs.toOption, appArgs.eventlog(),
      sparkSession.sparkContext.hadoopConfiguration)

    val appFilter = new AppFilterImpl(1000, sparkSession.sparkContext.hadoopConfiguration,
      Some(84000), 2)
    val result = appFilter.filterEventLogs(eventLogInfo, appArgs)
    assert(eventLogInfo.length == 3)
    assert(result.length == 2) // 2 out of 3 have "Spark shell" as appName.
  }

  test("test appName filter - Negation") {
    val appName = "~Spark shell"
    val appArgs = new QualificationArgs(Array(
      "--application-name",
      appName,
      s"$logDir/rdd_only_eventlog",
      s"$logDir/empty_eventlog",
      s"$logDir/udf_func_eventlog"
    ))

    val (eventLogInfo, _) = EventLogPathProcessor.processAllPaths(
      appArgs.filterCriteria.toOption, appArgs.matchEventLogs.toOption, appArgs.eventlog(),
      sparkSession.sparkContext.hadoopConfiguration)

    val appFilter = new AppFilterImpl(1000, sparkSession.sparkContext.hadoopConfiguration,
      Some(84000), 2)
    val result = appFilter.filterEventLogs(eventLogInfo, appArgs)
    assert(eventLogInfo.length == 3)
    assert(result.length == 1) // 1 out of 3 does not has "Spark shell" as appName.
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

  test("test event log write format") {
    val logFiles = Array(s"$logDir/writeformat_eventlog")
    runQualificationTest(logFiles, "write_format_expectation.csv")
  }

  test("test event log nested types in ReadSchema") {
    val logFiles = Array(s"$logDir/nested_type_eventlog")
    runQualificationTest(logFiles, "nested_type_expectation.csv")
  }

  // this tests parseReadSchema by passing different schemas as strings. Schemas
  // with complex types, complex nested types, decimals and simple types
  test("test different types in ReadSchema") {
    val testSchemas: ArrayBuffer[ArrayBuffer[String]] = ArrayBuffer(
      ArrayBuffer(""),
      ArrayBuffer("firstName:string,lastName:string", "", "address:string"),
      ArrayBuffer("properties:map<string,string>"),
      ArrayBuffer("name:array<string>"),
      ArrayBuffer("name:string,booksInterested:array<struct<name:string,price:decimal(8,2)," +
          "author:string,pages:int>>,authbook:array<map<name:string,author:string>>, " +
          "pages:array<array<struct<name:string,pages:int>>>,name:string,subject:string"),
      ArrayBuffer("name:struct<fn:string,mn:array<string>,ln:string>," +
          "add:struct<cur:struct<st:string,city:string>," +
          "previous:struct<st:map<string,string>,city:string>>," +
          "next:struct<fn:string,ln:string>"),
      ArrayBuffer("name:map<id:int,map<fn:string,ln:string>>, " +
          "address:map<id:int,struct<st:string,city:string>>," +
          "orders:map<id:int,order:array<map<oname:string,oid:int>>>," +
          "status:map<name:string,active:string>")
    )

    var index = 0
    val expectedResult = List(
      ("", ""),
      ("", ""),
      ("map<string;string>", ""),
      ("array<string>", ""),
      ("array<struct<name:string;price:decimal(8;2);author:string;pages:int>>;" +
          "array<map<name:string;author:string>>;array<array<struct<name:string;pages:int>>>",
          "array<struct<name:string;price:decimal(8;2);author:string;pages:int>>;" +
              "array<map<name:string;author:string>>;array<array<struct<name:string;pages:int>>>"),
      ("struct<fn:string;mn:array<string>;ln:string>;" +
          "struct<cur:struct<st:string;city:string>;previous:struct<st:map<string;string>;" +
          "city:string>>;struct<fn:string;ln:string>",
          "struct<fn:string;mn:array<string>;ln:string>;" +
              "struct<cur:struct<st:string;city:string>;previous:struct<st:map<string;string>;" +
              "city:string>>"),
      ("map<id:int;map<fn:string;ln:string>>;map<id:int;struct<st:string;city:string>>;" +
          "map<id:int;order:array<map<oname:string;oid:int>>>;map<name:string;active:string>",
          "map<id:int;map<fn:string;ln:string>>;map<id:int;struct<st:string;city:string>>;" +
              "map<id:int;order:array<map<oname:string;oid:int>>>"))

    val result = testSchemas.map(x => AppBase.parseReadSchemaForNestedTypes(x))
    result.foreach { actualResult =>
      assert(actualResult._1.equals(expectedResult(index)._1))
      assert(actualResult._2.equals(expectedResult(index)._2))
      index += 1
    }
  }

  test("test jdbc problematic") {
    val logFiles = Array(s"$logDir/jdbc_eventlog.zstd")
    runQualificationTest(logFiles, "jdbc_expectation.csv")
  }

  private def createDecFile(spark: SparkSession, dir: String): Unit = {
    import spark.implicits._
    val dfGen = Seq("1.32").toDF("value")
      .selectExpr("CAST(value AS DECIMAL(4, 2)) AS value")
    dfGen.write.parquet(dir)
  }

  test("test generate udf same") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/decparquet"
        createDecFile(sparkSession, tmpParquet)

        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          val plusOne = udf((x: Int) => x + 1)
          import spark.implicits._
          spark.udf.register("plusOne", plusOne)
          val df = spark.read.parquet(tmpParquet)
          val df2 = df.withColumn("mult", $"value" * $"value")
          val df4 = df2.withColumn("udfcol", plusOne($"value"))
          df4
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val probApp = appSum.head
        assert(probApp.potentialProblems.contains("UDF"))
        assert(probApp.sqlDataFrameDuration == probApp.sqlDurationForProblematic)
      }
    }
  }

  test("test generate udf different sql ops") {
    TrampolineUtil.withTempDir { outpath =>

      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/decparquet"
        createDecFile(sparkSession, tmpParquet)

        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          val plusOne = udf((x: Int) => x + 1)
          import spark.implicits._
          spark.udf.register("plusOne", plusOne)
          val df = spark.read.parquet(tmpParquet)
          val df2 = df.withColumn("mult", $"value" * $"value")
          // first run sql op with decimal only
          df2.collect()
          // run a separate sql op using just udf
          spark.sql("SELECT plusOne(5)").collect()
          // Then run another sql op that doesn't use with decimal or udf
          import spark.implicits._
          val t1 = Seq((1, 2), (3, 4)).toDF("a", "b")
          t1.createOrReplaceTempView("t1")
          spark.sql("SELECT a, MAX(b) FROM t1 GROUP BY a ORDER BY a")
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val probApp = appSum.head
        assert(probApp.potentialProblems.contains("UDF"))
        assert(probApp.sqlDurationForProblematic > 0)
        assert(probApp.sqlDataFrameDuration > probApp.sqlDurationForProblematic)
      }
    }
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

  test("test dsv1 complex") {
    val logFiles = Array(s"$logDir/complex_dec_eventlog.zstd")
    runQualificationTest(logFiles, "complex_dec_expectation.csv")
  }

  test("test dsv2 nested complex") {
    val logFiles = Array(s"$logDir/eventlog_nested_dsv2")
    runQualificationTest(logFiles, "nested_dsv2_expectation.csv")
  }

  test("sql metric agg") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val listener = new ToolTestListener
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "sqlmetric") { spark =>
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

  test("running qualification app join") {
    val qualApp = new RunningQualificationApp()
    ToolTestUtils.runAndCollect("streaming") { spark =>
      val listener = qualApp.getEventListener
      spark.sparkContext.addSparkListener(listener)
      import spark.implicits._
      val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
      testData.createOrReplaceTempView("t1")
      testData.createOrReplaceTempView("t2")
      spark.sql("SELECT a, MAX(b) FROM (SELECT t1.a, t2.b " +
        "FROM t1 JOIN t2 ON t1.a = t2.a) AS t " +
        "GROUP BY a ORDER BY a")
    }
    val sumOut = qualApp.getSummary()
    val detailedOut = qualApp.getDetailed()
    assert(sumOut.nonEmpty)
    assert(sumOut.startsWith("|") && sumOut.endsWith("|\n"))
    assert(detailedOut.nonEmpty)
    assert(detailedOut.startsWith("|") && detailedOut.endsWith("|\n"))

    val csvSumOut = qualApp.getSummary(",", false)
    val rowsSumOut = csvSumOut.split("\n")
    assert(rowsSumOut.size == 2)
    val headers = rowsSumOut(0).split(",")
    val values = rowsSumOut(1).split(",")
    assert(headers.size == QualOutputWriter.getSummaryHeaderStringsAndSizes(0).keys.size)
    assert(values.size == headers.size)
    // 2 should be the SQL DF Duration
    assert(headers(2).contains("SQL DF"))
    assert(values(2).toInt > 0)
    val csvDetailedOut = qualApp.getDetailed(",", false)
    val rowsDetailedOut = csvDetailedOut.split("\n")
    assert(rowsDetailedOut.size == 2)
    val headersDetailed = rowsDetailedOut(0).split(",")
    val valuesDetailed = rowsDetailedOut(1).split(",")
    assert(headersDetailed.size == QualOutputWriter
      .getDetailedHeaderStringsAndSizes(Seq(qualApp.aggregateStats().get._1), false).keys.size)
    assert(valuesDetailed.size == headersDetailed.size)
    // 2 should be the Score
    assert(headersDetailed(2).contains("Score"))
    assert(valuesDetailed(2).toDouble > 0)
  }

  test("running qualification app files") {
    TrampolineUtil.withTempPath { outParquetFile =>
      TrampolineUtil.withTempPath { outJsonFile =>

        val qualApp = new RunningQualificationApp()
        ToolTestUtils.runAndCollect("streaming") { spark =>
          val listener = qualApp.getEventListener
          spark.sparkContext.addSparkListener(listener)
          import spark.implicits._
          val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
          testData.write.json(outJsonFile.getCanonicalPath)
          testData.write.parquet(outParquetFile.getCanonicalPath)
          val df = spark.read.parquet(outParquetFile.getCanonicalPath)
          val df2 = spark.read.json(outJsonFile.getCanonicalPath)
          df.join(df2.select($"a" as "a2"), $"a" === $"a2")
        }
        // test different delimiter
        val sumOut = qualApp.getSummary(":", false)
        val rowsSumOut = sumOut.split("\n")
        assert(rowsSumOut.size == 2)
        val headers = rowsSumOut(0).split(":")
        val values = rowsSumOut(1).split(":")
        assert(headers.size == QualOutputWriter.getSummaryHeaderStringsAndSizes(0).keys.size)
        assert(values.size == headers.size)
        // 2 should be the SQL DF Duration
        assert(headers(2).contains("SQL DF"))
        assert(values(2).toInt > 0)
        val detailedOut = qualApp.getDetailed(":", prettyPrint = false, reportReadSchema = true)
        val rowsDetailedOut = detailedOut.split("\n")
        assert(rowsDetailedOut.size == 2)
        val headersDetailed = rowsDetailedOut(0).split(":")
        val valuesDetailed = rowsDetailedOut(1).split(":")
        // Read File Format Score
        assert(headersDetailed(12).contains("Read File Format Score"))
        assert(valuesDetailed(12).toDouble == 50.0)
        assert(headersDetailed(13).contains("Read File Formats"))
        assert(valuesDetailed(13).contains("JSON"))
        val readSchemaIndex = headersDetailed.length - 1
        assert(headersDetailed(readSchemaIndex).contains("Read Schema"))
        assert(
          valuesDetailed(readSchemaIndex).contains("json") &&
            valuesDetailed(readSchemaIndex).contains("parquet"))
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
