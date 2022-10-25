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
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerStageCompleted, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, TrampolineUtil}
import org.apache.spark.sql.functions.{desc, hex, udf}
import org.apache.spark.sql.rapids.tool.{AppBase, AppFilterImpl, ToolUtils}
import org.apache.spark.sql.rapids.tool.qualification.{QualificationAppInfo, QualificationSummaryInfo, RunningQualificationEventProcessor}
import org.apache.spark.sql.types._

// drop the fields that won't go to DataFrame without encoders
case class TestQualificationSummary(
    appName: String,
    appId: String,
    recommendation: String,
    estimatedGpuSpeedup: Double,
    estimatedGpuDur: Double,
    estimatedGpuTimeSaved: Double,
    sqlDataframeDuration: Long,
    sqlDataframeTaskDuration: Long,
    appDuration: Long,
    gpuOpportunity: Long,
    executorCpuTimePercent: Double,
    failedSQLIds: String,
    readFileFormatAndTypesNotSupported: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    potentialProblems: String,
    longestSqlDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    unsupportedSQLTaskDuration: Long,
    supportedSQLTaskDuration: Long,
    taskSpeedupFactor: Double,
    endDurationEstimated: Boolean)

class QualificationSuite extends FunSuite with BeforeAndAfterEach with Logging {

  private var sparkSession: SparkSession = _

  private val expRoot = ToolTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  private val csvDetailedFields = Seq(
    ("App Name", StringType),
    ("App ID", StringType),
    ("Recommendation", StringType),
    ("Estimated GPU Speedup", DoubleType),
    ("Estimated GPU Duration", DoubleType),
    ("Estimated GPU Time Saved", DoubleType),
    ("SQL DF Duration", LongType),
    ("SQL Dataframe Task Duration", LongType),
    ("App Duration", LongType),
    ("GPU Opportunity", LongType),
    ("Executor CPU Time Percent", DoubleType),
    ("SQL Ids with Failures", StringType),
    ("Unsupported Read File Formats and Types", StringType),
    ("Unsupported Write Data Format", StringType),
    ("Complex Types", StringType),
    ("Nested Complex Types", StringType),
    ("Potential Problems", StringType),
    ("Longest SQL Duration", LongType),
    ("NONSQL Task Duration Plus Overhead", LongType),
    ("Unsupported Task Duration", LongType),
    ("Supported SQL DF Task Duration", LongType),
    ("Task Speedup Factor", DoubleType),
    ("App Duration Estimated", BooleanType))

  private val csvPerSQLFields = Seq(
    ("App Name", StringType),
    ("App ID", StringType),
    ("SQL ID", StringType),
    ("SQL Description", StringType),
    ("SQL DF Duration", LongType),
    ("GPU Opportunity", LongType),
    ("Estimated GPU Duration", DoubleType),
    ("Estimated GPU Speedup", DoubleType),
    ("Estimated GPU Time Saved", DoubleType),
    ("Recommendation", StringType))

  val schema = new StructType(csvDetailedFields.map(f => StructField(f._1, f._2, true)).toArray)
  val perSQLSchema = new StructType(csvPerSQLFields.map(f => StructField(f._1, f._2, true)).toArray)

  def csvDetailedHeader(ind: Int) = csvDetailedFields(ind)._1

  private def createSparkSession(): Unit = {
    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Rapids Spark Profiling Tool Unit Tests")
      .getOrCreate()
  }

  override protected def beforeEach(): Unit = {
    TrampolineUtil.cleanupAnyExistingSession()
    createSparkSession()
  }

  def readExpectedFile(expected: File): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath(),
      Some(schema))
  }

  def readPerSqlFile(expected: File): DataFrame = {
    ToolTestUtils.readExpectationCSV(sparkSession, expected.getPath(),
      Some(perSQLSchema))
  }

  def readPerSqlTextFile(expected: File): Dataset[String] = {
    sparkSession.read.textFile(expected.getPath())
  }

  private def createSummaryForDF(
      appSums: Seq[QualificationSummaryInfo]): Seq[TestQualificationSummary] = {
    appSums.map { appInfoRec =>
      val sum = QualOutputWriter.createFormattedQualSummaryInfo(appInfoRec, ",")
      TestQualificationSummary(sum.appName, sum.appId, sum.recommendation,
        sum.estimatedGpuSpeedup, sum.estimatedGpuDur,
        sum.estimatedGpuTimeSaved, sum.sqlDataframeDuration,
        sum.sqlDataframeTaskDuration, sum.appDuration,
        sum.gpuOpportunity, sum.executorCpuTimePercent, sum.failedSQLIds,
        sum.readFileFormatAndTypesNotSupported, sum.writeDataFormat,
        sum.complexTypes, sum.nestedComplexTypes, sum.potentialProblems, sum.longestSqlDuration,
        sum.nonSqlTaskDurationAndOverhead,
        sum.unsupportedSQLTaskDuration, sum.supportedSQLTaskDuration, sum.taskSpeedupFactor,
        sum.endDurationEstimated)
    }
  }

  private def runQualificationTest(eventLogs: Array[String], expectFileName: String,
      shouldReturnEmpty: Boolean = false, expectPerSqlFileName: Option[String] = None) = {
    TrampolineUtil.withTempDir { outpath =>
      val resultExpectation = new File(expRoot, expectFileName)
      val outputArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath())

      val allArgs = if (expectPerSqlFileName.isDefined) {
        outputArgs ++ Array("--per-sql")
      } else {
        outputArgs
      }

      val appArgs = new QualificationArgs(allArgs ++ eventLogs)
      val (exit, appSum) = QualificationMain.mainInternal(appArgs)
      assert(exit == 0)
      val spark2 = sparkSession
      import spark2.implicits._
      val summaryDF = createSummaryForDF(appSum).toDF
      val dfQual = sparkSession.createDataFrame(summaryDF.rdd, schema)
      if (shouldReturnEmpty) {
        assert(appSum.head.estimatedInfo.sqlDfDuration == 0.0)
      } else {
        val dfExpect = readExpectedFile(resultExpectation)
        assert(!dfQual.isEmpty)
        ToolTestUtils.compareDataFrames(dfQual, dfExpect)
        if (expectPerSqlFileName.isDefined) {
          val resultExpectation = new File(expRoot, expectPerSqlFileName.get)
          val dfPerSqlExpect = readPerSqlFile(resultExpectation)
          val actualExpectation = s"$outpath/rapids_4_spark_qualification_output/" +
            s"rapids_4_spark_qualification_output_persql.csv"
          val dfPerSqlActual = readPerSqlFile(new File(actualExpectation))
          ToolTestUtils.compareDataFrames(dfPerSqlActual, dfPerSqlExpect)
        }
      }
    }
  }

  test("RunningQualificationEventProcessor per sql") {
    TrampolineUtil.withTempDir { qualOutDir =>
      TrampolineUtil.withTempPath { outParquetFile =>
        TrampolineUtil.withTempPath { outJsonFile =>
          // note don't close the application here so we test running output
          ToolTestUtils.runAndCollect("running per sql") { spark =>
            val sparkConf = spark.sparkContext.getConf
            sparkConf.set("spark.rapids.qualification.output.numSQLQueriesPerFile", "2")
            sparkConf.set("spark.rapids.qualification.output.maxNumFiles", "3")
            sparkConf.set("spark.rapids.qualification.outputDir", qualOutDir.getPath)
            val listener = new RunningQualificationEventProcessor(sparkConf)
            spark.sparkContext.addSparkListener(listener)
            import spark.implicits._
            val testData = Seq((1, 2), (3, 4)).toDF("a", "b")
            testData.write.json(outJsonFile.getCanonicalPath)
            testData.write.parquet(outParquetFile.getCanonicalPath)
            val df = spark.read.parquet(outParquetFile.getCanonicalPath)
            val df2 = spark.read.json(outJsonFile.getCanonicalPath)
            // generate a bunch of SQL queries to test the file rolling, should run
            // 10 sql queries total with above and below
            for (i <- 1 to 7) {
              df.join(df2.select($"a" as "a2"), $"a" === $"a2").count()
            }
            val df3 = df.join(df2.select($"a" as "a2"), $"a" === $"a2")
            df3
          }
          // the code above that runs the Spark query stops the Sparksession
          // so create a new one to read in the csv file
          createSparkSession()
          val outputDir = qualOutDir.getPath + "/"
          val csvOutput0 = outputDir + QualOutputWriter.LOGFILE_NAME + "_persql_0.csv"
          val txtOutput0 = outputDir + QualOutputWriter.LOGFILE_NAME + "_persql_0.log"
          // check that there are 6 files since configured for 3 and have 1 csv and 1 log
          // file each
          val outputDirPath = new Path(outputDir)
          val fs = FileSystem.get(outputDirPath.toUri,
            sparkSession.sparkContext.hadoopConfiguration)
          val allFiles = fs.listStatus(outputDirPath)
          assert(allFiles.size == 6)
          val dfPerSqlActual = readPerSqlFile(new File(csvOutput0))
          assert(dfPerSqlActual.columns.size == 10)
          val rows = dfPerSqlActual.collect()
          assert(rows.size == 2)
          val firstRow = rows(1)
          // , should be replaced with ;
          assert(firstRow(3).toString.contains("at QualificationSuite.scala"))

          // this reads everything into single column
          val dfPerSqlActualTxt = readPerSqlTextFile(new File(txtOutput0))
          assert(dfPerSqlActualTxt.columns.size == 1)
          val rowsTxt = dfPerSqlActualTxt.collect()
          // have to account for headers
          assert(rowsTxt.size == 6)
          val headerRowTxt = rowsTxt(1).toString
          assert(headerRowTxt.contains("Recommendation"))
          val firstValueRow = rowsTxt(3).toString
          assert(firstValueRow.contains("QualificationSuite.scala"))
        }
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
        assert(firstRow.contains("local-1623281204390"))
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
        "desc",
        "--per-sql")

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
      val persqlFileName = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output_persql.log"
      val persqlInputSource = Source.fromFile(persqlFileName)
      try {
        val lines = persqlInputSource.getLines.toArray
        // 4 lines of header and footer
        assert(lines.size == (4 + 17))
        // skip the 3 header lines
        val firstRow = lines(3)
        // this should be app + sqlID
        assert(firstRow.contains("local-1622043423018|     1"))
        assert(firstRow.contains("count at QualificationInfoUtils.scala:94"))
      } finally {
        persqlInputSource.close()
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
    TrampolineUtil.withTempDir { outpath =>
      val allArgs = Array(
        "--output-directory",
        outpath.getAbsolutePath(),
        "--order",
        "desc",
        "-n",
        "2",
        "--per-sql")

      val appArgs = new QualificationArgs(allArgs ++ logFiles)
      val (exit, _) = QualificationMain.mainInternal(appArgs)
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
      val persqlFileName = s"$outpath/rapids_4_spark_qualification_output/" +
        s"rapids_4_spark_qualification_output_persql.log"
      val persqlInputSource = Source.fromFile(persqlFileName)
      try {
        val lines = persqlInputSource.getLines
        // 4 lines of header and footer, limit is 2
        assert(lines.size == (4 + 2))
      } finally {
        persqlInputSource.close()
      }
    }
  }

  test("test datasource read format included") {
    val profileLogDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
    val logFiles = Array(s"$profileLogDir/eventlog_dsv1.zstd")
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
    runQualificationTest(logFiles, "qual_test_simple_expectation.csv",
      expectPerSqlFileName = Some("qual_test_simple_expectation_persql.csv"))
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
    runQualificationTest(logFiles, "nds_q86_test_expectation.csv",
      expectPerSqlFileName = Some("nds_q86_test_expectation_persql.csv"))
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
    runQualificationTest(logFiles, "nds_q86_fail_test_expectation.csv",
      expectPerSqlFileName = Some("nds_q86_fail_test_expectation_persql.csv"))
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
      assert(ToolUtils.formatComplexTypes(actualResult._1).equals(expectedResult(index)._1))
      assert(ToolUtils.formatComplexTypes(actualResult._2).equals(expectedResult(index)._2))
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

  private def createIntFile(spark:SparkSession, dir:String): Unit = {
    import spark.implicits._
    val t1 = Seq((1, 2), (3, 4), (1, 6)).toDF("a", "b")
    t1.write.parquet(dir)
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
        assert(probApp.unsupportedSQLTaskDuration > 0) // only UDF is unsupported in the query.
      }
    }
  }

  test("test with stage reuse") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>
        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "dot") { spark =>
          import spark.implicits._
          val df = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          val df2 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          val j1 = df.select( $"value" as "a")
            .join(df2.select($"value" as "b"), $"a" === $"b").cache()
          j1.count()
          j1.union(j1).count()
          // count above is important thing, here we just make up small df to return
          spark.sparkContext.makeRDD(1 to 2).toDF
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        // note this would have failed an assert with total task time to small if we
        // didn't dedup stages
      }
    }
  }

  test("test generate udf different sql ops") {
    TrampolineUtil.withTempDir { outpath =>

      TrampolineUtil.withTempDir { eventLogDir =>
        val tmpParquet = s"$outpath/decparquet"
        val grpParquet = s"$outpath/grpParquet"
        createDecFile(sparkSession, tmpParquet)
        createIntFile(sparkSession, grpParquet)

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
          val t2 = spark.read.parquet(grpParquet)
          val res = t2.groupBy("a").max("b").orderBy(desc("a"))
          res
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
        assert(probApp.unsupportedSQLTaskDuration > 0) // only UDF is unsupported in the query.
      }
    }
  }

  test("test clusterTags configs ") {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { eventLogDir =>

        val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "clustertags") { spark =>
          import spark.implicits._
          spark.conf.set("spark.databricks.clusterUsageTags.clusterAllTags",
            """[{"key":"Vendor",
              |"value":"Databricks"},{"key":"Creator","value":"abc@company.com"},
              |{"key":"ClusterName","value":"job-215-run-1"},{"key":"ClusterId",
              |"value":"0617-131246-dray530"},{"key":"JobId","value":"215"},
              |{"key":"RunName","value":"test73longer"},{"key":"DatabricksEnvironment",
              |"value":"workerenv-7026851462233806"}]""".stripMargin)

          val df1 = spark.sparkContext.makeRDD(1 to 1000, 6).toDF
          df1.sample(0.1)
        }
        val expectedClusterId = "0617-131246-dray530"
        val expectedJobId = "215"
        val expectedRunName = "test73longer"

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath())
        val appArgs = new QualificationArgs(allArgs ++ Array(eventLog))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == 1)
        val allTags = appSum.flatMap(_.allClusterTagsMap).toMap
        assert(allTags("ClusterId") == expectedClusterId)
        assert(allTags("JobId") == expectedJobId)
        assert(allTags("RunName") == expectedRunName)
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
        spark.sparkContext.setJobDescription("testing, csv delimiter; replacement")
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
          "--per-sql",
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo) =
          QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val persqlResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output_persql.csv"
        val dfPerSqlActual = readPerSqlFile(new File(persqlResults))
        // the number of columns actually won't be wrong if sql description is malformatted
        // because spark seems to drop extra column so need more checking
        assert(dfPerSqlActual.columns.size == 10)
        val rows = dfPerSqlActual.collect()
        assert(rows.size == 3)
        val firstRow = rows(1)
        // , should be replaced with ;
        assert(firstRow(3) == "testing; csv delimiter; replacement")

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

  test("running qualification print unsupported Execs and Exprs") {
    val qualApp = new RunningQualificationApp()
    ToolTestUtils.runAndCollect("streaming") { spark =>
      val listener = qualApp.getEventListener
      spark.sparkContext.addSparkListener(listener)
      import spark.implicits._
      val df1 = spark.sparkContext.parallelize(List(10, 20, 30, 40)).toDF
      df1.filter(hex($"value") === "A") // hex is not supported in GPU yet.
    }
    //stdout output tests
    val sumOut = qualApp.getSummary()
    val detailedOut = qualApp.getDetailed()
    assert(sumOut.nonEmpty)
    assert(sumOut.startsWith("|") && sumOut.endsWith("|\n"))
    assert(detailedOut.nonEmpty)
    assert(detailedOut.startsWith("|") && detailedOut.endsWith("|\n"))
    val stdOut = sumOut.split("\n")
    val stdOutHeader = stdOut(0).split("\\|")
    val stdOutValues = stdOut(1).split("\\|")
    val stdOutunsupportedExecs = stdOutValues(stdOutValues.length - 2) // index of unsupportedExecs
    val stdOutunsupportedExprs = stdOutValues(stdOutValues.length - 1) // index of unsupportedExprs
    val expectedstdOutExecs = "Scan;Filter;SerializeF..."
    assert(stdOutunsupportedExecs == expectedstdOutExecs)
    // Exec value is Scan;Filter;SerializeFromObject and UNSUPPORTED_EXECS_MAX_SIZE is 25
    val expectedStdOutExecsMaxLength = 25
    // Expr value is hex and length of expr header is 23 (Unsupported Expressions)
    val expectedStdOutExprsMaxLength = 23
    assert(stdOutunsupportedExecs.size == expectedStdOutExecsMaxLength)
    assert(stdOutunsupportedExprs.size == expectedStdOutExprsMaxLength)

    //csv output tests
    val csvSumOut = qualApp.getSummary(",", false)
    val rowsSumOut = csvSumOut.split("\n")
    val headers = rowsSumOut(0).split(",")
    val values = rowsSumOut(1).split(",")
    val expectedExecs = "Scan;Filter;SerializeFromObject" // Unsupported Execs
    val expectedExprs = "hex" //Unsupported Exprs
    val unsupportedExecs = values(values.length - 2) // index of unsupportedExecs
    val unsupportedExprs = values(values.length - 1) // index of unsupportedExprs
    assert(expectedExecs == unsupportedExecs)
    assert(expectedExprs == unsupportedExprs)
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
    val appInfo = qualApp.aggregateStats()
    assert(appInfo.nonEmpty)
    val appNameMaxSize = QualOutputWriter.getAppNameSize(Seq(appInfo.get))
    assert(headers.size ==
      QualOutputWriter.getSummaryHeaderStringsAndSizes(appNameMaxSize, 0).keys.size)
    assert(values.size == headers.size - 1) // unSupportedExpr is empty
    // 3 should be the SQL DF Duration
    assert(headers(3).contains("SQL DF"))
    assert(values(3).toInt > 0)
    val csvDetailedOut = qualApp.getDetailed(",", false)
    val rowsDetailedOut = csvDetailedOut.split("\n")
    assert(rowsDetailedOut.size == 2)
    val headersDetailed = rowsDetailedOut(0).split(",")
    val valuesDetailed = rowsDetailedOut(1).split(",")
    assert(headersDetailed.size == QualOutputWriter
      .getDetailedHeaderStringsAndSizes(Seq(qualApp.aggregateStats.get), false).keys.size)
    //UnsupportedeExecs and UnsupportedExprs is not present in the file
    assert(headersDetailed.size -2  == csvDetailedFields.size)
    assert(valuesDetailed.size - 1 == csvDetailedFields.size) // UnsupportedExprs is empty
    // check all headers exists
    for (ind <- 0 until csvDetailedFields.size) {
      assert(csvDetailedHeader(ind).equals(headersDetailed(ind)))
    }
    // check that recommendation field is relevant to GPU Speed-up
    // Note that range-check does not apply for NOT-APPLICABLE
    val estimatedFieldsIndStart = 2
    assert(valuesDetailed(estimatedFieldsIndStart + 1).toDouble >= 1.0)
    if (!valuesDetailed(estimatedFieldsIndStart).equals(QualificationAppInfo.NOT_APPLICABLE)) {
      if (valuesDetailed(estimatedFieldsIndStart + 1).toDouble >=
        QualificationAppInfo.LOWER_BOUND_STRONGLY_RECOMMENDED) {
        assert(
          valuesDetailed(estimatedFieldsIndStart).equals(QualificationAppInfo.STRONGLY_RECOMMENDED))
      } else if (valuesDetailed(estimatedFieldsIndStart + 1).toDouble >=
        QualificationAppInfo.LOWER_BOUND_RECOMMENDED) {
        assert(valuesDetailed(estimatedFieldsIndStart).equals(QualificationAppInfo.RECOMMENDED))
      } else {
        assert(valuesDetailed(estimatedFieldsIndStart).equals(QualificationAppInfo.NOT_RECOMMENDED))
      }
    }

    // check numeric fields skipping "Estimated Speed-up" on purpose
    for (ind <- estimatedFieldsIndStart + 2  until csvDetailedFields.size) {
      if (csvDetailedFields(ind)._1.equals(DoubleType)
        || csvDetailedFields(ind)._1.equals(LongType)) {
        val numValue = valuesDetailed(ind).toDouble
        if (headersDetailed(ind).equals(csvDetailedHeader(19))) {
          // unsupported task duration can be 0
          assert(numValue >= 0)
        } else if (headersDetailed(ind).equals(csvDetailedHeader(10))) {
          // cpu percentage 0-100
          assert(numValue >= 0.0 && numValue <= 100.0)
        } else if (headersDetailed(ind).equals(csvDetailedHeader(6)) ||
          headersDetailed(ind).equals(csvDetailedHeader(9))) {
          // "SQL DF Duration" and "GPU Opportunity" cannot be larger than App Duration
          assert(numValue >= 0 && numValue <= valuesDetailed(8).toDouble)
        } else {
          assert(valuesDetailed(ind).toDouble > 0)
        }
      }
    }
  }

  test("running qualification app files with per sql") {
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
        // just basic testing that line exists and has right separator
        val csvHeader = qualApp.getPerSqlCSVHeader
        assert(csvHeader.contains("App Name,App ID,SQL ID,SQL Description,SQL DF Duration," +
          "GPU Opportunity,Estimated GPU Duration,Estimated GPU Speedup," +
          "Estimated GPU Time Saved,Recommendation"))
        val txtHeader = qualApp.getPerSqlTextHeader
        assert(txtHeader.contains("|                              App Name|             App ID|" +
          "SQL ID" +
          "|                                                                                     " +
          "SQL Description|" +
          "SQL DF Duration|GPU Opportunity|Estimated GPU Duration|" +
          "Estimated GPU Speedup|Estimated GPU Time Saved|      Recommendation|"))
        val randHeader = qualApp.getPerSqlHeader(";", true, 20)
        assert(randHeader.contains(";                              App Name;             App ID" +
          ";SQL ID;     SQL Description;SQL DF Duration;GPU Opportunity;Estimated GPU Duration;" +
          "Estimated GPU Speedup;Estimated GPU Time Saved;      Recommendation;"))
        val allSQLIds = qualApp.getAvailableSqlIDs
        val numSQLIds = allSQLIds.size
        assert(numSQLIds > 0)
        val sqlIdToLookup = allSQLIds.head
        val (csvOut, txtOut) = qualApp.getPerSqlTextAndCSVSummary(sqlIdToLookup)
        assert(csvOut.contains("Profiling Tool Unit Tests") && csvOut.contains(","),
          s"CSV output was: $csvOut")
        assert(txtOut.contains("Profiling Tool Unit Tests") && txtOut.contains("|"),
          s"TXT output was: $txtOut")
        val sqlOut = qualApp.getPerSQLSummary(sqlIdToLookup, ":", true, 5)
        assert(sqlOut.contains("Tool Unit Tests:"), s"SQL output was: $sqlOut")

        // test different delimiter
        val sumOut = qualApp.getSummary(":", false)
        val rowsSumOut = sumOut.split("\n")
        assert(rowsSumOut.size == 2)
        val headers = rowsSumOut(0).split(":")
        val values = rowsSumOut(1).split(":")
        val appInfo = qualApp.aggregateStats()
        assert(appInfo.nonEmpty)
        assert(headers.size ==
          QualOutputWriter.getSummaryHeaderStringsAndSizes(30, 30).keys.size)
        assert(values.size == headers.size - 1) // UnsupportedExpr is empty
        // 3 should be the SQL DF Duration
        assert(headers(3).contains("SQL DF"))
        assert(values(3).toInt > 0)
        val detailedOut = qualApp.getDetailed(":", prettyPrint = false, reportReadSchema = true)
        val rowsDetailedOut = detailedOut.split("\n")
        assert(rowsDetailedOut.size == 2)
        val headersDetailed = rowsDetailedOut(0).split(":")
        val valuesDetailed = rowsDetailedOut(1).split(":")
        // Check Read Schema contains json and parquet
        val readSchemaIndex = headersDetailed.length - 1
        assert(headersDetailed(readSchemaIndex).contains("Read Schema"))
        assert(valuesDetailed(readSchemaIndex).contains("json") &&
            valuesDetailed(readSchemaIndex).contains("parquet"))
        qualApp.cleanupSQL(sqlIdToLookup)
        assert(qualApp.getAvailableSqlIDs.size == numSQLIds - 1)
      }
    }
  }

  test("test potential problems timestamp") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, _) = ToolTestUtils.generateEventLog(eventLogDir, "timezone") { spark =>
        import spark.implicits._
        val testData = Seq((1, 1662519019), (2, 1662519020)).toDF("id", "timestamp")
        spark.sparkContext.setJobDescription("timestamp functions as potential problems")
        testData.createOrReplaceTempView("t1")
        spark.sql("SELECT id, hour(current_timestamp()), second(to_timestamp(timestamp)) FROM t1")
      }

      // run the qualification tool
      TrampolineUtil.withTempDir { outpath =>
        val appArgs = new QualificationArgs(Array(
          "--output-directory",
          outpath.getAbsolutePath,
          eventLog))

        val (exit, sumInfo) =
          QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
  
        // the code above that runs the Spark query stops the Sparksession
        // so create a new one to read in the csv file
        createSparkSession()

        // validate that the SQL description in the csv file escapes commas properly
        val outputResults = s"$outpath/rapids_4_spark_qualification_output/" +
          s"rapids_4_spark_qualification_output.csv"
        val outputActual = readExpectedFile(new File(outputResults))
        assert(outputActual.collect().size == 1)
        assert(outputActual.select("Potential Problems").first.getString(0) == 
          "TIMEZONE to_timestamp():TIMEZONE hour():TIMEZONE current_timestamp():TIMEZONE second()")
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
