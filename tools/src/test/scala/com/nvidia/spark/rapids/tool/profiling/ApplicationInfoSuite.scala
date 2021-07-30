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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils
import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling._

class ApplicationInfoSuite extends FunSuite with Logging {

  lazy val sparkSession = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  lazy val hadoopConf = new Configuration()

  private val expRoot = ToolTestUtils.getTestResourceFile("ProfilingExpectations")
  private val logDir = ToolTestUtils.getTestResourcePath("spark-events-profiling")
  private val qualLogDir = ToolTestUtils.getTestResourcePath("spark-events-qualification")

  test("test single event") {
    testSqlCompression()
  }

  test("zstd: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("zstd"))
  }

  test("snappy: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("snappy"))
  }

  test("lzf: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("lz4"))
  }

  test("lz4: test sqlMetrics duration and execute cpu time") {
    testSqlCompression(Option("lzf"))
  }

  private def testSqlCompression(compressionNameOpt: Option[String] = None) = {
    val rawLog = s"$logDir/eventlog_minimal_events"
    compressionNameOpt.foreach { compressionName =>
      val codec = TrampolineUtil.createCodec(sparkSession.sparkContext.getConf,
        compressionName)
      TrampolineUtil.withTempDir { tempDir =>
        val compressionFileName = new File(tempDir,
          "eventlog_minimal_events." + compressionName)
        val inputStream = Files.newInputStream(Paths.get(rawLog))
        val outputStream = codec.compressedOutputStream(
          Files.newOutputStream(compressionFileName.toPath, StandardOpenOption.CREATE))
        // copy and close streams
        IOUtils.copyBytes(inputStream, outputStream, 4096, true)
        testSingleEventFile(Array(tempDir.toString))
        compressionFileName.delete()
      }
    }
    if (compressionNameOpt.isEmpty) {
      testSingleEventFile(Array(rawLog))
    }
  }

  private def testSingleEventFile(logs: Array[String]): Unit = {
    val apps = ToolTestUtils.processProfileApps(logs, sparkSession)
    assert(apps.size == 1)
    val firstApp = apps.head
    assert(firstApp.sparkVersion.equals("3.1.1"))
    assert(firstApp.gpuMode.equals(true))
    assert(firstApp.jobIdToInfo.keys.toSeq.contains(1))
    val stageInfo = firstApp.stageIdToInfo.get((0,0))
    assert(stageInfo.isDefined && stageInfo.get.info.numTasks.equals(1))
    assert(firstApp.stageIdToInfo.get((2, 0)).isDefined)
    assert(firstApp.taskEnd(firstApp.index).successful.equals(true))
    assert(firstApp.taskEnd(firstApp.index).endReason.equals("Success"))
    val execInfo = firstApp.executorIdToInfo.get(firstApp.executorIdToInfo.keys.head)
    assert(execInfo.isDefined && execInfo.get.totalCores.equals(8))
    val rp = firstApp.resourceProfIdToInfo.get(firstApp.resourceProfIdToInfo.keys.head)
    assert(rp.isDefined)
    val memory = rp.get.executorResources(ResourceProfile.MEMORY)
    assert(memory.amount.equals(1024L))
  }

  test("test rapids jar") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir//rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000),
        hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("3.0.1"))
    assert(apps.head.gpuMode.equals(true))
    val rapidsJar =
      apps.head.classpathEntries.filterKeys(_ matches ".*rapids-4-spark_2.12-0.5.0.jar.*")
    val cuDFJar = apps.head.classpathEntries.filterKeys(_ matches ".*cudf-0.19.2-cuda11.jar.*")
    assert(rapidsJar.size == 1, "Rapids jar check")
    assert(cuDFJar.size == 1, "CUDF jar check")
  }

  test("test sql and resourceprofile eventlog") {
    val eventLog = s"$logDir/rp_sql_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
    }
  }

  test("test spark2 eventlog") {
    val eventLog = Array(s"$logDir/spark2-eventlog.zstd")
    val apps = ToolTestUtils.processProfileApps(eventLog, sparkSession)
    assert(apps.size == 1)
    assert(apps.head.sparkVersion.equals("2.2.3"))
    assert(apps.head.gpuMode.equals(false))
    assert(apps.head.jobIdToInfo.keys.toSeq.size == 1)
    assert(apps.head.jobIdToInfo.keys.toSeq.contains(0))
    val stage0 = apps.head.stageIdToInfo.get((0, 0))
    assert(stage0.isDefined)
    assert(stage0.get.info.numTasks.equals(6))
  }

  test("test no sql eventlog") {
    val eventLog = s"$logDir/rp_nosql_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
    }
  }

  test("test printSQLPlanMetrics") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps, None, 1000)
    val sqlMetrics = collect.printSQLPlanMetrics()
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_sqlmetrics_expectation.csv")
    assert(sqlMetrics.size == 1)
    import sparkSession.implicits._
    val df = sqlMetrics.toDF
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(df, dfExpect)
  }

  /*

  test("test printSQLPlans") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps, None)
      collect.printSQLPlans(tempOutputDir.getAbsolutePath)
      val dotDirs = ToolTestUtils.listFilesMatching(tempOutputDir,
        _.startsWith("planDescriptions-"))
      assert(dotDirs.length === 1)
    }
  }

  test("test read datasourcev1") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps, None)
      val df = collect.getDataSourceInfo(apps.head, sparkSession)
      val rows = df.collect()
      assert(rows.size == 7)
      val allFormats = rows.map { r =>
        r.getString(r.schema.fieldIndex("format"))
      }.toSet
      val expectedFormats = Set("Text", "CSV", "Parquet", "ORC", "JSON")
      assert(allFormats.equals(expectedFormats))
      val allSchema = rows.map { r =>
        r.getString(r.schema.fieldIndex("schema"))
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = rows.filter { r =>
        r.getLong(r.schema.fieldIndex("sqlID")) == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.getString(parquetRow.schema.fieldIndex("schema")).contains("loan400"))
      assert(parquetRow.getString(parquetRow.schema.fieldIndex("location"))
        .contains("lotscolumnsout"))
    }
  }

  test("test read datasourcev2") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv2.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps, None)
      val df = collect.getDataSourceInfo(apps.head, sparkSession)
      val rows = df.collect()
      assert(rows.size == 9)
      val allFormats = rows.map { r =>
        r.getString(r.schema.fieldIndex("format"))
      }.toSet
      val expectedFormats = Set("Text", "csv", "parquet", "orc", "json")
      assert(allFormats.equals(expectedFormats))
      val allSchema = rows.map { r =>
        r.getString(r.schema.fieldIndex("schema"))
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = rows.filter { r =>
        r.getLong(r.schema.fieldIndex("sqlID")) == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      // schema is truncated in v2
      assert(!parquetRow.getString(parquetRow.schema.fieldIndex("schema")).contains("loan400"))
      assert(parquetRow.getString(parquetRow.schema.fieldIndex("schema")).contains("..."))
      assert(parquetRow.getString(parquetRow.schema.fieldIndex("location"))
        .contains("lotscolumnsout"))
    }
  }

  test("test printJobInfo") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    for (app <- apps) {
      val rows = app.runQuery(query = app.jobtoStagesSQL, fileWriter = None).collect()
      assert(rows.size == 2)
      val firstRow = rows.head
      assert(firstRow.getInt(firstRow.schema.fieldIndex("jobID")) === 0)
      assert(firstRow.getList(firstRow.schema.fieldIndex("stageIds")).size == 1)
      assert(firstRow.isNullAt(firstRow.schema.fieldIndex("sqlID")))

      val secondRow = rows(1)
      assert(secondRow.getInt(secondRow.schema.fieldIndex("jobID")) === 1)
      assert(secondRow.getList(secondRow.schema.fieldIndex("stageIds")).size == 4)
      assert(secondRow.getLong(secondRow.schema.fieldIndex("sqlID")) == 0)
    }
  }

  test("test multiple resource profile in single app") {
    var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/rp_nosql_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.resourceProfIdToInfo.size == 2)

    val row0= apps.head.resourceProfIdToInfo(0)
    assert(row0.id.equals(0))
    assert(row0.executorMemory.equals(20480L))
    assert(row0.executorCores.equals(4))

    val row1= apps.head.resourceProfIdToInfo(1)
    assert(row1.id.equals(1))
    assert(row1.executorMemory.equals(6144L))
    assert(row1.executorCores.equals(2))
  }

  test("test spark2 and spark3 event logs") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd",
      s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 2)
    val compare = new CompareApplications(apps, None)
    val df = compare.compareExecutorInfo()
    // just the fact it worked makes sure we can run with both files
    val execinfo = df.collect()
    // since we give them indexes above they should be in the right order
    // and spark2 event info should be second
    val firstRow = execinfo.head
    assert(firstRow.getInt(firstRow.schema.fieldIndex("resourceProfileId")) === 0)

    val secondRow = execinfo(1)
    assert(secondRow.isNullAt(secondRow.schema.fieldIndex("resourceProfileId")))
  }
*/
  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      s"$qualLogDir/udf_func_eventlog",
      s"$qualLogDir/udf_dataset_eventlog",
      s"$qualLogDir/dataset_eventlog"
    ))

    val result = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
      appArgs.matchEventLogs.toOption, appArgs.eventlog(),
      sparkSession.sparkContext.hadoopConfiguration)
    assert(result.length == 2)
  }

  test("test filter file newest") {
    val tempFile1 = File.createTempFile("tempOutputFile1", "")
    val tempFile2 = File.createTempFile("tempOutputFile2", "")
    val tempFile3 = File.createTempFile("tempOutputFile3", "")
    val tempFile4 = File.createTempFile("tempOutputFile4", "")
    try {
      tempFile1.deleteOnExit()
      tempFile2.deleteOnExit()
      tempFile3.deleteOnExit()
      tempFile4.deleteOnExit()

      tempFile1.setLastModified(98765432) // newest file
      tempFile2.setLastModified(12324567) // oldest file
      tempFile3.setLastModified(34567891) // second newest file
      tempFile4.setLastModified(23456789)
      val filterNew = "2-newest-filesystem"
      val appArgs = new ProfileArgs(Array(
        "--filter-criteria",
        filterNew,
        tempFile1.toString,
        tempFile2.toString,
        tempFile3.toString
      ))

      val result = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
        appArgs.matchEventLogs.toOption, appArgs.eventlog(),
        sparkSession.sparkContext.hadoopConfiguration)
      assert(result.length == 2)
      // Validate 2 newest files
      assert(result(0).eventLog.getName.equals(tempFile1.getName))
      assert(result(1).eventLog.getName.equals(tempFile3.getName))
    } finally {
      tempFile1.delete()
      tempFile2.delete()
      tempFile3.delete()
      tempFile4.delete()
    }
  }

  test("test filter file oldest and file name match") {

    val tempFile1 = File.createTempFile("tempOutputFile1", "")
    val tempFile2 = File.createTempFile("tempOutputFile2", "")
    val tempFile3 = File.createTempFile("tempOutputFile3", "")
    val tempFile4 = File.createTempFile("tempOutputFile4", "")
    try {
      tempFile1.deleteOnExit()
      tempFile2.deleteOnExit()
      tempFile3.deleteOnExit()
      tempFile4.deleteOnExit()

      tempFile1.setLastModified(98765432) // newest file
      tempFile2.setLastModified(12324567) // oldest file
      tempFile3.setLastModified(34567891) // second newest file
      tempFile4.setLastModified(23456789)

      val filterOld = "3-oldest-filesystem"
      val matchFileName = "temp"
      val appArgs = new ProfileArgs(Array(
        "--filter-criteria",
        filterOld,
        "--match-event-logs",
        matchFileName,
        tempFile1.toString,
        tempFile2.toString,
        tempFile3.toString,
        tempFile4.toString
      ))

      val result = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
        appArgs.matchEventLogs.toOption, appArgs.eventlog(),
        sparkSession.sparkContext.hadoopConfiguration)
      assert(result.length == 3)
      // Validate 3 oldest files
      assert(result(0).eventLog.getName.equals(tempFile2.getName))
      assert(result(1).eventLog.getName.equals(tempFile4.getName))
      assert(result(2).eventLog.getName.equals(tempFile3.getName))
    } finally {
      tempFile1.delete()
      tempFile2.delete()
      tempFile3.delete()
      tempFile4.delete()
    }
  }

  /*
  test("test gds-ucx-parameters") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/gds_ucx_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(appArgs.numOutputRows.getOrElse(1000), sparkSession,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    for (app <- apps) {
      val rows = app.runQuery(query = app.generateNvidiaProperties + " order by propertyName",
        fileWriter = None).collect()
      assert(rows.length == 5) // 5 properties captured.
      // verify  ucx parameters are captured.
      assert(rows(0)(0).equals("spark.executorEnv.UCX_RNDV_SCHEME"))

      //verify gds parameters are captured.
      assert(rows(1)(0).equals("spark.rapids.memory.gpu.direct.storage.spill.alignedIO"))
    }
  }
  */

}
