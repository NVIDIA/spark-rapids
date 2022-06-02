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

package com.nvidia.spark.rapids.tool.profiling

import java.io.File
import java.nio.file.{Files, Paths, StandardOpenOption}

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids.tool.{EventLogPathProcessor, ToolTestUtils}
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

  lazy val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

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
      new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
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

    val collect = new CollectInformation(apps)
    val rapidsJarResults = collect.getRapidsJARInfo
    assert(rapidsJarResults.size === 2)
    assert(rapidsJarResults.filter(_.jar.contains("rapids-4-spark_2.12-0.5.0.jar")).size === 1)
    assert(rapidsJarResults.filter(_.jar.contains("cudf-0.19.2-cuda11.jar")).size === 1)
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
    assert(apps.head.jobIdToInfo.keys.toSeq.size == 6)
    assert(apps.head.jobIdToInfo.keys.toSeq.contains(0))
    val stage0 = apps.head.stageIdToInfo.get((0, 0))
    assert(stage0.isDefined)
    assert(stage0.get.info.numTasks.equals(1))
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
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val sqlMetrics = collect.getSQLPlanMetrics
    val resultExpectation =
      new File(expRoot, "rapids_join_eventlog_sqlmetrics_expectation.csv")
    assert(sqlMetrics.size == 83)
    import sparkSession.implicits._
    val df = sqlMetrics.toDF
    val dfExpect = ToolTestUtils.readExpectationCSV(sparkSession, resultExpectation.getPath())
    ToolTestUtils.compareDataFrames(df, dfExpect)
  }

  test("test printSQLPlans") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/rapids_join_eventlog.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      CollectInformation.printSQLPlans(apps, tempOutputDir.getAbsolutePath)
      val outputDir = new File(tempOutputDir, apps.head.appId)
      val dotDirs = ToolTestUtils.listFilesMatching(outputDir, _.endsWith("planDescriptions.log"))
      assert(dotDirs.length === 1)
    }
  }

  test("test read GPU datasourcev1") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog-gpu-dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 5)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "CSV(GPU)", "Parquet(GPU)", "ORC(GPU)", "JSON(GPU)")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 4
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan_id"))
    }
  }

  test("test read GPU datasourcev2") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog-gpu-dsv2.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 5)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats =
        Set("Text", "gpucsv(GPU)", "gpujson(GPU)", "gpuparquet(GPU)", "gpuorc(GPU)")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 3
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan_id"))
    }
  }

  test("test read datasourcev1") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv1.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 7)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "CSV", "Parquet", "ORC", "JSON")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      assert(parquetRow.schema.contains("loan400"))
      assert(parquetRow.location.contains("lotscolumnsout"))
    }
  }

  test("test read datasourcev2") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$logDir/eventlog_dsv2.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }

      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      assert(dsRes.size == 9)
      val allFormats = dsRes.map { r =>
        r.format
      }.toSet
      val expectedFormats = Set("Text", "csv", "parquet", "orc", "json")
      assert(allFormats.equals(expectedFormats))
      val allSchema = dsRes.map { r =>
        r.schema
      }.toSet
      assert(allSchema.forall(_.nonEmpty))
      val schemaParquet = dsRes.filter { r =>
        r.sqlID == 2
      }
      assert(schemaParquet.size == 1)
      val parquetRow = schemaParquet.head
      // schema is truncated in v2
      assert(!parquetRow.schema.contains("loan400"))
      assert(parquetRow.schema.contains("..."))
      assert(parquetRow.location.contains("lotscolumnsout"))
    }
  }

  test("test jdbc read") {
    TrampolineUtil.withTempDir { tempOutputDir =>
      var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
      val appArgs = new ProfileArgs(Array(s"$qualLogDir/jdbc_eventlog.zstd"))
      var index: Int = 1
      val eventlogPaths = appArgs.eventlog()
      for (path <- eventlogPaths) {
        apps += new ApplicationInfo(hadoopConf,
          EventLogPathProcessor.getEventLogInfo(path,
            sparkSession.sparkContext.hadoopConfiguration).head._1, index)
        index += 1
      }
      assert(apps.size == 1)
      val collect = new CollectInformation(apps)
      val dsRes = collect.getDataSourceInfo
      val format = dsRes.map(r => r.format).toSet.mkString
      val expectedFormat = "JDBC"
      val location = dsRes.map(r => r.location).toSet.mkString
      val expectedLocation = "TBLS"
      assert(format.equals(expectedFormat))
      assert(location.equals(expectedLocation))
      dsRes.foreach { r =>
        assert(r.schema.contains("bigint"))
      }
    }
  }

  test("test printJobInfo") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/rp_sql_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    val jobInfo = collect.getJobInfo

    assert(jobInfo.size == 2)
    val firstRow = jobInfo.head
    assert(firstRow.jobID === 0)
    assert(firstRow.stageIds.size === 1)
    assert(firstRow.sqlID === None)

    val secondRow = jobInfo(1)
    assert(secondRow.jobID === 1)
    assert(secondRow.stageIds.size === 4)
    assert(secondRow.sqlID.isDefined && secondRow.sqlID.get === 0)
  }

  test("test multiple resource profile in single app") {
    var apps :ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/rp_nosql_eventlog"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    assert(apps.head.resourceProfIdToInfo.size == 2)

    val row0 = apps.head.resourceProfIdToInfo(0)
    assert(row0.resourceProfileId.equals(0))
    val execMem0 = row0.executorResources.get(ResourceProfile.MEMORY)
    val execCores0 = row0.executorResources.get(ResourceProfile.CORES)
    assert(execMem0.isDefined && execMem0.get.amount === 20480L)
    assert(execCores0.isDefined && execCores0.get.amount === 4)

    val row1 = apps.head.resourceProfIdToInfo(1)
    assert(row1.resourceProfileId.equals(1))
    val execMem1 = row1.executorResources.get(ResourceProfile.MEMORY)
    val execCores1 = row1.executorResources.get(ResourceProfile.CORES)
    assert(execMem1.isDefined && execMem1.get.amount === 6144L)
    assert(execCores1.isDefined && execCores1.get.amount === 2)
  }

  test("test spark2 and spark3 event logs") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs = new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd",
      s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 2)
    val collect = new CollectInformation(apps)
    val execInfos = collect.getExecutorInfo
    // just the fact it worked makes sure we can run with both files
    // since we give them indexes above they should be in the right order
    // and spark2 event info should be second
    val firstRow = execInfos.head
    assert(firstRow.resourceProfileId === 0)

    val secondRow = execInfos(1)
    assert(secondRow.resourceProfileId === 0)
  }

  test("test filename match") {
    val matchFileName = "udf"
    val appArgs = new ProfileArgs(Array(
      "--match-event-logs",
      matchFileName,
      s"$qualLogDir/udf_func_eventlog",
      s"$qualLogDir/udf_dataset_eventlog",
      s"$qualLogDir/dataset_eventlog"
    ))

    val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
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

      val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
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

      val (result, _) = EventLogPathProcessor.processAllPaths(appArgs.filterCriteria.toOption,
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

  test("test gds-ucx-parameters") {
    val apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/gds_ucx_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path,
          sparkSession.sparkContext.hadoopConfiguration).head._1, index)
      index += 1
    }
    assert(apps.size == 1)
    val collect = new CollectInformation(apps)
    for (app <- apps) {
      val rapidsProps = collect.getProperties(rapidsOnly = true)
      val rows = rapidsProps.map(_.rows.head)
      assert(rows.length == 5) // 5 properties captured.
      // verify  ucx parameters are captured.
      assert(rows.contains("spark.executorEnv.UCX_RNDV_SCHEME"))

      //verify gds parameters are captured.
      assert(rows.contains("spark.rapids.memory.gpu.direct.storage.spill.alignedIO"))

      val sparkProps = collect.getProperties(rapidsOnly = false)
      val sparkPropsRows = sparkProps.map(_.rows.head)
      assert(sparkPropsRows.contains("spark.eventLog.dir"))
      assert(sparkPropsRows.contains("spark.plugins"))
    }
  }

  test("test executor info local mode") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/spark2-eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val execInfo = collect.getExecutorInfo
    assert(execInfo.size == 1)
    assert(execInfo.head.numExecutors === 1)
    assert(execInfo.head.maxMem === 384093388L)
  }

  test("test executor info cluster mode") {
    var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
    val appArgs =
      new ProfileArgs(Array(s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"))
    var index: Int = 1
    val eventlogPaths = appArgs.eventlog()
    for (path <- eventlogPaths) {
      apps += new ApplicationInfo(hadoopConf,
        EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
      index += 1
    }
    assert(apps.size == 1)

    val collect = new CollectInformation(apps)
    val execInfo = collect.getExecutorInfo
    assert(execInfo.size == 1)
    assert(execInfo.head.numExecutors === 8)
    assert(execInfo.head.maxMem === 5538054144L)
  }

  test("test csv file output with failures") {
    val eventLog = s"$logDir/tasks_executors_fail_compressed_eventlog.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/application_1603128018386_7846")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      assert(dotDirs.length === 13)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }
    }
  }

  test("test csv file output gpu") {
    val eventLog = s"$qualLogDir/udf_dataset_eventlog"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/local-1651188809790")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      assert(dotDirs.length === 9)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }
    }
  }

  test("test csv file output compare mode") {
    val eventLog1 = s"$logDir/rapids_join_eventlog.zstd"
    val eventLog2 = s"$logDir/rapids_join_eventlog2.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "-c",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog1,
        eventLog2))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/compare")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      assert(dotDirs.length === 13)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }
    }
  }

  test("test csv file output combined mode") {
    val eventLog1 = s"$logDir/rapids_join_eventlog.zstd"
    val eventLog2 = s"$logDir/rapids_join_eventlog2.zstd"
    TrampolineUtil.withTempDir { tempDir =>
      val appArgs = new ProfileArgs(Array(
        "--csv",
        "--combined",
        "--output-directory",
        tempDir.getAbsolutePath,
        eventLog1,
        eventLog2))
      val exit = ProfileMain.mainInternal(appArgs)
      assert(exit == 0)
      val tempSubDir = new File(tempDir, s"${Profiler.SUBDIR}/combined")

      // assert that a file was generated
      val dotDirs = ToolTestUtils.listFilesMatching(tempSubDir, { f =>
        f.endsWith(".csv")
      })
      assert(dotDirs.length === 11)
      for (file <- dotDirs) {
        assert(file.getAbsolutePath.endsWith(".csv"))
        // just load each one to make sure formatted properly
        val df = sparkSession.read.option("header", "true").csv(file.getAbsolutePath)
        val res = df.collect()
        assert(res.nonEmpty)
      }
    }
  }

  test("test collectionAccumulator") {
    TrampolineUtil.withTempDir { eventLogDir =>
      val (eventLog, appId) = ToolTestUtils.generateEventLog(eventLogDir, "collectaccum") { spark =>
        val a = spark.sparkContext.collectionAccumulator[Long]("testCollect")
        val rdd = spark.sparkContext.parallelize(Array(1, 2, 3, 4))
        // run something to add it to collectionAccumulator
        rdd.foreach(x => a.add(x))
        import spark.implicits._
        rdd.toDF
      }

      TrampolineUtil.withTempDir { collectFileDir =>
        val appArgs = new ProfileArgs(Array(
          "--output-directory",
          collectFileDir.getAbsolutePath,
          eventLog))

        var apps: ArrayBuffer[ApplicationInfo] = ArrayBuffer[ApplicationInfo]()
        var index: Int = 1
        val eventlogPaths = appArgs.eventlog()
        for (path <- eventlogPaths) {
          apps += new ApplicationInfo(hadoopConf,
            EventLogPathProcessor.getEventLogInfo(path, hadoopConf).head._1, index)
          index += 1
        }
        // the fact we generated app properly shows we can handle collectionAccumulators
        // right now we just ignore them so nothing else to check
        assert(apps.size == 1)
      }
    }
  }
}
