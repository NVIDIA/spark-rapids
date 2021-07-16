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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Calendar

import org.scalatest.FunSuite

import org.apache.spark.sql.TrampolineUtil
import org.apache.spark.sql.rapids.tool.AppFilterImpl

class AppFilterSuite extends FunSuite {

  test("illegal args") {
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1hd"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1yr"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("-1d"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0m"))
  }

  test("time period minute parsing") {
    testTimePeriod(msMinAgo(6), "10min")
  }

  test("time period hour parsing") {
    testTimePeriod(msHoursAgo(10), "14h")
  }

  test("time period day parsing") {
    testTimePeriod(msDaysAgo(40), "41d")
  }

  test("time period day parsing default") {
    testTimePeriod(msDaysAgo(5), "6")
  }

  test("time period week parsing") {
    testTimePeriod(msWeeksAgo(2), "3w")
  }

  test("time period month parsing") {
    testTimePeriod(msMonthsAgo(8), "10m")
  }

  test("time period minute parsing fail") {
    testTimePeriod(msMinAgo(16), "10min", failFilter=true)
  }

  test("time period hour parsing fail") {
    testTimePeriod(msHoursAgo(10), "8h", failFilter=true)
  }

  test("time period day parsing fail") {
    testTimePeriod(msDaysAgo(40), "38d", failFilter=true)
  }

  test("time period week parsing fail") {
    testTimePeriod(msWeeksAgo(2), "1w", failFilter=true)
  }

  test("time period month parsing fail") {
    testTimePeriod(msMonthsAgo(8), "7m", failFilter=true)
  }

  private def testTimePeriod(eventLogTime: Long, startTimePeriod: String,
      failFilter: Boolean = false): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>

        val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath, "testTimeEventLog")

        // scalastyle:off line.size.limit
        val supText =
          s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
             |{"Event":"SparkListenerApplicationStart","App Name":"Spark shell","App ID":"local-1626104300434","Timestamp":${eventLogTime},"User":"user1"}""".stripMargin
        // scalastyle:on line.size.limit
        Files.write(elogFile, supText.getBytes(StandardCharsets.UTF_8))

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath(),
          "--start-app-time",
          startTimePeriod
        )
        val appArgs = new QualificationArgs(allArgs ++ Array(elogFile.toString()))
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        if (failFilter) {
          assert(appSum.size == 0)
        } else {
          assert(appSum.size == 1)
        }
      }
    }
  }

  private def msMonthsAgo(months: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.MONTH, -months)
    c.getTimeInMillis
  }

  private def msWeeksAgo(weeks: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.WEEK_OF_YEAR, -weeks)
    c.getTimeInMillis
  }

  private def msDaysAgo(days: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.DATE, -days)
    c.getTimeInMillis
  }

  private def msMinAgo(mins: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.MINUTE, -mins)
    c.getTimeInMillis
  }

  private def msHoursAgo(hours: Int): Long = {
    val c = Calendar.getInstance
    c.add(Calendar.HOUR, -hours)
    c.getTimeInMillis
  }

  val appsToTest = Array(TestEventLogInfo("ndshours18", msHoursAgo(18), 1),
    TestEventLogInfo("ndsweeks2", msWeeksAgo(2), 1),
    TestEventLogInfo("ndsmonths4", msMonthsAgo(5), 1),
    TestEventLogInfo("ndsdays3", msDaysAgo(3), 1),
    TestEventLogInfo("ndsmins34", msMinAgo(34), 1),
    TestEventLogInfo("nds86", msDaysAgo(4), 1),
    TestEventLogInfo("nds86", msWeeksAgo(2), 2),
    TestEventLogInfo("otherapp", msWeeksAgo(2), 1))

  test("app name and start time 20m") {
    testTimePeriodAndStart(appsToTest, "20m", "nds", appsToTest.size - 1)
  }

  test("app name no matche and start time 20m") {
    testTimePeriodAndStart(appsToTest, "20m", "nomatch", 0)
  }

  test("app name and start time 2d") {
    testTimePeriodAndStart(appsToTest, "2d", "nds", 2)
  }

  test("app name and start time small") {
    testTimePeriodAndStart(appsToTest, "1min", "nds", 0)
  }

  test("app name exact and start time 2d") {
    testTimePeriodAndStart(appsToTest, "2d", "ndsmins34", 1)
  }

  test("app name and start time 20h") {
    testTimePeriodAndStart(appsToTest, "20h", "nds", 2)
  }

  test("app name exact and start time 6d") {
    testTimePeriodAndStart(appsToTest, "6d", "nds86", 1)
  }

  case class TestEventLogInfo(appName: String, eventLogTime: Long, uniqueId: Int)

  private def testTimePeriodAndStart(apps: Array[TestEventLogInfo],
      startTimePeriod: String, filterAppName: String, expectedFilterSize: Int): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>
        val fileNames = apps.map { app =>
          val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath,
            s"${app.appName}-${app.uniqueId}-eventlog")
          // scalastyle:off line.size.limit
          val supText =
            s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
               |{"Event":"SparkListenerApplicationStart","App Name":"${app.appName}","App ID":"local-16261043003${app.uniqueId}","Timestamp":${app.eventLogTime},"User":"user1"}""".stripMargin
          // scalastyle:on line.size.limit
          Files.write(elogFile, supText.getBytes(StandardCharsets.UTF_8))
          elogFile.toString
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath(),
          "--start-app-time",
          startTimePeriod,
          "--application-name",
          filterAppName
        )
        val appArgs = new QualificationArgs(allArgs ++ fileNames)
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == expectedFilterSize)
      }
    }
  }

  case class TestEventLogFSAndAppNameInfo(appName: String, fsTime: Long, uniqueId: Int)

  private val appsWithFsToTest = Array(
    TestEventLogFSAndAppNameInfo("ndshours18", msHoursAgo(18), 1),
    TestEventLogFSAndAppNameInfo("ndsweeks2", msWeeksAgo(2), 1),
    TestEventLogFSAndAppNameInfo("nds86", msDaysAgo(4), 1),
    TestEventLogFSAndAppNameInfo("nds86", msWeeksAgo(2), 2))

  test("app name exact and fs 10-newest") {
    testFileSystemTimeAndStart(appsWithFsToTest, "10-newest", "nds86", 2)
  }

  test("app name exact and 2-oldest") {
    testFileSystemTimeAndStart(appsWithFsToTest, "2-oldest", "ndsweeks2", 1)
  }

  private def testFileSystemTimeAndStart(apps: Array[TestEventLogFSAndAppNameInfo],
      filterCriteria: String, filterAppName: String, expectedFilterSize: Int): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>

        val fileNames = apps.map { app =>
          val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath,
            s"${app.appName}-${app.uniqueId}-eventlog")
          // scalastyle:off line.size.limit
          val supText =
            s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
               |{"Event":"SparkListenerApplicationStart","App Name":"${app.appName}","App ID":"local-16261043003${app.uniqueId}","Timestamp":1626104299853,"User":"user1"}""".stripMargin
          // scalastyle:on line.size.limit
          Files.write(elogFile, supText.getBytes(StandardCharsets.UTF_8))
          new File(elogFile.toString).setLastModified(app.fsTime)
          elogFile.toString
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath(),
          "--filter-criteria",
          filterCriteria,
          "--application-name",
          filterAppName
        )
        val appArgs = new QualificationArgs(allArgs ++ fileNames)
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == expectedFilterSize)
      }
    }
  }

  case class TestEventLogFSAndAppInfo(fileName: String, fsTime: Long, appName: String,
    appTime: Long, uniqueId: Int)

  private val appsFullWithFsToTest = Array(
    TestEventLogFSAndAppInfo("app-ndshours18", msHoursAgo(16), "ndshours18", msHoursAgo(18), 1),
    TestEventLogFSAndAppInfo("app-ndsweeks2", msWeeksAgo(2), "ndsweeks2", msWeeksAgo(2), 1),
    TestEventLogFSAndAppInfo("app-nds86-1", msDaysAgo(3), "nds86", msDaysAgo(4), 1),
    TestEventLogFSAndAppInfo("app-nds86-2", msDaysAgo(13), "nds86", msWeeksAgo(2), 2))

  test("full app name exact and fs 10-newest 6 days") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "10-newest", "nds86", "nds86",
      "6d", 1)
  }

  test("full app name exact and 2-oldest no match from app start") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest", "ndsweeks2", "nds",
      "6d", 0)
  }

  test("full app name exact and 2-oldest") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest", "ndsweeks2", "nds",
      "3w", 1)
  }

  test("full app name exact and 2-oldest no match from filename") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest", "nds", "nomatch",
      "3w", 0)
  }

  test("full 2-oldest no match from app name") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "2-oldest", "nomatch", "nds",
      "3w", 0)
  }

  test("full app name exact and 10-oldest and 3w") {
    testFileSystemTimeAndStartAndAppFull(appsFullWithFsToTest, "10-oldest", "nds86", "app-nds86",
      "3w", 2)
  }

  private def testFileSystemTimeAndStartAndAppFull(apps: Array[TestEventLogFSAndAppInfo],
      filterCriteria: String, filterAppName: String, matchFileName: String,
      startTimePeriod: String, expectedFilterSize: Int): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>

        val fileNames = apps.map { app =>
          val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath, app.fileName)
          // scalastyle:off line.size.limit
          val supText =
            s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
               |{"Event":"SparkListenerApplicationStart","App Name":"${app.appName}","App ID":"local-16261043003${app.uniqueId}","Timestamp":${app.appTime},"User":"user1"}""".stripMargin
          // scalastyle:on line.size.limit
          Files.write(elogFile, supText.getBytes(StandardCharsets.UTF_8))
          new File(elogFile.toString).setLastModified(app.fsTime)
          elogFile.toString
        }

        val allArgs = Array(
          "--output-directory",
          outpath.getAbsolutePath(),
          "--filter-criteria",
          filterCriteria,
          "--application-name",
          filterAppName,
          "--start-app-time",
          startTimePeriod,
          "--match-event-logs",
          matchFileName
        )
        val appArgs = new QualificationArgs(allArgs ++ fileNames)
        val (exit, appSum) = QualificationMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(appSum.size == expectedFilterSize)
      }
    }
  }
}
