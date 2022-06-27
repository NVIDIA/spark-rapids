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
        val appArgs = new ProfileArgs(allArgs ++ Array(elogFile.toString()))
        val (exit, filteredSize) = ProfileMain.mainInternal(appArgs)
        assert(exit == 0)
        if (failFilter) {
          assert(filteredSize == 0)
        } else {
          assert(filteredSize == 1)
        }
      }
    }
  }

  case class TestEventLogFSAndAppStartInfo(fileName: String, fsTime: Long,
    appTime: Long, uniqueId: Int)

  private val appsWithFsAndStartTimeToTest = Array(
    TestEventLogFSAndAppStartInfo("app-ndshours18", msHoursAgo(16),msHoursAgo(18), 1),
    TestEventLogFSAndAppStartInfo("app-ndsweeks2", msWeeksAgo(2), msWeeksAgo(2), 1),
    TestEventLogFSAndAppStartInfo("app-nds86-1", msDaysAgo(3), msDaysAgo(4), 1),
    TestEventLogFSAndAppStartInfo("app-nds86-2", msDaysAgo(13), msWeeksAgo(2), 2))

  test("10-newest-filesystem and 6 days") {
    testFileSystemTimeAndAppStartTime(appsWithFsAndStartTimeToTest, "10-newest-filesystem",
      "6d", 2)
  }

  test("2-oldest-filesystem no match from app start") {
    testFileSystemTimeAndAppStartTime(appsWithFsAndStartTimeToTest, "2-oldest-filesystem",
      "6d", 0)
  }

  test("2-oldest-filesystem") {
    testFileSystemTimeAndAppStartTime(appsWithFsAndStartTimeToTest, "2-oldest-filesystem",
      "3w", 2)
  }

  test("10-oldest-filesystem and 3w") {
    testFileSystemTimeAndAppStartTime(appsWithFsAndStartTimeToTest, "10-oldest-filesystem",
      "3w", 4)
  }

  private def testFileSystemTimeAndAppStartTime(apps: Array[TestEventLogFSAndAppStartInfo],
      filterCriteria: String, startTimePeriod: String,
      expectedFilterSize: Int): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>

        val fileNames = apps.map { app =>
          val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath, app.fileName)
          // scalastyle:off line.size.limit
          val supText =
            s"""{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
               |{"Event":"SparkListenerApplicationStart","App Name":"Spark-shell","App ID":"local-16261043003${app.uniqueId}","Timestamp":${app.appTime},"User":"user1"}""".stripMargin
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
          "--start-app-time",
          startTimePeriod
        )
        val appArgs = new ProfileArgs(allArgs ++ fileNames)
        val (exit, filteredSize) = ProfileMain.mainInternal(appArgs)
        assert(exit == 0)
        assert(filteredSize == expectedFilterSize)
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
}
