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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Calendar

import com.nvidia.spark.rapids.tool.ToolTestUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.AppFilterImpl


class AppFilterSuite extends FunSuite with BeforeAndAfterEach with Logging {

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

  test("illegal args") {
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1hd"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1yr"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("-1d"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0m"))
  }

  test("time period minute parsing") {
    val c = Calendar.getInstance
    c.add(Calendar.MINUTE, -6)
    val newTimeStamp = c.getTimeInMillis
    testTimePeriod(newTimeStamp, "10min")
  }

  test("time period hour parsing") {
    val c = Calendar.getInstance
    c.add(Calendar.HOUR, -10)
    val newTimeStamp = c.getTimeInMillis
    testTimePeriod(newTimeStamp, "14h")
  }

  test("time period day parsing") {
    val c = Calendar.getInstance
    c.add(Calendar.DATE, -40)
    val newTimeStamp = c.getTimeInMillis
    testTimePeriod(newTimeStamp, "40d")
  }

  test("time period week parsing") {
    val c = Calendar.getInstance
    c.add(Calendar.WEEK_OF_YEAR, -2)
    val newTimeStamp = c.getTimeInMillis
    testTimePeriod(newTimeStamp, "3w")
  }

  test("time period month parsing") {
    val c = Calendar.getInstance
    c.add(Calendar.MONTH, -8)
    val newTimeStamp = c.getTimeInMillis
    testTimePeriod(newTimeStamp, "10m")
  }

  private def testTimePeriod(eventLogTime: Long, startTimePeriod: String): Unit = {
    TrampolineUtil.withTempDir { outpath =>
      TrampolineUtil.withTempDir { tmpEventLogDir =>

        val elogFile = Paths.get(tmpEventLogDir.getAbsolutePath, "testTimeEventLog")

        val supText =
          s"""
             |{"Event":"SparkListenerLogStart","Spark Version":"3.1.1"}
             |{"Event":"SparkListenerApplicationStart","App Name":"Spark shell","App ID":"local-1626104300434","Timestamp":${eventLogTime},"User":"user1"}
             |
          """.stripMargin
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
        assert(appSum.size == 1)
      }
    }
  }

}
