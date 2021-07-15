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

import java.util.Date

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

  test("time period parsing") {
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1hd"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("1yr"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("-1d"))
    assertThrows[IllegalArgumentException](AppFilterImpl.parseAppTimePeriod("0m"))
  }


}
