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

import java.io.{File, FileWriter}

import org.scalatest.FunSuite

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, TrampolineUtil}
import org.apache.spark.sql.rapids.tool.profiling._

class QualificationSuite extends FunSuite with Logging {

  val spark = {
    SparkSession
        .builder()
        .master("local[*]")
        .appName("Rapids Spark Profiling Tool Unit Tests")
        .getOrCreate()
  }

  private val expRoot = ProfilingTestUtils.getTestResourceFile("QualificationExpectations")
  private val logDir = ProfilingTestUtils.getTestResourcePath("spark-events-qualification")

  test("test udf event logs") {
    TrampolineUtil.withTempPath { csvOutpath =>
      val resultExpectation = new File(expRoot, "qual_test_simple_expectation.csv")

      val appArgs = new ProfileArgs(Array(
        s"$logDir/dataset_eventlog",
        s"$logDir/dsAndDf_eventlog",
        s"$logDir/udf_dataset_eventlog",
        s"$logDir/udf_func_eventlog",
      ))

      val (exit, dfQualOpt) = QualificationMain.mainInternal(spark, appArgs, writeOutput=false)

      // make sure to change null value so empty strings don't show up as nulls
      val dfExpect = spark.read.option("header", "true").option("nullValue", "\"-\"").csv(resultExpectation.getPath)
      val diffCount = dfQualOpt.map { dfQual =>
        dfQual.except(dfExpect).union(dfExpect.except(dfExpect)).count
      }.getOrElse(-1)

      if (diffCount.asInstanceOf[Long] > 0) {
        dfQualOpt.get.except(dfExpect).show()
        dfExpect.except(dfExpect).show()
      }
      dfExpect.write.option("header", "true").csv("expectcsv")
      dfQualOpt.get.repartition(1).write.option("header", "true").csv("qualcsv")
      assert(diffCount == 0)
    }
  }
}
