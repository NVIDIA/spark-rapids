/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Tests for writing Parquet files with the GPU on Spark 330+.
 * DayTimeIntervalType and YearMonthIntervalType do not exists before 330
 */
class TimeAddSuite extends SparkQueryCompareTestSuite {

  val conf = new SparkConf().set("spark.sql.parquet.fieldId.write.enabled", "true")

  // CPU write a parquet, then test the reading between CPU and GPU
  test("test ANSI timestamp + interval") {
    val tmpFile = File.createTempFile("interval", ".parquet")
    try {
      withCpuSparkSession(spark => genDf(spark).coalesce(1)
          .write.mode("overwrite").parquet(tmpFile.getAbsolutePath), conf)
      val c = withCpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath)
          .selectExpr("t + d")
          .collect())
      val g = withGpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath)
          .selectExpr("t + d")
          .collect())
      assert(compare(g, c))
    } finally {
      tmpFile.delete()
    }
  }

  def genDf(spark: SparkSession): DataFrame = {
    IntervalDataGen.genDf(spark)
  }
}

