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
import java.time.{Duration, Period}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StructField, StructType, YearMonthIntervalType}

/**
 * Tests for writing Parquet files with the GPU on Spark 330+.
 * DayTimeIntervalType and YearMonthIntervalType do not exists before 330
 */
class ParquetWriterIntervalSuite extends SparkQueryCompareTestSuite {

  // CPU write a parquet, then test the reading between CPU and GPU
  test("test ANSI interval read") {
    val tmpFile = File.createTempFile("interval", ".parquet")
    try {
      withCpuSparkSession(spark => genDf(spark).coalesce(1)
          .write.mode("overwrite").parquet(tmpFile.getAbsolutePath))
      val c = withCpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      val g = withGpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      assert(compare(g, c))
    } finally {
      tmpFile.delete()
    }
  }

  // GPU write a parquet, then test the reading between CPU and GPU
  test("test ANSI interval write") {
    val tmpFile = File.createTempFile("interval", ".parquet")
    try {
      withGpuSparkSession(spark => genDf(spark).coalesce(1)
          .write.mode("overwrite").parquet(tmpFile.getAbsolutePath))
      val c = withCpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      val g = withGpuSparkSession(spark => spark.read.parquet(tmpFile.getAbsolutePath).collect())
      assert(compare(g, c))
    } finally {
      tmpFile.delete()
    }
  }

  // Write 2 files with CPU and GPU separately, then read the 2 files with CPU and compare
  testSparkWritesAreEqual(
    "test ANSI interval read/write",
    genDf,
    (df, path) => df.coalesce(1).write.mode("overwrite").parquet(path),
    (spark, path) => spark.read.parquet(path)
  )

  def genDf(spark: SparkSession): DataFrame = {
    val data = (1 to 1024)
        .map(i => Row(i, Period.of(i, i, 0), Duration.ofDays(i).plusSeconds(i)))
    val schema = StructType(Array(
      StructField("i", IntegerType, false),
      StructField("y", YearMonthIntervalType(), false),
      StructField("d", DayTimeIntervalType(), false)))
    spark.createDataFrame(spark.sparkContext.parallelize(data, numSlices = 1), schema)
  }
}

