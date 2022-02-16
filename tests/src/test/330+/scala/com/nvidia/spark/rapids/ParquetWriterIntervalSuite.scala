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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DayTimeIntervalType, IntegerType, StructField, StructType, YearMonthIntervalType}

/**
 * Tests for writing Parquet files with the GPU on Spark 330+.
 * DayTimeIntervalType and YearMonthIntervalType do not exists before 330
 */
class ParquetWriterIntervalSuite extends SparkQueryCompareTestSuite {
  test("ANSI interval read/write") {
    val conf = new SparkConf().set(RapidsConf.SQL_ENABLED.key, "true")
    // Throws an exception if plan not converted to GPU
    conf.set("spark.rapids.sql.test.enabled", "true")

    val tempFile = File.createTempFile("interval", ".parquet")
    try {
      SparkSessionHolder.withSparkSession(conf, spark => {
        val data = (1 to 1024)
            .map(i => Row(i, Period.of(i, i, 0), Duration.ofDays(i).plusSeconds(i)))
        val schema = StructType(Array(
          StructField("i", IntegerType, false),
          StructField("y", YearMonthIntervalType(), false),
          StructField("d", DayTimeIntervalType(), false)))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data, numSlices = 1), schema)
        df.coalesce(1).write.mode("overwrite").parquet(tempFile.getAbsolutePath)
        val readDf = spark.read.parquet(tempFile.getAbsolutePath)
        assert(compare(readDf.collect(), df.collect()))
      })
    } finally {
      tempFile.delete()
    }
  }
}

