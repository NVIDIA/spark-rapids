/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

// import java.nio.charset.StandardCharsets

import com.nvidia.spark.rapids.{GpuFilterExec, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf

class ParquetFilterSuite extends SparkQueryCompareTestSuite {

  def stripSparkFilter(spark: SparkSession, df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
      case GpuFilterExec(_, child) => child
    }
    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    pairs.foreach { case (k, v) =>
      SQLConf.get.setConfString(k, v)
    }
    try f finally {
      pairs.foreach { case (k, _) =>
        SQLConf.get.unsetConf(k)
      }
    }
  }

  def withAllParquetReaders(code: => Unit): Unit = {
    // test the row-based reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
      withClue("Parquet-mr reader") {
        code
      }
    }
    // test the vectorized reader
    withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
      withClue("Vectorized reader") {
        code
      }
    }
  }

  def withAllDevicePair(func: (Boolean, Boolean) => Unit): Unit = {
    for {
      writeGpu <- Seq(true, false)
      readGpu <- Seq(true, false)
    } func(writeGpu, readGpu)
  }

  def testRangePartitioningPpd(spark: SparkSession, writeDf: DataFrame,
      partCol: String, predicate: String,
      length: Int)(writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllParquetReaders {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf
              .repartitionByRange(math.max(1, length / 128), col(partCol))
              .write
              .parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val df = spark.read.parquet(path.getAbsolutePath).filter(predicate)
          // Here, we strip the Spark side filter and check the actual results from Parquet.
          val actual = stripSparkFilter(spark, df).collect().length
          assert(actual > 1 && actual < length)
        }
      }
    }
  }

  def testOutOfRangePpd(spark: SparkSession, writeDf: DataFrame, predicate: String) 
      (writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllParquetReaders {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> "TIMESTAMP_MICROS",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf.coalesce(1).write.parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DATE_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_TIMESTAMP_ENABLED.key -> "true",
          SQLConf.PARQUET_FILTER_PUSHDOWN_DECIMAL_ENABLED.key -> "true",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val df = spark.read.parquet(path.getAbsolutePath).filter(predicate)
          // Here, we strip the Spark side filter and check the actual results from Parquet.
          val actual = stripSparkFilter(spark, df).collect().length
          assert(actual == 0)
        }
      }
    }
  }

  test("Parquet filter pushdown - boolean") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => false).toDF("a")
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == true"))
    })
  }

  test("Parquet filter pushdown - smallint") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toShort).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - binary") {
    implicit class IntToBinary(int: Int) {
      def b: Array[Byte] = int.toString.getBytes()
    }
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.b).toDF("a")
      withAllDevicePair(testOutOfRangePpd(spark, df, s"a = X'2000'"))
    })
  }

  test("Parquet filter pushdown - int") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - long") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toLong).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - float") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toFloat).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - double") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toDouble).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - string") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toString).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == '500'", 1024))
      // string is not supported for out of range predicate both on CPU and GPU
    })
  }

  test("Parquet filter pushdown - decimal") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => BigDecimal.valueOf(i)).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, "a == 2000"))
    })
  }

  test("Parquet filter pushdown - timestamp") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => new java.sql.Timestamp(i)).toDF("a")
      val predicate = "a == '1970-01-01 00:00:00.500'"
      val outOfRangePredicate = "a == '1970-01-01 00:00:02.000'"
      withAllDevicePair(testRangePartitioningPpd(spark, df, "a", predicate, 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df, outOfRangePredicate))
    })
  }

  def testDotsInNamePpd(spark: SparkSession, writeDf: DataFrame, predicate: String) 
      (writeGpu: Boolean, readGpu: Boolean): Unit = {
    withAllParquetReaders {
      withTempPath { path =>
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled"-> writeGpu.toString) {
          writeDf.write.parquet(path.getAbsolutePath)
        }
        withSQLConf(
          SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
          SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false",
          "spark.rapids.sql.test.enabled" -> "false",
          "spark.rapids.sql.enabled" -> readGpu.toString) {
          val readBack = spark.read.parquet(path.getAbsolutePath).where(predicate)
          assert(readBack.count() == 1)
        }
      }
    }
  }

  test("SPARK-31026: Parquet predicate pushdown for fields having dots in the names") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df1 = Seq(Some(1), None).toDF("col.dots")
      withAllDevicePair(testDotsInNamePpd(spark, df1, "`col.dots` IS NOT NULL"))
      val df2 = (1 to 1024).toDF("a")
      withAllDevicePair(testRangePartitioningPpd(spark, df2, "a", "a == 500", 1024))
      withAllDevicePair(testOutOfRangePpd(spark, df2, "a == 2000"))
    })
  }
}
