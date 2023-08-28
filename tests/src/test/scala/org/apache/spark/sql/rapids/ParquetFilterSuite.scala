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

import com.nvidia.spark.rapids.{GpuFilterExec, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.FilterExec
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

  def testPushDownPredicate[A](spark: SparkSession, writeDf: DataFrame, predicate: String, 
      length: Int): Unit = {
    withAllParquetReaders {
      withTempPath { path =>
        withSQLConf(
            // Makes sure disabling 'spark.sql.parquet.recordFilter' still enables
            // row group level filtering.
            SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
            SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
            writeDf.coalesce(1)
              .write.option("parquet.block.size", 512)
              .parquet(path.getAbsolutePath)
            val df = spark.read.parquet(path.getAbsolutePath).filter(predicate)
            println(df.explain())
            // Here, we strip the Spark side filter and check the actual results from Parquet.
            val actual = stripSparkFilter(spark, df).collect().length
            println(s"actual: $actual")
            assert(actual > 1 && actual < length)
          // })
        }
      }
    }
  }

  test("Parquet filter pushdown - int") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).toDF("a")
      val predicate = "a == 500"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - long") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toLong).toDF("a")
      val predicate = "a == 500"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - float") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toFloat).toDF("a")
      val predicate = "a == 500"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - double") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toDouble).toDF("a")
      val predicate = "a == 500"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - string") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(_.toString).toDF("a")
      val predicate = "a == '500'"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - decimal") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => BigDecimal.valueOf(i)).toDF("a")
      val predicate = "a == 500"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

  test("Parquet filter pushdown - timestamp") {
    withCpuSparkSession(spark => {
      import spark.implicits._
      val df = (1 to 1024).map(i => new java.sql.Timestamp(i)).toDF("a")
      val predicate = "a == '1970-01-01 00:00:00.500'"
      testPushDownPredicate(spark, df, predicate, 1024)
    })
  }

}
