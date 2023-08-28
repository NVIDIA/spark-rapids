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

import org.apache.spark.sql.{DataFrame, SparkSession, SQLContext, SQLImplicits}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.internal.SQLConf

class ParquetPushDownSuite extends SparkQueryCompareTestSuite {

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

  test("SPARK-31026: Parquet predicate pushdown for fields having dots in the names") {
    withAllParquetReaders {
      // withTempPath { path =>
        withGpuSparkSession(spark => {
          object testImplicits extends SQLImplicits {
            protected override def _sqlContext: SQLContext = spark.sqlContext
          }
          import testImplicits._

          // withAllParquetReaders {
            withSQLConf(
                SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> true.toString,
                SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
              withTempPath { path =>
                Seq(Some(1), None).toDF("col.dots").write.parquet(path.getAbsolutePath)
                val readBack = spark.read.parquet(path.getAbsolutePath)
                    .where("`col.dots` IS NOT NULL")
                assert(readBack.count() == 1)
              }
            }
        })
        withTempPath { path =>
          withSQLConf(
              // Makes sure disabling 'spark.sql.parquet.recordFilter' still enables
              // row group level filtering.
              SQLConf.PARQUET_RECORD_FILTER_ENABLED.key -> "false",
              SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "true") {
            withGpuSparkSession(spark => {
              object testImplicits extends SQLImplicits {
            protected override def _sqlContext: SQLContext = spark.sqlContext
          }
          import testImplicits._
            // withTempPath { path =>
              val data = (1 to 1024)
              data.toDF("coldots").coalesce(1)
                .write.option("parquet.block.size", 512)
                .parquet(path.getAbsolutePath)
            })
            withGpuSparkSession(spark => {
              val data = (1 to 1024)
              val df = spark.read.parquet(path.getAbsolutePath).filter("`coldots` == 500")
              // Here, we strip the Spark side filter and check the actual results from Parquet.
              val actual = stripSparkFilter(spark, df).collect().length
              // Since those are filtered at row group level, the result count should be less
              // than the total length but should not be a single record.
              // Note that, if record level filtering is enabled, it should be a single record.
              // If no filter is pushed down to Parquet, it should be the total length of data.
              assert(actual > 1 && actual < data.length)
            })
          }
        }
        // })
      // }
    }
  }
}
