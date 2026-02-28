/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import com.nvidia.spark.rapids.GpuFilterExec

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.parquet.ParquetQuerySuite
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetQuerySuite extends ParquetQuerySuite with RapidsSQLTestsBaseTrait {
  import testImplicits._

  testRapids("SPARK-26677: negated null-safe equality comparison should not filter " +
    "matched row groups") {
    withAllParquetReaders {
      withTempPath { path =>
        // Repeated values for dictionary encoding.
        Seq(Some("A"), Some("A"), None).toDF.repartition(1)
          .write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        checkAnswer(stripSparkFilterRapids(df.where("NOT (value <=> 'A')")), df)
      }
    }
  }

  testRapids("SPARK-34212 Parquet should read decimals correctly") {
    def readParquet(schema: String, path: String): DataFrame = {
      spark.read.schema(schema).parquet(path)
    }

    withAllParquetReaders {
      withTempPath { path =>
        val filePath = path.toString
        val df = sql("SELECT 1.0 a, CAST(1.23 AS DECIMAL(17, 2)) b, CAST(1.23 AS DECIMAL(36, 2)) c")
        df.write.parquet(filePath)

        Seq("a DECIMAL(3, 0)", "b DECIMAL(18, 1)", "c DECIMAL(37, 1)").foreach { schema =>
          var expectedRows = Seq.empty[org.apache.spark.sql.Row]
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "false") {
            expectedRows = readParquet(schema, filePath).collect().toSeq
          }
          var actualRows = Seq.empty[org.apache.spark.sql.Row]
          withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true") {
            actualRows = readParquet(schema, filePath).collect().toSeq
          }
          // GPU path should read narrowed-scale decimal schemas without vectorized-read failures.
          assert(actualRows == expectedRows, s"Unexpected result for schema: $schema")
        }
      }
    }
  }

  def stripSparkFilterRapids(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case GpuFilterExec(_, child) => child
    }
    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }
}
