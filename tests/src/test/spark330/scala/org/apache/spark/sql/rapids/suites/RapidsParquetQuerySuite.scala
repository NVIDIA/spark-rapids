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
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetQuerySuite extends ParquetQuerySuite with RapidsSQLTestsBaseTrait {
  import testImplicits._

  test("SPARK-26677: negated null-safe equality comparison should not filter " +
    "matched row groupsn Rapids") {
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

  def stripSparkFilterRapids(df: DataFrame): DataFrame = {
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case GpuFilterExec(_, child) => child
    }
    spark.internalCreateDataFrame(withoutFilters.execute(), schema)
  }
}
