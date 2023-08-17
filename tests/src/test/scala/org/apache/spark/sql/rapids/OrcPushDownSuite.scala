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

import java.sql.Timestamp

import com.nvidia.spark.rapids.{GpuFilterExec, SparkQueryCompareTestSuite}

import org.apache.spark.sql.{DataFrame, SparkSession}

class OrcPushDownSuite extends SparkQueryCompareTestSuite {

  private def checkPredicatePushDown(spark: SparkSession, df: DataFrame, 
      numRows: Int, predicate: String): Unit = {
    withTempPath { file =>
      df.repartition(10).write.orc(file.getCanonicalPath)
      val dfRead = spark.read.orc(file.getCanonicalPath).where(predicate)
      val schema = dfRead.schema
      val withoutFilters = dfRead.queryExecution.executedPlan.transform {
        case GpuFilterExec(_, child) => child
      }
      val actual = spark.internalCreateDataFrame(withoutFilters.execute(), schema, false).count()
      assert(actual < numRows)
    }
  }

  test("Support for pushing down filters for boolean types") {
    withGpuSparkSession(spark => {
      val data = (0 until 10).map(i => Tuple1(i == 2))
      checkPredicatePushDown(spark, spark.createDataFrame(data).toDF("a"), 10, "a == true")
    })
  }

  test("Support for pushing down filters for decimal types") {
    withCpuSparkSession(spark => {
      val data = (0 until 10).map(i => Tuple1(BigDecimal.valueOf(i)))
      checkPredicatePushDown(spark, spark.createDataFrame(data).toDF("a"), 10, "a == 2")
    })
  }

  test("Support for pushing down filters for timestamp types") {
    withGpuSparkSession(spark => {
      val timeString = "2015-08-20 14:57:00"
      val data = (0 until 10).map { i =>
        val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
        Tuple1(new Timestamp(milliseconds))
      }
      checkPredicatePushDown(spark, spark.createDataFrame(data).toDF("a"), 10, 
          s"a == '$timeString'")
    })
  }
}
