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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FilterExec

class OrcFilterSuite extends SparkQueryCompareTestSuite {

  private def checkPredicatePushDown(spark: SparkSession, filepath: String, numRows: Int, 
      predicate: String): Unit = {
    val df = spark.read.orc(filepath).where(predicate)
    val schema = df.schema
    val withoutFilters = df.queryExecution.executedPlan.transform {
      case FilterExec(_, child) => child
      case GpuFilterExec(_, child) => child
    }
    val actual = spark.internalCreateDataFrame(withoutFilters.execute(), schema, false).count()
    assert(0 < actual && actual < numRows)
  }

  test("Support for pushing down filters for boolean types gpu write gpu read") {
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(i == 2))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == true")
      })
    }
  }

  test("Support for pushing down filters for boolean types gpu write cpu read") {
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(i == 2))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withCpuSparkSession(spark => {
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == true")
      })
    }
  }

  test("Support for pushing down filters for boolean types cpu write gpu read") {
    withTempPath { file =>
      withCpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(i == 2))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withGpuSparkSession(spark => {
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == true")
      })
    }
  }

  test("Support for pushing down filters for decimal types gpu write gpu read") {
    assume(false, "https://github.com/rapidsai/cudf/issues/13933")
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(BigDecimal.valueOf(i)))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == 2")
      })
    }
  }

  test("Support for pushing down filters for decimal types gpu write cpu read") {
    assume(false, "https://github.com/rapidsai/cudf/issues/13933")
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(BigDecimal.valueOf(i)))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withCpuSparkSession(spark => {
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == 2")
      })
    }
  }

  test("Support for pushing down filters for decimal types cpu write gpu read") {
    withTempPath { file =>
      withCpuSparkSession(spark => {
        val data = (0 until 10).map(i => Tuple1(BigDecimal.valueOf(i)))
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withGpuSparkSession(spark => {
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, "a == 2")
      })
    }
  }

  test("Support for pushing down filters for timestamp types cpu write gpu read") {
    withTempPath { file =>
      withCpuSparkSession(spark => {
        val timeString = "2015-08-20 14:57:00"
        val data = (0 until 10).map { i =>
          val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
          Tuple1(new Timestamp(milliseconds))
        }
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withGpuSparkSession(spark => {
        val timeString = "2015-08-20 14:57:00"
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, s"a == '$timeString'")
      })
    }
  }

  test("Support for pushing down filters for timestamp types gpu write cpu read") {
    assume(false, "https://github.com/rapidsai/cudf/issues/13899")
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val timeString = "2015-08-20 14:57:00"
        val data = (0 until 10).map { i =>
          val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
          Tuple1(new Timestamp(milliseconds))
        }
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
      })
      withCpuSparkSession(spark => {
        val timeString = "2015-08-20 14:57:00"
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, s"a == '$timeString'")
      })
    }
  }

  test("Support for pushing down filters for timestamp types gpu write gpu read") {
    assume(false, "https://github.com/rapidsai/cudf/issues/13899")
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val timeString = "2015-08-20 14:57:00"
        val data = (0 until 10).map { i =>
          val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
          Tuple1(new Timestamp(milliseconds))
        }
        val df = spark.createDataFrame(data).toDF("a")
        df.repartition(10).write.orc(file.getCanonicalPath)
        checkPredicatePushDown(spark, file.getCanonicalPath, 10, s"a == '$timeString'")
      })
    }
  }
}
