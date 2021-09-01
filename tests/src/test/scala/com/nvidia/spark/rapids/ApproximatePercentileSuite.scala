/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DataType, DataTypes}

class ApproximatePercentileSuite extends SparkQueryCompareTestSuite {

  val DEFAULT_PERCENTILES = Array(0.05, 0.25, 0.5, 0.75, 0.95)

  //TODO: ai.rapids.cudf.CudfException: reduce_by_key: failed to synchronize:
  // cudaErrorIllegalAddress: an illegal memory access was encountered
  ignore("5 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 5, delta = Some(100))
  }

  test("250 rows per group, default delta, doubles") {
    doTest(DataTypes.DoubleType, 250, None)
  }

  test("250 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, 250, Some(100))
  }

  //TODO: CPU is more accurate
  ignore("2500 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, 2500, Some(100))
  }

  private def doTest(dataType: DataType, rowsPerGroup: Int, delta: Option[Int]) {

    val percentiles = withCpuSparkSession { spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, DEFAULT_PERCENTILES, delta,
        approx = false)
    }

    val approxPercentilesCpu = withCpuSparkSession { spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, DEFAULT_PERCENTILES, delta, approx = true)
    }

    val approxPercentilesGpu = withGpuSparkSession { spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, DEFAULT_PERCENTILES, delta, approx = true)
    }

    val keys = percentiles.keySet ++ approxPercentilesCpu.keySet ++ approxPercentilesGpu.keySet

    for (key <- keys) {
      val cpuDiff = percentiles(key).zip(approxPercentilesCpu(key)).map {
        case (p, ap) => (p - ap).abs
      }
      val gpuDiff = percentiles(key).zip(approxPercentilesGpu(key)).map {
        case (p, ap) => (p - ap).abs
      }
      val gpuAtLeastAsAccurate = cpuDiff.zip(gpuDiff).forall {
        case (cpu, gpu) => gpu <= cpu
      }
      if (!gpuAtLeastAsAccurate) {
        fail("GPU was less accurate than CPU:\n\n" +
          s"Percentiles: ${percentiles(key).mkString(", ")}\n\n" +
          s"CPU Approx Percentiles: ${approxPercentilesCpu(key).mkString(", ")}\n\n" +
          s"GPU Approx Percentiles: ${approxPercentilesGpu(key).mkString(", ")}"
        )
      }
    }
  }

  private def calcPercentiles(
      spark: SparkSession,
      dataType: DataType,
      rowsPerDept: Int,
      percentiles: Array[Double],
      delta: Option[Int],
      approx: Boolean
    ): Map[String, Array[Double]] = {

    val df = salaries(spark, dataType, rowsPerDept)

    val percentileArg = if (percentiles.length > 1) {
      s"array(${percentiles.mkString(", ")})"
    } else {
      s"${percentiles.head}"
    }

    val func = if (approx) "approx_percentile" else "percentile"

    val groupBy = df.groupBy(col("dept"))

    val aggrExpr = delta match {
      case None => expr(s"$func(salary, $percentileArg)")
      case Some(n) => expr(s"$func(salary, $percentileArg, $n)")
    }

    val df2 = groupBy.agg(aggrExpr.as("approx_percentiles"))
      .orderBy("dept")

    val rows = df2.collect()

    rows.map(row => {
      val dept = row.getString(0)
      val percentiles = row.getAs[mutable.WrappedArray[Double]](1)
      dept -> percentiles.map(d => d).toArray
    }).toMap
  }

  private def salaries(
      spark: SparkSession,
      salaryDataType: DataType, rowsPerDept: Int): DataFrame = {
    import spark.implicits._
    val rand = new Random(0)
    val base = salaryDataType match {
      case DataTypes.DoubleType => 1d
      case DataTypes.IntegerType => 1
      case DataTypes.LongType => 1L
    }
    Range(0, rowsPerDept).flatMap(_ => Seq(
      ("a", 1000 * base + rand.nextInt(1000)),
      ("b", 10000 * base + rand.nextInt(10000)),
      ("c", 100000 * base + rand.nextInt(100000)),
      ("d", 1000000 * base + rand.nextInt(1000000))))
      .toDF("dept", "salary").repartition(4)
  }

}
