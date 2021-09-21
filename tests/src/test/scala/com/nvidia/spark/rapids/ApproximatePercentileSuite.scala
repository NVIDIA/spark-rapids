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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DataType, DataTypes}

class ApproximatePercentileSuite extends SparkQueryCompareTestSuite {

  val DEFAULT_PERCENTILES = Array(0.005, 0.05, 0.25, 0.45, 0.5, 0.55, 0.75, 0.95, 0.995)

  test("1 row per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 1, delta = Some(100))
  }

  test("5 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 5, delta = Some(100))
  }

  test("250 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 250, delta = Some(100))
  }

  test("2500 rows per group, delta 100, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 2500, delta = Some(100))
  }

  test("250 rows per group, default delta, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 250, delta = None)
  }

  test("25000 rows per group, default delta, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 25000, delta = None)
  }

  test("50000 rows per group, default delta, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 50000, delta = None)
  }

  // test with a threshold just below the default level of 10000
  test("50000 rows per group, delta 9999, doubles") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 50000, delta = Some(9999))
  }

  test("empty input set") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 0, delta = None)
  }

  test("scalar percentile") {
    doTest(DataTypes.DoubleType, rowsPerGroup = 250,
      percentileArg = Left(0.5), delta = Some(100))
  }

  test("fall back to CPU for reduction") {

    val conf = new SparkConf()
      .set(RapidsConf.ENABLE_APPROX_PERCENTILE.key, "true")
      .set(RapidsConf.TEST_ALLOWED_NONGPU.key, "ShuffleExchangeExec,ObjectHashAggregateExec," +
        "AggregateExpression,ApproximatePercentile,Literal,Alias")

    withGpuSparkSession(spark => {
      salaries(spark, DataTypes.DoubleType, 50)
        .createOrReplaceTempView("salaries")

      val df = spark.sql("SELECT approx_percentile(salary, Array(0.5)) FROM salaries")
      df.collect()

      assert(TestUtils.findOperator(df.queryExecution.executedPlan,
        _.isInstanceOf[GpuHashAggregateExec]).isEmpty)

    }, conf)
  }

  private def doTest(dataType: DataType,
      rowsPerGroup: Int,
      percentileArg: Either[Double, Array[Double]] = Right(DEFAULT_PERCENTILES),
      delta: Option[Int]) {

    val percentiles = withCpuSparkSession { spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, percentileArg, delta,
        approx = false)
    }

    val approxPercentilesCpu = withCpuSparkSession { spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, percentileArg, delta, approx = true)
    }

    val conf = new SparkConf()
      .set(RapidsConf.ENABLE_APPROX_PERCENTILE.key, "true")

    val approxPercentilesGpu = withGpuSparkSession(spark =>
      calcPercentiles(spark, dataType, rowsPerGroup, percentileArg, delta, approx = true)
    , conf)

    val keys = percentiles.keySet ++ approxPercentilesCpu.keySet ++ approxPercentilesGpu.keySet

    for (key <- keys) {
      val p = percentiles(key)
      val cpuApprox = approxPercentilesCpu(key)
      val gpuApprox = approxPercentilesGpu(key)

      val gpuAtLeastAsAccurate = p.zip(cpuApprox).zip(gpuApprox).map {
        case ((exact, cpu), gpu) =>
          val gpu_delta = (gpu - exact).abs
          val cpu_delta = (cpu - exact).abs
          if (gpu_delta <= cpu_delta) {
            // GPU was at least as close
            true
          } else {
            // check that we are within some tolerance
            if (gpu_delta == 0.0) {
              (gpu_delta / cpu_delta).abs - 1 < 0.001
            } else {
              (cpu_delta / gpu_delta).abs - 1 < 0.001
            }
          }
      }

      if (gpuAtLeastAsAccurate.contains(false)) {
        fail("GPU was less accurate than CPU:\n\n" +
          s"Percentiles: ${p.mkString(", ")}\n\n" +
          s"CPU Approx Percentiles: ${cpuApprox.mkString(", ")}\n\n" +
          s"GPU Approx Percentiles: ${gpuApprox.mkString(", ")}"
        )
      }
    }
  }

  private def calcPercentiles(
      spark: SparkSession,
      dataType: DataType,
      rowsPerDept: Int,
      percentilesArg: Either[Double, Array[Double]],
      delta: Option[Int],
      approx: Boolean
    ): Map[String, Array[Double]] = {

    val df = salaries(spark, dataType, rowsPerDept)

    val percentileArg = percentilesArg match {
      case Left(n) => s"$n"
      case Right(n) => s"array(${n.mkString(", ")})"
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

      val foo = percentilesArg match {
        case Left(_) =>
          Array(row.getAs[Double](1))
        case Right(_) =>
          val value: mutable.Seq[Double] = row.getAs[mutable.WrappedArray[Double]](1)
          if (value == null) {
            Array[Double]()
          } else {
            value.map(d => d).toArray
          }
      }
      dept -> foo
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
      /// group 'd' has narrow range of values and many repeated values
      ("d", 100000 * base + rand.nextInt(10)),
      /// group 'e' has range of negative and positive values
      ("e", base - 5 + rand.nextInt(10))))
      .toDF("dept", "salary").repartition(4)
  }

}
