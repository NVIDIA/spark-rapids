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

import scala.util.Random

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.types.{DataType, DataTypes}

class ApproximatePercentileSuite extends SparkQueryCompareTestSuite {

  val DEFAULT_PERCENTILES = Array(0.05, 0.25, 0.5, 0.75, 0.95)

  testSparkResultsAreEqual("Approx percentile with grouping, ints, delta 100",
    spark => salaries(spark, DataTypes.IntegerType)) {
    doTest(DEFAULT_PERCENTILES, 100)
  }

  testSparkResultsAreEqual("Approx percentile with grouping, ints, delta 1000",
    spark => salaries(spark, DataTypes.IntegerType)) {
    doTest(DEFAULT_PERCENTILES, 1000)
  }

  test("Real percentile with grouping, ints, delta 1000") {
    withCpuSparkSession { spark =>
      val df = doTest(DEFAULT_PERCENTILES, 1000, func = "percentile")(
        salaries(spark, DataTypes.IntegerType))
      df.collect().foreach(println)
    }

  }

  testSparkResultsAreEqual("Approx percentile with grouping, single percentile, delta 1000",
    spark => salaries(spark, DataTypes.LongType)) {
    doTest(Array(0.25), 1000)
  }

  testSparkResultsAreEqual("Approx percentile with grouping, ints, default delta",
    spark => salaries(spark, DataTypes.IntegerType)) {
    doTest(DEFAULT_PERCENTILES, ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  }

  testSparkResultsAreEqual("Approx percentile with grouping, doubles, delta 100",
    spark => salaries(spark, DataTypes.DoubleType)) {
    doTest(DEFAULT_PERCENTILES, 100)
  }

  testSparkResultsAreEqual("Approx percentile with grouping, doubles, delta 1000",
    spark => salaries(spark, DataTypes.DoubleType)) {
    doTest(DEFAULT_PERCENTILES, 1000)
  }

  testSparkResultsAreEqual("Approx percentile with grouping, doubles, default delta",
    spark => salaries(spark, DataTypes.DoubleType)) {
    doTest(DEFAULT_PERCENTILES, ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)
  }


  private def doTest(
      percentiles: Array[Double],
      delta: Int,
      func: String = "approx_percentile"): DataFrame => DataFrame = {
    df => {
      val percentileArg = if (percentiles.length > 1) {
        s"array(${percentiles.mkString(", ")})"
      } else {
        s"${percentiles.head}"
      }
      df.groupBy(col("dept"))
        .agg(expr(s"$func(salary, $percentileArg, $delta)")
          .as("approx_percentiles"))
        .orderBy("dept")
    }
  }

  private def salaries(spark: SparkSession, salaryDataType: DataType): DataFrame = {
    import spark.implicits._
    val rand = new Random(0)
    val base = salaryDataType match {
      case DataTypes.DoubleType => 1d
      case DataTypes.IntegerType => 1
      case DataTypes.LongType => 1L
    }
    Range(0, 250).flatMap(_ => Seq(
      ("a", 1000 * base + rand.nextInt(1000)),
      ("b", 10000 * base + rand.nextInt(10000)),
      ("c", 100000 * base + rand.nextInt(100000)),
      ("d", 1000000 * base + rand.nextInt(1000000))))
      .toDF("dept", "salary").repartition(4)
  }

}
