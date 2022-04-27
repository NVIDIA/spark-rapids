/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.time.Period

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Can not put this suite to Pyspark test cases
 * because of currently Pyspark not have year-month type.
 * See: https://github.com/apache/spark/blob/branch-3.3/python/pyspark/sql/types.py
 * Should move the year-month scala test cases to the integration test module,
 * filed an issue to track: https://github.com/NVIDIA/spark-rapids/issues/5212
 */
class IntervalCastSuite extends SparkQueryCompareTestSuite {
  testSparkResultsAreEqual(
    "test cast year-month to integral",
    spark => {
      val data = (-128 to 127).map(i => Row(Period.ofMonths(i)))
      val schema = StructType(Seq(StructField("c_ym", YearMonthIntervalType())))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr("cast(c_ym as long)", "cast(c_ym as integer)", "cast(c_ym as short)",
        "cast(c_ym as byte)")
  }

  testSparkResultsAreEqual(
    "test cast integral to year-month",
    spark => {
      val data = (-128 to 127).map(i => Row(i.toLong, i, i.toShort, i.toByte))
      val schema = StructType(Seq(StructField("c_l", LongType),
        StructField("c_i", IntegerType),
        StructField("c_s", ShortType),
        StructField("c_b", ByteType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr("cast(c_l as interval month)", "cast(c_i as interval year)",
        "cast(c_s as interval year)", "cast(c_b as interval month)")
  }

  val toIntegralOverflowPairs = Array(
    (Byte.MinValue - 1, "byte"),
    (Byte.MaxValue + 1, "byte"),
    (Short.MinValue - 1, "short"),
    (Short.MinValue - 1, "short"))
  var testLoop = 1
  toIntegralOverflowPairs.foreach { case (months, toType) =>
    testBothCpuGpuExpectedException[SparkException](
      s"test cast year-month to integral, overflow $testLoop",
      e => e.getMessage.contains("overflow"),
      spark => {
        val data = Seq(Row(Period.ofMonths(months)))
        val schema = StructType(Seq(StructField("c_ym", YearMonthIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }) {
      df =>
        df.selectExpr(s"cast(c_ym as $toType)")
    }
    testLoop += 1
  }

  val toYMOverflows = Array(
    (Int.MinValue / 12 - 1, IntegerType, "year"),
    (Int.MaxValue / 12 + 1, IntegerType, "year"),
    (Int.MinValue - 1L, LongType, "month"),
    (Int.MaxValue + 1L, LongType, "month"),
    (Int.MinValue / 12 - 1L, LongType, "year"),
    (Int.MaxValue / 12 + 1L, LongType, "year"))
  testLoop = 1
  toYMOverflows.foreach { case (integral, integralType, toField) =>
    testBothCpuGpuExpectedException[SparkException](
      s"test cast integral to year-month, overflow $testLoop",
      e => e.getMessage.contains("overflow"),
      spark => {
        val data = Seq(Row(integral))
        val schema = StructType(Seq(StructField("c_integral", integralType)))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }) {
      df =>
        df.selectExpr(s"cast(c_integral as interval $toField)")
    }
    testLoop += 1
  }
}
