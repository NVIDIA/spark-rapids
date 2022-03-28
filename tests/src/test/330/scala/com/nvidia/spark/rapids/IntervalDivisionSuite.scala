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

import scala.util.Random

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

// TODO Currently Pyspark does not support YearMonthIntervalType,
// should move this to integration tests after Spark supports YearMonthIntervalType
class IntervalDivisionSuite extends SparkQueryCompareTestSuite {

  testSparkResultsAreEqual(
    "test year-month interval / num, normal case",
    spark => {
      val data = Seq(
        Row(Period.ofMonths(5), 3.toByte, 2.toShort, 1, 4L, 2.5f, 2.6d),
        Row(Period.ofMonths(5), null, null, null, null, null, null),
        Row(null, 3.toByte, 3.toShort, 3, 3L, 1.1f, 2.7d),
        Row(Period.ofMonths(0), 3.toByte, 3.toShort, 3, 3L, 3.1f, 3.4d),
        Row(Period.ofMonths(0), Byte.MinValue, Short.MinValue, Int.MinValue, Long.MinValue,
          Float.MinValue, Double.MinValue),
        Row(Period.ofMonths(0),
          Byte.MaxValue, Short.MaxValue, Int.MaxValue, Long.MaxValue,
          Float.MaxValue, Double.MaxValue),
        Row(Period.ofMonths(7), 4.toByte, 3.toShort, 2, 1L,
          Float.NegativeInfinity, Double.NegativeInfinity),
        Row(Period.ofMonths(7), 6.toByte, 5.toShort, 4, 3L,
          Float.PositiveInfinity, Double.PositiveInfinity),
      )
      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_b", ByteType),
        StructField("c_s", ShortType),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)
      ))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }) {
    df =>
      df.selectExpr(
        "c_ym / c_b", "c_ym / c_s", "c_ym / c_i", "c_ym / c_l", "c_ym / c_f", "c_ym / c_d",
        "c_ym / cast('15' as byte)", "c_ym / cast('15' as short)", "c_ym / 15",
        "c_ym / 15L", "c_ym / 15.1f", "c_ym / 15.1d",
        "interval '55' month / c_b",
        "interval '15' month / c_s",
        "interval '25' month / c_i",
        "interval '15' month / c_l",
        "interval '25' month / c_f",
        "interval '35' month / c_d")
  }

  testSparkResultsAreEqual("test year-month interval / float num, normal case",
    spark => {
      val numRows = 1024
      val r = new Random(0)

      def getSign: Int = if (r.nextBoolean()) 1 else -1

      val data = (0 until numRows).map(i => {
        Row(Period.ofMonths(getSign * r.nextInt(Int.MaxValue)),
          getSign * (r.nextFloat() + 10f), // add 10 to overflow
          getSign * (r.nextDouble() + 10d))
      })

      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_f", FloatType),
        StructField("c_d", DoubleType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("c_ym / c_f", "c_ym / c_d")
  }

  testSparkResultsAreEqual("test year-month interval / int num, normal case",
    spark => {
      val numRows = 1024
      val r = new Random(0)

      def getSign: Int = if (r.nextBoolean()) 1 else -1

      val data = (0 until numRows).map(i => {
        Row(Period.ofMonths(getSign * r.nextInt(Int.MaxValue)),
          getSign * r.nextInt(1024 * 1024) + 1, // add 1 to avoid dividing 0
          (getSign * r.nextInt(1024 * 1024) + 1).toLong)
      })

      val schema = StructType(Seq(
        StructField("c_ym", YearMonthIntervalType()),
        StructField("c_i", IntegerType),
        StructField("c_l", LongType)))
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    }
  ) {
    df => df.selectExpr("c_ym / c_i", "c_ym / c_l")
  }

  // both gpu and cpu will throw ArithmeticException
  def testDivideYMOverflow(testCaseName: String, months: Int, num: Number): Unit = {
    testBothCpuGpuExpectedException[SparkException](testCaseName + ", cv / scalar",
      e => e.getMessage.contains("ArithmeticException"),
      spark => {
        val data = Seq(Row(Period.ofMonths(months)))
        val schema = StructType(Seq(
          StructField("c1", YearMonthIntervalType())))
        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df =>
        if (num.isInstanceOf[Byte]) {
          df.selectExpr(s"c1 / cast('$num' as Byte)")
        } else if (num.isInstanceOf[Short]) {
          df.selectExpr(s"c1 / cast('$num' as Short)")
        } else if (num.isInstanceOf[Int]) {
          df.selectExpr(s"c1 / $num")
        } else if (num.isInstanceOf[Long]) {
          df.selectExpr(s"c1 / ${num}L")
        } else if (num.isInstanceOf[Float]) {
          if (num.equals(Float.NaN)) {
            df.selectExpr("c1 / cast('NaN' as float)")
          } else if (num.equals(Float.PositiveInfinity)) {
            df.selectExpr("c1 / cast('Infinity' as float)")
          }
          else if (num.equals(Float.NegativeInfinity)) {
            df.selectExpr("c1 / cast('-Infinity' as float)")
          } else {
            df.selectExpr(s"c1 / ${num}f")
          }
        } else if (num.isInstanceOf[Double]) {
          if (num.equals(Double.NaN)) {
            df.selectExpr("c1 / cast('NaN' as double)")
          } else if (num.equals(Double.PositiveInfinity)) {
            df.selectExpr("c1 / cast('Infinity' as double)")
          }
          else if (num.equals(Double.NegativeInfinity)) {
            df.selectExpr("c1 / cast('-Infinity' as double)")
          } else {
            df.selectExpr(s"c1 / ${num}d")
          }
        } else {
          throw new IllegalStateException("")
        }
    }

    testBothCpuGpuExpectedException[SparkException](testCaseName + ", cv / cv",
      e => e.getMessage.contains("ArithmeticException"),
      spark => {
        // Period is the external type of year-month type
        val (data, schema) =
          if (num.isInstanceOf[Byte]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Byte])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", ByteType)
                )))
          } else if (num.isInstanceOf[Short]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Short])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", ShortType)
                )))
          } else if (num.isInstanceOf[Int]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Int])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", IntegerType)
                )))
          } else if (num.isInstanceOf[Long]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Long])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", LongType)
                )))
          } else if (num.isInstanceOf[Float]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Float])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", FloatType)
                )))
          } else if (num.isInstanceOf[Double]) {
            (Seq(Row(Period.ofMonths(months), num.asInstanceOf[Double])),
                StructType(Seq(
                  StructField("c1", YearMonthIntervalType()),
                  StructField("c2", DoubleType)
                )))
          } else {
            throw new IllegalStateException("")
          }

        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"c1 / c2")
    }

    testBothCpuGpuExpectedException[SparkException](testCaseName + ", scalar / cv",
      e => e.getMessage.contains("ArithmeticException"),
      spark => {
        val (data, schema) =
          if (num.isInstanceOf[Byte]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", ByteType)
                )))
          } else if (num.isInstanceOf[Short]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", ShortType)
                )))
          } else if (num.isInstanceOf[Int]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", IntegerType)
                )))
          } else if (num.isInstanceOf[Long]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", LongType)
                )))
          } else if (num.isInstanceOf[Float]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", FloatType)
                )))
          } else if (num.isInstanceOf[Double]) {
            (Seq(Row(num)),
                StructType(Seq(
                  StructField("c1", DoubleType)
                )))
          } else {
            throw new IllegalStateException("")
          }

        spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
      }
    ) {
      df => df.selectExpr(s"interval '$months' month / c1")
    }
  }

  // divide by 0
  testDivideYMOverflow("year-month divide 0.toByte", 1, 0.toByte)
  testDivideYMOverflow("year-month divide 0.toShort", 1, 0.toShort)
  testDivideYMOverflow("year-month divide 0", 1, 0)
  testDivideYMOverflow("year-month divide 0L", 1, 0.toLong)
  testDivideYMOverflow("year-month divide 0.0f", 1, 0.0f)
  testDivideYMOverflow("year-month divide 0.0d", 1, 0.0d)
  testDivideYMOverflow("year-month divide -0.0f", 1, -0.0f)
  testDivideYMOverflow("year-month divide -0.0d", 1, -0.0d)

  // NaN
  testDivideYMOverflow("year-month divide Float.NaN", 1, Float.NaN)
  testDivideYMOverflow("year-month divide Double.NaN", 1, Double.NaN)
  // 0.0 / 0.0 = NaN
  testDivideYMOverflow("year-month 0 divide 0.0f", 0, 0.0f)
  testDivideYMOverflow("year-month 0 divide 0.0d", 0, 0.0d)

  // divide float/double overflow
  testDivideYMOverflow("year-month divide overflow 1", Int.MaxValue, 0.1f)
  testDivideYMOverflow("year-month divide overflow 2", Int.MaxValue, 0.1d)
  testDivideYMOverflow("year-month divide overflow 3", Int.MinValue, 0.1f)
  testDivideYMOverflow("year-month divide overflow 4", Int.MinValue, 0.1d)
}
