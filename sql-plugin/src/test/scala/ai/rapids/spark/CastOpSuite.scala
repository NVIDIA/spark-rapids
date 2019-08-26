/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import java.util.TimeZone

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

class CastOpSuite extends SparkQueryCompareTestSuite {
  private val timestampDatesMsecParquet = frameFromParquet("timestamp-date-test-msec.parquet")

  testSparkResultsAreEqual("Test cast from long", longsDf) {
    frame => frame.select(
      col("longs").cast(IntegerType),
      col("longs").cast(LongType),
      //col("longs").cast(StringType),
      col("more_longs").cast(BooleanType),
      col("more_longs").cast(ByteType),
      col("longs").cast(ShortType),
      col("longs").cast(FloatType),
      col("longs").cast(DoubleType))
  }

  testSparkResultsAreEqual("Test cast from double", doubleDf) {
    frame => frame.select(
      col("doubles").cast(IntegerType),
      col("doubles").cast(LongType),
      //col("doubles").cast(StringType),
      col("more_doubles").cast(BooleanType),
      col("more_doubles").cast(ByteType),
      col("doubles").cast(ShortType),
      col("doubles").cast(FloatType),
      col("doubles").cast(DoubleType))
  }

  testSparkResultsAreEqual("Test cast from boolean", booleanDf) {
    frame => frame.select(
      col("bools").cast(IntegerType),
      col("bools").cast(LongType),
      //col("bools").cast(StringType),
      col("more_bools").cast(BooleanType),
      col("more_bools").cast(ByteType),
      col("bools").cast(ShortType),
      col("bools").cast(FloatType),
      col("bools").cast(DoubleType))
  }

  testSparkResultsAreEqual("Test cast from date", timestampDatesMsecParquet) {
    frame => frame.select(
      col("date"),
      col("date").cast(BooleanType),
      col("date").cast(ByteType),
      col("date").cast(ShortType),
      col("date").cast(IntegerType),
      col("date").cast(LongType),
      col("date").cast(FloatType),
      col("date").cast(DoubleType),
      col("date").cast(LongType),
      col("date").cast(TimestampType))
   }

  private val timestampCastFn = { frame: DataFrame =>
    frame.select(
      col("time"),
      col("time").cast(BooleanType),
      col("time").cast(ByteType),
      col("time").cast(ShortType),
      col("time").cast(IntegerType),
      col("time").cast(LongType),
      col("time").cast(FloatType),
      col("time").cast(DoubleType),
      col("time").cast(LongType),
      col("time").cast(DateType))
  }

  testSparkResultsAreEqual("Test cast from timestamp", timestampDatesMsecParquet)(timestampCastFn)

  test("Test cast from timestamp in UTC-equivalent timezone") {
    val oldtz = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC-0"))
      val (fromCpu, fromGpu) = runOnCpuAndGpu(timestampDatesMsecParquet, timestampCastFn)
      compareResults(sort=false, 0, fromCpu, fromGpu)
    } finally {
      TimeZone.setDefault(oldtz)
    }
  }

  //  testSparkResultsAreEqual("Test cast from strings", doubleStringsDf) {
  //    frame => frame.select(
  //      col("doubles").cast(DoubleType))
  //  }
}
