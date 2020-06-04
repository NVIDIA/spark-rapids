/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  test("rand is okay") {
    // We cannot test that the results are exactly equal because our random number
    // generator is not identical to spark, so just make sure it does not crash
    // and all of the numbers are in the proper range
    withGpuSparkSession(session => {
      val df = nullableFloatCsvDf(session)
      val data = df.select(col("floats"), rand().as("RANDOM")).collect()
      data.foreach(row => {
        val d = row.getDouble(1)
        assert(d < 1.0)
        assert(d >= 0.0)
      })
    })
  }

  testSparkResultsAreEqual("Test literal values in select", mixedFloatDf) {
    frame => frame.select(col("floats"), lit(100), lit("hello, world!"))
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.select("time")
  }

  def booleanWithNullsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Boolean](
      true,
      false,
      true,
      false,
      null,
      null,
      true,
      false
    ).toDF("bools")
  }

  def bytesDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[Byte](
      0.toByte,
      2.toByte,
      3.toByte,
      (-1).toByte,
      (-10).toByte,
      (-128).toByte,
      127.toByte
    ).toDF("bytes")
  }

  def bytesWithNullsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Byte](
      0.toByte,
      2.toByte,
      3.toByte,
      (-1).toByte,
      (-10).toByte,
      (-128).toByte,
      127.toByte,
      null,
      null,
      0.toByte
    ).toDF("bytes")
  }

  def shortsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[Short](
      0.toShort,
      23456.toShort,
      3.toShort,
      (-1).toShort,
      (-10240).toShort,
      (-32768).toShort,
      32767.toShort
    ).toDF("shorts")
  }

  def shortsWithNullsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Short](
      0.toShort,
      23456.toShort,
      3.toShort,
      (-1).toShort,
      (-10240).toShort,
      (-32768).toShort,
      32767.toShort,
      null,
      null,
      0.toShort
    ).toDF("shorts")
  }

  testSparkResultsAreEqual("input_file_name", longsFromMultipleCSVDf, repart = 0) {
    // The filter forces a coalesce so we can test that we disabled coalesce properly in this case
    frame => frame.filter(col("longs") > 0).select(col("longs"), input_file_name())
  }

  testSparkResultsAreEqual("input_file_block_start", longsFromMultipleCSVDf, repart = 0) {
    // The filter forces a coalesce so we can test that we disabled coalesce properly in this case
    frame => frame.filter(col("longs") > 0).selectExpr("longs", "input_file_block_start()")
  }

  testSparkResultsAreEqual("input_file_block_length", longsFromMultipleCSVDf, repart = 0) {
    // The filter forces a coalesce so we can test that we disabled coalesce properly in this case
    frame => frame.filter(col("longs") > 0).selectExpr("longs", "input_file_block_length()")
  }

  testSparkResultsAreEqual("monotonically_increasing_id", shortsDf) {
    frame => frame.select(col("shorts"), monotonically_increasing_id())
  }

  testSparkResultsAreEqual("spark_partition_id", shortsDf) {
    frame => frame.select(col("shorts"), spark_partition_id())
  }
}
