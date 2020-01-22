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
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProjectExprSuite extends SparkQueryCompareTestSuite {
  def forceHostColumnarToGpu(): SparkConf = {
    // turns off BatchScanExec, so we get a CPU BatchScanExec together with a HostColumnarToGpu
    new SparkConf().set("spark.rapids.sql.exec.BatchScanExec", "false")
  }

  testSparkResultsAreEqual("project is not null", nullableFloatDf) {
    frame => frame.selectExpr("floats is not null")
  }

  testSparkResultsAreEqual("project is null", nullableFloatDf) {
    frame => frame.selectExpr("floats is null")
  }

  testSparkResultsAreEqual("project is null col1 OR is null col2", nullableFloatDf) {
    frame => frame.selectExpr("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("Test literal values in select", floatDf) {
    frame => frame.select(col("floats"), lit(100))
  }

  testSparkResultsAreEqual("IsNotNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is not null")
  }

  testSparkResultsAreEqual("IsNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is null")
  }

  testSparkResultsAreEqual("IsNull OR IsNull strings", nullableStringsDf) {
    frame => frame.selectExpr("strings is null OR more_strings is null")
  }

  testSparkResultsAreEqual("equals strings", nullableStringsDf) {
    frame => frame.selectExpr("strings = \"500.0\"")
  }

  testSparkResultsAreEqual("project time", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.select("time")
  }

  testSparkResultsAreEqual("IsNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("time is null")
  }

  testSparkResultsAreEqual("IsNotNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("time is not null")
  }

  testSparkResultsAreEqual("year timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("year(time)")
  }

  testSparkResultsAreEqual("month timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("month(time)")
  }

  testSparkResultsAreEqual("day timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu(),
    allowNonGpu = true) {
    frame => frame.selectExpr("day(time)")
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

  testSparkResultsAreEqual("SQL IN booleans", booleanDf) {
    frame => frame.selectExpr("bools IN (false)")
  }

  testSparkResultsAreEqual("SQL IN nullable booleans", booleanWithNullsDf) {
    frame => frame.selectExpr("bools IN (false)")
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

  testSparkResultsAreEqual("SQL IN bytes", bytesDf) {
    frame => frame.selectExpr("bytes IN (-128, 127, 0)")
  }

  testSparkResultsAreEqual("SQL IN nullable bytes", bytesWithNullsDf) {
    frame => frame.selectExpr("bytes IN (-128, 127, 0, -5)")
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

  testSparkResultsAreEqual("SQL IN shorts", shortsDf) {
    frame => frame.selectExpr("shorts IN (-10240, 23456, 100, 3)")
  }

  testSparkResultsAreEqual("SQL IN nullable shorts", shortsWithNullsDf) {
    frame => frame.selectExpr("shorts IN (-10240, 23456, 100, 3)")
  }

  testSparkResultsAreEqual("SQL IN ints", mixedDf) {
    frame => frame.selectExpr("ints IN (-200, 96, 98)")
  }

  testSparkResultsAreEqual("SQL IN nullable ints", mixedDfWithNulls) {
    frame => frame.selectExpr("ints IN (-200, 96, 98)")
  }

  testSparkResultsAreEqual("SQL IN longs", mixedDf) {
    frame => frame.selectExpr("longs IN (1000, 200, -500)")
  }

  testSparkResultsAreEqual("SQL IN nullable longs", mixedDfWithNulls) {
    frame => frame.selectExpr("longs IN (1000, 200, -500)")
  }

  testSparkResultsAreEqual("SQL IN floats", floatDf) {
    frame => frame.selectExpr("floats IN (12345, -100.0, -500.0)")
  }

  testSparkResultsAreEqual("SQL IN nullable floats", nullableFloatDf) {
    frame => frame.selectExpr("floats IN (12345, 5.0, 0.0)")
  }

  testSparkResultsAreEqual("SQL IN doubles", mixedDf) {
    frame => frame.selectExpr("doubles IN (12345, 5.0, 0.0)")
  }

  testSparkResultsAreEqual("SQL IN nullable doubles", mixedDfWithNulls) {
    frame => frame.selectExpr("doubles IN (12345, 5.0, 0.0)")
  }

  testSparkResultsAreEqual("SQL IN strings", mixedDf) {
    frame => frame.selectExpr("strings IN ('B', 'C', 'Z', 'IJ\"\u0100\u0101\u0500\u0501', 'E')")
  }

  testSparkResultsAreEqual("SQL IN nullable strings", mixedDfWithNulls) {
    frame => frame.selectExpr("strings IN ('B', 'C', 'Z', 'E\u0480\u0481', 'E')")
  }

  testSparkResultsAreEqual("SQL IN dates", datesDf) {
    frame => frame.selectExpr("""dates IN (DATE '1900-2-2', DATE '2020-5-5')""")
  }

  testSparkResultsAreEqual("SQL IN timestamps", frameFromParquet("timestamp-date-test.parquet")) {
    frame => frame.selectExpr("""time IN (TIMESTAMP '1900-05-05 12:34:56.108', TIMESTAMP '1900-05-05 12:34:56.118')""")
  }
}
