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

  testSparkResultsAreEqual("nanvl lit col", mixedFloatDf) {
    frame => frame.select(nanvl(lit(Float.NaN), col("floats")))
  }

  testSparkResultsAreEqual("nanvl col col", mixedFloatDf) {
    frame => frame.select(nanvl(col("floats"), col("more_floats")))
  }

  testSparkResultsAreEqual("nanvl col lit", mixedFloatDf) {
    frame => frame.select(nanvl(col("floats"), lit(1.0)))
  }

  testSparkResultsAreEqual("coalesce col lit", mixedFloatDf) {
    frame => frame.select(coalesce(col("floats"), lit(1.0)))
  }

  testSparkResultsAreEqual("coalesce lit col lit", mixedFloatDf) {
    frame => frame.select(coalesce(lit(null), col("floats"), lit(1.0)))
  }

  testSparkResultsAreEqual("coalesce col col", mixedFloatDf) {
    frame => frame.select(coalesce(col("more_floats"), col("floats")))
  }

  testSparkResultsAreEqual("coalesce string string", nullableStringsDf) {
    frame => frame.select(coalesce(col("more_strings"), col("strings")))
  }

  testSparkResultsAreEqual("nvl col col", mixedFloatDf) {
    frame => frame.selectExpr("nvl(floats, more_floats)")
  }

  testSparkResultsAreEqual("nvl null col", mixedFloatDf) {
    frame => frame.selectExpr("nvl(NULL, more_floats)")
  }

  testSparkResultsAreEqual("nvl col lit", mixedFloatDf) {
    frame => frame.selectExpr("nvl(floats, 1.0)")
  }

  testSparkResultsAreEqual("nvl2 col col lit", mixedFloatDf) {
    frame => frame.selectExpr("nvl2(floats, more_floats, 2.0)")
  }

  testSparkResultsAreEqual("nvl2 null col lit", mixedFloatDf) {
    frame => frame.selectExpr("nvl2(NULL, more_floats, 2.0)")
  }

  testSparkResultsAreEqual("ifnull col col", mixedFloatDf) {
    frame => frame.selectExpr("ifnull(floats, more_floats)")
  }

  testSparkResultsAreEqual("ifnull null col", mixedFloatDf) {
    frame => frame.selectExpr("ifnull(NULL, more_floats)")
  }

  testSparkResultsAreEqual("ifnull col lit", mixedFloatDf) {
    frame => frame.selectExpr("ifnull(floats, 1.0)")
  }

  testSparkResultsAreEqual("nullif col lit - string", doubleStringsDf) {
    frame => frame.selectExpr("nullif(doubles, \"400.0\")")
  }

  testSparkResultsAreEqual("nullif col col - string", doubleStringsDf) {
    frame => frame.selectExpr("nullif(doubles, more_doubles)")
  }

  testSparkResultsAreEqual("nullif col lit - long", nonZeroLongsDf) {
    frame => frame.selectExpr("nullif(longs, 400)")
  }

  testSparkResultsAreEqual("nullif col col - long", nonZeroLongsDf) {
    frame => frame.selectExpr("nullif(longs, more_longs)")
  }

  testSparkResultsAreEqual("project is not null", mixedFloatDf) {
    frame => frame.selectExpr("floats is not null")
  }

  testSparkResultsAreEqual("project is null", mixedFloatDf) {
    frame => frame.selectExpr("floats is null")
  }

  testSparkResultsAreEqual("project is null col1 OR is null col2", mixedFloatDf) {
    frame => frame.selectExpr("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("Test literal values in select", mixedFloatDf) {
    frame => frame.select(col("floats"), lit(100), lit("hello, world!"))
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
    conf = forceHostColumnarToGpu()) {
    frame => frame.select("time")
  }

  testSparkResultsAreEqual("IsNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.selectExpr("time is null")
  }

  testSparkResultsAreEqual("IsNotNull timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.selectExpr("time is not null")
  }

  testSparkResultsAreEqual("year timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.selectExpr("year(time)")
  }

  testSparkResultsAreEqual("month timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
    frame => frame.selectExpr("month(time)")
  }

  testSparkResultsAreEqual("day timestamp", frameFromParquet("timestamp-date-test.parquet"),
    conf = forceHostColumnarToGpu()) {
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

  testSparkResultsAreEqual("SQL IN floats", mixedFloatDf) {
    frame => frame.selectExpr("floats IN (12345, -100.0, -500.0)")
  }

  testSparkResultsAreEqual("SQL IN nullable floats", mixedFloatDf) {
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
    frame => frame.selectExpr(
      """time IN (TIMESTAMP '1900-05-05 12:34:56.108', TIMESTAMP '1900-05-05 12:34:56.118')""")
  }

  testSparkResultsAreEqual("SQL IF booleans", booleanDf) {
    frame => frame.selectExpr("IF(bools, 'true', 'false')")
  }

  testSparkResultsAreEqual("SQL IF nullable booleans", booleanWithNullsDf) {
    frame => frame.selectExpr("IF(bools, 'true', 'false')")
  }

  testSparkResultsAreEqual("SQL IF bytes", bytesDf) {
    frame => frame.selectExpr("IF(bytes < 2, 7, 11)")
  }

  testSparkResultsAreEqual("SQL IF nullable bytes", bytesWithNullsDf) {
    frame => frame.selectExpr("IF(bytes < 2, 7, 11)")
  }

  testSparkResultsAreEqual("SQL IF shorts", shortsDf) {
    frame => frame.selectExpr("IF(shorts - 5 > 0, 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF nullable shorts", shortsWithNullsDf) {
    frame => frame.selectExpr("IF(shorts - 5 > 0, 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF ints", mixedDf) {
    frame => frame.selectExpr("IF(ints == -200, ints + 4, ints - 3)")
  }

  testSparkResultsAreEqual("SQL IF nullable ints", mixedDfWithNulls) {
    frame => frame.selectExpr("IF(ints == -200, ints + 4, ints - 3)")
  }

  testSparkResultsAreEqual("SQL IF longs", mixedDf) {
    frame => frame.selectExpr("IF(longs * 2 == 400, 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF nullable longs", mixedDfWithNulls) {
    frame => frame.selectExpr("IF(longs * 2 == 400, 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF doubles", mixedDf) {
    frame => frame.selectExpr("IF(doubles / 5 > 1, doubles * 2, doubles / 4)")
  }

  testSparkResultsAreEqual("SQL IF nullable doubles", mixedDfWithNulls) {
    frame => frame.selectExpr("IF(doubles / 5 > 1, doubles * 2, doubles / 4)")
  }

  testSparkResultsAreEqual("SQL IF strings", mixedDf) {
    frame => frame.selectExpr("IF(strings > 'E', 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF nullable strings", mixedDfWithNulls) {
    frame => frame.selectExpr("IF(strings > 'E', 'hello', 'world')")
  }

  testSparkResultsAreEqual("SQL IF dates", datesDf) {
    frame => frame.selectExpr("IF(dates == DATE '1900-2-2', DATE '2020-5-5', dates)")
  }

  testSparkResultsAreEqual("SQL IF timestamps", frameFromParquet("timestamp-date-test.parquet")) {
    frame => frame.selectExpr(
      "IF(time != TIMESTAMP '1900-05-05 12:34:56.108', time, TIMESTAMP '1900-05-05 12:34:56.118')")
  }

  testSparkResultsAreEqual("SQL CASE WHEN booleans", booleanDf) {
    frame => frame.selectExpr("CASE WHEN NOT bools THEN TRUE END")
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable booleans", booleanWithNullsDf) {
    frame => frame.selectExpr("CASE WHEN bools THEN NOT bools ELSE bools END")
  }

  testSparkResultsAreEqual("SQL CASE WHEN bytes", bytesDf) {
    frame => frame.selectExpr(
      "CASE WHEN bytes = 2 THEN bytes + 3 WHEN bytes > 2 THEN bytes - 3 ELSE 0 END")
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable bytes", bytesWithNullsDf) {
    frame => frame.selectExpr(
      "CASE WHEN bytes = 2 THEN bytes + 3 WHEN bytes > 2 THEN bytes - 3 END")
  }

  testSparkResultsAreEqual("SQL CASE WHEN shorts", shortsDf) {
    frame => frame.selectExpr(
      """CASE WHEN shorts = 2 THEN shorts + 3
        | WHEN shorts > 2 THEN shorts - 3
        | WHEN shorts / 2 < -10 THEN 5 ELSE 0 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable shorts", shortsWithNullsDf) {
    frame => frame.selectExpr(
      """CASE WHEN shorts = 2 THEN shorts + 3
        | WHEN shorts > 2 THEN shorts - 3
        | WHEN shorts / 2 < -10 THEN 5 ELSE 0 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN ints", mixedDf) {
    frame => frame.selectExpr(
      """CASE WHEN ints = 92 THEN ints + 3
        | WHEN ints >= 98 THEN 100
        | WHEN ints / 9 > 10 THEN 5 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable ints", mixedDfWithNulls) {
    frame => frame.selectExpr(
      """CASE WHEN ints = 92 THEN ints + 3
        | WHEN ints >= 98 THEN 100
        | WHEN ints / 9 > 10 THEN 5 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN longs", mixedDf) {
    frame => frame.selectExpr(
      """CASE WHEN longs IN (300, 400, 500) THEN longs * longs
        | WHEN longs >= 98 THEN 100
        | WHEN longs / 9 > 10 THEN longs END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable longs", mixedDfWithNulls) {
    frame => frame.selectExpr(
      """CASE WHEN longs IN (300, 400, 500) THEN longs * longs
        | WHEN longs >= 98 THEN 100
        | WHEN longs / 9 > 10 THEN longs
        | ELSE 0 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN floats", mixedFloatDf) {
    frame => frame.selectExpr(
      """CASE WHEN more_floats < 5 THEN floats * 2
        | WHEN floats >= 300 THEN more_floats * 2
        | ELSE floats * more_floats END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable floats", mixedFloatDf) {
    frame => frame.selectExpr(
      """CASE WHEN more_floats < 5 THEN floats * 2
        | WHEN floats >= 300 THEN more_floats * 2
        | ELSE floats * more_floats END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN doubles", mixedDf) {
    frame => frame.selectExpr(
      """CASE WHEN longs IN (300, 400, 500) THEN doubles * longs
        | WHEN longs >= 98 THEN doubles
        | WHEN doubles > 8 THEN (doubles * doubles) / 8 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable doubles", mixedDfWithNulls) {
    frame => frame.selectExpr(
      """CASE WHEN longs IN (300, 400, 500) THEN doubles * longs
        | WHEN longs >= 98 THEN doubles
        | WHEN doubles > 8 THEN (doubles * doubles) / 8 END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN strings", mixedDf) {
    frame => frame.selectExpr(
      """CASE WHEN strings >= 'E' AND strings < 'F' THEN strings
        | WHEN strings = 'B' THEN 'hello'
        | WHEN strings = 'C' THEN 'world'
        | WHEN strings = 'G' THEN 'how'
        | WHEN strings = 'D' THEN 'are'
        | WHEN strings = 'A' THEN 'you'
        | ELSE NULL END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN nullable strings", mixedDfWithNulls) {
    frame => frame.selectExpr(
      """CASE WHEN strings >= 'E' AND strings < 'F' THEN strings
        | WHEN strings = 'B' THEN 'hello'
        | WHEN strings = 'C' THEN 'world'
        | WHEN strings = 'G' THEN 'how'
        | WHEN strings = 'D' THEN 'are'
        | WHEN strings = 'A' THEN 'you'
        | ELSE 'null' END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN dates", datesDf) {
    frame => frame.selectExpr(
      """CASE WHEN dates == DATE '1900-2-2' THEN DATE '2020-5-5'
        | ELSE dates END""".stripMargin)
  }

  testSparkResultsAreEqual("SQL CASE WHEN timestamps",
    frameFromParquet("timestamp-date-test.parquet")) {
    frame => frame.selectExpr(
      """CASE WHEN time <= TIMESTAMP '1900-05-05 12:34:56.108' THEN time
        | ELSE NULL END""".stripMargin)
  }

  def mixedFloatSingleSeq(): Seq[java.lang.Float] = {
    Seq[java.lang.Float](
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      0.8435376941f,
      23.1927672582f,
      2309.4430349398f,
      Float.NaN,
      null,
      -0.7078783860f,
      -70.9667587507f,
      -838600.5867225748f
    )
  }

  def mixedFloatSingleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    mixedDoubleSingleSeq().toDF("floats")
  }

  def mixedDoubleSingleSeq(): Seq[java.lang.Double] = {
    Seq[java.lang.Double](
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.8435376941d,
      23.1927672582d,
      2309.4430349398d,
      Double.NaN,
      null,
      -0.7078783860d,
      -70.9667587507d,
      -838600.5867225748d
    )
  }

  def mixedDoubleSingleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    mixedDoubleSingleSeq().toDF("doubles")
  }

  testSparkResultsAreEqual("SQL IF floats eq more_floats", mixedFloatDf) {
    frame => frame.selectExpr("IF(floats = more_floats, floats * 5, -1234567)")
  }

  testSparkResultsAreEqual("SQL IF doubles eq more_doubles", mixedDoubleDf) {
    frame => frame.selectExpr("IF(doubles = more_doubles, doubles * 5, -1234567)")
  }

  // Float comparisons
  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF float('%f') = floats".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(float('%f') = floats, floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF float('%f') > floats".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(float('%f') > floats, floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF float('%f') >= floats".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(float('%f') >= floats, floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF float('%f') < floats".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(float('%f') < floats, floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF float('%f') <= floats".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(float('%f') <= floats, floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF floats eq float('%f')".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(floats = float('%f'), floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF floats > float('%f')".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(floats > float('%f'), floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF floats >= float('%f')".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(floats >= float('%f'), floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF floats < float('%f')".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(floats < float('%f'), floats * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedFloatSingleSeq()) {
    testSparkResultsAreEqual("SQL IF floats <= float('%f')".format(d), mixedFloatSingleDf) {
      frame => frame.selectExpr("IF(floats <= float('%f'), floats * 5, -1234567)".format(d))
    }
  }

  // Double comparisons
  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF double('%f') = doubles".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(double('%f') = doubles, doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF double('%f') > doubles".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(double('%f') > doubles, doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF double('%f') >= doubles".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(double('%f') >= doubles, doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF double('%f') < doubles".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(double('%f') < doubles, doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF double('%f') <= doubles".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(double('%f') <= doubles, doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF doubles eq double('%f')".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(doubles = double('%f'), doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF doubles > double('%f')".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(doubles > double('%f'), doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF doubles >= double('%f')".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(doubles >= double('%f'), doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF doubles < double('%f')".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(doubles < double('%f'), doubles * 5, -1234567)".format(d))
    }
  }

  for (d <- mixedDoubleSingleSeq()) {
    testSparkResultsAreEqual("SQL IF doubles <= double('%f')".format(d), mixedDoubleSingleDf) {
      frame => frame.selectExpr("IF(doubles <= double('%f'), doubles * 5, -1234567)".format(d))
    }
  }
}
