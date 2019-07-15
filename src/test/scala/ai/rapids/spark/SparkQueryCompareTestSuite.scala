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

import java.sql.Date
import java.util.{Locale, TimeZone}

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Set of tests that compare the output using the CPU version of spark vs our GPU version.
 */
class SparkQueryCompareTestSuite extends FunSuite with BeforeAndAfterEach {

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  private def cleanupAnyExistingSession(): Unit = {
    val session = SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession)
    if (session.isDefined) {
      session.get.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def withSparkSession[U](appName: String, conf: SparkConf, f: SparkSession => U): U = {
    cleanupAnyExistingSession()
    val session = SparkSession.builder()
      .master("local[2]")
      .appName(appName)
      .config(conf)
      .getOrCreate()

    try {
      f(session)
    } finally {
      session.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  def withGpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    var c = conf.set("spark.sql.extensions", "ai.rapids.spark.Plugin")

    if (c.getOption(Plugin.TEST_CONF).isEmpty) {
       c = c.set(Plugin.TEST_CONF, "true")
    }
    withSparkSession("gpu-sql-test", c, f)
  }

  def withCpuSparkSession[U](f: SparkSession => U): U = {
    withSparkSession("cpu-sql-test", new SparkConf(), f)
  }

  private def compare(obj1: Any, obj2: Any, maxFloatDiff: Double = 0.0): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r, maxFloatDiff)}
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey), maxFloatDiff))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r, maxFloatDiff)}
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq, maxFloatDiff)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq, maxFloatDiff)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) if maxFloatDiff <= 0 =>
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Double, b: Double) if maxFloatDiff > 0 =>
      val ret = (Math.abs(a - b) <= maxFloatDiff)
      if (!ret) {
        System.err.println(s"\n\nABS(${a} - ${b}) == ${Math.abs(a - b)} is not <= ${maxFloatDiff} (double)")
      }
      ret
    case (a: Float, b: Float) if maxFloatDiff <= 0 =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a: Float, b: Float) if maxFloatDiff > 0 =>
      val ret = (Math.abs(a - b) <= maxFloatDiff)
      if (!ret) {
        System.err.println(s"\n\nABS(${a} - ${b}) == ${Math.abs(a - b)} is not <= ${maxFloatDiff} (float)")
      }
      ret
    case (a, b) => a == b
  }

  /**
    * Runs a test defined by fun, using dataframe df.
    *
    * @param df     - the DataFrame to use as input
    * @param fun    - the function to transform the DataFrame (produces another DataFrame)
    * @param conf   - spark conf
    * @return       - tuple of (cpu results, gpu results) as arrays of Row
    */
  def runOnCpuAndGpu(df: SparkSession => DataFrame, fun: DataFrame => DataFrame,
      conf: SparkConf = new SparkConf()): (Array[Row], Array[Row]) = {
    val fromCpu = withCpuSparkSession((session) => {
      // repartition the data so it is turned into a projection, not folded into the table scan exec
      fun(df(session).repartition(1)).collect
    })

    val fromGpu = withGpuSparkSession((session) => {
      // repartition the data so it is turned into a projection, not folded into the table scan exec
      fun(df(session).repartition(1)).collect
    }, conf)

    (fromCpu, fromGpu)
  }

  def INCOMPAT_testSparkResultsAreEqual(testName: String, df: SparkSession => DataFrame,
      maxFloatDiff: Double = 0.0, conf: SparkConf = new SparkConf())
    (fun: DataFrame => DataFrame): Unit = {
    test("INCOMPAT: " + testName) {
      val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
        conf.set(Plugin.INCOMPATIBLE_OPS_CONF, "true"))

      if (!compare(fromCpu, fromGpu, maxFloatDiff)) {
        fail(
          s"""
             |Running on the GPU and on the CPU did not match (relaxed float comparison)
             |CPU: ${fromCpu.toSeq}
             |GPU: ${fromGpu.toSeq}
         """.stripMargin)
      }
    }
  }

  def ALLOW_NON_GPU_testSparkResultsAreEqual(testName: String, df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf())(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual("NOT ALL ON GPU: " + testName, df,
      conf.set(Plugin.TEST_CONF, "false"))(fun)
  }

  def testSparkResultsAreEqual(testName: String, df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf())(fun: DataFrame => DataFrame): Unit = {
    test(testName) {
      val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun, conf)

      if (!compare(fromCpu, fromGpu)) {
        fail(
          s"""
             |Running on the GPU and on the CPU did not match
             |CPU: ${fromCpu.toSeq}
             |GPU: ${fromGpu.toSeq}
         """.stripMargin)
      }
    }
  }

  def booleanDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(Boolean, Boolean)](
      (true, true),
      (false, true),
      (true, false),
      (false, false)
    ).toDF("bools", "more_bools")
  }

  def datesDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (Date.valueOf("0100-1-1"), Date.valueOf("2100-1-1")),
      (Date.valueOf("0211-1-1"), Date.valueOf("1492-4-7")),
      (Date.valueOf("1900-2-2"), Date.valueOf("1776-7-4")),
      (Date.valueOf("1989-3-3"), Date.valueOf("1808-11-12")),
      (Date.valueOf("2010-4-4"), Date.valueOf("2100-12-30")),
      (Date.valueOf("2020-5-5"), Date.valueOf("2019-6-19")),
      (Date.valueOf("2050-10-30"), Date.valueOf("0100-5-28"))
    ).toDF("dates", "more_dates")
  }

  def longsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 0L)
    ).toDF("longs", "more_longs")
  }

  def nonZeroLongsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 50L)
    ).toDF("longs", "more_longs")
  }

  def doubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100.0, 1.0),
      (200.0, 2.0),
      (300.0, 3.0),
      (400.0, 4.0),
      (500.0, 5.0),
      (-100.0, 6.0),
      (-500.0, 0.0)
    ).toDF("doubles", "more_doubles")
  }

  def nonZeroDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100.0, 1.0),
      (200.0, 2.0),
      (300.0, 3.0),
      (400.0, 4.0),
      (500.0, 5.0),
      (-100.0, 6.0),
      (-500.0, 50.5)
    ).toDF("doubles", "more_doubles")
  }

  def floatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100.0f, 1.0f),
      (200.0f, 2.0f),
      (300.0f, 3.0f),
      (400.0f, 4.0f),
      (500.0f, 5.0f),
      (-100.0f, 6.0f),
      (-500.0f, 0.0f)
    ).toDF("floats", "more_floats")
  }

  def nonZeroFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100.0f, 1.0f),
      (200.0f, 2.0f),
      (300.0f, 3.0f),
      (400.0f, 4.0f),
      (500.0f, 5.0f),
      (-100.0f, 6.0f),
      (-500.0f, 50.5f)
    ).toDF("floats", "more_floats")
  }

  def doubleStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      ("100.0", "1.0"),
      ("200.0", "2.0"),
      ("300.0", "3.0"),
      ("400.0", "4.0"),
      ("500.0", "5.0"),
      ("-100.0", "6.0"),
      ("-500.0", "0.0")
    ).toDF("doubles", "more_doubles")
  }

  def intsFromCsv(session: SparkSession): DataFrame = {
    val schema = StructType(Array(
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)
    ))
    val path = this.getClass.getClassLoader.getResource("test.csv")
    session.read.schema(schema).csv(path.toString)
  }

  def intsFromCsvInferredSchema(session: SparkSession): DataFrame = {
    val path = this.getClass.getClassLoader.getResource("test.csv")
    session.read.option("inferSchema", "true").csv(path.toString)
  }

  def nullableFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (100.0f, 1.0f),
      (200.0f, null),
      (300.0f, 3.0f),
      (null, 4.0f),
      (500.0f, null),
      (null, 6.0f),
      (-500.0f, 50.5f)
    ).toDF("floats", "more_floats")
  }

  def nullableStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(String, String)](
      ("100.0", "1.0"),
      (null, "2.0"),
      ("300.0", "3.0"),
      ("400.0", null),
      ("500.0", "5.0"),
      ("-100.0", null),
      ("-500.0", "0.0")
    ).toDF("strings", "more_strings")
  }

  // Note: some tests here currently use this to force Spark not to
  // push down expressions into the scan (e.g. GpuFilters need this)
  def fromCsvDf(csvPath: String, schema: StructType)
               (session: SparkSession): DataFrame = {
    var df = session.read.format("csv")
      .option("header", "true")
    df.schema(schema).load(csvPath).toDF()
  }

  def nullableFloatCsvDf = {
    var path = this.getClass.getClassLoader.getResource("nullable_floats.csv")
    fromCsvDf(path.toString, StructType(Array(
      StructField("floats", FloatType, true),
      StructField("more_floats", FloatType, true)
    )))(_)
  }

  def floatCsvDf = {
    var path = this.getClass.getClassLoader.getResource("floats.csv")
    fromCsvDf(path.toString, StructType(Array(
      StructField("floats", FloatType, false),
      StructField("more_floats", FloatType, false)
    )))(_)
  }

  testSparkResultsAreEqual("Test CSV", intsFromCsv) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  val smallSplitsConf = {
    val conf = new SparkConf()
    conf.set("spark.sql.files.maxPartitionBytes", "10")
    conf
  }

  testSparkResultsAreEqual("Test CSV splits", intsFromCsv, smallSplitsConf) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  /**
   * Running with an inferred schema results in running things that are not columnar optimized.
   */
  ALLOW_NON_GPU_testSparkResultsAreEqual("Test CSV inferred schema", intsFromCsvInferredSchema) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test scalar addition", longsDf) {
    frame => frame.select(col("longs") + 100)
  }

  testSparkResultsAreEqual("Test addition", longsDf) {
    frame => frame.select(col("longs") + col("more_longs"))
  }

  testSparkResultsAreEqual("Test unary minus", longsDf) {
    frame => frame.select( -col("longs"))
  }

  testSparkResultsAreEqual("Test unary plus", longsDf) {
    frame => frame.selectExpr( "+longs")
  }

  testSparkResultsAreEqual("Test abs", longsDf) {
    frame => frame.select( abs(col("longs")))
  }

  testSparkResultsAreEqual("Test scalar subtraction", longsDf) {
    frame => frame.select(col("longs") - 100)
  }

  testSparkResultsAreEqual("Test scalar subtraction 2", longsDf) {
    frame => frame.selectExpr("50 - longs")
  }

  testSparkResultsAreEqual("Test subtraction", longsDf) {
    frame => frame.select(col("longs") - col("more_longs"))
  }

  testSparkResultsAreEqual("Test scalar multiply", longsDf) {
    frame => frame.select(col("longs") * 100)
  }

  testSparkResultsAreEqual("Test multiply", longsDf) {
    frame => frame.select(col("longs") * col("more_longs"))
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar divide", doubleDf) {
    frame => frame.select(col("doubles") / 100.0)
  }

  // Divide by 0 results in null for spark, but -Infinity for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test divide", nonZeroDoubleDf) {
    frame => frame.select(col("doubles") / col("more_doubles"))
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar int divide", longsDf) {
    frame => frame.selectExpr("longs DIV 100")
  }

  // Divide by 0 results in null for spark, but -1 for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test int divide", nonZeroLongsDf) {
    frame => frame.selectExpr("longs DIV more_longs")
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar remainder", longsDf) {
    frame => frame.selectExpr("longs % 100")
  }

  // Divide by 0 results in null for spark, but -1 for cudf...
  INCOMPAT_testSparkResultsAreEqual("Test remainder", nonZeroLongsDf) {
    frame => frame.selectExpr("longs % more_longs")
  }

  testSparkResultsAreEqual("Test cast from long", longsDf) {
    frame => frame.select(
      col("longs").cast(IntegerType),
      col("longs").cast(LongType),
      // col("longs").cast(StringType),
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
      // col("longs").cast(StringType),
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
      // col("longs").cast(StringType),
      col("more_bools").cast(BooleanType),
      col("more_bools").cast(ByteType),
      col("bools").cast(ShortType),
      col("bools").cast(FloatType),
      col("bools").cast(DoubleType))
  }

  // String are not currently supported
//  testSparkResultsAreEqual("Test cast from strings", doubleStringsDf) {
//    frame => frame.select(
//      col("doubles").cast(DoubleType))
//  }

  testSparkResultsAreEqual("Test acos doubles", doubleDf) {
    frame => frame.select(acos(col("doubles")), acos(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test acos floats", floatDf) {
    frame => frame.select(acos(col("floats")), acos(col("more_floats")))
  }

  testSparkResultsAreEqual("Test asin doubles", doubleDf) {
    frame => frame.select(asin(col("doubles")), asin(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test asin floats", floatDf) {
    frame => frame.select(asin(col("floats")), asin(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test atan doubles", doubleDf, 0.00001) {
    frame => frame.select(atan(col("doubles")), atan(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test atan floats", floatDf, 0.00001) {
    frame => frame.select(atan(col("floats")), atan(col("more_floats")))
  }

  testSparkResultsAreEqual("Test ceil doubles", doubleDf) {
    frame => frame.select(ceil(col("doubles")), ceil(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test ceil floats", floatDf) {
    frame => frame.select(col("floats"), ceil(col("floats")),
      col("more_floats"), ceil(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cos doubles", doubleDf, 0.00001) {
    frame => frame.select(cos(col("doubles")), cos(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cos floats", floatDf, 0.00001) {
    frame => frame.select(cos(col("floats")), cos(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp doubles", doubleDf) {
    frame => frame.select(exp(col("doubles")), exp(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp floats", floatDf) {
    frame => frame.select(exp(col("floats")), exp(col("more_floats")))
  }

  testSparkResultsAreEqual("Test floor doubles", doubleDf) {
    frame => frame.select(floor(col("doubles")), floor(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test floor floats", floatDf) {
    frame => frame.select(floor(col("floats")), floor(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log doubles", nonZeroDoubleDf) {
        // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(abs(col("doubles"))), log(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log floats", nonZeroFloatDf) {
    // Use ABS to work around incompatibility when input is negative and we also need to skip 0
    frame => frame.select(log(abs(col("floats"))), log(abs(col("more_floats"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sin doubles", doubleDf, 0.00001) {
    frame => frame.select(sin(col("doubles")), sin(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test sin floats", floatDf, 0.00001) {
    frame => frame.select(sin(col("floats")), sin(col("more_floats")))
  }

  testSparkResultsAreEqual("Test sqrt doubles", doubleDf) {
    frame => frame.select(sqrt(col("doubles")), sqrt(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test sqrt floats", floatDf) {
    frame => frame.select(sqrt(col("floats")), sqrt(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tan doubles", doubleDf, 0.00001) {
    frame => frame.select(tan(col("doubles")), tan(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test tan floats", floatDf, 0.00001) {
    frame => frame.select(tan(col("floats")), tan(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test scalar pow", longsDf, 0.00001) {
    frame => frame.select(pow(col("longs"), 3))
  }

  INCOMPAT_testSparkResultsAreEqual("Test pow", longsDf, 0.00001) {
    frame => frame.select(pow(col("longs"), col("more_longs")))
  }

  testSparkResultsAreEqual("Test year", datesDf) {
    frame => frame.select(year(col("dates")),
      year(col("more_dates")))
  }

  testSparkResultsAreEqual("Test month", datesDf) {
    frame => frame.select(month(col("dates")),
      month(col("more_dates")))
  }

  testSparkResultsAreEqual("Test day of month", datesDf) {
    frame => frame.select(dayofmonth(col("dates")),
      dayofmonth(col("more_dates")))
  }

  //////////////////
  // LOGICAL TESTS
  /////////////////
  testSparkResultsAreEqual("Test logical not", booleanDf) {
    frame => frame.select(!col("bools"))
  }

  testSparkResultsAreEqual("Test logical and", booleanDf) {
    frame => frame.select(col("bools") && col("more_bools"))
  }

  testSparkResultsAreEqual("Test logical or", booleanDf) {
    frame => frame.select(col("bools") || col("more_bools"))
  }

  //
  // (in)equality with longs
  //
  testSparkResultsAreEqual("Equal to longs", longsDf) {
    frame => frame.selectExpr("longs = more_longs")
  }

  testSparkResultsAreEqual("Not equal to longs", longsDf) {
    frame => frame.selectExpr("longs != more_longs")
  }

  testSparkResultsAreEqual("Less than longs", longsDf) {
    frame => frame.selectExpr("longs < more_longs")
  }

  testSparkResultsAreEqual("Less than or equal longs", longsDf) {
    frame => frame.selectExpr("longs <= more_longs")
  }

  testSparkResultsAreEqual("Greater than longs", longsDf) {
    frame => frame.selectExpr("longs > more_longs")
  }

  testSparkResultsAreEqual("Greater than or equal longs", longsDf) {
    frame => frame.selectExpr("longs >= more_longs")
  }
  
  //
  // (in)equality with doubles
  //
  testSparkResultsAreEqual("Equal to doubles", doubleDf) {
    frame => frame.selectExpr("doubles = more_doubles")
  }

  testSparkResultsAreEqual("Not equal to doubles", doubleDf) {
    frame => frame.selectExpr("doubles != more_doubles")
  }

  testSparkResultsAreEqual("Less than doubles", doubleDf) {
    frame => frame.selectExpr("doubles < more_doubles")
  }

  testSparkResultsAreEqual("Less than or equal doubles", doubleDf) {
    frame => frame.selectExpr("doubles <= more_doubles")
  }

  testSparkResultsAreEqual("Greater than doubles", doubleDf) {
    frame => frame.selectExpr("doubles > more_doubles")
  }

  testSparkResultsAreEqual("Greater than or equal doubles", doubleDf) {
    frame => frame.selectExpr("doubles >= more_doubles")
  }

  //
  // (in)equality with booleans
  // 
  testSparkResultsAreEqual("Equal to booleans", booleanDf) {
    frame => frame.selectExpr("bools = more_bools")
  }

  testSparkResultsAreEqual("Not equal to booleans", booleanDf) {
    frame => frame.selectExpr("bools != more_bools")
  }

  testSparkResultsAreEqual("Less than booleans", booleanDf) {
    frame => frame.selectExpr("bools < more_bools")
  }

  testSparkResultsAreEqual("Less than or equal booleans", booleanDf) {
    frame => frame.selectExpr("bools <= more_bools")
  }

  testSparkResultsAreEqual("Greater than boleans", booleanDf) {
    frame => frame.selectExpr("bools > more_bools")
  }

  testSparkResultsAreEqual("Greater than or equal", booleanDf) {
    frame => frame.selectExpr("bools >= more_bools")
  }
  ///////

  testSparkResultsAreEqual("project is not null", nullableFloatDf) {
    frame => frame.selectExpr("floats is not null")
  }

  testSparkResultsAreEqual("project is null", nullableFloatDf) {
    frame => frame.selectExpr("floats is null")
  }

  testSparkResultsAreEqual("project is null col1 OR is null col2", nullableFloatDf) {
    frame => frame.selectExpr("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("filter is not null", nullableFloatCsvDf) {
    frame => frame.filter("floats is not null")
  }

  testSparkResultsAreEqual("filter is null", nullableFloatCsvDf) {
    frame => frame.filter("floats is null")
  }

  testSparkResultsAreEqual("filter is null col1 OR is null col2", nullableFloatCsvDf) {
    frame => frame.filter("floats is null OR more_floats is null")
  }

  testSparkResultsAreEqual("filter less than", floatCsvDf) {
    frame => frame.filter("floats < more_floats")
  }

  testSparkResultsAreEqual("filter greater than", floatCsvDf) {
    frame => frame.filter("floats > more_floats")
  }

  testSparkResultsAreEqual("filter less than or equal", floatCsvDf) {
    frame => frame.filter("floats <= more_floats")
  }

  testSparkResultsAreEqual("filter greater than or equal", floatCsvDf) {
    frame => frame.filter("floats >= more_floats")
  }

  testSparkResultsAreEqual("filter is null and greater than or equal", nullableFloatCsvDf) {
    frame => frame.filter("floats is null AND more_floats >= 3.0")
  }

  testSparkResultsAreEqual("filter is not null and greater than or equal", nullableFloatCsvDf) {
    frame => frame.filter("floats is not null AND more_floats >= 3.0")
  }

  /*
  Strings are not currently supported
    testSparkResultsAreEqual("IsNotNull strings", nullableStringsDf) {
      frame => frame.selectExpr("strings is not null")
    }

    testSparkResultsAreEqual("IsNull strings", nullableStringsDf) {
      frame => frame.selectExpr("strings is null")
    }

    testSparkResultsAreEqual("IsNull OR IsNull strings", nullableStringsDf) {
      frame => frame.selectExpr("strings is null OR more_strings is null")
    }
    */
}
