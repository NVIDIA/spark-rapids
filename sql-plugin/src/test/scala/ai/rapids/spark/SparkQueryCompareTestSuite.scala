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

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

/**
 * Set of tests that compare the output using the CPU version of spark vs our GPU version.
 */
trait SparkQueryCompareTestSuite extends FunSuite with BeforeAndAfterEach {
  // Timezone is fixed to UTC to allow timestamps to work by default
  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

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
    var c = conf.clone()
      .set("spark.sql.extensions", "ai.rapids.spark.Plugin")
      .set("spark.executor.plugins", "ai.rapids.spark.GpuResourceManager")

    if (c.getOption(RapidsConf.TEST_CONF.key).isEmpty) {
       c = c.set(RapidsConf.TEST_CONF.key, "true")
    }
    withSparkSession("gpu-sql-test", c, f)
  }

  def withCpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    withSparkSession("cpu-sql-test", conf, f)
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
  def runOnCpuAndGpu(
      df: SparkSession => DataFrame,
      fun: DataFrame => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1): (Array[Row], Array[Row]) = {
    val fromCpu = withCpuSparkSession((session) => {
      var data = df(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection, not folded into the table scan exec
        data = data.repartition(repart)
      }
      fun(data).collect()
    }, conf)

    val fromGpu = withGpuSparkSession((session) => {
      var data = df(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection, not folded into the table scan exec
        data = data.repartition(repart)
      }
      fun(data).explain()
      fun(data).collect()
    }, conf)

    (fromCpu, fromGpu)
  }

  /**
   * Runs a test defined by fun, using 2 dataframes dfA and dfB.
   *
   * @param dfA  - the first DataFrame to use as input
   * @param dfB  - the second DataFrame to use as input
   * @param fun  - the function to transform the DataFrame (produces another DataFrame)
   * @param conf - spark conf
   * @return       - tuple of (cpu results, gpu results) as arrays of Row
   */
  def runOnCpuAndGpu2(dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      fun: (DataFrame, DataFrame) => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1): (Array[Row], Array[Row]) = {
    val fromCpu = withCpuSparkSession((session) => {
      var dataA = dfA(session)
      var dataB = dfB(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection, not folded into the table scan exec
        dataA = dataA.repartition(repart)
        dataB = dataB.repartition(repart)
      }
      fun(dataA, dataB).collect()
    })

    val fromGpu = withGpuSparkSession((session) => {
      var dataA = dfA(session)
      var dataB = dfB(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection, not folded into the table scan exec
        dataA = dataA.repartition(repart)
        dataB = dataB.repartition(repart)
      }
      fun(dataA, dataB).collect()
    }, conf)

    (fromCpu, fromGpu)
  }

  def INCOMPAT_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      maxFloatDiff: Double = 0.0,
      conf: SparkConf = new SparkConf(),
      sort: Boolean = false,
      repart: Integer = 1)
    (fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      sort=sort,
      maxFloatDiff=maxFloatDiff,
      incompat=true)(fun)
  }

  def ALLOW_NON_GPU_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf())(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      allowNonGpu=true)(fun)
  }

  def IGNORE_ORDER_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf())(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      sort=true)(fun)
  }

  def INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf())(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      incompat=true,
      sort=true)(fun)
  }

  // we guarantee that the types will be the same
  private def seqLt(a: Seq[Any], b: Seq[Any]): Boolean = {
    if (a.length < b.length) {
      return true
    }
    // lengths are the same
    for (i <- a.indices) {
      val v1 = a(i)
      val v2 = b(i)
      if (v1 != v2) {
        // null is always < anything but null
        if (v1 == null) {
          return true
        }

        if (v2 == null) {
          return false
        }

        return if ((v1, v2) match {
          case (i1: Int, i2:Int) => i1 < i2
          case (i1: Long, i2:Long) => i1 < i2
          case (i1: Float, i2:Float) => i1 < i2
          case (i1: Date, i2:Date) => i1.before(i2)
          case (i1: Double, i2:Double) => i1 < i2
          case (i1: Short, i2:Short) => i1 < i2
          case (o1, o2) => throw new UnsupportedOperationException(o1.getClass + " is not supported yet")
        }) {
          true
        } else {
          false
        }
      }
    }
    return false
  }

  def setupTestConfAndQualifierName(
      testName: String,
      incompat: Boolean,
      sort: Boolean,
      allowNonGpu: Boolean,
      conf: SparkConf): (SparkConf, String) = {

    var qualifiers = Set[String]()
    var testConf = conf
    if (incompat) {
      testConf = testConf.clone().set(RapidsConf.INCOMPATIBLE_OPS.key, "true")
      qualifiers = qualifiers + "INCOMPAT"
    }
    if (sort) {
      qualifiers = qualifiers + "IGNORE ORDER"
    }
    if (allowNonGpu) {
      testConf = testConf.clone().set(RapidsConf.TEST_CONF.key, "false")
      qualifiers = qualifiers + "NOT ALL ON GPU"
    }
    val qualifiedTestName = qualifiers.mkString("", ", ",
      (if (qualifiers.nonEmpty) ": " else "") + testName)
    (testConf, qualifiedTestName)
  }

  def compareResults(
      sort: Boolean,
      maxFloatDiff:Double,
      fromCpu: Array[Row],
      fromGpu: Array[Row]): Unit = {
    val relaxedFloatDisclaimer = if (maxFloatDiff > 0) {
      "(relaxed float comparison)"
    } else {
      ""
    }
    if (sort) {
      val cpu = fromCpu.map(_.toSeq).sortWith(seqLt)
      val gpu = fromGpu.map(_.toSeq).sortWith(seqLt)
      if (!compare(cpu, gpu, maxFloatDiff)) {
        fail(
          s"""
             |Running on the GPU and on the CPU did not match $relaxedFloatDisclaimer
             |CPU: ${cpu.seq}

             |GPU: ${gpu.seq}
         """.
            stripMargin)
      }
    } else {
      if (!compare(fromCpu, fromGpu, maxFloatDiff)) {
        fail(
          s"""
             |Running on the GPU and on the CPU did not match $relaxedFloatDisclaimer
             |CPU: ${fromCpu.toSeq}

             |GPU: ${fromGpu.toSeq}
         """.
            stripMargin)
      }
    }
  }

  def testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      allowNonGpu: Boolean = false)
      (fun: DataFrame => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf)

    test(qualifiedTestName) {
      var (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
        conf = testConf,
        repart = repart)

      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
    }
  }

  def testSparkResultsAreEqual2(
      testName: String,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      allowNonGpu: Boolean = false)
    (fun: (DataFrame, DataFrame) => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf)

    test(qualifiedTestName) {
      var (fromCpu, fromGpu) = runOnCpuAndGpu2(dfA, dfB, fun, conf = testConf, repart = repart)

      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
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
      (-500L, 0L),
      (0x123400L, 7L)
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

  def smallDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (1.0, 1.0),
      (2.0, 2.0),
      (3.0, 3.0),
      (4.0, 4.0),
      (5.0, 5.0),
      (-1.0, 6.0),
      (-5.0, 0.0)
    ).toDF("doubles", "more_doubles")
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

  def smallFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (1.0f, 1.0f),
      (2.0f, 2.0f),
      (3.0f, 3.0f),
      (4.0f, 4.0f),
      (5.0f, 5.0f),
      (-1.0f, 6.0f),
      (-5.0f, 0.0f)
    ).toDF("floats", "more_floats")
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

  def nullableStringsFromCsv = {
    fromCsvDf("strings.csv", StructType(Array(
      StructField("strings", StringType, true),
      StructField("more_strings", StringType, true)
    )))(_)
  }

  // Note: some tests here currently use this to force Spark not to
  // push down expressions into the scan (e.g. GpuFilters need this)
  def fromCsvDf(file: String, schema: StructType)
               (session: SparkSession): DataFrame = {
    val resource = this.getClass.getClassLoader.getResource(file).toString
    val df = session.read.format("csv")
    df.schema(schema).load(resource).toDF()
  }

  def shortsFromCsv = {
    fromCsvDf("shorts.csv", StructType(Array(
      StructField("shorts", ShortType),
      StructField("more_shorts", ShortType),
      StructField("five", ShortType),
      StructField("six", ShortType),
    )))(_)
  }

  def intsFromCsv = {
    fromCsvDf("test.csv", StructType(Array(
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)
    )))(_)
  }

  def intsFromPartitionedCsv= {
    fromCsvDf("partitioned-csv", StructType(Array(
      StructField("partKey", IntegerType),
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)
    )))(_)
  }

  def longsFromCSVDf = {
    fromCsvDf("lots_o_longs.csv", StructType(Array(
      StructField("longs", LongType, true),
      StructField("more_longs", LongType, true)
    )))(_)
  }

  def intsFromCsvInferredSchema(session: SparkSession): DataFrame = {
    val path = this.getClass.getClassLoader.getResource("test.csv")
    session.read.option("inferSchema", "true").csv(path.toString)
  }

  def nullableFloatCsvDf = {
    fromCsvDf("nullable_floats.csv", StructType(Array(
      StructField("floats", FloatType, true),
      StructField("more_floats", FloatType, true)
    )))(_)
  }

  def floatCsvDf = {
    fromCsvDf("floats.csv", StructType(Array(
      StructField("floats", FloatType, false),
      StructField("more_floats", FloatType, false)
    )))(_)
  }

  def frameCount(frame: DataFrame): DataFrame = {
    import frame.sparkSession.implicits._
    Seq(frame.count()).toDF
  }

  def intCsvDf= {
    fromCsvDf("ints.csv", StructType(Array(
      StructField("ints", IntegerType, false),
      StructField("more_ints", IntegerType, false),
      StructField("five", IntegerType, false),
      StructField("six", IntegerType, false)
    )))(_)
  }

  def longsCsvDf= {
    fromCsvDf("ints.csv", StructType(Array(
      StructField("longs", LongType, false),
      StructField("more_longs", LongType, false),
      StructField("five", IntegerType, false),
      StructField("six", IntegerType, false)
    )))(_)
  }

  def doubleCsvDf= {
    fromCsvDf("floats.csv", StructType(Array(
      StructField("doubles", DoubleType, false),
      StructField("more_doubles", DoubleType, false)
    )))(_)
  }

  def datesCsvDf= {
    fromCsvDf("dates.csv", StructType(Array(
      StructField("dates", DateType, false),
      StructField("ints", IntegerType, false)
    )))(_)
  }

  def frameFromParquet(filename: String): SparkSession => DataFrame = {
    val path = this.getClass.getClassLoader.getResource(filename)
    s: SparkSession => s.read.parquet(path.toString)
  }

  def frameFromOrc(filename: String): SparkSession => DataFrame = {
    val path = this.getClass.getClassLoader.getResource(filename)
    s: SparkSession => s.read.orc(path.toString)
  }
}
