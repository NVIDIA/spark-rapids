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
      fun(data).collect()
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
    val qualifiedTestName = qualifiers.mkString("",
      ", ",
      (if (qualifiers.nonEmpty) ": " else "") + testName)
    test(qualifiedTestName) {
      var (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
        conf = testConf,
        repart = repart)
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

  testSparkResultsAreEqual("Test CSV", intsFromCsv) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test CSV count", intsFromCsv)(frameCount)

  testSparkResultsAreEqual("Test partitioned CSV", intsFromPartitionedCsv) {
    frame => frame.select(col("partKey"), col("ints_1"), col("ints_3"), col("ints_5"))
  }

  private val smallSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "10")

  testSparkResultsAreEqual("Test CSV splits", intsFromCsv, conf=smallSplitsConf) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test CSV splits with header", floatCsvDf, conf=smallSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test partitioned CSV splits", intsFromPartitionedCsv, conf=smallSplitsConf) {
    frame => frame.select(col("partKey"), col("ints_1"), col("ints_3"), col("ints_5"))
  }

  testSparkResultsAreEqual("Test CSV splits with chunks", floatCsvDf, conf= new SparkConf().set(
    "spark.rapids.sql.maxReaderBatchSize", "1")) {
    frame => frame.select(col("floats"))
  }

  testSparkResultsAreEqual("Test CSV count chunked", intsFromCsv, conf= new SparkConf().set(
    "spark.rapids.sql.maxReaderBatchSize", "1"))(frameCount)

  /**
   * Running with an inferred schema results in running things that are not columnar optimized.
   */
  ALLOW_NON_GPU_testSparkResultsAreEqual("Test CSV inferred schema", intsFromCsvInferredSchema) {
    frame => frame.select(col("*"))
  }

  def frameFromParquet(filename: String): SparkSession => DataFrame = {
    val path = this.getClass.getClassLoader.getResource(filename)
    s: SparkSession => s.read.parquet(path.toString)
  }

  testSparkResultsAreEqual("Test Parquet", frameFromParquet("test.snappy.parquet")) {
    frame => frame.select(col("ints_1"), col("ints_3"), col("ints_5"))
  }

  private val fileSplitsParquet = frameFromParquet("file-splits.parquet")

  private val parquetSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "10000")

  testSparkResultsAreEqual("Test Parquet file splitting", fileSplitsParquet,
      conf=parquetSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test Parquet count", fileSplitsParquet,
      conf=parquetSplitsConf)(frameCount)

  testSparkResultsAreEqual("Test Parquet predicate push-down", fileSplitsParquet) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
        .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test Parquet splits predicate push-down", fileSplitsParquet,
    conf=parquetSplitsConf) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
        .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test partitioned Parquet", frameFromParquet("partitioned-parquet")) {
    frame => frame.select(col("partKey"), col("ints_1"), col("ints_3"), col("ints_5"))
  }

  def frameFromOrc(filename: String): SparkSession => DataFrame = {
    val path = this.getClass.getClassLoader.getResource(filename)
    s: SparkSession => s.read.orc(path.toString)
  }

  private val fileSplitsOrc = frameFromOrc("file-splits.orc")

  private val orcSplitsConf = new SparkConf().set("spark.sql.files.maxPartitionBytes", "30000")

  testSparkResultsAreEqual("Test ORC", frameFromOrc("test.snappy.orc")) {
    // dropping the timestamp column since timestamp expressions are not GPU supported yet
    frame => frame.select(col("*")).drop("timestamp")
  }

  testSparkResultsAreEqual("Test ORC file splitting", fileSplitsOrc, conf=orcSplitsConf) {
    frame => frame.select(col("*"))
  }

  testSparkResultsAreEqual("Test ORC count", fileSplitsOrc,
      conf=orcSplitsConf)(frameCount)

  testSparkResultsAreEqual("Test ORC predicate push-down", fileSplitsOrc) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
        .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test ORC splits predicate push-down", fileSplitsOrc,
      conf=orcSplitsConf) {
    frame => frame.select(col("loan_id"), col("orig_interest_rate"), col("zip"))
        .where(col("orig_interest_rate") > 10)
  }

  testSparkResultsAreEqual("Test partitioned ORC", frameFromOrc("partitioned-orc")) {
    frame => frame.select(col("partKey"), col("ints_5"), col("ints_3"), col("ints_1"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test hash agg with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("longs")).agg(sum(col("more_longs")))
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

  testSparkResultsAreEqual("Test literal values in select", floatDf) {
    frame => frame.select(col("floats"), lit(100))
  }

  // TODO need a way to fill a column from a string
//  testSparkResultsAreEqual("Test literal string values in select", floatDf) {
//    frame => frame.select(col("floats"), lit("test"))
//  }

  INCOMPAT_testSparkResultsAreEqual("Test cos doubles", doubleDf, 0.00001) {
    frame => frame.select(cos(col("doubles")), cos(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test cos floats", floatDf, 0.00001) {
    frame => frame.select(cos(col("floats")), cos(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp doubles", smallDoubleDf, 0.00001) {
    frame => frame.select(exp(col("doubles")), exp(col("more_doubles")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test exp floats", smallFloatDf, 0.00001) {
    frame => frame.select(exp(col("floats")), exp(col("more_floats")))
  }

  testSparkResultsAreEqual("Test floor doubles", doubleDf) {
    frame => frame.select(floor(col("doubles")), floor(col("more_doubles")))
  }

  testSparkResultsAreEqual("Test floor floats", floatDf) {
    frame => frame.select(floor(col("floats")), floor(col("more_floats")))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log doubles", nonZeroDoubleDf, 0.00001) {
        // Use ABS to work around incompatibility when input is negative, and we also need to skip 0
    frame => frame.select(log(abs(col("doubles"))), log(abs(col("more_doubles"))))
  }

  INCOMPAT_testSparkResultsAreEqual("Test log floats", nonZeroFloatDf, 0.00001) {
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

  // these are ALLOW_NON_GPU because you can't project a string
  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings are not null", nullableStringsFromCsv) {
    frame => frame.filter("strings is not null or more_strings is not null")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings equality", nullableStringsFromCsv) {
    frame => frame.filter("strings = 'Bar'")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings greater than", nullableStringsFromCsv) {
    frame => frame.filter("strings > 'Bar'")
  }

  ALLOW_NON_GPU_testSparkResultsAreEqual("filter strings less than", nullableStringsFromCsv) {
    frame => frame.filter("strings < 'Bar'")
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

  testSparkResultsAreEqual("Test union doubles", doubleDf) {
    frame => frame.union(frame)
  }

  testSparkResultsAreEqual("Test unionAll doubles", doubleDf) {
    frame => frame.unionAll(frame)
  }

  testSparkResultsAreEqual("Test unionByName doubles", doubleDf) {
    frame => frame.unionByName(frame.select(col("more_doubles"), col("doubles")))
  }

  /*
   * HASH AGGREGATE TESTS
   */
  IGNORE_ORDER_testSparkResultsAreEqual("short reduction aggs", shortsFromCsv) {
    frame => frame.agg(
      (max("shorts") - min("more_shorts")) * lit(5),
      sum("shorts"),
      count("*"),
      avg("shorts"),
      avg(col("more_shorts") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("reduction aggs", longsCsvDf) {
    frame => frame.agg(
      (max("longs") - min("more_longs")) * lit(5),
      sum("longs"),
      count("*"),
      avg("longs"),
      avg(col("more_longs") * lit("10")))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("test count, sum, max, min with shuffle", longsFromCSVDf, repart = 2) {
    frame => frame.groupBy(col("more_longs")).agg(
      count("*"),
      sum("more_longs"),
      sum("longs") * lit(2),
      (max("more_longs") - min("more_longs")) * 3.0)
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("float basic aggregates group by floats", floatCsvDf) {
    frame => frame.groupBy("floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("float basic aggregates group by more_floats", floatCsvDf) {
    frame => frame.groupBy("more_floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("nullable float basic aggregates group by more_floats", nullableFloatCsvDf) {
    frame => frame.groupBy("more_floats").agg(
      lit(456f),
      min(col("floats")) + lit(123),
      sum(col("more_floats") + lit(123.0)),
      max(col("floats") * col("more_floats")),
      max("floats") - min("more_floats"),
      max("more_floats") - min("floats"),
      sum("floats") + sum("more_floats"),
      avg("floats"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("shorts basic aggregates group by more_shorts", shortsFromCsv) {
    frame => frame.groupBy("more_shorts").agg(
      lit(456),
      min(col("shorts")) + lit(123),
      sum(col("more_shorts") + lit(123.0)),
      max(col("shorts") * col("more_shorts")),
      max("shorts") - min("more_shorts"),
      max("more_shorts") - min("shorts"),
      sum("shorts"),
      avg("shorts"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by longs", longsCsvDf) {
    frame => frame.groupBy("longs").agg(
      lit(456f),
      min(col("longs")) + lit(123),
      sum(col("more_longs") + lit(123.0)),
      max(col("longs") * col("more_longs")),
      max("longs") - min("more_longs"),
      max("more_longs") - min("longs"),
      sum("longs") + sum("more_longs"),
      avg("longs"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("long basic aggregates group by more_longs", longsCsvDf) {
    frame => frame.groupBy("more_longs").agg(
      lit(456f),
      min(col("longs")) + lit(123),
      sum(col("more_longs") + lit(123.0)),
      max(col("longs") * col("more_longs")),
      max("longs") - min("more_longs"),
      max("more_longs") - min("longs"),
      sum("longs") + sum("more_longs"),
      avg("longs"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by ints", intCsvDf) {
    frame => frame.groupBy("ints").agg(
      lit(456f),
      min(col("ints")) + lit(123),
      sum(col("more_ints") + lit(123.0)),
      max(col("ints") * col("more_ints")),
      max("ints") - min("more_ints"),
      max("more_ints") - min("ints"),
      sum("ints") + sum("more_ints"),
      avg("ints"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("ints basic aggregates group by more_ints", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      lit(456f),
      min(col("ints")) + lit(123),
      sum(col("more_ints") + lit(123.0)),
      max(col("ints") * col("more_ints")),
      max("ints") - min("more_ints"),
      max("more_ints") - min("ints"),
      sum("ints") + sum("more_ints"),
      avg("ints"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("doubles basic aggregates group by doubles", doubleCsvDf) {
    frame => frame.groupBy("doubles").agg(
      lit(456f),
      min(col("doubles")) + lit(123),
      sum(col("more_doubles") + lit(123.0)),
      max(col("doubles") * col("more_doubles")),
      max("doubles") - min("more_doubles"),
      max("more_doubles") - min("doubles"),
      sum("doubles") + sum("more_doubles"),
      avg("doubles"),
      count("*"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("doubles basic aggregates group by more_doubles", doubleCsvDf) {
    frame => frame.groupBy("more_doubles").agg(
      lit(456f),
      min(col("doubles")) + lit(123),
      sum(col("more_doubles") + lit(123.0)),
      max(col("doubles") * col("more_doubles")),
      max("doubles") - min("more_doubles"),
      max("more_doubles") - min("doubles"),
      sum("doubles") + sum("more_doubles"),
      avg("doubles"),
      count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("sum(longs) multi group by longs, more_longs", longsCsvDf) {
    frame => frame.groupBy("longs", "more_longs").agg(
      sum("longs"), count("*"))
  }

  // misc aggregation tests
  testSparkResultsAreEqual("sum(ints) group by literal", intCsvDf) {
    frame => frame.groupBy(lit(1)).agg(sum("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("sum(ints) group by dates", datesCsvDf) {
    frame => frame.groupBy("dates").sum("ints")
  }

  IGNORE_ORDER_testSparkResultsAreEqual("max(ints) group by month", datesCsvDf) {
    frame => frame.withColumn("monthval", month(col("dates")))
                  .groupBy(col("monthval"))
                  .agg(max("ints").as("max_ints_by_month"))
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("sum(floats) group by more_floats 2 partitions", floatCsvDf, repart = 2) {
    frame => frame.groupBy("more_floats").sum("floats")
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("avg(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame.groupBy("more_floats").avg("floats")
  }

  INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual("avg(floats),count(floats) group by more_floats 4 partitions", floatCsvDf, repart = 4) {
    frame => frame
      .groupBy("more_floats")
      .agg(avg("floats"), count("*"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions", intCsvDf) {
    frame => frame.groupBy(col("more_ints") * 2).agg(
      lit(1000) +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expressions 2", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      min("ints") +
        (lit(100) * (avg("ints") * sum("ints") - min("ints"))))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("complex aggregate expression 3", intCsvDf) {
    frame => frame.groupBy("more_ints").agg(
      min("ints"), avg("ints"),
      max(col("ints") + col("more_ints")), lit(1), min("ints"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions", longsCsvDf) {
    frame => frame.groupBy(col("more_longs") + lit(10)).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("grouping expressions 2", longsCsvDf) {
    frame => frame.groupBy(col("more_longs") + col("longs")).agg(min("longs"))
  }

  IGNORE_ORDER_testSparkResultsAreEqual("first/last aggregates", intCsvDf) {
    frame => frame.groupBy(col("more_ints")).agg(
      first("five", true), last("six", true))
  }
}
