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

import java.io.File
import java.sql.Date
import java.util.{Locale, TimeZone}

import org.scalatest.FunSuite

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

import scala.util.{Failure, Try}

/**
 * Set of tests that compare the output using the CPU version of spark vs our GPU version.
 */
trait SparkQueryCompareTestSuite extends FunSuite {
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
      .master("local[1]")
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
      .set("spark.plugins", "ai.rapids.spark.SQLPlugin")
      .set(RapidsConf.EXPLAIN.key, "ALL")

    if (c.getOption(RapidsConf.TEST_CONF.key).isEmpty) {
       c = c.set(RapidsConf.TEST_CONF.key, "true")
    }
    withSparkSession("gpu-sql-test", c, f)
  }

  def withCpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    withSparkSession("cpu-sql-test", conf, f)
  }

  private def compare(expected: Any, actual: Any, epsilon: Double = 0.0): Boolean = {
    def doublesAreEqualWithinPercentage(expected: Double, actual: Double): (String, Boolean) = {
      if (!compare(expected, actual)) {
        if (expected != 0) {
          val v = Math.abs((expected - actual) / expected)
          (s"\n\nABS($expected - $actual) / ABS($actual) == $v is not <= $epsilon ",  v <= epsilon)
        } else {
          val v = Math.abs(expected - actual)
          (s"\n\nABS($expected - $actual) == $v is not <= $epsilon ",  v <= epsilon)
        }
      } else {
        ("SUCESS", true)
      }
    }
    (expected, actual) match {
      case (a: Float, b: Float) if a.isNaN && b.isNaN => true
      case (a: Double, b: Double) if a.isNaN && b.isNaN => true
      case (null, null) => true
      case (null, _) => false
      case (_, null) => false
      case (a: Array[_], b: Array[_]) =>
        a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r, epsilon) }
      case (a: Map[_, _], b: Map[_, _]) =>
        a.size == b.size && a.keys.forall { aKey =>
          b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey), epsilon))
        }
      case (a: Iterable[_], b: Iterable[_]) =>
        a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r, epsilon) }
      case (a: Product, b: Product) =>
        compare(a.productIterator.toSeq, b.productIterator.toSeq, epsilon)
      case (a: Row, b: Row) =>
        compare(a.toSeq, b.toSeq, epsilon)
      // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
      case (a: Double, b: Double) if epsilon <= 0 =>
        java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
      case (a: Double, b: Double) if epsilon > 0 =>
        val ret = doublesAreEqualWithinPercentage(a, b)
        if (!ret._2) {
          System.err.println(ret._1 + " (double)")
        }
        ret._2
      case (a: Float, b: Float) if epsilon <= 0 =>
        java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
      case (a: Float, b: Float) if epsilon > 0 =>
        val ret = doublesAreEqualWithinPercentage(a, b)
        if (!ret._2) {
          System.err.println(ret._1 + " (float)")
        }
        ret._2
      case (a, b) => a == b
    }
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
    conf.setIfMissing("spark.sql.shuffle.partitions", "2")
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
    }, conf)

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
      repart: Integer = 1,
      sortBeforeRepart: Boolean = false)
    (fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      sort=sort,
      maxFloatDiff=maxFloatDiff,
      incompat=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def ALLOW_NON_GPU_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      allowNonGpu=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      sort=true,
      allowNonGpu=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def IGNORE_ORDER_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      repart=repart,
      incompat=true,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  /**
   * Writes and reads a dataframe to a file using the CPU and the GPU.
   *
   * @param df     - the DataFrame to use as input
   * @param writer - the function to write the data to a file
   * @param reader - the function to read the data from a file
   * @param conf   - spark conf
   * @return       - tuple of (cpu results, gpu results) as arrays of Row
   */
  def writeWithCpuAndGpu(
      df: SparkSession => DataFrame,
      writer: (DataFrame, String) => Unit,
      reader: (SparkSession, String) => DataFrame,
      conf: SparkConf = new SparkConf()): (Array[Row], Array[Row]) = {
    conf.setIfMissing("spark.sql.shuffle.partitions", "2")
    var tempCpuFile: File = null
    var tempGpuFile: File = null
    try {
      tempCpuFile = File.createTempFile("testdata", "cpu")
      // remove the empty file to prevent "file already exists" from writers
      tempCpuFile.delete()
      withCpuSparkSession(session => {
        val data = df(session)
        writer(data, tempCpuFile.getAbsolutePath)
      }, conf)

      tempGpuFile = File.createTempFile("testdata", "gpu")
      // remove the empty file to prevent "file already exists" from writers
      tempGpuFile.delete()
      withGpuSparkSession(session => {
        val data = df(session)
        writer(data, tempGpuFile.getAbsolutePath)
      }, conf)

      val (fromCpu, fromGpu) = withCpuSparkSession(session => {
        val cpuData = reader(session, tempCpuFile.getAbsolutePath)
        val gpuData = reader(session, tempGpuFile.getAbsolutePath)
        (cpuData.collect, gpuData.collect)
      }, conf)

      (fromCpu, fromGpu)
    } finally {
      if (tempCpuFile != null) {
        tempCpuFile.delete()
      }
      if (tempGpuFile != null) {
        tempGpuFile.delete()
      }
    }
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

        (v1, v2) match {
          case (i1: Int, i2: Int) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          }// else equal go on
          case (i1: Long, i2: Long) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Float, i2: Float) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Date, i2: Date) => if (i1.before(i2)) {
            return true
          } else if (i1.after(i2)) {
            return false
          } // else equal go on
          case (i1: Double, i2: Double) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Short, i2: Short) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (s1: String, s2: String) =>
            val cmp = s1.compareTo(s2)
            if (cmp < 0) {
              return true
            } else if (cmp > 0) {
              return false
            } // else equal go on
          case (o1, o2) => throw new UnsupportedOperationException(o1.getClass + " is not supported yet")
        }
      }
    }
    // They are equal...
    false
  }

  def setupTestConfAndQualifierName(
      testName: String,
      incompat: Boolean,
      sort: Boolean,
      allowNonGpu: Boolean,
      conf: SparkConf,
      execsAllowedNonGpu: Seq[String],
      sortBeforeRepart: Boolean): (SparkConf, String) = {

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
    if (execsAllowedNonGpu.nonEmpty) {
      val execStr = execsAllowedNonGpu.mkString(",")
      testConf = testConf.clone().set(RapidsConf.TEST_ALLOWED_NONGPU.key, execStr)
      qualifiers = qualifiers + s"NOT ON GPU[$execStr]"
    }

    testConf.set("spark.sql.execution.sortBeforeRepartition", sortBeforeRepart.toString)
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
      allowNonGpu: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf, execsAllowedNonGpu,
        sortBeforeRepart = sortBeforeRepart)
    test(qualifiedTestName) {
      val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
        conf = testConf,
        repart = repart)
      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
    }
  }

  def testExpectedExceptionStartsWith[T <: Throwable](
      testName: String,
      exceptionClass: Class[T],
      expectedException: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      allowNonGpu: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
           (fun: DataFrame => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf, execsAllowedNonGpu,
        sortBeforeRepart = sortBeforeRepart)

      test(qualifiedTestName) {
        val t = Try({
          val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
            conf = testConf,
            repart = repart)
          compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
        })
        t match {
          case Failure(e) if e.getClass == exceptionClass => {
            assert(e.getMessage != null && e.getMessage.startsWith(expectedException))
          }
          case Failure(e) => throw e
          case _ => fail("Expected an exception")
        }
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
      allowNonGpu: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
    (fun: (DataFrame, DataFrame) => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, allowNonGpu, conf, execsAllowedNonGpu,
        sortBeforeRepart = sortBeforeRepart)

    testConf.set("spark.sql.execution.sortBeforeRepartition", sortBeforeRepart.toString)
    test(qualifiedTestName) {
      val (fromCpu, fromGpu) = runOnCpuAndGpu2(dfA, dfB, fun, conf = testConf, repart = repart)
      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
    }
  }

  def INCOMPAT_testSparkResultsAreEqual2(
      testName: String,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: (DataFrame, DataFrame) => DataFrame): Unit = {
    testSparkResultsAreEqual2("INCOMPAT: " + testName, dfA, dfB,
      conf=conf.set(RapidsConf.INCOMPATIBLE_OPS.key, "true"),
      repart=repart, sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def INCOMPAT_IGNORE_ORDER_testSparkResultsAreEqual2(
      testName: String,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: (DataFrame, DataFrame) => DataFrame): Unit = {
    testSparkResultsAreEqual2("INCOMPAT, IGNORE ORDER: " + testName, dfA, dfB,
      conf=conf.set(RapidsConf.INCOMPATIBLE_OPS.key, "true"),
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def IGNORE_ORDER_testSparkResultsAreEqual2(
      testName: String,
      dfA: SparkSession => DataFrame,
      dfB: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: (DataFrame, DataFrame) => DataFrame): Unit = {
    testSparkResultsAreEqual2("IGNORE ORDER: " + testName, dfA, dfB,
      conf=conf,
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def testSparkWritesAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      writer: (DataFrame, String) => Unit,
      reader: (SparkSession, String) => DataFrame,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false,
      sort: Boolean = false): Unit = {
    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, false, sort, false, conf, Nil,
        sortBeforeRepart = sortBeforeRepart)

    test(qualifiedTestName) {
      val (fromCpu, fromGpu) = writeWithCpuAndGpu(df, writer, reader, testConf)
      compareResults(sort, 0, fromCpu, fromGpu)
    }
  }

  /** forces ColumnarBatch of batchSize bytes */
  def makeBatchedBytes(batchSize: Int, conf: SparkConf = new SparkConf()): SparkConf = {
    conf.set(RapidsConf.GPU_BATCH_SIZE_BYTES.key, batchSize.toString)
  }

  def mixedDfWithBuckets(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Integer, java.lang.Long, java.lang.Double, String, java.lang.Integer, String)](
      (99, 100L, 1.0, "A", 1, "A"),
      (98, 200L, 2.0, "B", 2, "B"),
      (97,300L, 3.0, "C", 3, "C"),
      (99, 400L, 4.0, "D", null, null),
      (98, 500L, 5.0, "E", 1, "A"),
      (97, -100L, 6.0, "F", 2, "B"),
      (96, -500L, 0.0, "G", 3, "C"),
      (95, -700L, 8.0, "E\u0480\u0481", null, ""),
      (94, -900L, 9.0, "g\nH", 1, "A"),
      (92, -1200L, 12.0, "IJ\"\u0100\u0101\u0500\u0501", 2, "B"),
      (90, 1500L, 15.0, "\ud720\ud721", 3, "C")
    ).toDF("ints", "longs", "doubles", "strings", "bucket_1", "bucket_2")
  }

  def mixedDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Integer, java.lang.Long, java.lang.Double, java.lang.String)](
      (99, 100L, 1.0, "A"),
      (98, 200L, 2.0, "B"),
      (97,300L, 3.0, "C"),
      (99, 400L, 4.0, "D"),
      (98, 500L, 5.0, "E"),
      (97, -100L, 6.0, "F"),
      (96, -500L, 0.0, "G"),
      (95, -700L, 8.0, "E\u0480\u0481"),
      (Int.MaxValue, Long.MinValue, Double.PositiveInfinity, "\u0000"),
      (Int.MinValue, Long.MaxValue, Double.NaN, "\u0000"),
      (null, null, null, "actions are judged by intentions"),
      (94, -900L, 9.0, "g\nH"),
      (92, -1200L, 12.0, "IJ\"\u0100\u0101\u0500\u0501"),
      (90, 1500L, 15.0, "\ud720\ud721")
    ).toDF("ints", "longs", "doubles", "strings")
  }

  def likeDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    val someDF = Seq(
      (150, "abc"), (140, "a*c"), (130, "ab"), (120, "ac"), (110, "a|bc"), (100, "a.b"), (90, "b$"),
      (80, "b$c"), (70, "\r"), (60, "a\rb"), (50, "\roo"), (40, "\n"), (30, "a\nb"), (20, "\noo"),
      (0, "\roo"), (10, "a\u20ACa") , (-10, "o\\aood"), (-20, "foo"), (-30, "food"),(-40, "foodu"),
      (-50,"abc%abc"), (-60,"abc%&^abc"), (-70, """"%SystemDrive%\Users\John"""),
      (-80, """%SystemDrive%\\Users\\John"""), (-90, "aa^def"), (-100, "acna"), (-110, "_"),
      (-110, "cn"), (-120, "aa[d]abc"), (-130, "aa(d)abc"), (-140, "a?b"), (-150, "a+c"), (-160, "a{3}"),
      (-170, "aaa"), (-180, """\abc"""))
    .toDF("number","word")
    // This persist call makes it so that the plan does not fold operations like filter etc. It is
    // used here since repartition() doesn't do the trick.
    // It does leak memory for the lifecycle of the test which is small.
    someDF.persist
  }

  def mixedDfWithNulls(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Long, java.lang.Double, java.lang.Integer, String)](
      (100L, 1.0, 99, "A"),
      (200L, 2.0, 98, "A"),
      (300L, null, 97, "B"),
      (400L, 4.0, null, "B"),
      (500L, 5.0, 98, "C"),
      (null, 6.0, 97, "C"),
      (-500L, null, 96, null),
      (-700L, 8.0, null, "E\u0480\u0481"),
      (null, 9.0, 90, "g\nH"),
      (1200L, null, null, "IJ\"\u0100\u0101\u0500\u0501"),
      (null, null, null, "\ud720\ud721")
    ).toDF("longs", "doubles", "ints", "strings")
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

  def datesPostEpochDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      118800L, //1970-01-02 09:00:00
      1293901860L, //2011-01-01 17:11:00
      318402000L, //1980-02-03 05:00:00
      604996200L, //1989-03-04 06:30:00
      1270413572L, //2010-04-04 20:39:32
      1588734621L, //2020-05-06 03:10:21
      2550814152L, //2050-10-31 07:29:12
      4102518778L, //2100-01-01 20:32:58
      702696234, //1992-04-08 01:23:54
      6516816203L, //2176-07-05 02:43:23
      26472091292L, //2808-11-12 22:41:32
      4133857172L, //2100-12-30 01:39:32
      1560948892, //2019-06-19 12:54:52
      4115217600L //2100-05-28 20:00:00
    ).toDF("dates")
  }

  def stringsAndLongsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      ("A", 1L),
      ("B", 2L),
      ("C", 3L),
      ("D", 4L),
      ("E", 5L),
      ("F", 6L),
      ("G", 0L),
      ("A", 10L)
    ).toDF("strings", "longs")
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

  def intsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100, 1),
      (200, 2),
      (300, 3),
      (400, 4),
      (500, 5),
      (-100, 6),
      (-500, 1),
      (23, 7)
    ).toDF("ints", "more_ints")
  }

  def biggerLongsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 0L),
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 0L),
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 0L),
      (100L, 1L),
      (200L, 2L),
      (300L, 3L),
      (400L, 4L),
      (500L, 5L),
      (-100L, 6L),
      (-500L, 0L),
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
      (-500L, 50L),
      (100L, 100L)
    ).toDF("longs", "more_longs")
  }

  def smallDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (1.4, 1.134),
      (2.1, 2.4),
      (3.0, 3.42),
      (4.4, 4.5),
      (5.345, 5.2),
      (-1.3, 6.0),
      (-5.14, 0.0)
    ).toDF("doubles", "more_doubles")
  }

  def doubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (24854.55893, 90770.74881),
      (79946.87288, -15456.4335),
      (7967.43488, 32213.22119),
      (-86099.68377, 36223.96138),
      (63477.14374, 98993.65544),
      (13763380.78173, 19869268.744),
      (8677894.99092, 4029109.83562)
    ).toDF("doubles", "more_doubles")
  }

  def nonZeroDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100.3, 1.09),
      (200.1, 2.12),
      (300.5, 3.5),
      (400.0, 4.32),
      (500.5, 5.0),
      (-100.1, 6.4),
      (-500.934, 50.5)
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
      (100.20f, 1.0f),
      (200.0f, 2.04f),
      (300.430f, 3.40f),
      (400.02f, 4.0f),
      (500.09f, 5.03f),
      (-100.2f, 6.102f),
      (-500.3f, 50.5f)
    ).toDF("floats", "more_floats")
  }

  def doubleStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      ("100.23", "1.0"),
      ("200.65", "2.3"),
      ("300.12", "3.6"),
      ("400.43", "4.1"),
      ("500.09", "5.0009"),
      ("-100.124", "6.234"),
      ("-500.13", "0.23"),
      ("50.65", "50.5")
    ).toDF("doubles", "more_doubles")
  }

  def nullableFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (100.44f, 1.046f),
      (200.2f, null),
      (300.230f, 3.04f),
      (null, 4.0f),
      (500.09f, null),
      (null, 6.10f),
      (-500.0f, 50.5f)
    ).toDF("floats", "more_floats")
  }

  def floatWithNansDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (100.50f, 1.0f),
      (200.80f, Float.NaN),
      (300.30f, 3.0f),
      (Float.NaN, 4.0f),
      (500.0f, Float.NaN),
      (Float.NaN, 6.0f),
      (-500.0f, 50.5f)
    ).toDF("floats", "more_floats")
  }


  def floatWithInfinityDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (100.50f, 1.0f),
      (200.80f, Float.NegativeInfinity),
      (300.30f, 3.0f),
      (Float.PositiveInfinity, 4.0f),
      (500.0f, Float.NegativeInfinity),
      (Float.PositiveInfinity, 6.0f),
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

  def nullableStringsIntsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(String, Integer)](
      ("100.0", 1),
      (null, 2),
      (null, 10),
      ("300.0", 3),
      ("400.0", null),
      ("500.0", 5),
      ("500.0", 7),
      ("-100.0", null),
      ("-100.0", 27),
      ("-500.0", 0)
    ).toDF("strings", "ints")
  }

  def utf8RepeatedDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = (0 until 256 /*65536*/).map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars ++ utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  def nullableStringsFromCsv = {
    fromCsvDf("strings.csv", StructType(Array(
      StructField("strings", StringType, true),
      StructField("more_strings", StringType, true)
    )))(_)
  }

  def singularDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(1.1).toDF("double")
  }

  // Note: some tests here currently use this to force Spark not to
  // push down expressions into the scan (e.g. GpuFilters need this)
  def fromCsvDf(file: String, schema: StructType, hasHeader: Boolean = false)
               (session: SparkSession): DataFrame = {
    val resource = this.getClass.getClassLoader.getResource(file).toString
    val df = if (hasHeader) {
      session.read.format("csv").option("header", "true")
    } else {
      session.read.format("csv")
    }
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

  def intsFromCsvWitHeader = {
    fromCsvDf("test_withheaders.csv", StructType(Array(
      StructField("ints_1", IntegerType),
      StructField("ints_2", IntegerType),
      StructField("ints_3", IntegerType),
      StructField("ints_4", IntegerType),
      StructField("ints_5", IntegerType)
    )), true)(_)
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

  def mixedTypesFromCsvWithHeader = {
    fromCsvDf("mixed-types.csv", StructType(Array(
      StructField("c_string", StringType),
      StructField("c_int", IntegerType),
      StructField("c_timestamp", TimestampType)
    )), hasHeader = true)(_)
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
