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
package com.nvidia.spark.rapids

import java.io.File
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.util.{Locale, TimeZone}

import scala.reflect.ClassTag
import scala.util.{Failure, Try}

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._

object TestResourceFinder {
  private [this] var resourcePrefix: String = _

  def setPrefix(prefix: String): Unit = {
    resourcePrefix = prefix
  }

  def getResourcePath(path: String): String = {
    if (resourcePrefix == null) {
      val resource = this.getClass.getClassLoader.getResource(path)
      if (resource == null) {
        throw new IllegalStateException(s"Could not find $path with " +
          s"classloader ${this.getClass.getClassLoader}")
      }
      resource.toString
    } else {
      resourcePrefix + "/" + path
    }
  }
}

object SparkSessionHolder extends Logging {

  private var spark = createSparkSession()
  private var origConf = spark.conf.getAll
  private var origConfKeys = origConf.keys.toSet

  private def setAllConfs(confs: Array[(String, String)]): Unit = confs.foreach {
    case (key, value) if spark.conf.get(key, null) != value =>
      spark.conf.set(key, value)
    case _ => // No need to modify it
  }

  private def createSparkSession(): SparkSession = {
    // Timezone is fixed to UTC to allow timestamps to work by default
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    // Add Locale setting
    Locale.setDefault(Locale.US)

    val builder = SparkSession.builder()
        .master("local[1]")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.rapids.sql.enabled", "false")
        .config("spark.rapids.sql.test.enabled", "false")
        .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
        .config("spark.sql.queryExecutionListeners",
          "com.nvidia.spark.rapids.ExecutionPlanCaptureCallback")
        .config("spark.sql.warehouse.dir", sparkWarehouseDir.getAbsolutePath)
        .appName("rapids spark plugin integration tests (scala)")

    // comma separated config from command line
    val commandLineVariables = System.getenv("SPARK_CONF")
    if (commandLineVariables != null) {
      commandLineVariables.split(",").foreach { s =>
        val a = s.split("=")
        builder.config(a(0), a(1))
      }
    }

    builder.getOrCreate()
  }

  private def reinitSession(): Unit = {
    spark = createSparkSession()
    origConf = spark.conf.getAll
    origConfKeys = origConf.keys.toSet
  }

  def sparkSession: SparkSession = {
    if (SparkSession.getActiveSession.isEmpty) {
      reinitSession()
    }
    spark
  }

  def resetSparkSessionConf(): Unit = {
    if (SparkSession.getActiveSession.isEmpty) {
      reinitSession()
    } else {
      setAllConfs(origConf.toArray)
      val currentKeys = spark.conf.getAll.keys.toSet
      val toRemove = currentKeys -- origConfKeys
      toRemove.foreach(spark.conf.unset)
    }
    logDebug(s"RESET CONF TO: ${spark.conf.getAll}")
  }

  def withSparkSession[U](conf: SparkConf, f: SparkSession => U): U = {
    resetSparkSessionConf
    logDebug(s"SETTING  CONF: ${conf.getAll.toMap}")
    setAllConfs(conf.getAll)
    logDebug(s"RUN WITH CONF: ${spark.conf.getAll}\n")
    f(spark)
  }

  private lazy val sparkWarehouseDir: File = {
    val path = Files.createTempDirectory("spark-warehouse")
    val file = new File(path.toString)
    file.deleteOnExit()
    file
  }
}

/**
 * Set of tests that compare the output using the CPU version of spark vs our GPU version.
 */
trait SparkQueryCompareTestSuite extends FunSuite with Arm {
  import SparkSessionHolder.withSparkSession

  //  @see java.lang.Float#intBitsToFloat
  // <quote>
  // If the argument is any value in the range `0x7f800001` through `0x7fffffff` or
  // in the range `0xff800001` through `0xffffffff`, the result is a NaN
  // </quote>
  val FLOAT_POSITIVE_NAN_LOWER_RANGE = java.lang.Float.intBitsToFloat(0x7f800001)
  val FLOAT_POSITIVE_NAN_UPPER_RANGE = java.lang.Float.intBitsToFloat(0x7fffffff)
  val FLOAT_NEGATIVE_NAN_LOWER_RANGE = java.lang.Float.intBitsToFloat(0xff800001)
  val FLOAT_NEGATIVE_NAN_UPPER_RANGE = java.lang.Float.intBitsToFloat(0xffffffff)

  // see java.lang.Double#longBitsToDouble
  // <quote>
  // <p>If the argument is any value in the range `0x7ff0000000000001L` through
  // `0x7fffffffffffffffL` or in the range
  // `0xfff0000000000001L` through
  // `0xffffffffffffffffL`, the result is a NaN
  val DOUBLE_POSITIVE_NAN_LOWER_RANGE = java.lang.Double.longBitsToDouble(0x7ff0000000000001L)
  val DOUBLE_POSITIVE_NAN_UPPER_RANGE = java.lang.Double.longBitsToDouble(0x7fffffffffffffffL)
  val DOUBLE_NEGATIVE_NAN_LOWER_RANGE = java.lang.Double.longBitsToDouble(0xfff0000000000001L)
  val DOUBLE_NEGATIVE_NAN_UPPER_RANGE = java.lang.Double.longBitsToDouble(0xffffffffffffffffL)

  def withGpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    val c = conf.clone()
      .set(RapidsConf.SQL_ENABLED.key, "true")
      .set(RapidsConf.TEST_CONF.key, "true")
      .set(RapidsConf.EXPLAIN.key, "ALL")
    withSparkSession(c, f)
  }

  def withCpuSparkSession[U](f: SparkSession => U, conf: SparkConf = new SparkConf()): U = {
    val c = conf.clone()
      .set(RapidsConf.SQL_ENABLED.key, "false") // Just to be sure
      // temp work around to unsupported timestamp type
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    withSparkSession(c, f)
  }

  def compare(expected: Any, actual: Any, epsilon: Double = 0.0): Boolean = {
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
        ("SUCCESS", true)
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
          b.keys.find(bKey => compare(aKey, bKey))
                .exists(bKey => compare(a(aKey), b(bKey), epsilon))
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

  def runOnCpuAndGpuWithCapture(df: SparkSession => DataFrame,
      fun: DataFrame => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1)
  : (Array[Row], SparkPlan, Array[Row], SparkPlan) = {
    conf.setIfMissing("spark.sql.shuffle.partitions", "2")

    // force a new session to avoid accidentally capturing a late callback from a previous query
    TrampolineUtil.cleanupAnyExistingSession()
    ExecutionPlanCaptureCallback.startCapture()
    var cpuPlan: Option[SparkPlan] = null
    val fromCpu =
      try {
        withCpuSparkSession(session => {
          var data = df(session)
          if (repart > 0) {
            // repartition the data so it is turned into a projection,
            // not folded into the table scan exec
            data = data.repartition(repart)
          }
          fun(data).collect()
        }, conf)
      } finally {
        cpuPlan = ExecutionPlanCaptureCallback.getResultWithTimeout()
      }
    if (cpuPlan.isEmpty) {
      throw new RuntimeException("Did not capture CPU plan")
    }

    ExecutionPlanCaptureCallback.startCapture()
    var gpuPlan: Option[SparkPlan] = null
    val fromGpu =
      try {
        withGpuSparkSession(session => {
          var data = df(session)
          if (repart > 0) {
            // repartition the data so it is turned into a projection,
            // not folded into the table scan exec
            data = data.repartition(repart)
          }
          fun(data).collect()
        }, conf)
      } finally {
        gpuPlan = ExecutionPlanCaptureCallback.getResultWithTimeout()
      }

    if (gpuPlan.isEmpty) {
      throw new RuntimeException("Did not capture GPU plan")
    }

    (fromCpu, cpuPlan.get, fromGpu, gpuPlan.get)
  }

  def testGpuFallback(testName: String,
      fallbackCpuClass: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame): Unit = {
    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)
    test(qualifiedTestName) {
      val (fromCpu, _, fromGpu, gpuPlan) = runOnCpuAndGpuWithCapture(df, fun,
        conf = testConf,
        repart = repart)
      // Now check the GPU Conditions
      ExecutionPlanCaptureCallback.assertDidFallBack(gpuPlan, fallbackCpuClass)
      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
    }
  }

  /**
   * Runs a test defined by fun, using dataframe df.
   *
   * @param df the DataFrame to use as input
   * @param fun the function to transform the DataFrame (produces another DataFrame)
   * @param conf spark conf
   * @return tuple of (cpu results, gpu results) as arrays of Row
   */
  def runOnCpuAndGpu(
      df: SparkSession => DataFrame,
      fun: DataFrame => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1): (Array[Row], Array[Row]) = {
    conf.setIfMissing("spark.sql.shuffle.partitions", "2")
    val fromCpu = withCpuSparkSession( session => {
      var data = df(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection,
        // not folded into the table scan exec
        data = data.repartition(repart)
      }
      fun(data).collect()
    }, conf)

    val fromGpu = withGpuSparkSession( session => {
      var data = df(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection,
        // not folded into the table scan exec
        data = data.repartition(repart)
      }
      fun(data).collect()
    }, conf)

    (fromCpu, fromGpu)
  }

  /**
   * Runs a test defined by fun, using 2 dataframes dfA and dfB.
   *
   * @param dfA the first DataFrame to use as input
   * @param dfB the second DataFrame to use as input
   * @param fun the function to transform the DataFrame (produces another DataFrame)
   * @param conf spark conf
   * @return tuple of (cpu results, gpu results) as arrays of Row
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
        // repartition the data so it is turned into a projection,
        // not folded into the table scan exec
        dataA = dataA.repartition(repart)
        dataB = dataB.repartition(repart)
      }
      fun(dataA, dataB).collect()
    }, conf)

    val fromGpu = withGpuSparkSession((session) => {
      var dataA = dfA(session)
      var dataB = dfB(session)
      if (repart > 0) {
        // repartition the data so it is turned into a projection,
        // not folded into the table scan exec
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
      execsAllowedNonGpu: Seq[String],
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      execsAllowedNonGpu=execsAllowedNonGpu,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      testName: String,
      df: SparkSession => DataFrame,
      execsAllowedNonGpu: Seq[String],
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame)
      (validateCapturedPlans: (SparkPlan, SparkPlan) => Unit): Unit = {
    testSparkResultsAreEqualWithCapture(testName, df,
      conf=conf,
      execsAllowedNonGpu=execsAllowedNonGpu,
      sortBeforeRepart = sortBeforeRepart)(fun)(validateCapturedPlans)
  }

  def IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqual(
      testName: String,
      df: SparkSession => DataFrame,
      execsAllowedNonGpu: Seq[String],
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    testSparkResultsAreEqual(testName, df,
      conf=conf,
      execsAllowedNonGpu=execsAllowedNonGpu,
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)
  }

  def IGNORE_ORDER_ALLOW_NON_GPU_testSparkResultsAreEqualWithCapture(
      testName: String,
      df: SparkSession => DataFrame,
      execsAllowedNonGpu: Seq[String],
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame)
      (validateCapturedPlans: (SparkPlan, SparkPlan) => Unit): Unit = {
    testSparkResultsAreEqualWithCapture(testName, df,
      conf=conf,
      execsAllowedNonGpu=execsAllowedNonGpu,
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)(validateCapturedPlans)
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

  def IGNORE_ORDER_testSparkResultsAreEqualWithCapture(
      testName: String,
      df: SparkSession => DataFrame,
      repart: Integer = 1,
      conf: SparkConf = new SparkConf(),
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame)
      (validateCapturedPlans: (SparkPlan, SparkPlan) => Unit): Unit = {
    testSparkResultsAreEqualWithCapture(testName, df,
      conf=conf,
      repart=repart,
      sort=true,
      sortBeforeRepart = sortBeforeRepart)(fun)(validateCapturedPlans)
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
   * @param df the DataFrame to use as input
   * @param writer the function to write the data to a file
   * @param reader the function to read the data from a file
   * @param conf spark conf
   * @return tuple of (cpu results, gpu results) as arrays of Row
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
          case (i1: Float, i2: Float) => if (i1.isNaN() && !i2.isNaN()) return false
          else if (!i1.isNaN() && i2.isNaN()) return true
          else if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Date, i2: Date) => if (i1.before(i2)) {
            return true
          } else if (i1.after(i2)) {
            return false
          } // else equal go on
          case (i1: Double, i2: Double) => if (i1.isNaN() && !i2.isNaN()) return false
          else if (!i1.isNaN() && i2.isNaN()) return true
          else if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Short, i2: Short) => if (i1 < i2) {
            return true
          } else if (i1 > i2) {
            return false
          } // else equal go on
          case (i1: Timestamp, i2: Timestamp) => if (i1.before(i2)) {
            return true
          } else if (i1.after(i2)) {
            return false
          } // else equal go on
          case (s1: String, s2: String) =>
            val cmp = s1.compareTo(s2)
            if (cmp < 0) {
              return true
            } else if (cmp > 0) {
              return false
            } // else equal go on
          case (o1, o2) =>
            throw new UnsupportedOperationException(o1.getClass + " is not supported yet")
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
      conf: SparkConf,
      execsAllowedNonGpu: Seq[String],
      maxFloatDiff: Double,
      sortBeforeRepart: Boolean): (SparkConf, String) = {

    var qualifiers = Set[String]()
    var testConf = conf
    if (incompat) {
      testConf = testConf.clone().set(RapidsConf.INCOMPATIBLE_OPS.key, "true")
      qualifiers = qualifiers + "INCOMPAT"
    } else if (maxFloatDiff > 0.0) {
      qualifiers = qualifiers + "APPROXIMATE FLOAT"
    }

    if (sort) {
      qualifiers = qualifiers + "IGNORE ORDER"
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
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false,
      assumeCondition: SparkSession => (Boolean, String) = null)
      (fun: DataFrame => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)

    test(qualifiedTestName) {
      if (assumeCondition != null) {
        val (isAllowed, reason) = withCpuSparkSession(assumeCondition, conf = testConf)
        assume(isAllowed, reason)
      }
      val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
        conf = testConf,
        repart = repart)
      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
    }
  }

  def testSparkResultsAreEqualWithCapture(
      testName: String,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame)
      (validateCapturedPlans: (SparkPlan, SparkPlan) => Unit): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)
    test(qualifiedTestName) {
      val (fromCpu, cpuPlan, fromGpu, gpuPlan) = runOnCpuAndGpuWithCapture(df, fun,
        conf = testConf,
        repart = repart)
      compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
      validateCapturedPlans(cpuPlan, gpuPlan)
    }
  }

  /** Create a DataFrame from a sequence of values and execution a transformation on CPU and GPU,
   *  and compare results */
  def testUnaryFunction(
    conf: SparkConf,
    values: Seq[Any],
    expectNull: Boolean = false,
    maxFloatDiff: Double = 0.0,
    sort: Boolean = false,
    repart: Integer = 1,
    sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {

    val df: SparkSession => DataFrame =
      (sparkSession: SparkSession) => createDataFrame(sparkSession, values)

    val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
      conf,
      repart = repart)

    compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
  }

  def testExpectedGpuException[T <: Throwable](
    testName: String,
    exceptionClass: Class[T],
    df: SparkSession => DataFrame,
    conf: SparkConf = new SparkConf(),
    repart: Integer = 1,
    sort: Boolean = false,
    maxFloatDiff: Double = 0.0,
    incompat: Boolean = false,
    execsAllowedNonGpu: Seq[String] = Seq.empty,
    sortBeforeRepart: Boolean = false)(fun: DataFrame => DataFrame): Unit = {
    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)

    test(qualifiedTestName) {
      val t = Try({
        val fromGpu = withGpuSparkSession( session => {
          var data = df(session)
          if (repart > 0) {
            // repartition the data so it is turned into a projection,
            // not folded into the table scan exec
            data = data.repartition(repart)
          }
          fun(data).collect()
        }, testConf)
      })
      t match {
        case Failure(e) if e.getClass == exceptionClass => // Good
        case Failure(e) => throw e
        case _ => fail("Expected an exception")
      }
    }
  }

  def testExpectedException[T <: Throwable](
      testName: String,
      expectedException: T => Boolean,
      df: SparkSession => DataFrame,
      conf: SparkConf = new SparkConf(),
      repart: Integer = 1,
      sort: Boolean = false,
      maxFloatDiff: Double = 0.0,
      incompat: Boolean = false,
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
      (fun: DataFrame => DataFrame)(implicit classTag: ClassTag[T]): Unit = {
    val clazz = classTag.runtimeClass
    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)

      test(qualifiedTestName) {
        val t = Try({
          val (fromCpu, fromGpu) = runOnCpuAndGpu(df, fun,
            conf = testConf,
            repart = repart)
          compareResults(sort, maxFloatDiff, fromCpu, fromGpu)
        })
        t match {
          case Failure(e) if clazz.isAssignableFrom(e.getClass) =>
            assert(expectedException(e.asInstanceOf[T]))
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
      execsAllowedNonGpu: Seq[String] = Seq.empty,
      sortBeforeRepart: Boolean = false)
    (fun: (DataFrame, DataFrame) => DataFrame): Unit = {

    val (testConf, qualifiedTestName) =
      setupTestConfAndQualifierName(testName, incompat, sort, conf, execsAllowedNonGpu,
        maxFloatDiff, sortBeforeRepart)

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
      setupTestConfAndQualifierName(testName, false, sort, conf, Nil,
        0.0, sortBeforeRepart)

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

  def mixedDf(session: SparkSession, numSlices: Int = 2): DataFrame = {
    val rows = Seq[Row](Row(99, 100L, 1.0, "A", Decimal("1.2")),
      Row(98, 200L, 2.0, "B", Decimal("1.3")),
      Row(97, 300L, 3.0, "C", Decimal("1.4")),
      Row(99, 400L, 4.0, "D", Decimal("1.5")),
      Row(98, 500L, 5.0, "E", Decimal("1.6")),
      Row(97, -100L, 6.0, "F", Decimal("1.7")),
      Row(96, -500L, 0.0, "G", Decimal("1.8")),
      Row(95, -700L, 8.0, "E\u0480\u0481", Decimal("1.9")),
      Row(Int.MaxValue, Long.MinValue, Double.PositiveInfinity, "\u0000", Decimal("2.0")),
      Row(Int.MinValue, Long.MaxValue, Double.NaN, "\u0000", Decimal("100.123")),
      Row(null, null, null, "actions are judged by intentions", Decimal("200.246")),
      Row(94, -900L, 9.0, "g\nH", Decimal("300.369")),
      Row(92, -1200L, 12.0, "IJ\"\u0100\u0101\u0500\u0501", Decimal("-1.47e3")),
      Row(90, 1500L, 15.0, "\ud720\ud721", Decimal("-22.2345")))
    val structType = StructType(Seq(StructField("ints", IntegerType),
        StructField("longs", LongType),
        StructField("doubles", DoubleType),
        StructField("strings", StringType),
        StructField("decimals", DecimalType(15, 5))))
    session.createDataFrame(
      session.sparkContext.parallelize(rows, numSlices), structType)
  }

  def likeDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (150, "abc"), (140, "a*c"), (130, "ab"), (120, "ac"), (110, "a|bc"), (100, "a.b"), (90, "b$"),
      (80, "b$c"), (70, "\r"), (60, "a\rb"), (50, "\roo"), (40, "\n"), (30, "a\nb"), (20, "\noo"),
      (0, "\roo"), (10, "a\u20ACa") , (-10, "o\\aood"), (-20, "foo"), (-30, "food"),(-40, "foodu"),
      (-50,"abc%abc"), (-60,"abc%&^abc"), (-70, """"%SystemDrive%\Users\John"""),
      (-80, """%SystemDrive%\\Users\\John"""), (-90, "aa^def"), (-100, "acna"), (-110, "_"),
      (-110, "cn"), (-120, "aa[d]abc"), (-130, "aa(d)abc"), (-140, "a?b"), (-150, "a+c"),
      (-160, "a{3}"), (-170, "aaa"), (-180, """\abc"""))
    .toDF("number","word")
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
    Seq[(java.lang.Boolean, java.lang.Boolean)](
      (true, true),
      (false, true),
      (true, false),
      (false, false),
      (null, true)
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

  def timestampsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._

    Seq(
      (Timestamp.valueOf("0100-1-1 23:00:01"), Timestamp.valueOf("2100-1-1 23:12:01")),
      (Timestamp.valueOf("0211-1-1 03:00:01"), Timestamp.valueOf("1492-4-7 07:00:01")),
      (Timestamp.valueOf("1900-2-2 01:10:01"), Timestamp.valueOf("1776-7-4 05:22:52")),
      (Timestamp.valueOf("1989-3-3 11:04:10"), Timestamp.valueOf("1808-11-12 21:00:33")),
      (Timestamp.valueOf("2010-4-4 13:00:56"), Timestamp.valueOf("2100-12-30 02:54:10")),
      (Timestamp.valueOf("2019-6-5 06:24:24"), Timestamp.valueOf("2019-6-19 16:00:01")),
      (Timestamp.valueOf("2020-2-5 06:24:24"), Timestamp.valueOf("2019-6-19 16:00:01")),
      (Timestamp.valueOf("2020-5-5 06:24:24"), Timestamp.valueOf("2100-12-30 02:54:10")),
      (Timestamp.valueOf("2050-10-30 08:44:30"), Timestamp.valueOf("0100-5-28 08:23:12"))
    ).toDF("timestamps", "more_timestamps")
  }

  def epochDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      -1586549475L, //1919-09-23 3:48:45 AM
      1435708800L, //2015-07-01 12:00:00 (the day after leap second was added)
      118800L, //1970-01-02 09:00:00
      1293901860L, //2011-01-01 17:11:00
      318402000L, //1980-02-03 05:00:00
      604996200L, //1989-03-04 06:30:00
      -12586549L, //1969-08-08 7:44:11 AM
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

  def datesPostEpochDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      1435708800L, //2015-07-01 12:00:00 (the day after leap second was added)
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

  def exponentsAsStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq("1.38E-2",
      "1.2e-2",
      "3.544E+2",
      "3.2e+2",
      "4.5E3",
      "9.8e5").toDF("c0")
  }

  def badFloatStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(("A", "null"), ("1.3", "43.54")).toDF("c0", "c1")
  }

  def badDoubleStringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq("1.7976931348623159E308", "-1.7976931348623159E308").toDF("c0")
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
      (Long.MinValue, 4L),
      (Long.MaxValue, 4L),
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

  def nanDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Double](
      DOUBLE_NEGATIVE_NAN_LOWER_RANGE,
      Double.NaN,
      1.0d,
      2.0d
    ).toDF("doubles")
  }

  def nullDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Long, java.lang.Long)](
      (100L, 15L),
      (100L, null)
    ).toDF("longs", "more_longs")
  }

  def mixedDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Double, java.lang.Double)](
      (Double.PositiveInfinity, Double.PositiveInfinity),
      (Double.PositiveInfinity, Double.NegativeInfinity),
      (Double.PositiveInfinity, 0.8435376941d),
      (Double.PositiveInfinity, 23.1927672582d),
      (Double.PositiveInfinity, 2309.4430349398d),
      (Double.PositiveInfinity, Double.NaN),
      (Double.PositiveInfinity, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
      (Double.PositiveInfinity, null),
      (Double.PositiveInfinity, -0.7078783860d),
      (Double.PositiveInfinity, -70.9667587507d),
      (Double.PositiveInfinity, -838600.5867225748d),

      (Double.NegativeInfinity, Double.PositiveInfinity),
      (Double.NegativeInfinity, Double.NegativeInfinity),
      (Double.NegativeInfinity, 0.8435376941d),
      (Double.NegativeInfinity, 23.1927672582d),
      (Double.NegativeInfinity, 2309.4430349398d),
      (Double.NegativeInfinity, Double.NaN),
      (Double.NegativeInfinity, DOUBLE_NEGATIVE_NAN_LOWER_RANGE),
      (Double.NegativeInfinity, null),
      (Double.NegativeInfinity, -0.7078783860d),
      (Double.NegativeInfinity, -70.9667587507d),
      (Double.NegativeInfinity, -838600.5867225748d),

      (30.0922503207d, Double.PositiveInfinity),
      (0.3547166487d, Double.NegativeInfinity),
      (430.0986947541d, 0.8435376941d),
      (40.2292464632d, 23.1927672582d),
      (0.0684630071d, 2309.4430349398d),
      (24310.3764726531d, Double.NaN),
      (0.0917656668d, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
      (50.6053004384d, null),
      (7880.7542578934d, -0.7078783860d),
      (20.5882386034d, -70.9667587507d),
      (0.6467140578d, -838600.5867225748d),

      (Double.NaN, Double.PositiveInfinity),
      (Double.NaN, Double.NegativeInfinity),
      (Double.NaN, 0.8435376941d),
      (Double.NaN, 23.1927672582d),
      (Double.NaN, 2309.4430349398d),
      (Double.NaN, Double.NaN),
      (Double.NaN, DOUBLE_NEGATIVE_NAN_LOWER_RANGE),
      (Double.NaN, null),
      (Double.NaN, -0.7078783860d),
      (Double.NaN, -70.9667587507d),
      (Double.NaN, -838600.5867225748d),

      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, Double.PositiveInfinity),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, Double.NegativeInfinity),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, 0.8435376941d),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, 23.1927672582d),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, 2309.4430349398d),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, Double.NaN),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, null),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, -0.7078783860d),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, -70.9667587507d),
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, -838600.5867225748d),

      (null, Double.PositiveInfinity),
      (null, Double.NegativeInfinity),
      (null, 0.8435376941d),
      (null, 23.1927672582d),
      (null, 2309.4430349398d),
      (null, Double.NaN),
      (null, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
      (null, null),
      (null, -0.7078783860d),
      (null, -70.9667587507d),
      (null, -838600.5867225748d),

      (-30.0922503207d, Double.PositiveInfinity),
      (-0.3547166487d, Double.NegativeInfinity),
      (-430.0986947541d, 0.8435376941d),
      (-40.2292464632d, 23.1927672582d),
      (-0.0684630071d, 2309.4430349398d),
      (-24310.3764726531d, Double.NaN),
      (-0.0917656668d, DOUBLE_NEGATIVE_NAN_UPPER_RANGE),
      (-50.6053004384d, null),
      (-7880.7542578934d, -0.7078783860d),
      (-20.5882386034d, -70.9667587507d),
      (-0.6467140578d, -838600.5867225748d)
    ).toDF("doubles", "more_doubles")
  }

  def mixedSingleColumnDoubleDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Double](
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      0.8435376941d,
      23.1927672582d,
      2309.4430349398d,
      Double.NaN,
      DOUBLE_POSITIVE_NAN_LOWER_RANGE,
      DOUBLE_POSITIVE_NAN_UPPER_RANGE,
      DOUBLE_NEGATIVE_NAN_LOWER_RANGE,
      DOUBLE_NEGATIVE_NAN_UPPER_RANGE,
      null,
      -0.7078783860d,
      -70.9667587507d,
      -838600.5867225748d
    ).toDF("doubles")
  }

  def mixedSingleColumnFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[java.lang.Float](
      Float.PositiveInfinity,
      Float.NegativeInfinity,
      0.8435376941f,
      23.1927672582f,
      2309.4430349398f,
      Float.NaN,
      FLOAT_POSITIVE_NAN_LOWER_RANGE,
      FLOAT_NEGATIVE_NAN_LOWER_RANGE,
      FLOAT_POSITIVE_NAN_UPPER_RANGE,
      FLOAT_NEGATIVE_NAN_UPPER_RANGE,
      null,
      -0.7078783860f,
      -70.9667587507f,
      -838600.5867225748f
    ).toDF("floats")
  }

  def mixedFloatDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (Float.PositiveInfinity, Float.PositiveInfinity),
      (Float.PositiveInfinity, Float.NegativeInfinity),
      (Float.PositiveInfinity, 0.8435376941f),
      (Float.PositiveInfinity, 23.1927672582f),
      (Float.PositiveInfinity, 2309.4430349398f),
      (Float.PositiveInfinity, Float.NaN),
      (Float.PositiveInfinity, FLOAT_NEGATIVE_NAN_UPPER_RANGE),
      (Float.PositiveInfinity, null),
      (Float.PositiveInfinity, -0.7078783860f),
      (Float.PositiveInfinity, -70.9667587507f),
      (Float.PositiveInfinity, -838600.5867225748f),

      (Float.NegativeInfinity, Float.PositiveInfinity),
      (Float.NegativeInfinity, Float.NegativeInfinity),
      (Float.NegativeInfinity, 0.8435376941f),
      (Float.NegativeInfinity, 23.1927672582f),
      (Float.NegativeInfinity, 2309.4430349398f),
      (Float.NegativeInfinity, Float.NaN),
      (Float.NegativeInfinity, FLOAT_NEGATIVE_NAN_LOWER_RANGE),
      (Float.NegativeInfinity, null),
      (Float.NegativeInfinity, -0.7078783860f),
      (Float.NegativeInfinity, -70.9667587507f),
      (Float.NegativeInfinity, -838600.5867225748f),

      (30.0922503207f, Float.PositiveInfinity),
      (0.3547166487f, Float.NegativeInfinity),
      (430.0986947541f, 0.8435376941f),
      (40.2292464632f, 23.1927672582f),
      (0.0684630071f, 2309.4430349398f),
      (24310.3764726531f, Float.NaN),
      (0.0917656668f, FLOAT_NEGATIVE_NAN_UPPER_RANGE),
      (50.6053004384f, null),
      (7880.7542578934f, -0.7078783860f),
      (20.5882386034f, -70.9667587507f),
      (0.6467140578f, -838600.5867225748f),

      (Float.NaN, Float.PositiveInfinity),
      (Float.NaN, Float.NegativeInfinity),
      (Float.NaN, 0.8435376941f),
      (Float.NaN, 23.1927672582f),
      (Float.NaN, 2309.4430349398f),
      (Float.NaN, Float.NaN),
      (Float.NaN, FLOAT_NEGATIVE_NAN_LOWER_RANGE),
      (Float.NaN, null),
      (Float.NaN, -0.7078783860f),
      (Float.NaN, -70.9667587507f),
      (Float.NaN, -838600.5867225748f),

      (-Float.NaN, Float.PositiveInfinity),
      (-Float.NaN, Float.NegativeInfinity),
      (-Float.NaN, 0.8435376941f),
      (-Float.NaN, 23.1927672582f),
      (-Float.NaN, 2309.4430349398f),
      (-Float.NaN, Float.NaN),
      (-Float.NaN, FLOAT_NEGATIVE_NAN_LOWER_RANGE),
      (-Float.NaN, null),
      (-Float.NaN, -0.7078783860f),
      (-Float.NaN, -70.9667587507f),
      (-Float.NaN, -838600.5867225748f),

      (null, Float.PositiveInfinity),
      (null, Float.NegativeInfinity),
      (null, 0.8435376941f),
      (null, 23.1927672582f),
      (null, 2309.4430349398f),
      (null, Float.NaN),
      (null, FLOAT_NEGATIVE_NAN_UPPER_RANGE),
      (null, null),
      (null, -0.7078783860f),
      (null, -70.9667587507f),
      (null, -838600.5867225748f),

      (-30.0922503207f, Float.PositiveInfinity),
      (-0.3547166487f, Float.NegativeInfinity),
      (-430.0986947541f, 0.8435376941f),
      (-40.2292464632f, 23.1927672582f),
      (-0.0684630071f, 2309.4430349398f),
      (-24310.3764726531f, Float.NaN),
      (-0.0917656668f, FLOAT_NEGATIVE_NAN_LOWER_RANGE),
      (-50.6053004384f, null),
      (-7880.7542578934f, -0.7078783860f),
      (-20.5882386034f, -70.9667587507f),
      (-0.6467140578f, -838600.5867225748f)
    ).toDF("floats", "more_floats")
  }

  def intnullableFloatWithNullAndNanDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Integer, java.lang.Float)](
      (100, 1.0f),
      (200, 2.04f),
      (300, 3.40f),
      (400, 4.20f),
      (500, FLOAT_POSITIVE_NAN_LOWER_RANGE),
      (100, FLOAT_POSITIVE_NAN_UPPER_RANGE),
      (200, null),
      (300, -0.0f),
      (400, FLOAT_NEGATIVE_NAN_LOWER_RANGE),
      (500, FLOAT_NEGATIVE_NAN_UPPER_RANGE),
      (-500, 50.5f)
    ).toDF("ints", "floats")
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

  def doubleWithNansDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Double, java.lang.Double)](
      (100.50d, 1.0d),
      (200.80d, Double.NaN),
      (300.30d, 3.0d),
      (Double.NaN, 4.0d),
      (500.0d, Double.NaN),
      (Double.NaN, 6.0d),
      (-500.0d, 50.5d),
      (Double.NegativeInfinity, Double.NaN),
      (Double.PositiveInfinity, 1.2d),
      (Double.NaN, 3.2d),
      (null, null)
    ).toDF("doubles", "more_doubles")
  }

  def doubleWithDifferentKindsOfNansAndZeros(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(Double, Int)](
      // Regular numbers
      (1, 10),
      (2, 20),

      // NaNs, in different forms, as per Java documentation for Double.
      // @see java.lang.Double#longBitsToDouble
      // <quote>
      //  If the argument is any value in the range 0x7ff0000000000001L through 0x7fffffffffffffffL,
      //  or in the range 0xfff0000000000001L through 0xffffffffffffffffL, the result is a NaN.
      // </quote>
      (DOUBLE_POSITIVE_NAN_LOWER_RANGE, 30), // Minimum Positive NaN value
      (DOUBLE_POSITIVE_NAN_UPPER_RANGE, 40), // Maximum Positive NaN value
      (DOUBLE_NEGATIVE_NAN_LOWER_RANGE, 50), // Maximum Negative NaN value
      (DOUBLE_NEGATIVE_NAN_UPPER_RANGE, 60), // Minimum Negative NaN value

      // Zeros
      (+0.0, 70), // Minimum Negative NaN value
      (-0.0, 80)  // Minimum Negative NaN value
    ).toDF("double", "int")
  }

  def floatWithDifferentKindsOfNansAndZeros(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(Float, Int)](
      // Regular numbers
      (1, 10),
      (2, 20),

      // NaNs, in different forms, as per Java documentation for Float.
      // @see java.lang.Float#longBitsToDouble
      (FLOAT_POSITIVE_NAN_LOWER_RANGE, 30), // Minimum Positive NaN value
      (FLOAT_POSITIVE_NAN_UPPER_RANGE, 40), // Maximum Positive NaN value
      (FLOAT_NEGATIVE_NAN_LOWER_RANGE, 50), // Maximum Negative NaN value
      (FLOAT_NEGATIVE_NAN_UPPER_RANGE, 60), // Minimum Negative NaN value

      // Zeros
      (+0.0f, 70), // Minimum Negative NaN value
      (-0.0f, 80)  // Minimum Negative NaN value
    ).toDF("float", "int")
  }

  def floatWithNansDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(java.lang.Float, java.lang.Float)](
      (100.50f, 1.0f),
      (200.80f, Float.NaN),
      (300.30f, 3.0f),
      (Float.PositiveInfinity, Float.NegativeInfinity),
      (Float.NegativeInfinity, Float.PositiveInfinity),
      (Float.NaN, 4.0f),
      (Float.PositiveInfinity, 4.0f),
      (Float.NegativeInfinity, 4.0f),
      (0.0f, 4.0f),
      (500.0f, Float.NaN),
      (Float.NaN, 6.0f),
      (-500.0f, 50.5f),
      (Float.NegativeInfinity, Float.NaN),
      (Float.PositiveInfinity, 1.2f),
      (Float.NaN, 3.2f),
      (null, null)
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

  def oldDatesDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      new Date(-141427L * 24 * 60 * 60 * 1000),
      new Date(-150000L * 24 * 60 * 60 * 1000),
      Date.valueOf("1582-10-15"),
      Date.valueOf("1582-10-13")
    ).toDF("dates")
  }

  def oldTsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      new Timestamp(-141427L * 24 * 60 * 60 * 1000),
      new Timestamp(-150000L * 24 * 60 * 60 * 1000),
      Timestamp.valueOf("1582-10-15 00:01:01"),
      Timestamp.valueOf("1582-10-13 12:03:12")
    ).toDF("times")
  }

  def utf8RepeatedDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    var utf8Chars = (0 until 256 /*65536*/).map(i => (i.toChar.toString, i))
    utf8Chars = utf8Chars ++ utf8Chars
    utf8Chars.toDF("strings", "ints")
  }

  def utf8StringsDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq[(String)](
      ("Foo"),
      ("Bar"),
      ("B\ud720\ud721  "),
      (" B\u0480\u0481"),
      (" Baz")
    ).toDF("strings")
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
  def fromCsvPatternDf(
      base: String,
      pattern: String,
      schema: StructType,
      hasHeader: Boolean = false)
    (session: SparkSession): DataFrame = {
    val resource = TestResourceFinder.getResourcePath(base) + "/" + pattern
    val df = if (hasHeader) {
      session.read.format("csv").option("header", "true")
    } else {
      session.read.format("csv")
    }
    df.schema(schema).load(resource).toDF()
  }

  // Note: some tests here currently use this to force Spark not to
  // push down expressions into the scan (e.g. GpuFilters need this)
  def fromCsvDf(file: String, schema: StructType, hasHeader: Boolean = false)
               (session: SparkSession): DataFrame = {
    val resource = TestResourceFinder.getResourcePath(file)
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
      StructField("six", ShortType)
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

  def veryLargeLongsFromCSVDf = {
    fromCsvDf("very_large_longs.csv", StructType(Array(
      StructField("large_longs", LongType, true)
    )))(_)
  }

  def intsFromCsvInferredSchema(session: SparkSession): DataFrame = {
    val path = TestResourceFinder.getResourcePath("test.csv")
    session.read.option("inferSchema", "true").csv(path)
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

  private def setNullableStateForAllColumns(df: DataFrame, nullable: Boolean) : DataFrame = {
    // get schema
    val schema = df.schema
    // modify [[StructField] with name `cn`
    val newSchema = StructType(schema.map {
      case StructField(c, t, _, m) ⇒ StructField(c, t, nullable = nullable, m)
    })
    // apply new schema
    df.sqlContext.createDataFrame(df.rdd, newSchema)
  }

  def windowTestDfOrcNonNullable: SparkSession => DataFrame = {
    frameFromOrcNonNullableColumns(
      filename="window-function-test-nonull.orc"
    )(_)
  }

  def windowTestDfOrc: SparkSession => DataFrame = {
    frameFromOrc(
      filename="window-function-test.orc"
    )(_)
  }

  def frameFromParquet(filename: String): SparkSession => DataFrame = {
    val path = TestResourceFinder.getResourcePath(filename)
    s: SparkSession => s.read.parquet(path)
  }

  def frameFromOrc(filename: String): SparkSession => DataFrame = {
    val path = TestResourceFinder.getResourcePath(filename)
    s: SparkSession => s.read.orc(path)
  }

  def frameFromOrcNonNullableColumns(filename: String): SparkSession => DataFrame = {
    val path = TestResourceFinder.getResourcePath(filename)
    s: SparkSession => {
      val df = s.read.orc(path)
      setNullableStateForAllColumns(df, false)
    }
  }

  /** Transform a sequence of values into a DataFrame with one column */
  def createDataFrame(session: SparkSession, values: Seq[Any]) : DataFrame = {
    import scala.collection.JavaConverters._
    val sparkType = getSparkType(values)
    val rows: Seq[Row] = values.map(v => Row.fromSeq(Seq(v)))
    session.createDataFrame(rows.asJava, StructType(Seq(StructField("n", sparkType))))
  }

  /** Determine the Spark data type for the given sequence of values */
  def getSparkType(values: Seq[Any]) = {
    values.filterNot(v => v == null).headOption match {
      case Some(v) => v match {
        case _: Byte => DataTypes.ByteType
        case _: Short => DataTypes.ShortType
        case _: Integer => DataTypes.IntegerType
        case _: Long => DataTypes.LongType
        case _: Float => DataTypes.FloatType
        case _: Double => DataTypes.DoubleType
      }
      case _ => throw new IllegalArgumentException("There must be at least one non-null value")
    }
  }

}
