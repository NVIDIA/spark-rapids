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

import java.util.{Locale, TimeZone}

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._

/**
 * Set of tests that compare the output using the CPU version of spark vs our GPU version.
 */
class SparkQueryCompareTestSuite extends FunSuite with BeforeAndAfterEach with Logging {

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

  def withGpuSparkSession[U](f: SparkSession => U): U = {
    withSparkSession("gpu-sql-test",
      new SparkConf()
        .set("spark.sql.extensions", "ai.rapids.spark.Plugin"),
      f)
  }

  def withCpuSparkSession[U](f: SparkSession => U): U = {
    withSparkSession("cpu-sql-test", new SparkConf(), f)
  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Map[_, _], b: Map[_, _]) =>
      a.size == b.size && a.keys.forall { aKey =>
        b.keys.find(bKey => compare(aKey, bKey)).exists(bKey => compare(a(aKey), b(bKey)))
      }
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Product, b: Product) =>
      compare(a.productIterator.toSeq, b.productIterator.toSeq)
    case (a: Row, b: Row) =>
      compare(a.toSeq, b.toSeq)
    // 0.0 == -0.0, turn float/double to bits before comparison, to distinguish 0.0 and -0.0.
    case (a: Double, b: Double) =>
      java.lang.Double.doubleToRawLongBits(a) == java.lang.Double.doubleToRawLongBits(b)
    case (a: Float, b: Float) =>
      java.lang.Float.floatToRawIntBits(a) == java.lang.Float.floatToRawIntBits(b)
    case (a, b) => a == b
  }

  def testSparkResultsAreEqual(testName: String, df: SparkSession => DataFrame)(fun: DataFrame => DataFrame): Unit = {
    test(testName) {
      val fromCpu = withCpuSparkSession((session) => {
        fun(df(session)).collect()
      })

      val fromGpu = withGpuSparkSession((session) => {
        fun(df(session)).collect()
      })

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

  def simpleLongDf(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    Seq(
      (100L),
      (200L),
      (300L),
      (400L),
      (500L),
      (-100L),
      (-500L)
    ).toDF("longs")
  }

  testSparkResultsAreEqual("Test scalar addition", simpleLongDf) {
    frame => frame.select(col("longs") + 100)
  }

  testSparkResultsAreEqual("Test addition", simpleLongDf) {
    frame => frame.select(col("longs") + col("longs"))
  }
}
