/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, Cast, CastBase}
import org.apache.spark.sql.execution.{ExplainMode, ProjectExec, SimpleMode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.annotation.tailrec

class AnsiCastOpSuite extends GpuExpressionTestSuite {

  private val sparkConf = new SparkConf()
    .set("spark.sql.ansi.enabled", "true")
    .set("spark.sql.storeAssignmentPolicy", "ANSI") // note this is the default in 3.0.0

  testSparkResultsAreEqual("ansi_cast timestamps to long", testData(DataTypes.TimestampType), sparkConf) {
    frame => assertContainsAnsiCast(frame.withColumn("c1", col("c0").cast(DataTypes.LongType)))
  }

  testSparkResultsAreEqual("Write ints to long", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to int (values within range)", intsAsLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to short (values within range)", shortsAsLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to byte (values within range)", bytesAsLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to short (values within range)", shortsAsInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to byte (values within range)", bytesAsInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write shorts to byte (values within range)", bytesAsShorts, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to long (values within range)", longsAsFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to int (values within range)", intsAsFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to short (values within range)", shortsAsFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to byte (values within range)", bytesAsFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to long (values within range)", longsAsDoubles, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to int (values within range)", intsAsDoubles, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to short (values within range)", shortsAsDoubles, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to byte (values within range)", bytesAsDoubles, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from long to int", testData(DataTypes.LongType), sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from long to short", testData(DataTypes.LongType), sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from long to byte", testData(DataTypes.LongType), sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from int to short", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from int to byte", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from short to byte", testData(DataTypes.ShortType), sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to long", testData(DataTypes.FloatType), sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to int", testData(DataTypes.FloatType), sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to short", testData(DataTypes.FloatType), sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to byte", testData(DataTypes.FloatType), sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to long", testData(DataTypes.DoubleType), sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to int", testData(DataTypes.DoubleType), sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to short", testData(DataTypes.DoubleType), sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to byte", testData(DataTypes.DoubleType), sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Copy ints to long", testData(DataTypes.IntegerType), sparkConf) {
    frame => doTableCopy(frame, HIVE_INT_SQL_TYPE, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Copy long to float", testData(DataTypes.LongType), sparkConf) {
    frame => doTableCopy(frame, HIVE_LONG_SQL_TYPE, HIVE_FLOAT_SQL_TYPE)
  }

  /**
   * Insert data into a table, causing a cast to happen.
   */
  private def doTableInsert(frame: DataFrame, sqlDataType: String): DataFrame = {
    val spark = frame.sparkSession
    val now = System.currentTimeMillis()
    val t1 = s"AnsiCastOpSuite_doTableInsert_${sqlDataType}_t1_$now"
    val t2 = s"AnsiCastOpSuite_doTableInsert_${sqlDataType}_t2_$now"
    frame.createOrReplaceTempView(t1)
    spark.sql(s"CREATE TABLE $t2 (a $sqlDataType)")
    assertContainsAnsiCast(spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1"))
    spark.sql(s"SELECT a FROM $t2")
  }

  /**
   * Copy data between two tables, causing ansi_cast() expressions to be inserted into the plan.
   */
  private def doTableCopy(frame: DataFrame, sqlSourceType: String, sqlDestType: String): DataFrame = {
    val spark = frame.sparkSession
    val now = System.currentTimeMillis()
    val t1 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t1_$now"
    val t2 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t2_$now"
    val t3 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t3_$now"
    frame.createOrReplaceTempView(t1)
    spark.sql(s"CREATE TABLE $t2 (c0 $sqlSourceType)")
    spark.sql(s"CREATE TABLE $t3 (c0 $sqlDestType)")
    // insert into t2
    assertContainsAnsiCast(spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1"))
    // copy from t2 to t1, with an ansi_cast()
    spark.sql(s"INSERT INTO $t3 SELECT c0 FROM $t2")
    spark.sql(s"SELECT c0 FROM $t3")
  }

  /** Test that a transformation is not supported on GPU */
  private def testNotSupportedOnGpu(testName: String, frame: SparkSession => DataFrame, sparkConf: SparkConf)(transformation: DataFrame => DataFrame): Unit = {
    test(testName) {
      try {
        withGpuSparkSession(spark => {
          val input = frame(spark).repartition(1)
          transformation(input).collect()
        }, sparkConf)
        fail("should not run on GPU")
      } catch {
        case e: IllegalArgumentException =>
          assert(e.getMessage.startsWith("Part of the plan is not columnar"))
      }
    }
  }

  /** Perform a transformation that is expected to fail due to values not being valid for an ansi_cast */
  private def testCastFailsForBadInputs(testName: String, frame: SparkSession => DataFrame, sparkConf: SparkConf)(transformation: DataFrame => DataFrame): Unit = {
    test(testName) {
      try {
        withGpuSparkSession(spark => {
          val input = frame(spark).repartition(1)
          transformation(input).collect()
        }, sparkConf)
        fail("should have thrown an exception due to input values that are not safe for an ansi_cast")
      } catch {
        case e: Exception =>
          assert(exceptionContains(e, "Column contains at least one value that is not in the required range"))
      }
    }
  }

  @tailrec
  private def exceptionContains(e: Throwable, message: String): Boolean = {
    if (e.getMessage.contains(message)) {
      true
    } else if (e.getCause != null) {
      exceptionContains(e.getCause, message)
    } else {
      false
    }
  }

  private def assertContainsAnsiCast(df: DataFrame, expected: Int = 1): DataFrame = {
    var count = 0
    df.queryExecution.sparkPlan.foreach {
      case p: ProjectExec => count += p.projectList.count {
        case c: CastBase => c.toString().startsWith("ansi_cast") // ansiEnabled is protected
        case Alias(c: CastBase, _) => c.toString().startsWith("ansi_cast") // ansiEnabled is protected
        case _ => false
      }
      case p: GpuProjectExec => count += p.projectList.count {
        case c: GpuCast => c.ansiMode
        case _ => false
      }
      case _ =>
    }
    if (count != expected) {
      throw new IllegalStateException("Plan does not contain the expected number of ansi_cast expressions")
    }
    df
  }

  def bytesAsShorts(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(_.toShort).toDF("c0")
  }

  def bytesAsInts(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(_.toInt).toDF("c0")
  }

  def bytesAsLongs(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(_.toLong).toDF("c0")
  }

  def bytesAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(_.toFloat).toDF("c0")
  }

  def bytesAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(_.toDouble).toDF("c0")
  }

  def shortsAsInts(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(_.toInt).toDF("c0")
  }

  def shortsAsLongs(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(_.toLong).toDF("c0")
  }

  def shortsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(_.toFloat).toDF("c0")
  }

  def shortsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(_.toDouble).toDF("c0")
  }

  def intsAsLongs(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(_.toLong).toDF("c0")
  }

  def intsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(_.toFloat).toDF("c0")
  }

  def intsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(_.toDouble).toDF("c0")
  }

  def longsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(_.toFloat).toDF("c0")
  }

  def longsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(_.toDouble).toDF("c0")
  }

  private val byteValues: Seq[Byte] = Seq(Byte.MinValue, Byte.MaxValue, 0, -0, -1, 1)
  private val shortValues: Seq[Short] = Seq(Short.MinValue, Short.MaxValue, 0, -0, -1, 1)
  private val intValues: Seq[Int] = Seq(Int.MinValue, Int.MaxValue, 0, -0, -1, 1)
  private val longValues: Seq[Long] = Seq(Long.MinValue, Long.MaxValue, 0, -0, -1, 1)

  private val HIVE_LONG_SQL_TYPE = "BIGINT"
  private val HIVE_INT_SQL_TYPE = "INT"
  private val HIVE_SHORT_SQL_TYPE = "SMALLINT"
  private val HIVE_BYTE_SQL_TYPE = "TINYINT"
  private val HIVE_FLOAT_SQL_TYPE = "FLOAT"
  private val HIVE_DOUBLE_SQL_TYPE = "DOUBLE"

  private def testData(dt: DataType)(spark: SparkSession) = {
    val schema = FuzzerUtils.createSchema(Seq(dt))
    FuzzerUtils.generateDataFrame(spark, schema, 100)
  }

}
