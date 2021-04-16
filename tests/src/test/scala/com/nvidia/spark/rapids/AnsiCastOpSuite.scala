/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import java.sql.Timestamp
import java.time.DateTimeException

import scala.util.Random

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Alias, AnsiCast, CastBase}
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class AnsiCastOpSuite extends GpuExpressionTestSuite {

  import CastOpSuite._

  private val sparkConf = new SparkConf()
    .set("spark.sql.ansi.enabled", "true")
    .set("spark.sql.storeAssignmentPolicy", "ANSI") // note this is the default in 3.0.0
    .set(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES.key, "true")
    .set(RapidsConf.ENABLE_CAST_FLOAT_TO_STRING.key, "true")
    .set(RapidsConf.ENABLE_CAST_STRING_TO_FLOAT.key, "true")
    .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "true")

  def generateOutOfRangeTimestampsDF(
      lowerValue: Long,
      upperValue: Long,
      outOfRangeValue: Long)(
      session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    // while creating timestamps we multiply the value by 1000 because spark divides it by 1000
    // before casting it to integral types
    generateValidValuesTimestampsDF(lowerValue, upperValue)(session)
      .union(Seq[Timestamp](new Timestamp(outOfRangeValue * 1000)).toDF("c0"))
  }

  def generateValidValuesTimestampsDF(lowerValid: Long, upperValid: Long)(
      session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    //static seed
    val r = new Random(4135277987418063300L)

    val seq = for (_ <- 1 to 100) yield r.nextInt(2) match {
      case 0 => new Timestamp((upperValid * r.nextDouble()).toLong)
      case 1 => new Timestamp((lowerValid * r.nextDouble()).toLong)
    }
    seq.toDF("c0")
  }

  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast from timestamp to integral types
  ///////////////////////////////////////////////////////////////////////////

  def before3_1_0(s: SparkSession): (Boolean, String) = {
    (s.version < "3.1.0", s"Spark version must be prior to 3.1.0")
  }

  testSparkResultsAreEqual("ansi_cast timestamps to long",
    generateValidValuesTimestampsDF(Short.MinValue, Short.MaxValue), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.LongType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast successful timestamps to shorts",
    generateValidValuesTimestampsDF(Short.MinValue, Short.MaxValue), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast successful timestamps to ints",
    generateValidValuesTimestampsDF(Int.MinValue, Int.MaxValue), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast successful timestamps to bytes",
    generateValidValuesTimestampsDF(Byte.MinValue, Byte.MaxValue), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast overflow timestamps to bytes",
    generateOutOfRangeTimestampsDF(Byte.MinValue, Byte.MaxValue, Byte.MaxValue + 1), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast underflow timestamps to bytes",
    generateOutOfRangeTimestampsDF(Byte.MinValue, Byte.MaxValue, Byte.MinValue - 1), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast overflow timestamps to shorts",
    generateOutOfRangeTimestampsDF(Short.MinValue, Short.MaxValue, Short.MaxValue + 1), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast underflow timestamps to shorts",
    generateOutOfRangeTimestampsDF(Short.MinValue, Short.MaxValue, Short.MinValue - 1), sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast overflow timestamps to int",
    generateOutOfRangeTimestampsDF(Int.MinValue, Int.MaxValue, Int.MaxValue.toLong + 1),
    sparkConf, assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast underflow timestamps to int",
    generateOutOfRangeTimestampsDF(Int.MinValue, Int.MaxValue, Int.MinValue.toLong - 1),
    sparkConf, assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast from date
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast date to bool", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to byte", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to short", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to int", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to long", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.LongType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to float", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.FloatType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to double", testDates, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.DoubleType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast date to timestamp", testDates, sparkConf) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  //////////////////////////////////////////////////////////////////////////
  // Ansi cast from boolean
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast bool to string", testBools, sparkConf) {
    frame => testCastTo(DataTypes.StringType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to byte", testBools, sparkConf) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to short", testBools, sparkConf) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to int", testBools, sparkConf) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to long", testBools, sparkConf) {
    frame => testCastTo(DataTypes.LongType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to float", testBools, sparkConf) {
    frame => testCastTo(DataTypes.FloatType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to double", testBools, sparkConf) {
    frame => testCastTo(DataTypes.DoubleType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast bool to timestamp", testBools, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast to boolean
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast byte to bool", testBytes, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast short to bool", testShorts, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast int to bool", testInts, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast long to bool", testLongs, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast float to bool", testFloats, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast double to bool", testDoubles, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast timestamp to bool", testTimestamps, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to bool", validBoolStrings, sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }

  private def validBoolStrings(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val boolStrings: Seq[String] = Seq("t", "true", "y", "yes", "1") ++
      Seq("f", "false", "n", "no", "0")
    boolStrings.toDF("c0")
  }

  testCastFailsForBadInputs("ansi_cast string to bool (invalid values)", testStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.BooleanType)(frame)
  }


  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast from string to integral types
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast string to byte (valid values)", bytesAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to short (valid values)", shortsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to int (valid values)", intsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to long (valid values)", longsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.LongType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast string to byte (invalid values)", shortsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.ByteType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast string to short (invalid values)", intsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.ShortType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast string to long (invalid decimal values)",
    longsAsDecimalStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.LongType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast string to int (invalid values)", longsAsStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  testCastFailsForBadInputs("ansi_cast string to int (non-numeric values)", testStrings,
    sparkConf) {
    frame => testCastTo(DataTypes.IntegerType)(frame)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast from string to floating point
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast string to double exp", exponentsAsStringsDf, sparkConf,
      maxFloatDiff = 0.0001) {
    frame => testCastTo(DataTypes.DoubleType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to float exp", exponentsAsStringsDf, sparkConf,
      maxFloatDiff = 0.0001) {
    frame => testCastTo(DataTypes.FloatType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to float", floatsAsStrings, sparkConf,
      maxFloatDiff = 0.0001) {
    frame => testCastTo(DataTypes.FloatType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast string to double", doublesAsStrings, sparkConf,
      maxFloatDiff = 0.0001) {
    frame => testCastTo(DataTypes.DoubleType)(frame)
  }

  testCastFailsForBadInputs("Test bad cast 1 from strings to floats", invalidFloatStringsDf,
      msg = GpuCast.INVALID_FLOAT_CAST_MSG) {
    frame =>frame.select(col("c0").cast(FloatType))
  }

  testCastFailsForBadInputs("Test bad cast 2 from strings to floats", invalidFloatStringsDf,
      msg = GpuCast.INVALID_FLOAT_CAST_MSG) {
    frame =>frame.select(col("c1").cast(FloatType))
  }

  testCastFailsForBadInputs("Test bad cast 1 from strings to double", invalidFloatStringsDf,
      msg = GpuCast.INVALID_FLOAT_CAST_MSG) {
    frame =>frame.select(col("c0").cast(DoubleType))
  }

  testCastFailsForBadInputs("Test bad cast 2 from strings to double", invalidFloatStringsDf,
      msg = GpuCast.INVALID_FLOAT_CAST_MSG) {
    frame =>frame.select(col("c1").cast(DoubleType))
  }

  // Currently there is a bug in cudf which doesn't convert some corner cases correctly
  // The bug is documented here https://github.com/rapidsai/cudf/issues/5225
  ignore("Test cast from strings to double that doesn't match") {
    testSparkResultsAreEqual("Test cast from strings to double that doesn't match",
        badDoubleStringsDf, conf = sparkConf, maxFloatDiff = 0.0001) {
      frame =>frame.select(
        col("c0").cast(DoubleType))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Ansi cast from floating point to string
  ///////////////////////////////////////////////////////////////////////////

  test("ansi_cast float to string") {
    testCastToString[Float](DataTypes.FloatType, ansiMode = true,
      comparisonFunc = Some(compareStringifiedFloats))
  }

  test("ansi_cast double to string") {
    testCastToString[Double](DataTypes.DoubleType, ansiMode = true,
      comparisonFunc = Some(compareStringifiedFloats))
  }

  test("ansi_cast decimal to string") {
    val sqlCtx = SparkSession.getActiveSession.get.sqlContext
    sqlCtx.setConf("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    sqlCtx.setConf("spark.rapids.sql.castDecimalToString.enabled", "true")

    Seq(10, 15, 18).foreach { precision =>
      Seq(-precision, -5, 0, 5, precision).foreach { scale =>
        testCastToString(DataTypes.createDecimalType(precision, scale),
          ansiMode = true,
          comparisonFunc = Some(compareStringifiedDecimalsInSemantic))
      }
    }
  }

  private def castToStringExpectedFun[T]: T => Option[String] = (d: T) => Some(String.valueOf(d))

  private def testCastToString[T](dataType: DataType, ansiMode: Boolean,
      comparisonFunc: Option[(String, String) => Boolean] = None) {
    val checks = GpuOverrides.expressions(classOf[AnsiCast]).getChecks.get.asInstanceOf[CastChecks]
    assert(checks.gpuCanCast(dataType, DataTypes.StringType))
    val schema = FuzzerUtils.createSchema(Seq(dataType))
    val childExpr: GpuBoundReference = GpuBoundReference(0, dataType, nullable = false)
    checkEvaluateGpuUnaryExpression(GpuCast(childExpr, DataTypes.StringType, ansiMode = true),
      dataType,
      DataTypes.StringType,
      expectedFun = castToStringExpectedFun[T],
      schema = schema,
      comparisonFunc = comparisonFunc)
  }

 ///////////////////////////////////////////////////////////////////////////
  // Ansi cast integral types to timestamp
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("ansi_cast bytes to timestamp", testBytes, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast shorts to timestamp", testShorts, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast ints to timestamp", testInts, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  testSparkResultsAreEqual("ansi_cast longs to timestamp", testLongs, sparkConf,
    assumeCondition = before3_1_0) {
    frame => testCastTo(DataTypes.TimestampType)(frame)
  }

  test("ANSI mode: cast string to timestamp with parse error") {
    // Copied from Spark CastSuite

    def checkCastWithParseError(str: String): Unit = {
      val exception = intercept[SparkException] {
        withGpuSparkSession(spark => {
          import spark.implicits._

          val df = Seq(str).toDF("c0")
              .repartition(2)
              .withColumn("c1", col("c0").cast(DataTypes.TimestampType))

          val result = df.collect()
          result.foreach(println)

        }, sparkConf)
      }
      assert(exception.getCause.isInstanceOf[DateTimeException])
    }

    checkCastWithParseError("123")
    checkCastWithParseError("2015-03-18 123142")
    checkCastWithParseError("2015-03-18T123123")
    checkCastWithParseError("2015-03-18X")
    checkCastWithParseError("2015/03/18")
    checkCastWithParseError("2015.03.18")
    checkCastWithParseError("20150318")
    checkCastWithParseError("2015-031-8")
    checkCastWithParseError("2015-03-18T12:03:17-0:70")
  }

  ///////////////////////////////////////////////////////////////////////////
  // Writing to Hive tables, which has special rules
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("Write bytes to string", testBytes, sparkConf) {
    frame => doTableInsert(frame, HIVE_STRING_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write shorts to string", testShorts, sparkConf) {
    frame => doTableInsert(frame, HIVE_STRING_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to string", testInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_STRING_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to string", testLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_STRING_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to long", testInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to int (values within range)", intsAsLongs,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to short (values within range)", shortsAsLongs,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write longs to byte (values within range)", bytesAsLongs,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to short (values within range)", shortsAsInts,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write ints to byte (values within range)", bytesAsInts,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write shorts to byte (values within range)", bytesAsShorts,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to long (values within range)", longsAsFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to int (values within range)", intsAsFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to short (values within range)", shortsAsFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write floats to byte (values within range)", bytesAsFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to long (values within range)", longsAsDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to int (values within range)", intsAsDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to short (values within range)",
    shortsAsDoubles, sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Write doubles to byte (values within range)", bytesAsDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Test for exceptions when casting out of range values
  ///////////////////////////////////////////////////////////////////////////

  testCastFailsForBadInputs("Detect overflow from long to int", testLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from long to short", testLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from long to byte", testLongs, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from int to short", testInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from int to byte", testInts, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from short to byte", testShorts, sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to long", testFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to int", testFloats, sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to short", testFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from float to byte", testFloats,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to long", testDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_LONG_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to int", testDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_INT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to short", testDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_SHORT_SQL_TYPE)
  }

  testCastFailsForBadInputs("Detect overflow from double to byte", testDoubles,
    sparkConf) {
    frame => doTableInsert(frame, HIVE_BYTE_SQL_TYPE)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Copying between Hive tables, which has special rules
  ///////////////////////////////////////////////////////////////////////////

  testSparkResultsAreEqual("Copy ints to long", testInts, sparkConf) {
    frame => doTableCopy(frame, HIVE_INT_SQL_TYPE, HIVE_LONG_SQL_TYPE)
  }

  testSparkResultsAreEqual("Copy long to float", testLongs, sparkConf) {
    frame => doTableCopy(frame, HIVE_LONG_SQL_TYPE, HIVE_FLOAT_SQL_TYPE)
  }

  private def testCastTo(castTo: DataType)(frame: DataFrame): DataFrame ={
    frame.withColumn("c1", col("c0").cast(castTo))
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
    spark.sql(s"CREATE TABLE $t2 (a $sqlDataType) USING parquet")
    assertContainsAnsiCast(spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1"))
    spark.sql(s"SELECT a FROM $t2")
  }

  /**
   * Copy data between two tables, causing ansi_cast() expressions to be inserted into the plan.
   */
  private def doTableCopy(frame: DataFrame,
    sqlSourceType: String,
    sqlDestType: String): DataFrame = {

    val spark = frame.sparkSession
    val now = System.currentTimeMillis()
    val t1 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t1_$now"
    val t2 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t2_$now"
    val t3 = s"AnsiCastOpSuite_doTableCopy_${sqlSourceType}_${sqlDestType}_t3_$now"
    frame.createOrReplaceTempView(t1)
    spark.sql(s"CREATE TABLE $t2 (c0 $sqlSourceType) USING parquet")
    spark.sql(s"CREATE TABLE $t3 (c0 $sqlDestType) USING parquet")
    // insert into t2
    assertContainsAnsiCast(spark.sql(s"INSERT INTO $t2 SELECT c0 AS a FROM $t1"))
    // copy from t2 to t1, with an ansi_cast()
    spark.sql(s"INSERT INTO $t3 SELECT c0 FROM $t2")
    spark.sql(s"SELECT c0 FROM $t3")
  }

  /**
   * Perform a transformation that is expected to fail due to values not being valid for
   * an ansi_cast
   */
  private def testCastFailsForBadInputs(
      testName: String,
      frame: SparkSession => DataFrame,
      sparkConf: SparkConf = sparkConf,
      msg: String = GpuCast.INVALID_INPUT_MESSAGE,
      assumeCondition: SparkSession => (Boolean, String) = null)
      (transformation: DataFrame => DataFrame)
    : Unit = {

    test(testName) {
      if (assumeCondition != null) {
        val (isAllowed, reason) = withCpuSparkSession(assumeCondition, conf = sparkConf)
        assume(isAllowed, reason)
      }
      try {
        withGpuSparkSession(spark => {
          val input = frame(spark).repartition(1)
          transformation(input).collect()
        }, sparkConf)
        fail("should have thrown an exception due to input values that are not safe " +
          "for an ansi_cast")
      } catch {
        case e: Exception =>
          assert(exceptionContains(e, msg))
      }
    }
  }


  private def assertContainsAnsiCast(df: DataFrame, expected: Int = 1): DataFrame = {
    var count = 0
    df.queryExecution.sparkPlan.foreach {
      case p: ProjectExec => count += p.projectList.count {
        // ansiEnabled is protected so we rely on CastBase.toString
        case c: CastBase => c.toString().startsWith("ansi_cast")
        case Alias(c: CastBase, _) => c.toString().startsWith("ansi_cast")
        case _ => false
      }
      case p: GpuProjectExec => count += p.projectList.count {
        case c: GpuCast => c.ansiMode
        case _ => false
      }
      case _ =>
    }
    if (count != expected) {
      throw new IllegalStateException("Plan does not contain the expected number of " +
        "ansi_cast expressions")
    }
    df
  }

  private def testBools = testData(DataTypes.BooleanType)(_)
  private def testBytes = testData(DataTypes.ByteType)(_)
  private def testShorts = testData(DataTypes.ShortType)(_)
  private def testInts = testData(DataTypes.IntegerType)(_)
  private def testLongs = testData(DataTypes.LongType)(_)
  private def testFloats = testData(DataTypes.FloatType)(_)
  private def testDoubles = testData(DataTypes.DoubleType)(_)
  private def testStrings = testData(DataTypes.StringType)(_)
  private def testTimestamps = testData(DataTypes.TimestampType)(_)
  private def testDates = testData(DataTypes.DateType)(_)

  private val HIVE_LONG_SQL_TYPE = "BIGINT"
  private val HIVE_INT_SQL_TYPE = "INT"
  private val HIVE_SHORT_SQL_TYPE = "SMALLINT"
  private val HIVE_BYTE_SQL_TYPE = "TINYINT"
  private val HIVE_FLOAT_SQL_TYPE = "FLOAT"
  private val HIVE_STRING_SQL_TYPE = "STRING"

  private def testData(dt: DataType)(spark: SparkSession) = {
    val schema = FuzzerUtils.createSchema(Seq(dt))
    FuzzerUtils.generateDataFrame(spark, schema, 100)
  }

}
