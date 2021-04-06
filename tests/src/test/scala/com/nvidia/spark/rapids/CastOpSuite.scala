/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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
import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.TimeZone

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AnsiCast, Cast}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class CastOpSuite extends GpuExpressionTestSuite {
  import CastOpSuite._

  private val sparkConf = new SparkConf()
    .set(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES.key, "true")
    .set(RapidsConf.ENABLE_CAST_STRING_TO_FLOAT.key, "true")

  private val timestampDatesMsecParquet = frameFromParquet("timestamp-date-test-msec.parquet")

  /** Data types supported by the plugin. */
  protected val supportedTypes = Seq(
    DataTypes.BooleanType,
    DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType,
    DataTypes.FloatType, DataTypes.DoubleType,
    DataTypes.DateType,
    DataTypes.TimestampType,
    DataTypes.StringType,
    DataTypes.NullType
  )

  /** Produces a matrix of all possible casts. */
  protected def typeMatrix: Seq[(DataType, DataType)] = {
    for (from <- supportedTypes; to <- supportedTypes) yield (from, to)
  }

  test("Test all supported casts with in-range values") {
    // test cast() and ansi_cast()
    Seq(false, true).foreach { ansiEnabled =>

      val conf = new SparkConf()
        .set(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES.key, "true")
        .set(RapidsConf.ENABLE_CAST_FLOAT_TO_STRING.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_FLOAT.key, "true")
        .set("spark.sql.ansi.enabled", String.valueOf(ansiEnabled))

      val key = if (ansiEnabled) classOf[AnsiCast] else classOf[Cast]
      val checks = GpuOverrides.expressions(key).getChecks.get.asInstanceOf[CastChecks]

      typeMatrix.foreach {
        case (from, to) =>
          // check if Spark supports this cast
          if (checks.sparkCanCast(from, to)) {
            // check if plugin supports this cast
            if (checks.gpuCanCast(from, to)) {
              // test the cast
              try {
                val (fromCpu, fromGpu) =
                  runOnCpuAndGpu(generateInRangeTestData(from, to, ansiEnabled),
                  frame => frame.select(col("c0").cast(to))
                    .orderBy(col("c0")), conf)

                // perform comparison logic specific to the cast
                (from, to) match {
                  case (DataTypes.FloatType | DataTypes.DoubleType, DataTypes.StringType) =>
                    compareFloatToStringResults(fromCpu, fromGpu)
                  case _ =>
                    compareResults(sort = false, 0.00001, fromCpu, fromGpu)
                }
              } catch {
                case e: Exception =>
                  fail(s"Cast from $from to $to failed; ansi=$ansiEnabled $e", e)
              }
            }
          } else {
            // if Spark doesn't support this cast then the plugin shouldn't either
            assert(!checks.gpuCanCast(from, to))
          }
      }
    }
  }

  private def compareFloatToStringResults(fromCpu: Array[Row], fromGpu: Array[Row]) = {
    fromCpu.zip(fromGpu).foreach {
      case (c, g) =>
        val cpuValue = c.getAs[String](0)
        val gpuValue = g.getAs[String](0)
        if (!compareStringifiedFloats(cpuValue, gpuValue)) {
          fail(s"Running on the GPU and on the CPU did not match: CPU " +
            s"value: $cpuValue. GPU value: $gpuValue.")
        }
    }
  }

  test("Test unsupported cast") {
    // this test tracks currently unsupported casts and will need updating as more casts are
    // supported
    val unsupported = getUnsupportedCasts(false)
    val expected = List.empty
    assert(unsupported == expected)
  }

  test("Test unsupported ansi_cast") {
    // this test tracks currently unsupported ansi_casts and will need updating as more casts are
    // supported
    val unsupported = getUnsupportedCasts(true)
    val expected = List.empty
    assert(unsupported == expected)
  }

  private def getUnsupportedCasts(ansiEnabled: Boolean): Seq[(DataType, DataType)] = {
    val key = if (ansiEnabled) classOf[AnsiCast] else classOf[Cast]
    val checks = GpuOverrides.expressions(key).getChecks.get.asInstanceOf[CastChecks]

    val unsupported = typeMatrix.flatMap {
      case (from, to) =>
        if (checks.sparkCanCast(from, to) && !checks.gpuCanCast(from, to)) {
          Some((from, to))
        } else {
          None
        }
    }
    unsupported
  }

  protected def generateInRangeTestData(from: DataType,
    to: DataType,
    ansiEnabled: Boolean)(spark: SparkSession): DataFrame = {

    // provide test data that won't cause overflows (separate tests exist for overflow cases)
    (from, to) match {

      case (DataTypes.FloatType, DataTypes.TimestampType) => timestampsAsFloats(spark)
      case (DataTypes.DoubleType, DataTypes.TimestampType) => timestampsAsDoubles(spark)

      case (DataTypes.TimestampType, DataTypes.ByteType) => bytesAsTimestamps(spark)
      case (DataTypes.TimestampType, DataTypes.ShortType) => shortsAsTimestamps(spark)
      case (DataTypes.TimestampType, DataTypes.IntegerType) => intsAsTimestamps(spark)
      case (DataTypes.TimestampType, DataTypes.LongType) => longsAsTimestamps(spark)
      case (DataTypes.TimestampType, DataTypes.StringType) => validTimestamps(spark)

      case (DataTypes.StringType, DataTypes.BooleanType) => validBoolStrings(spark)

      case (DataTypes.StringType, DataTypes.ByteType) if ansiEnabled => bytesAsStrings(spark)
      case (DataTypes.StringType, DataTypes.ShortType) if ansiEnabled => shortsAsStrings(spark)
      case (DataTypes.StringType, DataTypes.IntegerType) if ansiEnabled => intsAsStrings(spark)
      case (DataTypes.StringType, DataTypes.LongType)  => if (ansiEnabled) {
        // ansi_cast does not support decimals
        longsAsStrings(spark)
      } else {
        longsAsDecimalStrings(spark)
      }
      case (DataTypes.StringType, DataTypes.FloatType) if ansiEnabled => floatsAsStrings(spark)
      case (DataTypes.StringType, DataTypes.DoubleType) if ansiEnabled => doublesAsStrings(spark)

      case (DataTypes.StringType, DataTypes.DateType) =>
        timestampsAsStrings(spark, false, ansiEnabled)
      case (DataTypes.StringType, DataTypes.TimestampType) =>
        timestampsAsStrings(spark, true, ansiEnabled)

      case (DataTypes.ShortType, DataTypes.ByteType) if ansiEnabled => bytesAsShorts(spark)
      case (DataTypes.IntegerType, DataTypes.ByteType) if ansiEnabled => bytesAsInts(spark)
      case (DataTypes.LongType, DataTypes.ByteType) if ansiEnabled => bytesAsLongs(spark)
      case (DataTypes.FloatType, DataTypes.ByteType) if ansiEnabled => bytesAsFloats(spark)
      case (DataTypes.DoubleType, DataTypes.ByteType) if ansiEnabled => bytesAsDoubles(spark)

      case (DataTypes.IntegerType, DataTypes.ShortType) if ansiEnabled => shortsAsInts(spark)
      case (DataTypes.LongType, DataTypes.ShortType) if ansiEnabled => shortsAsLongs(spark)
      case (DataTypes.FloatType, DataTypes.ShortType) if ansiEnabled => shortsAsFloats(spark)
      case (DataTypes.DoubleType, DataTypes.ShortType) if ansiEnabled => shortsAsDoubles(spark)

      case (DataTypes.LongType, DataTypes.IntegerType) if ansiEnabled => intsAsLongs(spark)
      case (DataTypes.FloatType, DataTypes.IntegerType) if ansiEnabled => intsAsFloats(spark)
      case (DataTypes.DoubleType, DataTypes.IntegerType) if ansiEnabled => intsAsDoubles(spark)

      case (DataTypes.FloatType, DataTypes.LongType) if ansiEnabled => longsAsFloats(spark)
      case (DataTypes.DoubleType, DataTypes.LongType) if ansiEnabled => longsAsDoubles(spark)

      case _ => FuzzerUtils.createDataFrame(from)(spark)
    }
  }

  private def castToStringExpectedFun[T]: T => Option[String] = (d: T) => Some(String.valueOf(d))

  test("cast byte to string") {
    testCastToString[Byte](DataTypes.ByteType)
  }

  test("cast short to string") {
    testCastToString[Short](DataTypes.ShortType)
  }

  test("cast int to string") {
    testCastToString[Int](DataTypes.IntegerType)
  }

  test("cast long to string") {
    testCastToString[Long](DataTypes.LongType)
  }

  test("cast float to string") {
    testCastToString[Float](DataTypes.FloatType, comparisonFunc = Some(compareStringifiedFloats))
  }

  test("cast double to string") {
    testCastToString[Double](DataTypes.DoubleType, comparisonFunc = Some(compareStringifiedFloats))
  }

  test("cast decimal to string") {
    val sqlCtx = SparkSession.getActiveSession.get.sqlContext
    sqlCtx.setConf("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
    sqlCtx.setConf("spark.rapids.sql.castDecimalToString.enabled", "true")

    Seq(10, 15, 18).foreach { precision =>
      Seq(-precision, -5, 0, 5, precision).foreach { scale =>
        testCastToString(DataTypes.createDecimalType(precision, scale),
          comparisonFunc = Some(compareStringifiedDecimalsInSemantic))
      }
    }
  }

  private def testCastToString[T](
      dataType: DataType,
      comparisonFunc: Option[(String, String) => Boolean] = None) {
    val checks = GpuOverrides.expressions(classOf[Cast]).getChecks.get.asInstanceOf[CastChecks]

    assert(checks.gpuCanCast(dataType, DataTypes.StringType))
    val schema = FuzzerUtils.createSchema(Seq(dataType))
    val childExpr: GpuBoundReference = GpuBoundReference(0, dataType, nullable = false)
    checkEvaluateGpuUnaryExpression(GpuCast(childExpr, DataTypes.StringType),
      dataType,
      DataTypes.StringType,
      expectedFun = castToStringExpectedFun[T],
      schema = schema,
      comparisonFunc = comparisonFunc)
  }

  testSparkResultsAreEqual("Test cast from long", longsDf) {
    frame => frame.select(
      col("longs").cast(IntegerType),
      col("longs").cast(LongType),
      col("longs").cast(StringType),
      col("more_longs").cast(BooleanType),
      col("more_longs").cast(ByteType),
      // Test requires ProjectExec support BinaryType, tested within md5 hash functionality instead
      // col("longs").cast(BinaryType),
      col("longs").cast(ShortType),
      col("longs").cast(FloatType),
      col("longs").cast(DoubleType),
      col("longs").cast(TimestampType))
  }

  testSparkResultsAreEqual("Test cast from float", mixedFloatDf,
      conf = sparkConf) {
    frame => frame.select(
      col("floats").cast(IntegerType),
      col("floats").cast(LongType),
      //      col("doubles").cast(StringType),
      col("more_floats").cast(BooleanType),
      col("more_floats").cast(ByteType),
      col("floats").cast(ShortType),
      col("floats").cast(FloatType),
      col("floats").cast(DoubleType),
      col("floats").cast(TimestampType))
  }

  testSparkResultsAreEqual("Test cast from double", doubleWithNansDf,
      conf = sparkConf) {
    frame => frame.select(
      col("doubles").cast(IntegerType),
      col("doubles").cast(LongType),
//      col("doubles").cast(StringType),
      col("more_doubles").cast(BooleanType),
      col("more_doubles").cast(ByteType),
      col("doubles").cast(ShortType),
      col("doubles").cast(FloatType),
      col("doubles").cast(DoubleType),
      col("doubles").cast(TimestampType))
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

  testSparkResultsAreEqual("Test cast from date", timestampDatesMsecParquet) {
    frame => frame.select(
      col("date"),
      col("date").cast(BooleanType),
      col("date").cast(ByteType),
      col("date").cast(ShortType),
      col("date").cast(IntegerType),
      col("date").cast(LongType),
      col("date").cast(FloatType),
      col("date").cast(DoubleType),
      col("date").cast(LongType),
      col("date").cast(TimestampType))
   }

  testSparkResultsAreEqual("Test cast from string to bool", maybeBoolStrings) {
    frame => frame.select(col("maybe_bool").cast(BooleanType))
  }

  private def maybeBoolStrings(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val trueStrings = Seq("t", "true", "y", "yes", "1")
    val falseStrings = Seq("f", "false", "n", "no", "0")
    val maybeBool: Seq[String] = trueStrings ++ falseStrings ++ Seq(
      "maybe", " true ", " false ", null, "", "12")
    maybeBool.toDF("maybe_bool")
  }

  private val timestampCastFn = { frame: DataFrame =>
    frame.select(
      col("time"),
      col("time").cast(BooleanType),
      col("time").cast(ByteType),
      col("time").cast(ShortType),
      col("time").cast(IntegerType),
      col("time").cast(LongType),
      col("time").cast(FloatType),
      col("time").cast(DoubleType),
      col("time").cast(LongType),
      col("time").cast(DateType))
  }

  testSparkResultsAreEqual(
    "Test cast from timestamp", timestampDatesMsecParquet)(timestampCastFn)

  test("Test cast from timestamp in UTC-equivalent timezone") {
    val oldtz = TimeZone.getDefault
    try {
      TimeZone.setDefault(TimeZone.getTimeZone("Etc/UTC-0"))
      val (fromCpu, fromGpu) = runOnCpuAndGpu(timestampDatesMsecParquet, timestampCastFn)
      compareResults(sort=false, 0, fromCpu, fromGpu)
    } finally {
      TimeZone.setDefault(oldtz)
    }
  }

  testSparkResultsAreEqual("Test cast to timestamp", mixedDfWithNulls) {
    frame => frame.select(
      col("ints").cast(TimestampType),
      col("longs").cast(TimestampType),
      col("doubles").cast(TimestampType))
  }

  testSparkResultsAreEqual("Test cast from strings to int", doublesAsStrings,
    conf = sparkConf) {
    frame => frame.select(
      col("c0").cast(LongType),
      col("c0").cast(IntegerType),
      col("c0").cast(ShortType),
      col("c0").cast(ByteType))
  }

  testSparkResultsAreEqual("Test cast from strings to doubles", doublesAsStrings,
    conf = sparkConf, maxFloatDiff = 0.0001) {
    frame => frame.select(
      col("c0").cast(DoubleType))
  }

  testSparkResultsAreEqual("Test cast from strings to floats", floatsAsStrings,
    conf = sparkConf, maxFloatDiff = 0.0001) {
    frame => frame.select(
      col("c0").cast(FloatType))
  }

  testSparkResultsAreEqual("Test bad cast from strings to floats", invalidFloatStringsDf,
    conf = sparkConf, maxFloatDiff = 0.0001) {
    frame =>frame.select(
      col("c0").cast(DoubleType),
      col("c0").cast(FloatType),
      col("c1").cast(DoubleType),
      col("c1").cast(FloatType))
  }

  // Currently there is a bug in cudf which doesn't convert some corner cases correctly
  // The bug is documented here https://github.com/rapidsai/cudf/issues/5225
  ignore("Test cast from strings to double that doesn't match") {
    testSparkResultsAreEqual("Test cast from strings to double that doesn't match",
        badDoubleStringsDf, conf = sparkConf, maxFloatDiff = 0.0001) {
      frame => frame.select(
        col("c0").cast(DoubleType))
    }
  }

  testSparkResultsAreEqual("ansi_cast string to double exp", exponentsAsStringsDf,
    conf = sparkConf, maxFloatDiff = 0.0001) {
    frame => frame.select(
      col("c0").cast(DoubleType))
  }

  testSparkResultsAreEqual("ansi_cast string to float exp", exponentsAsStringsDf,
    conf = sparkConf, maxFloatDiff = 0.0001) {
    frame => frame.select(
      col("c0").cast(FloatType))
  }

  // Test requires ProjectExec support BinaryType, tested within md5 hash functionality instead
  // testSparkResultsAreEqual("Test cast from strings to binary", floatsAsStrings) {
  //   frame => frame.select(
  //     col("c0").cast(BinaryType))
  // }

  test("cast short to decimal") {
    List(-4, -2, 0,  1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.ShortType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast int to decimal") {
    List(-9, -5, -2, 0, 1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.IntegerType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast long to decimal") {
    List(-18, -10, -3, 0, 1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.LongType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast float to decimal") {
    List(-18, -10, -3, 0, 1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.FloatType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast float to decimal (include NaN/INF/-INF)") {
    def floatsIncludeNaNs(ss: SparkSession): DataFrame = {
      mixedFloatDf(ss).select(col("floats").as("col"))
    }
    List(-10, -1, 0, 1, 10).foreach { scale =>
      testCastToDecimal(DataTypes.FloatType, scale,
        customDataGenerator = Some(floatsIncludeNaNs))
    }
  }

  test("cast double to decimal") {
    List(-18, -10, -3, 0, 1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.DoubleType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast double to decimal (include NaN/INF/-INF)") {
    def doublesIncludeNaNs(ss: SparkSession): DataFrame = {
      mixedDoubleDf(ss).select(col("doubles").as("col"))
    }
    List(-10, -1, 0, 1, 10).foreach { scale =>
      testCastToDecimal(DataTypes.DoubleType, scale,
        customDataGenerator = Some(doublesIncludeNaNs))
    }
  }

  test("cast decimal to decimal") {
    // fromScale == toScale
    testCastToDecimal(DataTypes.createDecimalType(18, 0),
      scale = 0,
      customRandGenerator = Some(new scala.util.Random(1234L)))
    testCastToDecimal(DataTypes.createDecimalType(18, 2),
      scale = 2,
      ansiEnabled = true,
      customRandGenerator = Some(new scala.util.Random(1234L)))

    // fromScale > toScale
    testCastToDecimal(DataTypes.createDecimalType(18, 1),
      scale = -1,
      customRandGenerator = Some(new scala.util.Random(1234L)))
    testCastToDecimal(DataTypes.createDecimalType(18, 10),
      scale = 2,
      ansiEnabled = true,
      customRandGenerator = Some(new scala.util.Random(1234L)))
    testCastToDecimal(DataTypes.createDecimalType(18, 18),
      scale = 15,
      customRandGenerator = Some(new scala.util.Random(1234L)))

    // fromScale < toScale
    testCastToDecimal(DataTypes.createDecimalType(18, 0),
      scale = 3,
      customRandGenerator = Some(new scala.util.Random(1234L)))
    testCastToDecimal(DataTypes.createDecimalType(18, 5),
      scale = 10,
      customRandGenerator = Some(new scala.util.Random(1234L)))
    testCastToDecimal(DataTypes.createDecimalType(18, 10),
      scale = 17,
      customRandGenerator = Some(new scala.util.Random(1234L)))
  }

  test("Detect overflow from numeric types to decimal") {
    def intGenerator(column: Seq[Int])(ss: SparkSession): DataFrame = {
      import ss.sqlContext.implicits._
      column.toDF("col")
    }
    def longGenerator(column: Seq[Long])(ss: SparkSession): DataFrame = {
      import ss.sqlContext.implicits._
      column.toDF("col")
    }
    def floatGenerator(column: Seq[Float])(ss: SparkSession): DataFrame = {
      import ss.sqlContext.implicits._
      column.toDF("col")
    }
    def doubleGenerator(column: Seq[Double])(ss: SparkSession): DataFrame = {
      import ss.sqlContext.implicits._
      column.toDF("col")
    }
    def decimalGenerator(column: Seq[Decimal], decType: DecimalType
    )(ss: SparkSession): DataFrame = {
      val field = StructField("col", decType)
      ss.createDataFrame(column.map(Row(_)).asJava, StructType(Seq(field)))
    }
    def nonOverflowCase(dataType: DataType,
      generator: SparkSession => DataFrame,
      precision: Int,
      scale: Int): Unit = {
      testCastToDecimal(dataType,
        customDataGenerator = Some(generator),
        precision = precision, scale = scale,
        ansiEnabled = true, gpuOnly = true)
    }
    def overflowCase(dataType: DataType,
      generator: SparkSession => DataFrame,
      precision: Int,
      scale: Int): Unit = {
      // Catch out of range exception when AnsiMode is on
      assert(
        exceptionContains(
        intercept[org.apache.spark.SparkException] {
          nonOverflowCase(dataType, generator, precision, scale)
        },
        GpuCast.INVALID_INPUT_MESSAGE)
      )
      // Compare gpu results with cpu ones when AnsiMode is off (most of them should be null)
      testCastToDecimal(dataType,
        customDataGenerator = Some(generator),
        precision = precision, scale = scale)
    }

    // Test 1: overflow caused by half-up rounding
    nonOverflowCase(DataTypes.DoubleType, precision = 5, scale = 2,
      generator = doubleGenerator(Seq(999.994)))
    nonOverflowCase(DataTypes.DoubleType, precision = 4, scale = -2,
      generator = doubleGenerator(Seq(-999940)))

    overflowCase(DataTypes.DoubleType, precision = 5, scale = 2,
      generator = doubleGenerator(Seq(999.995)))
    overflowCase(DataTypes.DoubleType, precision = 5, scale = -3,
      generator = doubleGenerator(Seq(99999500f)))

    // Test 2: overflow caused by out of range integers
    nonOverflowCase(DataTypes.IntegerType, precision = 9, scale = -1,
      generator = intGenerator(Seq(Int.MinValue, Int.MaxValue)))
    nonOverflowCase(DataTypes.LongType, precision = 18, scale = -1,
      generator = longGenerator(Seq(Long.MinValue, Long.MaxValue)))

    overflowCase(DataTypes.IntegerType, precision = 9, scale = 0,
      generator = intGenerator(Seq(Int.MaxValue)))
    overflowCase(DataTypes.IntegerType, precision = 9, scale = 0,
      generator = intGenerator(Seq(Int.MinValue)))
    overflowCase(DataTypes.LongType, precision = 18, scale = 0,
      generator = longGenerator(Seq(Long.MaxValue)))
    overflowCase(DataTypes.LongType, precision = 18, scale = 0,
      generator = longGenerator(Seq(Long.MinValue)))

    // Test 3: overflow caused by out of range floats
    nonOverflowCase(DataTypes.FloatType, precision = 10, scale = 5,
      generator = floatGenerator(Seq(12345.678f)))
    nonOverflowCase(DataTypes.DoubleType, precision = 18, scale = 5,
      generator = doubleGenerator(Seq(123450000000.123456)))

    overflowCase(DataTypes.FloatType, precision = 10, scale = 6,
      generator = floatGenerator(Seq(12345.678f)))
    overflowCase(DataTypes.DoubleType, precision = 15, scale = -5,
      generator = doubleGenerator(Seq(1.23e21)))

    // Test 4: overflow caused by decimal rescaling
    val decType = DataTypes.createDecimalType(18, 0)
    nonOverflowCase(decType,
      precision = 18, scale = 10,
      generator = decimalGenerator(Seq(Decimal(99999999L)), decType))
    overflowCase(decType,
      precision = 18, scale = 10,
      generator = decimalGenerator(Seq(Decimal(100000000L)), decType))
  }

  test("cast string to decimal") {
    List(-18, -10, -3, 0, 1, 5, 15).foreach { scale =>
      testCastToDecimal(DataTypes.StringType, scale,
        customRandGenerator = Some(new scala.util.Random(1234L)))
    }
  }

  test("cast string to decimal (include NaN/INF/-INF)") {
    def doubleStrings(ss: SparkSession): DataFrame = {
      val df1 = floatsAsStrings(ss).selectExpr("cast(c0 as Double) as col")
      val df2 = doublesAsStrings(ss).select(col("c0").as("col"))
      df1.unionAll(df2)
    }
    List(-10, -1, 0, 1, 10).foreach { scale =>
      testCastToDecimal(DataTypes.StringType, scale = scale,
        customDataGenerator = Some(doubleStrings))
    }
  }

  test("cast string to decimal (truncated cases)") {
    def specialGenerator(column: Seq[String])(ss: SparkSession): DataFrame = {
      import ss.sqlContext.implicits._
      column.toDF("col")
    }
    testCastToDecimal(DataTypes.StringType, scale = 7,
      customDataGenerator = Some(specialGenerator(Seq("9999999999"))))
    testCastToDecimal(DataTypes.StringType, scale = 2,
      customDataGenerator = Some(specialGenerator(Seq("999999999999999"))))
    testCastToDecimal(DataTypes.StringType, scale = 0,
      customDataGenerator = Some(specialGenerator(Seq("99999999999999999"))))
    testCastToDecimal(DataTypes.StringType, scale = -1,
      customDataGenerator = Some(specialGenerator(Seq("99999999999999999"))))
    testCastToDecimal(DataTypes.StringType, scale = -10,
      customDataGenerator = Some(specialGenerator(Seq("99999999999999999"))))
  }

  test("ansi_cast string to decimal exp") {
    def exponentsAsStrings(ss: SparkSession): DataFrame = {
      exponentsAsStringsDf(ss).select(col("c0").as("col"))
    }
    List(-10, -1, 0, 1, 10).foreach { scale =>
      testCastToDecimal(DataTypes.StringType, scale = scale,
        customDataGenerator = Some(exponentsAsStrings),
        ansiEnabled = true)
    }
  }

  protected def testCastToDecimal(
    dataType: DataType,
    scale: Int,
    precision: Int = ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION,
    maxFloatDiff: Double = 1e-9,
    customDataGenerator: Option[SparkSession => DataFrame] = None,
    customRandGenerator: Option[scala.util.Random] = None,
    ansiEnabled: Boolean = false,
    gpuOnly: Boolean = false) {

    val dir = Files.createTempDirectory("spark-rapids-test").toFile
    val path = new File(dir,
      s"GpuAnsiCast-${System.currentTimeMillis()}.parquet").getAbsolutePath

    try {
      val conf = new SparkConf()
        .set(RapidsConf.DECIMAL_TYPE_ENABLED.key, "true")
        .set(RapidsConf.ENABLE_CAST_FLOAT_TO_DECIMAL.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_DECIMAL.key, "true")
        .set("spark.rapids.sql.exec.FileSourceScanExec", "false")
        .set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
        .set("spark.sql.ansi.enabled", ansiEnabled.toString)

      val defaultRandomGenerator: SparkSession => DataFrame = {
        val rnd = customRandGenerator.getOrElse(new scala.util.Random(1234L))
        generateCastToDecimalDataFrame(dataType, precision - scale, rnd, 500)
      }
      val generator = customDataGenerator.getOrElse(defaultRandomGenerator)
      withCpuSparkSession(spark => generator(spark).write.parquet(path), conf)

      val createDF = (ss: SparkSession) => ss.read.parquet(path)
      val decType = DataTypes.createDecimalType(precision, scale)
      val execFun = (df: DataFrame) => {
        df.withColumn("col2", col("col").cast(decType))
      }
      if (!gpuOnly) {
        val (fromCpu, fromGpu) = runOnCpuAndGpu(createDF, execFun, conf, repart = 0)
        val (cpuResult, gpuResult) = dataType match {
          case ShortType | IntegerType | LongType | _: DecimalType =>
            fromCpu.map(r => Row(r.getDecimal(1))) -> fromGpu.map(r => Row(r.getDecimal(1)))
          case FloatType | DoubleType | StringType =>
            // There may be tiny difference between CPU and GPU result when casting from double
            val fetchFromRow = (r: Row) => {
              if (r.isNullAt(1)) Double.NaN
              else r.getDecimal(1).unscaledValue().doubleValue()
            }
            fromCpu.map(r => Row(fetchFromRow(r))) -> fromGpu.map(r => Row(fetchFromRow(r)))
        }
        compareResults(sort = false, maxFloatDiff, cpuResult, gpuResult)
      } else {
        withGpuSparkSession((ss: SparkSession) => execFun(createDF(ss)).collect(), conf)
      }
    } finally {
      dir.delete()
    }
  }

  private def generateCastToDecimalDataFrame(
    dataType: DataType,
    integralSize: Int,
    rndGenerator: scala.util.Random,
    rowCount: Int = 100)(ss: SparkSession): DataFrame = {

    import ss.sqlContext.implicits._
    val enhancedRnd = new EnhancedRandom(rndGenerator, FuzzerOptions()) {
      override def nextLong(): Long = r.nextInt(11) match {
        case 0 => -999999999999999999L
        case 1 => 999999999999999999L
        case 2 => 0
        case x if x % 2 == 0 => (r.nextDouble() * -999999999999999999L).toLong
        case _ => (r.nextDouble() * 999999999999999999L).toLong
      }
    }
    val scaleRnd = new scala.util.Random(enhancedRnd.nextLong())
    val rawColumn: Seq[Any] = (0 until rowCount).map { _ =>
      val scale = 18 - scaleRnd.nextInt(integralSize + 1)
      dataType match {
        case ShortType =>
          enhancedRnd.nextLong() / math.pow(10, scale max 14).toLong
        case IntegerType =>
          enhancedRnd.nextLong() / math.pow(10, scale max 9).toLong
        case LongType =>
          enhancedRnd.nextLong() / math.pow(10, scale max 0).toLong
        case FloatType | DoubleType | StringType =>
          enhancedRnd.nextLong() / math.pow(10, scale + 2)
        case dt: DecimalType =>
          val unscaledValue = (enhancedRnd.nextLong() * math.pow(10, dt.precision - 18)).toLong
          Decimal.createUnsafe(unscaledValue, dt.precision, dt.scale)
        case _ =>
          throw new IllegalArgumentException(s"unsupported dataType: $dataType")
      }
    }
    dataType match {
      case ShortType =>
        rawColumn.map(_.asInstanceOf[Long].toShort).toDF("col")
      case IntegerType =>
        rawColumn.map(_.asInstanceOf[Long].toInt).toDF("col")
      case LongType =>
        rawColumn.map(_.asInstanceOf[Long]).toDF("col")
      case FloatType =>
        rawColumn.map(_.asInstanceOf[Double].toFloat).toDF("col")
      case DoubleType =>
        rawColumn.map(_.asInstanceOf[Double]).toDF("col")
      case StringType =>
        rawColumn.map(_.asInstanceOf[Double].toString).toDF("col")
      case dt: DecimalType =>
        val row = rawColumn.map(e => Row(e.asInstanceOf[Decimal])).asJava
        ss.createDataFrame(row, StructType(Seq(StructField("col", dt))))
    }
  }
}

/** Data shared between CastOpSuite and AnsiCastOpSuite. */
object CastOpSuite {

  def doublesAsStrings(session: SparkSession): DataFrame = {
    val schema = FuzzerUtils.createSchema(Seq(DoubleType), false)
    val df = FuzzerUtils.generateDataFrame(session, schema, 2048)
    df.withColumn("c0", col("c0").cast(StringType))
  }

  def floatsAsStrings(session: SparkSession): DataFrame = {
    val schema = FuzzerUtils.createSchema(Seq(FloatType), false)
    val df = FuzzerUtils.generateDataFrame(session, schema, 2048)
    df.withColumn("c0", col("c0").cast(StringType))
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

  def bytesAsTimestamps(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(value => new Timestamp(value)).toDF("c0")
  }

  def bytesAsStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    byteValues.map(value => String.valueOf(value)).toDF("c0")
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

  def shortsAsTimestamps(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(value => new Timestamp(value)).toDF("c0")
  }

  def shortsAsStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    shortValues.map(value => String.valueOf(value)).toDF("c0")
  }

  def intsAsLongs(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(_.toLong).toDF("c0")
  }

  def intsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    // Spark 3.1.0 changed the range of floats that can be cast to integral types and this
    // required the intsAsFloats to be updated to avoid using Int.MaxValue. The supported
    // range is now `Math.floor(x) <= Int.MaxValue && Math.ceil(x) >= Int.MinValue`
    Seq(Int.MinValue.toFloat, 2147483583.toFloat, 0, -0, -1, 1).toDF("c0")
  }

  def intsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(_.toDouble).toDF("c0")
  }

  def intsAsTimestamps(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(value => new Timestamp(value)).toDF("c0")
  }

  def intsAsStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    intValues.map(value => String.valueOf(value)).toDF("c0")
  }

  def longsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(_.toFloat).toDF("c0")
  }

  def longsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(_.toDouble).toDF("c0")
  }

  def longsAsTimestamps(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    timestampValues.map(value => new Timestamp(value)).toDF("c0")
  }

  def longsAsStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(value => String.valueOf(value)).toDF("c0")
  }

  def longsAsDecimalStrings(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    longValues.map(value => String.valueOf(value) + ".1").toDF("c0")
  }

  def timestampsAsFloats(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    timestampValues.map(_.toFloat).toDF("c0")
  }

  def timestampsAsDoubles(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    timestampValues.map(_.toDouble).toDF("c0")
  }

  def timestampsAsStrings(session: SparkSession,
      castStringToTimestamp: Boolean,
      validOnly: Boolean): DataFrame = {
    import session.sqlContext.implicits._

    val specialDates = Seq(
      "epoch",
      "now",
      "today",
      "yesterday",
      "tomorrow"
    )

    // `yyyy`
    val validYear = Seq("2006")

    // note that single-digit months are not currently supported on GPU for this format
    val validYearMonth = Seq(
      // `yyyy-[m]m`
      "2007-01",
      "2007-2"
    )

    // note that single-digit days are not currently supported on GPU for this format
    val validYearMonthDay = Seq(
      // `yyyy-[m]m-[d]d`
      "2008-1-02",
      "2008-01-03",
      "2008-01-4",
      "2008-1-05",
      "2008-1-6",
      // `yyyy-[m]m-[d]d `
      "2009-1-02 ",
      "2009-01-03 ",
      "2009-01-4 ",
      "2009-1-05 ",
      "2009-1-6 "
    )

    val validTimestamps = Seq(
      "2030-8-1 11:02:03.012345Z",
      "2030-9-11 11:02:03.012345Z",
      "2030-10-1 11:02:03.012345Z",
      "2030-11-11 12:02:03.012345Z",
      "2031-8-1T11:02:03.012345Z",
      "2031-9-11T11:02:03.012345Z",
      "2031-10-1T11:02:03.012345Z",
      "2031-11-11T12:02:03.012345Z"
    )

    // invalid values that should be cast to null on both CPU and GPU
    val invalidValues = if (validOnly) {
      Seq.empty
    } else {
      Seq(
        "200", // year too few digits
        "20000", // year too many digits
        "1999\rGARBAGE",
        "1999-1\rGARBAGE",
        "1999-12\rGARBAGE",
        "1999-12-31\rGARBAGE",
        "1999-10-1 TGARBAGE\nMORE GARBAGE",
        "200-1-1",
        "2000-1-1-1",
        "2000-1-1-1-1",
        "-1-1-1-",
        "2010-01-6\r\nT\r\n12:34:56.000111Z",
        "2010-01-6\nT 12:34:56.000111Z",
        "2010-01-6\nT\n12:34:56.000111Z",
        "2010-01-6 T 12:34:56.000111Z",
        "2010-01-6\tT\t12:34:56.000111Z",
        "2010-01-6T\t12:34:56.000111Z",
        "2010-01-6T 12:34:56.000111Z",
        "2010-01-6T  12:34:56.000111Z",
        "2010-01-6 T 12:34:56.000111Z",
        "2010-01-6  T12:34:56.000111Z",
        "2030-11-11 12:02:03.012345Z TRAILING TEXT",
        "2010-01-6 ",
        "2010-01-6 T",
        "2010-01-6 T\n",
        "2010-01-6 T\n12:34:56.000111Z",
        "2018random_text",
        "2018-11random_text",
        "2018-1random_text",
        "2018-11-08random_text",
        "2018-11-9random_text",
        // date component out of range
        "2020-13-01",
        "2020-12-32",
        "2020-02-30",
        "2030-00-11 12:02:03.012345Z",
        "2030-00-11T12:02:03.012345Z",
        // `yyyy-[m]m-[d]dT*` in Spark 3.1+ these no longer work for AnsiCast, but did before
        "2010-1-01T!@#$%",
        "2010-1-02T,",
        "2010-01-03T*",
        "2010-01-04TT",
        "2010-02-3T*",
        "2010-02-4TT",
        // `yyyy-[m]m-[d]d *` in Spark 3.1+ these no longer work for AnsiCast, but did before
        "2010-1-01 !@#$%",
        "2010-1-02 ,",
        "2010-01-03 *",
        "2010-01-04 T",
        "2010-01-5 T",
        "2010-1-06 T",
        "2010-1-7 T")
    }

    val timestampWithoutDate = if (validOnly && !castStringToTimestamp) {
      // 3.2.0+ throws exceptions on string to date ANSI cast errors
      Seq.empty
    } else {
      Seq(
        "23:59:59.333666Z",
        "T21:34:56.333666Z"
      )
    }

    val allValues = specialDates ++
        validYear ++
        validYearMonth ++
        validYearMonthDay ++
        invalidValues ++
        validTimestamps ++
        timestampWithoutDate

    // these partial timestamp formats are not yet supported in cast string to timestamp but are
    // supported by cast string to date
    val partialTimestamps = Seq(
      "2010-01-05T12:34:56Z",
      "2010-02-5T12:34:56Z"
    )

    val values = if (castStringToTimestamp) {
      // filter out "now" because it contains the current time so CPU and GPU will never match
      allValues.filterNot(_ == "now")
    } else {
      allValues ++ partialTimestamps
    }

    val valuesWithWhitespace = values.map(s => s"\t\n\t$s\r\n")

    (values ++ valuesWithWhitespace).toDF("c0")
  }

  def validTimestamps(session: SparkSession): DataFrame = {
    import session.sqlContext.implicits._
    val timestampStrings = Seq(
      "1920-12-31T11:59:59.999",
      "1969-12-31T23:59:59.999",
      "1969-12-31T23:59:59.999999",
      "1970-01-01T00:00:00.000",
      "1970-01-01T00:00:00.999",
      "1970-01-01T00:00:00.999111",
      "2020-12-31T11:59:59.990",
      "2020-12-31T11:59:59.900",
      "2020-12-31T11:59:59.000",
      "2020-12-31T11:59:50.000",
      "2020-12-31T11:59:00.000",
      "2020-12-31T11:50:00.000",
      "2020-12-31T11:00:00.000"
    )
    val timestamps = timestampStrings
      .map(s => Timestamp.valueOf(LocalDateTime.parse(s)))

    timestamps.toDF("c0")
  }

  protected def validBoolStrings(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val boolStrings: Seq[String] = Seq("t", "true", "y", "yes", "1") ++
      Seq("f", "false", "n", "no", "0")
    boolStrings.toDF("c0")
  }

  private val byteValues: Seq[Byte] = Seq(Byte.MinValue, Byte.MaxValue, 0, -0, -1, 1)
  private val shortValues: Seq[Short] = Seq(Short.MinValue, Short.MaxValue, 0, -0, -1, 1)
  private val intValues: Seq[Int] = Seq(Int.MinValue, Int.MaxValue, 0, -0, -1, 1)
  private val longValues: Seq[Long] = Seq(Long.MinValue, Long.MaxValue, 0, -0, -1, 1)
  private val timestampValues: Seq[Long] = Seq(6321706291000L)

}
