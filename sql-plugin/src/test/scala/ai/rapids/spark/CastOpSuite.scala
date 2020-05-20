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

import java.sql.Timestamp
import java.util.TimeZone

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class CastOpSuite extends GpuExpressionTestSuite {
  import CastOpSuite._

  private val timestampDatesMsecParquet =
    frameFromParquet("timestamp-date-test-msec.parquet")

  /** Data types supported by the plugin. */
  protected val supportedTypes = Seq(DataTypes.BooleanType,
    DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType,
    DataTypes.FloatType, DataTypes.DoubleType,
    DataTypes.DateType, DataTypes.TimestampType,
    DataTypes.StringType
  )

  /** Produces a matrix of all possible casts. */
  protected def typeMatrix: Seq[(DataType, DataType)] = {
    for (from <- supportedTypes; to <- supportedTypes) yield (from, to)
  }

  test("Test all supported casts with in-range values") {

    // test cast() and ansi_cast()
    Seq(false, true).foreach { ansiEnabled =>

      val conf = new SparkConf()
        .set(RapidsConf.ENABLE_CAST_FLOAT_TO_STRING.key, "true")
        .set(RapidsConf.ENABLE_CAST_TIMESTAMP_TO_STRING.key, "true")
        .set(RapidsConf.ENABLE_CAST_STRING_TO_INTEGER.key, "true")
        .set("spark.sql.ansi.enabled", String.valueOf(ansiEnabled))

      typeMatrix.foreach {
        case (from, to) =>
          // check if Spark supports this cast
          if (Cast.canCast(from, to)) {
            // check if plugin supports this cast
            if (GpuCast.canCast(from, to, ansiEnabled)) {
              // test the cast
              try {
                val (fromCpu, fromGpu) =
                  runOnCpuAndGpu(generateInRangeTestData(from, to, ansiEnabled),
                  frame => frame.select(col("c0").cast(to))
                    .orderBy(col("c0")), conf)

                // perform comparison logic specific to the cast
                (from, to) match {
                  case (DataTypes.TimestampType, DataTypes.StringType) =>
                    compareTimestampToStringResults(fromCpu, fromGpu)
                  case (DataTypes.FloatType | DataTypes.DoubleType, DataTypes.StringType) =>
                    compareFloatToStringResults(fromCpu, fromGpu)
                  case (DataTypes.FloatType | DataTypes.DoubleType, DataTypes.StringType) =>
                    // specific comparison logic required for this cast
                    fromCpu.zip(fromGpu).foreach {
                      case (c, g) =>
                        val cpuValue = c.getAs[String](0)
                        val gpuValue = g.getAs[String](0)
                        if (!compareStringifiedFloats(cpuValue, gpuValue)) {
                          fail(s"Running on the GPU and on the CPU did not match: CPU " +
                            s"value: $cpuValue. GPU value: $gpuValue.")
                        }
                    }

                  case _ =>
                    compareResults(sort = false, 0.00001, fromCpu, fromGpu)
                }

              } catch {
                case e: Exception =>
                  fail(s"Cast from $from to $to failed; ansi=$ansiEnabled", e)
              }
            }
          } else {
            // if Spark doesn't support this cast then the plugin shouldn't either
            assert(!GpuCast.canCast(from, to))
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

  private def compareTimestampToStringResults(fromCpu: Array[Row], fromGpu: Array[Row]) = {
    def removeTrailingZeros(timestampAsString: String): String = {
      if (timestampAsString == null) {
        null
      } else {
        var s = timestampAsString
        while (s.endsWith("0") || s.endsWith(".")) {
          s = s.substring(0, s.length - 1)
        }
        s
      }
    }
    val cpu = fromCpu.map(row => Row.fromSeq(Seq(removeTrailingZeros(row
      .getString(0)))))
    val gpu = fromGpu.map(row => Row.fromSeq(Seq(removeTrailingZeros(row
      .getString(0)))))
    compareResults(sort = false, 0.00001, cpu, gpu)
  }

  test("Test unsupported cast") {
    // this test tracks currently unsupported casts and will need updating as more casts are
    // supported
    val unsupported = getUnsupportedCasts(false)
    val expected = List((StringType,FloatType),
      (StringType,DoubleType),
      (StringType,DateType),
      (StringType,TimestampType))
    assert(unsupported == expected)
  }

  test("Test unsupported ansi_cast") {
    // this test tracks currently unsupported ansi_casts and will need updating as more casts are
    // supported
    val unsupported = getUnsupportedCasts(true)
    val expected = List((StringType,FloatType),
      (StringType,DoubleType),
      (StringType,DateType),
      (StringType,TimestampType))

    assert(unsupported == expected)
  }

  private def getUnsupportedCasts(ansiEnabled: Boolean) = {
    val unsupported = typeMatrix.flatMap {
      case (from, to) =>
        if (Cast.canCast(from, to) && !GpuCast.canCast(from, to, ansiEnabled)) {
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

      case (DataTypes.StringType, DataTypes.BooleanType) => validBoolStrings(spark)

      case (DataTypes.StringType, DataTypes.BooleanType) if ansiEnabled => validBoolStrings(spark)
      case (DataTypes.StringType, DataTypes.ByteType) if ansiEnabled => bytesAsStrings(spark)
      case (DataTypes.StringType, DataTypes.ShortType) if ansiEnabled => shortsAsStrings(spark)
      case (DataTypes.StringType, DataTypes.IntegerType) if ansiEnabled => intsAsStrings(spark)
      case (DataTypes.StringType, DataTypes.LongType)  => if (ansiEnabled) {
        // ansi_cast does not support decimals
        longsAsStrings(spark)
      } else {
        longsAsDecimalStrings(spark)
      }

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

  ignore("cast float to string") {
    testCastToString[Float](DataTypes.FloatType, comparisonFunc = Some(compareStringifiedFloats))
  }

  ignore("cast double to string") {
    testCastToString[Double](DataTypes.DoubleType, comparisonFunc = Some(compareStringifiedFloats))
  }

  private def testCastToString[T](
      dataType: DataType,
      comparisonFunc: Option[(String, String) => Boolean] = None) {
    assert(GpuCast.canCast(dataType, DataTypes.StringType, false))
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
      col("longs").cast(ShortType),
      col("longs").cast(FloatType),
      col("longs").cast(DoubleType),
      col("longs").cast(TimestampType))
  }

  testSparkResultsAreEqual("Test cast from float", mixedFloatDf) {
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

  testSparkResultsAreEqual("Test cast from double", doubleWithNansDf) {
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

  ignore("Test cast from double to string") {

    //NOTE that the testSparkResultsAreEqual method isn't adequate in this case because we
    // need to use a specialized comparison function

    val conf = new SparkConf()
      .set(RapidsConf.ENABLE_CAST_FLOAT_TO_STRING.key, "true")

    val (cpu, gpu) = runOnCpuAndGpu(doubleDf, frame => frame.select(
      col("doubles").cast(StringType))
      .orderBy(col("doubles")), conf)

    val fromCpu = cpu.map(row => row.getAs[String](0))
    val fromGpu = gpu.map(row => row.getAs[String](0))

    fromCpu.zip(fromGpu).foreach {
      case (c, g) =>
        if (!compareStringifiedFloats(c, g)) {
          fail(s"Running on the GPU and on the CPU did not match: CPU value: $c. " +
            s"GPU value: $g.")
        }
    }
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
      col("longs").cast(TimestampType))
    // There is a bug in the way we are casting doubles to timestamp.
    // https://gitlab-master.nvidia.com/nvspark/rapids-plugin-4-spark/issues/47
      //, col("doubles").cast(TimestampType))
  }

}

/** Data shared between CastOpSuite and AnsiCastOpSuite. */
object CastOpSuite {

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
    intValues.map(_.toFloat).toDF("c0")
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