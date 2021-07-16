/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, DecimalType}

class QueryFuzzer(val seed: Long) {

  val rand = new Random(seed)

  val idGen = new AtomicLong(0)

  def generateConfig(): SparkConf = {
    new SparkConf()
      .set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, boolString())
      .set(SQLConf.ANSI_ENABLED.key, boolString())
      .set(SQLConf.LEGACY_TIME_PARSER_POLICY.key, oneOf("LEGACY", "CORRECTED", "EXCEPTION"))
      .set("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .set(RapidsConf.DECIMAL_TYPE_ENABLED.key, boolString())
      .set(RapidsConf.INCOMPATIBLE_DATE_FORMATS.key, boolString())
      .set(RapidsConf.INCOMPATIBLE_OPS.key, boolString())
      .set(RapidsConf.ENABLE_CAST_STRING_TO_FLOAT.key, boolString())
      .set(RapidsConf.ENABLE_CAST_STRING_TO_DECIMAL.key, boolString())
      .set(RapidsConf.ENABLE_CAST_STRING_TO_DECIMAL.key, boolString())
      .set(RapidsConf.ENABLE_CAST_STRING_TO_INTEGER.key, boolString())
      .set(RapidsConf.ENABLE_CAST_STRING_TO_TIMESTAMP.key, boolString())
      .set(RapidsConf.ENABLE_CAST_FLOAT_TO_STRING.key, boolString())
      .set(RapidsConf.ENABLE_CAST_FLOAT_TO_DECIMAL.key, boolString())
      .set(RapidsConf.ENABLE_CAST_FLOAT_TO_INTEGRAL_TYPES.key, boolString())
      .set(RapidsConf.ENABLE_CAST_DECIMAL_TO_STRING.key, boolString())
      .set(RapidsConf.ENABLE_FLOAT_AGG.key, boolString())
      .set(RapidsConf.ENABLE_HASH_OPTIMIZE_SORT.key, boolString())
  }

  private def boolString(): String = {
    rand.nextBoolean().toString
  }

  private def oneOf(value: String*): String = {
    value(rand.nextInt(value.length))
  }

  def randomOperator(spark: SparkSession, depth: Int, maxDepth: Int): DataFrame = {

    // if we have reached maximum depth then create a leaf node
    if (depth == maxDepth) {
      return generateDataSource(spark)

      //TODO Add support for reading from CSV and Parquet data sources
//      if (rand.nextFloat() > 0.7) {
//        return generateDataSource(spark)
//      } else {
//        return renameColumns(
//          spark.read.parquet("/mnt/tpcds/parquet/sf100-decimals/web_sales.dat"))
//      }
    }

    // recurse down first to get a DataFrame
    val df = randomOperator(spark, depth+1, maxDepth)

    // build a list of available transformations
    val ctx = FuzzContext(spark, this)
    val transformations = Seq(
      Filter(ctx),
      SortRandomColumns(ctx),
      RandomCastFromString(ctx),
      Aggregate(ctx),
      Repartition(ctx),
      NoopTransform(ctx),
    )

    // filter down to a list of transformations that are applicable in the current context
    val validTransformations = transformations.filter(_.canTransform(df))

    // pick a transformation at random
    val tf = randomElement(validTransformations)

    // apply the transformation
    tf.transform(df)
  }

  /**
   * Generate a random in-memory DataFrame.
   */
  def generateDataSource(spark: SparkSession) = {
    val numFields = 1 + rand.nextInt(32)
    val supportedTypes = Seq(DataTypes.IntegerType,
      DataTypes.StringType,
      new DecimalType(10,2),
      DataTypes.DateType,
      DataTypes.TimestampType,
      DataTypes.BooleanType,
      DataTypes.DoubleType,
      DataTypes.FloatType)
    val dataTypes = (0 until numFields).map(_ => randomElement(supportedTypes))
    val schema = FuzzerUtils.createSchema(dataTypes)
    val options = FuzzerOptions(validStringChars = Some(" \t\r\n0123456789.+-/:aidfnT"),
      maxStringLen = 12)
    val df = FuzzerUtils.generateDataFrame(spark, schema, rowCount = 256, options, seed)
      .repartition(1 + rand.nextInt(4))
    renameColumns(df)
  }

  /**
   * Pick a random element from a sequence.
   */
  def randomElement[T](seq: Seq[T]): T = {
    seq(rand.nextInt(seq.length))
  }

  /**
   * Rename all of the columns in a DataFrame so that they are unique.
   */
  def renameColumns(df: DataFrame): DataFrame = {
    var dfRenamed = df;
    for (name <- df.columns) {
      dfRenamed = dfRenamed.withColumnRenamed(name, nextName())
    }
    dfRenamed
  }

  /**
   * Generate the next column name.
   */
  def nextName(): String = "c" + idGen.getAndIncrement()

}

case class FuzzContext(spark: SparkSession, fuzzer: QueryFuzzer)

trait Transformation {
  def canTransform(df: DataFrame): Boolean
  def transform(df: DataFrame): DataFrame
}

case class Filter(ctx: FuzzContext) extends Transformation {

  override def canTransform(df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.IntegerType)
  }

  override def transform(df: DataFrame): DataFrame = {
    val intCols = df.schema.fields.filter(_.dataType == DataTypes.IntegerType)
    df.filter(col(intCols(0).name).gt(col(intCols(1).name)))
  }
}

case class SortRandomColumns(ctx: FuzzContext) extends Transformation {

  override def canTransform(df: DataFrame): Boolean = df.schema.fields.nonEmpty

  override def transform(df: DataFrame): DataFrame = {
    val numSortColumns = 1 + ctx.fuzzer.rand.nextInt(2)
    val sortColumns = (0 until numSortColumns)
      .map(_ => df.columns(ctx.fuzzer.rand.nextInt(df.columns.length)))
    df.sort(sortColumns.head, sortColumns.drop(1): _*)
  }
}

case class Aggregate(ctx: FuzzContext) extends Transformation {

  override def canTransform(df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.IntegerType)
  }

  override def transform(df: DataFrame): DataFrame = {
    val groupCol = col(df.columns(ctx.fuzzer.rand.nextInt(df.columns.length)))
    val intCols = df.schema.fields.filter(_.dataType == DataTypes.IntegerType)
    if (intCols.nonEmpty) {
      val aggrCol = intCols(ctx.fuzzer.rand.nextInt(intCols.length)).name
      ctx.fuzzer.renameColumns(df.groupBy(groupCol).sum(aggrCol))
    } else {
      df
    }
  }
}

case class RandomCastFromString(ctx: FuzzContext) extends Transformation {

  val castTo = Seq(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType,
    DataTypes.LongType, DataTypes.FloatType, DataTypes.DoubleType,
    DataTypes.DateType, DataTypes.TimestampType)

  override def canTransform(df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.StringType)
  }

  override def transform(df: DataFrame): DataFrame = {
    val stringCol = df.schema.fields.find(_.dataType == DataTypes.StringType)
      df.withColumn(ctx.fuzzer.nextName(),
        col(stringCol.get.name).cast(castTo(ctx.fuzzer.rand.nextInt(castTo.length))))
  }
}

case class Repartition(ctx: FuzzContext) extends Transformation {

  override def canTransform(df: DataFrame): Boolean = true

  override def transform(df: DataFrame): DataFrame = {
    df.repartition(1 + ctx.fuzzer.rand.nextInt(4),
      col(df.columns(ctx.fuzzer.rand.nextInt(df.columns.length))))
  }
}

case class Join(ctx: FuzzContext) extends Transformation {

  override def canTransform(df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.IntegerType)
  }

  override def transform(df: DataFrame): DataFrame = {
    val df2 = ctx.fuzzer.generateDataSource(ctx.spark)
    val leftKey = df.schema.fields.find(_.dataType == DataTypes.IntegerType)
    val rightKey = df2.schema.fields.find(_.dataType == DataTypes.IntegerType)
    if (leftKey.isDefined && rightKey.isDefined) {
      df.join(df2, col(leftKey.get.name).equalTo(col(rightKey.get.name)))
    } else {
      df
    }
  }
}

case class NoopTransform(ctx: FuzzContext) extends Transformation {
  override def canTransform(df: DataFrame): Boolean = true
  override def transform(df: DataFrame): DataFrame = df
}