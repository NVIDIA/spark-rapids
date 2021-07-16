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

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataTypes, DecimalType}

class QueryFuzzer(val seed: Long) {

  val rand = new Random(seed)

  var idGen = 0L

  // these are some trivial transformations that demonstrate how the fuzzer works and
  // are enough to catch some bugs already but we will need to invest in building out
  // some more realistic/complex transformations to really stress the product
  val transformations = Seq(
    InnerJoinOnIntegerKeys(),
    FilterCompareTwoInts(),
    SortRandomColumns(),
    RandomCastFromString(),
    SimpleAggregate(),
    Repartition()
  )

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
    }

    // recurse down first to get a DataFrame
    val df = randomOperator(spark, depth+1, maxDepth)

    // build a list of available transformations
    val ctx = FuzzContext(spark, this)

    // filter down to a list of transformations that are applicable in the current context
    val validTransformations = transformations.filter(_.canTransform(ctx, df))
    if (validTransformations.isEmpty) {
      // there are no valid transformations so just return the DataFrame
      df
    } else {
      // pick a transformation at random
      val tf = randomElement(validTransformations)
      // apply the transformation
      tf.transform(ctx, df)
    }
  }

  /**
   * Generate a random in-memory DataFrame.
   */
  def generateDataSource(spark: SparkSession): DataFrame = {
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
  def nextName(): String = {
    val name = "c" + idGen
    idGen += 1
    name
  }

}

case class FuzzContext(spark: SparkSession, fuzzer: QueryFuzzer)

trait Transformation {
  def canTransform(ctx: FuzzContext, df: DataFrame): Boolean
  def transform(ctx: FuzzContext, df: DataFrame): DataFrame
}

/**
 * Introduce a filter comparing two integer columns.
 */
case class FilterCompareTwoInts() extends Transformation {

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = {
    df.schema.fields.count(_.dataType == DataTypes.IntegerType) > 1
  }

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
    val intCols = df.schema.fields.filter(_.dataType == DataTypes.IntegerType)
    df.filter(col(intCols(0).name).gt(col(intCols(1).name)))
  }
}

/**
 * Introduce a sort using a random set of columns.
 */
case class SortRandomColumns() extends Transformation {

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = df.schema.fields.nonEmpty

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
    val numSortColumns = 1 + ctx.fuzzer.rand.nextInt(2)
    val sortColumns = (0 until numSortColumns)
      .map(_ => df.columns(ctx.fuzzer.rand.nextInt(df.columns.length)))
    df.sort(sortColumns.head, sortColumns.drop(1): _*)
  }
}

/**
 * Introduce a simple aggregation (sum) of an integer column, using a
 * single random column as the grouping key.
 */
case class SimpleAggregate() extends Transformation {

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.IntegerType)
  }

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
    val groupCol = col(df.columns(ctx.fuzzer.rand.nextInt(df.columns.length)))
    val intCols = df.schema.fields.filter(_.dataType == DataTypes.IntegerType)
    val aggrCol = intCols(ctx.fuzzer.rand.nextInt(intCols.length)).name
    ctx.fuzzer.renameColumns(df.groupBy(groupCol).sum(aggrCol))
  }
}

/**
 * Introduce a projection that casts a string column to another data type.
 */
case class RandomCastFromString() extends Transformation {

  val castTo = Seq(DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType,
    DataTypes.LongType, DataTypes.FloatType, DataTypes.DoubleType,
    DataTypes.DateType, DataTypes.TimestampType)

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.StringType)
  }

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
    val stringCol = df.schema.fields.find(_.dataType == DataTypes.StringType)
      df.withColumn(ctx.fuzzer.nextName(),
        col(stringCol.get.name).cast(castTo(ctx.fuzzer.rand.nextInt(castTo.length))))
  }
}

/**
 * Repartition using a single column and a random number of partitions.
 */
case class Repartition() extends Transformation {

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = true

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
    df.repartition(1 + ctx.fuzzer.rand.nextInt(4),
      col(df.columns(ctx.fuzzer.rand.nextInt(df.columns.length))))
  }
}

/**
 * Performs an inner join on two relations using the first integer column from each
 * relation as the join keys.
 */
case class InnerJoinOnIntegerKeys() extends Transformation {

  override def canTransform(ctx: FuzzContext, df: DataFrame): Boolean = {
    df.schema.fields.exists(_.dataType == DataTypes.IntegerType)
  }

  override def transform(ctx: FuzzContext, df: DataFrame): DataFrame = {
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

