/*
 * Copyright (c) 2022-2023, NVIDIA CORPORATION.
 *
 * This file was derived from StatisticsCollection.scala
 * in the Delta Lake project at https://github.com/delta-io/delta.
 *
 * Copyright (2021) The Delta Lake Project Authors.
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

package com.nvidia.spark.rapids.delta

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnView, DType}
import com.nvidia.spark.RebaseHelper.withResource
import com.nvidia.spark.rapids.{Arm, GpuScalar}
import com.nvidia.spark.rapids.delta.shims.{ShimDeltaColumnMapping, ShimDeltaUDF, ShimUsesMetadataFields}

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.functions.{count, lit, max, min, struct, substring, sum, when}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** GPU version of Delta Lake's StatisticsCollection. */
trait GpuStatisticsCollection extends ShimUsesMetadataFields with Arm {
  def tableDataSchema: StructType
  def dataSchema: StructType
  val numIndexedCols: Int
  val stringPrefixLength: Int

  /**
   * statCollectionSchema is the schema that is composed of all the columns that have the stats
   * collected with our current table configuration.
   */
  lazy val statCollectionSchema: StructType = {
    if (numIndexedCols >= 0) {
      truncateSchema(tableDataSchema, numIndexedCols)._1
    } else {
      tableDataSchema
    }
  }

  /**
   * Returns a struct column that can be used to collect statistics for the current
   * schema of the table.
   * The types we keep stats on must be consistent with DataSkippingReader.SkippingEligibleLiteral.
   * @note The stats collected here must be kept in sync with how stats are gathered by the GPU
   *       in batchStatsToRow.
   */
  lazy val statsCollector: Column = {
    val prefixLength = stringPrefixLength
    struct(
      count(new Column("*")) as NUM_RECORDS,
      collectStats(MIN, statCollectionSchema) {
        // Truncate string min values as necessary
        case (c, GpuSkippingEligibleDataType(StringType)) =>
          substring(min(c), 0, stringPrefixLength)

        // Collect all numeric min values
        case (c, GpuSkippingEligibleDataType(_)) =>
          min(c)
      },
      collectStats(MAX, statCollectionSchema) {
        // Truncate and pad string max values as necessary
        case (c, GpuSkippingEligibleDataType(StringType)) =>
          val udfTruncateMax = ShimDeltaUDF.stringStringUdf(
            GpuStatisticsCollection.truncateMaxStringAgg(prefixLength)_)
          udfTruncateMax(max(c))

        // Collect all numeric max values
        case (c, GpuSkippingEligibleDataType(_)) =>
          max(c)
      },
      collectStats(NULL_COUNT, statCollectionSchema) {
        case (c, f) => sum(when(c.isNull, 1).otherwise(0))
      }
    ) as 'stats
  }

  /**
   * Generate a truncated data schema for stats collection
   * @param schema the original data schema
   * @param indexedCols the maximum number of leaf columns to collect stats on
   * @return truncated schema and the number of leaf columns in this schema
   */
  private def truncateSchema(schema: StructType, indexedCols: Int): (StructType, Int) = {
    var accCnt = 0
    var i = 0
    var fields = ArrayBuffer[StructField]()
    while (i < schema.length && accCnt < indexedCols) {
      val field = schema.fields(i)
      val newField = field match {
        case StructField(name, st: StructType, nullable, metadata) =>
          val (newSt, cnt) = truncateSchema(st, indexedCols - accCnt)
          accCnt += cnt
          StructField(name, newSt, nullable, metadata)
        case f =>
          accCnt += 1
          f
      }
      i += 1
      fields += newField
    }
    (StructType(fields.toSeq), accCnt)
  }

  /**
   * Recursively walks the given schema, constructing an expression to calculate
   * multiple statistics that mirrors structure of the data. When `function` is
   * defined for a given column, it return value is added to statistics structure.
   * When `function` is not defined, that column is skipped.
   *
   * @param name     The name of the top level column for this statistic (i.e. minValues).
   * @param schema   The schema of the data to collect statistics from.
   * @param function A partial function that is passed both a column and metadata about that
   *                 column. Based on the metadata, it can decide if the given statistic
   *                 should be collected by returning the correct aggregate expression.
   */
  private def collectStats(
      name: String,
      schema: StructType)(
      function: PartialFunction[(Column, StructField), Column]): Column = {

    def collectStats(
        schema: StructType,
        parent: Option[Column],
        function: PartialFunction[(Column, StructField), Column]): Seq[Column] = {
      schema.flatMap {
        case f @ StructField(name, s: StructType, _, _) =>
          val column = parent.map(_.getItem(name))
              .getOrElse(new Column(UnresolvedAttribute.quoted(name)))
          val stats = collectStats(s, Some(column), function)
          if (stats.nonEmpty) {
            Some(struct(stats: _*) as ShimDeltaColumnMapping.getPhysicalName(f))
          } else {
            None
          }
        case f @ StructField(name, _, _, _) =>
          val column = parent.map(_.getItem(name))
              .getOrElse(new Column(UnresolvedAttribute.quoted(name)))
          // alias the column with its physical name
          function.lift((column, f)).map(_.as(ShimDeltaColumnMapping.getPhysicalName(f)))
      }
    }

    val allStats = collectStats(schema, None, function)
    val stats = if (numIndexedCols > 0) {
      allStats.take(numIndexedCols)
    } else {
      allStats
    }

    if (stats.nonEmpty) {
      struct(stats: _*).as(name)
    } else {
      lit(null).as(name)
    }
  }
}

object GpuStatisticsCollection {
  /**
   * Helper method to truncate the input string `x` to the given `prefixLen` length, while also
   * appending the unicode max character to the end of the truncated string. This ensures that any
   * value in this column is less than or equal to the max.
   */
  def truncateMaxStringAgg(prefixLen: Int)(x: String): String = {
    if (x == null || x.length <= prefixLen) {
      x
    } else {
      // Grab the prefix. We want to append `\ufffd` as a tie-breaker, but that is only safe
      // if the character we truncated was smaller. Keep extending the prefix until that
      // condition holds, or we run off the end of the string.
      // scalastyle:off nonascii
      val tieBreaker = '\ufffd'
      x.take(prefixLen) + x.substring(prefixLen).takeWhile(_ >= tieBreaker) + tieBreaker
      // scalastyle:off nonascii
    }
  }

  /** UTF8String equivalent to truncateMaxStringAgg(Int)(String) */
  private def truncateMaxStringAgg(prefixLen: Int, x: UTF8String): UTF8String = {
    if (x == null || x.numChars() <= prefixLen) {
      x
    } else {
      UTF8String.fromString(truncateMaxStringAgg(prefixLen)(x.toString))
    }
  }

  def batchStatsToRow(
      schema: StructType,
      batch: Array[ColumnView],
      row: InternalRow): Unit = {
    var nextSlot = 0
    // NUM_RECORDS
    val numRows = if (batch.nonEmpty) batch(0).getRowCount.toInt else 0
    row.setLong(nextSlot, numRows)
    nextSlot += 1

    // MIN
    nextSlot = batchStatsToRow(schema, batch, row, nextSlot, GpuStatisticsCollection.computeMin,
      isForAllTypes = false)

    // MAX
    nextSlot = batchStatsToRow(schema, batch, row, nextSlot, GpuStatisticsCollection.computeMax,
      isForAllTypes = false)

    // NULL_COUNT
    nextSlot = batchStatsToRow(schema, batch, row, nextSlot,
      GpuStatisticsCollection.computeNullCount, isForAllTypes = true)

    require(nextSlot == row.numFields, s"Expected ${row.numFields} stats, only wrote $nextSlot")
  }

  private def batchStatsToRow(
      schema: StructType,
      batch: Array[ColumnView],
      row: InternalRow,
      slot: Int,
      statFunc: (ColumnView, DataType, InternalRow, Int) => Unit,
      isForAllTypes: Boolean): Int = {
    withResource(ColumnView.makeStructView(batch: _*)) { structView =>
      structStatsToRow(structView, schema, row, slot, statFunc, isForAllTypes)
    }
  }

  private def structStatsToRow(
      struct: ColumnView,
      structSchema: StructType,
      row: InternalRow,
      slot: Int,
      statFunc: (ColumnView, DataType, InternalRow, Int) => Unit,
      isForAllTypes: Boolean): Int = {
    require(struct.getType.equals(DType.STRUCT), s"Expected struct column, found ${struct.getType}")
    var nextSlot = slot
    (0 until structSchema.length).foreach { i =>
      structSchema.fields(i).dataType match {
        case s: StructType =>
          withResource(struct.getChildColumnView(i)) { c =>
            nextSlot = structStatsToRow(c, s, row, nextSlot, statFunc, isForAllTypes)
          }
        case dt if isForAllTypes || GpuSkippingEligibleDataType(dt) =>
          withResource(struct.getChildColumnView(i)) { c =>
            statFunc(c, dt, row, nextSlot)
          }
          nextSlot += 1
        case _ =>
      }
    }
    nextSlot
  }

  private def writeValueToRow(value: Any, dt: DataType, row: InternalRow, slot: Int): Unit = {
    val valueType = if (value == null) NullType else dt
    val writer = InternalRow.getWriter(slot, valueType)
    writer(row, value)
  }

  private def computeMin(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    val minValue = dt match {
      // cudf min with NaNs does not match Spark behavior, so work around it here
      case FloatType | DoubleType =>
        val (isAllNan, isAnyNan) = withResource(c.isNan) { isNan =>
          val isAllNan = withResource(isNan.all()) { allNans =>
            allNans.isValid && allNans.getBoolean
          }
          if (isAllNan) {
            (isAllNan, true)
          } else {
            val isAnyNan = withResource(isNan.any()) { anyNans =>
              anyNans.isValid && anyNans.getBoolean
            }
            (isAllNan, isAnyNan)
          }
        }
        if (isAllNan) {
          if (dt == FloatType) Float.NaN else Double.NaN
        } else if (isAnyNan) {
          // Need to replace NaNs with nulls before calling cudf min
          withResource(c.nansToNulls()) { noNans =>
            withResource(noNans.min()) { s =>
              GpuScalar.extract(s)
            }
          }
        } else {
          withResource(c.min()) { s =>
            GpuScalar.extract(s)
          }
        }
      case _ =>
        withResource(c.min()) { s =>
          GpuScalar.extract(s)
        }
    }
    writeValueToRow(minValue, dt, row, slot)
  }

  private def computeMax(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    def hasNans(c: ColumnView): Boolean = {
      withResource(c.isNan) { isNan =>
        withResource(isNan.any()) { anyNan =>
          anyNan.isValid && anyNan.getBoolean
        }
      }
    }

    // cudf max with NaNs does not match Spark behavior, so work around it here
    val maxValue = dt match {
      case FloatType if hasNans(c) => Float.NaN
      case DoubleType if hasNans(c) => Double.NaN
      case _ =>
        withResource(c.max()) { s =>
          GpuScalar.extract(s)
        }
    }
    writeValueToRow(maxValue, dt, row, slot)
  }

  private def computeNullCount(
      c: ColumnView,
      dt: DataType,
      row: InternalRow,
      slot: Int): Unit = {
    row.setLong(slot, c.getNullCount)
  }
}

/**
 * Based on Delta's SkippingEligibleDataType. Copied here to keep consistent with
 * statistics generating code and avoid surprises if provided Delta code changes.
 */
object GpuSkippingEligibleDataType {
  // Call this directly, e.g. `GpuSkippingEligibleDataType(dataType)`
  def apply(dataType: DataType): Boolean = dataType match {
    case _: NumericType | DateType | TimestampType | StringType => true
    case _ => false
  }

  // Use these in `match` statements
  def unapply(dataType: DataType): Option[DataType] = {
    if (GpuSkippingEligibleDataType(dataType)) Some(dataType) else None
  }

  def unapply(f: StructField): Option[DataType] = unapply(f.dataType)
}

