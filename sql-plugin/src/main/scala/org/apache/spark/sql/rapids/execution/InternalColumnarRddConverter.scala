/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION. All rights reserved.
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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SQLExecutionRDD
import org.apache.spark.sql.rapids.execution.GpuExternalRowToColumnConverter.{FixedWidthTypeConverter, VariableWidthTypeConverter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This code is based off of the code for [[com.nvidia.spark.rapids.GpuRowToColumnarExec]] but this
 * is for `Row` instead of `InternalRow`, so it is not an exact transition.
 */
private class GpuExternalRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuExternalRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: Row, builders: GpuColumnarBatchBuilder): Long = {
    var bytes: Long = 0
    for (idx <- 0 until row.length) {
      converters(idx) match {
        case tc: FixedWidthTypeConverter =>
          tc.append(row, idx, builders.builder(idx))
        case tc: VariableWidthTypeConverter =>
          bytes += tc.append(row, idx, builders.builder(idx))
      }
    }
    bytes
  }
}

private object GpuExternalRowToColumnConverter {

  private trait TypeConverter extends Serializable

  private abstract class FixedWidthTypeConverter extends TypeConverter {
    /** Append row value to the column builder */
    def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit
  }

  private abstract class VariableWidthTypeConverter extends TypeConverter {
    /** Append row value to the column builder and return the number of data bytes written */
    def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Long
  }

  private def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
    (dataType, nullable) match {
      case (BooleanType, true) => BooleanConverter
      case (BooleanType, false) => NotNullBooleanConverter
      case (ByteType, true) => ByteConverter
      case (ByteType, false) => NotNullByteConverter
      case (ShortType, true) => ShortConverter
      case (ShortType, false) => NotNullShortConverter
      case (IntegerType, true) => IntConverter
      case (IntegerType, false) => NotNullIntConverter
      case (FloatType, true) => FloatConverter
      case (FloatType, false) => NotNullFloatConverter
      case (LongType, true) => LongConverter
      case (LongType, false) => NotNullLongConverter
      case (DoubleType, true) => DoubleConverter
      case (DoubleType, false) => NotNullDoubleConverter
      case (DateType, true) => IntConverter
      case (DateType, false) => NotNullIntConverter
      case (TimestampType, true) => LongConverter
      case (TimestampType, false) => NotNullLongConverter
      case (StringType, true) => StringConverter
      case (StringType, false) => NotNullStringConverter
      // NOT SUPPORTED YET case CalendarIntervalType => CalendarConverter
      // NOT SUPPORTED YET case at: ArrayType => new ArrayConverter(
      //  getConverterForType(at.elementType))
      // NOT SUPPORTED YET case st: StructType => new StructConverter(st.fields.map(
      //  (f) => getConverterForType(f.dataType)))
      // NOT SUPPORTED YET case dt: DecimalType => new DecimalConverter(dt)
      // NOT SUPPORTED YET case mt: MapType => new MapConverter(getConverterForType(mt.keyType),
      //  getConverterForType(mt.valueType))
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object BooleanConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
  }

  private object NotNullBooleanConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
  }

  private object ByteConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
  }

  private object NotNullByteConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getByte(column))
  }

  private object ShortConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
  }

  private object NotNullShortConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getShort(column))
  }

  private object IntConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
  }

  private object NotNullIntConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getInt(column))
  }

  private object FloatConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
  }

  private object NotNullFloatConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getFloat(column))
  }

  private object LongConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
  }

  private object NotNullLongConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getLong(column))
  }

  private object DoubleConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
  }

  private object NotNullDoubleConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      builder.append(row.getDouble(column))
  }

  private object StringConverter extends FixedWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullStringConverter.append(row, column, builder)
      }
  }

  private object NotNullStringConverter extends VariableWidthTypeConverter {
    override def append(
        row: Row,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Long = {
      val bytes = row.getString(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length
    }
  }
  //
  //  private object CalendarConverter extends FixedWidthTypeConverter {
  //    override def append(
  //        row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        val c = row.getInterval(column)
  //        cv.appendStruct(false)
  //        cv.getChild(0).appendInt(c.months)
  //        cv.getChild(1).appendLong(c.microseconds)
  //      }
  //    }
  //  }
  //
  //  private case class ArrayConverter(childConverter: TypeConverter) extends TypeConverter {
  //    override def append(
  //        row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        val values = row.getArray(column)
  //        val numElements = values.numElements()
  //        cv.appendArray(numElements)
  //        val arrData = cv.arrayData()
  //        for (i <- 0 until numElements) {
  //          childConverter.append(values, i, arrData)
  //        }
  //      }
  //    }
  //  }
  //
  //  private case class StructConverter(childConverters: Array[TypeConverter])
  //    extends TypeConverter {
  //    override def append(row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        cv.appendStruct(false)
  //        val data = row.getStruct(column, childConverters.length)
  //        for (i <- 0 until childConverters.length) {
  //          childConverters(i).append(data, i, cv.getChild(i))
  //        }
  //      }
  //    }
  //  }
  //
  //  private case class DecimalConverter(dt: DecimalType) extends TypeConverter {
  //    override def append(
  //        row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        val d = row.getDecimal(column, dt.precision, dt.scale)
  //        if (dt.precision <= Decimal.MAX_INT_DIGITS) {
  //          cv.appendInt(d.toUnscaledLong.toInt)
  //        } else if (dt.precision <= Decimal.MAX_LONG_DIGITS) {
  //          cv.appendLong(d.toUnscaledLong)
  //        } else {
  //          val integer = d.toJavaBigDecimal.unscaledValue
  //          val bytes = integer.toByteArray
  //          cv.appendByteArray(bytes, 0, bytes.length)
  //        }
  //      }
  //    }
  //  }
  //
  //  private case class MapConverter(keyConverter: TypeConverter, valueConverter: TypeConverter)
  //    extends TypeConverter {
  //    override def append(
  //        row: SpecializedGetters,
  //        column: Int,
  //        builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
  //      if (row.isNullAt(column)) {
  //        builder.appendNull()
  //      } else {
  //        val m = row.getMap(column)
  //        val keys = cv.getChild(0)
  //        val values = cv.getChild(1)
  //        val numElements = m.numElements()
  //        cv.appendArray(numElements)
  //
  //        val srcKeys = m.keyArray()
  //        val srcValues = m.valueArray()
  //
  //        for (i <- 0 until numElements) {
  //          keyConverter.append(srcKeys, i, keys)
  //          valueConverter.append(srcValues, i, values)
  //        }
  //      }
  //    }
  //  }
}

private class ExternalRowToColumnarIterator(
    rowIter: Iterator[Row],
    localSchema: StructType,
    localGoal: CoalesceGoal,
    converters: GpuExternalRowToColumnConverter) extends Iterator[ColumnarBatch] {

  private val dataTypes: Array[DataType] = localSchema.fields.map(_.dataType)
  private val variableWidthColumnCount = dataTypes.count(dt => !GpuBatchUtils.isFixedWidth(dt))
  private val fixedWidthDataSizePerRow = dataTypes.filter(GpuBatchUtils.isFixedWidth)
    .map(_.defaultSize).sum
  private val nullableColumns = localSchema.fields.count(_.nullable)
  private val targetSizeBytes = localGoal.targetSizeBytes
  private var targetRows = 0
  private var totalOutputBytes: Long = 0
  private var totalOutputRows: Long = 0

  override def hasNext: Boolean = rowIter.hasNext

  override def next(): ColumnarBatch = {
    if (!rowIter.hasNext) {
      throw new NoSuchElementException
    }
    buildBatch()
  }

  private def buildBatch(): ColumnarBatch = {

    // estimate the size of the first batch based on the schema
    if (targetRows == 0) {
      if (localSchema.fields.isEmpty) {
        targetRows = Integer.MAX_VALUE
      } else {
        val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
        val sampleBytes = GpuBatchUtils.estimateGpuMemory(localSchema, sampleRows)
        targetRows = GpuBatchUtils.estimateRowCount(targetSizeBytes, sampleBytes, sampleRows)
      }
    }

    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows)
    try {
      var rowCount = 0
      var byteCount: Long = variableWidthColumnCount * 4 // offset bytes
      while (rowCount < targetRows && byteCount < targetSizeBytes  && rowIter.hasNext) {
        val row = rowIter.next()
        val variableWidthDataBytes = converters.convert(row, builders)
        byteCount += fixedWidthDataSizePerRow // fixed-width data bytes
        byteCount += variableWidthDataBytes // variable-width data bytes
        byteCount += variableWidthColumnCount * GpuBatchUtils.OFFSET_BYTES // offset bytes
        if (nullableColumns > 0 && rowCount % GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS == 0) {
          byteCount += GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_BYTES * nullableColumns
        }
        rowCount += 1
      }

      // enforce single batch limit when appropriate
      if (rowIter.hasNext && localGoal == RequireSingleBatch) {
        throw new IllegalStateException("A single batch is required for this operation." +
          " Please try increasing your partition count.")
      }

      // About to place data back on the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val ret = builders.build(rowCount)

      // refine the targetRows estimate based on the average of all batches processed so far
      totalOutputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
      totalOutputRows += rowCount
      if (totalOutputRows > 0 && totalOutputBytes > 0) {
        targetRows =
          GpuBatchUtils.estimateRowCount(targetSizeBytes, totalOutputBytes, totalOutputRows)
      }

      // The returned batch will be closed by the consumer of it
      ret
    } finally {
      builders.close()
    }
  }
}

/**
 * Please don't use this class directly use [[com.nvidia.spark.rapids.ColumnarRdd]] instead. We had
 * to place the implementation in a spark specific package to poke at the internals of spark more
 * than anyone should know about.
 *
 * This provides a way to get back out GPU Columnar data RDD[Table]. Each Table will have the same
 * schema as the dataframe passed in.  If the schema of the dataframe is something that Rapids does
 * not currently support an `IllegalArgumentException` will be thrown.
 *
 * The size of each table will be determined by what is producing that table but typically will be
 * about the number of bytes set by `RapidsConf.GPU_BATCH_SIZE_BYTES`.
 *
 * Table is not a typical thing in an RDD so special care needs to be taken when working with it.
 * By default it is not serializable so repartitioning the RDD or any other operator that involves
 * a shuffle will not work. This is because it is very expensive to serialize and deserialize a GPU
 * Table using a conventional spark shuffle. Also most of the memory associated with the Table is
 * on the GPU itself, so each table must be closed when it is no longer needed to avoid running out
 * of GPU memory.  By convention it is the responsibility of the one consuming the data to close it
 * when they no longer need it.
 */
object InternalColumnarRddConverter extends Logging {
  def apply(df: DataFrame): RDD[Table] = {
    convert(df)
  }

  def convert(df: DataFrame): RDD[Table] = {
    val schema = df.schema
    if (!GpuOverrides.areAllSupportedTypes(schema.map(_.dataType) :_*)) {
      val unsupported = schema.map(_.dataType).filter(!GpuOverrides.isSupportedType(_)).toSet
      throw new IllegalArgumentException(s"Cannot convert $df to GPU columnar $unsupported are " +
        s"not currently supported data types for columnar.")
    }
    //This config lets the plugin tag the columnar transition we care about so we know what we got.
    df.sqlContext.setConf(RapidsConf.EXPORT_COLUMNAR_RDD.key, "true")
    val input = try {
      df.rdd
    } finally {
      df.sqlContext.setConf(RapidsConf.EXPORT_COLUMNAR_RDD.key, "false")
    }
    var batch: Option[RDD[ColumnarBatch]] = None
    // If we are exporting the data we will see
    // 1) MapPartitionsRDD - which is converting an InternalRow[ObjectType[Row]] to just a Row
    // 2) SQLExecutionRDD - which is where the SQL execution starts, and tries to tie the metrics
    //        collection back into the SQL side of things.
    // 3) MapPartitionsRDD - which is converting an InternalRow[Whatever] to
    //        InternalRow[ObjectType[Row]]
    // 4) GpuColumnToRowMapPartitionsRDD - which is the special RDD we are looking for.
    //
    // The GpuColumnToRowMapPartitionsRDD will only ever be inserted if the plugin sees the correct
    // SQL operators so we don't need to worry too much about what the MapPartitionsRDDs are doing
    // internally.  But if we don't see this pattern exactly we need to fall back to doing it the
    // slow way using Row as the source, because we don't know if one of the MapPartitionsRDDs
    // contains codegen etc.
    input match {
      case in: MapPartitionsRDD[Row, _] =>
        in.prev match {
          case sqlExecRdd: SQLExecutionRDD =>
            sqlExecRdd.sqlRDD match {
              case rowConversionRdd: MapPartitionsRDD[InternalRow, _] =>
                rowConversionRdd.prev match {
                  case c2rRdd: GpuColumnToRowMapPartitionsRDD =>
                    batch = Some(c2rRdd.prev)
                  case rdd =>
                    logDebug("Cannot extract columnar RDD directly. " +
                      s"(column to row not found $rdd)")
                }
              case rdd =>
                logDebug("Cannot extract columnar RDD directly. " +
                  s"(Internal row to row rdd not found $rdd)")
            }
          case rdd =>
            logDebug("Cannot extract columnar RDD directly. " +
              s"(SQLExecutionRDD not found $rdd)")
        }
      case rdd =>
        logDebug(s"Cannot extract columnar RDD directly. " +
          s"(First MapPartitionsRDD not found $rdd)")
    }

    val b = batch.getOrElse({
      // We have to fall back to doing a slow transition.
      val converters = new GpuExternalRowToColumnConverter(schema)
      val conf = new RapidsConf(df.sqlContext.conf)
      val goal = TargetSize(conf.gpuTargetBatchSizeBytes)
      input.mapPartitions { rowIter =>
        new ExternalRowToColumnarIterator(rowIter, schema, goal, converters)
      }
    })

    b.map(cb => try {
      GpuColumnVector.from(cb)
    } finally {
      cb.close()
    })
  }
}

object GpuColumnToRowMapPartitionsRDD {
  /**
   * Return a new RDD by applying a function to each partition of this RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves the partitioner, which
   * should be `false` unless this is a pair RDD and the input function doesn't modify the keys.
   */
  def mapPartitions(
      orig: RDD[ColumnarBatch],
      f: Iterator[ColumnarBatch] => Iterator[InternalRow],
      preservesPartitioning: Boolean = false): RDD[InternalRow] = {
    val sc = orig.context
    val cleanedF = sc.clean(f)
    new GpuColumnToRowMapPartitionsRDD(orig,
      (_: TaskContext, _: Int, iter: Iterator[ColumnarBatch]) => cleanedF(iter),
      preservesPartitioning)
  }
}

class GpuColumnToRowMapPartitionsRDD(
    prev: RDD[ColumnarBatch],
    // (TaskContext, partition index, iterator)
    f: (TaskContext, Int, Iterator[ColumnarBatch]) => Iterator[InternalRow],
    preservesPartitioning: Boolean = false,
    isFromBarrier: Boolean = false,
    isOrderSensitive: Boolean = false)
  extends MapPartitionsRDD[InternalRow, ColumnarBatch](
    prev,
    f,
    preservesPartitioning,
    isFromBarrier,
    isOrderSensitive) {
}
