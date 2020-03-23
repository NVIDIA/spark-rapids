/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION. All rights reserved.
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

package org.apache.spark.sql.execution

import ai.rapids.cudf.Table
import ai.rapids.spark.{CoalesceGoal, GpuColumnVector, GpuOverrides, GpuSemaphore, RapidsConf, TargetSize}
import ai.rapids.spark.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._

/**
 * This code is based off of the code for [[ai.rapids.spark.GpuRowToColumnarExec]] but this is for
 * Row instead of InternalRow, so it is not an exact transition.
 */
private class GpuExternalRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuExternalRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: Row, builders: GpuColumnarBatchBuilder): Unit = {
    for (idx <- 0 until row.length) {
      converters(idx).append(row, idx, builders.builder(idx))
    }
  }
}

private object GpuExternalRowToColumnConverter {
  private abstract class TypeConverter extends Serializable {
    def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit
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
      // NOT SUPPORTED YET case at: ArrayType => new ArrayConverter(getConverterForType(at.elementType))
      // NOT SUPPORTED YET case st: StructType => new StructConverter(st.fields.map(
      //  (f) => getConverterForType(f.dataType)))
      // NOT SUPPORTED YET case dt: DecimalType => new DecimalConverter(dt)
      // NOT SUPPORTED YET case mt: MapType => new MapConverter(getConverterForType(mt.keyType),
      //  getConverterForType(mt.valueType))
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
  }

  private object NotNullIntConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getInt(column))
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getFloat(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
  }

  private object NotNullLongConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullStringConverter.append(row, column, builder)
      }
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: Row, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.appendUTF8String(row.getString(column).getBytes)
  }
  //
  //  private object CalendarConverter extends TypeConverter {
  //    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  //    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  //  private case class StructConverter(childConverters: Array[TypeConverter]) extends TypeConverter {
  //    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  //    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  //      extends TypeConverter {
  //    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  private val targetRows = localGoal.targetSizeRows.toInt
  override def hasNext: Boolean = rowIter.hasNext

  override def next(): ColumnarBatch = {
    if (!rowIter.hasNext) {
      throw new NoSuchElementException
    }
    buildBatch()
  }

  private def buildBatch(): ColumnarBatch = {
    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows, null)
    try {
      var rowCount = 0
      while (rowCount < targetRows && rowIter.hasNext) {
        val row = rowIter.next()
        converters.convert(row, builders)
        rowCount += 1
      }
      if (rowIter.hasNext && (rowCount + 1L) > localGoal.targetSizeRows) {
        localGoal.whenTargetExceeded(rowCount + 1L)
      }

      // About to place data back on the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val ret = builders.build(rowCount)
      // The returned batch will be closed by the consumer of it
      ret
    } finally {
      builders.close()
    }
  }
}

/**
 * Please don't use this class directly use [[ai.rapids.spark.ColumnarRdd]] instead. We had to
 * place the implementation in a spark specific package to poke at the internals of spark more than
 * anyone should know about.
 *
 * This provides a way to get back out GPU Columnar data RDD[Table]. Each Table will have the same
 * schema as the dataframe passed in.  If the schema of the dataframe is something that Rapids does
 * not currently support an [[IllegalArgumentException]] will be thrown.
 *
 * The size of each table will be determined by what is producing that table but typically will be
 * about the number of rows set by [[RapidsConf.GPU_BATCH_SIZE_ROWS]].
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
      val unsupported = schema.map(_.dataType).filter(!GpuOverrides.areAllSupportedTypes(_)).toSet
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
    // 3) MapPartitionsRDD - which is converting an InternalRow[Whatever] to InternalRow[ObjectType[Row]]
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
                    logDebug(s"Cannot extract columnar RDD directly. (column to row not found $rdd)")
                }
              case rdd =>
                logDebug(s"Cannot extract columnar RDD directly. (Internal row to row rdd not found $rdd)")
            }
          case rdd =>
            logDebug(s"Cannot extract columnar RDD directly. (SQLExecutionRDD not found $rdd)")
        }
      case rdd =>
        logDebug(s"Cannot extract columnar RDD directly. (First MapPartitionsRDD not found $rdd)")
    }

    val b = batch.getOrElse({
      // We have to fall back to doing a slow transition.
      val converters = new GpuExternalRowToColumnConverter(schema)
      val conf = new RapidsConf(df.sqlContext.conf)
      val goal = TargetSize(conf.gpuTargetBatchSizeRows.toInt, conf.gpuTargetBatchSizeBytes)
      input.mapPartitions(rowIter => new ExternalRowToColumnarIterator(rowIter, schema, goal, converters))
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
    f: (TaskContext, Int, Iterator[ColumnarBatch]) => Iterator[InternalRow],  // (TaskContext, partition index, iterator)
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