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

import ai.rapids.cudf.NvtxColor
import ai.rapids.spark.GpuColumnVector.GpuColumnarBatchBuilder
import ai.rapids.spark.GpuMetricNames._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, TrampolineUtil, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.TaskContext

private class GpuRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  final def convert(row: InternalRow, builders: GpuColumnarBatchBuilder): Unit = {
    for (idx <- 0 until row.numFields) {
      converters(idx).append(row, idx, builders.builder(idx))
    }
  }
}

private object GpuRowToColumnConverter {
  private abstract class TypeConverter extends Serializable {
    def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit
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
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getByte(column))
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getShort(column))
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
  }

  private object NotNullIntConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getInt(column))
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getFloat(column))
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
  }

  private object NotNullLongConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getLong(column))
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.append(row.getDouble(column))
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullStringConverter.append(row, column, builder)
      }
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: SpecializedGetters, column: Int, builder: ai.rapids.cudf.HostColumnVector.Builder): Unit =
      builder.appendUTF8String(row.getUTF8String(column).getBytes)
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

class RowToColumnarIterator(
    rowIter: Iterator[InternalRow],
    localSchema: StructType,
    localGoal: CoalesceGoal,
    converters: GpuRowToColumnConverter,
    totalTime: SQLMetric,
    numInputRows: SQLMetric,
    numOutputRows: SQLMetric,
    numOutputBatches: SQLMetric) extends Iterator[ColumnarBatch] {
  val targetRows = localGoal.targetSize.toInt
  override def hasNext: Boolean = rowIter.hasNext

  override def next(): ColumnarBatch = {
    if (!rowIter.hasNext) {
      throw new NoSuchElementException
    }
    buildBatch()
  }

  private def buildBatch(): ColumnarBatch = {
    // TODO eventually we should be smarter about allocating memory for these batches
    // so we can support building large batches with all of the data.
    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows, null)
    try {
      var rowCount = 0
      while (rowCount < targetRows && rowIter.hasNext) {
        val row = rowIter.next()
        converters.convert(row, builders)
        rowCount += 1
      }
      if (rowIter.hasNext && (rowCount + 1L) > localGoal.targetSize) {
        localGoal.whenTargetExceeded(rowCount + 1L)
      }

      // About to place data back on the GPU
      GpuSemaphore.acquireIfNecessary(TaskContext.get())

      val buildRange = new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN, totalTime)
      val ret = try {
        builders.build(rowCount)
      } finally {
        buildRange.close()
      }
      numInputRows += rowCount
      numOutputRows += rowCount
      numOutputBatches += 1
      // The returned batch will be closed by the consumer of it
      ret
    } finally {
      builders.close()
    }
  }
}

/**
 * GPU version of row to columnar transition.
 */
case class GpuRowToColumnarExec(child: SparkPlan, goal: CoalesceGoal)
  extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    TrampolineUtil.doExecuteBroadcast(child)
  }

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    "numInputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // TODO need to add in the GPU accelerated unsafe row translation when there is no
    // variable length operations.

    // use local variables instead of class global variables to prevent the entire
    // object from having to be serialized
    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val totalTime = longMetric(TOTAL_TIME)
    val localSchema = schema
    val converters = new GpuRowToColumnConverter(localSchema)
    val localGoal = goal
    val rowBased = child.execute()
    rowBased.mapPartitions(rowIter => new RowToColumnarIterator(rowIter,
      localSchema, localGoal, converters,
      totalTime, numInputRows, numOutputRows, numOutputBatches))
  }
}
