/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.closeOnExcept
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{splitTargetSizeInHalfCpu, withRetry}
import com.nvidia.spark.rapids.shims.{GpuTypeShims, ShimUnaryExecNode}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, SortOrder, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

private class GpuRowToColumnConverter(schema: StructType) extends Serializable {
  private val converters = schema.fields.map {
    f => GpuRowToColumnConverter.getConverterForType(f.dataType, f.nullable)
  }

  /**
   * Append row values to the column builders and return an approximation of the data bytes
   * written.  It is a Double because of validity can be less than a single byte.
   */
  final def convert(row: InternalRow, builders: GpuColumnarBatchBuilder): Double = {
    var bytes: Double = 0
    for (idx <- schema.fields.indices) {
      bytes += converters(idx).append(row, idx, builders.builder(idx))
    }
    bytes
  }

  /**
   * Convert an array of rows into a batch. Please note that this does not do bounds or size
   * checking so keep the size of the batch small.
   * @param rows the rows to convert.
   * @param schema the schema of the rows.
   * @return The batch on the GPU.
   */
  final def convertBatch(rows: Array[InternalRow], schema: StructType): ColumnarBatch = {
    val numRows = rows.length
    withResource(new GpuColumnarBatchBuilder(schema, numRows)) { builders =>
      rows.foreach(convert(_, builders))
      builders.build(numRows)
    }
  }
}

private[rapids] object GpuRowToColumnConverter {
  // Sizes estimates for different things
  /*
   * size of an offset entry.  In general we have 1 more offset entry than rows, so
   * we might be off by one entry per column.
   */
  private[this] val OFFSET = Integer.BYTES
  private[this] val VALIDITY = 0.125 // 1/8th of a byte (1 bit)
  private[this] val VALIDITY_N_OFFSET = OFFSET + VALIDITY

  private[rapids] abstract class TypeConverter extends Serializable {
    /** Append row value to the column builder and return the number of data bytes written */
    def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double

    /**
     * This is here for structs.  When you append a null to a struct the size is not known
     * ahead of time.  Also because structs push nulls down to the children this size should
     * assume a validity even if the schema says it cannot be null.
     */
    def getNullSize: Double
  }

  private def getConverterFor(field: StructField): TypeConverter =
    getConverterForType(field.dataType, field.nullable)

  private[rapids] def getConverterForType(dataType: DataType, nullable: Boolean): TypeConverter = {
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
      case (BinaryType, true) => BinaryConverter
      case (BinaryType, false) => NotNullBinaryConverter
      // NOT SUPPORTED YET
      // case CalendarIntervalType => CalendarConverter
      case (at: ArrayType, true) =>
        ArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case (at: ArrayType, false) =>
        NotNullArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case (st: StructType, true) =>
        StructConverter(st.fields.map(getConverterFor))
      case (st: StructType, false) =>
        NotNullStructConverter(st.fields.map(getConverterFor))
      case (dt: DecimalType, true) =>
        new DecimalConverter(dt.precision, dt.scale)
      case (dt: DecimalType, false) =>
        new NotNullDecimalConverter(dt.precision, dt.scale)
      case (MapType(k, v, vcn), true) =>
        MapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (MapType(k, v, vcn), false) =>
        NotNullMapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (NullType, true) =>
        NullConverter
      // check special Shims types, such as DayTimeIntervalType
      case (otherType, nullable) if GpuTypeShims.hasConverterForType(otherType) =>
        GpuTypeShims.getConverterForType(otherType, nullable)
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object NullConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      builder.appendNull()
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getByte(column))
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
      2 + VALIDITY
    }

    override def getNullSize: Double = 2 + VALIDITY
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getShort(column))
      2
    }

    override def getNullSize: Double = 2 + VALIDITY
  }

  private[rapids] object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private[rapids] object NotNullIntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getInt(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getFloat(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
  }

  private[rapids] object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private[rapids] object NotNullLongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getLong(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      builder.append(row.getDouble(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullStringConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bytes = row.getUTF8String(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object BinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullBinaryConverter.append(row, column, builder) + VALIDITY
      }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object NotNullBinaryConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      val bytes = row.getBinary(column)
      builder.appendByteList(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private[this] def mapConvert(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter,
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val m = row.getMap(column)
    val numElements = m.numElements()
    val srcKeys = m.keyArray()
    val srcValues = m.valueArray()
    val structBuilder = builder.getChild(0)
    val keyBuilder = structBuilder.getChild(0)
    val valueBuilder = structBuilder.getChild(1)
    for (i <- 0 until numElements) {
      ret += keyConverter.append(srcKeys, i, keyBuilder)
      ret += valueConverter.append(srcValues, i, valueBuilder)
      structBuilder.endStruct()
    }
    builder.endList()
    ret + OFFSET
  }

  private case class MapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        mapConvert(keyConverter, valueConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullMapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: RapidsHostColumnBuilder): Double =
      mapConvert(keyConverter, valueConverter, row, column, builder)

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  //  private object CalendarConverter extends FixedWidthTypeConverter {
  //    override def append(row: SpecializedGetters,
  //    column: Int,
  //    builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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

  private[this] def arrayConvert(
      childConverter: TypeConverter,
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val values = row.getArray(column)
    val numElements = values.numElements()
    val child = builder.getChild(0)
    for (i <- 0 until numElements) {
      ret += childConverter.append(values, i, child)
    }
    builder.endList()
    ret + OFFSET
  }

  private case class ArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        arrayConvert(childConverter, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: RapidsHostColumnBuilder): Double = {
      arrayConvert(childConverter, row, column, builder)
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private[this] def structConvert(
      childConverters: Array[TypeConverter],
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder) : Double = {
    var ret = 0.0
    val struct = row.getStruct(column, childConverters.length)
    for (i <- childConverters.indices) {
      ret += childConverters(i).append(struct, i, builder.getChild(i))
    }
    builder.endStruct()
    ret
  }

  private case class StructConverter(
      childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        childConverters.map(_.getNullSize).sum + VALIDITY
        // each child has to insert a null too, which is dependent on the child
      } else {
        structConvert(childConverters, row, column, builder) + VALIDITY
      }
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private case class NotNullStructConverter(
      childConverters: Array[TypeConverter]) extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: RapidsHostColumnBuilder): Double = {
      structConvert(childConverters, row, column, builder)
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private class DecimalConverter(
    precision: Int, scale: Int) extends NotNullDecimalConverter(precision, scale) {
    private val appendedSize = if (precision <= Decimal.MAX_LONG_DIGITS) {
      8 + VALIDITY
    } else {
      16 + VALIDITY
    }

    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        super.append(row, column, builder)
      }
      appendedSize
    }
  }

  private class NotNullDecimalConverter(precision: Int, scale: Int) extends TypeConverter {
    private val appendedSize = if (precision <= Decimal.MAX_INT_DIGITS) {
      4
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      8
    } else {
      16
    }

    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: RapidsHostColumnBuilder): Double = {
      val bigDecimal = row.getDecimal(column, precision, scale).toJavaBigDecimal
      builder.append(bigDecimal)
      appendedSize
    }

    override def getNullSize: Double = {
      appendedSize + VALIDITY
    }
  }
}

class RowToColumnarIterator(
    rowIter: Iterator[InternalRow],
    localSchema: StructType,
    localGoal: CoalesceSizeGoal,
    converters: GpuRowToColumnConverter,
    numInputRows: GpuMetric = NoopMetric,
    numOutputRows: GpuMetric = NoopMetric,
    numOutputBatches: GpuMetric = NoopMetric,
    streamTime: GpuMetric = NoopMetric,
    opTime: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] {

  private val targetSizeBytes = localGoal.targetSizeBytes
  private var targetRows = 0
  private var totalOutputBytes: Long = 0
  private var totalOutputRows: Long = 0
  private[this] val pending = new scala.collection.mutable.Queue[InternalRow]()

  override def hasNext: Boolean =  pending.nonEmpty || rowIter.hasNext

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    buildBatch()
  }

  // Attempt to allocate a single host buffer for the full batch of columns, retrying
  // with fewer rows if necessary.  Then make it spillable.
  // Returns of tuple of (actual rows, per-column-sizes, SpillableHostBuffer).
  private def allocBufWithRetry(rows : Int) : (Int, Array[Long], SpillableHostBuffer) = {
    val  targetRowCount = AutoCloseableTargetSize(rows, 1)
    withRetry(targetRowCount, splitTargetSizeInHalfCpu) { attempt =>
      val perColBytes = GpuBatchUtils.estimatePerColumnGpuMemory(localSchema, attempt.targetSize)
      closeOnExcept(HostAlloc.alloc(perColBytes.sum, true)) { hBuf =>
        (attempt.targetSize.toInt, perColBytes,
            SpillableHostBuffer(hBuf, hBuf.getLength, SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
              RapidsBufferCatalog.singleton))
      }
    }.next()
  }

  private def buildBatch(): ColumnarBatch = {
    withResource(new NvtxRange("RowToColumnar", NvtxColor.CYAN)) { _ =>
      val streamStart = System.nanoTime()
      // estimate the size of the first batch based on the schema
      if (targetRows == 0) {
        if (localSchema.fields.isEmpty) {
          // if there are no columns then we just default to a small number
          // of rows for the first batch
          targetRows = 1024
        } else {
          val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
          val sampleBytes = GpuBatchUtils.estimateGpuMemory(localSchema, sampleRows)
          targetRows = GpuBatchUtils.estimateRowCount(targetSizeBytes, sampleBytes, sampleRows)
        }
      }
      val (actualRows, perColumnBytes, sBuf) = allocBufWithRetry(targetRows)
      targetRows = actualRows

      withResource(new GpuColumnarBatchBuilder(localSchema, targetRows, sBuf,
        perColumnBytes)) { builders =>
        var rowCount = 0
        // Double because validity can be < 1 byte, and this is just an estimate anyways
        var byteCount: Double = 0
        var overWrite = false
        // read at least one row
        while (!overWrite && hasNext && (rowCount == 0 ||
            ((rowCount < targetRows) && (byteCount < targetSizeBytes)))) {
          val row = if (pending.nonEmpty) {
            pending.dequeue()
          } else {
            rowIter.next()
          }
          try {
            builders.checkpoint()
            val rowBytes = converters.convert(row, builders)
            byteCount += rowBytes
            rowCount += 1
          } catch {
            case _ : RapidsHostColumnOverflow => {
              // We overwrote the pre-allocated buffers.  Restore state and stop here if we can.
              builders.restore()
              // If this happens on the first row, we aren't going to succeed.  If we require
              // a single batch, it will fail below.
              // For now we will just retry these cases with growth re-enabled - we may run out
              // of memory though.
              if ((rowCount == 0) || (localGoal.isInstanceOf[RequireSingleBatchLike])) {
                builders.setAllowGrowth(true)
              } else {
                // We wrote some rows, so we can go on to building the batch
                overWrite = true
              }
              pending.enqueue(row)  // we need to try this row again
            }
            case e: Throwable => throw e
          }
        }

        // enforce RequireSingleBatch limit
        if (hasNext && localGoal.isInstanceOf[RequireSingleBatchLike]) {
          throw new IllegalStateException("A single batch is required for this operation." +
              " Please try increasing your partition count.")
        }

        streamTime += System.nanoTime() - streamStart

        // About to place data back on the GPU
        // note that TaskContext.get() can return null during unit testing so we wrap it in an
        // option here
        Option(TaskContext.get())
            .foreach(ctx => GpuSemaphore.acquireIfNecessary(ctx))

        val ret = withResource(new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN,
            opTime)) { _ =>
          RmmRapidsRetryIterator.withRetryNoSplit[ColumnarBatch] {
            builders.tryBuild(rowCount)
          }
        }
        numInputRows += rowCount
        numOutputRows += rowCount
        numOutputBatches += 1

        // refine the targetRows estimate based on the average of all batches processed so far
        totalOutputBytes += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
        totalOutputRows += rowCount
        if (totalOutputRows > 0 && totalOutputBytes > 0) {
          targetRows =
            GpuBatchUtils.estimateRowCount(targetSizeBytes, totalOutputBytes, totalOutputRows)
        }

        // The returned batch will be closed by the consumer of it
        ret
      }
    }
  }
}

object GeneratedInternalRowToCudfRowIterator extends Logging {
  def apply(input: Iterator[InternalRow],
      schema: Array[Attribute],
      goal: CoalesceSizeGoal,
      streamTime: GpuMetric,
      opTime: GpuMetric,
      numInputRows: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): InternalRowToColumnarBatchIterator = {
    val ctx = new CodegenContext
    // setup code generation context to use our custom row variable
    val internalRow = ctx.freshName("internalRow")
    ctx.currentVars = null
    ctx.INPUT_ROW = internalRow

    val generateUnsafeProj = GenerateUnsafeProjection.createCode(ctx,
      schema.zipWithIndex.map { case (attr, i) => BoundReference(i, attr.dataType, attr.nullable) }
    )

    val iterRef = ctx.addReferenceObj("iter", input, classOf[Iterator[UnsafeRow]].getName)
    val schemaRef = ctx.addReferenceObj("schema", schema,
      classOf[Array[Attribute]].getCanonicalName)
    val goalRef = ctx.addReferenceObj("goal", goal, classOf[CoalesceSizeGoal].getName)
    val streamTimeRef = ctx.addReferenceObj("streamTime", streamTime, classOf[GpuMetric].getName)
    val opTimeRef = ctx.addReferenceObj("opTime", opTime, classOf[GpuMetric].getName)
    val numInputRowsRef = ctx.addReferenceObj("numInputRows", numInputRows,
      classOf[GpuMetric].getName)
    val numOutputRowsRef = ctx.addReferenceObj("numOutputRows", numOutputRows,
      classOf[GpuMetric].getName)
    val numOutputBatchesRef = ctx.addReferenceObj("numOutputBatches", numOutputBatches,
      classOf[GpuMetric].getName)

    val rowBaseObj = ctx.freshName("rowBaseObj")
    val rowBaseOffset = ctx.freshName("rowBaseOffset")

    val sparkValidityOffset = UnsafeRow.calculateBitSetWidthInBytes(schema.length)
    // This needs to match what is in cudf and CudfUnsafeRow
    var cudfOffset = 0
    // scalastyle:off line.size.limit
    // The map will execute here because schema is an array. Not sure if there is a better way to
    // do this or not
    val copyData = schema.zipWithIndex.map { pair =>
      val attr = pair._1
      val colIndex = pair._2
      // This only works on fixed width types
      val length = DecimalUtil.getDataTypeSize(attr.dataType)
      cudfOffset = CudfUnsafeRow.alignOffset(cudfOffset, length)
      val ret = length match {
        case 1 => s"Platform.putByte(null, startAddress + $cudfOffset, Platform.getByte($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 2 => s"Platform.putShort(null, startAddress + $cudfOffset, Platform.getShort($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 4 => s"Platform.putInt(null, startAddress + $cudfOffset, Platform.getInt($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 8 => s"Platform.putLong(null, startAddress + $cudfOffset, Platform.getLong($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
//        case 16 => s"Platform.setDecimal(null, startAddress + $cudfOffset, getDecimal($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case _ => throw new IllegalStateException(s"$length  NOT SUPPORTED YET")
      }
      cudfOffset += length
      ret
    }

    val sparkValidityTmp = ctx.freshName("unsafeRowTmp")

    val cudfValidityStart = cudfOffset
    val cudfBitSetWidthInBytes = CudfUnsafeRow.calculateBitSetWidthInBytes(schema.length)

    val copyValidity = (0 until cudfBitSetWidthInBytes).map { cudfValidityByteIndex =>
      var ret = ""
      val byteOffsetInSparkLong = cudfValidityByteIndex % 8
      if (byteOffsetInSparkLong == 0) {
        // We need to get the validity data out of the unsafe row.
        ret += s"$sparkValidityTmp = Platform.getLong($rowBaseObj, $rowBaseOffset + $cudfValidityByteIndex);\n"
      }
      // Now write out the sub-part of it we care about, but converted to validity instead of isNull
      ret += s"Platform.putByte(null, startAddress + ${cudfValidityStart + cudfValidityByteIndex}, (byte)(0xFF & ~($sparkValidityTmp >> ${byteOffsetInSparkLong * 8})));"
      ret
    }

    // Each row is 64-bit aligned
    val rowLength = CudfUnsafeRow.alignOffset(cudfOffset + cudfBitSetWidthInBytes, 8)

    // First copy the data, validity we will copy afterwards

    val codeBody =
      s"""
         |public java.lang.Object generate(Object[] references) {
         |  return new SpecificInternalRowToColumnarBatchIterator(references);
         |}
         |
         |final class SpecificInternalRowToColumnarBatchIterator extends ${classOf[InternalRowToColumnarBatchIterator].getName} {
         |  private final org.apache.spark.sql.catalyst.expressions.UnsafeProjection unsafeProj;
         |
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificInternalRowToColumnarBatchIterator(Object[] references) {
         |    super(
         |      $iterRef,
         |      $schemaRef,
         |      $goalRef,
         |      $streamTimeRef,
         |      $opTimeRef,
         |      $numInputRowsRef,
         |      $numOutputRowsRef,
         |      $numOutputBatchesRef);
         |
         |      ${ctx.initMutableStates()}
         |  }
         |
         |  // Avoid virtual function calls by copying the data in a batch at a time instead
         |  // of a row at a time.
         |  @Override
         |  public int[] fillBatch(ai.rapids.cudf.HostMemoryBuffer dataBuffer,
         |      ai.rapids.cudf.HostMemoryBuffer offsetsBuffer,
         |      long dataLength, int numRows) {
         |    final long dataBaseAddress = dataBuffer.getAddress();
         |    final long endDataAddress = dataBaseAddress + dataLength;
         |
         |    int dataOffset = 0;
         |    int currentRow = 0;
         |    int offsetIndex = 0;
         |
         |    // If we are here we have at least one row to process, so don't bother checking yet
         |    boolean done = false;
         |    while (!done) {
         |      UnsafeRow row;
         |      if (pending != null) {
         |        row = pending;
         |        pending = null;
         |      } else {
         |        InternalRow $internalRow = (InternalRow) input.next();
         |        if ($internalRow instanceof UnsafeRow) {
         |          row = (UnsafeRow) $internalRow;
         |        } else {
         |          ${generateUnsafeProj.code}
         |          row = ${generateUnsafeProj.value};
         |        }
         |      }
         |      int numBytesUsedByRow = copyInto(row, dataBaseAddress + dataOffset, endDataAddress);
         |      offsetsBuffer.setInt(offsetIndex, dataOffset);
         |      offsetIndex += 4;
         |      if (numBytesUsedByRow < 0) {
         |        pending = row;
         |        done = true;
         |      } else {
         |        currentRow += 1;
         |        dataOffset += numBytesUsedByRow;
         |        done = !(currentRow < numRows &&
         |            dataOffset < dataLength &&
         |            input.hasNext());
         |      }
         |    }
         |    return new int[] {dataOffset, currentRow};
         |  }
         |
         |  private int copyInto(UnsafeRow input, long startAddress, long endAddress) {
         |    Object $rowBaseObj = input.getBaseObject();
         |    long $rowBaseOffset = input.getBaseOffset();
         |    ${copyData.mkString("\n")}
         |
         |    // validity
         |    long $sparkValidityTmp;
         |    ${copyValidity.mkString("\n")}
         |
         |    return $rowLength;
         |  }
         |
         |  ${ctx.declareAddedFunctions()}
         |}
       """.stripMargin
    // scalastyle:on line.size.limit

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${schema.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    clazz.generate(ctx.references.toArray).asInstanceOf[InternalRowToColumnarBatchIterator]
  }
}

/**
 * GPU version of row to columnar transition.
 */
case class GpuRowToColumnarExec(child: SparkPlan, goal: CoalesceSizeGoal)
  extends ShimUnaryExecNode with GpuExec {
  import GpuMetric._

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputBatching: CoalesceGoal = goal

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    TrampolineUtil.doExecuteBroadcast(child)
  }

  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    STREAM_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_STREAM_TIME),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS)
  )

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    // use local variables instead of class global variables to prevent the entire
    // object from having to be serialized
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val streamTime = gpuLongMetric(STREAM_TIME)
    val opTime = gpuLongMetric(OP_TIME)
    val localGoal = goal
    val rowBased = child.execute()

    // cache in a local to avoid serializing the plan
    val localSchema = schema

    // We can support upto 2^31 bytes per row. That is ~250M columns of 64-bit fixed-width data.
    // This number includes the 1-bit validity per column, but doesn't include padding.
    // We are being conservative by only allowing 100M columns until we feel the need to
    // increase this number. Spark by default limits codegen to 100 fields
    // "spark.sql.codegen.maxFields".
    if ((1 until 100000000).contains(output.length) &&
        CudfRowTransitions.areAllSupported(output)) {
      val localOutput = output
      rowBased.mapPartitions(rowIter => GeneratedInternalRowToCudfRowIterator(
        rowIter, localOutput.toArray, localGoal, streamTime, opTime,
        numInputRows, numOutputRows, numOutputBatches))
    } else {
      val converters = new GpuRowToColumnConverter(localSchema)
      rowBased.mapPartitions(rowIter => new RowToColumnarIterator(rowIter,
        localSchema, localGoal, converters,
        numInputRows, numOutputRows, numOutputBatches, streamTime, opTime))
    }
  }
}
