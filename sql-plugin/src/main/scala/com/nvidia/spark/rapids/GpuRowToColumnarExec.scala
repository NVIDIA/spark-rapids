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

package com.nvidia.spark.rapids

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder
import com.nvidia.spark.rapids.GpuMetricNames._

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CudfUnsafeRow, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    for (idx <- 0 until row.numFields) {
      bytes += converters(idx).append(row, idx, builders.builder(idx))
    }
    bytes
  }
}

private object GpuRowToColumnConverter {
  // Sizes estimates for different things
  /*
   * size of an offset entry.  In general we have 1 more offset entry than rows, so
   * we might be off by one entry per column.
   */
  private[this] val OFFSET = Integer.BYTES
  private[this] val VALIDITY = 0.125 // 1/8th of a byte (1 bit)
  private[this] val VALIDITY_N_OFFSET = OFFSET + VALIDITY

  private abstract class TypeConverter extends Serializable {
    /** Append row value to the column builder and return the number of data bytes written */
    def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double
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
      case (dt: DecimalType, true) => new DecimalConverter(dt.precision, dt.scale)
      case (dt: DecimalType, false) => new NotNullDecimalConverter(dt.precision, dt.scale)
      // NOT SUPPORTED YET
      // case CalendarIntervalType => CalendarConverter
      case (at: ArrayType, true) =>
        ArrayConverter(getConverterForType(at.elementType, at.containsNull))
      case (at: ArrayType, false) =>
        NotNullArrayConverter(getConverterForType(at.elementType, at.containsNull))
      // NOT SUPPORTED YET
      // case st: StructType => new StructConverter(st.fields.map(
      // (f) => getConverterForType(f.dataType)))
      //       NOT SUPPORTED YET
      case (MapType(k, v, vcn), true) =>
        MapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (MapType(k, v, vcn), false) =>
        NotNullMapConverter(getConverterForType(k, nullable = false),
          getConverterForType(v, vcn))
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object BooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullBooleanConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
      1
    }
  }

  private object ByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullByteConverter.append(row, column, builder)
      }
      1 + VALIDITY
    }
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getByte(column))
      1
    }
  }

  private object ShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullShortConverter.append(row, column, builder)
      }
      2 + VALIDITY
    }
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getShort(column))
      2
    }
  }

  private object IntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullIntConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }
  }

  private object NotNullIntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getInt(column))
      4
    }
  }

  private object FloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullFloatConverter.append(row, column, builder)
      }
      4 + VALIDITY
    }
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getFloat(column))
      4
    }
  }

  private object LongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullLongConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }
  }

  private object NotNullLongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getLong(column))
      8
    }
  }

  private object DoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        NotNullDoubleConverter.append(row, column, builder)
      }
      8 + VALIDITY
    }
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getDouble(column))
      8
    }
  }

  private object StringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double =
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        NotNullStringConverter.append(row, column, builder) + VALIDITY
      }
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      val bytes = row.getUTF8String(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length + OFFSET
    }
  }

  private class DecimalConverter(precision: Int, scale: Int) extends TypeConverter {
    override def append(
        row: SpecializedGetters,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        new NotNullDecimalConverter(precision, scale).append(row, column, builder)
      }
      // Infer the storage type via precision, because we can't access DType of builder.
      (if (precision > ai.rapids.cudf.DType.DECIMAL32_MAX_PRECISION) 8 else 4) + VALIDITY
    }
  }

  private class NotNullDecimalConverter(precision: Int, scale: Int) extends TypeConverter {
    override def append(
        row: SpecializedGetters,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getDecimal(column, precision, scale).toJavaBigDecimal)
      // Infer the storage type via precision, because we can't access DType of builder.
      if (precision > ai.rapids.cudf.DType.DECIMAL32_MAX_PRECISION) 8 else 4
    }
  }

  private[this] def mapConvert(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter,
      row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder) : Double = {
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
        column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        mapConvert(keyConverter, valueConverter, row, column, builder) + VALIDITY
      }
    }
  }

  private case class NotNullMapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double =
      mapConvert(keyConverter, valueConverter, row, column, builder)
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
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder) : Double = {
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
        column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
        VALIDITY_N_OFFSET
      } else {
        arrayConvert(childConverter, row, column, builder) + VALIDITY
      }
    }
  }

  private case class NotNullArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      arrayConvert(childConverter, row, column, builder)
    }
  }
  //
  //  private case class StructConverter(
  //      childConverters: Array[TypeConverter]) extends TypeConverter {
  //    override def append(row: SpecializedGetters,
  //    column: Int,
  //    builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
  //    override def append(row: SpecializedGetters,
  //    column: Int,
  //    builder: ai.rapids.cudf.HostColumnVector.Builder): Unit = {
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
}

class RowToColumnarIterator(
    rowIter: Iterator[InternalRow],
    localSchema: StructType,
    localGoal: CoalesceGoal,
    converters: GpuRowToColumnConverter,
    totalTime: SQLMetric = null,
    numInputRows: SQLMetric = null,
    numOutputRows: SQLMetric = null,
    numOutputBatches: SQLMetric = null) extends Iterator[ColumnarBatch] {

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
        // if there are no columns then we just default to a small number
        // of rows for the first batch
        targetRows = 1024
      } else {
        val sampleRows = GpuBatchUtils.VALIDITY_BUFFER_BOUNDARY_ROWS
        val sampleBytes = GpuBatchUtils.estimateGpuMemory(localSchema, sampleRows)
        targetRows = GpuBatchUtils.estimateRowCount(targetSizeBytes, sampleBytes, sampleRows)
      }
    }

    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows, null)
    try {
      var rowCount = 0
      // Double because validity can be < 1 byte, and this is just an estimate anyways
      var byteCount: Double = 0
      // read at least one row
      while (rowIter.hasNext &&
        (rowCount == 0 || rowCount < targetRows && byteCount < targetSizeBytes)) {
        val row = rowIter.next()
        byteCount += converters.convert(row, builders)
        rowCount += 1
      }

      // enforce RequireSingleBatch limit
      if (rowIter.hasNext && localGoal == RequireSingleBatch) {
        throw new IllegalStateException("A single batch is required for this operation." +
          " Please try increasing your partition count.")
      }

      // About to place data back on the GPU
      // note that TaskContext.get() can return null during unit testing so we wrap it in an
      // option here
      Option(TaskContext.get()).foreach(GpuSemaphore.acquireIfNecessary)

      var buildRange: NvtxRange = null
      if (totalTime != null) {
        buildRange = new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN, totalTime)
      } else {
        buildRange = new NvtxRange("RowToColumnar", NvtxColor.GREEN)
      }
      val ret = try {
        builders.build(rowCount)
      } finally {
        buildRange.close()
      }
      if (numInputRows != null) {
        numInputRows += rowCount
      }
      if (numOutputRows != null) {
        numOutputRows += rowCount
      }
      if (numOutputBatches != null) {
        numOutputBatches += 1
      }

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

object GeneratedUnsafeRowToCudfRowIterator extends Logging {
  def apply(input: Iterator[UnsafeRow],
      schema: Array[Attribute],
      goal: CoalesceGoal,
      totalTime: SQLMetric,
      numInputRows: SQLMetric,
      numOutputRows: SQLMetric,
      numOutputBatches: SQLMetric): UnsafeRowToColumnarBatchIterator = {
    val ctx = new CodegenContext

    ctx.addReferenceObj("iter", input, classOf[Iterator[UnsafeRow]].getName)
    ctx.addReferenceObj("schema", schema, classOf[Array[Attribute]].getName)
    ctx.addReferenceObj("goal", goal, classOf[CoalesceGoal].getName)
    ctx.addReferenceObj("totalTime", totalTime, classOf[SQLMetric].getName)
    ctx.addReferenceObj("numInputRows", numInputRows, classOf[SQLMetric].getName)
    ctx.addReferenceObj("numOutputRows", numOutputRows, classOf[SQLMetric].getName)
    ctx.addReferenceObj("numOutputBatches", numOutputBatches, classOf[SQLMetric].getName)

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
      val length = attr.dataType.defaultSize
      cudfOffset = CudfUnsafeRow.alignOffset(cudfOffset, length)
      val ret = length match {
        case 1 => s"Platform.putByte(null, startAddress + $cudfOffset, Platform.getByte($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 2 => s"Platform.putShort(null, startAddress + $cudfOffset, Platform.getShort($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 4 => s"Platform.putInt(null, startAddress + $cudfOffset, Platform.getInt($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
        case 8 => s"Platform.putLong(null, startAddress + $cudfOffset, Platform.getLong($rowBaseObj, $rowBaseOffset + ${sparkValidityOffset + (colIndex * 8)}));"
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
         |  return new SpecificUnsafeRowToColumnarBatchIterator(references);
         |}
         |
         |final class SpecificUnsafeRowToColumnarBatchIterator extends ${classOf[UnsafeRowToColumnarBatchIterator].getName} {
         |
         |  ${ctx.declareMutableStates()}
         |
         |  public SpecificUnsafeRowToColumnarBatchIterator(Object[] references) {
         |    super((scala.collection.Iterator<UnsafeRow>)references[0],
         |      (org.apache.spark.sql.catalyst.expressions.Attribute[])references[1],
         |      (com.nvidia.spark.rapids.CoalesceGoal)references[2],
         |      (org.apache.spark.sql.execution.metric.SQLMetric)references[3],
         |      (org.apache.spark.sql.execution.metric.SQLMetric)references[4],
         |      (org.apache.spark.sql.execution.metric.SQLMetric)references[5],
         |      (org.apache.spark.sql.execution.metric.SQLMetric)references[6]);
         |    ${ctx.initMutableStates()}
         |  }
         |
         |  // Avoid virtual function calls by copying the data in a batch at a time instead
         |  // of a row at a time.
         |  @Override
         |  public int[] fillBatch(ai.rapids.cudf.HostMemoryBuffer dataBuffer,
         |      ai.rapids.cudf.HostMemoryBuffer offsetsBuffer) {
         |    final long dataBaseAddress = dataBuffer.getAddress();
         |    final long endDataAddress = dataBaseAddress + dataLength;
         |
         |    int dataOffset = 0;
         |    int currentRow = 0;
         |
         |    offsetsBuffer.setInt(0, dataOffset);
         |    // If we are here we have at least one row to process, so don't bother checking yet
         |    boolean done = false;
         |    while (!done) {
         |      UnsafeRow row;
         |      if (pending != null) {
         |        row = pending;
         |        pending = null;
         |      } else {
         |        row = (UnsafeRow)input.next();
         |      }
         |      int numBytesUsedByRow = copyInto(row, dataBaseAddress + dataOffset, endDataAddress);
         |      if (numBytesUsedByRow < 0) {
         |        pending = row;
         |        done = true;
         |      } else {
         |        currentRow += 1;
         |        dataOffset += numBytesUsedByRow;
         |        done = !(currentRow < numRowsEstimate &&
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
    clazz.generate(ctx.references.toArray).asInstanceOf[UnsafeRowToColumnarBatchIterator]
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

  override def outputBatching: CoalesceGoal = goal

  override def doExecute(): RDD[InternalRow] = {
    child.execute()
  }

  override def doExecuteBroadcast[T](): Broadcast[T] = {
    TrampolineUtil.doExecuteBroadcast(child)
  }

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    NUM_INPUT_ROWS -> SQLMetrics.createMetric(sparkContext, DESCRIPTION_NUM_INPUT_ROWS)
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // use local variables instead of class global variables to prevent the entire
    // object from having to be serialized
    val numInputRows = longMetric(NUM_INPUT_ROWS)
    val numOutputBatches = longMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = longMetric(NUM_OUTPUT_ROWS)
    val totalTime = longMetric(TOTAL_TIME)
    val localGoal = goal
    val rowBased = child.execute()

    // cache in a local to avoid serializing the plan
    val localSchema = schema

    // The cudf kernel only supports up to 1.5 KB per row which means at most 184 double/long
    // values. Spark by default limits codegen to 100 fields "spark.sql.codegen.maxFields".
    // So, we are going to be cautious and start with that until we have tested it more.
    if (output.length > 0 && output.length < 100 &&
        CudfRowTransitions.areAllSupported(output)) {
      val localOutput = output
      rowBased.mapPartitions(rowIter => GeneratedUnsafeRowToCudfRowIterator(
        rowIter.asInstanceOf[Iterator[UnsafeRow]],
        localOutput.toArray, localGoal, totalTime, numInputRows, numOutputRows,
        numOutputBatches))
    } else {
      val converters = new GpuRowToColumnConverter(localSchema)
      rowBased.mapPartitions(rowIter => new RowToColumnarIterator(rowIter,
        localSchema, localGoal, converters,
        totalTime, numInputRows, numOutputRows, numOutputBatches))
    }
  }
}
