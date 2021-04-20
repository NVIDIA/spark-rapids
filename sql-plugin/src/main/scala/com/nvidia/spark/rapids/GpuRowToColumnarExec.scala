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

import ai.rapids.cudf.NvtxColor
import com.nvidia.spark.rapids.GpuColumnVector.GpuColumnarBatchBuilder

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CudfUnsafeRow, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, SpecializedGetters, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

private class GpuRowToColumnConverter(schema: StructType) extends Serializable with Arm {
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

  /**
   * Convert an array of rows into a batch. Please note that this does not do bounds or size
   * checking so keep the size of the batch small.
   * @param rows the rows to convert.
   * @param schema the schema of the rows.
   * @return The batch on the GPU.
   */
  final def convertBatch(rows: Array[InternalRow], schema: StructType): ColumnarBatch = {
    val numRows = rows.length
    val builders = new GpuColumnarBatchBuilder(schema, numRows)
    rows.foreach(convert(_, builders))
    builders.build(numRows)
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

    /**
     * This is here for structs.  When you append a null to a struct the size is not known
     * ahead of time.  Also because structs push nulls down to the children this size should
     * assume a validity even if the schema says it cannot be null.
     */
    def getNullSize: Double
  }

  private def getConverterFor(field: StructField): TypeConverter =
    getConverterForType(field.dataType, field.nullable)

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
      case (unknown, _) => throw new UnsupportedOperationException(
        s"Type $unknown not supported")
    }
  }

  private object NullConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int,
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.appendNull()
      1 + VALIDITY
    }

    override def getNullSize: Double = 1 + VALIDITY
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

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullBooleanConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(if (row.getBoolean(column)) 1.toByte else 0.toByte)
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
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

    override def getNullSize: Double = 1 + VALIDITY
  }

  private object NotNullByteConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getByte(column))
      1
    }

    override def getNullSize: Double = 1 + VALIDITY
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

    override def getNullSize: Double = 2 + VALIDITY
  }

  private object NotNullShortConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getShort(column))
      2
    }

    override def getNullSize: Double = 2 + VALIDITY
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

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object NotNullIntConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getInt(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
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

    override def getNullSize: Double = 4 + VALIDITY
  }

  private object NotNullFloatConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getFloat(column))
      4
    }

    override def getNullSize: Double = 4 + VALIDITY
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

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object NotNullLongConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getLong(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
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

    override def getNullSize: Double = 8 + VALIDITY
  }

  private object NotNullDoubleConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getDouble(column))
      8
    }

    override def getNullSize: Double = 8 + VALIDITY
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

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private object NotNullStringConverter extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      val bytes = row.getUTF8String(column).getBytes
      builder.appendUTF8String(bytes)
      bytes.length + OFFSET
    }

    override def getNullSize: Double = OFFSET + VALIDITY
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

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullMapConverter(
      keyConverter: TypeConverter,
      valueConverter: TypeConverter) extends TypeConverter {
    override def append(row: SpecializedGetters,
      column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double =
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

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private case class NotNullArrayConverter(childConverter: TypeConverter)
      extends TypeConverter {
    override def append(row: SpecializedGetters,
        column: Int, builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      arrayConvert(childConverter, row, column, builder)
    }

    override def getNullSize: Double = OFFSET + VALIDITY
  }

  private[this] def structConvert(
      childConverters: Array[TypeConverter],
      row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder) : Double = {
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
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
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
        builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      structConvert(childConverters, row, column, builder)
    }

    override def getNullSize: Double = childConverters.map(_.getNullSize).sum + VALIDITY
  }

  private class DecimalConverter(
    precision: Int, scale: Int) extends NotNullDecimalConverter(precision, scale) {
    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      if (row.isNullAt(column)) {
        builder.appendNull()
      } else {
        super.append(row, column, builder)
      }
      8 + VALIDITY
    }
  }

  private class NotNullDecimalConverter(precision: Int, scale: Int) extends TypeConverter {
    override def append(
      row: SpecializedGetters,
      column: Int,
      builder: ai.rapids.cudf.HostColumnVector.ColumnBuilder): Double = {
      builder.append(row.getDecimal(column, precision, scale).toJavaBigDecimal)
      // We are basing our DType.DECIMAL on precision in GpuColumnVector#toRapidsOrNull so we can
      // safely assume the underlying vector is Int if precision < 10 otherwise Long
      if (precision <= Decimal.MAX_INT_DIGITS) {
        4
      } else {
        8
      }
    }

    override def getNullSize: Double = {
      if (precision <= Decimal.MAX_INT_DIGITS) {
        4
      } else {
        8
      } + VALIDITY
    }
  }
}

class RowToColumnarIterator(
    rowIter: Iterator[InternalRow],
    localSchema: StructType,
    localGoal: CoalesceGoal,
    converters: GpuRowToColumnConverter,
    totalTime: GpuMetric = NoopMetric,
    numInputRows: GpuMetric = NoopMetric,
    numOutputRows: GpuMetric = NoopMetric,
    numOutputBatches: GpuMetric = NoopMetric) extends Iterator[ColumnarBatch] with Arm {

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

    val builders = new GpuColumnarBatchBuilder(localSchema, targetRows)
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

      val ret = withResource(new NvtxWithMetrics("RowToColumnar", NvtxColor.GREEN, totalTime)) { _=>
        builders.build(rowCount)
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
    } finally {
      builders.close()
    }
  }
}

object GeneratedUnsafeRowToCudfRowIterator extends Logging {
  def apply(input: Iterator[UnsafeRow],
      schema: Array[Attribute],
      goal: CoalesceGoal,
      totalTime: GpuMetric,
      numInputRows: GpuMetric,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): UnsafeRowToColumnarBatchIterator = {
    val ctx = new CodegenContext

    ctx.addReferenceObj("iter", input, classOf[Iterator[UnsafeRow]].getName)
    ctx.addReferenceObj("schema", schema, classOf[Array[Attribute]].getName)
    ctx.addReferenceObj("goal", goal, classOf[CoalesceGoal].getName)
    ctx.addReferenceObj("totalTime", totalTime, classOf[GpuMetric].getName)
    ctx.addReferenceObj("numInputRows", numInputRows, classOf[GpuMetric].getName)
    ctx.addReferenceObj("numOutputRows", numOutputRows, classOf[GpuMetric].getName)
    ctx.addReferenceObj("numOutputBatches", numOutputBatches, classOf[GpuMetric].getName)

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
         |      (com.nvidia.spark.rapids.GpuMetric)references[3],
         |      (com.nvidia.spark.rapids.GpuMetric)references[4],
         |      (com.nvidia.spark.rapids.GpuMetric)references[5],
         |      (com.nvidia.spark.rapids.GpuMetric)references[6]);
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
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS)
  )

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // use local variables instead of class global variables to prevent the entire
    // object from having to be serialized
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val totalTime = gpuLongMetric(TOTAL_TIME)
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
