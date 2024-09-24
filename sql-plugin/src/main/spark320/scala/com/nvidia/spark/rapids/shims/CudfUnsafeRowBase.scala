/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION.
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
/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "350db"}
{"spark": "351"}
{"spark": "352"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.shims

import java.util.Arrays

import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SpecializedGettersReader
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.unsafe.types.UTF8String

abstract class CudfUnsafeRowBase(
   protected val attributes: Array[Attribute],
   protected val remapping: Array[Int]) extends InternalRow {
  protected var address: Long = _
  private var startOffsets: Array[Int] = _
  private var fixedWidthSizeInBytes: Int = _
  protected var sizeInBytes: Int = _

  def this() = this(null, null)

  init(attributes, remapping)

  private def init(attributes: Array[Attribute], remapping: Array[Int]): Unit = {
    var offset = 0
    startOffsets = new Array[Int](attributes.length)
    for (i <- attributes.indices) {
      val attr = attributes(i)
      val length = GpuColumnVector.getNonNestedRapidsType(attr.dataType).getSizeInBytes
      assert(length > 0, "Only fixed width types are currently supported.")
      offset = CudfUnsafeRow.alignOffset(offset, length)
      startOffsets(i) = offset
      offset += length
    }
    fixedWidthSizeInBytes = offset
    assert(startOffsets.length == remapping.length)
  }

  override def numFields: Int = startOffsets.length

  def pointTo(address: Long, sizeInBytes: Int): Unit = {
    assert(startOffsets != null && startOffsets.length > 0, "startOffsets not properly initialized")
    assert(sizeInBytes % 8 == 0, s"sizeInBytes ($sizeInBytes) should be a multiple of 8")
    this.address = address
    this.sizeInBytes = sizeInBytes
  }

  override def update(ordinal: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def get(ordinal: Int, dataType: DataType): Object = {
    SpecializedGettersReader.read(this, ordinal, dataType, true, true)
  }

  override def isNullAt(ordinal: Int): Boolean = {
    val i = remapping(ordinal)
    assertIndexIsValid(i)
    val validByteIndex = i / 8
    val validBitIndex = i % 8
    val b = Platform.getByte(null, address + fixedWidthSizeInBytes + validByteIndex)
    ((1 << validBitIndex) & b) == 0
  }

  override def setNullAt(ordinal: Int): Unit = {
    val i = remapping(ordinal)
    assertIndexIsValid(i)
    val validByteIndex = i / 8
    val validBitIndex = i % 8
    var b = Platform.getByte(null, address + fixedWidthSizeInBytes + validByteIndex)
    b = (b & ~(1 << validBitIndex)).toByte
    Platform.putByte(null, address + fixedWidthSizeInBytes + validByteIndex, b)
  }

  override def getBoolean(ordinal: Int): Boolean = {
    Platform.getBoolean(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getByte(ordinal: Int): Byte = {
    Platform.getByte(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getShort(ordinal: Int): Short = {
    Platform.getShort(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getInt(ordinal: Int): Int = {
    Platform.getInt(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getLong(ordinal: Int): Long = {
    Platform.getLong(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getFloat(ordinal: Int): Float = {
    Platform.getFloat(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getDouble(ordinal: Int): Double = {
    Platform.getDouble(null, getFieldAddressFromOrdinal(ordinal))
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    if (isNullAt(ordinal)) {
      null
    } else if (precision <= Decimal.MAX_INT_DIGITS) {
      Decimal.createUnsafe(getInt(ordinal), precision, scale)
    } else if (precision <= Decimal.MAX_LONG_DIGITS) {
      Decimal.createUnsafe(getLong(ordinal), precision, scale)
    } else {
      throw new IllegalArgumentException("NOT IMPLEMENTED YET")
    }
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def getStruct(ordinal: Int, numFields: Int): CudfUnsafeRow = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def getArray(ordinal: Int): ArrayData = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def getMap(ordinal: Int): MapData = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def copy(): CudfUnsafeRow = {
    throw new IllegalArgumentException("NOT IMPLEMENTED YET")
  }

  override def hashCode(): Int = {
    Murmur3_x86_32.hashUnsafeWords(null, address, sizeInBytes, 42)
  }

  override def equals(other: Any): Boolean = other match {
    case o: CudfUnsafeRow =>
      sizeInBytes == o.sizeInBytes &&
        ByteArrayMethods.arrayEquals(null, address, null, o.address, sizeInBytes) &&
        Arrays.equals(this.remapping, o.remapping)
    case _ => false
  }

  override def toString: String = {
    val build = new StringBuilder("[")
    for (i <- 0 until sizeInBytes by 8) {
      if (i != 0) build.append(',')
      build.append(java.lang.Long.toHexString(Platform.getLong(null, address + i)))
    }
    build.append(']')
    build.append(" remapped with ")
    build.append(Arrays.toString(remapping))
    build.toString()
  }

  override def anyNull(): Boolean = throw new IllegalArgumentException("NOT IMPLEMENTED YET")

  private def getFieldAddressFromOrdinal(ordinal: Int): Long = {
    assertIndexIsValid(ordinal)
    val i = remapping(ordinal)
    address + startOffsets(i)
  }

  private def assertIndexIsValid(index: Int): Unit = {
    assert(index >= 0, s"index ($index) should >= 0")
    assert(index < startOffsets.length, s"index ($index) should < ${startOffsets.length}")
  }
}

trait CudfUnsafeRowTrait {
  def alignOffset(offset: Int, alignment: Int): Int = (offset + alignment - 1) & -alignment

  def calculateBitSetWidthInBytes(numFields: Int): Int = (numFields + 7) / 8

  def getRowSizeEstimate(attributes: Array[Attribute]): Int = {
    var offset = 0
    for (attr <- attributes) {
      val length = GpuColumnVector.getNonNestedRapidsType(attr.dataType).getSizeInBytes
      offset = alignOffset(offset, length)
      offset += length
    }
    val bitSetWidthInBytes = calculateBitSetWidthInBytes(attributes.length)
    alignOffset(offset + bitSetWidthInBytes, 8)
  }
}
