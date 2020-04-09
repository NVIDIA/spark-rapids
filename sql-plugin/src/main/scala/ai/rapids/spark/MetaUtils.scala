/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.spark.format.{BufferMeta, CodecType, ColumnMeta, SubBufferMeta, TableMeta}
import com.google.flatbuffers.FlatBufferBuilder

object MetaUtils {
  /**
   * Build a TableMeta message from a Table in contiguous memory
   *
   * @param tableId the ID to use for this table
   * @param table the table whose metadata will be encoded in the message
   * @param buffer the contiguous buffer backing the Table
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(tableId: Int, table: Table, buffer: DeviceMemoryBuffer): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    val baseAddress = buffer.getAddress
    val bufferSize = buffer.getLength
    val bufferMetaOffset = BufferMeta.createBufferMeta(
      fbb,
      tableId,
      bufferSize,
      bufferSize,
      CodecType.UNCOMPRESSED)

    val columnMetaOffsets = new ArrayBuffer[Int](table.getNumberOfColumns)
    for (i <- 0 until table.getNumberOfColumns) {
      columnMetaOffsets.append(addColumnMeta(fbb, baseAddress, table.getColumn(i)))
    }

    val columnMetasOffset = TableMeta.createColumnMetasVector(fbb, columnMetaOffsets.toArray)
    TableMeta.startTableMeta(fbb)
    TableMeta.addBufferMeta(fbb, bufferMetaOffset)
    TableMeta.addRowCount(fbb, table.getRowCount)
    TableMeta.addColumnMetas(fbb, columnMetasOffset)
    fbb.finish(TableMeta.endTableMeta(fbb))
    // copy the message to trim the backing array to only what is needed
    TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
  }

  /** Add a ColumnMeta, returning the buffer offset where it was added */
  private def addColumnMeta(
      fbb: FlatBufferBuilder,
      baseAddress: Long,
      column: ColumnVector): Int = {
    ColumnMeta.startColumnMeta(fbb)
    ColumnMeta.addNullCount(fbb, column.getNullCount)
    ColumnMeta.addRowCount(fbb, column.getRowCount)
    val data = column.getDeviceBufferFor(BufferType.DATA)
    if (data != null) {
      ColumnMeta.addData(fbb, addSubBuffer(fbb, baseAddress, data))
    }
    val validity = column.getDeviceBufferFor(BufferType.VALIDITY)
    if (validity != null) {
      ColumnMeta.addValidity(fbb, addSubBuffer(fbb, baseAddress, validity))
    }
    val offsets = column.getDeviceBufferFor(BufferType.OFFSET)
    if (offsets != null) {
      ColumnMeta.addOffsets(fbb, addSubBuffer(fbb, baseAddress, offsets))
    }
    ColumnMeta.addDtype(fbb, column.getType.getNativeId)
    ColumnMeta.endColumnMeta(fbb)
  }

  /** Add a SubBuffer, returning the buffer offset where it was added */
  private def addSubBuffer(
      fbb: FlatBufferBuilder,
      baseAddress: Long,
      buffer: BaseDeviceMemoryBuffer): Int = {
    SubBufferMeta.createSubBufferMeta(fbb, buffer.getAddress - baseAddress, buffer.getLength)
  }
}
