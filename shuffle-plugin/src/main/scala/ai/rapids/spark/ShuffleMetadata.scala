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

import java.nio.{ByteBuffer, ByteOrder}

import ai.rapids.cudf.{BufferType, DType, Table}
import ai.rapids.spark.format._
import com.google.flatbuffers.FlatBufferBuilder

import org.apache.spark.storage.{BlockId, ShuffleBlockId}

class DirectByteBufferFactory extends FlatBufferBuilder.ByteBufferFactory {
  override def newByteBuffer(capacity : Int) : ByteBuffer = {
    ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN)
  }
}

object ShuffleMetadata {

  val bbFactory = new DirectByteBufferFactory

  val r = scala.util.Random
  val contentMetaSize = 1000000

  def getShuffleMetadataResponse (isValid: Boolean, tables: Array[Table], waitPeriod: Long) : ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    if (isValid) {
      val tableOffsets = new Array[Int](tables.length)
      for (tableIndex <- tables.indices) {
        val table = tables(tableIndex)
        val numCols = table.getNumberOfColumns
        val dataMetaRefs= new Array[Int](numCols)
        val validityMetaRefs = new Array[Int](numCols)
        val offsetMetaRefs = new Array[Int](numCols)
        val colMetaRefs = new Array[Int](numCols)

        for (i <- 0 until numCols) {
          val columnVector = table.getColumn(i)
          val dType: DType = table.getColumn(i).getType

          val dataBuffer = columnVector.getDeviceBufferFor(BufferType.DATA)
          val validityBuffer = columnVector.getDeviceBufferFor(BufferType.VALIDITY)
          val offsetBuffer = columnVector.getDeviceBufferFor(BufferType.OFFSET)

          dataMetaRefs(i) = BufMeta.createBufMeta(fbb, r.nextInt(), dataBuffer.getLength)
          if (validityBuffer != null) {
            validityMetaRefs(i) = BufMeta.createBufMeta(fbb, r.nextInt(), validityBuffer.getLength)
          }
          if (dType == DType.STRING) {
            offsetMetaRefs(i) = BufMeta.createBufMeta(fbb, r.nextInt(), offsetBuffer.getLength)
          }
        }

        for (i <- 0 until numCols) {
          val nullCount: Long = table.getColumn(i).getNullCount
          val rowCount: Long = table.getRowCount
          val dType: DType = table.getColumn(i).getType

          ColumnMeta.startColumnMeta(fbb)
          ColumnMeta.addRowCount(fbb, rowCount)
          ColumnMeta.addNullCount(fbb, nullCount)
          ColumnMeta.addDType(fbb, dType.ordinal())
          ColumnMeta.addData(fbb, dataMetaRefs(i))
          ColumnMeta.addValidity(fbb, validityMetaRefs(i))
          if (dType == DType.STRING) {
            ColumnMeta.addOffsets(fbb, offsetMetaRefs(i))
          }
          colMetaRefs(i) = ColumnMeta.endColumnMeta(fbb)
        }

        val colOffsetForTable = TableMeta.createColumnMetasVector(fbb, colMetaRefs)
        val tableOffset = TableMeta.createTableMeta(fbb, colOffsetForTable, table.getRowCount)
        tableOffsets(tableIndex) = tableOffset
      }
      val tablesOffset = MetadataResponse.createTableMetaVector(fbb, tableOffsets)
      val finIndex = MetadataResponse.createMetadataResponse(fbb, tablesOffset, contentMetaSize, isValid, waitPeriod)
      fbb.finish(finIndex)
    } else {
      val finIndex = MetadataResponse.createMetadataResponse(fbb, 0, -1, false, waitPeriod)
      fbb.finish(finIndex)
    }
    fbb.dataBuffer()
  }

  def getShuffleMetadataRequest (responseTag: Long,
      blockIds : Array[BlockId]) : ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val blockIdOffsets = new Array[Int](blockIds.length)
    for (i <- blockIds.indices) {
      val shuffleBlockBatchId = blockIds(i).asInstanceOf[ShuffleBlockId]
      blockIdOffsets(i) = BlockIdMeta.createBlockIdMeta(fbb, shuffleBlockBatchId.shuffleId,
        shuffleBlockBatchId.mapId, shuffleBlockBatchId.reduceId, shuffleBlockBatchId.reduceId)
    }
    val blockIdOffset = MetadataRequest.createBlockIdsVector(fbb, blockIdOffsets)
    val finIndex = MetadataRequest.createMetadataRequest(fbb, responseTag, blockIdOffset)
    fbb.finish(finIndex)
    fbb.dataBuffer()
  }

  def materializeResponse(byteBuffer: ByteBuffer) : MetadataResponse = {
    MetadataResponse.getRootAsMetadataResponse(byteBuffer)
  }

  def printResponse(state: String, res: MetadataResponse): String = {
    val out = new StringBuilder
    out.append(s"----------------------- METADATA RESPONSE ${state} --------------------------\n")
    out.append(s"${res.tableMetaLength()} tables\n")
    out.append("------------------------------------------------------------------------------\n")
    for (tableIndex <- 0 until res.tableMetaLength()) {
      val tableMeta = res.tableMeta(tableIndex)
      out.append(s"table: $tableIndex [cols=${tableMeta.columnMetasLength}, rows=${tableMeta.rowCount}, content_size=${res.contentMetaSize}]\n")
      for (i <- 0 until tableMeta.columnMetasLength()) {
        val columnMeta = tableMeta.columnMetas(i)
        if (SerializedDType.names(columnMeta.dType()) == "STRING") {
          val offsetLenStr = columnMeta.offsets().len().toString
          out.append(s"column: $i [rows=${columnMeta.rowCount}, data_len=${columnMeta.data().len()}, offset_len=${offsetLenStr}, " +
            s"validity_len=${columnMeta.validity().len()}, type=${SerializedDType.names(columnMeta.dType())}, " +
            s"null_count=${columnMeta.nullCount()}, " +
            s"data_tag=NA, validity_tag=NA, offset_tag=${columnMeta.offsets().tag()}]\n")
        } else {
          val offsetLenStr = "NC"
          out.append(s"column: $i [rows=${columnMeta.rowCount}, data_len=${columnMeta.data().len()}, offset_len=${offsetLenStr}, " +
            s"validity_len=${columnMeta.validity().len()}, type=${SerializedDType.names(columnMeta.dType())}, " +
            s"null_count=${columnMeta.nullCount()}, " +
            s"data_tag=${columnMeta.data().tag()}, validity_tag=${columnMeta.validity().tag}]\n")
        }
      }
    }
    out.append(s"----------------------- END METADATA RESPONSE ${state} ----------------------\n")
    out.toString()
  }

  def materializeRequest(byteBuffer: ByteBuffer) : MetadataRequest = {
    val req = MetadataRequest.getRootAsMetadataRequest(byteBuffer)
    req
  }

  def printRequest(req: MetadataRequest): String = {
    val out = new StringBuilder
    out.append("----------------------- METADATA REQUEST --------------------------\n")
    out.append(s"blockId length: ${req.blockIdsLength()}\n")
    for (i <- 0 until req.blockIdsLength()) {
      out.append(s"block_id = [shuffle_id=${req.blockIds(i).shuffleId()}, " +
        s"map_id=${req.blockIds(i).mapId()}, start_reduce_id=${req.blockIds(i).startReduceId()} , " +
        s"end_reduce_id=${req.blockIds(i).endReduceId()}]\n")
    }
    out.append("----------------------- END METADATA REQUEST ----------------------\n")
    out.toString()
  }
}
