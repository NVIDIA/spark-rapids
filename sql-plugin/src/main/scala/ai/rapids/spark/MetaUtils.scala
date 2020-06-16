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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import ai.rapids.spark.format.{BlockIdMeta, BufferMeta, BufferTransferRequest, BufferTransferResponse, CodecType, ColumnMeta, MetadataRequest, MetadataResponse, SubBufferMeta, TableMeta, TransferRequest, TransferResponse, TransferState}
import com.google.flatbuffers.FlatBufferBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockBatchId

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

  /**
   * Build a TableMeta message for a degenerate table (zero columns or rows)
   * @param tableId the ID to use for this table
   * @param batch the degenerate columnar batch
   * @return heap-based flatbuffer message
   */
  def buildDegenerateTableMeta(tableId: Int, batch: ColumnarBatch): TableMeta = {
    require(batch.numRows == 0 || batch.numCols == 0, "batch not degenerate")
    val fbb = new FlatBufferBuilder(1024)
    val columnMetaOffset = if (batch.numCols > 0) {
      val columns = GpuColumnVector.extractBases(batch)
      val columnMetaOffsets = new ArrayBuffer[Int](batch.numCols)
      for (i <- 0 until batch.numCols) {
        ColumnMeta.startColumnMeta(fbb)
        ColumnMeta.addNullCount(fbb, 0)
        ColumnMeta.addRowCount(fbb, batch.numRows)
        ColumnMeta.addDtype(fbb, columns(i).getType.getNativeId)
        columnMetaOffsets.append(ColumnMeta.endColumnMeta(fbb))
      }
      Some(TableMeta.createColumnMetasVector(fbb, columnMetaOffsets.toArray))
    } else {
      None
    }

    TableMeta.startTableMeta(fbb)
    TableMeta.addRowCount(fbb, batch.numRows)
    columnMetaOffset.foreach(c => TableMeta.addColumnMetas(fbb, c))
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

class DirectByteBufferFactory extends FlatBufferBuilder.ByteBufferFactory {
  override def newByteBuffer(capacity : Int) : ByteBuffer = {
    ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN)
  }
}

object ShuffleMetadata extends Logging{

  val bbFactory = new DirectByteBufferFactory

  /**
    * Given a sequence of `TableMeta`, re-lay the metas using the flat buffer builder in `fbb`.
    * @param fbb builder to use
    * @param tables sequence of `TableMeta` to copy
    * @return an array of flat buffer offsets for the copied `TableMeta`s
    */
  def copyTables(fbb: FlatBufferBuilder, tables: Seq[TableMeta]): Array[Int] = {
    tables.map { tableMeta =>
      val buffMeta = tableMeta.bufferMeta()

      val buffMetaOffset = if (buffMeta != null) {
        Some(BufferMeta.createBufferMeta(fbb, buffMeta.id(), buffMeta.actualSize(),
          buffMeta.compressedSize(), buffMeta.codec()))
      } else {
        None
      }

      val columnMetaOffsets = (0 until tableMeta.columnMetasLength()).map { c =>
        val col = tableMeta.columnMetas(c)
        val data = col.data()
        val validity = col.validity()
        val offsets = col.offsets()
        var dataOffset, validityOffset, offsetsOffset = -1

        ColumnMeta.startColumnMeta(fbb)
        ColumnMeta.addNullCount(fbb, col.nullCount())
        ColumnMeta.addRowCount(fbb, col.rowCount())
        if (data != null) {
          dataOffset = SubBufferMeta.createSubBufferMeta(fbb, data.offset(), data.length())
          ColumnMeta.addData(fbb, dataOffset)
        }
        if (validity != null) {
          validityOffset = SubBufferMeta.createSubBufferMeta(fbb, validity.offset(),
              validity.length())
          ColumnMeta.addValidity(fbb, validityOffset)
        }

        if (offsets != null) {
          offsetsOffset = SubBufferMeta.createSubBufferMeta(fbb, offsets.offset(),
              offsets.length())
          ColumnMeta.addOffsets(fbb, offsetsOffset)
        }
        ColumnMeta.addDtype(fbb, col.dtype())
        ColumnMeta.endColumnMeta(fbb)
      }

      val columnMetaOffset = if (columnMetaOffsets.nonEmpty) {
        Some(TableMeta.createColumnMetasVector(fbb, columnMetaOffsets.toArray))
      } else {
        None
      }

      TableMeta.startTableMeta(fbb)
      if (buffMetaOffset.isDefined) {
        TableMeta.addBufferMeta(fbb, buffMetaOffset.get)
      }
      if (columnMetaOffset.isDefined) {
        TableMeta.addColumnMetas(fbb, columnMetaOffset.get)
      }
      TableMeta.addRowCount(fbb, tableMeta.rowCount())
      TableMeta.endTableMeta(fbb)
    }.toArray
  }

  def buildMetaResponse(tables: Seq[TableMeta], maximumResponseSize: Long): ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val tableOffsets = copyTables(fbb, tables)
    val tableMetasOffset = MetadataResponse.createTableMetasVector(fbb, tableOffsets)
    val finIndex = MetadataResponse.createMetadataResponse(fbb, 0, tableMetasOffset)
    fbb.finish(finIndex)
    val bb = fbb.dataBuffer()
    val responseSize = bb.remaining()
    if (responseSize > maximumResponseSize) {
      throw new IllegalStateException("response size is bigger than what receiver wants")
    }
    val materializedResponse = ShuffleMetadata.getMetadataResponse(bb)
    materializedResponse.mutateFullResponseSize(responseSize)
    bb
  }

  def buildShuffleMetadataRequest(executorId: Long,
                                  responseTag: Long,
                                  blockIds : Seq[ShuffleBlockBatchId],
                                  maxResponseSize: Long) : ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val blockIdOffsets = blockIds.map { case blockId =>
      BlockIdMeta.createBlockIdMeta(fbb,
        blockId.shuffleId,
        blockId.mapId,
        blockId.startReduceId,
        blockId.endReduceId)
    }
    val blockIdVectorOffset = MetadataRequest.createBlockIdsVector(fbb, blockIdOffsets.toArray)
    val finIndex = MetadataRequest.createMetadataRequest(fbb, executorId, responseTag,
      maxResponseSize, blockIdVectorOffset)
    fbb.finish(finIndex)
    fbb.dataBuffer()
  }

  def getBuilder = new FlatBufferBuilder(1024, bbFactory)

  def getHeapBuilder = new FlatBufferBuilder(1024)

  def getMetadataRequest(byteBuffer: ByteBuffer): MetadataRequest = {
    MetadataRequest.getRootAsMetadataRequest(byteBuffer)
  }

  def getMetadataResponse(byteBuffer: ByteBuffer): MetadataResponse = {
    MetadataResponse.getRootAsMetadataResponse(byteBuffer)
  }

  def getTransferResponse(resp: ByteBuffer): TransferResponse = {
    TransferResponse.getRootAsTransferResponse(resp)
  }

  def getTransferRequest(transferRequest: ByteBuffer): TransferRequest = {
    TransferRequest.getRootAsTransferRequest(transferRequest)
  }

  def buildBufferTransferRequest(fbb: FlatBufferBuilder, bufferId: Int, tag: Long): Int = {
    BufferTransferRequest.createBufferTransferRequest(fbb, bufferId, tag)
  }

  def buildBufferTransferResponse(bufferMetas: Seq[BufferMeta]): ByteBuffer = {
    val fbb = new FlatBufferBuilder(1024, bbFactory)
    val responses = bufferMetas.map { bm =>
      val buffMetaOffset = BufferMeta.createBufferMeta(fbb, bm.id(), bm.actualSize(),
          bm.compressedSize(), bm.codec())
      BufferTransferResponse.createBufferTransferResponse(fbb, bm.id(), TransferState.STARTED,
          buffMetaOffset)
    }.toArray
    val responsesVector = TransferResponse.createResponsesVector(fbb, responses)
    val root = TransferResponse.createTransferResponse(fbb, responsesVector)
    fbb.finish(root)
    fbb.dataBuffer()
  }

  def buildTransferRequest(
      localExecutorId: Long,
      responseTag: Long,
      toIssue: Seq[(TableMeta, Long)]): ByteBuffer = {
    val fbb = ShuffleMetadata.getBuilder
    val requestIds = new ArrayBuffer[Int](toIssue.size)
    toIssue.foreach { case (tableMeta, tag) =>
      requestIds.append(
        ShuffleMetadata.buildBufferTransferRequest(
          fbb,
          tableMeta.bufferMeta().id(),
          tag))
    }
    val requestVec = TransferRequest.createRequestsVector(fbb, requestIds.toArray)
    val transferRequestOffset = TransferRequest.createTransferRequest(fbb, localExecutorId,
        responseTag, requestVec)
    fbb.finish(transferRequestOffset)
    fbb.dataBuffer()
  }

  def printResponse(state: String, res: MetadataResponse): String = {
    val out = new StringBuilder
    out.append(s"----------------------- METADATA RESPONSE ${state} --------------------------\n")
    out.append(s"${res.tableMetasLength()} tables\n")
    out.append("------------------------------------------------------------------------------\n")
    for (tableIndex <- 0 until res.tableMetasLength()) {
      val tableMeta = res.tableMetas(tableIndex)
      out.append(s"table: $tableIndex [cols=${tableMeta.columnMetasLength()}, " +
          s"rows=${tableMeta.rowCount}, full_content_size=${res.fullResponseSize()}]\n")
      for (i <- 0 until tableMeta.columnMetasLength()) {
        val columnMeta = tableMeta.columnMetas(i)

        val validityLen = if (columnMeta.validity() == null) {
          -1
        } else {
          columnMeta.validity().length()
        }

        // TODO: Need to expose native ID in cudf
        if (DType.STRING == DType.fromNative(columnMeta.dtype())) {
          val offsetLenStr = columnMeta.offsets().length().toString
          val validityLen = if (columnMeta.validity() == null) {
            -1
          } else {
            columnMeta.validity().length()
          }
          out.append(s"column: $i [rows=${columnMeta.rowCount}, " +
              s"data_len=${columnMeta.data().length()}, offset_len=${offsetLenStr}, " +
              s"validity_len=$validityLen, type=${DType.fromNative(columnMeta.dtype())}, " +
              s"null_count=${columnMeta.nullCount()}]\n")
        } else {
          val offsetLenStr = "NC"

          val validityLen = if (columnMeta.validity() == null) {
            -1
          } else {
            columnMeta.validity().length()
          }

          out.append(s"column: $i [rows=${columnMeta.rowCount}, " +
              s"data_len=${columnMeta.data().length()}, offset_len=${offsetLenStr}, " +
              s"validity_len=$validityLen, type=${DType.fromNative(columnMeta.dtype())}, " +
              s"null_count=${columnMeta.nullCount()}]\n")
        }
      }
    }
    out.append(s"----------------------- END METADATA RESPONSE ${state} ----------------------\n")
    out.toString()
  }


  def printRequest(req: MetadataRequest): String = {
    val out = new StringBuilder
    out.append("----------------------- METADATA REQUEST --------------------------\n")
    out.append(s"blockId length: ${req.blockIdsLength()}\n")
    for (i <- 0 until req.blockIdsLength()) {
      out.append(s"block_id = [shuffle_id=${req.blockIds(i).shuffleId()}, " +
          s"map_id=${req.blockIds(i).mapId()}, " +
          s"start_reduce_id=${req.blockIds(i).startReduceId()} , " +
          s"end_reduce_id=${req.blockIds(i).endReduceId()}]\n")
    }
    out.append("----------------------- END METADATA REQUEST ----------------------\n")
    out.toString()
  }


  /**
    * Utility function to transfer a `TableMeta` to the heap,
    * @todo we would like to look for an easier way, perhaps just a memcpy will do.
    */
  def copyTableMetaToHeap(meta: TableMeta): TableMeta = {
    val fbb = ShuffleMetadata.getHeapBuilder
    val tables = ShuffleMetadata.copyTables(fbb, Seq(meta))
    fbb.finish(tables.head)
    TableMeta.getRootAsTableMeta(fbb.dataBuffer())
  }
}
