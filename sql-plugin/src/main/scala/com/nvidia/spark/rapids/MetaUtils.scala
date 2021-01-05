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

package com.nvidia.spark.rapids

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf._
import com.google.flatbuffers.FlatBufferBuilder
import com.nvidia.spark.rapids.format._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.ShuffleBlockBatchId

object MetaUtils extends Arm {
  // Used in cases where the `tableId` is not known at meta creation. 
  // We pick a non-zero integer since FlatBuffers prevent mutating the 
  // field if originally set to the default value as specified in the 
  // FlatBuffer (0 by default).
  val TableIdDefaultValue: Int = -1

  /**
   * Build a TableMeta message from a Table in contiguous memory
   *
   * @param tableId the ID to use for this table
   * @param table the table whose metadata will be encoded in the message
   * @param buffer the contiguous buffer backing the Table
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(tableId: Int, table: Table, buffer: DeviceMemoryBuffer): TableMeta = {
    buildTableMeta(tableId,
      (0 until table.getNumberOfColumns).map(i => table.getColumn(i)),
      table.getRowCount,
      buffer)
  }

  /**
   * Build a TableMeta message
   * @param tableId the ID to use for this table
   * @param columns the columns in the table
   * @param numRows the number of rows in the table
   * @param buffer the contiguous buffer backing the columns in the table
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(
      tableId: Int,
      columns: Seq[ColumnVector],
      numRows: Long,
      buffer: DeviceMemoryBuffer): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    val bufferSize = buffer.getLength
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, tableId)
    BufferMeta.addSize(fbb, bufferSize)
    BufferMeta.addUncompressedSize(fbb, bufferSize)
    val bufferMetaOffset = BufferMeta.endBufferMeta(fbb)
    buildTableMeta(fbb, bufferMetaOffset, columns, numRows, buffer.getAddress)
  }

  /**
   * Build a TableMeta message from a Table that originated in contiguous memory that has
   * since been compressed.
   * @param tableId ID to use for this table
   * @param table table whose metadata will be encoded in the message
   * @param uncompressedBuffer uncompressed buffer that backs the Table
   * @param codecId identifier of the codec being used, see CodecType
   * @param compressedSize compressed data from the uncompressed buffer
   * @return heap-based flatbuffer message
   */
  def buildTableMeta(
      tableId: Option[Int],
      table: Table,
      uncompressedBuffer: DeviceMemoryBuffer,
      codecId: Byte,
      compressedSize: Long): TableMeta = {
    val fbb = new FlatBufferBuilder(1024)
    val codecDescrOffset = CodecBufferDescriptor.createCodecBufferDescriptor(
      fbb,
      codecId,
      0,
      compressedSize,
      0,
      uncompressedBuffer.getLength)
    val codecDescrArrayOffset =
      BufferMeta.createCodecBufferDescrsVector(fbb, Array(codecDescrOffset))
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, tableId.getOrElse(MetaUtils.TableIdDefaultValue))
    BufferMeta.addSize(fbb, compressedSize)
    BufferMeta.addUncompressedSize(fbb, uncompressedBuffer.getLength)
    BufferMeta.addCodecBufferDescrs(fbb, codecDescrArrayOffset)
    val bufferMetaOffset = BufferMeta.endBufferMeta(fbb)
    val columns = (0 until table.getNumberOfColumns).map(i => table.getColumn(i))
    buildTableMeta(fbb, bufferMetaOffset, columns, table.getRowCount, uncompressedBuffer.getAddress)
  }

  /**
   * Build a TableMeta message with a pre-built BufferMeta message
   * @param fbb flatbuffer builder that has an already built BufferMeta message
   * @param bufferMetaOffset offset where the BufferMeta message was built
   * @param columns the columns in the table
   * @param numRows the number of rows in the table
   * @param baseAddress address of uncompressed contiguous buffer holding the table
   * @return flatbuffer message
   */
  def buildTableMeta(
      fbb: FlatBufferBuilder,
      bufferMetaOffset: Int,
      columns: Seq[ColumnVector],
      numRows: Long,
      baseAddress: Long): TableMeta = {
    val columnMetaOffsets = columns.map(col => addColumnMeta(fbb, baseAddress, col)).toArray
    val columnMetasOffset = TableMeta.createColumnMetasVector(fbb, columnMetaOffsets)
    TableMeta.startTableMeta(fbb)
    TableMeta.addBufferMeta(fbb, bufferMetaOffset)
    TableMeta.addRowCount(fbb, numRows)
    TableMeta.addColumnMetas(fbb, columnMetasOffset)
    fbb.finish(TableMeta.endTableMeta(fbb))
    // copy the message to trim the backing array to only what is needed
    TableMeta.getRootAsTableMeta(ByteBuffer.wrap(fbb.sizedByteArray()))
  }

  /**
   * Build a TableMeta message for a degenerate table (zero columns or rows)
   * @param batch the degenerate columnar batch
   * @return heap-based flatbuffer message
   */
  def buildDegenerateTableMeta(batch: ColumnarBatch): TableMeta = {
    require(batch.numRows == 0 || batch.numCols == 0, "batch not degenerate")
    if (batch.numCols > 0) {
      // Batched compression can result in degenerate batches appearing compressed. In that case
      // the table metadata has already been computed and can be returned directly.
      batch.column(0) match {
        case c: GpuCompressedColumnVector =>
          return c.getTableMeta
        case _ =>
      }
    }
    val fbb = new FlatBufferBuilder(1024)
    val columnMetaOffset = if (batch.numCols > 0) {
      val columns = GpuColumnVector.extractBases(batch)
      val columnMetaOffsets = new ArrayBuffer[Int](batch.numCols)
      for (i <- 0 until batch.numCols) {
        ColumnMeta.startColumnMeta(fbb)
        ColumnMeta.addNullCount(fbb, 0)
        ColumnMeta.addRowCount(fbb, batch.numRows)
        val columnType = columns(i).getType
        ColumnMeta.addDtypeId(fbb, columnType.getTypeId.getNativeId)
        ColumnMeta.addDtypeScale(fbb, columnType.getScale)
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
      column: ColumnView): Int = {
    val columnType = column.getType
    val childVectorOffset = if (columnType.isNestedType) {
      val childMetaOffsets = (0 until column.getNumChildren).map { i =>
        withResource(column.getChildColumnView(i)) { childView =>
          addColumnMeta(fbb, baseAddress, childView)
        }
      }
      Some(ColumnMeta.createChildrenVector(fbb, childMetaOffsets.toArray))
    } else {
      None
    }

    ColumnMeta.startColumnMeta(fbb)
    ColumnMeta.addNullCount(fbb, column.getNullCount)
    ColumnMeta.addRowCount(fbb, column.getRowCount)
    val data = column.getData
    if (data != null) {
      ColumnMeta.addDataOffset(fbb, data.getAddress - baseAddress)
      ColumnMeta.addDataLength(fbb, data.getLength)
    }
    val validity = column.getValid
    if (validity != null) {
      ColumnMeta.addValidityOffset(fbb, validity.getAddress - baseAddress)
    }
    val offsets = column.getOffsets
    if (offsets != null) {
      ColumnMeta.addOffsetsOffset(fbb, offsets.getAddress - baseAddress)
    }
    ColumnMeta.addDtypeId(fbb, columnType.getTypeId.getNativeId)
    ColumnMeta.addDtypeScale(fbb, columnType.getScale)
    childVectorOffset.foreach(x => ColumnMeta.addChildren(fbb, x))
    ColumnMeta.endColumnMeta(fbb)
  }

  /**
   * Construct a table from a contiguous device buffer and a
   * `TableMeta` message describing the schema of the buffer data.
   * @param deviceBuffer contiguous buffer
   * @param meta schema metadata
   * @return table that must be closed by the caller
   */
  def getTableFromMeta(deviceBuffer: DeviceMemoryBuffer, meta: TableMeta): Table = {
    withResource(new Array[ColumnVector](meta.columnMetasLength())) { columns =>
      val columnMeta = new ColumnMeta
      (0 until meta.columnMetasLength).foreach { i =>
        columns(i) = makeCudfColumn(deviceBuffer, meta.columnMetas(columnMeta, i))
      }
      new Table(columns :_*)
    }
  }

  /**
   * Construct a columnar batch from a contiguous device buffer and a
   * `TableMeta` message describing the schema of the buffer data.
   * @param deviceBuffer contiguous buffer
   * @param meta schema metadata
   * @param sparkTypes the spark types that the `ColumnarBatch` should have.
   * @return columnar batch that must be closed by the caller
   */
  def getBatchFromMeta(deviceBuffer: DeviceMemoryBuffer,
      meta: TableMeta,
      sparkTypes: Array[DataType]): ColumnarBatch = {
    closeOnExcept(new ArrayBuffer[GpuColumnVector](meta.columnMetasLength())) { columns =>
      val columnMeta = new ColumnMeta
      (0 until meta.columnMetasLength).foreach { i =>
        columns.append(makeColumn(deviceBuffer, meta.columnMetas(columnMeta, i), sparkTypes(i)))
      }
      new ColumnarBatch(columns.toArray, meta.rowCount.toInt)
    }
  }

  private def makeColumn(
      buffer: DeviceMemoryBuffer,
      meta: ColumnMeta,
      sparkType: DataType): GpuColumnVector = {
    val columnView = makeCudfColumnView(buffer, meta)
    val column = ColumnViewUtil.fromViewWithContiguousAllocation(columnView, buffer)
    new GpuColumnVectorFromBuffer(sparkType, column, buffer)
  }

  private def makeCudfColumn(buffer: DeviceMemoryBuffer, meta: ColumnMeta): ColumnVector = {
    val columnView = makeCudfColumnView(buffer, meta)
    ColumnViewUtil.fromViewWithContiguousAllocation(columnView, buffer)
  }

  private def makeCudfColumnView(
      buffer: DeviceMemoryBuffer,
      meta: ColumnMeta): Long = {
    val numChildren = meta.childrenLength()
    val childViews = if (numChildren > 0) new Array[Long](numChildren) else null
    try {
      if (childViews != null) {
        val columnMetaObj = new ColumnMeta
        childViews.indices.foreach { i =>
          val childMeta = meta.children(columnMetaObj, i)
          childViews(i) = makeCudfColumnView(buffer, childMeta)
        }
      }

      val dtype = DType.fromNative(meta.dtypeId(), meta.dtypeScale())
      val rowCount = meta.rowCount().toInt
      val nullCount = meta.nullCount().toInt
      val baseAddress = buffer.getAddress
      val dataLength = meta.dataLength()
      val dataAddress = if (dtype.isNestedType) {
        0L
      } else {
        baseAddress + meta.dataOffset()
      }
      val validityAddress = if (nullCount > 0) {
        baseAddress + meta.validityOffset()
      } else {
        0L
      }
      val offsetsAddress = if (dtype.hasOffsets) {
        baseAddress + meta.offsetsOffset()
      } else {
        0L
      }
      ColumnViewUtil.makeCudfColumnView(
        dtype.getTypeId.getNativeId,
        dtype.getScale,
        dataAddress,
        dataLength,
        offsetsAddress,
        validityAddress,
        nullCount,
        rowCount,
        childViews)
    } catch {
      case t: Throwable =>
        if (childViews != null) {
          try {
            childViews.foreach(ColumnViewUtil.deleteColumnView)
          } catch {
            case e: Throwable =>
              t.addSuppressed(e)
          }
        }
        throw t
    }
  }
}

class DirectByteBufferFactory extends FlatBufferBuilder.ByteBufferFactory {
  override def newByteBuffer(capacity : Int) : ByteBuffer = {
    ByteBuffer.allocateDirect(capacity).order(ByteOrder.LITTLE_ENDIAN)
  }
}

object ShuffleMetadata extends Logging{

  val bbFactory = new DirectByteBufferFactory

  private def copyBufferMeta(fbb: FlatBufferBuilder, buffMeta: BufferMeta): Int = {
    val descrOffsets = (0 until buffMeta.codecBufferDescrsLength()).map { i =>
      val descr = buffMeta.codecBufferDescrs(i)
      CodecBufferDescriptor.createCodecBufferDescriptor(
        fbb,
        descr.codec,
        descr.compressedOffset,
        descr.compressedSize,
        descr.uncompressedOffset,
        descr.uncompressedSize)
    }
    val codecDescrArrayOffset = if (descrOffsets.nonEmpty) {
      Some(BufferMeta.createCodecBufferDescrsVector(fbb, descrOffsets.toArray))
    } else {
      None
    }
    BufferMeta.startBufferMeta(fbb)
    BufferMeta.addId(fbb, buffMeta.id)
    BufferMeta.addSize(fbb, buffMeta.size)
    BufferMeta.addUncompressedSize(fbb, buffMeta.uncompressedSize)
    codecDescrArrayOffset.foreach(off => BufferMeta.addCodecBufferDescrs(fbb, off))
    BufferMeta.endBufferMeta(fbb)
  }

  private def copyColumnMeta(fbb: FlatBufferBuilder, col: ColumnMeta): Int = {
    val numChildren = col.childrenLength()
    val childMetaVectorOffset = if (numChildren > 0) {
      val columnMetaObj = new ColumnMeta
      val childMetaOffsets = (0 until numChildren).map { i =>
        val childMeta = col.children(columnMetaObj, i)
        copyColumnMeta(fbb, childMeta)
      }
      Some(ColumnMeta.createChildrenVector(fbb, childMetaOffsets.toArray))
    } else {
      None
    }

    ColumnMeta.startColumnMeta(fbb)
    ColumnMeta.addNullCount(fbb, col.nullCount())
    ColumnMeta.addRowCount(fbb, col.rowCount())
    ColumnMeta.addDataOffset(fbb, col.dataOffset())
    ColumnMeta.addDataLength(fbb, col.dataLength())
    ColumnMeta.addValidityOffset(fbb, col.validityOffset())
    ColumnMeta.addOffsetsOffset(fbb, col.offsetsOffset())
    childMetaVectorOffset.foreach(x => ColumnMeta.addChildren(fbb, x))
    ColumnMeta.addDtypeId(fbb, col.dtypeId())
    ColumnMeta.addDtypeScale(fbb, col.dtypeScale())
    ColumnMeta.endColumnMeta(fbb)
  }

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
        Some(copyBufferMeta(fbb, buffMeta))
      } else {
        None
      }

      val columnMetaObj = new ColumnMeta
      val columnMetaOffsets = (0 until tableMeta.columnMetasLength()).map { i =>
        val columnMeta = tableMeta.columnMetas(columnMetaObj, i)
        copyColumnMeta(fbb, columnMeta)
      }

      val columnMetaOffset = if (columnMetaOffsets.nonEmpty) {
        Some(TableMeta.createColumnMetasVector(fbb, columnMetaOffsets.toArray))
      } else {
        None
      }

      TableMeta.startTableMeta(fbb)
      buffMetaOffset.foreach(bmo => TableMeta.addBufferMeta(fbb, bmo))
      columnMetaOffset.foreach(cmo => TableMeta.addColumnMetas(fbb, cmo))
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
    val blockIdOffsets = blockIds.map { blockId =>
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
      val buffMetaOffset = copyBufferMeta(fbb, bm)
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

  private def printColumnMeta(colName: String, columnMeta: ColumnMeta): String = {
    val columnType = DType.fromNative(columnMeta.dtypeId(), columnMeta.dtypeScale())
    val numChildren = columnMeta.childrenLength()
    val childrenStr = if (numChildren > 0) {
      val columnMetaObj = new ColumnMeta
      (0 until numChildren).map { i =>
        val childName = s"$colName child $i"
        val childMeta = columnMeta.children(columnMetaObj, i)
        printColumnMeta(childName, childMeta)
      }.mkString
    } else {
      ""
    }
    s"$colName [type=$columnType rows=${columnMeta.rowCount} " +
        s"nullcount=${columnMeta.nullCount()} data_len=${columnMeta.dataLength()}]\n" +
        s"$childrenStr"
  }

  def printResponse(state: String, res: MetadataResponse): String = {
    val out = new StringBuilder
    out.append(s"----------------------- METADATA RESPONSE $state --------------------------\n")
    out.append(s"${res.tableMetasLength()} tables\n")
    out.append("------------------------------------------------------------------------------\n")
    for (tableIndex <- 0 until res.tableMetasLength()) {
      val tableMeta = res.tableMetas(tableIndex)
      out.append(s"table: $tableIndex [cols=${tableMeta.columnMetasLength()}, " +
          s"rows=${tableMeta.rowCount}, full_content_size=${res.fullResponseSize()}]\n")
      val columnMetaObj = new ColumnMeta
      (0 until tableMeta.columnMetasLength()).foreach { i =>
        val columnMeta = tableMeta.columnMetas(columnMetaObj, i)
        out.append(printColumnMeta(s"column: $i", columnMeta))
      }
    }
    out.append(s"----------------------- END METADATA RESPONSE $state ----------------------\n")
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
