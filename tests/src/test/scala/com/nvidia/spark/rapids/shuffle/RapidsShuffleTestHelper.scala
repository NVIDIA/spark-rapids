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

package com.nvidia.spark.rapids.shuffle

import java.util.concurrent.Executor

import ai.rapids.cudf.{ColumnVector, ContiguousTable}
import com.nvidia.spark.rapids.{Arm, GpuColumnVector, MetaUtils, RapidsConf, RapidsDeviceMemoryStore, ShuffleMetadata, ShuffleReceivedBufferCatalog}
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, when}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.mockito.MockitoSugar
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.rapids.ShuffleMetricsUpdater
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId}

class TestShuffleMetricsUpdater extends ShuffleMetricsUpdater {
  var totalRemoteBlocksFetched = 0L
  var totalRemoteBytesRead = 0L
  var totalRowsFetched = 0L
  override def update(
      fetchWaitTimeInMs: Long,
      remoteBlocksFetched: Long,
      remoteBytesRead: Long,
      rowsFetched: Long): Unit = {
    totalRemoteBlocksFetched += remoteBlocksFetched
    totalRemoteBytesRead += remoteBytesRead
    totalRowsFetched += rowsFetched
  }
}

class RapidsShuffleTestHelper extends FunSuite
    with BeforeAndAfterEach
    with MockitoSugar
    with Arm {
  var mockTransaction: Transaction = _
  var mockConnection: MockConnection = _
  var mockTransport: RapidsShuffleTransport = _
  var mockExecutor: Executor = _
  var mockCopyExecutor: Executor = _
  var mockHandler: RapidsShuffleFetchHandler = _
  var mockStorage: RapidsDeviceMemoryStore = _
  var mockCatalog: ShuffleReceivedBufferCatalog = _
  var mockConf: RapidsConf = _
  var testMetricsUpdater: TestShuffleMetricsUpdater = _
  var client: RapidsShuffleClient = _

  override def beforeEach(): Unit = {
    newMocks()
  }

  def newMocks(): Unit = {
    mockTransaction = mock[Transaction]
    mockConnection = spy(new MockConnection(mockTransaction))
    mockTransport = mock[RapidsShuffleTransport]
    mockExecutor = new ImmediateExecutor
    mockCopyExecutor = new ImmediateExecutor
    mockHandler = mock[RapidsShuffleFetchHandler]
    mockStorage = mock[RapidsDeviceMemoryStore]
    mockCatalog = mock[ShuffleReceivedBufferCatalog]
    mockConf = mock[RapidsConf]
    testMetricsUpdater = spy(new TestShuffleMetricsUpdater)

    client = spy(new RapidsShuffleClient(
      1,
      mockConnection,
      mockTransport,
      mockExecutor,
      mockCopyExecutor,
      1024,
      mockStorage,
      mockCatalog))
  }
}

object RapidsShuffleTestHelper extends MockitoSugar with Arm {
  def buildMockTableMeta(tableId: Int, contigTable: ContiguousTable): TableMeta = {
    val tbl = contigTable.getTable
    val cols = (0 until tbl.getNumberOfColumns).map(tbl.getColumn)
    MetaUtils.buildTableMeta(tableId, cols, tbl.getRowCount, contigTable.getBuffer)
  }

  def buildDegenerateMockTableMeta(): TableMeta = {
    MetaUtils.buildDegenerateTableMeta(new ColumnarBatch(Array.empty, 123))
  }

  def withMockContiguousTable[T](numRows: Long)(body: ContiguousTable => T): T = {
    val rows: Seq[Integer] = (0 until numRows.toInt).map(new Integer(_))
    withResource(ColumnVector.fromBoxedInts(rows:_*)) { cvBase =>
      cvBase.incRefCount()
      val gpuCv = GpuColumnVector.from(cvBase)
      withResource(new ColumnarBatch(Seq(gpuCv).toArray)) { cb =>
        withResource(GpuColumnVector.from(cb)) { table =>
          withResource(table.contiguousSplit(0, numRows.toInt)) { ct =>
            body(ct(1)) // we get a degenerate table at 0 and another at 2
          }
        }
      }
    }
  }

  def mockMetaResponse(
      mockTransport: RapidsShuffleTransport,
      numRows: Long,
      numBatches: Int,
      maximumResponseSize: Long = 10000): Seq[TableMeta] =
    withMockContiguousTable(numRows) { ct =>
      val tableMetas = (0 until numBatches).map(b => buildMockTableMeta(b, ct))
      val res = ShuffleMetadata.buildMetaResponse(tableMetas, maximumResponseSize)
      val refCountedRes = new RefCountedDirectByteBuffer(res)
      when(mockTransport.getMetaBuffer(any())).thenReturn(refCountedRes)
      tableMetas
    }

  def mockDegenerateMetaResponse(
      mockTransport: RapidsShuffleTransport,
      numRows: Long,
      numBatches: Int,
      maximumResponseSize: Long = 10000): Seq[TableMeta] = {
    val tableMetas = (0 until numBatches).map(b => buildDegenerateMockTableMeta())
    val res = ShuffleMetadata.buildMetaResponse(tableMetas, maximumResponseSize)
    val refCountedRes = new RefCountedDirectByteBuffer(res)
    when(mockTransport.getMetaBuffer(any())).thenReturn(refCountedRes)
    tableMetas
  }

  def prepareMetaTransferResponse(
      mockTransport: RapidsShuffleTransport,
      numRows: Long): TableMeta =
    withMockContiguousTable(numRows) { ct =>
      val tableMeta = buildMockTableMeta(1, ct)
      val bufferMeta = tableMeta.bufferMeta()
      val res = ShuffleMetadata.buildBufferTransferResponse(Seq(bufferMeta))
      val refCountedRes = new RefCountedDirectByteBuffer(res)
      when(mockTransport.getMetaBuffer(any())).thenReturn(refCountedRes)
      tableMeta
    }

  def getShuffleBlocks: Seq[(ShuffleBlockBatchId, Long, Int)] = {
    Seq(
      (ShuffleBlockBatchId(1,1,1,1), 123L, 1),
      (ShuffleBlockBatchId(2,2,2,2), 456L, 2),
      (ShuffleBlockBatchId(3,3,3,3), 456L, 3)
    )
  }

  def makeMockBlockManager(execId: String, host: String): BlockManagerId = {
    val bmId = mock[BlockManagerId]
    when(bmId.executorId).thenReturn(execId)
    when(bmId.host).thenReturn(host)
    bmId
  }

  def getBlocksByAddress: Array[(BlockManagerId, Seq[(BlockId, Long, Int)])] = {
    val blocksByAddress = new ArrayBuffer[(BlockManagerId, Seq[(BlockId, Long, Int)])]()
    val blocks = getShuffleBlocks
    blocksByAddress.append((makeMockBlockManager("2", "2"), blocks))
    blocksByAddress.toArray
  }
}

class ImmediateExecutor extends Executor {
  override def execute(runnable: Runnable): Unit = {
    runnable.run()
  }
}

class MockConnection(mockTransaction: Transaction) extends ClientConnection {
  val requests = new ArrayBuffer[AddressLengthTag]
  val receiveLengths = new ArrayBuffer[Long]
  override def request(
      request: AddressLengthTag,
      response: AddressLengthTag,
      cb: TransactionCallback): Transaction = {
    requests.append(request)
    cb(mockTransaction)
    mockTransaction
  }

  override def receive(header: AddressLengthTag, cb: TransactionCallback): Transaction = {
    cb(mockTransaction)
    mockTransaction
  }

  override def receive(
      bounceBuffers: Seq[AddressLengthTag],
      cb: TransactionCallback): Transaction = {
    receiveLengths.appendAll(bounceBuffers.map(_.length))
    cb(mockTransaction)
    mockTransaction
  }

  override def getPeerExecutorId: Long =
    throw new UnsupportedOperationException

  override def assignResponseTag: Long = 1L
  override def assignBufferTag(msgId: Int): Long = 2L
  override def composeRequestTag(requestType:  RequestType.Value): Long = 3L
}

