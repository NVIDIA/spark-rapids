/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ContiguousTable, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.{Arm, GpuColumnVector, MetaUtils, RapidsConf, RapidsDeviceMemoryStore, ShuffleMetadata, ShuffleReceivedBufferCatalog}
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, when}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.ShuffleMetricsUpdater
import org.apache.spark.sql.types.IntegerType
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
  var mockConnection: MockClientConnection = _
  var mockTransport: RapidsShuffleTransport = _
  var mockExecutor: Executor = _
  var mockCopyExecutor: Executor = _
  var mockBssExecutor: Executor = _
  var mockHandler: RapidsShuffleFetchHandler = _
  var mockStorage: RapidsDeviceMemoryStore = _
  var mockCatalog: ShuffleReceivedBufferCatalog = _
  var mockConf: RapidsConf = _
  var testMetricsUpdater: TestShuffleMetricsUpdater = _
  var client: RapidsShuffleClient = _

  def getBounceBuffer(size: Long): BounceBuffer = {
    withResource(HostMemoryBuffer.allocate(size)) { hmb =>
      fillBuffer(hmb)
      val db = DeviceMemoryBuffer.allocate(size)
      db.copyFromHostBuffer(hmb)
      new BounceBuffer(db) {
        override def free(bb: BounceBuffer): Unit = {
          db.close()
        }
      }
    }
  }

  def fillBuffer(hmb: HostMemoryBuffer): Unit = {
    (0 until hmb.getLength.toInt by 4).foreach { b =>
      hmb.setInt(b, b)
    }
  }

  def areBuffersEqual(orig: HostMemoryBuffer, buff: DeviceMemoryBuffer): Boolean = {
    var areEqual = orig.getLength == buff.getLength
    if (areEqual) {
      val len = orig.getLength.toInt
      withResource(HostMemoryBuffer.allocate(len)) { hmb =>
        hmb.copyFromDeviceBuffer(buff)
        (0 until len by 4).foreach { b =>
          areEqual = areEqual && hmb.getInt(b) == orig.getInt(b)
          if (!areEqual) {
            println(s"not equal at offset ${b} ${hmb.getInt(b)} -- ${orig.getInt(b)}")
          }
        }
      }
    } else {
      println(s"NOT EQUAL LENGTH ${orig} vs ${buff}")
    }
    areEqual
  }

  def getSendBounceBuffer(size: Long): SendBounceBuffers = {
    val db = DeviceMemoryBuffer.allocate(size)
    SendBounceBuffers(new BounceBuffer(db) {
      override def free(bb: BounceBuffer): Unit = {
        db.close()
      }
    }, None)
  }

  var buffersToClose = new ArrayBuffer[DeviceMemoryBuffer]()

  override def beforeEach(): Unit = {
    assert(buffersToClose.isEmpty)
    newMocks()
  }

  override def afterEach(): Unit = {
    buffersToClose.foreach(_.close())
    buffersToClose.clear()
  }

  def newMocks(): Unit = {
    mockTransaction = mock[Transaction]
    when(mockTransaction.getStats).thenReturn(mock[TransactionStats])
    mockConnection = spy(new MockClientConnection(mockTransaction))
    mockTransport = mock[RapidsShuffleTransport]
    mockExecutor = new ImmediateExecutor
    mockCopyExecutor = new ImmediateExecutor
    mockBssExecutor = mock[Executor]
    mockHandler = mock[RapidsShuffleFetchHandler]
    mockStorage = mock[RapidsDeviceMemoryStore]
    mockCatalog = mock[ShuffleReceivedBufferCatalog]
    mockConf = mock[RapidsConf]
    testMetricsUpdater = spy(new TestShuffleMetricsUpdater)

    val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
    when(mockStorage.addBuffer(any(), dmbCaptor.capture(), any(), any(), any())).thenAnswer(_ => {
      buffersToClose.append(dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer])
    })

    client = spy(new RapidsShuffleClient(
      mockConnection,
      mockTransport,
      mockExecutor,
      mockCopyExecutor,
      mockStorage,
      mockCatalog))
  }
}

object RapidsShuffleTestHelper extends MockitoSugar with Arm {
  def buildMockTableMeta(tableId: Int, contigTable: ContiguousTable): TableMeta = {
    MetaUtils.buildTableMeta(tableId, contigTable)
  }

  def buildDegenerateMockTableMeta(): TableMeta = {
    MetaUtils.buildDegenerateTableMeta(new ColumnarBatch(Array.empty, 123))
  }

  def withMockContiguousTable[T](numRows: Long)(body: ContiguousTable => T): T = {
    val rows: Seq[Integer] = (0 until numRows.toInt).map(Integer.valueOf)
    withResource(ColumnVector.fromBoxedInts(rows:_*)) { cvBase =>
      cvBase.incRefCount()
      val gpuCv = GpuColumnVector.from(cvBase, IntegerType)
      withResource(new ColumnarBatch(Array(gpuCv))) { cb =>
        withResource(GpuColumnVector.from(cb)) { table =>
          withResource(table.contiguousSplit(0, numRows.toInt)) { ct =>
            body(ct(1)) // we get a degenerate table at 0 and another at 2
          }
        }
      }
    }
  }

  def mockMetaResponse(
      mockTransaction: Transaction,
      numRows: Long,
      numBatches: Int): (Seq[TableMeta], MetadataTransportBuffer) =
    withMockContiguousTable(numRows) { ct =>
      val tableMetas = (0 until numBatches).map(b => buildMockTableMeta(b, ct))
      val res = ShuffleMetadata.buildMetaResponse(tableMetas)
      val refCountedRes = new MetadataTransportBuffer(new RefCountedDirectByteBuffer(res))
      when(mockTransaction.releaseMessage()).thenReturn(refCountedRes)
      (tableMetas, refCountedRes)
    }

  def mockDegenerateMetaResponse(
      mockTransaction: Transaction,
      numBatches: Int): (Seq[TableMeta], MetadataTransportBuffer) = {
    val tableMetas = (0 until numBatches).map(b => buildDegenerateMockTableMeta())
    val res = ShuffleMetadata.buildMetaResponse(tableMetas)
    val refCountedRes = new MetadataTransportBuffer(new RefCountedDirectByteBuffer(res))
    when(mockTransaction.releaseMessage()).thenReturn(refCountedRes)
    (tableMetas, refCountedRes)
  }

  def prepareMetaTransferRequest(numTables: Int, numRows: Long): MetadataTransportBuffer =
    withMockContiguousTable(numRows) { ct =>
      val trBuffer = ShuffleMetadata.buildTransferRequest(123L, 0 until numTables)
      val refCountedRes = new RefCountedDirectByteBuffer(trBuffer)
      new MetadataTransportBuffer(refCountedRes)
    }

  def mockTableMeta(numRows: Long): TableMeta =
    withMockContiguousTable(numRows) { ct =>
      buildMockTableMeta(1, ct)
    }

  def prepareMetaTransferResponse(
      mockTransaction: Transaction,
      numRows: Long): TableMeta =
    withMockContiguousTable(numRows) { ct =>
      val tableMeta = buildMockTableMeta(1, ct)
      val bufferMeta = tableMeta.bufferMeta()
      val res = ShuffleMetadata.buildBufferTransferResponse(Seq(bufferMeta))
      val refCountedRes = new MetadataTransportBuffer(new RefCountedDirectByteBuffer(res))
      when(mockTransaction.releaseMessage()).thenReturn(refCountedRes)
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

class MockClientConnection(mockTransaction: Transaction) extends ClientConnection {
  var requests: Int = 0

  override def getPeerExecutorId: Long = 0

  override def request(messageType: MessageType.Value,
                       request: ByteBuffer, cb: TransactionCallback): Transaction = {
    requests += 1
    cb(mockTransaction)
    mockTransaction
  }

  override def registerReceiveHandler(messageType: MessageType.Value): Unit = {}
}

