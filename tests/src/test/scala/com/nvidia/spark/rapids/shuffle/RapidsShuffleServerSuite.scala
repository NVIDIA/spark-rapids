/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION.
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

import java.io.{FileNotFoundException, IOException}
import java.nio.ByteBuffer

import ai.rapids.cudf.{CudaFatalException, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{MetaUtils, RapidsShuffleHandle, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import org.apache.spark.shuffle.rapids.RapidsShuffleSendPrepareException
import org.apache.spark.storage.ShuffleBlockBatchId

class MockRapidsShuffleRequestHandler(mockBuffers: Seq[RapidsShuffleHandle])
    extends RapidsShuffleRequestHandler with AutoCloseable {
  var acquiredTables = Seq[Int]()
  override def getShuffleBufferMetas(
      shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta] = {
    throw new NotImplementedError("getShuffleBufferMetas")
  }

  override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
    acquiredTables = acquiredTables :+ tableId
    mockBuffers(tableId)
  }

  override def close(): Unit = {
    // a removeShuffle action would likewise remove handles
    mockBuffers.foreach(_.close())
  }
}

class RapidsShuffleServerSuite extends RapidsShuffleTestHelper {

  def setupMocks(deviceBuffers: Seq[DeviceMemoryBuffer]): MockRapidsShuffleRequestHandler = {
    val mockBuffers = deviceBuffers.map { deviceBuffer =>
      withResource(HostMemoryBuffer.allocate(deviceBuffer.getLength)) { hostBuff =>
        fillBuffer(hostBuff)
        deviceBuffer.copyFromHostBuffer(hostBuff)
        val mockMeta = RapidsShuffleTestHelper.mockTableMeta(100000)
        RapidsShuffleHandle(SpillableDeviceBufferHandle(deviceBuffer), mockMeta)
      }
    }
    new MockRapidsShuffleRequestHandler(mockBuffers)
  }

  class MockBlockWithSize(override val size: Long) extends BlockWithSize {}

  def compareRanges(
      bounceBuffer: SendBounceBuffers,
      receiveBlocks: Seq[(BlockRange[MockBlockWithSize], DeviceMemoryBuffer)]): Unit = {
    var bounceBuffOffset = 0L
    receiveBlocks.foreach { case (range, deviceBuff) =>
      val deviceBounceBuff = bounceBuffer.deviceBounceBuffer.buffer
      withResource(deviceBounceBuff.slice(bounceBuffOffset, range.rangeSize())) { bbSlice =>
        bounceBuffOffset = bounceBuffOffset + range.rangeSize()
        withResource(HostMemoryBuffer.allocate(bbSlice.getLength)) { hostCopy =>
          hostCopy.copyFromDeviceBuffer(bbSlice.asInstanceOf[DeviceMemoryBuffer])
          withResource(deviceBuff.slice(range.rangeStart, range.rangeSize())) { origSlice =>
            assert(areBuffersEqual(hostCopy, origSlice))
          }
        }
      }
    }
  }

  /** Mocked send bounce buffers of 1024 bytes whose slices are `slice`. */
  private def makeSendBuffer(slice: DeviceMemoryBuffer): SendBounceBuffers = {
    val sendBuffer = mock[SendBounceBuffers]
    val bounceBuffer = mock[BounceBuffer]
    val bounceMemory = mock[DeviceMemoryBuffer]
    when(bounceBuffer.buffer).thenReturn(bounceMemory)
    when(bounceMemory.getLength).thenReturn(1024L)
    when(bounceMemory.slice(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong()))
      .thenReturn(slice)
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(bounceBuffer)
    sendBuffer
  }

  /** A transfer request for one table. */
  private def makeTx(tableId: Int): Transaction = {
    val tx = mock[Transaction]
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, Seq(tableId)))))
    tx
  }

  /** A handle whose materialization answers with `materialize`. */
  private def makeHandle(tableMeta: TableMeta, materialize: () => DeviceMemoryBuffer)
      : RapidsShuffleHandle = {
    val handle = mock[SpillableDeviceBufferHandle]
    when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
    when(handle.materialize()).thenAnswer(_ => materialize())
    RapidsShuffleHandle(handle, tableMeta)
  }

  private def newServer(requestHandler: RapidsShuffleRequestHandler,
      serverConnection: ServerConnection): RapidsShuffleServer = {
    spy(new RapidsShuffleServer(
      mockTransport,
      serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
      requestHandler,
      mockExecutor,
      mockBssExecutor,
      mockConf))
  }

  test("sending tables that fit within one bounce buffer") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(10, 1000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 10).map(_ => DeviceMemoryBuffer.allocate(1000))
      val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(1000))
      withResource(setupMocks(deviceBuffers)) { handler =>
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          assert(bss.hasMoreSends)
          withResource(bss.getBufferToSend()) { mb =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assertResult(10000)(mb.getLength)
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("sending tables that require two bounce buffer lengths") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(20, 1000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 20).map(_ => DeviceMemoryBuffer.allocate(1000))
      val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(1000))
      val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
      withResource(setupMocks(deviceBuffers)) { handler =>
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assert(bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }

          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("sending buffers larger than bounce buffer") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(20, 10000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      val deviceBuffers = (0 until 20).map(_ => DeviceMemoryBuffer.allocate(123000))
      withResource(setupMocks(deviceBuffers)) { handler =>
        val receiveSide = deviceBuffers.map(_ => new MockBlockWithSize(123000))
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          (0 until 246).foreach { _ =>
            withResource(bss.getBufferToSend()) { _ =>
              val receiveBlocks = receiveWindow.next()
              compareRanges(bounceBuffer, receiveBlocks.zip(deviceBuffers))
              bss.releaseAcquiredToCatalog()
            }
          }
          assert(!bss.hasMoreSends)
        }
      }
      deviceBuffers.foreach { b =>
        assertResult(0)(b.getRefCount)
      }
      bounceBuffer
    }
    assert(bb.deviceBounceBuffer.isClosed)
    assert(transferRequest.dbb.isClosed)
  }

  test("when a send fails, we un-acquire buffers that are currently being sent") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    withResource(DeviceMemoryBuffer.allocate(123)) { buff =>
      when(mockDeviceBounceBuffer.buffer).thenReturn(buff)
      when(mockSendBuffer.bounceBufferSize).thenReturn(buff.getLength)
      when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
      when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

      when(mockTransport.tryGetSendBounceBuffers(any(), any()))
        .thenReturn(Seq(mockSendBuffer))

      val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1))
      when(mockTransaction.getStatus)
        .thenReturn(TransactionStatus.Success)
        .thenReturn(TransactionStatus.Error)
      when(mockTransaction.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

      val mockServerConnection = mock[ServerConnection]
      val ac = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(mockServerConnection.send(
        any(), any(), any(), any[MemoryBuffer](), ac.capture())).thenReturn(mockTransaction)

      val mockRequestHandler = mock[RapidsShuffleRequestHandler]

      val bb = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
        val testHandle = SpillableDeviceBufferHandle(DeviceMemoryBuffer.allocate(456))
        val rapidsBuffer = RapidsShuffleHandle(testHandle, tableMeta)
        when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
          .thenReturn(rapidsBuffer)

        val server = new RapidsShuffleServer(
          mockTransport,
          mockServerConnection,
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
          mockRequestHandler,
          mockExecutor,
          mockBssExecutor,
          mockConf)

        server.start()

        val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler)
        server.doHandleTransferRequest(Seq(bss))
        val cb = ac.getValue.asInstanceOf[TransactionCallback]
        cb(mockTransaction)
        server.doHandleTransferRequest(Seq(bss)) //Error
        cb(mockTransaction)
        // bounce buffers are freed
        verify(mockSendBuffer, times(1)).close()
        // acquire once at the beginning, and closed at the end
        verify(mockRequestHandler, times(1))
          .getShuffleHandle(ArgumentMatchers.eq(1))
        withResource(rapidsBuffer.spillable.materialize()) { dmb =>
          // refcount=2 because it was on the device, and we +1 to materialize.
          // but it shows no leaks.
          assertResult(2)(dmb.getRefCount)
        }
      }
    }
  }

  test("when we fail to prepare a send, end the request if nothing can be handled") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    val mockDeviceMemoryBuffer = mock[DeviceMemoryBuffer]
    when(mockDeviceBounceBuffer.buffer).thenReturn(mockDeviceMemoryBuffer)
    when(mockSendBuffer.bounceBufferSize).thenReturn(1024)
    when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
    when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

    when(mockTransport.tryGetSendBounceBuffers(any(), any()))
      .thenReturn(Seq(mockSendBuffer))

    val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1, 2))
    when(mockTransaction.getStatus)
      .thenReturn(TransactionStatus.Success)
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

    val mockServerConnection = mock[ServerConnection]
    val mockRequestHandler = mock[RapidsShuffleRequestHandler]

    val bb = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
      val mockHandle = mock[SpillableDeviceBufferHandle]
      val mockHandleThatThrows = mock[SpillableDeviceBufferHandle]
      val mockMaterialized = mock[DeviceMemoryBuffer]
      when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(mockHandle.materialize()).thenAnswer(_ => mockMaterialized)

      when(mockHandleThatThrows.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      val ex = new IllegalStateException("something happened")
      when(mockHandleThatThrows.materialize()).thenThrow(ex)

      val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
      val rapidsBufferThatThrows = RapidsShuffleHandle(mockHandleThatThrows, tableMeta)

      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(rapidsBuffer)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(rapidsBufferThatThrows)

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mockServerConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        mockRequestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      server.start()

      val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null)
      // if nothing else can be handled, the request ends rather than the send loop
      server.doHandleTransferRequest(Seq(bss))
      assertResult(Some(BufferSendState.Failed))(bss.endedWith)

      // since nothing could be handled, we don't try again
      verify(server, times(0)).addToContinueQueue(any())

      // bounce buffers are freed, and the request transaction is closed, once
      verify(mockSendBuffer, times(1)).close()
      verify(mockTransaction, times(1)).close()

      verify(mockRequestHandler, times(1))
        .getShuffleHandle(ArgumentMatchers.eq(1))

      // the spillable that materialized we need to close
      verify(mockMaterialized, times(1)).close()
    }
  }

  test("when GPU OOM prevents all sends, stop the request at the shuffle fetch timeout") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    val mockDeviceMemoryBuffer = mock[DeviceMemoryBuffer]
    val mockServerConnection = mock[ServerConnection]
    when(mockDeviceBounceBuffer.buffer).thenReturn(mockDeviceMemoryBuffer)
    when(mockSendBuffer.bounceBufferSize).thenReturn(1024)
    when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
    when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

    val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1, 2))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

    val mockRequestHandler = mock[RapidsShuffleRequestHandler]
    val bb = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
      val mockHandle = mock[SpillableDeviceBufferHandle]
      val mockHandleThatThrows = mock[SpillableDeviceBufferHandle]
      val mockMaterialized = mock[DeviceMemoryBuffer]
      when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(mockHandle.materialize()).thenReturn(mockMaterialized)
      when(mockHandleThatThrows.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      val oom = new OutOfMemoryError("GPU allocation failed in test")
      when(mockHandleThatThrows.materialize()).thenThrow(oom)
      val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
      val rapidsBufferThatThrows = RapidsShuffleHandle(mockHandleThatThrows, tableMeta)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(rapidsBuffer)
      when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(rapidsBufferThatThrows)

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mockServerConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        mockRequestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 1L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null)
      assertResult(10L)(server.oomRetryBackoffMillis(1))
      assertResult(20L)(server.oomRetryBackoffMillis(2))
      assertResult(100L)(server.oomRetryBackoffMillis(5))
      assertResult(100L)(server.oomRetryBackoffMillis(1000))
      server.doHandleTransferRequest(Seq(bss))

      verify(server, times(1)).addToContinueQueue(Seq(bss))
      verify(server, times(1)).waitBeforeOomRetry(10L)
      verify(mockSendBuffer, times(0)).close()

      nowNanos = 1L
      server.doHandleTransferRequest(Seq(bss))
      verify(server, times(1)).addToContinueQueue(Seq(bss))
      verify(server, times(1)).waitBeforeOomRetry(10L)
      verify(mockHandle, times(2)).materialize()
      verify(mockHandleThatThrows, times(2)).materialize()
      verify(mockMaterialized, times(2)).close()
      verify(mockSendBuffer, times(1)).close()
      assertResult(Some(BufferSendState.OomExpired))(bss.endedWith)
      verify(mockTransaction, times(1)).close()
    }
  }

  test("successful preparation resets the OOM retry window on the same send state") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    val deviceMemoryBuffer = mock[DeviceMemoryBuffer]
    val bufferSlice = mock[DeviceMemoryBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(deviceMemoryBuffer)
    when(deviceMemoryBuffer.getLength).thenReturn(1024L)
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)
    when(deviceMemoryBuffer.slice(ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong()))
      .thenReturn(bufferSlice)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = mock[SpillableDeviceBufferHandle]
      val materialized = mock[DeviceMemoryBuffer]
      when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(handle.materialize()).thenReturn(materialized)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(handle, tableMeta))

      withResource(new BufferSendState(
        mockTransaction, sendBuffer, requestHandler, null)) { bss =>
        assertResult(Some(1))(bss.recordOomAndGetRetryAttempt(0L, 10L))
        withResource(bss.getBufferToSend()) { _ => }

        // This is a new OOM episode. If resetOomRetryWindow were a no-op or its successful
        // preparation call site were removed, the old window would expire at this timestamp.
        assertResult(Some(1))(bss.recordOomAndGetRetryAttempt(10L, 10L))
      }
    }
  }

  test("an expired OOM window does not close a co-batched send with a fresh window") {
    def makeSendBuffer(name: String): SendBounceBuffers = {
      val sb = mock[SendBounceBuffers](name)
      val dbb = mock[BounceBuffer]
      when(dbb.buffer).thenReturn(mock[DeviceMemoryBuffer])
      when(sb.bounceBufferSize).thenReturn(1024)
      when(sb.hostBounceBuffer).thenReturn(None)
      when(sb.deviceBounceBuffer).thenReturn(dbb)
      sb
    }
    val sendBufferA = makeSendBuffer("sendBufferA")
    val sendBufferB = makeSendBuffer("sendBufferB")

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val oomHandle = mock[SpillableDeviceBufferHandle]
      when(oomHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(oomHandle.materialize())
        .thenThrow(new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(oomHandle, tableMeta))

      def makeTx(peerExecutorId: Long): Transaction = {
        val tx = mock[Transaction]
        when(tx.peerExecutorId()).thenReturn(peerExecutorId)
        when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
          new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, Seq(1)))))
        tx
      }

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mock[ServerConnection],
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 10L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val bssA = new BufferSendState(makeTx(1L), sendBufferA, requestHandler, null)
      server.doHandleTransferRequest(Seq(bssA))
      verify(server, times(1)).addToContinueQueue(Seq(bssA))

      nowNanos = 10L
      val bssB = new BufferSendState(makeTx(2L), sendBufferB, requestHandler, null)
      server.doHandleTransferRequest(Seq(bssA, bssB))

      verify(sendBufferA, times(1)).close()
      verify(sendBufferB, times(0)).close()
      verify(server, times(1)).addToContinueQueue(Seq(bssB))
      assertResult(Some(BufferSendState.OomExpired))(bssA.endedWith)
      assertResult(None)(bssB.endedWith)
    }
  }

  test("progress by a co-batched send does not reset another send state's OOM window") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(mock[DeviceMemoryBuffer])
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val oomHandle = mock[SpillableDeviceBufferHandle]
      when(oomHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(oomHandle.materialize())
        .thenThrow(new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(oomHandle, tableMeta))

      val successfulState = mock[BufferSendState]
      val successfulBuffer = mock[MemoryBuffer]
      when(successfulState.hasMoreSends).thenReturn(true)
      when(successfulState.getBufferToSend()).thenReturn(successfulBuffer)

      val serverConnection = mock[ServerConnection]
      when(serverConnection.send(
        any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]()))
        .thenReturn(mock[Transaction])

      var nowNanos = 0L
      val server = spy(new RapidsShuffleServer(
        mockTransport,
        serverConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf) {
        override private[shuffle] def currentTimeNanos(): Long = nowNanos
        override private[shuffle] def oomRetryTimeoutNanos: Long = 10L
        override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit = {}
      })

      val oomState = new BufferSendState(
        mockTransaction, sendBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(oomState))
      verify(server, times(1)).addToContinueQueue(Seq(oomState))

      nowNanos = 10L
      server.doHandleTransferRequest(Seq(oomState, successfulState))

      verify(sendBuffer, times(1)).close()
      verify(server, times(1)).addToContinueQueue(any())
    }
  }

  test("when we fail to prepare a send, re-queue the request if anything can be handled") {
    val mockSendBuffer = mock[SendBounceBuffers]
    val mockDeviceBounceBuffer = mock[BounceBuffer]
    withResource(DeviceMemoryBuffer.allocate(123)) { buff =>
      when(mockDeviceBounceBuffer.buffer).thenReturn(buff)
      when(mockSendBuffer.bounceBufferSize).thenReturn(buff.getLength)
      when(mockSendBuffer.hostBounceBuffer).thenReturn(None)
      when(mockSendBuffer.deviceBounceBuffer).thenReturn(mockDeviceBounceBuffer)

      when(mockTransport.tryGetSendBounceBuffers(any(), any()))
          .thenReturn(Seq(mockSendBuffer))

      val tr = ShuffleMetadata.buildTransferRequest(0, Seq(1))
      when(mockTransaction.getStatus)
          .thenReturn(TransactionStatus.Success)
      when(mockTransaction.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr)))

      val tr2 = ShuffleMetadata.buildTransferRequest(0, Seq(2))
      val mockTransaction2 = mock[Transaction]
      when(mockTransaction2.getStatus)
          .thenReturn(TransactionStatus.Success)
      when(mockTransaction2.releaseMessage()).thenReturn(
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(tr2)))

      val mockServerConnection = mock[ServerConnection]
      val ac = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(mockServerConnection.send(
        any(), any(), any(), any[MemoryBuffer](), ac.capture())).thenReturn(mockTransaction)

      val mockRequestHandler = mock[RapidsShuffleRequestHandler]

      def makeMockBuffer(tableId: Int, bb: ByteBuffer, error: Boolean): RapidsShuffleHandle  = {
        val tableMeta = MetaUtils.buildTableMeta(tableId, 456, bb, 100)
        val rapidsBuffer = if (error) {
          val mockHandle = mock[SpillableDeviceBufferHandle]
          val rapidsBuffer = RapidsShuffleHandle(mockHandle, tableMeta)
          when(mockHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
          // mock an error with the copy
          when(rapidsBuffer.spillable.materialize())
            .thenAnswer(_ => {
              throw new IOException("mmap failed in test")
            })
          rapidsBuffer
        } else {
          val testHandle = spy(SpillableDeviceBufferHandle(spy(DeviceMemoryBuffer.allocate(456))))
          RapidsShuffleHandle(testHandle, tableMeta)
        }
        when(mockRequestHandler.getShuffleHandle(ArgumentMatchers.eq(tableId)))
          .thenAnswer(_ => rapidsBuffer)
        rapidsBuffer
      }

      val bb = ByteBuffer.allocateDirect(123)
      val bb2 = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        withResource(new RefCountedDirectByteBuffer(bb2)) { _ =>
          val rapidsHandle = makeMockBuffer(1, bb, error = true)
          val rapidsHandle2 = makeMockBuffer(2, bb2, error = false)

          val server = spy(new RapidsShuffleServer(
            mockTransport,
            mockServerConnection,
            RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
            mockRequestHandler,
            mockExecutor,
            mockBssExecutor,
            mockConf))

          server.start()

          val bssFailed = new BufferSendState(
            mockTransaction, mockSendBuffer, mockRequestHandler)

          val bssSuccess = spy(new BufferSendState(
            mockTransaction2, mockSendBuffer, mockRequestHandler))

          var callCount = 0
          doAnswer { _ =>
            callCount += 1
            // send 1 buffer length
            if (callCount > 1){
              false
            } else {
              true
            }
          }.when(bssSuccess).hasMoreSends

          // if something else can be handled we don't throw, and re-queue
          server.doHandleTransferRequest(Seq(bssFailed, bssSuccess))

          val cb = ac.getValue.asInstanceOf[TransactionCallback]
          cb(mockTransaction)

          verify(server, times(1)).addToContinueQueue(any())

          // the bounce buffer is freed 1 time for `bssSuccess`, but not for `bssFailed`
          verify(mockSendBuffer, times(1)).close()

          // we obtained the handles once, we don't need to get them again
          verify(mockRequestHandler, times(1))
              .getShuffleHandle(ArgumentMatchers.eq(1))
          verify(mockRequestHandler, times(1))
              .getShuffleHandle(ArgumentMatchers.eq(2))
          // this handle fails to materialize
          verify(rapidsHandle.spillable, times(1)).materialize()

          // this handle materializes, so make sure we close it
          verify(rapidsHandle2.spillable, times(1)).materialize()
          withResource(rapidsHandle2.spillable.materialize()) { dmb =>
            // refcount=2 because it was on the device, and we +1 to materialize.
            // but it shows no leaks.
            assertResult(2)(dmb.getRefCount)
          }
        }
      }
    }
  }

  test("when a buffer is gone from the catalog, end its request instead of retrying it") {
    val sendBuffer = mock[SendBounceBuffers]
    val deviceBounceBuffer = mock[BounceBuffer]
    when(deviceBounceBuffer.buffer).thenReturn(mock[DeviceMemoryBuffer])
    when(sendBuffer.bounceBufferSize).thenReturn(1024)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(deviceBounceBuffer)

    val request = ShuffleMetadata.buildTransferRequest(0, Seq(1))
    when(mockTransaction.releaseMessage()).thenReturn(
      new MetadataTransportBuffer(new RefCountedDirectByteBuffer(request)))

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val handle = mock[SpillableDeviceBufferHandle]
      when(handle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      // a disk-spilled buffer whose spill file the unregister deleted
      when(handle.materialize()).thenAnswer(_ => {
        throw new FileNotFoundException("spill file deleted in test")
      })
      val rapidsHandle = RapidsShuffleHandle(handle, tableMeta)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(rapidsHandle)
      when(requestHandler.isShuffleBufferRemoved(ArgumentMatchers.eq(1),
        ArgumentMatchers.eq(rapidsHandle))).thenReturn(true)

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        mock[ServerConnection],
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      val bss = new BufferSendState(mockTransaction, sendBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(bss))

      assertResult(Some(BufferSendState.MissingOutput))(bss.endedWith)
      verify(server, times(0)).addToContinueQueue(any())
      verify(sendBuffer, times(1)).close()
      verify(mockTransaction, times(1)).close()
    }
  }

  test("a slice that fails to close after its send leaves its state to the send's callback") {
    // the slice of the state that is sent fails to close once its send returned
    val sentSlice = mock[DeviceMemoryBuffer]
    doThrow(new IllegalStateException("slice close failed in test")).when(sentSlice).close()
    val sentBuffer = makeSendBuffer(sentSlice)
    val retriedBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val sentTx = makeTx(1)
    val retriedTx = makeTx(2)

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val sentHandle = mock[SpillableDeviceBufferHandle]
      when(sentHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(sentHandle.materialize()).thenReturn(mock[DeviceMemoryBuffer])
      // fails to prepare for a reason that is not classified, so it would be retried
      val retriedHandle = mock[SpillableDeviceBufferHandle]
      when(retriedHandle.sizeInBytes).thenReturn(tableMeta.bufferMeta().size())
      when(retriedHandle.materialize()).thenAnswer(_ => {
        throw new IOException("mmap failed in test")
      })
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1)))
        .thenReturn(RapidsShuffleHandle(sentHandle, tableMeta))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(2)))
        .thenReturn(RapidsShuffleHandle(retriedHandle, tableMeta))

      val serverConnection = mock[ServerConnection]
      val callback = ArgumentCaptor.forClass(classOf[TransactionCallback])
      when(serverConnection.send(
        any(), any(), any(), any[MemoryBuffer](), callback.capture())).thenReturn(mock[Transaction])

      val server = spy(new RapidsShuffleServer(
        mockTransport,
        serverConnection,
        RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
        requestHandler,
        mockExecutor,
        mockBssExecutor,
        mockConf))

      val sent = new BufferSendState(sentTx, sentBuffer, requestHandler, null)
      val retried = new BufferSendState(retriedTx, retriedBuffer, requestHandler, null)
      server.doHandleTransferRequest(Seq(sent, retried))

      // the failure came after the send returned, so the transport may still read the buffer
      assertResult(None)(sent.endedWith)
      verify(sentBuffer, times(0)).close()
      assertResult(1)(server.inFlightCount)
      // the state that had not been sent ends in that pass, and is never queued to retry
      assertResult(Some(BufferSendState.Failed))(retried.endedWith)
      verify(retriedBuffer, times(1)).close()
      verify(retriedTx, times(1)).close()
      verify(server, times(0)).addToContinueQueue(any())

      val done = mock[Transaction]
      when(done.getStatus).thenReturn(TransactionStatus.Success)
      callback.getValue.asInstanceOf[TransactionCallback](done)
      assertResult(Some(BufferSendState.Completed))(sent.endedWith)
      verify(sentTx, times(1)).respond(any(), any())
      verify(sentBuffer, times(1)).close()
      verify(sentTx, times(1)).close()
      assertResult(0)(server.inFlightCount)
    }
  }

  test("a fatal CUDA error found while cleaning up after another failure still ends the loop") {
    val constructor =
      classOf[CudaFatalException].getDeclaredConstructor(classOf[String], classOf[Int])
    constructor.setAccessible(true)
    val fatal = constructor.newInstance("an illegal memory access in test", Int.box(700))
    // both states prepare; releasing the first one's buffer then fails with an ordinary error,
    // and releasing the second one's, while the pass cleans up, with a fatal CUDA error
    val ordinaryRelease = mock[DeviceMemoryBuffer]
    doThrow(new IllegalStateException("release failed in test")).when(ordinaryRelease).close()
    val fatalRelease = mock[DeviceMemoryBuffer]
    doThrow(fatal).when(fatalRelease).close()
    val firstBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val secondBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val firstTx = makeTx(1)
    val secondTx = makeTx(2)

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val firstHandle = makeHandle(tableMeta, () => ordinaryRelease)
      val secondHandle = makeHandle(tableMeta, () => fatalRelease)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(firstHandle)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(2))).thenReturn(secondHandle)
      val serverConnection = mock[ServerConnection]
      val server = newServer(requestHandler, serverConnection)

      val first = new BufferSendState(firstTx, firstBuffer, requestHandler, null)
      val second = new BufferSendState(secondTx, secondBuffer, requestHandler, null)
      val failure = intercept[IllegalStateException] {
        server.doHandleTransferRequest(Seq(first, second))
      }

      assertResult("release failed in test")(failure.getMessage)
      assert(failure.getSuppressed.contains(fatal))
      assertResult(Some(BufferSendState.Failed))(first.endedWith)
      assertResult(Some(BufferSendState.Failed))(second.endedWith)
      verify(firstBuffer, times(1)).close()
      verify(secondBuffer, times(1)).close()
      verify(firstTx, times(1)).close()
      verify(secondTx, times(1)).close()
      verify(serverConnection, times(0)).send(
        any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]())
    }
  }

  test("an interrupted preparation ends the loop and keeps the thread's interrupt") {
    val healthyBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val interruptedBuffer = makeSendBuffer(mock[DeviceMemoryBuffer])
    val healthyTx = makeTx(1)
    val interruptedTx = makeTx(2)

    val requestHandler = mock[RapidsShuffleRequestHandler]
    val metadataBuffer = ByteBuffer.allocateDirect(123)
    withResource(new RefCountedDirectByteBuffer(metadataBuffer)) { _ =>
      val tableMeta = MetaUtils.buildTableMeta(1, 456, metadataBuffer, 100)
      val materialized = mock[DeviceMemoryBuffer]
      val healthyHandle = makeHandle(tableMeta, () => materialized)
      // as a disk-spilled buffer does when its wait for a host spill bounce buffer is interrupted
      val interruptedHandle =
        makeHandle(tableMeta, () => throw new InterruptedException("in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(1))).thenReturn(healthyHandle)
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(2))).thenReturn(interruptedHandle)
      val serverConnection = mock[ServerConnection]
      val server = newServer(requestHandler, serverConnection)

      val healthy = new BufferSendState(healthyTx, healthyBuffer, requestHandler, null)
      val interrupted = new BufferSendState(interruptedTx, interruptedBuffer, requestHandler, null)
      val failure = intercept[RapidsShuffleSendPrepareException] {
        server.doHandleTransferRequest(Seq(healthy, interrupted))
      }
      // read, and cleared, before anything else on this thread could be interrupted by it
      val interruptKept = Thread.interrupted()

      assert(interruptKept)
      assert(failure.getCause.isInstanceOf[InterruptedException])
      assertResult(Some(BufferSendState.Failed))(healthy.endedWith)
      assertResult(Some(BufferSendState.Failed))(interrupted.endedWith)
      verify(healthyBuffer, times(1)).close()
      verify(interruptedBuffer, times(1)).close()
      verify(server, times(0)).addToContinueQueue(any())
      verify(serverConnection, times(0)).send(
        any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]())
    }
  }
}
