/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import java.io.IOException
import java.nio.ByteBuffer
import java.util

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{MetaUtils, RapidsBuffer, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito._

import org.apache.spark.storage.ShuffleBlockBatchId

class RapidsShuffleServerSuite extends RapidsShuffleTestHelper {

  def setupMocks(deviceBuffers: Seq[DeviceMemoryBuffer]): (RapidsShuffleRequestHandler,
      Seq[RapidsBuffer], util.HashMap[RapidsBuffer, Int]) = {

    val numCloses = new util.HashMap[RapidsBuffer, Int]()
    val mockBuffers = deviceBuffers.map { deviceBuffer =>
      withResource(HostMemoryBuffer.allocate(deviceBuffer.getLength)) { hostBuff =>
        fillBuffer(hostBuff)
        deviceBuffer.copyFromHostBuffer(hostBuff)
        val mockBuffer = mock[RapidsBuffer]
        val mockMeta = RapidsShuffleTestHelper.mockTableMeta(100000)
        when(mockBuffer.copyToMemoryBuffer(anyLong(), any[MemoryBuffer](), anyLong(), anyLong(),
          any[Cuda.Stream]())).thenAnswer { invocation =>
          // start at 1 close, since we'll need to close at refcount 0 too
          val newNumCloses = numCloses.getOrDefault(mockBuffer, 1) + 1
          numCloses.put(mockBuffer, newNumCloses)
          val srcOffset = invocation.getArgument[Long](0)
          val dst = invocation.getArgument[MemoryBuffer](1)
          val dstOffset = invocation.getArgument[Long](2)
          val length = invocation.getArgument[Long](3)
          val stream = invocation.getArgument[Cuda.Stream](4)
          dst.copyFromMemoryBuffer(dstOffset, deviceBuffer, srcOffset, length, stream)
        }
        when(mockBuffer.getMemoryUsedBytes).thenReturn(deviceBuffer.getLength)
        when(mockBuffer.getMeta).thenReturn(mockMeta)
        mockBuffer
      }
    }

    val handler = new RapidsShuffleRequestHandler {
      var acquiredTables = Seq[Int]()
      override def getShuffleBufferMetas(
          shuffleBlockBatchId: ShuffleBlockBatchId): Seq[TableMeta] = {
        throw new NotImplementedError("getShuffleBufferMetas")
      }

      override def acquireShuffleBuffer(tableId: Int): RapidsBuffer = {
        acquiredTables = acquiredTables :+ tableId
        mockBuffers(tableId)
      }
    }
    (handler, mockBuffers, numCloses)
  }

  class MockBlockWithSize(val b: DeviceMemoryBuffer) extends BlockWithSize {
    override def size: Long = b.getLength
  }

  def compareRanges(
      bounceBuffer: SendBounceBuffers,
      receiveBlocks: Seq[BlockRange[MockBlockWithSize]]): Unit = {
    var bounceBuffOffset = 0L
    receiveBlocks.foreach { range =>
      val deviceBuff = range.block.b
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

  test("sending tables that fit within one bounce buffer") {
    val mockTx = mock[Transaction]
    val transferRequest = RapidsShuffleTestHelper.prepareMetaTransferRequest(10, 1000)
    when(mockTx.releaseMessage()).thenReturn(transferRequest)

    val bb = closeOnExcept(getSendBounceBuffer(10000)) { bounceBuffer =>
      withResource((0 until 10).map(_ => DeviceMemoryBuffer.allocate(1000))) { deviceBuffers =>
        val receiveSide = deviceBuffers.map(b => new MockBlockWithSize(b))
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        val (handler, mockBuffers, numCloses) = setupMocks(deviceBuffers)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          assert(bss.hasMoreSends)
          withResource(bss.getBufferToSend()) { mb =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks)
            assertResult(10000)(mb.getLength)
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
            mockBuffers.foreach { b: RapidsBuffer =>
              // should have seen 2 closes, one for BufferSendState acquiring for metadata
              // and the second acquisition for copying
              verify(b, times(numCloses.get(b))).close()
            }
          }
        }
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
      withResource((0 until 20).map(_ => DeviceMemoryBuffer.allocate(1000))) { deviceBuffers =>
        val receiveSide = deviceBuffers.map(b => new MockBlockWithSize(b))
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        val (handler, mockBuffers, numCloses) = setupMocks(deviceBuffers)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks)
            assert(bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }

          withResource(bss.getBufferToSend()) { _ =>
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks)
            assert(!bss.hasMoreSends)
            bss.releaseAcquiredToCatalog()
          }

          mockBuffers.foreach { b: RapidsBuffer =>
            // should have seen 2 closes, one for BufferSendState acquiring for metadata
            // and the second acquisition for copying
            verify(b, times(numCloses.get(b))).close()
          }
        }
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
      withResource((0 until 20).map(_ => DeviceMemoryBuffer.allocate(123000))) { deviceBuffers =>
        val (handler, mockBuffers, numCloses) = setupMocks(deviceBuffers)

        val receiveSide = deviceBuffers.map(b => new MockBlockWithSize(b))
        val receiveWindow = new WindowedBlockIterator[MockBlockWithSize](receiveSide, 10000)
        withResource(new BufferSendState(mockTx, bounceBuffer, handler)) { bss =>
          (0 until 246).foreach { _ =>
            withResource(bss.getBufferToSend()) { _ =>
              val receiveBlocks = receiveWindow.next()
              compareRanges(bounceBuffer, receiveBlocks)
              bss.releaseAcquiredToCatalog()
            }
          }
          assert(!bss.hasMoreSends)
        }
        mockBuffers.foreach { b: RapidsBuffer =>
          verify(b, times(numCloses.get(b))).close()
        }
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
      val rapidsBuffer = mock[RapidsBuffer]

      val bb = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
        when(rapidsBuffer.getMeta).thenReturn(tableMeta)
        when(rapidsBuffer.getMemoryUsedBytes).thenReturn(tableMeta.bufferMeta().size())
        when(mockRequestHandler.acquireShuffleBuffer(ArgumentMatchers.eq(1)))
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

        val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null)
        server.doHandleTransferRequest(Seq(bss))
        val cb = ac.getValue.asInstanceOf[TransactionCallback]
        cb(mockTransaction)
        server.doHandleTransferRequest(Seq(bss)) //Error
        cb(mockTransaction)
        // bounce buffers are freed
        verify(mockSendBuffer, times(1)).close()
        // acquire 3 times, and close 3 times
        verify(mockRequestHandler, times(3))
          .acquireShuffleBuffer(ArgumentMatchers.eq(1))
        verify(rapidsBuffer, times(3)).close()
      }
    }
  }

  test("when we fail to prepare a send, throw if nothing can be handled") {
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

      val mockServerConnection = mock[ServerConnection]
      val mockRequestHandler = mock[RapidsShuffleRequestHandler]
      val rapidsBuffer = mock[RapidsBuffer]

      val bb = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
        when(rapidsBuffer.getMeta).thenReturn(tableMeta)
        when(rapidsBuffer.getMemoryUsedBytes).thenReturn(tableMeta.bufferMeta().size())
        when(mockRequestHandler.acquireShuffleBuffer(ArgumentMatchers.eq(1)))
            .thenReturn(rapidsBuffer)

        val server = spy(new RapidsShuffleServer(
          mockTransport,
          mockServerConnection,
          RapidsShuffleTestHelper.makeMockBlockManager("1", "foo"),
          mockRequestHandler,
          mockExecutor,
          mockBssExecutor,
          mockConf))

        server.start()

        val ioe = new IOException("mmap failed in test")

        when(rapidsBuffer.copyToMemoryBuffer(any(), any(), any(), any(), any()))
            .thenAnswer(_ => throw ioe)

        val bss = new BufferSendState(mockTransaction, mockSendBuffer, mockRequestHandler, null)
        // if nothing else can be handled, we throw
        assertThrows[IllegalStateException] {
          try {
            server.doHandleTransferRequest(Seq(bss))
          } catch {
            case e: Throwable =>
              assertResult(1)(e.getSuppressed.length)
              assertResult(ioe)(e.getSuppressed()(0).getCause)
              throw e
          }
        }

        // since nothing could be handled, we don't try again
        verify(server, times(0)).addToContinueQueue(any())

        // bounce buffers are freed
        verify(mockSendBuffer, times(1)).close()

        // acquire 2 times, 1 to make the ranges, and the 2 before the copy
        // close 2 times corresponding to each open
        verify(mockRequestHandler, times(2))
            .acquireShuffleBuffer(ArgumentMatchers.eq(1))
        verify(rapidsBuffer, times(2)).close()
      }
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

      def makeMockBuffer(tableId: Int, bb: ByteBuffer): RapidsBuffer = {
        val rapidsBuffer = mock[RapidsBuffer]
        val tableMeta = MetaUtils.buildTableMeta(tableId, 456, bb, 100)
        when(rapidsBuffer.getMeta).thenReturn(tableMeta)
        when(rapidsBuffer.getMemoryUsedBytes).thenReturn(tableMeta.bufferMeta().size())
        when(mockRequestHandler.acquireShuffleBuffer(ArgumentMatchers.eq(tableId)))
            .thenReturn(rapidsBuffer)
        rapidsBuffer
      }

      val bb = ByteBuffer.allocateDirect(123)
      val bb2 = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        withResource(new RefCountedDirectByteBuffer(bb2)) { _ =>
          val rapidsBuffer = makeMockBuffer(1, bb)
          val rapidsBuffer2 = makeMockBuffer(2, bb2)

          // error with copy
          when(rapidsBuffer.copyToMemoryBuffer(any(), any(), any(), any(), any()))
              .thenAnswer(_ => {
                throw new IOException("mmap failed in test")
              })

          // successful copy
          doNothing()
              .when(rapidsBuffer2)
              .copyToMemoryBuffer(any(), any(), any(), any(), any())

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
            mockTransaction, mockSendBuffer, mockRequestHandler, null)

          val bssSuccess = spy(new BufferSendState(
            mockTransaction2, mockSendBuffer, mockRequestHandler, null))

          when(bssSuccess.hasMoreSends)
              .thenReturn(true) // send 1 bounce buffer length
              .thenReturn(false)

          // if something else can be handled we don't throw, and re-queue
          server.doHandleTransferRequest(Seq(bssFailed, bssSuccess))

          val cb = ac.getValue.asInstanceOf[TransactionCallback]
          cb(mockTransaction)

          verify(server, times(1)).addToContinueQueue(any())

          // the bounce buffer is freed 1 time for `bssSuccess`, but not for `bssFailed`
          verify(mockSendBuffer, times(1)).close()

          // acquire/close 4 times =>
          // we had two requests for 1 buffer, and each request acquires 2 times and closes
          // 2 times.
          verify(mockRequestHandler, times(2))
              .acquireShuffleBuffer(ArgumentMatchers.eq(1))
          verify(mockRequestHandler, times(2))
              .acquireShuffleBuffer(ArgumentMatchers.eq(2))
          verify(rapidsBuffer, times(2)).close()
          verify(rapidsBuffer2, times(2)).close()
        }
      }
    }
  }
}
