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
import java.util

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{Arm, MetaUtils, RapidsBuffer, ShuffleMetadata}
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.{any, anyLong}
import org.mockito.Mockito._

import org.apache.spark.storage.ShuffleBlockBatchId

class RapidsShuffleServerSuite extends RapidsShuffleTestHelper with Arm {

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
        when(mockBuffer.size).thenReturn(deviceBuffer.getLength)
        when(mockBuffer.meta).thenReturn(mockMeta)
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
          assert(bss.hasNext)
          val mb = bss.next()
          val receiveBlocks = receiveWindow.next()
          compareRanges(bounceBuffer, receiveBlocks)
          assertResult(10000)(mb.getLength)
          assert(!bss.hasNext)
          bss.releaseAcquiredToCatalog()
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
          var buffs = bss.next()
          var receiveBlocks = receiveWindow.next()
          compareRanges(bounceBuffer, receiveBlocks)
          assert(bss.hasNext)
          bss.releaseAcquiredToCatalog()

          buffs = bss.next()
          receiveBlocks = receiveWindow.next()
          compareRanges(bounceBuffer, receiveBlocks)
          assert(!bss.hasNext)
          bss.releaseAcquiredToCatalog()

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
            bss.next()
            val receiveBlocks = receiveWindow.next()
            compareRanges(bounceBuffer, receiveBlocks)
            bss.releaseAcquiredToCatalog()
          }
          assert(!bss.hasNext)
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
        any(), any(), any(), any[ByteBuffer](), ac.capture())).thenReturn(mockTransaction)

      val mockRequestHandler = mock[RapidsShuffleRequestHandler]
      val rapidsBuffer = mock[RapidsBuffer]

      val bb = ByteBuffer.allocateDirect(123)
      withResource(new RefCountedDirectByteBuffer(bb)) { _ =>
        val tableMeta = MetaUtils.buildTableMeta(1, 456, bb, 100)
        when(rapidsBuffer.meta).thenReturn(tableMeta)
        when(rapidsBuffer.size).thenReturn(tableMeta.bufferMeta().size())
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
}
