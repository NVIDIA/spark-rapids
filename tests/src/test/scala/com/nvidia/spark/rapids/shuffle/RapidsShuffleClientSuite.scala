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

import ai.rapids.cudf.{DeviceMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.format.{BufferMeta, TableMeta}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class RapidsShuffleClientSuite extends RapidsShuffleTestHelper {
  def prepareBufferReceiveState(
      tableMeta: TableMeta,
      bounceBuffers: Seq[MemoryBuffer]): BufferReceiveState = {
    val brs = spy(new BufferReceiveState(mockTransport, bounceBuffers))
    val tag = 123
    val ptr = PendingTransferRequest(client, tableMeta, tag, mockHandler)
    val targetAlt = mock[AddressLengthTag]
    when(targetAlt.cudaCopyFrom(any(), any())).thenReturn(bounceBuffers.head.getLength)
    brs.addRequest(ptr)
    brs
  }

  def verifyTableMeta(expected: TableMeta, actual: TableMeta): Unit = {
    assertResult(expected.rowCount())(actual.rowCount())
    assertResult(expected.columnMetasLength())(actual.columnMetasLength())
    verifyBufferMeta(expected.bufferMeta, actual.bufferMeta)
  }

  def verifyBufferMeta(expected: BufferMeta, actual: BufferMeta): Unit = {
    assertResult(expected.id)(actual.id)
    assertResult(expected.size)(actual.size)
    assertResult(expected.uncompressedSize)(actual.uncompressedSize)
    assertResult(expected.codecBufferDescrsLength)(actual.codecBufferDescrsLength)
    (0 until expected.codecBufferDescrsLength).foreach { i =>
      val expectedDescr = expected.codecBufferDescrs(i)
      val actualDescr = actual.codecBufferDescrs(i)
      assertResult(expectedDescr.codec)(actualDescr.codec)
      assertResult(expectedDescr.compressedOffset)(actualDescr.compressedOffset)
      assertResult(expectedDescr.compressedSize)(actualDescr.compressedSize)
      assertResult(expectedDescr.uncompressedOffset)(actualDescr.uncompressedOffset)
      assertResult(expectedDescr.uncompressedSize)(actualDescr.uncompressedSize)
    }
  }

  test("successful metadata fetch") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
    val contigBuffSize = 100000
    val numBatches = 3
    val tableMetas =
      RapidsShuffleTestHelper.mockMetaResponse(mockTransport, contigBuffSize, numBatches)

    // initialize metadata fetch
    client.doFetch(shuffleRequests.map(_._1), mockHandler)

    // the connection saw one request (for metadata)
    assertResult(1)(mockConnection.requests.size)

    // upon a successful response, the `start()` method in the fetch handler
    // will be called with 3 expected batches
    verify(mockHandler, times(1)).start(ArgumentMatchers.eq(numBatches))

    // the transport will receive 3 pending requests (for buffers) for queuing
    val ac = ArgumentCaptor.forClass(classOf[Seq[PendingTransferRequest]])
    verify(mockTransport, times(1)).queuePending(ac.capture())
    val ptrs = ac.getValue.asInstanceOf[Seq[PendingTransferRequest]]
    assertResult(numBatches)(ptrs.size)

    // we check their metadata below
    (0 until numBatches).foreach { t =>
      val expected = tableMetas(t)
      val tm = ptrs(t).tableMeta
      verifyTableMeta(expected, tm)
    }
  }

  test("errored/cancelled metadata fetch") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)
      when(mockTransaction.getErrorMessage).thenReturn(Some("Error/cancel occurred"))

      val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
      val contigBuffSize = 100000
      RapidsShuffleTestHelper.mockMetaResponse(
        mockTransport, contigBuffSize, 3)

      client.doFetch(shuffleRequests.map(_._1), mockHandler)

      assertResult(1)(mockConnection.requests.size)

      // upon an errored response, the start handler will not be called
      verify(mockHandler, times(0)).start(any())

      // but the error handler will
      verify(mockHandler, times(1)).transferError(anyString())

      // the transport will receive no pending requests (for buffers) for queuing
      verify(mockTransport, times(0)).queuePending(any())

      newMocks()
    }
  }

  test("successful buffer fetch") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 25001
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransport, numRows)

    // assume we obtained 2 bounce buffers for reuse
    val numBuffers = 2
    // 10000 in bytes ~ 2500 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we need 10 buffer-lengths to receive all of 25000 rows,
    // the extra one adds 1 receive. Note that each receive is 2 buffers (except for the last one),
    // so that is 6 receives expected.
    val sizePerBuffer = 10000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 6
    val bbs = (0 until numBuffers).map( _ => DeviceMemoryBuffer.allocate(sizePerBuffer))

    withResource(bbs) { bounceBuffers =>
      val brs = prepareBufferReceiveState(tableMeta, bounceBuffers)

      assert(!brs.isDone)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      // If transactions are successful, we should have completed the receive
      assert(brs.isDone)

      // we would issue as many requests as required in order to get the full contiguous
      // buffer
      verify(mockConnection, times(expectedReceives))
        .receive(any[Seq[AddressLengthTag]](), any[TransactionCallback]())

      // the mock connection keeps track of every receive length
      val totalReceived = mockConnection.receiveLengths.sum
      val numBuffersUsed = mockConnection.receiveLengths.size

      assertResult(tableMeta.bufferMeta().size())(totalReceived)
      assertResult(11)(numBuffersUsed)

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(1)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      verifyTableMeta(tableMeta, tmCaptor.getValue.asInstanceOf[TableMeta])
      verify(mockStorage, times(1))
          .addBuffer(any(), dmbCaptor.capture(), any(), any())

      assertResult(tableMeta.bufferMeta().size())(
        dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer].getLength)

      // after closing, we should have freed our bounce buffers.
      val capturedBuffers: ArgumentCaptor[Seq[MemoryBuffer]] =
        ArgumentCaptor.forClass(classOf[Seq[MemoryBuffer]])
      verify(mockTransport, times(1))
        .freeReceiveBounceBuffers(capturedBuffers.capture())

      val freedBuffers = capturedBuffers.getValue
      assertResult(bounceBuffers)(freedBuffers)
    }
  }

  test("errored/cancelled buffer fetch") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)
      when(mockTransaction.getErrorMessage).thenReturn(Some("Error/cancel occurred"))

      val numRows = 100000
      val tableMeta =
        RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransport, numRows)

      // assume we obtained 2 bounce buffers, for reuse
      val numBuffers = 2

      // error condition, so it doesn't matter much what we set here, only the first
      // receive will happen
      val sizePerBuffer = numRows * 4 / 10
      val bbs = (0 until numBuffers).map(_ => DeviceMemoryBuffer.allocate(sizePerBuffer))
      withResource(bbs) { bounceBuffers =>
        val brs = prepareBufferReceiveState(tableMeta, bounceBuffers)

        assert(!brs.isDone)

        // Kick off receives
        client.doIssueBufferReceives(brs)

        // Errored transaction. Therefore we should not be done
        assert(!brs.isDone)

        // We should have called `transferError` in the `RapidsShuffleFetchHandler`
        verify(mockHandler, times(1)).transferError(any())

        // there was 1 receive, and the chain stopped because it wasn't successful
        verify(mockConnection, times(1)).receive(any[Seq[AddressLengthTag]](), any())

        // we would have issued 1 request to issue a `TransferRequest` for the server to start
        verify(mockConnection, times(1)).request(any(), any(), any())

        // ensure we closed the BufferReceiveState => releasing the bounce buffers
        val capturedBuffers: ArgumentCaptor[Seq[MemoryBuffer]] =
          ArgumentCaptor.forClass(classOf[Seq[MemoryBuffer]])
        verify(mockTransport, times(1))
            .freeReceiveBounceBuffers(capturedBuffers.capture())

        val freedBuffers = capturedBuffers.getValue
        assertResult(bounceBuffers)(freedBuffers)
      }

      newMocks()
    }
  }
}
