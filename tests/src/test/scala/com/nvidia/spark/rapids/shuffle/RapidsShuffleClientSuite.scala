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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.format.{BufferMeta, TableMeta}
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

class RapidsShuffleClientSuite extends RapidsShuffleTestHelper {
  def prepareBufferReceiveState(
      tableMeta: TableMeta,
      bounceBuffer: BounceBuffer): BufferReceiveState = {
    val ptr = PendingTransferRequest(client, tableMeta, 123L, mockHandler)
    spy(new BufferReceiveState(bounceBuffer, Seq(ptr)))
  }

  def prepareBufferReceiveState(
      tableMetas: Seq[TableMeta],
      bounceBuffer: BounceBuffer): BufferReceiveState = {

    var tag = 123
    val ptrs = tableMetas.map { tm =>
      val ptr = PendingTransferRequest(client, tm, tag, mockHandler)
      tag = tag + 1
      ptr
    }

    spy(new BufferReceiveState(bounceBuffer, ptrs))
  }

  def verifyTableMeta(expected: TableMeta, actual: TableMeta): Unit = {
    assertResult(expected.rowCount())(actual.rowCount())
    assertResult(expected.packedMetaAsByteBuffer())(actual.packedMetaAsByteBuffer())
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
    val (tableMetas, response) =
      RapidsShuffleTestHelper.mockMetaResponse(mockTransaction, contigBuffSize, numBatches)

    // initialize metadata fetch
    client.doFetch(shuffleRequests.map(_._1), mockHandler)

    // the connection saw one request (for metadata)
    assertResult(1)(mockConnection.requests)

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

    assert(response.isClosed)
  }

  test("successful degenerate metadata fetch") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
    val numRows = 100000
    val numBatches = 3

    RapidsShuffleTestHelper.mockDegenerateMetaResponse(mockTransaction, numBatches)

    // initialize metadata fetch
    client.doFetch(shuffleRequests.map(_._1), mockHandler)

    // the connection saw one request (for metadata)
    assertResult(1)(mockConnection.requests)

    // upon a successful response, the `start()` method in the fetch handler
    // will be called with 3 expected batches
    verify(mockHandler, times(1)).start(ArgumentMatchers.eq(numBatches))

    // nothing gets queued to be received since it's just metadata
    verify(mockTransport, times(0)).queuePending(any())

    // ensure our handler (iterator) received 3 batches
    verify(mockHandler, times(numBatches)).batchReceived(any())
  }

  test("errored/cancelled metadata fetch") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)
      when(mockTransaction.getErrorMessage).thenReturn(Some("Error/cancel occurred"))

      val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
      val contigBuffSize = 100000
      val (_, response) = RapidsShuffleTestHelper.mockMetaResponse(
        mockTransaction, contigBuffSize, 3)

      client.doFetch(shuffleRequests.map(_._1), mockHandler)

      assertResult(1)(mockConnection.requests)

      // upon an errored response, the start handler will not be called
      verify(mockHandler, times(0)).start(any())

      // but the error handler will
      verify(mockHandler, times(1)).transferError(anyString(), isNull())

      // the transport will receive no pending requests (for buffers) for queuing
      verify(mockTransport, times(0)).queuePending(any())

      assert(response.isClosed)

      newMocks()
    }
  }

  test("exception in metadata fetch escalates to handler"){
    when(mockTransaction.getStatus).thenThrow(new RuntimeException("test exception"))
    val shuffleRequests = RapidsShuffleTestHelper.getShuffleBlocks
    val contigBuffSize = 100000
    var (_, response) = RapidsShuffleTestHelper.mockMetaResponse(
      mockTransaction, contigBuffSize, 3)

    client.doFetch(shuffleRequests.map(_._1), mockHandler)

    assertResult(1)(mockConnection.requests)

    // upon an errored response, the start handler will not be called
    verify(mockHandler, times(0)).start(any())

    // but the error handler will, and there will be an exception along.
    verify(mockHandler, times(1))
        .transferError(anyString(), any[RuntimeException]())

    // the transport will receive no pending requests (for buffers) for queuing
    verify(mockTransport, times(0)).queuePending(any())

    assert(response.isClosed)

    newMocks()
  }

  test("successful buffer fetch") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 25001
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)

    // 10000 in bytes ~ 2500 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we need 10 buffer-lengths to receive all of 25000 rows,
    // the extra one adds 1 receive. Note that each receive is 1 buffer, so that is
    // 11 receives expected.
    val sizePerBuffer = 10000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 11

    withResource(HostMemoryBuffer.allocate(100032)) { refHostBuffer =>
      var count = 0
      (0 until refHostBuffer.getLength.toInt)
          .foreach { off =>
            refHostBuffer.setByte(off, count.toByte)
            count = count + 1
            if (count >= sizePerBuffer) {
              count = 0
            }
          }

      closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
        val db = bounceBuffer.buffer.asInstanceOf[DeviceMemoryBuffer]
        withResource(refHostBuffer.slice(0, sizePerBuffer)) { slice =>
          db.copyFromHostBuffer(slice)
        }
        val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

        assert(brs.hasNext)

        // Kick off receives
        client.doIssueBufferReceives(brs)

        // If transactions are successful, we should have completed the receive
        assert(!brs.hasNext)

        // we would issue as many requests as required in order to get the full contiguous
        // buffer
        verify(mockConnection, times(expectedReceives))
            .receive(any[AddressLengthTag](), any[TransactionCallback]())

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
            .addBuffer(any(), dmbCaptor.capture(), any(), any(), any())

        val receivedBuff = dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer]
        assertResult(tableMeta.bufferMeta().size())(receivedBuff.getLength)

        withResource(HostMemoryBuffer.allocate(receivedBuff.getLength)) { hostBuff =>
          hostBuff.copyFromDeviceBuffer(receivedBuff)
          (0 until numRows).foreach { r =>
            assertResult(refHostBuffer.getByte(r))(hostBuff.getByte(r))
          }
        }

        // after closing, we should have freed our bounce buffers.
        assert(bounceBuffer.isClosed)
      }
    }
  }

  test("successful buffer fetch - but handler rejected it") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)
    when(mockHandler.batchReceived(any())).thenReturn(false) // reject incoming batches

    val numRows = 100
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
    val sizePerBuffer = 10000
    val expectedReceives = 1
    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

      assert(brs.hasNext)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      // If transactions are successful, we should have completed the receive
      assert(!brs.hasNext)

      // we would issue as many requests as required in order to get the full contiguous
      // buffer
      verify(mockConnection, times(expectedReceives))
          .receive(any[AddressLengthTag](), any[TransactionCallback]())

      // the mock connection keeps track of every receive length
      val totalReceived = mockConnection.receiveLengths.sum
      val numBuffersUsed = mockConnection.receiveLengths.size

      assertResult(tableMeta.bufferMeta().size())(totalReceived)
      assertResult(1)(numBuffersUsed)

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(1)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      verifyTableMeta(tableMeta, tmCaptor.getValue.asInstanceOf[TableMeta])
      verify(mockStorage, times(1))
          .addBuffer(any(), dmbCaptor.capture(), any(), any(), any())
      verify(mockCatalog, times(1)).removeBuffer(any())

      val receivedBuff = dmbCaptor.getValue.asInstanceOf[DeviceMemoryBuffer]
      assertResult(tableMeta.bufferMeta().size())(receivedBuff.getLength)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  test("successful buffer fetch multi-buffer") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 500
    val tableMetas =
      (0 until 5).map {
        _ => RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
      }

    // 20000 in bytes ~ 5000 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we can pack 5 device receives into a single bounce buffer
    val sizePerBuffer = 20000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 1

    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMetas, bounceBuffer)

      assert(brs.hasNext)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      // If transactions are successful, we should have completed the receive
      assert(!brs.hasNext)

      // we would issue as many requests as required in order to get the full contiguous
      // buffer
      verify(mockConnection, times(expectedReceives))
          .receive(any[AddressLengthTag](), any[TransactionCallback]())

      // the mock connection keeps track of every receive length
      val totalReceived = mockConnection.receiveLengths.sum
      val numBuffersUsed = mockConnection.receiveLengths.size

      val totalExpectedSize = tableMetas.map(tm => tm.bufferMeta().size()).sum
      assertResult(totalExpectedSize)(totalReceived)
      assertResult(1)(numBuffersUsed)

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(5)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      tableMetas.zipWithIndex.foreach { case (tm, ix) =>
        verifyTableMeta(tm, tmCaptor.getAllValues().get(ix).asInstanceOf[TableMeta])
      }

      verify(mockStorage, times(5))
          .addBuffer(any(), dmbCaptor.capture(), any(), any(), any())

      assertResult(totalExpectedSize)(
        dmbCaptor.getAllValues().toArray().map(_.asInstanceOf[DeviceMemoryBuffer].getLength).sum)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  test("successful buffer fetch multi-buffer, larger than a single bounce buffer") {
    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Success)

    val numRows = 500
    val tableMetas =
      (0 until 20).map {
        _ => RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)
      }

    // 20000 in bytes ~ 5000 rows (minus validity/offset buffers) worth of contiguous
    // single column int table, so we can pack 5 device receives into a single bounce buffer
    // we have 20 bounce buffers, so we expect in this case 3 receives.
    val sizePerBuffer = 20000
    // 5 receives (each with 2 buffers) makes for 100000 bytes + 1 receive for the remaining byte
    val expectedReceives = 3

    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMetas, bounceBuffer)

      assert(brs.hasNext)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      // If transactions are successful, we should have completed the receive
      assert(!brs.hasNext)

      // we would issue as many requests as required in order to get the full contiguous
      // buffer
      verify(mockConnection, times(expectedReceives))
          .receive(any[AddressLengthTag](), any[TransactionCallback]())

      // the mock connection keeps track of every receive length
      val totalReceived = mockConnection.receiveLengths.sum
      val numBuffersUsed = mockConnection.receiveLengths.size

      val totalExpectedSize = tableMetas.map(tm => tm.bufferMeta().size()).sum
      assertResult(totalExpectedSize)(totalReceived)
      assertResult(3)(numBuffersUsed)

      // we would perform 1 request to issue a `TransferRequest`, so the server can start.
      verify(mockConnection, times(1)).request(any(), any(), any[TransactionCallback]())

      // we will hand off a `DeviceMemoryBuffer` to the catalog
      val dmbCaptor = ArgumentCaptor.forClass(classOf[DeviceMemoryBuffer])
      val tmCaptor = ArgumentCaptor.forClass(classOf[TableMeta])
      verify(client, times(20)).track(any[DeviceMemoryBuffer](), tmCaptor.capture())
      tableMetas.zipWithIndex.foreach { case (tm, ix) =>
        verifyTableMeta(tm, tmCaptor.getAllValues().get(ix).asInstanceOf[TableMeta])
      }

      verify(mockStorage, times(20))
          .addBuffer(any(), dmbCaptor.capture(), any(), any(), any())

      assertResult(totalExpectedSize)(
        dmbCaptor.getAllValues().toArray().map(_.asInstanceOf[DeviceMemoryBuffer].getLength).sum)

      // after closing, we should have freed our bounce buffers.
      assert(bounceBuffer.isClosed)
    }
  }

  test("errored/cancelled buffer fetch") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)
      when(mockTransaction.getErrorMessage).thenReturn(Some("Error/cancel occurred"))

      val numRows = 100000
      val tableMeta =
        RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)

      // error condition, so it doesn't matter much what we set here, only the first
      // receive will happen
      val sizePerBuffer = numRows * 4 / 10
      closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
        val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

        assert(brs.hasNext)

        // Kick off receives
        client.doIssueBufferReceives(brs)

        // Errored transaction. Therefore we should not be done
        assert(brs.hasNext)

        // We should have called `transferError` in the `RapidsShuffleFetchHandler` and not
        // pass a throwable, since this was a transport-level exception we caught
        verify(mockHandler, times(1)).transferError(any(), isNull())

        // there was 1 receive, and the chain stopped because it wasn't successful
        verify(mockConnection, times(1)).receive(any[AddressLengthTag](), any())

        // we would have issued 1 request to issue a `TransferRequest` for the server to start
        verify(mockConnection, times(1)).request(any(), any(), any())

        // ensure we closed the BufferReceiveState => releasing the bounce buffers
        assert(bounceBuffer.isClosed)
      }

      newMocks()
    }
  }

  test("exception in buffer fetch escalates to handler") {
    when(mockTransaction.getStatus).thenThrow(new RuntimeException("test exception"))

    val numRows = 100000
    val tableMeta =
      RapidsShuffleTestHelper.prepareMetaTransferResponse(mockTransaction, numRows)

    // error condition, so it doesn't matter much what we set here, only the first
    // receive will happen
    val sizePerBuffer = numRows * 4 / 10
    closeOnExcept(getBounceBuffer(sizePerBuffer)) { bounceBuffer =>
      val brs = prepareBufferReceiveState(tableMeta, bounceBuffer)

      assert(brs.hasNext)

      // Kick off receives
      client.doIssueBufferReceives(brs)

      // Errored transaction. Therefore we should not be done
      assert(brs.hasNext)

      // We should have called `transferError` in the `RapidsShuffleFetchHandler`
      verify(mockHandler, times(1)).transferError(any(), any[RuntimeException]())

      // there was 1 receive, and the chain stopped because it wasn't successful
      verify(mockConnection, times(1)).receive(any[AddressLengthTag](), any())

      // we would have issued 1 request to issue a `TransferRequest` for the server to start
      verify(mockConnection, times(1)).request(any(), any(), any())

      // ensure we closed the BufferReceiveState => releasing the bounce buffers
      assert(bounceBuffer.isClosed)
    }

    newMocks()
  }

  def makeRequest(
      tag: Long, numRows: Long): (PendingTransferRequest, HostMemoryBuffer, TableMeta) = {
    val ptr = mock[PendingTransferRequest]
    val mockTable = RapidsShuffleTestHelper.mockTableMeta(numRows)
    when(ptr.getLength).thenReturn(mockTable.bufferMeta().size())
    when(ptr.tag).thenReturn(tag)
    when(ptr.tableMeta).thenReturn(mockTable)
    val buff = HostMemoryBuffer.allocate(mockTable.bufferMeta().size())
    fillBuffer(buff)
    (ptr, buff, mockTable)
  }

  def checkBuffer(
      source: HostMemoryBuffer,
      mockTable: TableMeta,
      consumed: ConsumedBatchFromBounceBuffer): Unit = {
    assertResult(mockTable.bufferMeta().size())(consumed.contigBuffer.getLength)
    assert(areBuffersEqual(source, consumed.contigBuffer))
  }

  class MockBlock(val hmb: HostMemoryBuffer) extends BlockWithSize {
    override def size: Long = hmb.getLength
  }

  case class ExpectedTagAndMaterialized(
      tag: Long,
      blocksInWindow: Int,
      materializedBatches: Int,
      bytesInWindow: Long,
      isLast: Boolean)

  def endToEndTest(buff: BounceBuffer,
      expected: Seq[ExpectedTagAndMaterialized],
      ptrBuffs: Seq[(PendingTransferRequest, HostMemoryBuffer, TableMeta)]): Unit = {
    withResource(ptrBuffs.map(_._2)) { sources =>
      withResource(new BufferReceiveState(buff, ptrBuffs.map(_._1))) { br =>
        val blocks = sources.map(x => new MockBlock(x))
        val sendWindow = new WindowedBlockIterator[MockBlock](blocks, 1000)

        expected.foreach {
          case ExpectedTagAndMaterialized(tag, inWindow, materialized, bytesInWindow, isLast) =>
            val state = br.next()
            assertResult(state.length)(bytesInWindow)
            assertResult(state.tag)(tag)

            val bb = state.memoryBuffer.get.asInstanceOf[DeviceMemoryBuffer]
            val ranges = sendWindow.next()
            assertResult(inWindow)(ranges.size)
            var offset = 0L

            ranges.foreach(r => {
              bb.copyFromHostBuffer(offset, r.block.hmb, r.rangeStart, r.rangeSize())
              offset = offset + r.rangeSize()
            })

            val consumed = br.consumeWindow()
            assertResult(materialized)(consumed.size)
            ranges.zip(consumed).foreach {
              case (range: BlockRange[MockBlock], c: ConsumedBatchFromBounceBuffer) =>
                checkBuffer(range.block.hmb, c.meta, c)
                c.contigBuffer.close()
            }
            assertResult(!isLast)(br.hasNext)
        }
      }
    }
  }

  test("one request spanning several bounce buffer lengths") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(1000)
      val ptrBuffs = rowCounts.zipWithIndex.map { case (c, ix) => makeRequest(ix + 1, c) }
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ExpectedTagAndMaterialized]()

      // 4 buffer lengths
      (0 until 4).foreach { _ =>
        expected.append(
          ExpectedTagAndMaterialized(
            tag = 1,
            blocksInWindow = 1,
            materializedBatches = 0,
            bytesInWindow = math.min(1000, remaining),
            isLast = false))
        remaining = remaining - 1000
      }

      // then a last one to finish it off
      expected.append(
        ExpectedTagAndMaterialized(
          tag = 1,
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("three requests within a bounce buffer") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(40, 50, 60)
      val ptrBuffs = rowCounts.zipWithIndex.map { case (c, ix) => makeRequest(ix + 1, c) }
      val remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ExpectedTagAndMaterialized]()
      expected.append(
        ExpectedTagAndMaterialized(
          tag = 1,
          blocksInWindow = 3,
          materializedBatches = 3,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("three requests spanning two bounce buffer lengths") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(40, 180, 100)
      val ptrBuffs = rowCounts.zipWithIndex.map { case (c, ix) => makeRequest(ix + 1, c) }
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ExpectedTagAndMaterialized]()
      expected.append(
        ExpectedTagAndMaterialized(
          tag = 1,
          blocksInWindow = 3,
          materializedBatches = 2,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ExpectedTagAndMaterialized(
          tag = 3,
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }

  test ("two requests larger than the bounce buffer length") {
    val bb = closeOnExcept(getBounceBuffer(1000)) { buff =>
      val rowCounts = Seq(300, 300)
      val ptrBuffs = rowCounts.zipWithIndex.map { case (c, ix) => makeRequest(ix + 1, c) }
      var remaining = ptrBuffs.map(_._2.getLength).sum
      val expected = new ArrayBuffer[ExpectedTagAndMaterialized]()
      expected.append(
        ExpectedTagAndMaterialized(
          tag = 1,
          blocksInWindow = 1,
          materializedBatches = 0,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ExpectedTagAndMaterialized(
          tag = 1,
          blocksInWindow = 2,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = false))
      remaining = remaining - 1000

      expected.append(
        ExpectedTagAndMaterialized(
          tag = 2,
          blocksInWindow = 1,
          materializedBatches = 1,
          bytesInWindow = math.min(1000, remaining),
          isLast = true))

      endToEndTest(buff, expected, ptrBuffs)
      buff
    }
    assert(bb.isClosed)
  }
}
