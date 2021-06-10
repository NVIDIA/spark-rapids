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

import com.nvidia.spark.rapids.{RapidsBuffer, ShuffleReceivedBufferId}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._

import org.apache.spark.shuffle.{RapidsShuffleFetchFailedException, RapidsShuffleTimeoutException}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class RapidsShuffleIteratorSuite extends RapidsShuffleTestHelper {
  test("inability to get a client raises a fetch failure") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val cl = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      mockConf,
      mockTransport,
      blocksByAddress,
      testMetricsUpdater,
      Array.empty,
      mockCatalog,
      123)

    when(mockTransaction.getStatus).thenReturn(TransactionStatus.Error)

    when(mockTransport.makeClient(any(), any())).thenThrow(new IllegalStateException("Test"))

    assert(cl.hasNext)
    assertThrows[RapidsShuffleFetchFailedException](cl.next())

    // not invoked, since we never blocked
    verify(testMetricsUpdater, times(0))
        .update(any(), any(), any(), any())
  }

  test("a transport error/cancellation raises a fetch failure") {
    Seq(TransactionStatus.Error, TransactionStatus.Cancelled).foreach { status =>
      when(mockTransaction.getStatus).thenReturn(status)

      val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

      val cl = spy(new RapidsShuffleIterator(
        RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
        mockConf,
        mockTransport,
        blocksByAddress,
        testMetricsUpdater,
        Array.empty,
        mockCatalog,
        123))

      val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
      when(mockTransport.makeClient(any(), any())).thenReturn(client)
      doNothing().when(client).doFetch(any(), ac.capture())
      cl.start()

      val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]
      handler.transferError("Test", null)

      assert(cl.hasNext)
      assertThrows[RapidsShuffleFetchFailedException](cl.next())

      verify(mockTransport, times(1)).cancelPending(handler)

      verify(testMetricsUpdater, times(1))
          .update(any(), any(), any(), any())
      assertResult(0)(testMetricsUpdater.totalRemoteBlocksFetched)
      assertResult(0)(testMetricsUpdater.totalRemoteBytesRead)
      assertResult(0)(testMetricsUpdater.totalRowsFetched)

      newMocks()
    }
  }

  test("a transport exception raises a fetch failure with the cause exception") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val cl = spy(new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      mockConf,
      mockTransport,
      blocksByAddress,
      testMetricsUpdater,
      Array.empty,
      mockCatalog,
      123))

    val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
    when(mockTransport.makeClient(any(), any())).thenReturn(client)
    doNothing().when(client).doFetch(any(), ac.capture())
    cl.start()

    val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]
    val causeException = new RuntimeException("some exception")
    handler.transferError("Test", causeException)

    assert(cl.hasNext)
    assertThrows[RapidsShuffleFetchFailedException] {
      try {
        cl.next()
      } catch {
        case rsffe: RapidsShuffleFetchFailedException =>
          val cause = rsffe.getCause
          assertResult(cause)(causeException)
          throw rsffe
      }
    }

    verify(mockTransport, times(1)).cancelPending(handler)

    verify(testMetricsUpdater, times(1))
        .update(any(), any(), any(), any())
    assertResult(0)(testMetricsUpdater.totalRemoteBlocksFetched)
    assertResult(0)(testMetricsUpdater.totalRemoteBytesRead)
    assertResult(0)(testMetricsUpdater.totalRowsFetched)

    newMocks()
  }

  test("a timeout while waiting for batches raises a fetch failure") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val cl = spy(new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      mockConf,
      mockTransport,
      blocksByAddress,
      testMetricsUpdater,
      Array.empty,
      mockCatalog,
      123))

    val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
    when(mockTransport.makeClient(any(), any())).thenReturn(client)
    doNothing().when(client).doFetch(any(), ac.capture())
    cl.start()

    val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]

    // signal a timeout to the iterator
    when(cl.pollForResult(any())).thenReturn(None)

    assertThrows[RapidsShuffleTimeoutException](cl.next())

    verify(testMetricsUpdater, times(1))
        .update(any(), any(), any(), any())
    assertResult(0)(testMetricsUpdater.totalRemoteBlocksFetched)
    assertResult(0)(testMetricsUpdater.totalRemoteBytesRead)
    assertResult(0)(testMetricsUpdater.totalRowsFetched)

    newMocks()
  }

  test("a new good batch is queued") {
    val blocksByAddress = RapidsShuffleTestHelper.getBlocksByAddress

    val cl = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"),
      mockConf,
      mockTransport,
      blocksByAddress,
      testMetricsUpdater,
      Array.empty,
      mockCatalog,
      123)

    val ac = ArgumentCaptor.forClass(classOf[RapidsShuffleFetchHandler])
    when(mockTransport.makeClient(any(), any())).thenReturn(client)
    doNothing().when(client).doFetch(any(), ac.capture())
    val bufferId = ShuffleReceivedBufferId(1)
    val mockBuffer = mock[RapidsBuffer]

    val cb = new ColumnarBatch(Array.empty, 10)

    when(mockBuffer.getColumnarBatch(Array.empty)).thenReturn(cb)
    when(mockCatalog.acquireBuffer(any[ShuffleReceivedBufferId]())).thenReturn(mockBuffer)
    doNothing().when(mockCatalog).removeBuffer(any())
    cl.start()

    val handler = ac.getValue.asInstanceOf[RapidsShuffleFetchHandler]
    handler.start(1)
    handler.batchReceived(bufferId)

    verify(mockTransport, times(0)).cancelPending(handler)

    assert(cl.hasNext)
    assertResult(cb)(cl.next())
    assertResult(1)(testMetricsUpdater.totalRemoteBlocksFetched)
    assertResult(mockBuffer.size)(testMetricsUpdater.totalRemoteBytesRead)
    assertResult(10)(testMetricsUpdater.totalRowsFetched)

    newMocks()
  }
}
