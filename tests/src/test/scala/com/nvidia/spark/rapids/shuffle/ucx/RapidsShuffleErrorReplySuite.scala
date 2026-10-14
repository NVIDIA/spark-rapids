/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.shuffle.ucx

import java.nio.ByteBuffer
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, LinkedBlockingQueue,
  TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.DeviceMemoryBuffer
import com.nvidia.spark.rapids.{MetaUtils, RapidsShuffleHandle, ShuffleMetadata}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.jni.RmmSpark
import com.nvidia.spark.rapids.shuffle._
import com.nvidia.spark.rapids.spill.SpillableDeviceBufferHandle
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.{any, anyInt, anyLong, contains, isNull}
import org.mockito.Mockito._

import org.apache.spark.shuffle.rapids.RapidsShuffleFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId}

/**
 * What a requester does when the server answers its transfer request with an error reply. The
 * reply reaches the client through the real UCX client connection and transaction, with only
 * UCX mocked, because the transaction's callback wrapper drops whatever the client's callback
 * throws. The suite is in the ucx package so that it can answer a request through the response
 * handler the connection registers, as UCX does.
 */
class RapidsShuffleErrorReplySuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L

  private val reason = "the shuffle output it asked for is gone"

  /** A client of executor 2 whose requests are answered by the test rather than sent. */
  private class Requester {
    private val ucx = mock[UCX]
    private val txIds = new AtomicLong()
    when(ucx.getNextTransactionId).thenAnswer(_ => txIds.incrementAndGet())
    private val unanswered = new LinkedBlockingQueue[UCXAmCallback]()
    doAnswer { invocation =>
      unanswered.add(invocation.getArgument[UCXAmCallback](1))
      null
    }.when(ucx).registerResponseHandler(any(), any())

    val client = new RapidsShuffleClient(
      new UCXClientConnection(2L, ucx, mock[UCXShuffleTransport]),
      mockTransport, mockExecutor, mockCopyExecutor, mockCatalog)

    /** Answers the oldest request still waiting for a response, as UCX delivers one. */
    def answer(response: ByteBuffer): Unit = {
      val callback = unanswered.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(callback != null, "the client sent no request to answer")
      callback.onSuccess(UCXActiveMessage(0, 0L, forceRndv = false),
        new MetadataTransportBuffer(new RefCountedDirectByteBuffer(response)))
    }
  }

  private def tableMeta(tableId: Int, size: Long): TableMeta =
    MetaUtils.buildTableMeta(tableId, size, ByteBuffer.allocate(8), 100)

  private def reader(): RapidsShuffleFetchHandler = {
    val handler = mock[RapidsShuffleFetchHandler]
    when(handler.getTaskIds).thenReturn(Array[Long](1))
    handler
  }

  /** Runs `body` with a receive bounce buffer of `size` bytes, closing it if nothing did. */
  private def withBounceBuffer(size: Long)(body: BounceBuffer => Unit): Unit = {
    val bounceBuffer = getBounceBuffer(size)
    try {
      body(bounceBuffer)
    } finally {
      if (!bounceBuffer.isClosed) {
        bounceBuffer.close()
      }
    }
  }

  test("an error reply before the first window ends the receive state once") {
    RmmSpark.removeAllCurrentThreadAssociation()
    val requester = new Requester
    val (first, second) = (reader(), reader())
    // tables 1 to 7, alternating between two readers
    val requests = (1 to 7).map { id =>
      PendingTransferRequest(requester.client, tableMeta(id, 100L * id),
        if (id % 2 == 1) first else second)
    }
    val closes = new AtomicInteger()
    withBounceBuffer(4096) { bounceBuffer =>
      val state = spy(new BufferReceiveState(123L, bounceBuffer, requests,
        () => closes.incrementAndGet()))
      requester.client.doIssueBufferReceives(state)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(1 to 7, reason))

      // each reader with a request in the state fails once, told of the first few buffers
      Seq(first, second).foreach { handler =>
        verify(handler, times(1)).transferError(
          contains(s"buffers 1, 2, 3, 4, 5 and 2 more: $reason"), isNull())
      }
      // the receive pool slot, the in-flight bytes and the transport's entry are returned
      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      verify(mockTransport, times(1)).doneBytesInFlight(2800L)
    }
  }

  test("an error reply with a window still to consume ends the state once it is consumed") {
    RmmSpark.removeAllCurrentThreadAssociation()
    val requester = new Requester
    val (delivered, failed) = (reader(), reader())
    // the first window holds the first table and half of the second
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 512), delivered),
      PendingTransferRequest(requester.client, tableMeta(2, 1024), failed))
    val closes = new AtomicInteger()
    withBounceBuffer(1024) { bounceBuffer =>
      val state = spy(new BufferReceiveState(123L, bounceBuffer, requests,
        () => closes.incrementAndGet()))
      requester.client.doIssueBufferReceives(state)
      // the first window arrives, and UCX receives it into the slot
      state.getBufferWhenReady(_ => (), 1024)
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      // UCX can still be writing the window, so the slot stays with the state
      assert(!bounceBuffer.isClosed)
      assertResult(0)(closes.get)

      val windowReceived = mock[Transaction]
      when(windowReceived.getStatus).thenReturn(TransactionStatus.Success)
      requester.client.doHandleBounceBufferReceive(windowReceived, state)

      verify(delivered, times(1)).batchReceived(any())
      verify(delivered, never()).transferError(any(), any())
      verify(failed, times(1)).transferError(contains(reason), isNull())
      verify(state, times(1)).close()
      assert(bounceBuffer.isClosed)
      assertResult(1)(closes.get)
      // the delivered table's bytes come back with its batch, the failed one's with the reply
      verify(mockTransport, times(1)).doneBytesInFlight(512L)
      verify(mockTransport, times(1)).doneBytesInFlight(1024L)
    }
  }

  test("an error reply after the requester ended the state itself releases nothing again") {
    RmmSpark.removeAllCurrentThreadAssociation()
    val requester = new Requester
    val (first, second) = (reader(), reader())
    // the window cannot be handed to its reader, so the copy thread ends the state itself
    when(first.batchReceived(any())).thenThrow(new IllegalStateException("reader failed in test"))
    val requests = Seq(
      PendingTransferRequest(requester.client, tableMeta(1, 1024), first),
      PendingTransferRequest(requester.client, tableMeta(2, 512), second))
    val closes = new AtomicInteger()
    withBounceBuffer(1024) { bounceBuffer =>
      val state = spy(new BufferReceiveState(123L, bounceBuffer, requests,
        () => closes.incrementAndGet()))
      requester.client.doIssueBufferReceives(state)
      state.getBufferWhenReady(_ => (), 1024)
      val windowReceived = mock[Transaction]
      when(windowReceived.getStatus).thenReturn(TransactionStatus.Success)
      requester.client.doHandleBounceBufferReceive(windowReceived, state)
      assertResult(1)(closes.get)

      // the server, which does not know, fails the request's next window
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(2), reason))
      verify(state, times(1)).close()
      assertResult(1)(closes.get)
    }
  }

  test("a reader waiting on a transfer that the server fails gets a fetch failure") {
    val requester = new Requester
    val bounceBuffers = new ArrayBuffer[BounceBuffer]()
    val closes = new AtomicInteger()
    when(mockTransport.makeClient(any())).thenReturn(requester.client)
    // as the transport's throttle does, issuing every queued request in one receive state
    val states = new ArrayBuffer[BufferReceiveState]()
    doAnswer { invocation =>
      val bounceBuffer = getBounceBuffer(4096)
      bounceBuffers += bounceBuffer
      val state = spy(new BufferReceiveState(123L, bounceBuffer,
        invocation.getArgument[Seq[PendingTransferRequest]](0), () => closes.incrementAndGet()))
      states += state
      requester.client.issueBufferReceives(state)
      null
    }.when(mockTransport).queuePending(any())

    val taskId = 1L
    // far longer than the test takes, but short enough that a reader that times out shows it
    val timeoutSeconds = 10L
    val blocks: collection.Seq[(BlockId, Long, Int)] =
      RapidsShuffleTestHelper.getShuffleBlocks.toSeq
    val iterator = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), mockConf, mockTransport,
      Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])](
        (RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), blocks)),
      testMetricsUpdater, Array.empty, taskId, mockCatalog, timeoutSeconds)
    val failure = new AtomicReference[Throwable]()
    val readerThread = new Thread(() => {
      RmmSpark.currentThreadIsDedicatedToTask(taskId)
      try {
        iterator.next()
      } catch {
        case t: Throwable => failure.set(t)
      }
    }, "shuffle-reader")
    readerThread.setDaemon(true)
    try {
      readerThread.start()
      requester.answer(
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 1000), tableMeta(2, 2000))))
      requester.answer(ShuffleMetadata.buildTransferErrorResponse(Seq(1, 2), reason))
      readerThread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))

      assert(failure.get.isInstanceOf[RapidsShuffleFetchFailedException],
        s"the reader ended with ${failure.get}")
      assert(failure.get.getMessage.contains(reason))
      assertResult(1)(states.size)
      verify(states.head, times(1)).close()
      assert(bounceBuffers.forall(_.isClosed))
      assertResult(1)(closes.get)
    } finally {
      RmmSpark.taskDone(taskId)
      bounceBuffers.filterNot(_.isClosed).foreach(_.close())
    }
  }

  /**
   * A clock that a server's OOM backoff advances and a reader waits on. When the clock passes
   * the end of the reader's wait, `advance` returns only once the reader has given up, so that
   * whatever the server does next reaches the reader after its wait ended, as it would in time.
   */
  private class SharedClock {
    private[this] var now = 0L
    private[this] var waitEnds = Long.MaxValue
    private[this] var waiting = false

    def nanos: Long = synchronized(now)

    def advance(nanos: Long): Unit = synchronized {
      now += nanos
      notifyAll()
      val giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      while (waiting && now >= waitEnds && System.nanoTime() < giveUpAt) {
        wait(10)
      }
    }

    /** Waits on this clock until `poll` returns a result, for at most `timeoutNanos`. */
    def await[R](timeoutNanos: Long)(poll: => Option[R]): Option[R] = {
      val ends = synchronized {
        waitEnds = now + timeoutNanos
        waiting = true
        notifyAll()
        waitEnds
      }
      try {
        var result = poll
        while (result.isEmpty && nanos < ends) {
          synchronized(wait(10))
          result = poll
        }
        result
      } finally {
        synchronized {
          waiting = false
          notifyAll()
        }
      }
    }

    /** Blocks until a reader is waiting on this clock. */
    def awaitReader(): Unit = synchronized {
      val giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      while (!waiting && System.nanoTime() < giveUpAt) {
        wait(10)
      }
      assert(waiting, "the reader is not waiting")
    }
  }

  test("a reader waiting through a persistent OOM gets the error reply before its wait ends") {
    // The server and the reader wait the same length, spark.network.timeout, on one clock. The
    // reader starts waiting before its request reaches the server, and every attempt to
    // prepare the request's buffers runs out of GPU memory.
    val timeoutSeconds = 30L
    val clock = new SharedClock
    val requester = new Requester
    val bounceBuffers = new ArrayBuffer[BounceBuffer]()
    val states = new ArrayBuffer[BufferReceiveState]()
    when(mockTransport.makeClient(any())).thenReturn(requester.client)
    doAnswer { invocation =>
      val bounceBuffer = getBounceBuffer(4096)
      bounceBuffers += bounceBuffer
      val state = spy(new BufferReceiveState(123L, bounceBuffer,
        invocation.getArgument[Seq[PendingTransferRequest]](0), () => ()))
      states += state
      requester.client.issueBufferReceives(state)
      null
    }.when(mockTransport).queuePending(any())

    // the server's side: the requested buffers, which never fit in GPU memory
    val requestHandler = mock[RapidsShuffleRequestHandler]
    Seq(1 -> 1000L, 2 -> 2000L).foreach { case (id, size) =>
      val handle = mock[SpillableDeviceBufferHandle]
      when(handle.sizeInBytes).thenReturn(size)
      when(handle.materialize()).thenAnswer(_ =>
        throw new OutOfMemoryError("GPU allocation failed in test"))
      when(requestHandler.getShuffleHandle(ArgumentMatchers.eq(id)))
        .thenReturn(RapidsShuffleHandle(handle, tableMeta(id, size)))
    }
    val sendBuffer = mock[SendBounceBuffers]
    val bounceBuffer = mock[BounceBuffer]
    val bounceMemory = mock[DeviceMemoryBuffer]
    when(bounceBuffer.buffer).thenReturn(bounceMemory)
    when(bounceMemory.getLength).thenReturn(4096L)
    when(sendBuffer.bounceBufferSize).thenReturn(4096)
    when(sendBuffer.hostBounceBuffer).thenReturn(None)
    when(sendBuffer.deviceBounceBuffer).thenReturn(bounceBuffer)
    when(mockTransport.tryGetSendBounceBuffers(anyLong(), anyInt())).thenReturn(Seq(sendBuffer))

    // the request as it reaches the server, whose reply goes to the requester when it is sent
    val replySentAt = new AtomicLong(-1L)
    val replied = new CountDownLatch(1)
    val request = mock[Transaction]
    when(request.peerExecutorId()).thenReturn(1L)
    when(request.getStatus).thenReturn(TransactionStatus.Success)
    when(request.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(0, Seq(1, 2)))))
    doAnswer { invocation =>
      replySentAt.set(clock.nanos)
      // a copy, as the requester's own receive buffer would be
      val sent = invocation.getArgument[ByteBuffer](0)
      val reply = ByteBuffer.allocateDirect(sent.remaining())
      reply.put(sent.duplicate())
      reply.flip()
      requester.answer(reply)
      val responseTx = mock[Transaction]
      when(responseTx.getStatus).thenReturn(TransactionStatus.Success)
      invocation.getArgument[TransactionCallback](1)(responseTx)
      // the requester handled the reply on its copy thread, here inline, and released its state
      replied.countDown()
      responseTx
    }.when(request).respond(any(), any())

    val serverConnection = mock[ServerConnection]
    val requestCallbacks = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    doAnswer { invocation =>
      requestCallbacks.put(invocation.getArgument[MessageType.Value](0),
        invocation.getArgument[TransactionCallback](1))
      null
    }.when(serverConnection).registerRequestHandler(any(), any())
    val sendLoop = Executors.newSingleThreadExecutor()
    val server = new RapidsShuffleServer(mockTransport, serverConnection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), requestHandler, mockExecutor,
      sendLoop, mockConf) {
      override private[shuffle] def currentTimeNanos(): Long = clock.nanos
      override private[shuffle] def oomRetryTimeoutNanos: Long =
        TimeUnit.SECONDS.toNanos(timeoutSeconds)
      override private[shuffle] def waitBeforeOomRetry(backoffMillis: Long): Unit =
        clock.advance(TimeUnit.MILLISECONDS.toNanos(backoffMillis))
    }

    val taskId = 1L
    val blocks: collection.Seq[(BlockId, Long, Int)] =
      RapidsShuffleTestHelper.getShuffleBlocks.toSeq
    val iterator = new RapidsShuffleIterator(
      RapidsShuffleTestHelper.makeMockBlockManager("2", "2"), mockConf, mockTransport,
      Array[(BlockManagerId, collection.Seq[(BlockId, Long, Int)])](
        (RapidsShuffleTestHelper.makeMockBlockManager("1", "1"), blocks)),
      testMetricsUpdater, Array.empty, taskId, mockCatalog, timeoutSeconds) {
      override def pollForResult(timeoutSeconds: Long): Option[ShuffleClientResult] =
        clock.await(TimeUnit.SECONDS.toNanos(timeoutSeconds))(super.pollForResult(0))
    }
    val failure = new AtomicReference[Throwable]()
    val readerThread = new Thread(() => {
      RmmSpark.currentThreadIsDedicatedToTask(taskId)
      try {
        iterator.next()
      } catch {
        case t: Throwable => failure.set(t)
      }
    }, "shuffle-reader")
    readerThread.setDaemon(true)
    try {
      server.start()
      readerThread.start()
      requester.answer(
        ShuffleMetadata.buildMetaResponse(Seq(tableMeta(1, 1000), tableMeta(2, 2000))))
      clock.awaitReader()
      // the request reaches the server a second after the reader started waiting
      clock.advance(TimeUnit.SECONDS.toNanos(1))
      requestCallbacks.get(MessageType.TransferRequest)(request)
      readerThread.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      // a reply that comes after the reader gave up is still sent, and says when; the reader
      // can wake before the requester has closed the receive state, so this waits for that too
      assert(replied.await(awaitSeconds, TimeUnit.SECONDS), "the server never replied")

      assert(failure.get.isInstanceOf[RapidsShuffleFetchFailedException],
        s"the reader ended with ${failure.get}, and the server replied at " +
          s"${TimeUnit.NANOSECONDS.toMillis(replySentAt.get)} ms of a " +
          s"${timeoutSeconds * 1000} ms wait")
      assert(failure.get.getMessage.contains(
        RapidsShuffleServer.failureReason(BufferSendState.OomExpired)))
      // the server kept retrying for most of the reader's wait before it gave up
      assert(replySentAt.get >= TimeUnit.SECONDS.toNanos(timeoutSeconds) / 2)
      assertResult(1)(states.size)
      verify(states.head, times(1)).close()
      assert(bounceBuffers.forall(_.isClosed))
    } finally {
      server.close()
      sendLoop.shutdownNow()
      RmmSpark.taskDone(taskId)
      bounceBuffers.filterNot(_.isClosed).foreach(_.close())
    }
  }
}
