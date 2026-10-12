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

package com.nvidia.spark.rapids.shuffle

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Rmm, Table}
import com.nvidia.spark.rapids.{RapidsShuffleHandle, ShuffleBufferCatalog, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.SpillFramework
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

/**
 * Deterministic interleavings of `ShuffleBufferCatalog.unregisterShuffle`, and of the server's
 * close, with the UCX shuffle server's send path.
 *
 * Each test records what the requester receives, whether the server's send loop survives and
 * serves the next request, how each send state ends, and what happens to the catalog's device
 * buffer and to the send's bounce buffer and request transaction, with exact counts. The
 * outcomes are logged with `info` and then compared, in one assertion, against what the server
 * does, so a change to the ownership of these buffers shows up as the list of outcomes it
 * changed rather than as the first failed check. A failed request gets no reply yet: the
 * requester times out, until the server can send an error reply.
 */
class ShuffleBufferLifetimeSuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L

  /** Outcomes of one interleaving, logged as observed and compared at the end of the test. */
  private class Outcomes {
    private val observed = new ArrayBuffer[(String, String)]()

    def apply(key: String, value: Any): Unit = {
      observed += key -> String.valueOf(value)
      info(s"$key = $value")
    }

    def check(expected: (String, Any)*): Unit = {
      val want = expected.map { case (k, v) => k -> String.valueOf(v) }
      val got = observed.toMap
      val wrong = want.collect {
        case (k, v) if !got.get(k).contains(v) =>
          s"$k: expected <$v>, observed <${got.getOrElse(k, "nothing")}>"
      }
      val unexpected = (got.keySet -- want.map(_._1)).toList.sorted
      assert(wrong.isEmpty && unexpected.isEmpty,
        (wrong ++ unexpected.map(k => s"$k: observed <${got(k)}>, not expected"))
          .mkString("outcomes differ from the code under test:\n", "\n", ""))
    }
  }

  private def describe(t: Throwable): String =
    if (t == null) "none" else s"${t.getClass.getSimpleName}: ${t.getMessage}"

  private def attempt(body: => Unit): Throwable = {
    try {
      body
      null
    } catch {
      case t: Throwable => t
    }
  }

  private def calls(mockObject: AnyRef, method: String): Int = {
    var n = 0
    val it = mockingDetails(mockObject).getInvocations.iterator()
    while (it.hasNext) {
      if (it.next().getMethod.getName == method) {
        n += 1
      }
    }
    n
  }

  /** Runs `body`, then every cleanup it registered, last registered first. */
  private def withCleanup[T](body: ArrayBuffer[() => Unit] => T): T = {
    val cleanups = new ArrayBuffer[() => Unit]()
    try {
      body(cleanups)
    } finally {
      cleanups.reverse.foreach(c => attempt(c()))
    }
  }

  /** A buffer in the catalog, the test's own reference to its device memory, and its bytes. */
  private case class Cached(
      blockId: ShuffleBlockId,
      tableId: Int,
      device: DeviceMemoryBuffer,
      bytes: HostMemoryBuffer) extends AutoCloseable {
    def size: Long = device.getLength

    /** References to the device memory other than the test's own. */
    def otherRefs: Int = device.getRefCount - 1

    override def close(): Unit = {
      device.close()
      bytes.close()
    }
  }

  private def contiguousTable(first: Int, numRows: Int): ContiguousTable = {
    withResource(ColumnVector.fromInts((first until first + numRows).toArray: _*)) { cv =>
      withResource(new Table(cv)) { table =>
        table.contiguousSplit()(0)
      }
    }
  }

  /** Adds a 1000-row batch whose values start at `first`, so that no two buffers match. */
  private def cache(catalog: ShuffleBufferCatalog, blockId: ShuffleBlockId, first: Int,
      cleanups: ArrayBuffer[() => Unit]): Cached = {
    val ct = contiguousTable(first, 1000)
    val (device, bytes) = closeOnExcept(ct) { _ =>
      val buffer = ct.getBuffer
      val host = closeOnExcept(HostMemoryBuffer.allocate(buffer.getLength)) { host =>
        host.copyFromDeviceBuffer(buffer)
        host
      }
      buffer.incRefCount()
      (buffer, host)
    }
    val cached = closeOnExcept(device) { _ =>
      closeOnExcept(bytes) { _ =>
        Cached(blockId, catalog.addContiguousTable(blockId, ct, -1).tableId, device, bytes)
      }
    }
    cleanups += (() => cached.close())
    cached
  }

  private def newCatalog(cleanups: ArrayBuffer[() => Unit], shuffleIds: Int*)
      : ShuffleBufferCatalog = {
    val catalog = new ShuffleBufferCatalog()
    shuffleIds.foreach(catalog.registerShuffle)
    cleanups += (() => shuffleIds.filter(catalog.hasActiveShuffle)
      .foreach(catalog.unregisterShuffle))
    catalog
  }

  /** What the catalog and the spill store hold, and the other references to each buffer. */
  private def holding(catalog: ShuffleBufferCatalog, cached: Cached*): String = {
    val (handles, tables, listed) = catalog.bookkeepingSizes
    val tracked = SpillFramework.stores.deviceStore.numHandles
    s"catalog=($handles,$tables,$listed) spillStore=$tracked" +
      s" deviceRefs=${cached.map(_.otherRefs).mkString(",")}"
  }

  private def sameBytes(a: HostMemoryBuffer, b: HostMemoryBuffer): Boolean = {
    a.getLength == b.getLength && {
      val (x, y) = (new Array[Byte](a.getLength.toInt), new Array[Byte](b.getLength.toInt))
      a.getBytes(x, 0, 0, x.length)
      b.getBytes(y, 0, 0, y.length)
      java.util.Arrays.equals(x, y)
    }
  }

  /** The request handler `RapidsShuffleInternalManagerBase` gives the server, with a pause. */
  private class CatalogRequestHandler(catalog: ShuffleBufferCatalog)
      extends RapidsShuffleRequestHandler {
    @volatile var pauseAfterLookupOf: Option[Int] = None
    @volatile var resumedInTime = false
    val lookedUp = new CountDownLatch(1)
    val resume = new CountDownLatch(1)

    override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
      val handle = catalog.getShuffleBufferHandle(tableId)
      if (pauseAfterLookupOf.contains(tableId)) {
        lookedUp.countDown()
        resumedInTime = resume.await(awaitSeconds, TimeUnit.SECONDS)
      }
      handle
    }

    override def isShuffleBufferRemoved(tableId: Int, handle: RapidsShuffleHandle): Boolean =
      catalog.isShuffleBufferRemoved(tableId, handle.spillable)

    override def getShuffleBufferMetas(sbbId: ShuffleBlockBatchId): Seq[TableMeta] = {
      (sbbId.startReduceId to sbbId.endReduceId).flatMap { reduceId =>
        catalog.blockIdToMetas(ShuffleBlockId(sbbId.shuffleId, sbbId.mapId, reduceId))
      }
    }
  }

  private case class Sent(bytes: HostMemoryBuffer, callback: TransactionCallback)

  /** The requester's side of the transport: keeps what the server sends and completes it. */
  private class Peer(cleanups: ArrayBuffer[() => Unit]) {
    val connection: ServerConnection = mock[ServerConnection]
    val sends = new LinkedBlockingQueue[Sent]()
    /** Runs at the start of every data send, with the peer's executor id; it may throw. */
    @volatile var beforeSend: (Long => Unit) = (_: Long) => ()
    private val dataSendCount = new AtomicInteger()
    private val copies = new ConcurrentLinkedQueue[HostMemoryBuffer]()
    private val handlers = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    cleanups += (() => copies.forEach(_.close()))

    when(connection.send(any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]()))
      .thenAnswer { invocation =>
        beforeSend(invocation.getArgument[Long](0))
        val buffer = invocation.getArgument[MemoryBuffer](3)
        val copy = HostMemoryBuffer.allocate(buffer.getLength)
        copies.add(copy)
        copy.copyFromDeviceBuffer(buffer.asInstanceOf[DeviceMemoryBuffer])
        dataSendCount.incrementAndGet()
        sends.put(Sent(copy, invocation.getArgument[TransactionCallback](4)))
        mock[Transaction]
      }
    doAnswer { invocation =>
      handlers.put(invocation.getArgument[MessageType.Value](0),
        invocation.getArgument[TransactionCallback](1))
      null
    }.when(connection).registerRequestHandler(any(), any())

    def dataSends: Int = dataSendCount.get

    def request(messageType: MessageType.Value, tx: Transaction): Unit =
      handlers.get(messageType)(tx)

    def nextSend(): Sent = {
      val sent = sends.poll(awaitSeconds, TimeUnit.SECONDS)
      assert(sent != null, "the server never sent the data it was asked for")
      sent
    }

    /**
     * Completes `sent` with `status`. A success is when the server continues the send or
     * replies. A failed send reports InProgress, as the UCX transport does.
     */
    def complete(sent: Sent, status: TransactionStatus.Value = TransactionStatus.Success)
        : Unit = {
      val done = mock[Transaction]
      when(done.getStatus).thenReturn(status)
      sent.callback(done)
    }

    def completeAll(status: TransactionStatus.Value = TransactionStatus.Success): Unit = {
      var sent = sends.poll()
      while (sent != null) {
        complete(sent, status)
        sent = sends.poll()
      }
    }
  }

  // counted down when a request transaction is closed, which can happen on the loop's thread
  private val closedRequests = new ConcurrentHashMap[Transaction, CountDownLatch]()

  private def transferRequest(peerId: Long, tableIds: Seq[Int]): Transaction = {
    val tx = mock[Transaction]
    val closed = new CountDownLatch(1)
    closedRequests.put(tx, closed)
    doAnswer { _ =>
      closed.countDown()
      null
    }.when(tx).close()
    when(tx.peerExecutorId()).thenReturn(peerId)
    when(tx.getStatus).thenReturn(TransactionStatus.Success)
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(peerId, tableIds))))
    tx
  }

  private def awaitClosed(tx: Transaction): Unit =
    assert(closedRequests.get(tx).await(awaitSeconds, TimeUnit.SECONDS),
      "the server never closed the request transaction")

  private def reply(tx: Transaction): String = if (calls(tx, "respond") > 0) "sent" else "none"

  private def txCloses(tx: Transaction): Int = calls(tx, "close")

  private def ended(state: BufferSendState): String =
    state.endedWith.map(o => s"ended: $o").getOrElse("live")

  private def bounceState(bounce: SendBounceBuffers): String =
    if (bounce.deviceBounceBuffer.isClosed) "returned" else "not returned"

  private def sendBounceBuffer(size: Long, cleanups: ArrayBuffer[() => Unit]): SendBounceBuffers = {
    val bounce = getSendBounceBuffer(size)
    cleanups += (() => if (!bounce.deviceBounceBuffer.isClosed) bounce.close())
    bounce
  }

  private def newServer(handler: RapidsShuffleRequestHandler, peer: Peer): RapidsShuffleServer = {
    val server = new RapidsShuffleServer(mockTransport, peer.connection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "server"), handler,
      new ImmediateExecutor, mockBssExecutor, mockConf)
    server.start()
    server
  }

  /** A server whose send loop runs on its own thread, as `UCXShuffleTransport` runs it. */
  private class LoopServer(handler: RapidsShuffleRequestHandler, bounceBufferSize: Long,
      cleanups: ArrayBuffer[() => Unit]) {
    val peer = new Peer(cleanups)
    private val failure = new AtomicReference[Throwable]()
    private val died = new CountDownLatch(1)
    // the production thread factory sets no handler, so an executor's default one gets this
    private val threads = new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val thread = new Thread(r, "test-shuffle-server-bss")
        thread.setDaemon(true)
        thread.setUncaughtExceptionHandler { (_: Thread, t: Throwable) =>
          failure.set(t)
          died.countDown()
        }
        thread
      }
    }
    private val bssExec = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue[Runnable](), threads)
    private val bounceBuffers = new ConcurrentLinkedQueue[SendBounceBuffers]()
    private val transport = mock[RapidsShuffleTransport]
    when(transport.tryGetSendBounceBuffers(any(), any())).thenAnswer { _ =>
      val bounce = getSendBounceBuffer(bounceBufferSize)
      bounceBuffers.add(bounce)
      Seq(bounce)
    }
    val server = new RapidsShuffleServer(transport, peer.connection,
      RapidsShuffleTestHelper.makeMockBlockManager("1", "server"), handler,
      new ImmediateExecutor, bssExec, mockConf)
    server.start()
    cleanups += { () =>
      server.close()
      bssExec.shutdown()
      bssExec.awaitTermination(awaitSeconds, TimeUnit.SECONDS)
      bounceBuffers.forEach(b => if (!b.deviceBounceBuffer.isClosed) b.close())
    }

    def requestTransfer(tx: Transaction): Unit = peer.request(MessageType.TransferRequest, tx)

    /** Waits until the loop sends data or dies, and returns the send if there was one. */
    def awaitSendOrDeath(): Option[Sent] = {
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(awaitSeconds)
      var sent: Sent = null
      while (sent == null && died.getCount > 0 && System.nanoTime() < deadline) {
        sent = peer.sends.poll(10, TimeUnit.MILLISECONDS)
      }
      Option(sent)
    }

    /** Asks for `tableId` from another peer and reports whether the loop served it. */
    def nextRequestGets(tableId: Int): String = {
      val next = transferRequest(3L, Seq(tableId))
      requestTransfer(next)
      // nothing else polls the request queue once the loop is gone, so there is nothing to wait for
      Option(peer.sends.poll(if (died.getCount == 0) 0 else awaitSeconds, TimeUnit.SECONDS))
        .foreach(peer.complete(_))
      s"${reply(next)}, read by the server: ${calls(next, "releaseMessage") > 0}"
    }

    def loop: String = {
      if (died.getCount == 0) {
        s"exited, pool threads running it: ${bssExec.getActiveCount}: ${describe(failure.get)}"
      } else {
        "running"
      }
    }

    def bounceBuffersTaken: Int = bounceBuffers.size

    def bounceBuffersNotReturned: Int = {
      var n = 0
      bounceBuffers.forEach(b => if (!b.deviceBounceBuffer.isClosed) n += 1)
      n
    }
  }

  // Send path, one step at a time without the send loop.

  test("T1 send: unregister after the send state looks the buffer up, before its first window") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bounce = sendBounceBuffer(cached.size, cleanups)
      val bss = new BufferSendState(tx, bounce, handler)
      catalog.unregisterShuffle(1)
      outcomes("buffer after unregister", holding(catalog, cached))

      outcomes("server throws", describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      outcomes("send state", ended(bss))
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffer at the end", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "server throws" -> "none",
      "send state" -> "ended: MissingOutput",
      "data sends" -> 0,
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "send bounce buffer" -> "returned",
      "buffer at the end" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T1b send: a co-batched send state of a closed buffer ends in the pass that fails it") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      val goneTx = transferRequest(2L, Seq(gone.tableId))
      val liveTx = transferRequest(3L, Seq(live.tableId))
      val goneBounce = sendBounceBuffer(gone.size, cleanups)
      val goneState = new BufferSendState(goneTx, goneBounce, handler)
      val liveState = new BufferSendState(liveTx, sendBounceBuffer(live.size, cleanups), handler)
      catalog.unregisterShuffle(1)

      outcomes("batch of both throws",
        describe(attempt(server.doHandleTransferRequest(Seq(goneState, liveState)))))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      outcomes("closed buffer's send state", ended(goneState))
      outcomes("closed buffer's bounce buffer", bounceState(goneBounce))
      outcomes("data sends", peer.dataSends)
      peer.sends.forEach(s => outcomes("live data matches", sameBytes(s.bytes, live.bytes)))
      peer.completeAll()
      outcomes("live requester gets", reply(liveTx))
      outcomes("live send state", ended(liveState))
      outcomes("closed buffer's requester gets", reply(goneTx))
      outcomes("closed buffer's request transaction closes", txCloses(goneTx))
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "batch of both throws" -> "none",
      "states re-queued" -> 0,
      "closed buffer's send state" -> "ended: MissingOutput",
      "closed buffer's bounce buffer" -> "returned",
      "data sends" -> 1,
      "live data matches" -> true,
      "live requester gets" -> "sent",
      "live send state" -> "ended: Completed",
      "closed buffer's requester gets" -> "none",
      "closed buffer's request transaction closes" -> 1,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "device bytes still allocated" -> 0)
  }

  test("T2 send: unregister between the windows of a two-window send") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      // a bounce buffer the size of one buffer makes one window per buffer
      val bounce = sendBounceBuffer(first.size, cleanups)
      val bss = new BufferSendState(tx, bounce, handler)

      outcomes("first window throws", describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      val window1 = peer.nextSend()
      outcomes("first window matches", sameBytes(window1.bytes, first.bytes))
      peer.complete(window1)
      outcomes("more to send", bss.hasMoreSends)
      catalog.unregisterShuffle(1)
      outcomes("buffers after unregister", holding(catalog, first, second))

      outcomes("second window throws",
        describe(attempt(server.doHandleTransferRequest(Seq(bss)))))
      outcomes("send state", ended(bss))
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffers at the end", holding(catalog, first, second))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window throws" -> "none",
      "first window matches" -> true,
      "more to send" -> true,
      "buffers after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "second window throws" -> "none",
      "send state" -> "ended: MissingOutput",
      "data sends" -> 1,
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "send bounce buffer" -> "returned",
      "buffers at the end" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "device bytes still allocated" -> 0)
  }

  test("T5 send: unregister after a window materialized the buffer, before its release") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bss = new BufferSendState(tx, sendBounceBuffer(cached.size, cleanups), handler)
      withResource(bss) { _ =>
        withResource(bss.getBufferToSend()) { window =>
          catalog.unregisterShuffle(1)
          outcomes("buffer after unregister", holding(catalog, cached))
          Cuda.DEFAULT_STREAM.sync()
          withResource(HostMemoryBuffer.allocate(window.getLength)) { copy =>
            copy.copyFromDeviceBuffer(window.asInstanceOf[DeviceMemoryBuffer])
            outcomes("window matches", sameBytes(copy, cached.bytes))
          }
          bss.releaseAcquiredToCatalog()
          outcomes("buffer after the window's release", holding(catalog, cached))
        }
        outcomes("more to send", bss.hasMoreSends)
      }
      outcomes("buffer after the send state closes", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=1",
      "window matches" -> true,
      "buffer after the window's release" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "more to send" -> false,
      "buffer after the send state closes" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T7 send: a send state that looked the buffer up and is never driven") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bss = new BufferSendState(tx, sendBounceBuffer(cached.size, cleanups), handler)
      catalog.unregisterShuffle(1)
      outcomes("buffer while the send state is abandoned", holding(catalog, cached))
      bss.close()
      outcomes("buffer once the send state closes", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer while the send state is abandoned" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "buffer once the send state closes" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T8 send: the transport reports a data send as failed, on a first and on a last window") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      // two windows, of which the first fails
      val twoTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      val twoBounce = sendBounceBuffer(first.size, cleanups)
      val two = new BufferSendState(twoTx, twoBounce, handler)
      // one window, which fails
      val oneTx = transferRequest(3L, Seq(first.tableId))
      val oneBounce = sendBounceBuffer(first.size, cleanups)
      val one = new BufferSendState(oneTx, oneBounce, handler)

      server.doHandleTransferRequest(Seq(two, one))
      outcomes("data sends", peer.dataSends)
      outcomes("bounce buffers before the callbacks",
        s"${bounceState(twoBounce)}, ${bounceState(oneBounce)}")
      peer.completeAll(TransactionStatus.InProgress)
      outcomes("first window's send state", ended(two))
      outcomes("last window's send state", ended(one))
      outcomes("requesters get", s"${reply(twoTx)}, ${reply(oneTx)}")
      outcomes("request transactions close", s"${txCloses(twoTx)}, ${txCloses(oneTx)}")
      outcomes("bounce buffers after the callbacks",
        s"${bounceState(twoBounce)}, ${bounceState(oneBounce)}")
      outcomes("states re-queued", calls(server, "addToContinueQueue"))

      val nextTx = transferRequest(4L, Seq(second.tableId))
      val next = new BufferSendState(nextTx, sendBounceBuffer(second.size, cleanups), handler)
      server.doHandleTransferRequest(Seq(next))
      peer.completeAll()
      outcomes("next request gets", reply(nextTx))
      outcomes("buffers at the end", holding(catalog, first, second))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "data sends" -> 2,
      "bounce buffers before the callbacks" -> "not returned, not returned",
      "first window's send state" -> "ended: SendFailed",
      "last window's send state" -> "ended: SendFailed",
      "requesters get" -> "none, none",
      "request transactions close" -> "1, 1",
      "bounce buffers after the callbacks" -> "returned, returned",
      "states re-queued" -> 0,
      "next request gets" -> "sent",
      "buffers at the end" -> "catalog=(2,2,2) spillStore=2 deviceRefs=1,1",
      "device bytes still allocated" -> 0)
  }

  test("T9 send: the server closes while sends' callbacks are pending") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      def newState(peerId: Long, tableIds: Int*): (Transaction, SendBounceBuffers,
          BufferSendState) = {
        val tx = transferRequest(peerId, tableIds)
        val bounce = sendBounceBuffer(first.size, cleanups)
        (tx, bounce, new BufferSendState(tx, bounce, handler))
      }
      val (lastTx, lastBounce, last) = newState(2L, first.tableId)
      val (earlierTx, earlierBounce, earlier) = newState(3L, first.tableId, second.tableId)
      val (failedTx, failedBounce, failed) = newState(4L, second.tableId)
      server.doHandleTransferRequest(Seq(last, earlier, failed))
      // the sends go out in the order of the batch
      val (lastSend, earlierSend, failedSend) = (peer.nextSend(), peer.nextSend(), peer.nextSend())

      server.close()
      outcomes("bounce buffers before the callbacks",
        Seq(lastBounce, earlierBounce, failedBounce).map(bounceState).mkString(", "))
      peer.complete(lastSend)
      peer.complete(earlierSend)
      peer.complete(failedSend, TransactionStatus.InProgress)
      outcomes("send states", Seq(last, earlier, failed).map(ended).mkString(", "))
      outcomes("requesters get", Seq(lastTx, earlierTx, failedTx).map(reply).mkString(", "))
      outcomes("request transactions close",
        Seq(lastTx, earlierTx, failedTx).map(txCloses).mkString(", "))
      outcomes("bounce buffers after the callbacks",
        Seq(lastBounce, earlierBounce, failedBounce).map(bounceState).mkString(", "))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))
      outcomes("data sends", peer.dataSends)
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "bounce buffers before the callbacks" -> "not returned, not returned, not returned",
      "send states" -> "ended: ServerClosed, ended: ServerClosed, ended: SendFailed",
      "requesters get" -> "none, none, none",
      "request transactions close" -> "1, 1, 1",
      "bounce buffers after the callbacks" -> "returned, returned, returned",
      // the earlier window's callback offers the state, and gets it back from the closed server
      "states re-queued" -> 1,
      "data sends" -> 3,
      "device bytes still allocated" -> 0)
  }

  test("T10 send: the transport closes before a send's callback arrives") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      def newState(peerId: Long): (Transaction, SendBounceBuffers, BufferSendState) = {
        val tx = transferRequest(peerId, Seq(cached.tableId))
        val bounce = sendBounceBuffer(cached.size, cleanups)
        (tx, bounce, new BufferSendState(tx, bounce, handler))
      }

      // the progress thread terminated, so nothing can read the bounce buffer any more
      val peer = new Peer(cleanups)
      val server = newServer(handler, peer)
      val (tx, bounce, state) = newState(2L)
      server.doHandleTransferRequest(Seq(state))
      val pending = peer.nextSend()
      server.close()
      server.releaseAbandoned(transportStopped = true)
      outcomes("abandoned send state", ended(state))
      outcomes("abandoned bounce buffer", bounceState(bounce))
      outcomes("abandoned request transaction closes", txCloses(tx))
      peer.complete(pending)
      outcomes("after a late callback", s"${ended(state)}, closes ${txCloses(tx)}, " +
        s"reply ${reply(tx)}")
      val (afterTx, afterBounce, after) = newState(3L)
      server.doHandleTransferRequest(Seq(after))
      outcomes("a send after the sweep", s"${ended(after)}, ${bounceState(afterBounce)}, " +
        s"closes ${txCloses(afterTx)}, data sends ${peer.dataSends}")

      // the progress thread did not terminate, so the transport may still read it
      val stuckPeer = new Peer(cleanups)
      val stuckServer = newServer(handler, stuckPeer)
      val (stuckTx, stuckBounce, stuck) = newState(4L)
      stuckServer.doHandleTransferRequest(Seq(stuck))
      stuckPeer.nextSend()
      stuckServer.close()
      stuckServer.releaseAbandoned(transportStopped = false)
      outcomes("send state the transport may still read",
        s"${ended(stuck)}, ${bounceState(stuckBounce)}, closes ${txCloses(stuckTx)}")
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "abandoned send state" -> "ended: ServerClosed",
      "abandoned bounce buffer" -> "returned",
      "abandoned request transaction closes" -> 1,
      "after a late callback" -> "ended: ServerClosed, closes 1, reply none",
      "a send after the sweep" -> "ended: ServerClosed, returned, closes 1, data sends 1",
      "send state the transport may still read" -> "live, not returned, closes 0",
      "device bytes still allocated" -> 0)
  }

  test("T14 send: a send that throws ends only its own state, while a co-batched one goes on") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val third = cache(catalog, block, 10000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val peer = new Peer(cleanups)
      val server = spy(newServer(handler, peer))
      // two windows, still going when the other send throws
      val goingTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      val goingBounce = sendBounceBuffer(first.size, cleanups)
      val going = new BufferSendState(goingTx, goingBounce, handler)
      // one window, whose send throws before it queues anything
      val throwingTx = transferRequest(3L, Seq(third.tableId))
      val throwingBounce = sendBounceBuffer(third.size, cleanups)
      val throwing = new BufferSendState(throwingTx, throwingBounce, handler)
      val firstWindowDelivered = new CountDownLatch(1)
      peer.beforeSend = peerId => if (peerId == 3L) {
        assert(firstWindowDelivered.await(awaitSeconds, TimeUnit.SECONDS))
        throw new IllegalStateException("the send failed before it queued anything")
      }

      val passFailure = new AtomicReference[Throwable]()
      val pass = new Thread(() => passFailure.set(
        attempt(server.doHandleTransferRequest(Seq(going, throwing)))), "send-pass")
      pass.setDaemon(true)
      pass.start()
      // the first window's callback puts its state back on the queue while the pass still runs
      peer.complete(peer.nextSend())
      firstWindowDelivered.countDown()
      pass.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      outcomes("pass throws", describe(passFailure.get))
      outcomes("throwing send's state", ended(throwing))
      outcomes("throwing send's bounce buffer", bounceState(throwingBounce))
      outcomes("throwing send's request transaction closes", txCloses(throwingTx))
      outcomes("throwing send's requester gets", reply(throwingTx))
      outcomes("co-batched state after the pass", ended(going))
      outcomes("states re-queued", calls(server, "addToContinueQueue"))

      // as the loop's next pass does with the re-queued state
      server.doHandleTransferRequest(Seq(going))
      val window2 = peer.nextSend()
      outcomes("second window matches", sameBytes(window2.bytes, second.bytes))
      outcomes("co-batched bounce buffer before its last callback", bounceState(goingBounce))
      peer.complete(window2)
      outcomes("co-batched state", ended(going))
      outcomes("co-batched requester gets", reply(goingTx))
      outcomes("co-batched request transaction closes", txCloses(goingTx))
      outcomes("co-batched bounce buffer", bounceState(goingBounce))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "pass throws" -> "none",
      "throwing send's state" -> "ended: Failed",
      "throwing send's bounce buffer" -> "returned",
      "throwing send's request transaction closes" -> 1,
      "throwing send's requester gets" -> "none",
      "co-batched state after the pass" -> "live",
      "states re-queued" -> 1,
      "second window matches" -> true,
      "co-batched bounce buffer before its last callback" -> "not returned",
      "co-batched state" -> "ended: Completed",
      "co-batched requester gets" -> "sent",
      "co-batched request transaction closes" -> 1,
      "co-batched bounce buffer" -> "returned",
      "device bytes still allocated" -> 0)
  }

  test("a bounce buffer pool hands out nothing once it is closed") {
    val pool = new BounceBufferManager[DeviceMemoryBuffer]("test", 1024, 2,
      size => DeviceMemoryBuffer.allocate(size))
    pool.acquireBuffersNonBlocking(1).foreach(_.close())
    pool.close()
    assert(pool.acquireBuffersNonBlocking(1).isEmpty)
  }

  // Send path through the send loop, on a thread of its own.

  test("T3 send loop: unregister after the metadata reply, before the transfer request") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = -1
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), gone.size, cleanups)
      outcomes("a request before the unregister gets", loop.nextRequestGets(live.tableId))

      catalog.unregisterShuffle(1)
      val tx = transferRequest(2L, Seq(gone.tableId))
      loop.requestTransfer(tx)
      awaitClosed(tx)
      outcomes("send loop", loop.loop)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "a request before the unregister gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "data sends" -> 2,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T3b send loop: unregister between the lookups of a two-buffer request") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = -1
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 10000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, first.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      // the end-to-end runs: the request's first lookup succeeds, then its shuffle goes
      handler.pauseAfterLookupOf = Some(first.tableId)
      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(tx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      catalog.unregisterShuffle(1)
      handler.resume.countDown()
      awaitClosed(tx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("send loop", loop.loop)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, first, second, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "send loop" -> "running",
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "data sends" -> 1,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0,1",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T1L send loop: unregister while the loop is paused after the lookup") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = -1
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val gone = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 5000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, gone.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      handler.pauseAfterLookupOf = Some(gone.tableId)
      val tx = transferRequest(2L, Seq(gone.tableId))
      loop.requestTransfer(tx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      catalog.unregisterShuffle(1)
      outcomes("buffer after unregister", holding(catalog, gone))
      handler.resume.countDown()
      awaitClosed(tx)
      assert(handler.resumedInTime, "the paused lookup timed out before the test resumed it")
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0",
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "data sends" -> 1,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T4 send loop: unregister between windows, then the loop continues the send alone") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = -1
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1, 2)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val live = cache(catalog, ShuffleBlockId(2, 1L, 0), 10000, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), first.size, cleanups)

      val tx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(tx)
      val window1 = loop.peer.nextSend()
      outcomes("first window matches", sameBytes(window1.bytes, first.bytes))
      catalog.unregisterShuffle(1)
      outcomes("buffers after unregister", holding(catalog, first, second))
      // delivering the first window puts the send state back on the loop's queue
      loop.peer.complete(window1)
      awaitClosed(tx)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction closes", txCloses(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("data sends", loop.peer.dataSends)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, first, second, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window matches" -> true,
      "buffers after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0",
      "requester gets" -> "none",
      "request transaction closes" -> 1,
      "next request gets" -> "sent, read by the server: true",
      "send loop" -> "running",
      "data sends" -> 2,
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0,1",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T11 send loop: a transfer request that arrives after the server closed") {
    val outcomes = new Outcomes
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val loop = new LoopServer(new CatalogRequestHandler(catalog), cached.size, cleanups)
      loop.server.close()
      val tx = transferRequest(2L, Seq(cached.tableId))
      loop.requestTransfer(tx)
      outcomes("request transaction closes", txCloses(tx))
      outcomes("requester gets", reply(tx))
      outcomes("read by the server", calls(tx, "releaseMessage") > 0)
      outcomes("bounce buffers taken", loop.bounceBuffersTaken)
      outcomes("send loop", loop.loop)
    }
    outcomes.check(
      "request transaction closes" -> 1,
      "requester gets" -> "none",
      "read by the server" -> false,
      "bounce buffers taken" -> 0,
      "send loop" -> "running")
  }

  test("T12 send loop: a send state still queued when the server closes ends as the loop exits") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    var notReturned = -1
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val paused = cache(catalog, ShuffleBlockId(1, 2L, 0), 10000, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, first.size, cleanups)
      cleanups += (() => handler.resume.countDown())

      val queuedTx = transferRequest(2L, Seq(first.tableId, second.tableId))
      loop.requestTransfer(queuedTx)
      val window1 = loop.peer.nextSend()
      // keep the loop busy in another request's lookup while the first state is re-queued
      handler.pauseAfterLookupOf = Some(paused.tableId)
      val pausedTx = transferRequest(3L, Seq(paused.tableId))
      loop.requestTransfer(pausedTx)
      assert(handler.lookedUp.await(awaitSeconds, TimeUnit.SECONDS))
      loop.peer.complete(window1)
      // the loop is still paused, so close gives up waiting for it
      loop.server.close()
      handler.resume.countDown()
      // the pass in progress still sends the request it was building
      val pausedSend = loop.awaitSendOrDeath()
      outcomes("the paused request's window is sent", pausedSend.isDefined)
      awaitClosed(queuedTx)
      outcomes("queued requester gets", reply(queuedTx))
      outcomes("queued request transaction closes", txCloses(queuedTx))
      pausedSend.foreach(loop.peer.complete(_))
      outcomes("paused requester gets", reply(pausedTx))
      outcomes("paused request transaction closes", txCloses(pausedTx))
      outcomes("data sends", loop.peer.dataSends)
      outcomes("send loop", loop.loop)
      notReturned = loop.bounceBuffersNotReturned
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "the paused request's window is sent" -> true,
      "queued requester gets" -> "none",
      "queued request transaction closes" -> 1,
      "paused requester gets" -> "none",
      "paused request transaction closes" -> 1,
      "data sends" -> 2,
      "send loop" -> "running",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  test("T13 send loop: a send state queued after it ended is skipped") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val handler = new CatalogRequestHandler(catalog)
      val loop = new LoopServer(handler, cached.size, cleanups)
      val tx = transferRequest(2L, Seq(cached.tableId))
      val bounce = sendBounceBuffer(cached.size, cleanups)
      val state = spy(new BufferSendState(tx, bounce, handler))
      state.finish(BufferSendState.Failed)
      loop.server.addToContinueQueue(Seq(state))
      // served in a later pass than the one that polled the ended state
      outcomes("next request gets", loop.nextRequestGets(cached.tableId))
      outcomes("ended state prepared again", calls(state, "getBufferToSend"))
      outcomes("ended state", ended(state))
      outcomes("ended state's request transaction closes", txCloses(tx))
      outcomes("ended state's bounce buffer", bounceState(bounce))
      outcomes("data sends", loop.peer.dataSends)
      outcomes("send loop", loop.loop)
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "next request gets" -> "sent, read by the server: true",
      "ended state prepared again" -> 0,
      "ended state" -> "ended: Failed",
      "ended state's request transaction closes" -> 1,
      "ended state's bounce buffer" -> "returned",
      "data sends" -> 1,
      "send loop" -> "running",
      "device bytes still allocated" -> 0)
  }
}
