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

import java.lang.management.ManagementFactory
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, CountDownLatch, LinkedBlockingQueue, ThreadFactory, ThreadPoolExecutor, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Rmm, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsShuffleHandle, ShuffleBufferCatalog, ShuffleBufferId, ShuffleMetadata}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.format.TableMeta
import com.nvidia.spark.rapids.spill.{SpillableDeviceBufferHandle, SpillFramework}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}

import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.{ShuffleBlockBatchId, ShuffleBlockId}

/**
 * Deterministic interleavings of `ShuffleBufferCatalog.unregisterShuffle` with the readers of a
 * cached shuffle buffer: the UCX server's metadata and send paths, and the local read path.
 *
 * Each test records what the requester receives, whether the server's send loop survives, what
 * happens to the catalog's device buffer and to the send's bounce buffer and transaction, and
 * the device memory still allocated afterwards. The outcomes are logged with `info` and then
 * compared, in one assertion, against what the code does today, so a change to the ownership of
 * these buffers shows up as the list of outcomes it changed rather than as the first failed
 * check. The errors a local read gets are only recorded: a missing buffer is expected to become
 * a fetch failure, which changes them.
 */
class ShuffleBufferLifetimeSuite extends RapidsShuffleTestHelper {
  private val awaitSeconds = 30L
  private val readTypes: Array[DataType] = Array(IntegerType)
  // the send loop waits at most 100 ms between polls, so this covers ten of them
  private val unservedWaitMillis = 1000L
  private val unableToPrepare = "IllegalStateException: Unable to prepare any sends. This " +
    "issue can occur when requesting too many shuffle blocks. The sends will not be retried."
  private val closedHandleForPeer2 = "RapidsShuffleSendPrepareException: Error while copying " +
    "to bounce buffer for executor 2 and header 0x0000000000000002 <- IllegalStateException: " +
    "attempting to materialize a closed handle"

  /** Outcomes of one interleaving, logged as observed and compared at the end of the test. */
  private class Outcomes {
    private val observed = new ArrayBuffer[(String, String)]()

    def apply(key: String, value: Any): Unit = {
      observed += key -> String.valueOf(value)
      info(s"$key = $value")
    }

    /** An outcome that a fix of the missing-buffer error is expected to change. */
    def record(key: String, value: Any): Unit = info(s"$key = $value (recorded, not compared)")

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
          .mkString("outcomes differ from the code under test today:\n", "\n", ""))
    }
  }

  private def describe(t: Throwable): String =
    if (t == null) "none" else s"${t.getClass.getSimpleName}: ${t.getMessage}"

  private def causes(t: Throwable): String =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(describe).mkString(" <- ")

  private def firstSuppressed(t: Throwable): String =
    Option(t).flatMap(_.getSuppressed.headOption).map(causes).getOrElse("none")

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
    val lookedUp = new CountDownLatch(1)
    val resume = new CountDownLatch(1)

    override def getShuffleHandle(tableId: Int): RapidsShuffleHandle = {
      val handle = catalog.getShuffleBufferHandle(tableId)
      if (pauseAfterLookupOf.contains(tableId)) {
        lookedUp.countDown()
        resume.await(awaitSeconds, TimeUnit.SECONDS)
      }
      handle
    }

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
    private val dataSendCount = new AtomicInteger()
    private val copies = new ConcurrentLinkedQueue[HostMemoryBuffer]()
    private val handlers = new ConcurrentHashMap[MessageType.Value, TransactionCallback]()
    cleanups += (() => copies.forEach(_.close()))

    when(connection.send(any(), any(), any(), any[MemoryBuffer](), any[TransactionCallback]()))
      .thenAnswer { invocation =>
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

    /** Reports `sent` as delivered, which is when the server continues the send or replies. */
    def complete(sent: Sent): Unit = {
      val done = mock[Transaction]
      when(done.getStatus).thenReturn(TransactionStatus.Success)
      sent.callback(done)
    }

    def completeAll(): Unit = {
      var sent = sends.poll()
      while (sent != null) {
        complete(sent)
        sent = sends.poll()
      }
    }
  }

  private def transferRequest(peerId: Long, tableIds: Seq[Int]): Transaction = {
    val tx = mock[Transaction]
    when(tx.peerExecutorId()).thenReturn(peerId)
    when(tx.getStatus).thenReturn(TransactionStatus.Success)
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildTransferRequest(peerId, tableIds))))
    tx
  }

  private def metadataRequest(peerId: Long, blockIds: Seq[ShuffleBlockBatchId]): Transaction = {
    val tx = mock[Transaction]
    when(tx.peerExecutorId()).thenReturn(peerId)
    when(tx.getStatus).thenReturn(TransactionStatus.Success)
    when(tx.releaseMessage()).thenReturn(new MetadataTransportBuffer(
      new RefCountedDirectByteBuffer(ShuffleMetadata.buildShuffleMetadataRequest(blockIds))))
    tx
  }

  private def reply(tx: Transaction): String = if (calls(tx, "respond") > 0) "sent" else "none"

  private def txState(tx: Transaction): String = if (calls(tx, "close") > 0) "closed" else "open"

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
    private val server = new RapidsShuffleServer(transport, peer.connection,
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
      if (died.getCount == 0) {
        // nothing else polls the request queue once the loop is gone
        Thread.sleep(unservedWaitMillis)
      }
      Option(peer.sends.poll(if (died.getCount == 0) 0 else awaitSeconds, TimeUnit.SECONDS))
        .foreach(peer.complete)
      s"${reply(next)}, read by the server: ${calls(next, "releaseMessage") > 0}"
    }

    def loop: String = {
      if (died.getCount == 0) {
        s"exited, pool threads running it: ${bssExec.getActiveCount}: ${describe(failure.get)}"
      } else {
        "running"
      }
    }

    def failureSuppressed: String = Option(failure.get)
      .map(_.getSuppressed.map(causes).mkString(" | ")).getOrElse("none")

    def bounceBuffersNotReturned: Int = {
      var n = 0
      bounceBuffers.forEach(b => if (!b.deviceBounceBuffer.isClosed) n += 1)
      n
    }
  }

  private def handleOf(catalog: ShuffleBufferCatalog,
      cached: Cached): SpillableDeviceBufferHandle = {
    val field = classOf[ShuffleBufferCatalog].getDeclaredFields
      .find(_.getName.endsWith("bufferIdToHandle")).get
    field.setAccessible(true)
    val handles = field.get(catalog).asInstanceOf[java.util.Map[ShuffleBufferId, AnyRef]]
    handles.get(ShuffleBufferId(cached.blockId, cached.tableId))
      .asInstanceOf[(Option[SpillableDeviceBufferHandle], TableMeta)]._1.get
  }

  private def awaitBlockedOn(thread: Thread, monitor: AnyRef): Unit = {
    val threads = ManagementFactory.getThreadMXBean
    eventually(Timeout(Span(awaitSeconds, Seconds))) {
      val info = threads.getThreadInfo(thread.getId)
      assert(info != null && info.getThreadState == Thread.State.BLOCKED &&
        info.getLockInfo.getIdentityHashCode == System.identityHashCode(monitor),
        s"${thread.getName} never blocked on the monitor")
    }
  }

  private def firstValue(batch: ColumnarBatch): Int = {
    withResource(GpuColumnVector.from(batch)) { table =>
      withResource(table.getColumn(0).copyToHost()) { host =>
        host.getInt(0)
      }
    }
  }

  // Metadata path: the requester asks which buffers a block has, then asks for them by table id.

  test("M1 metadata: a request for a shuffle that is already unregistered") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val cached = cache(catalog, ShuffleBlockId(1, 1L, 0), 0, cleanups)
      val peer = new Peer(cleanups)
      val server = newServer(new CatalogRequestHandler(catalog), peer)
      catalog.unregisterShuffle(1)
      outcomes("buffer after unregister", holding(catalog, cached))

      val tx = metadataRequest(2L, Seq(ShuffleBlockBatchId(1, 1L, 0, 0)))
      outcomes("escapes the server task",
        describe(attempt(peer.request(MessageType.MetadataRequest, tx))))
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      val direct = metadataRequest(2L, Seq(ShuffleBlockBatchId(1, 1L, 0, 0)))
      outcomes("the handler throws, inside the task",
        describe(attempt(server.doHandleMetadataRequest(direct))))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "escapes the server task" -> "none",
      "requester gets" -> "none",
      "request transaction" -> "closed",
      "the handler throws, inside the task" -> "NoSuchElementException: unknown shuffle 1",
      "device bytes still allocated" -> 0)
  }

  test("M2 metadata: unregister removes the buffers while the lookup waits for the block") {
    val outcomes = new Outcomes
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val cached = cache(catalog, block, 0, cleanups)
      val shufflesField = classOf[ShuffleBufferCatalog].getDeclaredField("activeShuffles")
      shufflesField.setAccessible(true)
      val list = shufflesField.get(catalog).asInstanceOf[
        java.util.Map[Any, java.util.Map[ShuffleBlockId, AnyRef]]].get(1).get(block)
      val result = new AtomicReference[String]()
      val lookup = new Thread(() => result.set(
        try s"${catalog.blockIdToMetas(block).size} metas" catch {
          case t: Throwable => describe(t)
        }), "metadata-lookup")
      lookup.setDaemon(true)
      list.synchronized {
        lookup.start()
        awaitBlockedOn(lookup, list)
        // this thread holds the block's list, so the removal completes before the lookup resumes
        catalog.unregisterShuffle(1)
      }
      lookup.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      outcomes("lookup", result.get)
      outcomes("buffer after unregister", holding(catalog, cached))
    }
    outcomes.check(
      "lookup" -> "MatchError: null",
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0")
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

      val failure = attempt(server.doHandleTransferRequest(Seq(bss)))
      outcomes("server throws", describe(failure))
      outcomes("first suppressed", firstSuppressed(failure))
      outcomes("data sends", peer.dataSends)
      peer.sends.forEach(s => outcomes("data matches", sameBytes(s.bytes, cached.bytes)))
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffer at the end", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "server throws" -> unableToPrepare,
      "first suppressed" -> closedHandleForPeer2,
      "data sends" -> 0,
      "requester gets" -> "none",
      "request transaction" -> "closed",
      "send bounce buffer" -> "returned",
      "buffer at the end" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }

  test("T1b send: a co-batched send state of a closed buffer is retried until it is alone") {
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
      outcomes("data sends", peer.dataSends)
      peer.completeAll()
      outcomes("live requester gets", reply(liveTx))
      outcomes("closed buffer's bounce buffer after the batch", bounceState(goneBounce))
      // as the loop does when it next polls the re-queued state, now alone
      val alone = attempt(if (goneState.hasMoreSends) {
        server.doHandleTransferRequest(Seq(goneState))
      })
      outcomes("retried alone, throws", describe(alone))
      peer.completeAll()
      outcomes("closed buffer's requester gets", reply(goneTx))
      outcomes("closed buffer's bounce buffer at the end", bounceState(goneBounce))
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "batch of both throws" -> "none",
      "states re-queued" -> 1,
      "data sends" -> 1,
      "live requester gets" -> "sent",
      "closed buffer's bounce buffer after the batch" -> "not returned",
      "retried alone, throws" -> unableToPrepare,
      "closed buffer's requester gets" -> "none",
      "closed buffer's bounce buffer at the end" -> "returned",
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

      val failure = attempt(server.doHandleTransferRequest(Seq(bss)))
      outcomes("second window throws", describe(failure))
      outcomes("first suppressed", firstSuppressed(failure))
      outcomes("data sends", peer.dataSends)
      peer.sends.forEach(s => outcomes("second window matches", sameBytes(s.bytes, second.bytes)))
      peer.completeAll()
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      outcomes("send bounce buffer", bounceState(bounce))
      outcomes("buffers at the end", holding(catalog, first, second))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window throws" -> "none",
      "first window matches" -> true,
      "more to send" -> true,
      "buffers after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "second window throws" -> unableToPrepare,
      "first suppressed" -> closedHandleForPeer2,
      "data sends" -> 1,
      "requester gets" -> "none",
      "request transaction" -> "closed",
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
      // what a server shutdown would have to do for a send state it never reached
      bss.close()
      outcomes("buffer once the send state closes", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer while the send state is abandoned" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "buffer once the send state closes" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
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
      loop.awaitSendOrDeath().foreach(loop.peer.complete)
      outcomes("send loop", loop.loop)
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "a request before the unregister gets" -> "sent, read by the server: true",
      "send loop" -> ("exited, pool threads running it: 0: NoSuchElementException: " +
        "unknown table ID 0"),
      "requester gets" -> "none",
      "request transaction" -> "open",
      "next request gets" -> "none, read by the server: false",
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,1",
      "send bounce buffers not returned" -> 1,
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
      loop.awaitSendOrDeath().foreach { sent =>
        outcomes("data matches", sameBytes(sent.bytes, gone.bytes))
        loop.peer.complete(sent)
      }
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("loop failure's suppressed", loop.failureSuppressed)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, gone, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0",
      "requester gets" -> "none",
      "request transaction" -> "closed",
      "next request gets" -> "none, read by the server: false",
      "send loop" -> s"exited, pool threads running it: 0: $unableToPrepare",
      "loop failure's suppressed" -> (s"$closedHandleForPeer2 | IllegalStateException: " +
        "ALREADY CLOSED!"),
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
      loop.awaitSendOrDeath().foreach { sent =>
        outcomes("second window matches", sameBytes(sent.bytes, second.bytes))
        loop.peer.complete(sent)
      }
      outcomes("requester gets", reply(tx))
      outcomes("request transaction", txState(tx))
      outcomes("next request gets", loop.nextRequestGets(live.tableId))
      outcomes("send loop", loop.loop)
      outcomes("loop failure's suppressed", loop.failureSuppressed)
      notReturned = loop.bounceBuffersNotReturned
      outcomes("buffers at the end", holding(catalog, first, second, live))
    }
    outcomes("send bounce buffers not returned", notReturned)
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first window matches" -> true,
      "buffers after unregister" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0",
      "requester gets" -> "none",
      "request transaction" -> "closed",
      "next request gets" -> "none, read by the server: false",
      "send loop" -> s"exited, pool threads running it: 0: $unableToPrepare",
      "loop failure's suppressed" -> (s"$closedHandleForPeer2 | IllegalStateException: " +
        "ALREADY CLOSED!"),
      "buffers at the end" -> "catalog=(1,1,1) spillStore=1 deviceRefs=0,0,1",
      "send bounce buffers not returned" -> 0,
      "device bytes still allocated" -> 0)
  }

  // Local read path: a reduce task reads the blocks its own executor cached.

  test("T6 local read: unregister after one batch of a two-buffer block") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val first = cache(catalog, block, 0, cleanups)
      val second = cache(catalog, block, 5000, cleanups)
      val notYetRead = ShuffleBlockId(1, 2L, 0)
      cache(catalog, notYetRead, 10000, cleanups)
      val batches = catalog.getColumnarBatchIterator(block, readTypes)
      outcomes("first batch starts at", withResource(batches.next())(firstValue))
      catalog.unregisterShuffle(1)
      outcomes("buffers after unregister", holding(catalog, first, second))
      outcomes("has a next batch", batches.hasNext)
      outcomes.record("second next()",
        describe(attempt(withResource(batches.next())(firstValue))))
      outcomes.record("first next() of a block not yet reached",
        describe(attempt(catalog.getColumnarBatchIterator(notYetRead, readTypes))))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "first batch starts at" -> 0,
      "buffers after unregister" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0,0",
      "has a next batch" -> true,
      "device bytes still allocated" -> 0)
  }

  test("T6b local read: unregister between a buffer's lookup and its materialize") {
    val outcomes = new Outcomes
    val allocatedBefore = Rmm.getTotalBytesAllocated
    withCleanup { cleanups =>
      val catalog = newCatalog(cleanups, 1)
      val block = ShuffleBlockId(1, 1L, 0)
      val cached = cache(catalog, block, 0, cleanups)
      val handle = handleOf(catalog, cached)
      val batches = catalog.getColumnarBatchIterator(block, readTypes)
      val result = new AtomicReference[String]()
      val reader = new Thread(() => result.set(
        try s"a batch starting at ${withResource(batches.next())(firstValue)}" catch {
          case t: Throwable => describe(t)
        }), "local-reader")
      reader.setDaemon(true)
      handle.synchronized {
        reader.start()
        // the reader has looked the handle up and waits to materialize it
        awaitBlockedOn(reader, handle)
        catalog.unregisterShuffle(1)
      }
      reader.join(TimeUnit.SECONDS.toMillis(awaitSeconds))
      outcomes.record("next()", result.get)
      outcomes("buffer after the read", holding(catalog, cached))
    }
    outcomes("device bytes still allocated", Rmm.getTotalBytesAllocated - allocatedBefore)
    outcomes.check(
      "buffer after the read" -> "catalog=(0,0,0) spillStore=0 deviceRefs=0",
      "device bytes still allocated" -> 0)
  }
}
