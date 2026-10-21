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

package com.nvidia.spark.rapids.spill

import java.io.File
import java.util.concurrent.{Callable, CountDownLatch, ExecutionException, Executors, TimeUnit}

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, HostMemoryBuffer}
import com.nvidia.spark.rapids.{GpuColumnVector, TestUtils}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}

import org.apache.spark.SparkConf
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.BlockId

class SpillLifecycleReplaySuite extends SpillUnitTestBase {
  private val timeoutSeconds = 15L
  private val expectedBytes = Array.tabulate[Byte](64)(index => (index * 3).toByte)

  private def await(latch: CountDownLatch): Unit = {
    assert(latch.await(timeoutSeconds, TimeUnit.SECONDS))
  }

  private class PausingHostStore(maxSizeInBytes: Long = 1024L)
    extends SpillableHostStore(Some(maxSizeInBytes)) {
    val copyFinished = new CountDownLatch(1)
    val resumeCopy = new CountDownLatch(1)

    override def makeBuilder(
        handle: SpillableHostBufferHandle): SpillableHostBufferHandleBuilder = {
      val underlying = super.makeBuilder(handle)
      new SpillableHostBufferHandleBuilder {
        override def copyNext(buffer: DeviceMemoryBuffer, length: Long,
            stream: Cuda.Stream): Unit = {
          underlying.copyNext(buffer, length, stream)
          copyFinished.countDown()
          await(resumeCopy)
        }

        override def build: SpillableHostBufferHandle = underlying.build

        override def close(): Unit = underlying.close()
      }
    }
  }

  private class PausingDeviceStore extends SpillableDeviceStore {
    val beforeRelease = new CountDownLatch(1)
    val resumeRelease = new CountDownLatch(1)
    val beforeSync = new CountDownLatch(1)
    val resumeSync = new CountDownLatch(1)

    override protected def synchronizeDevice(): Unit = {
      beforeSync.countDown()
      await(resumeSync)
      super.synchronizeDevice()
    }

    override protected def failExecutorOnSyncFailure(): Unit = ()

    override def postSpill(plan: SpillPlan): Unit = {
      beforeRelease.countDown()
      await(resumeRelease)
      super.postSpill(plan)
    }
  }

  private class RecordingDiskStore extends DiskHandleStore(new SparkConf) {
    @volatile var spillFile: File = _

    override def getFile(blockId: BlockId): File = {
      val file = super.getFile(blockId)
      spillFile = file
      file
    }
  }

  private class PausingFailingHostStore extends SpillableHostStore(Some(1L << 20)) {
    val copyStarted = new CountDownLatch(1)
    val resumeCopy = new CountDownLatch(1)

    override def makeBuilder(
        handle: SpillableHostBufferHandle): SpillableHostBufferHandleBuilder = {
      val underlying = super.makeBuilder(handle)
      new SpillableHostBufferHandleBuilder {
        override def copyNext(buffer: DeviceMemoryBuffer, length: Long,
            stream: Cuda.Stream): Unit = {
          copyStarted.countDown()
          await(resumeCopy)
          throw new IllegalStateException("injected host copy failure")
        }

        override def build: SpillableHostBufferHandle = underlying.build

        override def close(): Unit = underlying.close()
      }
    }
  }

  private class PausingFailingDiskStore extends DiskHandleStore(new SparkConf) {
    val writeStarted = new CountDownLatch(1)
    val resumeWrite = new CountDownLatch(1)

    override def getFile(blockId: BlockId): File = {
      writeStarted.countDown()
      await(resumeWrite)
      throw new IllegalStateException("injected disk write failure")
    }
  }

  private class FailingHostStore extends SpillableHostStore(Some(1024L)) {
    private var copies = 0

    override def makeBuilder(
        handle: SpillableHostBufferHandle): SpillableHostBufferHandleBuilder = {
      val underlying = super.makeBuilder(handle)
      new SpillableHostBufferHandleBuilder {
        override def copyNext(buffer: DeviceMemoryBuffer, length: Long,
            stream: Cuda.Stream): Unit = {
          copies += 1
          if (copies == 2) {
            throw new IllegalStateException("injected host copy failure")
          }
          underlying.copyNext(buffer, length, stream)
        }

        override def build: SpillableHostBufferHandle = underlying.build

        override def close(): Unit = underlying.close()
      }
    }
  }

  private class TestPlanHandle(
      priority: Long,
      spillFailure: Option[Throwable] = None,
      releaseFailure: Option[Throwable] = None) extends DeviceStoreHandle {
    override val approxSizeInBytes: Long = 1L
    taskPriority = priority
    var released = false

    override def spill(): Long = {
      spillFailure.foreach(error => throw error)
      spilling = true
      approxSizeInBytes
    }

    override def releaseSpilled(): Unit = {
      released = true
      spilling = false
      SpillFramework.removeFromDeviceStore(this)
      releaseFailure.foreach(error => throw error)
    }

    override private[spill] def doClose(): Unit = {
      SpillFramework.removeFromDeviceStore(this)
    }
  }

  private class FailingSyncDeviceStore(syncFailure: Throwable) extends SpillableDeviceStore {
    var exitRequested = false

    override protected def synchronizeDevice(): Unit = throw syncFailure

    override protected def failExecutorOnSyncFailure(): Unit = {
      exitRequested = true
    }
  }

  private def createHandle(): SpillableDeviceBufferHandle = {
    closeOnExcept(DeviceMemoryBuffer.allocate(expectedBytes.length)) { deviceBuffer =>
      withResource(HostMemoryBuffer.allocate(expectedBytes.length)) { hostBuffer =>
        hostBuffer.setBytes(0, expectedBytes, 0, expectedBytes.length)
        deviceBuffer.copyFromHostBuffer(hostBuffer)
      }
      SpillableDeviceBufferHandle(deviceBuffer)
    }
  }

  private def assertContents(buffer: DeviceMemoryBuffer): Unit = {
    withResource(HostMemoryBuffer.allocate(expectedBytes.length)) { hostBuffer =>
      hostBuffer.copyFromDeviceBuffer(buffer)
      val actual = new Array[Byte](expectedBytes.length)
      hostBuffer.asByteBuffer.get(actual)
      assert(actual.sameElements(expectedBytes))
    }
  }

  private case class ReplayHandle(
      handle: DeviceSpillableHandle[_ <: AutoCloseable],
      hasHost: () => Boolean,
      verify: () => Unit)

  private case class HandleCase(
      name: String,
      create: () => ReplayHandle,
      hostLimit: Long = 1024L,
      diskBacked: Boolean = false)

  private def verifyOrdinaryBatch(batch: ColumnarBatch): Unit = {
    val (expected, _) = buildTable()
    withResource(expected) { _ =>
      withResource(GpuColumnVector.from(batch)) { actual =>
        TestUtils.compareTables(expected, actual)
      }
    }
  }

  private def compressedReplayHandle(): ReplayHandle = {
    val handle = SpillableCompressedColumnarBatchHandle(buildCompressedBatch(0, 1000))
    ReplayHandle(handle, () => handle.host.isDefined,
      () => withResource(handle.materialize()) { compressed =>
        withResource(decompressBatch(compressed)) { batch =>
          withResource(buildContiguousTable(0, 1000)) { contiguous =>
            withResource(contiguous.getTable) { expected =>
              withResource(GpuColumnVector.from(batch)) { actual =>
                TestUtils.compareTables(expected, actual)
              }
            }
          }
        }
      })
  }

  private val siblingHandles = Seq(
    HandleCase("columnar batch", () => {
      val (handle, dataTypes) = addTableToFramework()
      ReplayHandle(handle, () => handle.host.isDefined,
        () => withResource(handle.materialize(dataTypes)) { batch =>
          verifyOrdinaryBatch(batch)
        })
    }),
    HandleCase("contiguous columnar batch", () => {
      val (_, handle, dataTypes) = addContiguousTableToFramework()
      ReplayHandle(handle, () => handle.host.isDefined,
        () => withResource(handle.materialize(dataTypes)) { batch =>
          verifyOrdinaryBatch(batch)
        })
    }),
    HandleCase("compressed columnar batch with host staging", () => compressedReplayHandle(),
      hostLimit = 1L << 20),
    HandleCase("compressed columnar batch with disk staging", () => compressedReplayHandle(),
      hostLimit = 1L, diskBacked = true),
    HandleCase("table", () => {
      val (handle, _) = addSpillableTableToFramework()
      ReplayHandle(handle, () => handle.host.isDefined,
        () => withResource(handle.materialize()) { table =>
          val (expected, _) = buildTable()
          withResource(expected) { _ =>
            TestUtils.compareTables(expected, table)
          }
        })
    }))

  private def runSiblingScenario(handleCase: HandleCase, closeAt: String): Unit = {
    val hostStore = new PausingHostStore(handleCase.hostLimit)
    val deviceStore = new PausingDeviceStore
    val diskStore = if (handleCase.diskBacked) Some(new RecordingDiskStore) else None
    SpillFramework.stores.hostStore = hostStore
    SpillFramework.stores.deviceStore = deviceStore
    diskStore.foreach(store => SpillFramework.stores.diskStore = store)
    val executor = Executors.newSingleThreadExecutor()
    val replay = handleCase.create()
    try {
      val spilled = executor.submit(new Callable[Long] {
        override def call(): Long = deviceStore.spill(replay.handle.approxSizeInBytes)
      })
      await(hostStore.copyFinished)
      val stagedFile = diskStore.map { store =>
        val file = store.spillFile
        assert(file != null)
        assert(file.exists())
        file
      }
      if (closeAt == "copy") replay.handle.close()
      hostStore.resumeCopy.countDown()
      await(deviceStore.beforeRelease)
      if (closeAt == "publication") replay.handle.close()
      assert(replay.handle.dev.isDefined)
      if (closeAt == "after release") assert(replay.hasHost())
      deviceStore.resumeRelease.countDown()
      await(deviceStore.beforeSync)
      assert(replay.handle.dev.isDefined)
      deviceStore.resumeSync.countDown()
      assertResult(replay.handle.approxSizeInBytes)(spilled.get(timeoutSeconds, TimeUnit.SECONDS))
      assert(replay.handle.dev.isEmpty)
      if (closeAt == "after release") replay.verify()
      replay.handle.close()
      assertResult(0)(deviceStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
      stagedFile.foreach(file => assert(!file.exists()))
    } finally {
      hostStore.resumeCopy.countDown()
      deviceStore.resumeRelease.countDown()
      deviceStore.resumeSync.countDown()
      executor.shutdown()
      if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
      replay.handle.close()
    }
  }

  private def runScenario(
      atCopy: SpillableDeviceBufferHandle => Unit,
      atPublication: SpillableDeviceBufferHandle => Unit,
      afterRelease: SpillableDeviceBufferHandle => Unit = _ => ()): Unit = {
    val hostStore = new PausingHostStore
    val deviceStore = new PausingDeviceStore
    SpillFramework.stores.hostStore = hostStore
    SpillFramework.stores.deviceStore = deviceStore
    val executor = Executors.newSingleThreadExecutor()
    val handle = createHandle()
    try {
      val spilled = executor.submit(new Callable[Long] {
        override def call(): Long = deviceStore.spill(handle.approxSizeInBytes)
      })
      await(hostStore.copyFinished)
      atCopy(handle)
      hostStore.resumeCopy.countDown()
      await(deviceStore.beforeRelease)
      atPublication(handle)
      deviceStore.resumeRelease.countDown()
      await(deviceStore.beforeSync)
      assert(handle.dev.isDefined)
      deviceStore.resumeSync.countDown()
      assertResult(handle.approxSizeInBytes)(spilled.get(timeoutSeconds, TimeUnit.SECONDS))
      afterRelease(handle)
      handle.close()
      assertResult(0)(deviceStore.numHandles)
      assertResult(0)(hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
    } finally {
      hostStore.resumeCopy.countDown()
      deviceStore.resumeRelease.countDown()
      deviceStore.resumeSync.countDown()
      executor.shutdown()
      if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
      handle.close()
    }
  }

  test("materialize during host copy and recover after spill") {
    var borrowed: DeviceMemoryBuffer = null
    try {
      runScenario(
        handle => {
          borrowed = handle.materialize()
          assertContents(borrowed)
        },
        handle => {
          assert(handle.host.isDefined)
          val materialized = borrowed
          borrowed = null
          withResource(materialized)(assertContents)
          withResource(handle.materialize())(assertContents)
        },
        handle => {
          assert(handle.dev.isEmpty)
          withResource(handle.materialize())(assertContents)
        })
    } finally {
      Option(borrowed).foreach { buffer =>
        withResource(buffer)(_ => ())
      }
    }
  }

  test("close during host copy releases all stores") {
    runScenario(
      handle => handle.close(),
      handle => {
        assert(handle.closed)
        assert(handle.dev.isDefined)
      })
  }

  test("close after host copy waits for post-spill synchronization") {
    runScenario(
      _ => (),
      handle => {
        handle.close()
        assert(handle.dev.isDefined)
      })
  }

  test("failed spill plan releases earlier completed device buffers") {
    val hostStore = new FailingHostStore
    val deviceStore = new SpillableDeviceStore
    SpillFramework.stores.hostStore = hostStore
    SpillFramework.stores.deviceStore = deviceStore

    withResource(createHandle()) { first =>
      withResource(createHandle()) { second =>
        val failure = intercept[IllegalStateException] {
          deviceStore.spill(first.approxSizeInBytes + second.approxSizeInBytes)
        }
        assertResult("injected host copy failure")(failure.getMessage)
        val completed = Seq(first, second).filter(_.host.isDefined)
        assertResult(1)(completed.size)
        assert(completed.head.dev.isEmpty)
        withResource(completed.head.materialize())(assertContents)
      }
    }
    assertResult(0)(deviceStore.numHandles)
    assertResult(0)(hostStore.numHandles)
  }

  for (handleCase <- siblingHandles; closeAt <- Seq("copy", "publication", "after release")) {
    test(s"${handleCase.name} retains device ownership until post-spill: $closeAt") {
      runSiblingScenario(handleCase, closeAt)
    }
  }

  test("partial plan failure preserves the copy error and releases every completed handle") {
    val deviceStore = SpillFramework.stores.deviceStore
    val copyFailure = new IllegalStateException("injected copy failure")
    val releaseFailure = new IllegalStateException("injected release failure")
    val first = new TestPlanHandle(0L, releaseFailure = Some(releaseFailure))
    val second = new TestPlanHandle(1L)
    val third = new TestPlanHandle(2L, spillFailure = Some(copyFailure))
    Seq(first, second, third).foreach(deviceStore.track)

    val failure = intercept[IllegalStateException] {
      deviceStore.spill(3L)
    }
    assert(failure eq copyFailure)
    assertResult(Seq(releaseFailure))(failure.getSuppressed.toSeq)
    assert(first.released)
    assert(second.released)
    assert(!third.released)
    third.close()
    assertResult(0)(deviceStore.numHandles)
  }

  test("shared cleanup failure preserves the copy error and releases later handles") {
    val deviceStore = SpillFramework.stores.deviceStore
    val sharedFailure = new IllegalStateException("injected shared failure")
    val first = new TestPlanHandle(0L, releaseFailure = Some(sharedFailure))
    val second = new TestPlanHandle(1L, releaseFailure = Some(sharedFailure))
    val third = new TestPlanHandle(2L)
    val fourth = new TestPlanHandle(3L, spillFailure = Some(sharedFailure))
    Seq(first, second, third, fourth).foreach(deviceStore.track)

    val failure = intercept[IllegalStateException] {
      deviceStore.spill(4L)
    }
    assert(failure eq sharedFailure)
    assert(failure.getSuppressed.isEmpty)
    assert(first.released)
    assert(second.released)
    assert(third.released)
    assert(!fourth.released)
    fourth.close()
    assertResult(0)(deviceStore.numHandles)
  }

  test("partial plan failure finalizes a shared recomputable handle") {
    var closedResources = 0
    def createResource(): AutoCloseable = new AutoCloseable {
      override def close(): Unit = closedResources += 1
    }

    val deviceStore = SpillFramework.stores.deviceStore
    val recomputable = SharedRecomputableHandle(1L, createResource())(createResource())
    val copyFailure = new IllegalStateException("injected later spill failure")
    val failing = new TestPlanHandle(0L, spillFailure = Some(copyFailure))
    deviceStore.track(failing)
    try {
      val failure = intercept[IllegalStateException] {
        deviceStore.spill(2L)
      }
      assert(failure eq copyFailure)
      assertResult(1)(closedResources)
      assert(!recomputable.spilling)
      withResource(recomputable.acquire()) { lease =>
        assert(lease.rebuilt)
      }
    } finally {
      failing.close()
      recomputable.close()
    }
    assertResult(2)(closedResources)
    assertResult(0)(deviceStore.numHandles)
  }

  test("failed device release still completes a concurrent close") {
    var closedResources = 0
    val releaseFailure = new IllegalStateException("injected device release failure")
    val handle = new DeviceSpillableHandle[AutoCloseable] {
      override val approxSizeInBytes: Long = 1L
      private[spill] override var dev: Option[AutoCloseable] = Some(new AutoCloseable {
        override def close(): Unit = closedResources += 1
      })
      private var failNextRelease = true

      override def spill(): Long = {
        spilling = true
        approxSizeInBytes
      }

      override protected def releaseDeviceResource(): Unit = {
        if (failNextRelease) {
          failNextRelease = false
          throw releaseFailure
        }
        super.releaseDeviceResource()
      }

      override private[spill] def doClose(): Unit = releaseDeviceResource()
    }
    SpillFramework.stores.deviceStore.track(handle)
    handle.spill()
    handle.close()
    val failure = intercept[IllegalStateException] {
      handle.releaseSpilled()
    }
    assert(failure eq releaseFailure)
    assert(!handle.spilling)
    assertResult(1)(closedResources)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }

  test("repeated device release failure preserves the original error") {
    var closedResources = 0
    val releaseFailure = new IllegalStateException("injected repeated release failure")
    val handle = new DeviceSpillableHandle[AutoCloseable] {
      override val approxSizeInBytes: Long = 1L
      private[spill] override var dev: Option[AutoCloseable] = Some(new AutoCloseable {
        override def close(): Unit = closedResources += 1
      })
      private var remainingFailures = 2

      override def spill(): Long = {
        spilling = true
        approxSizeInBytes
      }

      override protected def releaseDeviceResource(): Unit = {
        if (remainingFailures > 0) {
          remainingFailures -= 1
          throw releaseFailure
        }
        super.releaseDeviceResource()
      }

      override private[spill] def doClose(): Unit = releaseDeviceResource()
    }
    SpillFramework.stores.deviceStore.track(handle)
    handle.spill()
    handle.close()
    val failure = intercept[IllegalStateException] {
      handle.releaseSpilled()
    }
    assert(failure eq releaseFailure)
    assert(failure.getSuppressed.isEmpty)
    assert(!handle.spilling)
    handle.releaseSpilled()
    assertResult(1)(closedResources)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }

  test("device close failure clears ownership and completes host cleanup") {
    val closeFailure = new IllegalStateException("injected device close failure")
    var deviceCloseAttempts = 0
    var hostClosed = false
    val handle = new DeviceSpillableHandle[AutoCloseable] {
      override val approxSizeInBytes: Long = 1L
      private[spill] override var dev: Option[AutoCloseable] = Some(new AutoCloseable {
        override def close(): Unit = {
          deviceCloseAttempts += 1
          throw closeFailure
        }
      })

      override def spill(): Long = {
        spilling = true
        approxSizeInBytes
      }

      override private[spill] def doClose(): Unit = {
        releaseDeviceResource()
        hostClosed = true
      }
    }
    SpillFramework.stores.deviceStore.track(handle)
    handle.spill()
    handle.close()
    val failure = intercept[IllegalStateException] {
      handle.releaseSpilled()
    }
    assert(failure eq closeFailure)
    assertResult(1)(deviceCloseAttempts)
    assert(handle.dev.isEmpty)
    assert(hostClosed)
    assert(!handle.spilling)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }

  test("failed device synchronization fails the executor before releasing completed handles") {
    val syncFailure = new IllegalStateException("injected synchronization failure")
    val deviceStore = new FailingSyncDeviceStore(syncFailure)
    SpillFramework.stores.deviceStore = deviceStore
    val handle = new TestPlanHandle(0L)
    deviceStore.track(handle)

    val failure = intercept[IllegalStateException] {
      deviceStore.spill(1L)
    }
    assert(failure eq syncFailure)
    assert(deviceStore.exitRequested)
    assert(!handle.released)
    assert(handle.spilling)
    handle.releaseSpilled()
    assertResult(0)(deviceStore.numHandles)
  }

  private def runFailedSpillClose(
      handle: SpillableHandle,
      started: CountDownLatch,
      resume: CountDownLatch,
      released: () => Boolean): Unit = {
    val executor = Executors.newSingleThreadExecutor()
    try {
      val spilled = executor.submit(new Callable[Long] {
        override def call(): Long = handle.spill()
      })
      await(started)
      handle.close()
      resume.countDown()
      val failure = intercept[ExecutionException] {
        spilled.get(timeoutSeconds, TimeUnit.SECONDS)
      }
      assert(failure.getCause.getMessage.startsWith("injected"))
      assert(!handle.spilling)
      assert(released())
      assert(!handle.spillable)
      assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
      assertResult(0)(SpillFramework.stores.hostStore.numHandles)
      assertResult(0)(SpillFramework.stores.diskStore.numHandles)
    } finally {
      resume.countDown()
      executor.shutdown()
      if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS)) {
        executor.shutdownNow()
      }
      handle.close()
    }
  }

  test("device buffer completes a close deferred by a failed spill") {
    val hostStore = new PausingFailingHostStore
    SpillFramework.stores.hostStore = hostStore
    val handle = createHandle()
    runFailedSpillClose(handle, hostStore.copyStarted, hostStore.resumeCopy,
      () => handle.dev.isEmpty)
  }

  for (handleCase <- siblingHandles if !handleCase.diskBacked) {
    test(s"${handleCase.name} completes a close deferred by a failed spill") {
      val hostStore = new PausingFailingHostStore
      SpillFramework.stores.hostStore = hostStore
      val replay = handleCase.create()
      runFailedSpillClose(replay.handle, hostStore.copyStarted, hostStore.resumeCopy,
        () => replay.handle.dev.isEmpty)
    }
  }

  test("host buffer completes a close deferred by a failed disk spill") {
    val diskStore = new PausingFailingDiskStore
    SpillFramework.stores.diskStore = diskStore
    val handle = closeOnExcept(HostMemoryBuffer.allocate(expectedBytes.length)) { buffer =>
      buffer.setBytes(0, expectedBytes, 0, expectedBytes.length)
      SpillableHostBufferHandle(buffer)
    }
    runFailedSpillClose(handle, diskStore.writeStarted, diskStore.resumeWrite,
      () => handle.host.isEmpty)
  }

  test("host columnar batch completes a close deferred by a failed disk spill") {
    val diskStore = new PausingFailingDiskStore
    SpillFramework.stores.diskStore = diskStore
    val (batch, _) = buildHostBatch()
    val handle = SpillableHostColumnarBatchHandle(batch)
    runFailedSpillClose(handle, diskStore.writeStarted, diskStore.resumeWrite,
      () => handle.host.isEmpty)
  }

  test("failed spill keeps its error primary when the deferred close fails") {
    val spillFailure = new IllegalStateException("injected spill failure")
    val closeFailure = new IllegalStateException("injected close failure")
    var closes = 0
    val handle = new DeviceStoreHandle {
      override val approxSizeInBytes: Long = 1L

      override def spill(): Long = {
        synchronized {
          spilling = true
        }
        executeSpill {
          close()
          throw spillFailure
        }
      }

      override def releaseSpilled(): Unit = ()

      override private[spill] def doClose(): Unit = {
        closes += 1
        SpillFramework.removeFromDeviceStore(this)
        throw closeFailure
      }
    }
    SpillFramework.stores.deviceStore.track(handle)
    val failure = intercept[IllegalStateException] {
      handle.spill()
    }
    assert(failure eq spillFailure)
    assert(failure.getSuppressed.toSeq == Seq(closeFailure))
    assert(!handle.spilling)
    assertResult(1)(closes)
    handle.close()
    assertResult(1)(closes)
    assertResult(0)(SpillFramework.stores.deviceStore.numHandles)
  }
}
