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

package org.apache.spark.sql.rapids.execution

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream,
  ObjectOutputStream}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeoutException, TimeUnit}

import ai.rapids.cudf.Table
import com.nvidia.spark.rapids.{GpuColumnVector, GpuDeviceManager, GpuSemaphore, NoopMetric,
  SpillableColumnarBatch, SpillableColumnarBatchFromBufferImpl, TestUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.spill.{SpillFramework, SpillUnitTestBase}
import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Spark's `MemoryStore` serializes an evicted broadcast block while it holds the memory manager
 * lock, which GPU tasks may also need. Writing a materialized GPU broadcast must therefore not
 * wait for the GPU semaphore (https://github.com/NVIDIA/cudf-spark/issues/16460).
 */
class BroadcastEvictionSuite extends SpillUnitTestBase {

  override def beforeEach(): Unit = {
    super.beforeEach()
    // The semaphore initializes the GPU for a task from the SparkEnv.
    SparkSession.builder.master("local[1]").appName(getClass.getSimpleName).getOrCreate()
    GpuDeviceManager.setRmmTaskInitEnabled(false)
    GpuSemaphore.shutdown()
    GpuSemaphore.initialize(maxConcurrentGpuTasksLimit = 1)
  }

  override def afterEach(): Unit = {
    GpuSemaphore.shutdown()
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    super.afterEach()
  }

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType))
    }
  }

  private def makeBroadcastBatch(
      gpuBatch: ColumnarBatch): SerializeConcatHostBuffersDeserializeBatch = {
    val attrs = GpuColumnVector.extractTypes(gpuBatch).map(t => AttributeReference("", t)())
    val buffer = withResource(new SerializeBatchDeserializeHostBuffer(gpuBatch)) { obj =>
      SerializationUtils.clone(obj)
    }
    GpuBroadcastExchangeExecBase.makeBroadcastBatch(
      Array(buffer), attrs, NoopMetric, NoopMetric, NoopMetric, SQLConf.get)
  }

  private def withBroadcast[T](b: SerializeConcatHostBuffersDeserializeBatch)
      (body: SerializeConcatHostBuffersDeserializeBatch => T): T = {
    try body(b) finally b.closeInternal()
  }

  private def write(broadcast: SerializeConcatHostBuffersDeserializeBatch): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    withResource(new ObjectOutputStream(baos)) { oos =>
      broadcast.doWriteObject(oos)
    }
    baos.toByteArray
  }

  private def read(bytes: Array[Byte]): SerializeConcatHostBuffersDeserializeBatch = {
    val broadcast = new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 0, 0L)
    withResource(new ObjectInputStream(new ByteArrayInputStream(bytes))) { ois =>
      broadcast.doReadObject(ois)
    }
    broadcast
  }

  private def assertSameData(
      expected: ColumnarBatch,
      broadcast: SerializeConcatHostBuffersDeserializeBatch): Unit = {
    withResource(broadcast.batch.getColumnarBatch()) { actual =>
      TestUtils.compareBatches(expected, actual)
    }
  }

  /** Run `body` like a `MemoryStore` eviction while another task holds the only GPU permit. */
  private def evictWhileGpuIsBusy[T](body: => T): T = {
    val holderContext = new MockTaskContext(taskAttemptId = 1, partitionId = 0)
    val evictorContext = new MockTaskContext(taskAttemptId = 2, partitionId = 1)
    val memoryManager = SparkEnv.get.memoryManager
    val pool = Executors.newSingleThreadExecutor()
    try {
      GpuSemaphore.acquireIfNecessary(holderContext)
      // Spark evicts the block from a task thread while holding the memory manager lock
      val evictor = pool.submit(new Callable[T] {
        override def call(): T = {
          TrampolineUtil.setTaskContext(evictorContext)
          try {
            memoryManager.synchronized(body)
          } finally {
            TrampolineUtil.unsetTaskContext()
          }
        }
      })
      try {
        evictor.get(30, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException =>
          fail("deadlock: eviction holds the memory manager lock and waits for the GPU")
      }
    } finally {
      // Completing the holder releases its permit, which also ends a deadlock.
      holderContext.markTaskComplete()
      pool.shutdown()
      pool.awaitTermination(30, TimeUnit.SECONDS)
      evictorContext.markTaskComplete()
    }
  }

  /** Number of spill handles on the device, in host memory, and on disk. */
  private def residency: (Int, Int, Int) = {
    val stores = SpillFramework.stores
    (stores.deviceStore.numHandles, stores.hostStore.numHandles, stores.diskStore.numHandles)
  }

  Seq(("on the device", (1, 0, 0)), ("in host memory", (0, 1, 0)), ("on disk", (0, 0, 1))).foreach {
    case (location, expectedResidency) =>
      test(s"evicting a broadcast whose GPU batch is $location does not wait for the GPU") {
        withResource(buildBatch()) { expected =>
          withBroadcast(makeBroadcastBatch(expected)) { broadcast =>
            val size = broadcast.batch.sizeInBytes
            assert(broadcast.batch.isInstanceOf[SpillableColumnarBatchFromBufferImpl])
            if (expectedResidency._1 == 0) {
              assert(SpillFramework.stores.deviceStore.spill(size) > 0)
            }
            if (expectedResidency._3 == 1) {
              assert(SpillFramework.stores.hostStore.spill(size) > 0)
            }
            assert(residency == expectedResidency)
            // Evict it, then evict the reloaded copy before any task materializes it
            val (bytes, bytesAgain) = evictWhileGpuIsBusy {
              val bytes = write(broadcast)
              (bytes, withBroadcast(read(bytes))(write))
            }
            assert(residency == expectedResidency)
            withBroadcast(read(bytes))(assertSameData(expected, _))
            withBroadcast(read(bytesAgain))(assertSameData(expected, _))
            assertSameData(expected, broadcast)
          }
        }
      }
  }

  Seq(false, true).foreach { packed =>
    test(s"evicting a broadcast does not wait for a task building its GPU batch, " +
        s"packed=$packed") {
      withResource(buildBatch()) { expected =>
        val bytes = withBroadcast(makeBroadcastBatch(expected)) { b =>
          if (packed) {
            b.batch
          }
          write(b)
        }
        val building = new CountDownLatch(1)
        val mayBuild = new CountDownLatch(1)
        // A task pauses while building the GPU batch, as it would while waiting for device memory
        val broadcast = new SerializeConcatHostBuffersDeserializeBatch(null, Seq.empty, 0, 0L) {
          override protected def buildGpuBatch(): SpillableColumnarBatch = {
            building.countDown()
            assert(mayBuild.await(30, TimeUnit.SECONDS))
            super.buildGpuBatch()
          }
        }
        withResource(new ObjectInputStream(new ByteArrayInputStream(bytes))) { ois =>
          broadcast.doReadObject(ois)
        }
        withBroadcast(broadcast) { _ =>
          val memoryManager = SparkEnv.get.memoryManager
          val pool = Executors.newFixedThreadPool(2)
          try {
            val materializer = pool.submit(new Callable[SpillableColumnarBatch] {
              override def call(): SpillableColumnarBatch = broadcast.batch
            })
            assert(building.await(30, TimeUnit.SECONDS))
            val evictor = pool.submit(new Callable[Array[Byte]] {
              override def call(): Array[Byte] = memoryManager.synchronized(write(broadcast))
            })
            val evicted = try {
              evictor.get(30, TimeUnit.SECONDS)
            } catch {
              case _: TimeoutException =>
                fail("deadlock: eviction waits for a task building the GPU batch")
            }
            mayBuild.countDown()
            materializer.get(30, TimeUnit.SECONDS)
            withBroadcast(read(evicted))(assertSameData(expected, _))
            assertSameData(expected, broadcast)
          } finally {
            mayBuild.countDown()
            pool.shutdown()
            pool.awaitTermination(30, TimeUnit.SECONDS)
          }
        }
      }
    }
  }

  test("reading an unknown broadcast stream format fails") {
    val baos = new ByteArrayOutputStream()
    withResource(new ObjectOutputStream(baos))(_.writeByte(Byte.MaxValue))
    assertThrows[IllegalStateException](read(baos.toByteArray).closeInternal())
  }

  test("reading a truncated packed broadcast fails") {
    withResource(buildBatch()) { expected =>
      val bytes = withBroadcast(makeBroadcastBatch(expected)) { b =>
        b.batch
        write(b)
      }
      val truncated = java.util.Arrays.copyOf(bytes, bytes.length / 2)
      assertThrows[Exception](read(truncated).closeInternal())
    }
  }
}
