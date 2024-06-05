/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "321"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "331"}
{"spark": "332"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "342"}
{"spark": "343"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import com.nvidia.spark.rapids.{GpuColumnarBatchSerializer, GpuColumnVector, NoopMetric, SparkSessionHolder}
import com.nvidia.spark.rapids.Arm.withResource
import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.rapids.shims.RapidsShuffleThreadedReader
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

class InjectedShuffleErrorInTests extends Exception {
}

class ErrorInputStream(wrapped: InputStream) extends InputStream {
  override def read(): Int = {
    throw new InjectedShuffleErrorInTests
  }
}

/**
 *
 * Code ported over from `BlockStoreShuffleReaderSuite` in Apache Spark.
 *
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on).
 */
class RecordingManagedBuffer(
    underlyingBuffer: NioManagedBuffer,
    injectError: Boolean) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = {
    val is = underlyingBuffer.createInputStream()
    if (injectError) {
      new ErrorInputStream(is)
    } else {
      is
    }
  }
  override def convertToNetty(): AnyRef = underlyingBuffer.convertToNetty()
  override def retain(): ManagedBuffer = {
    callsToRetain += 1
    underlyingBuffer.retain()
  }
  override def release(): ManagedBuffer = {
    callsToRelease += 1
    underlyingBuffer.release()
  }
}

class RapidsShuffleThreadedReaderSuite
    extends AnyFunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    RapidsShuffleInternalManagerBase.stopThreadPool()
  }

  def runShuffleRead(numReaderThreads: Int, injectError: Boolean = false): Unit = {
    val testConf = new SparkConf(false)
    // this sets the session and the SparkEnv
    SparkSessionHolder.withSparkSession(testConf, _ => {
      if (numReaderThreads > 1) {
        RapidsShuffleInternalManagerBase.startThreadPoolIfNeeded(0, numReaderThreads)
      }

      val reduceId = 15
      val shuffleId = 22
      val numMaps = 6
      val keyValuePairsPerMap = 10
      val serializer = new GpuColumnarBatchSerializer(NoopMetric)

      // Make a mock BlockManager that will return RecordingManagedByteBuffers of data, so that we
      // can ensure retain() and release() are properly called.
      val blockManager = mock(classOf[BlockManager])

      // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
      // from each mappers (all mappers return the same shuffle data).
      val byteOutputStream = new ByteArrayOutputStream()
      val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
      withResource(GpuColumnVector.emptyBatchFromTypes(Array.empty)) { emptyBatch =>
        (0 until keyValuePairsPerMap).foreach { i =>
          serializationStream.writeKey(i)
          serializationStream.writeValue(GpuColumnVector.incRefCounts(emptyBatch))
        }
      }

      // Setup the mocked BlockManager to return RecordingManagedBuffers.
      val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
      when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
      val buffers = (0 until numMaps).map { mapId =>
        // Create a ManagedBuffer with the shuffle data.
        val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
        val managedBuffer = new RecordingManagedBuffer(nioBuffer, injectError)

        // Setup the blockManager mock so the buffer gets returned when the shuffle code tries to
        // fetch shuffle data.
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        when(blockManager.getLocalBlockData(meq(shuffleBlockId))).thenReturn(managedBuffer)
        managedBuffer
      }

      // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
      // shuffle data to read.
      val mapOutputTracker = mock(classOf[MapOutputTracker])
      when(mapOutputTracker.getMapSizesByExecutorId(
        shuffleId, 0, numMaps, reduceId, reduceId + 1)).thenReturn {
        // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
        // for the code to read data over the network.
        val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
          val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
          (shuffleBlockId, byteOutputStream.size().toLong, mapId)
        }
        Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).iterator
      }

      // Create a mocked shuffle handle to pass into HashShuffleReader.
      val shuffleHandle = {
        val dependency = mock(classOf[GpuShuffleDependency[Int, Int, Int]])
        when(dependency.serializer).thenReturn(serializer)
        when(dependency.aggregator).thenReturn(None)
        when(dependency.keyOrdering).thenReturn(None)
        new ShuffleHandleWithMetrics[Int, Int, Int](
          shuffleId, Map.empty, dependency)
      }

      val serializerManager = new SerializerManager(
        serializer,
        new SparkConf()
          .set(config.SHUFFLE_COMPRESS, false)
          .set(config.SHUFFLE_SPILL_COMPRESS, false))

      val taskContext = TaskContext.empty()
      val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
      val shuffleReader = new RapidsShuffleThreadedReader[Int, Int](
        0,
        numMaps,
        reduceId,
        reduceId + 1,
        shuffleHandle,
        taskContext,
        metrics,
        1024 * 1024,
        serializerManager,
        blockManager,
        mapOutputTracker = mapOutputTracker,
        numReaderThreads = numReaderThreads)

      if (injectError) {
        var e: Throwable = null
        assertThrows[InjectedShuffleErrorInTests] {
          try {
            shuffleReader.read().length
          } catch {
            case t: Throwable =>
              e = t
              throw t
          }
        }
        taskContext.markTaskCompleted(Some(e))
      } else {
        assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)
        taskContext.markTaskCompleted(None)
      }

      // Calling .length above will have exhausted the iterator; make sure that exhausting the
      // iterator caused retain and release to be called on each buffer.
      buffers.foreach { buffer =>
        assert(buffer.callsToRetain === 1)
        assert(buffer.callsToRelease === 1)
      }
    })
  }

  /**
   * This test makes sure that, when data is read from a HashShuffleReader, the underlying
   * ManagedBuffers that contain the data are eventually released.
   */
  Seq(1, 2).foreach { numReaderThreads =>
    test(s"read() releases resources on completion - numThreads=$numReaderThreads") {
      runShuffleRead(numReaderThreads)
    }

    test(s"read() releases resources on error - numThreads=$numReaderThreads") {
      runShuffleRead(numReaderThreads, injectError = true)
    }
  }
}
