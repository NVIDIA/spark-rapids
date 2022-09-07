/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids

import java.io.{ByteArrayOutputStream, InputStream}
import java.nio.ByteBuffer

import com.nvidia.spark.rapids.SparkSessionHolder
import org.mockito.ArgumentMatchers.{eq => meq}
import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.sql.rapids.shims.RapidsShuffleThreadedReader
import org.apache.spark.storage.{BlockManager, BlockManagerId, ShuffleBlockId}

/**
 *
 * Code ported over from `BlockStoreShuffleReaderSuite` in Apache Spark.
 *
 * Wrapper for a managed buffer that keeps track of how many times retain and release are called.
 *
 * We need to define this class ourselves instead of using a spy because the NioManagedBuffer class
 * is final (final classes cannot be spied on).
 */
class RecordingManagedBuffer(underlyingBuffer: NioManagedBuffer) extends ManagedBuffer {
  var callsToRetain = 0
  var callsToRelease = 0

  override def size(): Long = underlyingBuffer.size()
  override def nioByteBuffer(): ByteBuffer = underlyingBuffer.nioByteBuffer()
  override def createInputStream(): InputStream = underlyingBuffer.createInputStream()
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
    extends FunSuite with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    RapidsShuffleInternalManagerBase.stopThreadPool()
  }

  override def beforeAll(): Unit = {
    // stop any active contexts before we go an fiddle with them
    SparkContext.getActive.foreach(_.stop())
  }

  /**
   * This test makes sure that, when data is read from a HashShuffleReader, the underlying
   * ManagedBuffers that contain the data are eventually released.
   */
  Seq(1, 2).foreach { numReaderThreads =>
    test(s"read() releases resources on completion - numThreads=$numReaderThreads") {
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
        val serializer = new JavaSerializer(testConf)

        // Make a mock BlockManager that will return RecordingManagedByteBuffers of data, so that we
        // can ensure retain() and release() are properly called.
        val blockManager = mock(classOf[BlockManager])

        // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
        // from each mappers (all mappers return the same shuffle data).
        val byteOutputStream = new ByteArrayOutputStream()
        val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
        (0 until keyValuePairsPerMap).foreach { i =>
          serializationStream.writeKey(i)
          serializationStream.writeValue(2 * i)
        }

        // Setup the mocked BlockManager to return RecordingManagedBuffers.
        val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
        when(blockManager.blockManagerId).thenReturn(localBlockManagerId)
        val buffers = (0 until numMaps).map { mapId =>
          // Create a ManagedBuffer with the shuffle data.
          val nioBuffer = new NioManagedBuffer(ByteBuffer.wrap(byteOutputStream.toByteArray))
          val managedBuffer = new RecordingManagedBuffer(nioBuffer)

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
          val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
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
          serializerManager,
          blockManager,
          mapOutputTracker = mapOutputTracker,
          numReaderThreads = numReaderThreads)

        assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

        // Calling .length above will have exhausted the iterator; make sure that exhausting the
        // iterator caused retain and release to be called on each buffer.
        buffers.foreach { buffer =>
          assert(buffer.callsToRetain === 1)
          assert(buffer.callsToRelease === 1)
        }
      })
    }
  }
}
