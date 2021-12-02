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

package com.nvidia.spark.rapids

import java.io.File
import java.math.RoundingMode

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, StringType}

class RapidsDeviceMemoryStoreSuite extends FunSuite with Arm with MockitoSugar {
  private def buildContiguousTable(): ContiguousTable = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  test("add table registers with catalog") {
    val catalog = mock[RapidsBufferCatalog]
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val spillPriority = 3
      val bufferId = MockRapidsBufferId(7)
      withResource(buildContiguousTable()) { ct =>
        store.addContiguousTable(bufferId, ct, spillPriority)
      }
      val captor: ArgumentCaptor[RapidsBuffer] = ArgumentCaptor.forClass(classOf[RapidsBuffer])
      verify(catalog).registerNewBuffer(captor.capture())
      val resultBuffer = captor.getValue
      assertResult(bufferId)(resultBuffer.id)
      assertResult(spillPriority)(resultBuffer.getSpillPriority)
    }
  }

  test("add buffer registers with catalog") {
    val catalog = mock[RapidsBufferCatalog]
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val spillPriority = 3
      val bufferId = MockRapidsBufferId(7)
      val meta = withResource(buildContiguousTable()) { ct =>
        val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct)
        // store takes ownership of the buffer
        ct.getBuffer.incRefCount()
        store.addBuffer(bufferId, ct.getBuffer, meta, spillPriority)
        meta
      }
      val captor: ArgumentCaptor[RapidsBuffer] = ArgumentCaptor.forClass(classOf[RapidsBuffer])
      verify(catalog).registerNewBuffer(captor.capture())
      val resultBuffer = captor.getValue
      assertResult(bufferId)(resultBuffer.id)
      assertResult(spillPriority)(resultBuffer.getSpillPriority)
      assertResult(meta)(resultBuffer.meta)
    }
  }

  test("get memory buffer") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val bufferId = MockRapidsBufferId(7)
      withResource(buildContiguousTable()) { ct =>
        withResource(HostMemoryBuffer.allocate(ct.getBuffer.getLength)) { expectedHostBuffer =>
          expectedHostBuffer.copyFromDeviceBuffer(ct.getBuffer)
          val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct)
          // store takes ownership of the buffer
          ct.getBuffer.incRefCount()
          store.addBuffer(bufferId, ct.getBuffer, meta, initialSpillPriority = 3)
          withResource(catalog.acquireBuffer(bufferId)) { buffer =>
            withResource(buffer.getMemoryBuffer.asInstanceOf[DeviceMemoryBuffer]) { devbuf =>
              withResource(HostMemoryBuffer.allocate(devbuf.getLength)) { actualHostBuffer =>
                actualHostBuffer.copyFromDeviceBuffer(devbuf)
                assertResult(expectedHostBuffer.asByteBuffer())(actualHostBuffer.asByteBuffer())
              }
            }
          }
        }
      }
    }
  }

  test("get column batch") {
    val catalog = new RapidsBufferCatalog
    val sparkTypes = Array[DataType](IntegerType, StringType, DoubleType,
      DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5))
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val bufferId = MockRapidsBufferId(7)
      withResource(buildContiguousTable()) { ct =>
        withResource(GpuColumnVector.from(ct.getTable, sparkTypes)) {
          expectedBatch =>
            val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct)
            // store takes ownership of the buffer
            ct.getBuffer.incRefCount()
            store.addBuffer(bufferId, ct.getBuffer, meta, initialSpillPriority = 3)
            withResource(catalog.acquireBuffer(bufferId)) { buffer =>
              withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
                TestUtils.compareBatches(expectedBatch, actualBatch)
              }
            }
        }
      }
    }
  }

  test("cannot receive spilled buffers") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      assertThrows[IllegalStateException](store.copyBuffer(
        mock[RapidsBuffer], mock[MemoryBuffer], Cuda.DEFAULT_STREAM))
    }
  }

  test("size statistics") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      assertResult(0)(store.currentSize)
      val bufferSizes = new Array[Long](2)
      bufferSizes.indices.foreach { i =>
        withResource(buildContiguousTable()) { ct =>
          bufferSizes(i) = ct.getBuffer.getLength
          // store takes ownership of the table
          store.addContiguousTable(MockRapidsBufferId(i), ct, initialSpillPriority = 0)
        }
        assertResult(bufferSizes.take(i+1).sum)(store.currentSize)
      }
      catalog.removeBuffer(MockRapidsBufferId(0))
      assertResult(bufferSizes(1))(store.currentSize)
      catalog.removeBuffer(MockRapidsBufferId(1))
      assertResult(0)(store.currentSize)
    }
  }

  test("spill") {
    val catalog = new RapidsBufferCatalog
    val spillStore = new MockSpillStore(catalog)
    val spillPriorities = Array(0, -1, 2)
    val bufferSizes = new Array[Long](spillPriorities.length)
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      store.setSpillStore(spillStore)
      spillPriorities.indices.foreach { i =>
        withResource(buildContiguousTable()) { ct =>
          bufferSizes(i) = ct.getBuffer.getLength
          // store takes ownership of the table
          store.addContiguousTable(MockRapidsBufferId(i), ct, spillPriorities(i))
        }
      }
      assert(spillStore.spilledBuffers.isEmpty)

      // asking to spill 0 bytes should not spill
      val sizeBeforeSpill = store.currentSize
      store.synchronousSpill(sizeBeforeSpill)
      assert(spillStore.spilledBuffers.isEmpty)
      assertResult(sizeBeforeSpill)(store.currentSize)
      store.synchronousSpill(sizeBeforeSpill + 1)
      assert(spillStore.spilledBuffers.isEmpty)
      assertResult(sizeBeforeSpill)(store.currentSize)

      // spilling 1 byte should force one buffer to spill in priority order
      store.synchronousSpill(sizeBeforeSpill - 1)
      assertResult(1)(spillStore.spilledBuffers.length)
      assertResult(bufferSizes.drop(1).sum)(store.currentSize)
      assertResult(1)(spillStore.spilledBuffers(0).tableId)

      // spilling to zero should force all buffers to spill in priority order
      store.synchronousSpill(0)
      assertResult(3)(spillStore.spilledBuffers.length)
      assertResult(0)(store.currentSize)
      assertResult(0)(spillStore.spilledBuffers(1).tableId)
      assertResult(2)(spillStore.spilledBuffers(2).tableId)
    }
  }

  case class MockRapidsBufferId(tableId: Int) extends RapidsBufferId {
    override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
      throw new UnsupportedOperationException
  }

  class MockSpillStore(catalog: RapidsBufferCatalog)
      extends RapidsBufferStore(StorageTier.HOST, catalog) with Arm {
    val spilledBuffers = new ArrayBuffer[RapidsBufferId]

    override protected def createBuffer(
        b: RapidsBuffer,
        m: MemoryBuffer,
        s: Cuda.Stream): RapidsBufferBase = {
      withResource(m) { _ =>
        spilledBuffers += b.id
        new MockRapidsBuffer(b.id, b.size, b.meta, b.getSpillPriority)
      }
    }

    class MockRapidsBuffer(id: RapidsBufferId, size: Long, meta: TableMeta, spillPriority: Long)
        extends RapidsBufferBase(id, size, meta, spillPriority, RapidsBuffer.defaultSpillCallback) {
      override protected def releaseResources(): Unit = {}

      override val storageTier: StorageTier = StorageTier.HOST

      override def getMemoryBuffer: MemoryBuffer =
        throw new UnsupportedOperationException
    }
  }
}
