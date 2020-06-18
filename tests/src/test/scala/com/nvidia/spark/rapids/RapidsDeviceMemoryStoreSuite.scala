/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

class RapidsDeviceMemoryStoreSuite extends FunSuite with Arm with MockitoSugar {
  private def buildContiguousTable(): ContiguousTable = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  test("add table registers with catalog") {
    val catalog = mock[RapidsBufferCatalog]
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val spillPriority = 3
      val bufferId = MockRapidsBufferId(7)
      var ct: ContiguousTable = buildContiguousTable()
      try {
        store.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
        ct = null
        val captor: ArgumentCaptor[RapidsBuffer] = ArgumentCaptor.forClass(classOf[RapidsBuffer])
        verify(catalog).registerNewBuffer(captor.capture())
        val resultBuffer = captor.getValue
        assertResult(bufferId)(resultBuffer.id)
        assertResult(spillPriority)(resultBuffer.getSpillPriority)
      } finally {
        if (ct != null) {
          ct.close()
        }
      }
    }
  }

  test("add buffer registers with catalog") {
    val catalog = mock[RapidsBufferCatalog]
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val spillPriority = 3
      val bufferId = MockRapidsBufferId(7)
      val ct = buildContiguousTable()
      val meta = try {
        val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct.getTable, ct.getBuffer)
        store.addBuffer(bufferId, ct.getBuffer, meta, spillPriority)
        meta
      } catch {
        case t: Throwable =>
          ct.close()
          throw t
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
      var ct = buildContiguousTable()
      try {
        withResource(HostMemoryBuffer.allocate(ct.getBuffer.getLength)) { expectedHostBuffer =>
          expectedHostBuffer.copyFromDeviceBuffer(ct.getBuffer)
          val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct.getTable, ct.getBuffer)
          store.addBuffer(bufferId, ct.getBuffer, meta, initialSpillPriority = 3)
          ct = null
          withResource(catalog.acquireBuffer(bufferId)) { buffer =>
            withResource(buffer.getMemoryBuffer.asInstanceOf[DeviceMemoryBuffer]) { devbuf =>
              withResource(HostMemoryBuffer.allocate(devbuf.getLength)) { actualHostBuffer =>
                actualHostBuffer.copyFromDeviceBuffer(devbuf)
                assertResult(expectedHostBuffer.asByteBuffer())(actualHostBuffer.asByteBuffer())
              }
            }
          }
        }
      } finally {
        if (ct != null) {
          ct.close()
        }
      }
    }
  }

  test("get column batch") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      val bufferId = MockRapidsBufferId(7)
      var ct = buildContiguousTable()
      try {
        withResource(GpuColumnVector.from(ct.getTable)) { expectedBatch =>
          val meta = MetaUtils.buildTableMeta(bufferId.tableId, ct.getTable, ct.getBuffer)
          store.addBuffer(bufferId, ct.getBuffer, meta, initialSpillPriority = 3)
          ct = null
          withResource(catalog.acquireBuffer(bufferId)) { buffer =>
            withResource(buffer.getColumnarBatch) { actualBatch =>
              TestUtils.compareBatches(expectedBatch, actualBatch)
            }
          }
        }
      } finally {
        if (ct != null) {
          ct.close()
        }
      }
    }
  }

  test("cannot receive spilled buffers") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      assertThrows[IllegalStateException](store.copyBuffer(mock[RapidsBuffer], Cuda.DEFAULT_STREAM))
    }
  }

  test("size statistics") {
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { store =>
      assertResult(0)(store.currentSize)
      val bufferSizes = new Array[Long](2)
      bufferSizes.indices.foreach { i =>
        val ct = buildContiguousTable()
        try {
          bufferSizes(i) = ct.getBuffer.getLength
          store.addTable(MockRapidsBufferId(i), ct.getTable, ct.getBuffer, initialSpillPriority = 0)
        } catch {
          case t: Throwable =>
            ct.close()
            throw t
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
        val ct = buildContiguousTable()
        try {
          bufferSizes(i) = ct.getBuffer.getLength
          store.addTable(MockRapidsBufferId(i), ct.getTable, ct.getBuffer, spillPriorities(i))
        } catch {
          case t: Throwable =>
            ct.close()
            throw t
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
      extends RapidsBufferStore("mock spill store", catalog) {
    val spilledBuffers = new ArrayBuffer[RapidsBufferId]

    override protected def createBuffer(b: RapidsBuffer, s: Cuda.Stream): RapidsBufferBase = {
      spilledBuffers += b.id
      new MockRapidsBuffer(b.id, b.size, b.meta, b.getSpillPriority)
    }

    class MockRapidsBuffer(id: RapidsBufferId, size: Long, meta: TableMeta, spillPriority: Long)
        extends RapidsBufferBase(id, size, meta, spillPriority) {
      override protected def releaseResources(): Unit = {}

      override val storageTier: StorageTier = StorageTier.HOST

      override def getMemoryBuffer: MemoryBuffer =
        throw new UnsupportedOperationException
    }
  }
}
