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

import ai.rapids.cudf.{ContiguousTable, HostMemoryBuffer, Table}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{spy, verify}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

class RapidsHostMemoryStoreSuite extends FunSuite with Arm with MockitoSugar {
  private def buildContiguousTable(): ContiguousTable = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  test("spill updates catalog") {
    val bufferId = MockRapidsBufferId(7)
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    val catalog = spy(new RapidsBufferCatalog)
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsHostMemoryStore(catalog, hostStoreMaxSize)) { hostStore =>
        assertResult(0)(hostStore.currentSize)
        assertResult(hostStoreMaxSize)(hostStore.numBytesFree)
        devStore.setSpillStore(hostStore)

        val ct = buildContiguousTable()
        val bufferSize = ct.getBuffer.getLength
        try {
          // store takes ownership of the table
          devStore.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
        } catch {
          case t: Throwable =>
            ct.close()
            throw t
        }

        devStore.synchronousSpill(0)
        assertResult(bufferSize)(hostStore.currentSize)
        assertResult(hostStoreMaxSize - bufferSize)(hostStore.numBytesFree)
        verify(catalog).updateBufferMap(
          ArgumentMatchers.eq(StorageTier.DEVICE), ArgumentMatchers.any[RapidsBuffer])
        withResource(catalog.acquireBuffer(bufferId)) { buffer =>
          assertResult(StorageTier.HOST)(buffer.storageTier)
          assertResult(bufferSize)(buffer.size)
          assertResult(bufferId)(buffer.id)
          assertResult(spillPriority)(buffer.getSpillPriority)
        }
      }
    }
  }

  test("get columnar batch") {
    val bufferId = MockRapidsBufferId(7)
    val spillPriority = -10
    val hostStoreMaxSize = 1L * 1024 * 1024
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsHostMemoryStore(catalog, hostStoreMaxSize)) { hostStore =>
        devStore.setSpillStore(hostStore)
        var ct = buildContiguousTable()
        try {
          withResource(HostMemoryBuffer.allocate(ct.getBuffer.getLength)) { expectedBuffer =>
            expectedBuffer.copyFromDeviceBuffer(ct.getBuffer)
            // store takes ownership of the table
            devStore.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
            ct = null

            devStore.synchronousSpill(0)
            withResource(catalog.acquireBuffer(bufferId)) { buffer =>
              withResource(buffer.getMemoryBuffer) { actualBuffer =>
                assert(actualBuffer.isInstanceOf[HostMemoryBuffer])
                assertResult(expectedBuffer.asByteBuffer) {
                  actualBuffer.asInstanceOf[HostMemoryBuffer].asByteBuffer
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
  }

  test("get memory buffer") {
    val bufferId = MockRapidsBufferId(7)
    val spillPriority = -10
    val hostStoreMaxSize = 1L * 1024 * 1024
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsHostMemoryStore(catalog, hostStoreMaxSize)) { hostStore =>
        devStore.setSpillStore(hostStore)
        var ct = buildContiguousTable()
        try {
          withResource(GpuColumnVector.from(ct.getTable)) { expectedBatch =>
            // store takes ownership of the table
            devStore.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
            ct = null

            devStore.synchronousSpill(0)
            withResource(catalog.acquireBuffer(bufferId)) { buffer =>
              assertResult(StorageTier.HOST)(buffer.storageTier)
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
  }

  case class MockRapidsBufferId(tableId: Int) extends RapidsBufferId {
    override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
      throw new UnsupportedOperationException
  }
}
