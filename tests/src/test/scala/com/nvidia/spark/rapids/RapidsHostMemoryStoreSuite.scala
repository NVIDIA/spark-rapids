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
import java.math.RoundingMode

import ai.rapids.cudf.{ContiguousTable, Cuda, HostColumnVector, HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.Mockito.{never, spy, verify}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, LongType, StringType}

class RapidsHostMemoryStoreSuite extends FunSuite with Arm with MockitoSugar {
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

  private def buildContiguousTable(numRows: Int): ContiguousTable = {
    val vals = (0 until numRows).map(_.toLong)
    withResource(HostColumnVector.fromLongs(vals:_*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(new Table(cv)) { table =>
          table.contiguousSplit()(0)
        }
      }
    }
  }

  test("spill updates catalog") {
    val bufferId = MockRapidsBufferId(7)
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    val catalog = spy(new RapidsBufferCatalog)
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, catalog)) { hostStore =>
        assertResult(0)(hostStore.currentSize)
        assertResult(hostStoreMaxSize)(hostStore.numBytesFree)
        devStore.setSpillStore(hostStore)

        val bufferSize = closeOnExcept(buildContiguousTable()) { ct =>
          val len = ct.getBuffer.getLength
          // store takes ownership of the table
          devStore.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
          len
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
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, catalog)) { hostStore =>
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
    val sparkTypes = Array[DataType](IntegerType, StringType, DoubleType,
      DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5))
    val bufferId = MockRapidsBufferId(7)
    val spillPriority = -10
    val hostStoreMaxSize = 1L * 1024 * 1024
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, catalog)) { hostStore =>
        devStore.setSpillStore(hostStore)
        var ct = buildContiguousTable()
        try {
          withResource(GpuColumnVector.from(ct.getTable, sparkTypes)) {
            expectedBatch =>
              // store takes ownership of the table
              devStore.addTable(bufferId, ct.getTable, ct.getBuffer, spillPriority)
              ct = null

              devStore.synchronousSpill(0)
              withResource(catalog.acquireBuffer(bufferId)) { buffer =>
                assertResult(StorageTier.HOST)(buffer.storageTier)
                withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
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

  test("buffer exceeds maximum size") {
    val sparkTypes = Array[DataType](LongType)
    val bigBufferId = MockRapidsBufferId(7)
    val smallBufferId = MockRapidsBufferId(8)
    val spillPriority = -10
    val hostStoreMaxSize = 256
    val catalog = new RapidsBufferCatalog
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      val mockStore = mock[RapidsBufferStore]
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, catalog)) { hostStore =>
        devStore.setSpillStore(hostStore)
        hostStore.setSpillStore(mockStore)
        var bigTable: ContiguousTable = null
        var smallTable: ContiguousTable = null
        try {
          bigTable = buildContiguousTable(1024 * 1024)
          smallTable = buildContiguousTable(1)
          withResource(GpuColumnVector.from(bigTable.getTable, sparkTypes)) { expectedBatch =>
            // store takes ownership of the table
            devStore.addTable(bigBufferId, bigTable.getTable, bigTable.getBuffer, spillPriority)
            bigTable = null

            devStore.synchronousSpill(0)
            verify(mockStore, never()).copyBuffer(ArgumentMatchers.any[RapidsBuffer],
              ArgumentMatchers.any[Cuda.Stream])
            withResource(catalog.acquireBuffer(bigBufferId)) { buffer =>
              assertResult(StorageTier.HOST)(buffer.storageTier)
              withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
                TestUtils.compareBatches(expectedBatch, actualBatch)
              }
            }

            devStore.addTable(smallBufferId, smallTable.getTable, smallTable.getBuffer,
              spillPriority)
            smallTable = null
            devStore.synchronousSpill(0)
            val ac: ArgumentCaptor[RapidsBuffer] = ArgumentCaptor.forClass(classOf[RapidsBuffer])
            verify(mockStore).copyBuffer(ac.capture(), ArgumentMatchers.any[Cuda.Stream])
            assertResult(bigBufferId)(ac.getValue.id)
          }
        } finally {
          Seq(bigTable, smallTable).safeClose()
        }
      }
    }
  }

  case class MockRapidsBufferId(tableId: Int) extends RapidsBufferId {
    override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
      throw new UnsupportedOperationException
  }
}
