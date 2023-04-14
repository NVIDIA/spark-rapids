/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, Cuda, HostColumnVector, HostMemoryBuffer, MemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm._
import org.mockito.{ArgumentCaptor, ArgumentMatchers}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, spy, times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class RapidsHostMemoryStoreSuite extends FunSuite with MockitoSugar {
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
    withResource(HostColumnVector.fromLongs(vals: _*)) { hcv =>
      withResource(hcv.copyToDevice()) { cv =>
        withResource(new Table(cv)) { table =>
          table.contiguousSplit()(0)
        }
      }
    }
  }

  test("spill updates catalog") {
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    val mockStore = mock[RapidsHostMemoryStore]
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = spy(new RapidsBufferCatalog(devStore))
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, hostStoreMaxSize)) {
        hostStore =>
          assertResult(0)(hostStore.currentSize)
          assertResult(hostStoreMaxSize)(hostStore.numBytesFree)
          devStore.setSpillStore(hostStore)
          hostStore.setSpillStore(mockStore)

          val (bufferSize, handle) = withResource(buildContiguousTable()) { ct =>
            val len = ct.getBuffer.getLength
            // store takes ownership of the table
            val handle = catalog.addContiguousTable(
              ct,
              spillPriority)
            (len, handle)
          }

          catalog.synchronousSpill(devStore, 0)
          assertResult(bufferSize)(hostStore.currentSize)
          assertResult(hostStoreMaxSize - bufferSize)(hostStore.numBytesFree)
          verify(catalog, times(2)).registerNewBuffer(ArgumentMatchers.any[RapidsBuffer])
          verify(catalog).removeBufferTier(
            ArgumentMatchers.eq(handle.id), ArgumentMatchers.eq(StorageTier.DEVICE))
          withResource(catalog.acquireBuffer(handle)) { buffer =>
            assertResult(StorageTier.HOST)(buffer.storageTier)
            assertResult(bufferSize)(buffer.size)
            assertResult(handle.id)(buffer.id)
            assertResult(spillPriority)(buffer.getSpillPriority)
          }
      }
    }
  }

  test("get columnar batch") {
    val spillPriority = -10
    val hostStoreMaxSize = 1L * 1024 * 1024
    val mockStore = mock[RapidsHostMemoryStore]
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, hostStoreMaxSize)) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          hostStore.setSpillStore(mockStore)
          var expectedBuffer: HostMemoryBuffer = null
          val handle = withResource(buildContiguousTable()) { ct =>
            expectedBuffer = HostMemoryBuffer.allocate(ct.getBuffer.getLength)
            expectedBuffer.copyFromDeviceBuffer(ct.getBuffer)
            catalog.addContiguousTable(
              ct,
              spillPriority)
          }
          withResource(expectedBuffer) { _ =>
            catalog.synchronousSpill(devStore, 0)
            withResource(catalog.acquireBuffer(handle)) { buffer =>
              withResource(buffer.getMemoryBuffer) { actualBuffer =>
                assert(actualBuffer.isInstanceOf[HostMemoryBuffer])
                assertResult(expectedBuffer.asByteBuffer) {
                  actualBuffer.asInstanceOf[HostMemoryBuffer].asByteBuffer
                }
              }
            }
          }
      }
    }
  }

  test("get memory buffer") {
    val sparkTypes = Array[DataType](IntegerType, StringType, DoubleType,
      DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5))
    val spillPriority = -10
    val hostStoreMaxSize = 1L * 1024 * 1024
    val mockStore = mock[RapidsHostMemoryStore]
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, hostStoreMaxSize)) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          hostStore.setSpillStore(mockStore)
          var expectedBatch: ColumnarBatch = null
          val handle = withResource(buildContiguousTable()) { ct =>
            // make a copy of the table so we can compare it later to the
            // one reconstituted after the spill
            withResource(ct.getTable.contiguousSplit()) { copied  =>
              expectedBatch = GpuColumnVector.from(copied(0).getTable, sparkTypes)
            }
            catalog.addContiguousTable(
              ct,
              spillPriority)
          }
          withResource(expectedBatch) { _ =>
            catalog.synchronousSpill(devStore, 0)
            withResource(catalog.acquireBuffer(handle)) { buffer =>
              assertResult(StorageTier.HOST)(buffer.storageTier)
              withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
                TestUtils.compareBatches(expectedBatch, actualBatch)
              }
            }
          }
      }
    }
  }

  test("buffer exceeds maximum size") {
    val sparkTypes = Array[DataType](LongType)
    val spillPriority = -10
    val hostStoreMaxSize = 256
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      val mockStore = mock[RapidsBufferStore]
      val mockBuff = mock[mockStore.RapidsBufferBase]
      when(mockBuff.id).thenReturn(new RapidsBufferId {
        override val tableId: Int = 0
        override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File = null
      })
      when(mockStore.getMaxSize).thenAnswer(_ => None)
      when(mockStore.copyBuffer(any(), any(), any())).thenReturn(mockBuff)
      when(mockStore.tier) thenReturn (StorageTier.DISK)
      withResource(new RapidsHostMemoryStore(hostStoreMaxSize, hostStoreMaxSize)) { hostStore =>
        devStore.setSpillStore(hostStore)
        hostStore.setSpillStore(mockStore)
        var bigHandle: RapidsBufferHandle = null
        var bigTable = buildContiguousTable(1024 * 1024)
        var smallTable = buildContiguousTable(1)
        closeOnExcept(bigTable) { _ =>
          closeOnExcept(smallTable) { _ =>
            // make a copy of the table so we can compare it later to the
            // one reconstituted after the spill
            val expectedBatch =
              withResource(bigTable.getTable.contiguousSplit()) { expectedTable =>
                GpuColumnVector.from(expectedTable(0).getTable, sparkTypes)
              }
            withResource(expectedBatch) { _ =>
              bigHandle = withResource(bigTable) { _ =>
                catalog.addContiguousTable(
                  bigTable,
                  spillPriority)
              } // close the bigTable so it can be spilled
              bigTable = null
              catalog.synchronousSpill(devStore, 0)
              verify(mockStore, never()).copyBuffer(ArgumentMatchers.any[RapidsBuffer],
                ArgumentMatchers.any[MemoryBuffer],
                ArgumentMatchers.any[Cuda.Stream])
              withResource(catalog.acquireBuffer(bigHandle)) { buffer =>
                assertResult(StorageTier.HOST)(buffer.storageTier)
                withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
                  TestUtils.compareBatches(expectedBatch, actualBatch)
                }
              }
            }
            withResource(smallTable) { _ =>
              catalog.addContiguousTable(
                smallTable, spillPriority,
                false)
            } // close the smallTable so it can be spilled
            smallTable = null
            catalog.synchronousSpill(devStore, 0)
            val rapidsBufferCaptor: ArgumentCaptor[RapidsBuffer] =
              ArgumentCaptor.forClass(classOf[RapidsBuffer])
            val memoryBufferCaptor: ArgumentCaptor[MemoryBuffer] =
              ArgumentCaptor.forClass(classOf[MemoryBuffer])
            verify(mockStore).copyBuffer(rapidsBufferCaptor.capture(),
              memoryBufferCaptor.capture(), ArgumentMatchers.any[Cuda.Stream])
            withResource(memoryBufferCaptor.getValue) { _ =>
              assertResult(bigHandle.id)(rapidsBufferCaptor.getValue.id)
            }
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
