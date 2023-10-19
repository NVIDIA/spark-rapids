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

import ai.rapids.cudf.{ColumnVector, ContiguousTable, Cuda, DeviceMemoryBuffer, HostMemoryBuffer, Table}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify, when}
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.{RapidsDiskBlockManager, TempSpillBufferId}
import org.apache.spark.sql.types.{DataType, DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.storage.{BlockId, ShuffleBlockId}


class RapidsDiskStoreSuite extends FunSuiteWithTempDir with MockitoSugar {

  private def buildContiguousTable(): ContiguousTable = {
    withResource(buildTable()) { table =>
      table.contiguousSplit()(0)
    }
  }

  private def buildTable(): Table = {
    new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1)
        .column("five", "two", null, null)
        .column(5.0, 2.0, 3.0, 1.0)
        .decimal64Column(-5, RoundingMode.UNNECESSARY, 0, null, -1.4, 10.123)
        .build()
  }

  private def buildEmptyTable(): Table = {
    withResource(buildTable()) { tbl =>
      withResource(ColumnVector.fromBooleans(false, false, false, false)) { mask =>
        tbl.filter(mask) // filter all out
      }
    }
  }

  private val mockTableDataTypes: Array[DataType] =
    Array(IntegerType, StringType, DoubleType, DecimalType(10, 5))

  test("spill updates catalog") {
    val bufferId = MockRapidsBufferId(7, canShareDiskPaths = false)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = spy(new RapidsBufferCatalog(devStore))
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            assertResult(0)(diskStore.currentSize)
            hostStore.setSpillStore(diskStore)
            val (bufferSize, handle) =
              addContiguousTableToCatalog(catalog, bufferId, spillPriority)
            val path = handle.id.getDiskPath(null)
            assert(!path.exists())
            catalog.synchronousSpill(devStore, 0)
            catalog.synchronousSpill(hostStore, 0)
            assertResult(0)(hostStore.currentSize)
            assertResult(bufferSize)(diskStore.currentSize)
            assert(path.exists)
            assertResult(bufferSize)(path.length)
            verify(catalog, times(3)).registerNewBuffer(ArgumentMatchers.any[RapidsBuffer])
            verify(catalog).removeBufferTier(
              ArgumentMatchers.eq(handle.id), ArgumentMatchers.eq(StorageTier.DEVICE))
            withResource(catalog.acquireBuffer(handle)) { buffer =>
              assertResult(StorageTier.DISK)(buffer.storageTier)
              assertResult(bufferSize)(buffer.memoryUsedBytes)
              assertResult(handle.id)(buffer.id)
              assertResult(spillPriority)(buffer.getSpillPriority)
            }
          }
      }
    }
  }

  test("Get columnar batch") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    assert(!bufferPath.exists)
    val sparkTypes = Array[DataType](IntegerType, StringType, DoubleType,
      DecimalType(ai.rapids.cudf.DType.DECIMAL64_MAX_PRECISION, 5))
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) {
            diskStore =>
              hostStore.setSpillStore(diskStore)
              val (_, handle) = addContiguousTableToCatalog(catalog, bufferId, spillPriority)
              assert(!handle.id.getDiskPath(null).exists())
              val expectedTable = withResource(catalog.acquireBuffer(handle)) { buffer =>
                assertResult(StorageTier.DEVICE)(buffer.storageTier)
                withResource(buffer.getColumnarBatch(sparkTypes)) { beforeSpill =>
                  withResource(GpuColumnVector.from(beforeSpill)) { table =>
                    table.contiguousSplit()(0)
                  }
                } // closing the batch from the store so that we can spill it
              }
              withResource(expectedTable) { _ =>
                withResource(
                    GpuColumnVector.from(expectedTable.getTable, sparkTypes)) { expectedBatch =>
                  catalog.synchronousSpill(devStore, 0)
                  catalog.synchronousSpill(hostStore, 0)
                  withResource(catalog.acquireBuffer(handle)) { buffer =>
                    assertResult(StorageTier.DISK)(buffer.storageTier)
                    withResource(buffer.getColumnarBatch(sparkTypes)) { actualBatch =>
                      TestUtils.compareBatches(expectedBatch, actualBatch)
                    }
                  }
                }
              }
          }
      }
    }
  }

  test("get memory buffer") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    assert(!bufferPath.exists)
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            val (_, handle) = addContiguousTableToCatalog(catalog, bufferId, spillPriority)
            assert(!handle.id.getDiskPath(mockDiskBlockManager).exists())
            val expectedBuffer = withResource(catalog.acquireBuffer(handle)) { buffer =>
              assertResult(StorageTier.DEVICE)(buffer.storageTier)
              withResource(buffer.getMemoryBuffer) { devbuf =>
                closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
                  hostbuf.copyFromDeviceBuffer(devbuf.asInstanceOf[DeviceMemoryBuffer])
                  hostbuf
                }
              }
            }
            withResource(expectedBuffer) { expectedBuffer =>
              catalog.synchronousSpill(devStore, 0)
              catalog.synchronousSpill(hostStore, 0)
              withResource(catalog.acquireBuffer(handle)) { buffer =>
                assertResult(StorageTier.DISK)(buffer.storageTier)
                withResource(buffer.getMemoryBuffer) { actualBuffer =>
                  assert(actualBuffer.isInstanceOf[HostMemoryBuffer])
                  val actualHostBuffer = actualBuffer.asInstanceOf[HostMemoryBuffer]
                  assertResult(expectedBuffer.
                    asByteBuffer.limit())(actualHostBuffer.asByteBuffer.limit())
                }
              }
            }
          }
      }
    }
  }

  test("Compression on with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, TempSpillBufferId.apply())
    }
  }

  test("Compression off with or without encryption for spill block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, TempSpillBufferId.apply())
    }
  }

  test("Compression on with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, TempSpillBufferId.apply(), TempSpillBufferId.apply())
    }
  }

  test("Compression off with or without encryption for spill block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, TempSpillBufferId.apply(), TempSpillBufferId.apply())
    }
  }

  // ===== Tests for shuffle block =====

  test("Compression on with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf, ShuffleBufferId(ShuffleBlockId(1, 1, 1), 1))
    }
  }

  test("Compression off with or without encryption for shuffle block using single batch") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf, ShuffleBufferId(ShuffleBlockId(1, 1, 1), 1))
    }
  }

  test("Compression on with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.io.compression.codec", "zstd")
      conf.set("spark.shuffle.spill.compress", "true")
      conf.set("spark.shuffle.compress", "true")
      readWriteTestWithBatches(conf,
        ShuffleBufferId(ShuffleBlockId(1, 1, 1), 1), ShuffleBufferId(ShuffleBlockId(2, 2, 2), 2))
    }
  }

  test("Compression off with or without encryption for shuffle block using multiple batches") {
    Seq("true", "false").foreach { encryptionEnabled =>
      val conf = new SparkConf()
      conf.set(RapidsConf.TEST_IO_ENCRYPTION.key, encryptionEnabled)
      conf.set("spark.shuffle.spill.compress", "false")
      conf.set("spark.shuffle.compress", "false")
      readWriteTestWithBatches(conf,
        ShuffleBufferId(ShuffleBlockId(1, 1, 1), 1), ShuffleBufferId(ShuffleBlockId(2, 2, 2), 2))
    }
  }

  test("No encryption and compression for shuffle block using multiple batches") {
    readWriteTestWithBatches(new SparkConf(),
      ShuffleBufferId(ShuffleBlockId(1, 1, 1), 1), ShuffleBufferId(ShuffleBlockId(2, 2, 2), 2))
  }

  private def readWriteTestWithBatches(conf: SparkConf, bufferIds: RapidsBufferId*) = {
    assert(bufferIds.size != 0)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(conf))

    if (bufferIds(0).canShareDiskPaths) {
      // Return the same path
      val bufferPath = new File(TEST_FILES_ROOT, s"diskbuffer-${bufferIds(0).tableId}")
      when(mockDiskBlockManager.getFile(any[BlockId]())).thenReturn(bufferPath)
      if (bufferPath.exists) bufferPath.delete()
    } else {
      when(mockDiskBlockManager.getFile(any[BlockId]()))
        .thenAnswer { invocation =>
          new File(TEST_FILES_ROOT, s"diskbuffer-${invocation.getArgument[BlockId](0).name}")
        }
    }

    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            bufferIds.foreach { bufferId =>
              val (_, handle) = addContiguousTableToCatalog(catalog, bufferId, spillPriority)
              val expectedBuffer = withResource(catalog.acquireBuffer(handle)) { buffer =>
                assertResult(StorageTier.DEVICE)(buffer.storageTier)
                withResource(buffer.getMemoryBuffer) { devbuf =>
                  closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
                    hostbuf.copyFromDeviceBuffer(devbuf.asInstanceOf[DeviceMemoryBuffer])
                    hostbuf
                  }
                }
              }
              withResource(expectedBuffer) { expectedBuffer =>
                catalog.synchronousSpill(devStore, 0)
                catalog.synchronousSpill(hostStore, 0)
                withResource(catalog.acquireBuffer(handle)) { buffer =>
                  assertResult(StorageTier.DISK)(buffer.storageTier)
                  withResource(buffer.getMemoryBuffer) { actualBuffer =>
                    assert(actualBuffer.isInstanceOf[HostMemoryBuffer])
                    val actualHostBuffer = actualBuffer.asInstanceOf[HostMemoryBuffer]
                    assertResult(expectedBuffer.asByteBuffer)(actualHostBuffer.asByteBuffer)
                  }
                }
              }
            }
          }
      }
    }
  }

  test("skip host: spill device memory buffer to disk") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    assert(!bufferPath.exists)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new AlwaysFailingRapidsHostMemoryStore) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            val (_, handle) = addContiguousTableToCatalog(catalog, bufferId, spillPriority)
            assert(!handle.id.getDiskPath(null).exists())
            val expectedBuffer = withResource(catalog.acquireBuffer(handle)) { buffer =>
              assertResult(StorageTier.DEVICE)(buffer.storageTier)
              withResource(buffer.getMemoryBuffer) { devbuf =>
                closeOnExcept(HostMemoryBuffer.allocate(devbuf.getLength)) { hostbuf =>
                  hostbuf.copyFromDeviceBuffer(devbuf.asInstanceOf[DeviceMemoryBuffer])
                  hostbuf
                }
              }
            }
            withResource(expectedBuffer) { expectedBuffer =>
              catalog.synchronousSpill(devStore, 0)
              withResource(catalog.acquireBuffer(handle)) { buffer =>
                assertResult(StorageTier.DISK)(buffer.storageTier)
                withResource(buffer.getMemoryBuffer) { actualBuffer =>
                  assert(actualBuffer.isInstanceOf[HostMemoryBuffer])
                  val actualHostBuffer = actualBuffer.asInstanceOf[HostMemoryBuffer]
                  assertResult(expectedBuffer.asByteBuffer)(actualHostBuffer.asByteBuffer)
                }
              }
            }
          }
      }
    }
  }

  test("skip host: spill table to disk") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    assert(!bufferPath.exists)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new AlwaysFailingRapidsHostMemoryStore) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            val handle = addTableToCatalog(catalog, bufferId, spillPriority)
            withResource(buildTable()) { expectedTable =>
              withResource(
                  GpuColumnVector.from(expectedTable, mockTableDataTypes)) { expectedBatch =>
                catalog.synchronousSpill(devStore, 0)
                withResource(catalog.acquireBuffer(handle)) { buffer =>
                  assert(handle.id.getDiskPath(null).exists())
                  assertResult(StorageTier.DISK)(buffer.storageTier)
                  withResource(buffer.getColumnarBatch(mockTableDataTypes)) { fromDiskBatch =>
                    TestUtils.compareBatches(expectedBatch, fromDiskBatch)
                  }
                }
              }
            }
          }
      }
    }
  }

  test("skip host: spill table to disk with small host bounce buffer") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    assert(!bufferPath.exists)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    withResource(new RapidsDeviceMemoryStore(1L*1024*1024, 10)) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new AlwaysFailingRapidsHostMemoryStore) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            val handle = addTableToCatalog(catalog, bufferId, spillPriority)
            withResource(buildTable()) { expectedTable =>
              withResource(
                GpuColumnVector.from(expectedTable, mockTableDataTypes)) { expectedBatch =>
                catalog.synchronousSpill(devStore, 0)
                withResource(catalog.acquireBuffer(handle)) { buffer =>
                  assert(handle.id.getDiskPath(null).exists())
                  assertResult(StorageTier.DISK)(buffer.storageTier)
                  withResource(buffer.getColumnarBatch(mockTableDataTypes)) { fromDiskBatch =>
                    TestUtils.compareBatches(expectedBatch, fromDiskBatch)
                  }
                }
              }
            }
          }
      }
    }
  }


  test("0-byte table is never spillable as we would fail to mmap") {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths = false)
    val bufferPath = bufferId.getDiskPath(null)
    val bufferId2 = MockRapidsBufferId(2, canShareDiskPaths = false)
    assert(!bufferPath.exists)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) { hostStore =>
        devStore.setSpillStore(hostStore)
        withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
          hostStore.setSpillStore(diskStore)
          val handle = addZeroRowsTableToCatalog(catalog, bufferId, spillPriority - 1)
          val handle2 = addTableToCatalog(catalog, bufferId2, spillPriority)
          withResource(handle2) { _ =>
            assert(!handle.id.getDiskPath(null).exists())
            withResource(buildTable()) { expectedTable =>
              withResource(buildEmptyTable()) { expectedEmptyTable =>
                withResource(
                  GpuColumnVector.from(
                    expectedTable, mockTableDataTypes)) { expectedCb =>
                  withResource(
                    GpuColumnVector.from(
                      expectedEmptyTable, mockTableDataTypes)) { expectedEmptyCb =>
                    catalog.synchronousSpill(devStore, 0)
                    catalog.synchronousSpill(hostStore, 0)
                    withResource(catalog.acquireBuffer(handle2)) { buffer =>
                      withResource(catalog.acquireBuffer(handle)) { emptyBuffer =>
                        // the 0-byte table never moved from device. It is not spillable
                        assertResult(StorageTier.DEVICE)(emptyBuffer.storageTier)
                        withResource(emptyBuffer.getColumnarBatch(mockTableDataTypes)) { cb =>
                          TestUtils.compareBatches(expectedEmptyCb, cb)
                        }
                        // the second table (with rows) did spill
                        assertResult(StorageTier.DISK)(buffer.storageTier)
                        withResource(buffer.getColumnarBatch(mockTableDataTypes)) { cb =>
                          TestUtils.compareBatches(expectedCb, cb)
                        }
                      }
                    }
                    assertResult(0)(devStore.currentSize)
                    assertResult(0)(hostStore.currentSize)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  test("exclusive spill files are deleted when buffer deleted") {
    testBufferFileDeletion(canShareDiskPaths = false)
  }

  test("shared spill files are not deleted when a buffer is deleted") {
    testBufferFileDeletion(canShareDiskPaths = true)
  }

  class AlwaysFailingRapidsHostMemoryStore extends RapidsHostMemoryStore(Some(0L)){
    override def createBuffer(
        other: RapidsBuffer,
        catalog: RapidsBufferCatalog,
        stream: Cuda.Stream): Option[RapidsBufferBase] = {
      None
    }
  }

  private def testBufferFileDeletion(canShareDiskPaths: Boolean): Unit = {
    val bufferId = MockRapidsBufferId(1, canShareDiskPaths)
    val bufferPath = bufferId.getDiskPath(null)
    assert(!bufferPath.exists)
    val mockDiskBlockManager = mock[RapidsDiskBlockManager]
    when(mockDiskBlockManager.getSerializerManager())
      .thenReturn(new RapidsSerializerManager(new SparkConf()))
    val spillPriority = -7
    val hostStoreMaxSize = 1L * 1024 * 1024
    withResource(new RapidsDeviceMemoryStore) { devStore =>
      val catalog = new RapidsBufferCatalog(devStore)
      withResource(new RapidsHostMemoryStore(Some(hostStoreMaxSize))) {
        hostStore =>
          devStore.setSpillStore(hostStore)
          withResource(new RapidsDiskStore(mockDiskBlockManager)) { diskStore =>
            hostStore.setSpillStore(diskStore)
            val (_, handle) = addContiguousTableToCatalog(catalog, bufferId, spillPriority)
            val bufferPath = handle.id.getDiskPath(null)
            assert(!bufferPath.exists())
            catalog.synchronousSpill(devStore, 0)
            catalog.synchronousSpill(hostStore, 0)
            assert(bufferPath.exists)
            handle.close()
            if (canShareDiskPaths) {
              assert(bufferPath.exists())
            } else {
              assert(!bufferPath.exists)
            }
          }
      }
    }
  }

  private def addContiguousTableToCatalog(
      catalog: RapidsBufferCatalog,
      bufferId: RapidsBufferId,
      spillPriority: Long): (Long, RapidsBufferHandle) = {
    withResource(buildContiguousTable()) { ct =>
      val bufferSize = ct.getBuffer.getLength
      // store takes ownership of the table
      val handle = catalog.addContiguousTable(
        bufferId,
        ct,
        spillPriority,
        false)
      (bufferSize, handle)
    }
  }

  private def addTableToCatalog(
      catalog: RapidsBufferCatalog,
      bufferId: RapidsBufferId,
      spillPriority: Long): RapidsBufferHandle = {
    // store takes ownership of the table
    catalog.addTable(
      bufferId,
      buildTable(),
      spillPriority,
      false)
  }

  private def addZeroRowsTableToCatalog(
      catalog: RapidsBufferCatalog,
      bufferId: RapidsBufferId,
      spillPriority: Long): RapidsBufferHandle = {
    val table = buildEmptyTable()
    // store takes ownership of the table
    catalog.addTable(
      bufferId,
      table,
      spillPriority,
      false)
  }

  case class MockRapidsBufferId(
      tableId: Int,
      override val canShareDiskPaths: Boolean) extends RapidsBufferId {
    override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
      new File(TEST_FILES_ROOT, s"diskbuffer-$tableId")
  }
}
