/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, CuFile, Table}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{spy, times, verify, when}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.compatible.Assertion
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager
import org.apache.spark.storage.BlockId

class RapidsGdsStoreSuite extends FunSuite with BeforeAndAfterEach with Arm with MockitoSugar {
  val TEST_FILES_ROOT: File = TestUtils.getTempDir(this.getClass.getSimpleName)

  override def beforeEach(): Unit = {
    TEST_FILES_ROOT.mkdirs()
  }

  override def afterEach(): Unit = {
    org.apache.commons.io.FileUtils.deleteDirectory(TEST_FILES_ROOT)
  }

  test("single shot spill with shared path") {
    assume(CuFile.libraryLoaded())
    verifySingleShotSpill(canShareDiskPaths = true)
  }

  test("single shot spill with exclusive path") {
    assume(CuFile.libraryLoaded())
    verifySingleShotSpill(canShareDiskPaths = false)
  }

  test("batch spill") {
    assume(CuFile.libraryLoaded())

    val bufferIds = Array(MockRapidsBufferId(7), MockRapidsBufferId(8), MockRapidsBufferId(9))
    val diskBlockManager = mock[RapidsDiskBlockManager]
    val paths = Array(
      new File(TEST_FILES_ROOT, s"gdsbuffer-0"), new File(TEST_FILES_ROOT, s"gdsbuffer-1"))
    when(diskBlockManager.getFile(any[BlockId]()))
        .thenReturn(paths(0))
        .thenReturn(paths(1))
    paths.foreach(f => assert(!f.exists))
    val spillPriority = -7
    val catalog = spy(new RapidsBufferCatalog)
    val batchWriteBufferSize = 16384 // Holds 2 buffers.
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsGdsStore(
        diskBlockManager, batchWriteBufferSize, catalog)) { gdsStore =>

        devStore.setSpillStore(gdsStore)
        assertResult(0)(gdsStore.currentSize)

        val bufferSizes = bufferIds.map(id => {
          val size = addTableToStore(devStore, id, spillPriority)
          devStore.synchronousSpill(0)
          size
        })
        val totalSize = bufferSizes.sum
        assertResult(totalSize)(gdsStore.currentSize)

        assert(paths(0).exists)
        assert(!paths(1).exists)
        val alignedSize = Math.ceil((bufferSizes(0) + bufferSizes(1)) / 4096d).toLong * 4096
        assertResult(alignedSize)(paths(0).length)

        verify(catalog, times(6)).registerNewBuffer(ArgumentMatchers.any[RapidsBuffer])
        (bufferIds, bufferSizes).zipped.foreach { (id, size) =>
          verify(catalog).removeBufferTier(
            ArgumentMatchers.eq(id), ArgumentMatchers.eq(StorageTier.DEVICE))
          withResource(catalog.acquireBuffer(id)) { buffer =>
            assertResult(StorageTier.GDS)(buffer.storageTier)
            assertResult(id)(buffer.id)
            assertResult(size)(buffer.size)
            assertResult(spillPriority)(buffer.getSpillPriority)
          }
        }

        catalog.removeBuffer(bufferIds(0))
        assert(paths(0).exists)
        catalog.removeBuffer(bufferIds(1))
        assert(!paths(0).exists)
      }
    }
  }

  private def verifySingleShotSpill(canShareDiskPaths: Boolean): Assertion = {
    val bufferId = MockRapidsBufferId(7, canShareDiskPaths)
    val path = bufferId.getDiskPath(null)
    assert(!path.exists)
    val spillPriority = -7
    val catalog = spy(new RapidsBufferCatalog)
    withResource(new RapidsDeviceMemoryStore(catalog)) { devStore =>
      withResource(new RapidsGdsStore(mock[RapidsDiskBlockManager], 4096, catalog)) { gdsStore =>
        devStore.setSpillStore(gdsStore)
        assertResult(0)(gdsStore.currentSize)
        val bufferSize = addTableToStore(devStore, bufferId, spillPriority)
        devStore.synchronousSpill(0)
        assertResult(bufferSize)(gdsStore.currentSize)
        assert(path.exists)
        assertResult(bufferSize)(path.length)
        verify(catalog, times(2)).registerNewBuffer(ArgumentMatchers.any[RapidsBuffer])
        verify(catalog).removeBufferTier(
          ArgumentMatchers.eq(bufferId), ArgumentMatchers.eq(StorageTier.DEVICE))
        withResource(catalog.acquireBuffer(bufferId)) { buffer =>
          assertResult(StorageTier.GDS)(buffer.storageTier)
          assertResult(bufferSize)(buffer.size)
          assertResult(bufferId)(buffer.id)
          assertResult(spillPriority)(buffer.getSpillPriority)
        }

        catalog.removeBuffer(bufferId)
        if (canShareDiskPaths) {
          assert(path.exists())
        } else {
          assert(!path.exists)
        }
      }
    }
  }

  private def addTableToStore(
      devStore: RapidsDeviceMemoryStore,
      bufferId: RapidsBufferId,
      spillPriority: Long): Long = {
    withResource(buildContiguousTable()) { ct =>
      val bufferSize = ct.getBuffer.getLength
      // store takes ownership of the table
      devStore.addContiguousTable(bufferId, ct, spillPriority)
      bufferSize
    }
  }

  /** Build a table of size 7808 bytes. */
  private def buildContiguousTable(): ContiguousTable = {
    withResource(new Table.TestBuilder()
        .column(Array.fill[String](512)("Lorem Ipsum"))
        .build()) { table =>
      table.contiguousSplit()(0)
    }
  }

  case class MockRapidsBufferId(
      tableId: Int,
      override val canShareDiskPaths: Boolean = false) extends RapidsBufferId {
    override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
      new File(TEST_FILES_ROOT, s"gdsbuffer-$tableId")
  }
}
