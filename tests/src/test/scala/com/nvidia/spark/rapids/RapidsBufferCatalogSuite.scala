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
import java.util.NoSuchElementException

import com.nvidia.spark.rapids.StorageTier.{DEVICE, DISK, GDS, HOST, StorageTier}
import com.nvidia.spark.rapids.format.TableMeta
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.sql.rapids.RapidsDiskBlockManager

class RapidsBufferCatalogSuite extends FunSuite with MockitoSugar {
  test("lookup unknown buffer") {
    val catalog = new RapidsBufferCatalog
    val bufferId = new RapidsBufferId {
      override val tableId: Int = 10
      override def getDiskPath(m: RapidsDiskBlockManager): File = null
    }
    assertThrows[NoSuchElementException](catalog.acquireBuffer(bufferId))
    assertThrows[NoSuchElementException](catalog.getBufferMeta(bufferId))
  }

  test("buffer double register throws") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId)
    assertThrows[DuplicateBufferException](catalog.registerNewBuffer(buffer2))
  }

  test("buffer registering slower tier does not hide faster tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer2)
    val buffer3 = mockBuffer(bufferId, tier = DISK)
    catalog.registerNewBuffer(buffer3)
    val acquired = catalog.acquireBuffer(MockBufferId(5))
    assertResult(5)(acquired.id.tableId)
    assertResult(buffer)(acquired)
    verify(buffer).addReference()
  }

  test("acquire buffer") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId)
    catalog.registerNewBuffer(buffer)
    val acquired = catalog.acquireBuffer(MockBufferId(5))
    assertResult(5)(acquired.id.tableId)
    assertResult(buffer)(acquired)
    verify(buffer).addReference()
  }

  test("acquire buffer retries automatically") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, acquireAttempts = 9)
    catalog.registerNewBuffer(buffer)
    val acquired = catalog.acquireBuffer(MockBufferId(5))
    assertResult(5)(acquired.id.tableId)
    assertResult(buffer)(acquired)
    verify(buffer, times(9)).addReference()
  }

  test("acquire buffer at specific tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer2)
    val acquired = catalog.acquireBuffer(MockBufferId(5), HOST).get
    assertResult(5)(acquired.id.tableId)
    assertResult(buffer2)(acquired)
    verify(buffer2).addReference()
  }

  test("acquire buffer at nonexistent tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer)
    assert(catalog.acquireBuffer(MockBufferId(5), DEVICE).isEmpty)
    assert(catalog.acquireBuffer(MockBufferId(5), DISK).isEmpty)
  }

  test("get buffer meta") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val expectedMeta = new TableMeta
    val buffer = mockBuffer(bufferId, meta = expectedMeta)
    catalog.registerNewBuffer(buffer)
    val meta = catalog.getBufferMeta(bufferId)
    assertResult(expectedMeta)(meta)
  }

  test("buffer is spilled to slower tier only") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer2)
    val buffer3 = mockBuffer(bufferId, tier = DISK)
    catalog.registerNewBuffer(buffer3)
    assert(catalog.isBufferSpilled(bufferId, DEVICE))
    assert(catalog.isBufferSpilled(bufferId, HOST))
    assert(!catalog.isBufferSpilled(bufferId, DISK))
  }

  test("remove buffer tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer2)
    val buffer3 = mockBuffer(bufferId, tier = DISK)
    catalog.registerNewBuffer(buffer3)
    catalog.removeBufferTier(bufferId, DEVICE)
    catalog.removeBufferTier(bufferId, DISK)
    assert(catalog.acquireBuffer(MockBufferId(5), DEVICE).isEmpty)
    assert(catalog.acquireBuffer(MockBufferId(5), HOST).isDefined)
    assert(catalog.acquireBuffer(MockBufferId(5), DISK).isEmpty)
  }

  test("remove nonexistent buffer tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    catalog.removeBufferTier(bufferId, HOST)
    catalog.removeBufferTier(bufferId, DISK)
    assert(catalog.acquireBuffer(MockBufferId(5), DEVICE).isDefined)
    assert(catalog.acquireBuffer(MockBufferId(5), HOST).isEmpty)
    assert(catalog.acquireBuffer(MockBufferId(5), DISK).isEmpty)
  }

  test("remove buffer releases buffer resources") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId)
    catalog.registerNewBuffer(buffer)
    catalog.removeBuffer(bufferId)
    verify(buffer).free()
  }

  test("remove buffer releases buffer resources at all tiers") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId, tier = DEVICE)
    catalog.registerNewBuffer(buffer)
    val buffer2 = mockBuffer(bufferId, tier = HOST)
    catalog.registerNewBuffer(buffer2)
    val buffer3 = mockBuffer(bufferId, tier = DISK)
    catalog.registerNewBuffer(buffer3)
    catalog.removeBuffer(bufferId)
    verify(buffer).free()
    verify(buffer2).free()
    verify(buffer3).free()
  }

  private def mockBuffer(
      bufferId: RapidsBufferId,
      meta: TableMeta = null,
      tier: StorageTier = StorageTier.DEVICE,
      acquireAttempts: Int = 1): RapidsBuffer = {
    val buffer = mock[RapidsBuffer]
    when(buffer.id).thenReturn(bufferId)
    when(buffer.storageTier).thenReturn(tier)
    when(buffer.meta).thenReturn(meta)
    var stub = when(buffer.addReference())
    (0 until acquireAttempts - 1).foreach(_ => stub = stub.thenReturn(false))
    stub.thenReturn(true)
    buffer
  }
}

case class MockBufferId(override val tableId: Int) extends RapidsBufferId {
  override def getDiskPath(dbm: RapidsDiskBlockManager): File =
    throw new UnsupportedOperationException
}
