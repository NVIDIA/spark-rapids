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

import com.nvidia.spark.rapids.StorageTier.StorageTier
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
    assertThrows[IllegalStateException](catalog.registerNewBuffer(buffer2))
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

  test("get buffer meta") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val expectedMeta = new TableMeta
    val buffer = mockBuffer(bufferId, meta = expectedMeta)
    catalog.registerNewBuffer(buffer)
    val meta = catalog.getBufferMeta(bufferId)
    assertResult(expectedMeta)(meta)
  }

  test("update buffer map only updates for faster tier") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer1 = mockBuffer(bufferId, tier = StorageTier.HOST)
    catalog.registerNewBuffer(buffer1)
    val buffer2 = mockBuffer(bufferId, tier = StorageTier.DEVICE)
    catalog.updateBufferMap(StorageTier.HOST, buffer2)
    var resultBuffer = catalog.acquireBuffer(bufferId)
    assertResult(buffer2)(resultBuffer)
    catalog.updateBufferMap(StorageTier.HOST, buffer1)
    resultBuffer = catalog.acquireBuffer(bufferId)
    assertResult(buffer2)(resultBuffer)
  }

  test("remove buffer releases buffer resources") {
    val catalog = new RapidsBufferCatalog
    val bufferId = MockBufferId(5)
    val buffer = mockBuffer(bufferId)
    catalog.registerNewBuffer(buffer)
    catalog.removeBuffer(bufferId)
    verify(buffer).free()
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
