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

package org.apache.spark.sql.rapids

import java.util.UUID

import ai.rapids.cudf.{Cuda, DeviceMemoryBuffer, MemoryBuffer}
import com.nvidia.spark.rapids.{RapidsBuffer, RapidsBufferCatalog, RapidsBufferId, SpillableColumnarBatchImpl, StorageTier}
import com.nvidia.spark.rapids.StorageTier.StorageTier
import com.nvidia.spark.rapids.format.TableMeta
import org.scalatest.FunSuite

import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.storage.TempLocalBlockId

class SpillableColumnarBatchSuite extends FunSuite {

  test("close updates catalog") {
    val id = TempSpillBufferId(0, TempLocalBlockId(new UUID(1, 2)))
    val mockBuffer = new MockBuffer(id)
    val catalog = RapidsBufferCatalog.singleton
    val oldBufferCount = catalog.numBuffers
    catalog.registerNewBuffer(mockBuffer)
    val handle = catalog.makeNewHandle(id, -1)
    assertResult(oldBufferCount + 1)(catalog.numBuffers)
    val spillableBatch = new SpillableColumnarBatchImpl(
      handle,
      5,
      Array[DataType](IntegerType))
    spillableBatch.close()
    assertResult(oldBufferCount)(catalog.numBuffers)
  }

  class MockBuffer(override val id: RapidsBufferId) extends RapidsBuffer {
    override def getMemoryUsedBytes: Long = 123
    override def getMeta: TableMeta = null
    override val storageTier: StorageTier = StorageTier.DEVICE
    override def getMemoryBuffer: MemoryBuffer = null
    override def copyToMemoryBuffer(srcOffset: Long, dst: MemoryBuffer, dstOffset: Long,
        length: Long, stream: Cuda.Stream): Unit = {}
    override def getDeviceMemoryBuffer: DeviceMemoryBuffer = null
    override def addReference(): Boolean = true
    override def free(): Unit = {}
    override def getSpillPriority: Long = 0
    override def setSpillPriority(priority: Long): Unit = {}
    override def close(): Unit = {}
    override def getColumnarBatch(
      sparkTypes: Array[DataType]): ColumnarBatch = null
  }
}
