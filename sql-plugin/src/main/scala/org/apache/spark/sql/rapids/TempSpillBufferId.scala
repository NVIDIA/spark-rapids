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

package org.apache.spark.sql.rapids

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.IntUnaryOperator

import com.nvidia.spark.rapids.RapidsBufferId

import org.apache.spark.storage.TempLocalBlockId

object TempSpillBufferId {
  private val MAX_TABLE_ID = Integer.MAX_VALUE
  private val TABLE_ID_UPDATER = new IntUnaryOperator {
    override def applyAsInt(i: Int): Int = if (i < MAX_TABLE_ID) i + 1 else 0
  }

  /** Tracks the next table identifier */
  private[this] val tableIdCounter = new AtomicInteger(0)

  def apply(): TempSpillBufferId = {
    val tableId = tableIdCounter.getAndUpdate(TABLE_ID_UPDATER)
    val tempBlockId = TempLocalBlockId(UUID.randomUUID())
    new TempSpillBufferId(tableId, tempBlockId)
  }
}

case class TempSpillBufferId private(
    override val tableId: Int,
    bufferId: TempLocalBlockId) extends RapidsBufferId {

  override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
    diskBlockManager.getFile(bufferId)
}