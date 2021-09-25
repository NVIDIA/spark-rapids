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

import org.apache.spark.SparkConf
import org.apache.spark.storage.{BlockId, DiskBlockManager, TempLocalBlockId}

/** Maps logical blocks to local disk locations. */
class RapidsDiskBlockManager(conf: SparkConf) {
  private[this] val blockManager = new DiskBlockManager(conf, true)

  def getFile(blockId: BlockId): File = blockManager.getFile(blockId)

  def getFile(file: String): File = blockManager.getFile(file)
}
