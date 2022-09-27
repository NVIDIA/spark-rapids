/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.sql.rapids.{RapidsShuffleThreadedWriterBase, ShuffleHandleWithMetrics}
import org.apache.spark.storage.BlockManager

class RapidsShuffleThreadedWriter[K, V](
    blockManager: BlockManager,
    handle: ShuffleHandleWithMetrics[K, V, V],
    mapId: Long,
    sparkConf: SparkConf,
    writeMetrics: ShuffleWriteMetricsReporter,
    shuffleExecutorComponents: ShuffleExecutorComponents,
    numWriterThreads: Int)
  extends RapidsShuffleThreadedWriterBase[K, V](
    blockManager,
    handle,
    mapId,
    sparkConf,
    writeMetrics,
    shuffleExecutorComponents,
    numWriterThreads) {

  // emptyChecksums: unused in versions of Spark before 3.2.0
  override def doCommitAllPartitions(
      writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long] = {
    writer.commitAllPartitions().getPartitionLengths
  }
}

