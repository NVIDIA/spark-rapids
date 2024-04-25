/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "332db"}
{"spark": "333"}
{"spark": "334"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.shims

import org.apache.spark.SparkConf
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter}
import org.apache.spark.sql.rapids.{RapidsShuffleThreadedWriterBase, ShuffleHandleWithMetrics}
import org.apache.spark.storage.{BlockManager, DiskBlockObjectWriter}

object RapidsShuffleThreadedWriter {
  // this is originally defined in ShuffleChecksumHelper in Apache Spark
  // copying here as it seemed relatively small.
  val EMPTY_CHECKSUM_VALUE = new Array[Long](0)
}

class RapidsShuffleThreadedWriter[K, V](
    blockManager: BlockManager,
    handle: ShuffleHandleWithMetrics[K, V, V],
    mapId: Long,
    sparkConf: SparkConf,
    writeMetrics: ShuffleWriteMetricsReporter,
    maxBytesInFlight: Long,
    shuffleExecutorComponents: ShuffleExecutorComponents,
    numWriterThreads: Int)
  extends RapidsShuffleThreadedWriterBase[K, V](
    blockManager,
    handle,
    mapId,
    sparkConf,
    writeMetrics,
    maxBytesInFlight,
    shuffleExecutorComponents,
    numWriterThreads)
      with org.apache.spark.shuffle.checksum.ShuffleChecksumSupport {

  // Spark 3.2.0+ computes checksums per map partition as it writes the
  // temporary files to disk. They are stored in a Checksum array.
  private val checksums =
    createPartitionChecksums(handle.dependency.partitioner.numPartitions, sparkConf)

  override def setChecksumIfNeeded(writer: DiskBlockObjectWriter, partition: Int): Unit = {
    if (checksums.length > 0) {
      writer.setChecksum(checksums(partition))
    }
  }

  override def doCommitAllPartitions(
      writer: ShuffleMapOutputWriter, emptyChecksums: Boolean): Array[Long] = {
    if (emptyChecksums) {
      writer.commitAllPartitions(RapidsShuffleThreadedWriter.EMPTY_CHECKSUM_VALUE)
        .getPartitionLengths
    } else {
      writer.commitAllPartitions(getChecksumValues(checksums))
        .getPartitionLengths
    }
  }
}

