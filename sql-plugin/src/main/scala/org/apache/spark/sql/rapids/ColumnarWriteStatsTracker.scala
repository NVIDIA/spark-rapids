/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

import org.apache.spark.sql.execution.datasources.WriteTaskStats
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A trait for classes that are capable of collecting statistics on columnar data that's being
 * processed by a single write task in [[GpuFileFormatDataWriter]] - i.e. there should be one
 * instance per executor.
 *
 * This trait is coupled with the way [[GpuFileFormatWriter]] works, in the sense that its methods
 * will be called according to how column batches are being written out to disk, namely in
 * sorted order according to partitionValue(s), then bucketId.
 *
 * As such, a typical call scenario is:
 *
 * newPartition -> newBucket -> newFile -> newRow -.
 *    ^        |______^___________^ ^         ^____|
 *    |               |             |______________|
 *    |               |____________________________|
 *    |____________________________________________|
 *
 * newPartition and newBucket events are only triggered if the relation to be written out is
 * partitioned and/or bucketed, respectively.
 */
trait ColumnarWriteTaskStatsTracker {

  /**
   * Process the fact that a new partition is about to be written.
   * Only triggered when the relation is partitioned by a (non-empty) sequence of columns.
   * NOTE: The partition values are stubbed for now as the original code only updated a
   *       count of partitions without examining the values.
   * //@param partitionValues The values that define this new partition.
   */
  def newPartition(/*partitionValues: InternalRow*/): Unit

  /**
   * Process the fact that a new bucket is about to written.
   * Only triggered when the relation is bucketed by a (non-empty) sequence of columns.
   * @param bucketId The bucket number.
   */
  def newBucket(bucketId: Int): Unit

  /**
   * Process the fact that a new file is about to be written.
   * @param filePath Path of the file into which future rows will be written.
   */
  def newFile(filePath: String): Unit

  /**
   * Process a new column batch to update the tracked statistics accordingly.
   * The batch will be written to the most recently witnessed file (via `newFile`).
   * @param batch Current data batch to be processed.
   */
  def newBatch(batch: ColumnarBatch): Unit

  /**
   * Returns the final statistics computed so far.
   * @note This may only be called once. Further use of the object may lead to undefined behavior.
   * @return An object of subtype of `org.apache.spark.sql.execution.datasources.WriteTaskStats`,
   *         to be sent to the driver.
   */
  def getFinalStats(): WriteTaskStats
}


/**
 * A class implementing this trait is basically a collection of parameters that are necessary
 * for instantiating a (derived type of) [[ColumnarWriteTaskStatsTracker]] on all executors and then
 * process the statistics produced by them (e.g. save them to memory/disk, issue warnings, etc).
 * It is therefore important that such an objects is `Serializable`, as it will be sent
 * from the driver to all executors.
 */
trait ColumnarWriteJobStatsTracker extends Serializable {

  /**
   * Instantiates a [[ColumnarWriteTaskStatsTracker]], based on (non-transient) members of this
   * class.
   * To be called by executors.
   * @return A [[ColumnarWriteTaskStatsTracker]] instance to be used for computing stats
   *         during a write task
   */
  def newTaskInstance(): ColumnarWriteTaskStatsTracker

  /**
   * Process the given collection of stats computed during this job.
   * E.g. aggregate them, write them to memory / disk, issue warnings, whatever.
   * @param stats One `WriteTaskStats` object from each successful write task.
   */
  def processStats(stats: Seq[WriteTaskStats]): Unit
}
