/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.rapids.{ColumnarWriteTaskStatsTracker, GpuWriteTaskStatsTracker}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A factory that produces [[ColumnarOutputWriter]]s.  A new [[ColumnarOutputWriterFactory]] is
 * created on the driver side, and then gets serialized to executor side to create
 * [[ColumnarOutputWriter]]s. This is the columnar version of
 * [[org.apache.spark.sql.execution.datasources.OutputWriterFactory]].
 */
abstract class ColumnarOutputWriterFactory extends Serializable {

  /** Returns the file extension to be used when writing files out. */
  def getFileExtension(context: TaskAttemptContext): String

  /**
   * When writing to a [[org.apache.spark.sql.execution.datasources.HadoopFsRelation]], this method
   * gets called by each task on executor side to instantiate new [[ColumnarOutputWriter]]s.
   *
   * @param path Path to write the file.
   * @param dataSchema Schema of the columnar data to be written. Partition columns are not
   *                   included in the schema if the relation being written is partitioned.
   * @param context The Hadoop MapReduce task context.
   */
  def newInstance(
      path: String,
      dataSchema: StructType,
      context: TaskAttemptContext): ColumnarOutputWriter
}

/**
 * This is used to write columnar data to a file system. Subclasses of [[ColumnarOutputWriter]]
 * must provide a zero-argument constructor. This is the columnar version of
 * [[org.apache.spark.sql.execution.datasources.OutputWriter]].
 */
abstract class ColumnarOutputWriter {
  /**
   * Persists a column batch. Invoked on the executor side. When writing to dynamically partitioned
   * tables, dynamic partition columns are not included in columns to be written.
   * NOTE: It is the writer's responsibility to close the batch.
   */
  def write(batch: ColumnarBatch, statsTrackers: Seq[ColumnarWriteTaskStatsTracker]): Unit = {
    var needToCloseBatch = true
    try {
      val writeStartTimestamp = System.nanoTime
      val writeRange = new NvtxRange("File write", NvtxColor.YELLOW)
      val gpuTime = try {
        needToCloseBatch = false
        writeBatch(batch)
      } finally {
        writeRange.close()
      }

      // Update statistics
      val writeTime = System.nanoTime - writeStartTimestamp
      statsTrackers.foreach {
        case gpuTracker: GpuWriteTaskStatsTracker =>
          gpuTracker.addWriteTime(writeTime)
          gpuTracker.addGpuTime(gpuTime)
        case _ =>
      }
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
    }
  }

  /**
   * Writes the columnar batch and returns the time in ns taken to write
   * @param batch - Columnar batch that needs to be written
   * @return - time in ns taken to write the batch
   */
  def writeBatch(batch: ColumnarBatch): Long

  /**
   * Closes the [[ColumnarOutputWriter]]. Invoked on the executor side after all columnar batches
   * are persisted, before the task output is committed.
   */
  def close(): Unit
}
