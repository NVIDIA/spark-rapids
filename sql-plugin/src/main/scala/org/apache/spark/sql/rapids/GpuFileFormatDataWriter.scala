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

import scala.collection.mutable

import ai.rapids.spark.{ColumnarOutputWriter, ColumnarOutputWriterFactory}
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, WriteTaskResult}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Abstract class for writing out data in a single Spark task using the GPU.
 * This is the GPU version of [[org.apache.spark.sql.execution.datasources.FileFormatDataWriter]].
 */
abstract class GpuFileFormatDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol) extends DataWriter[ColumnarBatch] {
  /**
   * Max number of files a single task writes out due to file size. In most cases the number of
   * files written should be very small. This is just a safe guard to protect some really bad
   * settings, e.g. maxRecordsPerFile = 1.
   */
  protected val MAX_FILE_COUNTER: Int = 1000 * 1000
  protected val updatedPartitions: mutable.Set[String] = mutable.Set[String]()
  protected var currentWriter: ColumnarOutputWriter = _

  /** Trackers for computing various statistics on the data as it's being written out. */
  protected val statsTrackers: Seq[ColumnarWriteTaskStatsTracker] =
    description.statsTrackers.map(_.newTaskInstance())

  protected def releaseResources(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
      } finally {
        currentWriter = null
      }
    }
  }

  /** Writes a columnar batch of records */
  def write(batch: ColumnarBatch): Unit

  /**
   * Returns the summary of relative information which
   * includes the list of partition strings written out. The list of partitions is sent back
   * to the driver and used to update the catalog. Other information will be sent back to the
   * driver too and used to e.g. update the metrics in UI.
   */
  override def commit(): WriteTaskResult = {
    releaseResources()
    val summary = ExecutedWriteSummary(
      updatedPartitions = updatedPartitions.toSet,
      stats = statsTrackers.map(_.getFinalStats()))
    WriteTaskResult(committer.commitTask(taskAttemptContext), summary)
  }

  override def abort(): Unit = {
    try {
      releaseResources()
    } finally {
      committer.abortTask(taskAttemptContext)
    }
  }

  override def close(): Unit = {}
}

/** GPU data writer for empty partitions */
class GpuEmptyDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol
) extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  override def write(batch: ColumnarBatch): Unit = {
    batch.close()
  }
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class GpuSingleDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  // Initialize currentWriter and statsTrackers
  newOutputWriter()

  private def newOutputWriter(): Unit = {
    recordsInFile = 0
    releaseResources()

    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = committer.newTaskTempFile(
      taskAttemptContext,
      None,
      f"-c$fileCounter%03d" + ext)

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  override def write(batch: ColumnarBatch): Unit = {
    var needToCloseBatch = true
    try {
      // TODO: This does not handle batch splitting to make sure maxRecordsPerFile is not exceeded.
      if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
        fileCounter += 1
        assert(fileCounter < MAX_FILE_COUNTER,
          s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

        newOutputWriter()
      }

      statsTrackers.foreach(_.newBatch(batch))
      recordsInFile += batch.numRows
      needToCloseBatch = false
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
    }

    // It is the responsibility of the writer to close the batch.
    currentWriter.write(batch, statsTrackers)
  }
}

/**
 * A shared job description for all the GPU write tasks.
 * This is the GPU version of [[org.apache.spark.sql.execution.datasources.WriteJobDescription]].
 */
class GpuWriteJobDescription(
    val uuid: String, // prevent collision between different (appending) write jobs
    val serializableHadoopConf: SerializableConfiguration,
    val outputWriterFactory: ColumnarOutputWriterFactory,
    val allColumns: Seq[Attribute],
    val dataColumns: Seq[Attribute],
    val partitionColumns: Seq[Attribute],
    val bucketIdExpression: Option[Expression],
    val path: String,
    val customPartitionLocations: Map[TablePartitionSpec, String],
    val maxRecordsPerFile: Long,
    val timeZoneId: String,
    val statsTrackers: Seq[ColumnarWriteJobStatsTracker])
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}
