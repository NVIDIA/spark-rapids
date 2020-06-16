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

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.datasources.{BasicWriteTaskStats, WriteTaskStats}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * Simple metrics collected during an instance of [[GpuFileFormatDataWriter]].
 * This is the columnar version of
 * `org.apache.spark.sql.execution.datasources.BasicWriteTaskStatsTracker`.
 */
class BasicColumnarWriteTaskStatsTracker(hadoopConf: Configuration)
    extends ColumnarWriteTaskStatsTracker with Logging {
  private[this] var numPartitions: Int = 0
  private[this] var numFiles: Int = 0
  private[this] var submittedFiles: Int = 0
  private[this] var numBytes: Long = 0L
  private[this] var numRows: Long = 0L

  private[this] var curFile: Option[String] = None

  /**
   * Get the size of the file expected to have been written by a worker.
   * @param filePath path to the file
   * @return the file size or None if the file was not found.
   */
  private def getFileSize(filePath: String): Option[Long] = {
    val path = new Path(filePath)
    val fs = path.getFileSystem(hadoopConf)
    try {
      Some(fs.getFileStatus(path).getLen())
    } catch {
      case e: FileNotFoundException =>
        // may arise against eventually consistent object stores
        logDebug(s"File $path is not yet visible", e)
        None
    }
  }

  override def newPartition(/*partitionValues: InternalRow*/): Unit = {
    numPartitions += 1
  }

  override def newBucket(bucketId: Int): Unit = {
    // currently unhandled
  }

  override def newFile(filePath: String): Unit = {
    statCurrentFile()
    curFile = Some(filePath)
    submittedFiles += 1
  }

  private def statCurrentFile(): Unit = {
    curFile.foreach { path =>
      getFileSize(path).foreach { len =>
        numBytes += len
        numFiles += 1
      }
      curFile = None
    }
  }

  override def newBatch(batch: ColumnarBatch): Unit = {
    numRows += batch.numRows
  }

  override def getFinalStats(): WriteTaskStats = {
    statCurrentFile()

    // Reports bytesWritten and recordsWritten to the Spark output metrics.
    Option(TaskContext.get()).map(_.taskMetrics().outputMetrics).foreach { outputMetrics =>
      outputMetrics.setBytesWritten(numBytes)
      outputMetrics.setRecordsWritten(numRows)
    }

    if (submittedFiles != numFiles) {
      logInfo(s"Expected $submittedFiles files, but only saw $numFiles. " +
        "This could be due to the output format not writing empty files, " +
        "or files being not immediately visible in the filesystem.")
    }
    BasicWriteTaskStats(numPartitions, numFiles, numBytes, numRows)
  }
}


/**
 * Simple [[ColumnarWriteJobStatsTracker]] implementation that's serializable,
 * capable ofinstantiating [[BasicColumnarWriteTaskStatsTracker]] on executors and processing the
 * `BasicWriteTaskStats` they produce by aggregating the metrics and posting them
 * as DriverMetricUpdates.
 */
class BasicColumnarWriteJobStatsTracker(
    serializableHadoopConf: SerializableConfiguration,
    @transient val metrics: Map[String, SQLMetric])
  extends ColumnarWriteJobStatsTracker {

  override def newTaskInstance(): ColumnarWriteTaskStatsTracker = {
    new BasicColumnarWriteTaskStatsTracker(serializableHadoopConf.value)
  }

  override def processStats(stats: Seq[WriteTaskStats]): Unit = {
    val sparkContext = SparkContext.getActive.get
    var numPartitions: Long = 0L
    var numFiles: Long = 0L
    var totalNumBytes: Long = 0L
    var totalNumOutput: Long = 0L

    val basicStats = stats.map(_.asInstanceOf[BasicWriteTaskStats])

    basicStats.foreach { summary =>
      numPartitions += summary.numPartitions
      numFiles += summary.numFiles
      totalNumBytes += summary.numBytes
      totalNumOutput += summary.numRows
    }

    metrics(BasicColumnarWriteJobStatsTracker.NUM_FILES_KEY).add(numFiles)
    metrics(BasicColumnarWriteJobStatsTracker.NUM_OUTPUT_BYTES_KEY).add(totalNumBytes)
    metrics(BasicColumnarWriteJobStatsTracker.NUM_OUTPUT_ROWS_KEY).add(totalNumOutput)
    metrics(BasicColumnarWriteJobStatsTracker.NUM_PARTS_KEY).add(numPartitions)

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toList)
  }
}

object BasicColumnarWriteJobStatsTracker {
  private val NUM_FILES_KEY = "numFiles"
  private val NUM_OUTPUT_BYTES_KEY = "numOutputBytes"
  private val NUM_OUTPUT_ROWS_KEY = "numOutputRows"
  private val NUM_PARTS_KEY = "numParts"

  def metrics: Map[String, SQLMetric] = {
    val sparkContext = SparkContext.getActive.get
    Map(
      NUM_FILES_KEY -> SQLMetrics.createMetric(sparkContext, "number of written files"),
      NUM_OUTPUT_BYTES_KEY -> SQLMetrics.createSizeMetric(sparkContext, "written output"),
      NUM_OUTPUT_ROWS_KEY -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
      NUM_PARTS_KEY -> SQLMetrics.createMetric(sparkContext, "number of dynamic part")
    )
  }
}
