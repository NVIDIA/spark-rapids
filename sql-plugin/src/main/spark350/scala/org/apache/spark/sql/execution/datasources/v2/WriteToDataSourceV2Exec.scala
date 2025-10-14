/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "400"}
{"spark": "401"}
spark-rapids-shim-json-lines ***/

package org.apache.spark.sql.execution.datasources.v2

import com.nvidia.spark.rapids.GpuColumnarToRowExec
import com.nvidia.spark.rapids.GpuExec
import com.nvidia.spark.rapids.GpuMetric
import com.nvidia.spark.rapids.GpuWrite

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, PhysicalWriteInfoImpl, Write, WriterCommitMessage}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{CustomMetrics, SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{LongAccumulator, Utils}

trait GpuV2ExistingTableWriteExec extends GpuV2TableWriteExec {
  def refreshCache: () => Unit
  def write: Write

  override lazy val additionalMetrics: Map[String, GpuMetric] =
    write.supportedCustomMetrics().map { customMetric =>
      customMetric.name() -> GpuMetric.wrap(SQLMetrics.createV2CustomMetric(sparkContext,
        customMetric))
    }.toMap

  override protected def run(): Seq[InternalRow] = {
    val writtenRows = writeWithV2(write.toBatch)
    refreshCache()
    writtenRows
  }
}


/**
 * A trait for GPU implementations of V2 table write commands.
 * <br/>
 *
 * This class is derived from
 * [[org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec]].
 */
trait GpuV2TableWriteExec extends V2CommandExec with UnaryExecNode with GpuExec {
  def query: SparkPlan

  def writingTask: GpuWritingSparkTask[_] = GpuDataWritingSparkTask

  var commitProgress: Option[StreamWriterCommitProgress] = None

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Seq.empty

  protected def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[ColumnarBatch] = {
      val tempRdd = query.executeColumnar()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTask
    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = metrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[ColumnarBatch]) =>
          task.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        throw cause
    }

    Nil
  }
}

/**
 * Physical plan node for append into a v2 table.
 *
 * Rows in the output data set are appended.
 */
case class GpuAppendDataExec(
  inner: SparkPlan,
  refreshCache: () => Unit,
  write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = {
    inner match {
      case c2r: GpuColumnarToRowExec => c2r.child
      case _ => inner
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuAppendDataExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): GpuAppendDataExec = {
    copy(inner = newChild)
  }
}

/**
 * Physical plan node for dynamic partition overwrite into a v2 table.
 *
 * Overwrites data in a table based on partitions present in the write data.
 * Only partitions represented in the write data will be overwritten; other partitions
 * remain intact.
 */
case class GpuOverwritePartitionsDynamicExec(
  inner: SparkPlan,
  refreshCache: () => Unit,
  write: GpuWrite) extends GpuV2ExistingTableWriteExec {

  override def supportsColumnar: Boolean = false

  override def query: SparkPlan = {
    inner match {
      case c2r: GpuColumnarToRowExec => c2r.child
      case _ => inner
    }
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(
      "GpuOverwritePartitionsDynamicExec does not support columnar execution")
  }

  override protected def withNewChildInternal(newChild: SparkPlan):
  GpuOverwritePartitionsDynamicExec = {
    copy(inner = newChild)
  }
}

/**
 * This class is derived from [[org.apache.spark.sql.execution.datasources.v2.WritingSparkTask]].
 */
trait GpuWritingSparkTask[W <: DataWriter[ColumnarBatch]] extends Logging with Serializable {

  protected def write(writer: W, row: ColumnarBatch): Unit

  def run(
    writerFactory: DataWriterFactory,
    context: TaskContext,
    iter: Iterator[ColumnarBatch],
    useCommitCoordinator: Boolean,
    customMetrics: Map[String, SQLMetric]): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        // Count is here.
        val batch = iter.next()
        val numRows = batch.numRows
        write(dataWriter, batch)

        count += numRows
        CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)
      }

      CustomMetrics.updateMetrics(dataWriter.currentMetricsValues, customMetrics)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(commitDeniedException.getMessage)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

object GpuDataWritingSparkTask extends GpuWritingSparkTask[DataWriter[ColumnarBatch]] {
  override protected def write(writer: DataWriter[ColumnarBatch], row: ColumnarBatch): Unit = {
    writer.write(row)
  }
}