/*
 * Copyright (c) 2023-2024, NVIDIA CORPORATION.
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
{"spark": "332db"}
{"spark": "340"}
{"spark": "341"}
{"spark": "341db"}
{"spark": "342"}
{"spark": "343"}
{"spark": "350"}
{"spark": "351"}
{"spark": "400"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.execution.datasources

import java.util.Date

import com.nvidia.spark.rapids.{DataFromReplacementRule, GpuExec, RapidsConf, RapidsMeta, SparkPlanMeta}
import com.nvidia.spark.rapids.shims.ShimUnaryExecNode

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.{GpuFileFormatWriter, GpuWriteJobDescription}
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * The write files spec holds all information of [[V1WriteCommand]] if its provider is
 * [[FileFormat]].
 */
case class GpuWriteFilesSpec(
    description: GpuWriteJobDescription,
    committer: FileCommitProtocol,
    concurrentOutputWriterSpecFunc: SparkPlan => Option[GpuConcurrentOutputWriterSpec])

class GpuWriteFilesMeta(
    writeFilesExec: WriteFilesExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends SparkPlanMeta[WriteFilesExec](writeFilesExec, conf, parent, rule) {

  override def convertToGpu(): GpuExec = {
    // WriteFilesExec only has one child
    val child = childPlans.head.convertIfNeeded()
    GpuWriteFilesExec(
      child,
      writeFilesExec.fileFormat,
      writeFilesExec.partitionColumns,
      writeFilesExec.bucketSpec,
      writeFilesExec.options,
      writeFilesExec.staticPartitions
    )
  }
}

/**
 * Responsible for writing files.
 */
case class GpuWriteFilesExec(
    child: SparkPlan,
    fileFormat: FileFormat,
    partitionColumns: Seq[Attribute],
    bucketSpec: Option[BucketSpec],
    options: Map[String, String],
    staticPartitions: TablePartitionSpec) extends ShimUnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = Seq.empty

  /**
   * Cpu version for SparkPlan.executeWrite, just throw an exception
   */
  override protected def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] =
    throw new UnsupportedOperationException(
      s"${getClass.getCanonicalName} does not support row-based execution")

  /**
   * Gpu version for SparkPlan.executeWrite
   */
  def executeColumnarWrite(
      writeFilesSpec: GpuWriteFilesSpec): RDD[WriterCommitMessage] = executeQuery {
    // Copied from SparkPlan.executeWrite
    if (isCanonicalizedPlan) {
      throw SparkException.internalError("A canonicalized plan is not supposed to be executed.")
    }
    doExecuteColumnarWrite(writeFilesSpec)
  }

  /**
   * Gpu version for SparkPlan.doExecuteWrite
   *
   * @param writeFilesSpec
   * @return
   */
  private def doExecuteColumnarWrite(
      writeFilesSpec: GpuWriteFilesSpec): RDD[WriterCommitMessage] = {
    val rdd = child.executeColumnar()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      session.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
    } else {
      rdd
    }

    val concurrentOutputWriterSpec = writeFilesSpec.concurrentOutputWriterSpecFunc(child)
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    val jobTrackerID = SparkHadoopWriterUtils.createJobTrackerID(new Date())
    rddWithNonEmptyPartitions.mapPartitionsInternal { iterator =>
      val sparkStageId = TaskContext.get().stageId()
      val sparkPartitionId = TaskContext.get().partitionId()
      val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue
      val ret = GpuFileFormatWriter.executeTask(
        description,
        jobTrackerID,
        sparkStageId,
        sparkPartitionId,
        sparkAttemptNumber,
        committer,
        iterator,
        concurrentOutputWriterSpec
      )

      Iterator(ret)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw SparkException.internalError(s"$nodeName does not support doExecute")
  }

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Internal Error ${this.getClass} has column support" +
        s" mismatch:\n$this")
  }

  override def stringArgs: Iterator[Any] = Iterator(child)
}

object GpuWriteFiles {

  def createConcurrentOutputWriterSpec(
      sparkSession: SparkSession,
      sortColumns: Seq[Attribute],
      output: Seq[Attribute],
      batchSize: Long,
      sortOrder: Seq[SortOrder]): Option[GpuConcurrentOutputWriterSpec] = {
    val maxWriters = sparkSession.sessionState.conf.maxConcurrentOutputFileWriters
    val concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty
    if (concurrentWritersEnabled) {
      Some(GpuConcurrentOutputWriterSpec(maxWriters, output, batchSize, sortOrder))
    } else {
      None
    }
  }

  /**
   * Find the first `GpuWriteFilesExec`
   */
  def getWriteFilesOpt(p: SparkPlan): Option[GpuWriteFilesExec] = {
    p.collectFirst {
      case w: GpuWriteFilesExec => w
    }
  }
}
