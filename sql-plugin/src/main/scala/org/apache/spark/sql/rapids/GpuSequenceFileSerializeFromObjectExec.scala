/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.sequencefile.GpuSequenceFileMultiFilePartitionReaderFactory
import com.nvidia.spark.rapids.shims.{GpuDataSourceRDD, PartitionedFileUtilsShim}
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow,
  SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning,
  UnknownPartitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * GPU replacement for SerializeFromObjectExec over SequenceFile object scans.
 *
 * The GPU columnar path bypasses the child RDD entirely and reads SequenceFiles
 * directly using the multi-threaded reader with combine mode for optimal batch
 * sizes and GPU utilization.  This restores the same I/O path that the old
 * logical-plan conversion used via GpuFileSourceScanExec.
 *
 * The CPU fallback path (doExecute) still uses the original child RDD.
 */
case class GpuSequenceFileSerializeFromObjectExec(
    outputAttrs: Seq[Attribute],
    child: SparkPlan,
    goal: CoalesceSizeGoal,
    inputPaths: Seq[String])(
    @transient val rapidsConf: RapidsConf)
  extends UnaryExecNode with GpuExec {

  override def output: Seq[Attribute] = outputAttrs
  override def outputPartitioning: Partitioning = UnknownPartitioning(0)
  override def outputOrdering: Seq[SortOrder] = Nil
  override def outputBatching: CoalesceGoal = goal
  override def otherCopyArgs: Seq[AnyRef] = Seq(rapidsConf)

  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    OP_TIME_NEW ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME_NEW),
    NUM_OUTPUT_ROWS ->
      createMetric(ESSENTIAL_LEVEL, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES ->
      createMetric(MODERATE_LEVEL, DESCRIPTION_NUM_OUTPUT_BATCHES),
    GPU_DECODE_TIME ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_GPU_DECODE_TIME),
    BUFFER_TIME ->
      createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUFFER_TIME),
    SCAN_TIME ->
      createNanoTimingMetric(ESSENTIAL_LEVEL, DESCRIPTION_SCAN_TIME),
    SCHEDULE_TIME ->
      createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_SCHEDULE_TIME),
    BUFFER_TIME_BUBBLE ->
      createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_BUFFER_TIME_BUBBLE),
    SCHEDULE_TIME_BUBBLE ->
      createNanoTimingMetric(DEBUG_LEVEL,
        DESCRIPTION_SCHEDULE_TIME_BUBBLE)
  )

  private lazy val readDataSchema: StructType = StructType(
    outputAttrs.map(a => StructField(a.name, a.dataType, a.nullable)))

  /**
   * List all input files and bin-pack them into FilePartitions.
   * Evaluated lazily on the driver.
   */
  @transient private lazy val filePartitions: Seq[FilePartition] = {
    val session = SparkSession.active
    val hadoopConf = session.sessionState.newHadoopConf()

    val allFiles = new ArrayBuffer[FileStatus]()
    inputPaths.foreach { pathStr =>
      val path = new Path(pathStr)
      val fs = path.getFileSystem(hadoopConf)
      val statuses = fs.globStatus(path)
      if (statuses != null) {
        statuses.foreach { s =>
          if (s.isFile) {
            allFiles += s
          } else {
            val iter = fs.listFiles(s.getPath, true)
            while (iter.hasNext) allFiles += iter.next()
          }
        }
      }
    }

    // One PartitionedFile per file (no splitting; uncompressed
    // SequenceFiles are handled as whole files by the reader).
    // Use shim factory to handle String vs SparkPath across versions.
    val splitFiles = allFiles.map { f =>
      PartitionedFileUtilsShim.newPartitionedFile(
        InternalRow.empty, f.getPath.toUri.toString, 0, f.getLen)
    }.sortBy(_.length)(implicitly[Ordering[Long]].reverse)

    val maxSplitBytes =
      session.sessionState.conf.filesMaxPartitionBytes
    FilePartition.getFilePartitions(
      session, splitFiles.toSeq, maxSplitBytes)
  }

  /**
   * Multi-threaded reader factory with combine mode support.
   * Evaluated lazily on the driver.
   */
  @transient private lazy val readerFactory = {
    val session = SparkSession.active
    val hadoopConf = session.sessionState.newHadoopConf()
    val broadcastedConf = session.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    GpuSequenceFileMultiFilePartitionReaderFactory(
      session.sessionState.conf,
      broadcastedConf,
      readDataSchema,
      new StructType(), // no partition schema
      rapidsConf,
      allMetrics,
      queryUsesInputFile = false)
  }

  // ---- CPU fallback (uses the original child RDD) -----------------------

  override def doExecute(): RDD[InternalRow] = {
    val localOutput = output
    val childObjType = child.output.head.dataType
    val numOutCols = localOutput.length
    val outSchema = StructType(localOutput.map(a =>
      StructField(a.name, a.dataType, a.nullable)))
    child.execute().mapPartitionsWithIndexInternal { (index, it) =>
      val unsafeProj = UnsafeProjection.create(outSchema)
      unsafeProj.initialize(index)
      it.map { row =>
        val obj = row.get(0, childObjType)
        val outRow = new GenericInternalRow(numOutCols)
        if (numOutCols == 1) {
          outRow.update(0, obj.asInstanceOf[Array[Byte]])
        } else {
          val tuple = obj.asInstanceOf[Product]
          outRow.update(0,
            tuple.productElement(0).asInstanceOf[Array[Byte]])
          outRow.update(1,
            tuple.productElement(1).asInstanceOf[Array[Byte]])
        }
        unsafeProj(outRow).copy()
      }
    }
  }

  // ---- GPU columnar path (multi-threaded reader + combine) ---------------

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val scanTime = gpuLongMetric(SCAN_TIME)
    GpuDataSourceRDD(
      SparkSession.active.sparkContext, filePartitions, readerFactory
    ).asInstanceOf[RDD[ColumnarBatch]].mapPartitionsInternal { batches =>
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = scanTime.ns {
          batches.hasNext
        }
        override def next(): ColumnarBatch = {
          val batch = batches.next()
          numOutputRows += batch.numRows()
          batch
        }
      }
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)(rapidsConf)
  }
}
