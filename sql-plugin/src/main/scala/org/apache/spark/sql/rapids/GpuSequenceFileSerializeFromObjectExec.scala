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

import java.io.FileNotFoundException
import java.io.IOException
import java.util

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.sequencefile.GpuSequenceFileMultiFilePartitionReaderFactory
import com.nvidia.spark.rapids.shims.{GpuDataSourceRDD, PartitionedFileUtilsShim}
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException

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
/**
 * @param sequenceFileSchema schema with effective column names (after any parent Project
 *                           rename) used to determine which SequenceFile columns to read.
 *                           The field names "key"/"value" map to the corresponding
 *                           SequenceFile key/value columns. This may differ from outputAttrs
 *                           names when a parent ProjectExec renames columns (e.g., encoder
 *                           default "value" renamed to "key" by .toDF("key")).
 */
case class GpuSequenceFileSerializeFromObjectExec(
    outputAttrs: Seq[Attribute],
    child: SparkPlan,
    goal: CoalesceSizeGoal,
    inputPaths: Seq[String],
    sequenceFileSchema: StructType)(
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

  // Use sequenceFileSchema (effective column names) to determine which SequenceFile
  // columns (key/value) the reader should produce. This may differ from outputAttrs
  // when a parent ProjectExec renames columns.
  private lazy val readDataSchema: StructType = sequenceFileSchema

  /**
   * List all input files and bin-pack them into FilePartitions.
   * Evaluated lazily on the driver.
   */
  @transient private lazy val filePartitions: Seq[FilePartition] = {
    val session = SparkSession.active
    val hadoopConf = session.sessionState.newHadoopConf()
    val ignoreMissingFiles = session.sessionState.conf.ignoreMissingFiles

    val allFiles = new ArrayBuffer[FileStatus]()
    val hiddenFilter: PathFilter = (p: Path) =>
      !p.getName.startsWith("_") && !p.getName.startsWith(".")
    inputPaths.foreach { pathStr =>
      val path = new Path(pathStr)
      val fs = path.getFileSystem(hadoopConf)
      val statuses = Option(
        GpuSequenceFileSerializeFromObjectExec.resolveInputStatuses(
          path, hadoopConf, ignoreMissingFiles))
        .getOrElse(Array.empty[FileStatus])
      statuses.foreach { s =>
        if (s.isFile && hiddenFilter.accept(s.getPath)) {
          allFiles += s
        } else if (!s.isFile) {
          val iter = fs.listFiles(s.getPath, true)
          while (iter.hasNext) {
            val child = iter.next()
            if (hiddenFilter.accept(child.getPath)) {
              allFiles += child
            }
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
    val outSchema = StructType(localOutput.map(a =>
      StructField(a.name, a.dataType, a.nullable)))
    child.execute().mapPartitionsWithIndexInternal { (index, it) =>
      val unsafeProj = UnsafeProjection.create(outSchema)
      unsafeProj.initialize(index)
      it.map { row =>
        val obj = row.get(0, childObjType)
        val outRow = GpuSequenceFileSerializeFromObjectExec.projectObjectToOutputRow(
          obj, localOutput)
        unsafeProj(outRow).copy()
      }
    }
  }

  // ---- GPU columnar path (multi-threaded reader + combine) ---------------

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
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
          numOutputBatches += 1
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

object GpuSequenceFileSerializeFromObjectExec {
  private def noMatchesError(path: Path): InvalidInputException = {
    new InvalidInputException(util.Arrays.asList(
      new IOException(s"Input Pattern $path matches 0 files")))
  }

  def resolveInputStatuses(
      path: Path,
      hadoopConf: org.apache.hadoop.conf.Configuration,
      ignoreMissingFiles: Boolean = false): Array[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    val statuses = fs.globStatus(path)
    if (statuses == null || statuses.isEmpty) {
      if (ignoreMissingFiles) {
        return Array.empty[FileStatus]
      }
      val pathStr = path.toString
      val looksLikeGlob = pathStr.exists(
        ch => ch == '*' || ch == '?' || ch == '[' || ch == '{')
      if (looksLikeGlob) {
        throw noMatchesError(path)
      } else {
        throw new FileNotFoundException(
          s"Input path does not exist: $path")
      }
    }
    statuses
  }

  private def sequenceFileFieldBytes(obj: Any, fieldName: String): Array[Byte] = {
    obj match {
      case bytes: Array[Byte] =>
        bytes
      case tuple: Product =>
        if (fieldName.equalsIgnoreCase("key")) {
          tuple.productElement(0).asInstanceOf[Array[Byte]]
        } else {
          tuple.productElement(1).asInstanceOf[Array[Byte]]
        }
      case other =>
        throw new IllegalStateException(
          s"Unexpected SequenceFile object type: ${other.getClass.getName}")
    }
  }

  private[rapids] def projectObjectToOutputRow(
      obj: Any,
      outputAttrs: Seq[Attribute]): GenericInternalRow = {
    val outRow = new GenericInternalRow(outputAttrs.length)
    outputAttrs.zipWithIndex.foreach { case (attr, idx) =>
      val bytes =
        if (attr.name.equalsIgnoreCase("key") || attr.name.equalsIgnoreCase("value")) {
          sequenceFileFieldBytes(obj, attr.name)
        } else {
          null
        }
      outRow.update(idx, bytes)
    }
    outRow
  }
}
