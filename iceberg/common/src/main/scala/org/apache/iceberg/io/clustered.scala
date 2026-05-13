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

package org.apache.iceberg.io

import java.io.{IOException, UncheckedIOException}
import java.util.{Comparator, HashSet => JHashSet, Set => JSet}

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, DeleteFile, PartitionSpec, StructLike}
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory
import org.apache.iceberg.types.Comparators
import org.apache.iceberg.util.{CharSequenceSet, StructLikeSet, StructLikeUtil}

abstract class GpuClusteredWriter[T, R] extends PartitioningWriter[T, R] {
  private val completedSpecIds = new JHashSet[Integer]()
  private var currentSpec: PartitionSpec = _
  private var partitionComparator: Comparator[StructLike] = _
  private var completedPartitions: JSet[StructLike] = _
  private var currentPartition: StructLike = _
  private var currentWriter: FileWriter[T, R] = _
  private var closed = false

  protected def newWriter(partitionSpec: PartitionSpec, partition: StructLike): FileWriter[T, R]
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  override def write(row: T, spec: PartitionSpec, partition: StructLike): Unit = {
    require(!closed, "Cannot write to a closed writer")

    if (currentSpec == null || !spec.equals(currentSpec)) {
      if (currentSpec != null) {
        closeCurrentWriter()
        completedSpecIds.add(Integer.valueOf(currentSpec.specId()))
        completedPartitions.clear()
      }

      if (completedSpecIds.contains(Integer.valueOf(spec.specId()))) {
        throw new IllegalStateException(
          s"Incoming records violate the writer assumption that records are clustered by spec: " +
            s"spec $spec has already been completed")
      }

      currentSpec = spec
      partitionComparator = Comparators.forType(spec.partitionType())
      completedPartitions = StructLikeSet.create(spec.partitionType())
      currentPartition = StructLikeUtil.copy(partition)
      currentWriter = newWriter(currentSpec, currentPartition)
    } else if ((partition ne currentPartition) &&
        partitionComparator.compare(partition, currentPartition) != 0) {
      closeCurrentWriter()
      completedPartitions.add(currentPartition)

      if (completedPartitions.contains(partition)) {
        throw new IllegalStateException(
          s"Incoming records violate the writer assumption that records are clustered by " +
            s"partition: partition '${spec.partitionToPath(partition)}' in spec $spec has " +
            s"already been completed")
      }

      currentPartition = StructLikeUtil.copy(partition)
      currentWriter = newWriter(currentSpec, currentPartition)
    }

    currentWriter.write(row)
  }

  override def close(): Unit = {
    if (!closed) {
      closeCurrentWriter()
      closed = true
    }
  }

  private def closeCurrentWriter(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
      } catch {
        case ioe: IOException =>
          throw new UncheckedIOException("Failed to close current writer", ioe)
      }
      addResult(currentWriter.result())
      currentWriter = null
    }
  }

  override final def result(): R = {
    require(closed, "Cannot get result from unclosed writer")
    aggregatedResult()
  }
}

class GpuClusteredDataWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long) extends
  GpuClusteredWriter[SpillableColumnarBatch, DataWriteResult] {

  private val dataFiles = Lists.newArrayList[DataFile]()

  override def newWriter(partitionSpec: PartitionSpec, structLike: StructLike):
  FileWriter[SpillableColumnarBatch, DataWriteResult] = {
    new GpuRollingDataWriter(
      writerFactory,
      fileFactory,
      io,
      targetFileSize,
      partitionSpec,
      structLike)
  }

  override def addResult(r: DataWriteResult): Unit = {
    dataFiles.addAll(r.dataFiles())
  }

  override def aggregatedResult(): DataWriteResult = {
    new DataWriteResult(dataFiles)
  }
}

class GpuClusteredPositionDeleteWriter(
    writerFactory: GpuSparkFileWriterFactory,
    fileFactory: OutputFileFactory,
    io: FileIO,
    targetFileSize: Long) extends
  GpuClusteredWriter[SpillableColumnarBatch, DeleteWriteResult] {

  private val deleteFiles = Lists.newArrayList[DeleteFile]()
  private val referencedDataFiles = CharSequenceSet.empty()

  override def newWriter(partitionSpec: PartitionSpec, structLike: StructLike):
  FileWriter[SpillableColumnarBatch, DeleteWriteResult] = new GpuRollingPositionDeleteWriter(
    writerFactory, fileFactory, io, targetFileSize, partitionSpec, structLike)

  override def addResult(r: DeleteWriteResult): Unit = {
    deleteFiles.addAll(r.deleteFiles())
    referencedDataFiles.addAll(r.referencedDataFiles())
  }

  override def aggregatedResult(): DeleteWriteResult = {
    new DeleteWriteResult(deleteFiles, referencedDataFiles)
  }
}
