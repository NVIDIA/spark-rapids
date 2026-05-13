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

import java.util.{HashMap => JHashMap}

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, DeleteFile, PartitionSpec, StructLike}
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory
import org.apache.iceberg.util.{CharSequenceSet, StructLikeMap, StructLikeUtil}

abstract class GpuFanoutWriter[T, R] extends PartitioningWriter[T, R] {
  private val writers =
    new JHashMap[Integer, StructLikeMap[FileWriter[T, R]]]()
  private var closed = false

  protected def newWriter(partitionSpec: PartitionSpec, partition: StructLike): FileWriter[T, R]
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  override def write(row: T, spec: PartitionSpec, partition: StructLike): Unit = {
    require(!closed, "Cannot write to a closed writer")
    writer(spec, partition).write(row)
  }

  private def writer(spec: PartitionSpec, partition: StructLike): FileWriter[T, R] = {
    val specId = Integer.valueOf(spec.specId())
    var specWriters = writers.get(specId)
    if (specWriters == null) {
      specWriters = StructLikeMap.create[FileWriter[T, R]](spec.partitionType())
      writers.put(specId, specWriters)
    }

    var partitionWriter = specWriters.get(partition)
    if (partitionWriter == null) {
      val copiedPartition = StructLikeUtil.copy(partition)
      partitionWriter = newWriter(spec, copiedPartition)
      specWriters.put(copiedPartition, partitionWriter)
    }

    partitionWriter
  }

  override def close(): Unit = {
    if (!closed) {
      closeWriters()
      closed = true
    }
  }

  private def closeWriters(): Unit = {
    val specIterator = writers.values().iterator()
    while (specIterator.hasNext) {
      val specWriters = specIterator.next()
      val writerIterator = specWriters.values().iterator()
      while (writerIterator.hasNext) {
        val writer = writerIterator.next()
        writer.close()
        addResult(writer.result())
      }
      specWriters.clear()
    }
    writers.clear()
  }

  override final def result(): R = {
    require(closed, "Cannot get result from unclosed writer")
    aggregatedResult()
  }
}

class GpuFanoutDataWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long) extends GpuFanoutWriter[SpillableColumnarBatch, DataWriteResult] {
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

class GpuFanoutPositionDeleteWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long) extends GpuFanoutWriter[SpillableColumnarBatch, DeleteWriteResult] {

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
