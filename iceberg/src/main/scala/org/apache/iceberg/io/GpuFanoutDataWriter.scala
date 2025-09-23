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

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, PartitionSpec, StructLike}
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory

class GpuFanoutDataWriter(
  writerFactory: GpuSparkFileWriterFactory,
  fileFactory: OutputFileFactory,
  io: FileIO,
  targetFileSize: Long,
) extends FanoutWriter[SpillableColumnarBatch, DataWriteResult] {
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
