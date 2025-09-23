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

import java.util.{List => JList}

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.{DataFile, PartitionSpec, StructLike}
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.apache.iceberg.relocated.com.google.common.collect.Lists
import org.apache.iceberg.spark.source.GpuSparkFileWriterFactory

class GpuRollingDataWriter(
  val gpuSparkFileWriterFactory: GpuSparkFileWriterFactory,
  val fileFactory: OutputFileFactory,
  val io: FileIO,
  val targetFileSize: Long,
  val spec: PartitionSpec,
  val partition: StructLike) extends
  GpuRollingFileWriter[DataWriter[SpillableColumnarBatch], DataWriteResult] {

  private val dataFiles: JList[DataFile] = Lists.newArrayList[DataFile]()
  openCurrentWriter()

  protected override def newWriter(file: EncryptedOutputFile): DataWriter[SpillableColumnarBatch] =
  {
    gpuSparkFileWriterFactory.newDataWriter(file, spec, partition)
  }

  protected override def addResult(result: DataWriteResult): Unit = {
    dataFiles.addAll(result.dataFiles())
  }

  protected override def aggregatedResult(): DataWriteResult = {
    new DataWriteResult(dataFiles)
  }
}
