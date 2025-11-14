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
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.apache.iceberg.encryption.EncryptedOutputFile


trait GpuRollingFileWriter[W <: FileWriter[SpillableColumnarBatch, R], R] extends
  FileWriter[SpillableColumnarBatch, R] {

  val fileFactory: OutputFileFactory
  val io: FileIO
  val targetFileSize: Long
  val spec: PartitionSpec
  val partition: StructLike

  private var currentFile: Option[EncryptedOutputFile] = None
  private var currentFileRows: Long = 0L
  private var currentWriter: Option[W] = None

  private var closed: Boolean = false

  protected def newWriter(file: EncryptedOutputFile): W
  protected def addResult(result: R): Unit
  protected def aggregatedResult(): R

  override def length(): Long = {
    throw new UnsupportedOperationException("length is not supported" +
      " in GpuRollingFileWriter")
  }

  override def write(batch: SpillableColumnarBatch): Unit = {
    if (closed) {
      throw new IllegalStateException("Cannot write to a closed writer")
    }

    if (batch.numRows() <= 0) {
      return
    }

    if (currentWriter.isEmpty) {
      openCurrentWriter()
    }

    currentWriter.get.write(batch)
    currentFileRows += batch.numRows()

    if (currentWriter.get.length() >= targetFileSize) {
      closeCurrentWriter()
    }
  }

  protected def openCurrentWriter(): Unit = {
    require(currentWriter.isEmpty,
      "Current writer should be empty when opening a new writer")

    currentFile = Some(newFile())
    currentWriter = Some(newWriter(currentFile.get))
    currentFileRows = 0L
  }

  private def newFile(): EncryptedOutputFile = {
    if (spec.isUnpartitioned || partition == null) {
      fileFactory.newOutputFile
    } else {
      fileFactory.newOutputFile(spec, partition)
    }
  }

  private def closeCurrentWriter(): Unit = {
    currentWriter.foreach { w =>

      if (currentFileRows == 0L) {
        io.deleteFile(currentFile.get.encryptingOutputFile)
      } else {
        w.close()
        addResult(w.result())
      }

      this.currentFile = None
      this.currentFileRows = 0
      this.currentWriter = None
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closeCurrentWriter()
      this.closed = true
    }
  }

  override def result: R = {
    require(closed, "Cannot get result from unclosed writer")
    aggregatedResult()
  }
}
