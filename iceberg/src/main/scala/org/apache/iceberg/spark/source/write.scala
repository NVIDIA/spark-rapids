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

package org.apache.iceberg.spark.source

import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}

abstract class GpuBaseBatchWrite(write: GpuSparkWrite) extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory
  = write.createDataWriterFactory

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit =
    write.abort(writerCommitMessages)

  override def useCommitCoordinator(): Boolean = false
}

class GpuBatchAppend(write: GpuSparkWrite) extends GpuBaseBatchWrite(write) {
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    val append = write.table.newAppend()

    val files = write.files(messages)

    val numFiles = files.length
    files.foreach(append.appendFile)

    write.commitOperation(append, s"append with $numFiles new data files")
  }
}

class GpuDynamicOverwrite(
    write: GpuSparkWrite,
    cpuBatchWrite: BatchWrite) extends GpuBaseBatchWrite(write) {
  
  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    // Delegate to the CPU version's commit method
    // The CPU version handles all the complex logic for dynamic partition overwrite
    cpuBatchWrite.commit(messages)
  }
}