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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.SpillableColumnarBatch
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.StructLike
import org.apache.iceberg.encryption.EncryptedOutputFile
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuRollingFileWriterSuite extends AnyFunSuite with MockitoSugar {

  test("rolling writer rotates files when reaching the target row threshold") {
    val fileFactory = mock[OutputFileFactory]
    val io = mock[FileIO]

    val firstFile = mockEncryptedOutputFile("file-0")
    val secondFile = mockEncryptedOutputFile("file-1")

    when(fileFactory.newOutputFile()).thenReturn(firstFile, secondFile)

    val writer = new TestRollingFileWriter(
      fileFactory,
      io,
      targetFileSize = 300L,
      spec = PartitionSpec.unpartitioned(),
      partition = null)

    val batches = Seq(200L, 100L, 200L).map(size => new FixedSizeSpillableColumnarBatch(size))
    batches.foreach(writer.write)

    writer.close()
    val results = writer.result()

    assertResult(Seq(300L, 200L))(results)
    assertResult(2)(writer.createdWriterCount)

    verify(fileFactory, times(2)).newOutputFile()
    verify(io, never()).deleteFile(any[OutputFile])
  }

  private def mockEncryptedOutputFile(id: String): EncryptedOutputFile = {
    val outputFile = mock[OutputFile]
    when(outputFile.location()).thenReturn(id)

    val encrypted = mock[EncryptedOutputFile]
    when(encrypted.encryptingOutputFile()).thenReturn(outputFile)
    encrypted
  }
}

private[io] class TestRollingFileWriter(
    override val fileFactory: OutputFileFactory,
    override val io: FileIO,
    override val targetFileSize: Long,
    override val spec: PartitionSpec,
    override val partition: StructLike)
  extends GpuRollingFileWriter[FakeFileWriter, Seq[Long]] {

  private val results = ListBuffer.empty[Long]
  private var writersCreated = 0

  openCurrentWriter()

  override protected def newWriter(file: EncryptedOutputFile): FakeFileWriter = {
    writersCreated += 1
    new FakeFileWriter
  }

  override protected def addResult(result: Seq[Long]): Unit = {
    results.appendAll(result)
  }

  override protected def aggregatedResult(): Seq[Long] = results.toSeq

  def createdWriterCount: Int = writersCreated
}

private[io] class FakeFileWriter extends FileWriter[SpillableColumnarBatch, Seq[Long]] {
  private var bytesWritten = 0L

  override def write(batch: SpillableColumnarBatch): Unit = {
    bytesWritten += batch.sizeInBytes
    batch.close()
  }

  override def result(): Seq[Long] = Seq(bytesWritten)

  override def length(): Long = bytesWritten

  override def close(): Unit = {}
}

private[io] class FixedSizeSpillableColumnarBatch(
    override val sizeInBytes: Long,
    rows: Int = 1) extends SpillableColumnarBatch {

  override def numRows(): Int = rows

  override def setSpillPriority(priority: Long): Unit = {}

  override def incRefCount(): SpillableColumnarBatch = this

  override def getColumnarBatch(): ColumnarBatch =
    throw new UnsupportedOperationException("materialization is not supported in tests")

  override def dataTypes: Array[DataType] = Array.empty

  override def close(): Unit = {}

  override def toString: String = s"FixedSizeSCB(size=$sizeInBytes, rows=$rows)"
}
