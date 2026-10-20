/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import java.io.{FileNotFoundException, IOException}

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.io.async.AsyncRunner
import com.nvidia.spark.rapids.parquet._
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.schema.{MessageType, Type}
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiFileReaderSuite extends AnyFunSuite with RmmSparkRetrySuiteBase {

  test("coalescing async missing-file failure preserves the Hadoop file URI") {
    val path = new Path("file:///tmp/missing ORC.orc")
    val expectedPath = "file:///tmp/missing%20ORC.orc"
    assert(path.toUri.toString === expectedPath)
    val missingFile = new FileNotFoundException("missing ORC file")
    val runner = AsyncRunner.newUnboundedTask[Unit](() => throw missingFile)
    MultiFileReaderUtils.attachFilePathToMissingFile(runner, path)

    val transformed = intercept[FileNotFoundException](runner.call())
    transformed match {
      case GpuFileNotFoundException(path, originalException) =>
        assert(path === expectedPath)
        assert(originalException eq missingFile)
      case _ => fail(s"Missing-file failure did not carry its file path: $transformed")
    }
  }

  test("avoid infinite loop when host buffers empty") {
    val conf = new Configuration(false)
    val memBuffers: Array[SingleHMBAndMeta] = {
      val singleBuf = SpillableHostBuffer(HostMemoryBuffer.allocate(0), 0,
        SpillPriorities.ACTIVE_BATCHING_PRIORITY)
      Array(SingleHMBAndMeta(Array(singleBuf), 0L, 0, Seq.empty))
    }
    val poolConf = new ThreadPoolConfBuilder(
      maxThreadNumber = 1,
      isMemoryBounded = true,
      memoryCapacityFromDriver =  1L << 20,
      timeoutMs = 10 * 1000L, // 10 seconds
      stageLevelPool = false
    ).build()
    val multiFileReader = new MultiFileCloudPartitionReaderBase(
      conf,
      inputFiles = Array.empty,
      poolConf = poolConf,
      maxNumFileProcessed = 1,
      filters = Array.empty,
      execMetrics = Map.empty,
      maxReadBatchSizeRows = 1000,
      maxReadBatchSizeBytes = 64L * 1024L * 1024L) {

      // Setup some empty host buffers at the start
      currentFileHostBuffers = Some(new HostMemoryBuffersWithMetaDataBase {
        override def partitionedFile: PartitionedFile =
          PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty, "", 0, 0)
        override def memBuffersAndSizes: Array[SingleHMBAndMeta] = memBuffers
        override def bytesRead: Long = 0
      })

      override def getBatchRunner(
          tc: TaskContext,
          file: PartitionedFile,
          conf: Configuration,
          filters: Array[Filter]): AsyncRunner[HostMemoryBuffersWithMetaDataBase] = {
        AsyncRunner.newUnboundedTask(() => null)
      }

      override def readBatches(h: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] =
        EmptyGpuColumnarBatchIterator

      override def getFileFormatShortName: String = ""
    }

    withResource(multiFileReader) { _ =>
      assertResult(false)(multiFileReader.next())
    }
  }

  private class PendingFilesReader(firstProducesRows: Boolean)
      extends MultiFileCloudPartitionReaderBase(
        new Configuration(false), Array.empty,
        new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false).build(),
        1, Array.empty, Map.empty, 1000, 64L * 1024 * 1024) {
    var released = 0

    private def file(rows: Long): HostMemoryBuffersWithMetaDataBase = {
      val result = new HostMemoryBuffersWithMetaDataBase {
        override def partitionedFile: PartitionedFile =
          PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty, "file:///tmp/test", 0, 0)
        override def memBuffersAndSizes: Array[SingleHMBAndMeta] =
          Array(SingleHMBAndMeta.empty(rows))
        override def bytesRead: Long = 0
      }
      result.addReleaseResourceCallback(() => released += 1)
      result
    }

    combineLeftOverFiles = Some(Array(file(1), file(2)))

    override def combineHMBs(input: Array[HostMemoryBuffersWithMetaDataBase])
        : HostMemoryBuffersWithMetaDataBase = {
      combineLeftOverFiles = if (input.length > 1) Some(input.tail) else None
      input.head
    }

    override def getBatchRunner(tc: TaskContext, file: PartitionedFile,
        conf: Configuration, filters: Array[Filter])
        : AsyncRunner[HostMemoryBuffersWithMetaDataBase] =
      throw new UnsupportedOperationException("All files are already pending")

    override def readBatches(meta: HostMemoryBuffersWithMetaDataBase)
        : Iterator[ColumnarBatch] = withResource(meta) { _ =>
      val rows = meta.memBuffersAndSizes.head.numRows.toInt
      if (rows == 1 && !firstProducesRows) {
        EmptyGpuColumnarBatchIterator
      } else {
        Iterator(new ColumnarBatch(Array.empty, rows))
      }
    }

    override def getFileFormatShortName: String = "test"
  }

  test("drain pending files after a group produces no rows and all futures are consumed") {
    withResource(new PendingFilesReader(firstProducesRows = false)) { reader =>
      assert(reader.next())
      withResource(reader.get()) { batch => assert(batch.numRows() == 2) }
      assert(!reader.next())
      assert(reader.released == 2)
    }
  }

  test("close releases already collected files after early termination") {
    val reader = new PendingFilesReader(firstProducesRows = true)
    withResource(reader) { _ =>
      assert(reader.next())
      withResource(reader.get()) { batch => assert(batch.numRows() == 1) }
      assert(reader.released == 1)
    }
    assert(reader.released == 2)
  }

  private class CorruptFileReader(failingChunk: Int, withHealthyFile: Boolean)
      extends MultiFileCloudParquetPartitionReader(
        null, new Configuration(false), Array.empty,
        _ => throw new UnsupportedOperationException("All files are already pending"),
        true, None, false, 1000, 1L << 20, 1L << 20, 1L << 20, false, 0,
        false, CpuCompressionConfig.disabled(), Map.empty, new StructType(),
        new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false).build(),
        1, false, true, false, false, true, CombineConf(1L << 20, 0)) {
    val attemptedChunks = ArrayBuffer[Int]()
    val hostBuffers = ArrayBuffer[HostMemoryBuffer]()
    var releasedFiles = 0

    private val parquetSchema = new MessageType("test", java.util.Collections.emptyList[Type]())
    private val readSchema = new StructType()

    private def partitionedFile(name: String): PartitionedFile =
      PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty, s"file:///tmp/$name", 0, 0)

    private def corruptFile(): HostMemoryBuffersWithMetaData = {
      val buffers = (1 to 3).map { rows =>
        val hostBuffer = HostMemoryBuffer.allocate(1)
        hostBuffers += hostBuffer
        val spillable = SpillableHostBuffer(hostBuffer, 1,
          SpillPriorities.ACTIVE_BATCHING_PRIORITY)
        SingleHMBAndMeta(Array(spillable), 1, rows, Seq.empty)
      }.toArray
      val file = partitionedFile("corrupt.parquet")
      val metadata = ParquetFileInfoWithBlockMeta(new Path(file.filePath.toString()),
        Seq.empty, file.partitionValues, parquetSchema, readSchema,
        DateTimeRebaseCorrected, DateTimeRebaseCorrected, false)
      val result = newHMBWithMetaDataForChunks(file, buffers, buffers.map(_.bytes).sum, metadata)
      result.addReleaseResourceCallback(() => releasedFiles += 1)
      result
    }

    private def healthyEmptyFile(): HostMemoryBuffersWithMetaDataBase = {
      // The production combiner keeps metadata-only rows separate from the buffered file.
      val result = newHMEmptyMetadataForChunks(partitionedFile("healthy.parquet"), 0, 0,
        DateTimeRebaseCorrected, DateTimeRebaseCorrected, false,
        parquetSchema, readSchema, 4, Seq.empty)
      result.addReleaseResourceCallback(() => releasedFiles += 1)
      result
    }

    val files: Array[HostMemoryBuffersWithMetaDataBase] =
      if (withHealthyFile) Array(corruptFile(), healthyEmptyFile()) else Array(corruptFile())
    combineLeftOverFiles = Some(files)

    override protected def readBufferToBatches(buffer: HostMemoryBuffersWithMetaData)
        : Iterator[ColumnarBatch] = {
      val chunk = buffer.memBuffersAndSizes.head
      val rows = chunk.numRows.toInt
      attemptedChunks += rows
      // The file has only three buffered chunks, so exceeding this limit indicates retries.
      // Bound those retries so a regression fails instead of hanging. AssertionError escapes
      // the ignoreCorruptFiles handler, which would swallow RuntimeException or IOException.
      if (attemptedChunks.size > 3) {
        throw new AssertionError("Reader retried a corrupt chunk instead of skipping the file")
      }
      withResource(chunk.hmbs) { _ =>
        if (rows == failingChunk) {
          throw new IOException("Corrupt chunk")
        }
        Iterator(new ColumnarBatch(Array.empty, rows))
      }
    }
  }

  for (failingChunk <- Seq(1, 2, 3); withHealthyFile <- Seq(false, true)) {
    test(s"skip corrupt chunk $failingChunk and remaining chunks, healthy file=$withHealthyFile") {
      val reader = new CorruptFileReader(failingChunk, withHealthyFile)
      // Also own the original wrappers so a failed regression test cannot leak skipped buffers.
      withResource(reader.files) { _ =>
        withResource(reader) { _ =>
          val expectedRows = (1 until failingChunk) ++ (if (withHealthyFile) Seq(4) else Seq.empty)
          expectedRows.foreach { rows =>
            assert(reader.next())
            withResource(reader.get()) { batch => assert(batch.numRows() == rows) }
          }
          assert(!reader.next())
          assert(reader.attemptedChunks.toSeq === (1 to failingChunk))
          assert(reader.releasedFiles == reader.files.length)
          assert(reader.hostBuffers.forall(_.getRefCount == 0))
        }
      }
    }
  }
}
