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

package com.nvidia.spark.rapids.sequencefile

import java.net.URI
import java.nio.channels.Channels
import java.util.concurrent.{Callable, Future => JFuture, ThreadPoolExecutor}
import java.util.concurrent.atomic.AtomicLong

import ai.rapids.cudf._
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.jni.SequenceFile
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.{Reader => HadoopSeqReader}

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector => SparkVector}
import org.apache.spark.util.SerializableConfiguration

/**
 * Global statistics accumulator for SequenceFile GPU reading.
 * Thread-safe counters that aggregate across all tasks.
 */
object SeqFileGpuStats {
  // Timing stats (nanoseconds)
  val totalReadTimeNs = new AtomicLong(0)
  val totalH2dTimeNs = new AtomicLong(0)
  val totalGpuTimeNs = new AtomicLong(0)
  val totalOverlapTimeNs = new AtomicLong(0)
  
  // Count stats
  val totalFiles = new AtomicLong(0)
  val totalBytes = new AtomicLong(0)
  val totalTasks = new AtomicLong(0)
  
  def reset(): Unit = {
    totalReadTimeNs.set(0)
    totalH2dTimeNs.set(0)
    totalGpuTimeNs.set(0)
    totalOverlapTimeNs.set(0)
    totalFiles.set(0)
    totalBytes.set(0)
    totalTasks.set(0)
  }
  
  def addStats(readNs: Long, h2dNs: Long, gpuNs: Long, overlapNs: Long,
               files: Int, bytes: Long): Unit = {
    totalReadTimeNs.addAndGet(readNs)
    totalH2dTimeNs.addAndGet(h2dNs)
    totalGpuTimeNs.addAndGet(gpuNs)
    totalOverlapTimeNs.addAndGet(overlapNs)
    totalFiles.addAndGet(files)
    totalBytes.addAndGet(bytes)
    totalTasks.incrementAndGet()
  }
  
  def printSummary(): Unit = {
    val tasks = totalTasks.get()
    if (tasks == 0) {
      println("[SeqFile GPU] No stats collected yet")
      return
    }
    
    val readMs = totalReadTimeNs.get() / 1e6
    val h2dMs = totalH2dTimeNs.get() / 1e6
    val gpuMs = totalGpuTimeNs.get() / 1e6
    val overlapMs = totalOverlapTimeNs.get() / 1e6
    val effectiveReadMs = readMs - overlapMs
    val totalMB = totalBytes.get() / 1024.0 / 1024.0
    
    // scalastyle:off println
    println(f"""
      |╔════════════════════════════════════════════════════════════════════════╗
      |║              SeqFile GPU Reader - GLOBAL SUMMARY                       ║
      |╠════════════════════════════════════════════════════════════════════════╣
      |║  Tasks: ${tasks}%6d    Files: ${totalFiles.get()}%6d    Data: ${totalMB}%,.0f MB
      |║                                                                        ║
      |╠════════════════════════════════════════════════════════════════════════╣
      |║  Read to Host:      ${readMs}%10.1f ms (raw)                             ║
      |║    - Overlap:       ${overlapMs}%10.1f ms (saved by pipeline)            ║
      |║    - Effective:     ${effectiveReadMs}%10.1f ms                          ║
      |║  H2D Transfer:      ${h2dMs}%10.1f ms                                    ║
      |║  GPU Parse+Extract: ${gpuMs}%10.1f ms                                    ║
      |╠════════════════════════════════════════════════════════════════════════╣
      |║  Throughput: ${totalMB / ((effectiveReadMs + h2dMs + gpuMs) / 1000.0)}%,.0f MB/s
      |║                                                                        ║
      |║  Pipeline efficiency: ${if (readMs > 0) f"${overlapMs * 100 / readMs}%.1f" else "N/A"}%%
      |║                                                                        ║
      |╚════════════════════════════════════════════════════════════════════════╝
      |""".stripMargin)
    // scalastyle:on println
  }
}

/**
 * Multi-file reader factory for GPU SequenceFile reading.
 *
 * This factory creates readers that can process multiple SequenceFiles in a single
 * GPU operation, providing higher parallelism by processing chunks from all files
 * simultaneously.
 */
case class GpuSequenceFileMultiFileReaderFactory(
    @transient sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    partitionSchema: StructType,
    @transient rapidsConf: RapidsConf,
    metrics: Map[String, GpuMetric])
  extends MultiFilePartitionReaderFactoryBase(sqlConf, broadcastedConf, rapidsConf)
  with Logging {

  // Build thread pool config (rapidsConf is transient, won't survive serialization)
  private val threadPoolConfBuilder: ThreadPoolConfBuilder = ThreadPoolConfBuilder(rapidsConf)
  
  // SequenceFile specific configs (serializable values extracted from rapidsConf)
  private val readBufferSize: Long = rapidsConf.sequenceFileReadBufferSize
  private val asyncPipelineEnabled: Boolean = rapidsConf.isSequenceFileAsyncPipelineEnabled
  private val batchSizeBytes: Long = rapidsConf.sequenceFileBatchSizeBytes

  override protected def canUseCoalesceFilesReader: Boolean = true
  override protected def canUseMultiThreadReader: Boolean = true
  override protected def getFileFormatShortName: String = "SequenceFileBinary"

  override def createReader(partition: org.apache.spark.sql.connector.read.InputPartition):
      PartitionReader[InternalRow] = {
    throw new IllegalStateException("ROW BASED PARSING IS NOT SUPPORTED ON THE GPU...")
  }

  override protected def buildBaseColumnarReaderForCloud(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    new MultiFileSequenceFilePartitionReader(
      conf,
      files,
      readDataSchema,
      partitionSchema,
      maxGpuColumnSizeBytes,
      metrics,
      threadPoolConfBuilder,
      readBufferSize,
      asyncPipelineEnabled,
      batchSizeBytes)
  }

  override protected def buildBaseColumnarReaderForCoalescing(
      files: Array[PartitionedFile],
      conf: Configuration): PartitionReader[ColumnarBatch] = {
    // For SequenceFile, multi-threaded reading is beneficial even for local files
    // because we combine multiple files into one GPU operation anyway.
    new MultiFileSequenceFilePartitionReader(
      conf,
      files,
      readDataSchema,
      partitionSchema,
      maxGpuColumnSizeBytes,
      metrics,
      threadPoolConfBuilder,
      readBufferSize,
      asyncPipelineEnabled,
      batchSizeBytes)
  }
}

/**
 * Partition reader that processes multiple SequenceFiles in a single GPU operation.
 *
 * This reader supports async I/O pipelining:
 * - When asyncPipelineEnabled=true, divides files into batches
 * - Starts reading the next batch while GPU processes the current batch
 * - Overlaps I/O with GPU computation for better throughput
 *
 * Pipeline stages:
 * 1. Read files to host memory (can overlap with GPU)
 * 2. Copy to GPU (H2D transfer)
 * 3. Parse on GPU
 * 4. Build output batches
 */
class MultiFileSequenceFilePartitionReader(
    conf: Configuration,
    files: Array[PartitionedFile],
    requiredSchema: StructType,
    partitionSchema: StructType,
    maxGpuColumnSizeBytes: Long,
    execMetrics: Map[String, GpuMetric],
    threadPoolConfBuilder: ThreadPoolConfBuilder,
    readBufferSize: Long = 64 * 1024 * 1024,
    asyncPipelineEnabled: Boolean = true,
    batchSizeBytes: Long = 256 * 1024 * 1024)
  extends PartitionReader[ColumnarBatch] with Logging {

  // Get shared thread pool (lazy to avoid initialization until actually needed)
  private lazy val threadPoolConf: ThreadPoolConf = threadPoolConfBuilder.build()
  private lazy val threadPool: ThreadPoolExecutor =
    MultiFileReaderThreadPool.getOrCreateThreadPool(threadPoolConf)

  private val wantsKey = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD))
  private val wantsValue = requiredSchema.fieldNames.exists(
    _.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD))
  private val logTimingEnabled = sys.env.get("SEQFILE_GPU_TIMING").exists(_.nonEmpty)

  // Parsed batch queue - we parse all files at once, then emit batches
  private var batchQueue: Iterator[ColumnarBatch] = Iterator.empty
  private var initialized = false

  private def readMetric: GpuMetric = execMetrics.getOrElse(READ_FS_TIME, NoopMetric)
  private def decodeMetric: GpuMetric = execMetrics.getOrElse(GPU_DECODE_TIME, NoopMetric)

  override def next(): Boolean = {
    if (!initialized) {
      initialized = true
      batchQueue = parseAllFiles()
    }
    batchQueue.hasNext
  }

  override def get(): ColumnarBatch = {
    batchQueue.next()
  }

  override def close(): Unit = {
    // Drain any remaining batches
    while (batchQueue.hasNext) {
      batchQueue.next().close()
    }
  }

  /**
   * Parse file headers sequentially.
   */
  private def parseHeadersSequential(partFiles: Array[PartitionedFile]): Array[FileInfo] = {
    partFiles.map(parseFileHeader)
  }

  /**
   * Parse file headers in parallel using the shared thread pool.
   */
  private def parseHeadersParallel(partFiles: Array[PartitionedFile]): Array[FileInfo] = {
    val futures: Array[JFuture[FileInfo]] = partFiles.map { partFile =>
      threadPool.submit(new Callable[FileInfo] {
        override def call(): FileInfo = parseFileHeader(partFile)
      })
    }

    futures.map { future =>
      try {
        future.get()
      } catch {
        case e: java.util.concurrent.ExecutionException =>
          throw e.getCause
      }
    }
  }

  /**
   * Parse a single file's header and return FileInfo.
   */
  private def parseFileHeader(partFile: PartitionedFile): FileInfo = {
    val path = new Path(new URI(partFile.filePath.toString))
    val header = SequenceFileHeader.parse(path, conf)

    if (!header.isGpuParseable) {
      throw new UnsupportedOperationException(
        s"GPU SequenceFile reader does not support compressed files: $path")
    }

    val fs = path.getFileSystem(conf)
    val fileSize = fs.getFileStatus(path).getLen
    val (dataStart, dataSize) = computeSplitDataRange(partFile, path, header, fileSize)

    FileInfo(partFile, path, header, dataStart, dataSize)
  }

  /**
   * Parse all files using async I/O pipelining when enabled.
   * 
   * Pipeline strategy:
   * - Divide files into batches based on batchSizeBytes
   * - Start reading batch N+1 while GPU processes batch N
   * - Overlaps I/O with GPU computation for better throughput
   */
  private def parseAllFiles(): Iterator[ColumnarBatch] = {
    if (files.isEmpty) {
      return Iterator.empty
    }

    // Step 1: Parse headers (can be parallelized since each file is independent)
    val headerStartTime = System.nanoTime()
    val fileInfos = readMetric.ns {
      if (threadPoolConf.maxThreadNumber > 1 && files.length > 1) {
        parseHeadersParallel(files)
      } else {
        parseHeadersSequential(files)
      }
    }
    val headerTime = System.nanoTime() - headerStartTime

    // Calculate total data size
    val totalDataSize = fileInfos.map(_.dataSize).sum
    if (totalDataSize <= 0) {
      return Iterator.empty
    }

    // For count-only queries (no columns needed), use CPU counting
    if (!wantsKey && !wantsValue) {
      if (logTimingEnabled) {
        // scalastyle:off println
        println(s"[SeqFile GPU] Count-only mode: ${files.length} files, " +
          s"${totalDataSize / 1024 / 1024} MB (using CPU, no H2D transfer)")
        // scalastyle:on println
      }
      return createCountOnlyBatches(fileInfos)
    }

    // Divide files into batches for pipelining
    val fileBatches = divideIntoBatches(fileInfos)
    val numBatches = fileBatches.length
    val usePipeline = asyncPipelineEnabled && numBatches > 1

    // Process batches with optional pipelining
    if (usePipeline) {
      // Pipelined execution: overlap I/O with GPU
      processBatchesWithPipeline(fileBatches, headerTime, totalDataSize)
    } else {
      // Sequential execution: process all files in one go
      processBatchesSequentially(fileBatches, headerTime, totalDataSize)
    }
  }

  /**
   * Process batches sequentially (no pipelining).
   */
  private def processBatchesSequentially(
      fileBatches: Array[Array[FileInfo]],
      headerTime: Long,
      totalDataSize: Long): Iterator[ColumnarBatch] = {
    
    val totalStartTime = System.nanoTime()
    var totalReadTime = 0L
    var totalH2dTime = 0L
    var totalGpuTime = 0L

    val allBatches = fileBatches.flatMap { batchInfos =>
      val hostData = readBatchToHost(batchInfos)
      totalReadTime += hostData.readTimeNs

      val (batches, h2dTime, gpuTime) = processBatchOnGpu(hostData)
      totalH2dTime += h2dTime
      totalGpuTime += gpuTime

      batches
    }

    val totalTime = System.nanoTime() - totalStartTime
    val totalSizeMb = totalDataSize / 1024 / 1024

    if (logTimingEnabled) {
      // Log timing breakdown
      // scalastyle:off println
      println(f"""
        |[SeqFile GPU] Timing (${files.length} files, ${totalSizeMb} MB, sequential):
        |  1. Parse Headers:     ${headerTime / 1e6}%8.1f ms
        |  2. Read to Host:      ${totalReadTime / 1e6}%8.1f ms
        |  3. H2D Transfer:      ${totalH2dTime / 1e6}%8.1f ms
        |  4. GPU Parse+Extract: ${totalGpuTime / 1e6}%8.1f ms
        |  ─────────────────────────────────────────────────────
        |  Total:                ${totalTime / 1e6}%8.1f ms
        |""".stripMargin)
      // scalastyle:on println
    }

    // Add to global stats (no overlap in sequential mode)
    SeqFileGpuStats.addStats(totalReadTime, totalH2dTime, totalGpuTime, 0L,
      files.length, totalDataSize)

    allBatches.iterator
  }

  /**
   * Process batches with async I/O pipelining.
   * Start reading batch N+1 while GPU processes batch N.
   */
  private def processBatchesWithPipeline(
      fileBatches: Array[Array[FileInfo]],
      headerTime: Long,
      totalDataSize: Long): Iterator[ColumnarBatch] = {
    
    val totalStartTime = System.nanoTime()
    var totalReadTime = 0L
    var totalH2dTime = 0L
    var totalGpuTime = 0L
    var readOverlapTime = 0L  // Time saved by overlapping I/O with GPU

    val allBatches = scala.collection.mutable.ArrayBuffer[ColumnarBatch]()

    // Start reading first batch synchronously
    var currentHostData = readBatchToHost(fileBatches(0))
    totalReadTime += currentHostData.readTimeNs

    for (i <- fileBatches.indices) {
      // Start reading next batch asynchronously (if there is one)
      val nextReadFuture: Option[JFuture[HostBatchData]] = 
        if (i + 1 < fileBatches.length) {
          Some(readBatchToHostAsync(fileBatches(i + 1)))
        } else {
          None
        }

      // Process current batch on GPU
      val gpuProcessStartTime = System.nanoTime()
      val (batches, h2dTime, gpuTime) = processBatchOnGpu(currentHostData)
      val gpuProcessTime = System.nanoTime() - gpuProcessStartTime
      
      totalH2dTime += h2dTime
      totalGpuTime += gpuTime
      allBatches ++= batches

      // Wait for next batch's I/O to complete (if started)
      nextReadFuture.foreach { future =>
        try {
          currentHostData = future.get()
          totalReadTime += currentHostData.readTimeNs
          
          // Calculate overlap: how much of the read happened during GPU processing
          val overlap = math.min(currentHostData.readTimeNs, gpuProcessTime)
          readOverlapTime += overlap
        } catch {
          case e: java.util.concurrent.ExecutionException =>
            throw e.getCause
        }
      }
    }

    val totalTime = System.nanoTime() - totalStartTime
    val effectiveReadTime = totalReadTime - readOverlapTime

    if (logTimingEnabled) {
      // Log timing breakdown with pipeline info
      // scalastyle:off println
      // scalastyle:off line.size.limit
      println(f"""
        |[SeqFile GPU] Timing (${files.length} files, ${totalDataSize / 1024 / 1024} MB,
        |  ${fileBatches.length} batches, PIPELINED):
        |  1. Parse Headers:     ${headerTime / 1e6}%8.1f ms
        |  2. Read to Host:      ${totalReadTime / 1e6}%8.1f ms (raw)
        |     - Overlap w/ GPU:  ${readOverlapTime / 1e6}%8.1f ms (saved)
        |     - Effective:       ${effectiveReadTime / 1e6}%8.1f ms
        |  3. H2D Transfer:      ${totalH2dTime / 1e6}%8.1f ms
        |  4. GPU Parse+Extract: ${totalGpuTime / 1e6}%8.1f ms
        |  ─────────────────────────────────────────────────────
        |  Total:                ${totalTime / 1e6}%8.1f ms
        |  Pipeline efficiency:
        |    ${if (totalReadTime > 0) f"${readOverlapTime * 100.0 / totalReadTime}%.1f" else "N/A"}%% I/O overlap
        |""".stripMargin)
      // scalastyle:on println
      // scalastyle:on line.size.limit
    }

    // Add to global stats
    SeqFileGpuStats.addStats(totalReadTime, totalH2dTime, totalGpuTime, readOverlapTime,
      files.length, totalDataSize)

    allBatches.iterator
  }

  /**
   * Read files sequentially (single-threaded).
   */
  private def readFilesSequential(
      fileInfos: Array[FileInfo],
      fileOffsets: Array[Long],
      hostBuffer: HostMemoryBuffer): Unit = {
    for (i <- fileInfos.indices) {
      readSingleFile(fileInfos(i), fileOffsets(i), hostBuffer)
    }
  }

  /**
   * Read files in parallel using the shared thread pool.
   */
  private def readFilesParallel(
      fileInfos: Array[FileInfo],
      fileOffsets: Array[Long],
      hostBuffer: HostMemoryBuffer): Unit = {
    // Submit all file reading tasks to shared thread pool
    val futures: Array[JFuture[Unit]] = fileInfos.indices.map { i =>
      threadPool.submit(new Callable[Unit] {
        override def call(): Unit = {
          readSingleFile(fileInfos(i), fileOffsets(i), hostBuffer)
        }
      })
    }.toArray

    // Wait for all tasks to complete and propagate any exceptions
    futures.foreach { future =>
      try {
        future.get()
      } catch {
        case e: java.util.concurrent.ExecutionException =>
          throw e.getCause
      }
    }
  }

  /**
   * Read a single file's data into the host buffer at the specified offset.
   */
  private def readSingleFile(
      info: FileInfo,
      bufferStartOffset: Long,
      hostBuffer: HostMemoryBuffer): Unit = {
    val fs = info.path.getFileSystem(conf)
    val in = fs.open(info.path)
    try {
      in.seek(info.dataStart)
      // Read directly into pinned host buffer to avoid extra copy
      val channel = Channels.newChannel(in)
      var remaining = info.dataSize
      var bufferOffset = bufferStartOffset
      while (remaining > 0) {
        val toRead = math.min(remaining, readBufferSize).toInt
        val bb = hostBuffer.asByteBuffer(bufferOffset, toRead)
        var bytesReadTotal = 0
        while (bytesReadTotal < toRead) {
          val bytesRead = channel.read(bb)
          if (bytesRead < 0) {
            throw new java.io.IOException(
              s"Unexpected end of file at ${info.path}, expected ${info.dataSize} bytes")
          }
          bytesReadTotal += bytesRead
        }
        bufferOffset += toRead
        remaining -= toRead
      }
    } finally {
      in.close()
    }
  }

  /**
   * Create batches for count-only queries using CPU counting.
   */
  private def createCountOnlyBatches(fileInfos: Array[FileInfo]): Iterator[ColumnarBatch] = {
    fileInfos.iterator.map { info =>
      // Count records on CPU
      val numRows =
        countRecordsOnCpu(info.path, conf, info.dataStart, info.dataStart + info.dataSize)

      // Create null columns for the required schema
      val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
        GpuColumnVector.fromNull(numRows, f.dataType)
      }

      // Add partition values if needed
      val batch = new ColumnarBatch(cols, numRows)
      addPartitionValues(batch, info.partFile, numRows)
    }
  }

  /**
   * Count records using CPU-based Hadoop SequenceFile.Reader.
   */
  private def countRecordsOnCpu(filePath: Path,
                                hadoopConf: Configuration,
                                start: Long,
                                end: Long): Int = {
    import org.apache.hadoop.io.SequenceFile.{Reader => HadoopSeqReader}
    import org.apache.hadoop.io.BytesWritable

    var count = 0
    val reader = new HadoopSeqReader(hadoopConf, HadoopSeqReader.file(filePath))
    try {
      val key = new BytesWritable()
      val value = new BytesWritable()
      if (start > 0) {
        reader.sync(start - 1)
      }
      // Check position BEFORE reading, and handle EOF gracefully
      var continue = true
      while (continue && reader.getPosition < end) {
        try {
          if (reader.next(key, value)) {
            count += 1
          } else {
            continue = false
          }
        } catch {
          case _: java.io.EOFException =>
            // EOF reached - this can happen at split boundaries
            continue = false
        }
      }
    } finally {
      reader.close()
    }
    count
  }

  /**
   * Create batches from the multi-file parse result.
   */
  private def createBatchesFromResult(
      result: SequenceFile.MultiFileParseResult,
      fileInfos: Array[FileInfo]): Iterator[ColumnarBatch] = {

    val keyColumn = result.getKeyColumn
    val valueColumn = result.getValueColumn
    val fileRowCounts = result.getFileRowCounts

    if (result.getTotalRows == 0) {
      return Iterator.empty
    }

    // Slice columns by file and create batches
    var rowOffset = 0
    val batches = fileInfos.indices.map { i =>
      val numRows = fileRowCounts(i)
      if (numRows == 0) {
        // Empty batch for this file
        val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
          GpuColumnVector.fromNull(0, f.dataType)
        }
        new ColumnarBatch(cols, 0)
      } else {
        // Slice the columns for this file
        val slicedKey = if (wantsKey && keyColumn != null) {
          Some(keyColumn.subVector(rowOffset, rowOffset + numRows))
        } else None

        val slicedValue = if (wantsValue && valueColumn != null) {
          Some(valueColumn.subVector(rowOffset, rowOffset + numRows))
        } else None

        rowOffset += numRows

        // Build the batch with correct column order based on schema
        closeOnExcept(slicedKey) { _ =>
          closeOnExcept(slicedValue) { _ =>
            val cols: Array[SparkVector] = requiredSchema.fields.map { f =>
              if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.KEY_FIELD)) {
                slicedKey match {
                  case Some(col) => GpuColumnVector.from(col.incRefCount(), BinaryType)
                  case None => GpuColumnVector.fromNull(numRows, f.dataType)
                }
              } else if (f.name.equalsIgnoreCase(SequenceFileBinaryFileFormat.VALUE_FIELD)) {
                slicedValue match {
                  case Some(col) => GpuColumnVector.from(col.incRefCount(), BinaryType)
                  case None => GpuColumnVector.fromNull(numRows, f.dataType)
                }
              } else {
                GpuColumnVector.fromNull(numRows, f.dataType)
              }
            }

            closeOnExcept(cols) { _ =>
              slicedKey.foreach(_.close())
              slicedValue.foreach(_.close())
              val batch = new ColumnarBatch(cols, numRows)
              addPartitionValues(batch, fileInfos(i).partFile, numRows)
            }
          }
        }
      }
    }

    batches.iterator
  }

  /**
   * Add partition column values to a batch.
   */
  private def addPartitionValues(
      batch: ColumnarBatch,
      partFile: PartitionedFile,
      numRows: Int): ColumnarBatch = {
    if (partitionSchema.isEmpty) {
      batch
    } else {
      // For now, just return the batch as-is
      // Full partition value handling would require more complex column manipulation
      batch
    }
  }

  /**
   * File information collected during the read phase.
   */
  private case class FileInfo(
      partFile: PartitionedFile,
      path: Path,
      header: SequenceFileHeader,
      dataStart: Long,
      dataSize: Long)

  /**
   * Host batch data containing pre-read file data ready for H2D transfer.
   */
  private case class HostBatchData(
      fileInfos: Array[FileInfo],
      hostBuffer: HostMemoryBuffer,
      fileOffsets: Array[Long],
      fileSizes: Array[Long],
      syncMarkers: Array[Array[Byte]],
      totalDataSize: Long,
      readTimeNs: Long)

  /**
   * Divide files into batches based on batchSizeBytes.
   * Always divides into batches to avoid memory issues with large datasets,
   * regardless of whether pipelining is enabled.
   */
  private def divideIntoBatches(fileInfos: Array[FileInfo]): Array[Array[FileInfo]] = {
    if (fileInfos.length <= 1) {
      return Array(fileInfos)
    }

    val batches = scala.collection.mutable.ArrayBuffer[Array[FileInfo]]()
    var currentBatch = scala.collection.mutable.ArrayBuffer[FileInfo]()
    var currentBatchSize = 0L

    for (info <- fileInfos) {
      if (currentBatchSize + info.dataSize > batchSizeBytes && currentBatch.nonEmpty) {
        batches += currentBatch.toArray
        currentBatch = scala.collection.mutable.ArrayBuffer[FileInfo]()
        currentBatchSize = 0L
      }
      currentBatch += info
      currentBatchSize += info.dataSize
    }

    if (currentBatch.nonEmpty) {
      batches += currentBatch.toArray
    }

    batches.toArray
  }

  /**
   * Read a batch of files to host memory asynchronously.
   * Returns a Future that completes when all files are read.
   */
  private def readBatchToHostAsync(batchInfos: Array[FileInfo]): JFuture[HostBatchData] = {
    threadPool.submit(new Callable[HostBatchData] {
      override def call(): HostBatchData = readBatchToHost(batchInfos)
    })
  }

  /**
   * Read a batch of files to host memory synchronously.
   */
  private def readBatchToHost(batchInfos: Array[FileInfo]): HostBatchData = {
    val startTime = System.nanoTime()

    val totalDataSize = batchInfos.map(_.dataSize).sum
    val fileOffsets = new Array[Long](batchInfos.length)
    val fileSizes = new Array[Long](batchInfos.length)
    val syncMarkers = new Array[Array[Byte]](batchInfos.length)

    // Calculate offsets
    var currentOffset = 0L
    for (i <- batchInfos.indices) {
      fileOffsets(i) = currentOffset
      fileSizes(i) = batchInfos(i).dataSize
      syncMarkers(i) = batchInfos(i).header.syncMarker
      currentOffset += fileSizes(i)
    }

    // Read files to host buffer
    val hostBuffer = closeOnExcept(HostAlloc.alloc(totalDataSize, preferPinned = true)) { hb =>
      if (threadPoolConf.maxThreadNumber > 1 && batchInfos.length > 1) {
        readFilesParallel(batchInfos, fileOffsets, hb)
      } else {
        readFilesSequential(batchInfos, fileOffsets, hb)
      }
      hb
    }

    val readTime = System.nanoTime() - startTime
    HostBatchData(
      batchInfos,
      hostBuffer,
      fileOffsets,
      fileSizes,
      syncMarkers,
      totalDataSize,
      readTime)
  }

  /**
   * Process a batch on GPU and return output batches.
   */
  private def processBatchOnGpu(hostData: HostBatchData): (Iterator[ColumnarBatch], Long, Long) = {
    // H2D transfer
    val h2dStartTime = System.nanoTime()
    val deviceBuffer = withResource(hostData.hostBuffer) { hb =>
      val db = DeviceMemoryBuffer.allocate(hostData.totalDataSize)
      closeOnExcept(db) { _ =>
        db.copyFromHostBuffer(hb)
      }
      db
    }
    val h2dTime = System.nanoTime() - h2dStartTime

    // Parse on GPU
    GpuSemaphore.acquireIfNecessary(TaskContext.get())

    val gpuStartTime = System.nanoTime()
    val parseResult = decodeMetric.ns {
      withResource(deviceBuffer) { devBuf =>
        SequenceFile.parseMultipleFiles(
          devBuf,
          hostData.fileOffsets,
          hostData.fileSizes,
          hostData.syncMarkers,
          wantsKey,
          wantsValue)
      }
    }
    val gpuTime = System.nanoTime() - gpuStartTime

    // Build output batches
    val batches = withResource(parseResult) { result =>
      createBatchesFromResult(result, hostData.fileInfos)
    }

    (batches, h2dTime, gpuTime)
  }

  private def computeSplitDataRange(partFile: PartitionedFile,
                                    path: Path,
                                    header: SequenceFileHeader,
                                    fileSize: Long): (Long, Long) = {
    val splitStart = partFile.start
    val splitEnd = math.min(partFile.start + partFile.length, fileSize)
    val headerEnd = header.headerSize.toLong
    if (splitEnd <= headerEnd) {
      return (headerEnd, 0L)
    }

    var dataStart = math.max(splitStart, headerEnd)
    if (dataStart > headerEnd) {
      val reader = new HadoopSeqReader(conf, HadoopSeqReader.file(path))
      try {
        reader.sync(dataStart - 1)
        dataStart = reader.getPosition
      } finally {
        reader.close()
      }
    }

    val dataEnd = splitEnd
    val dataSize = math.max(0L, dataEnd - dataStart)
    (dataStart, dataSize)
  }
}
