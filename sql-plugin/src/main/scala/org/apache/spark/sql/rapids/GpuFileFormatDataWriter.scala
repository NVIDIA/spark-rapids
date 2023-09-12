/*
 * Copyright (c) 2019-2023, NVIDIA CORPORATION.
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

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

import ai.rapids.cudf.{ColumnVector, OrderByArg, Table}
import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.GpuFileFormatDataWriterShim
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Cast, Concat, Expression, Literal, NullsFirst, ScalaUDF, SortOrder, UnsafeProjection}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.{BucketingUtils, PartitioningUtils, WriteTaskResult}
import org.apache.spark.sql.rapids.GpuFileFormatDataWriter.{shouldSplitToFitMaxRecordsPerFile, splitToFitMaxRecordsAndClose}
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

object GpuFileFormatDataWriter {
  private def ceilingDiv(num: Long, divisor: Long) = {
    ((num + divisor - 1) / divisor).toInt
  }

  def shouldSplitToFitMaxRecordsPerFile(
      maxRecordsPerFile: Long, recordsInFile: Long, numRowsInBatch: Long) = {
    maxRecordsPerFile > 0 && (recordsInFile + numRowsInBatch) > maxRecordsPerFile
  }

  private def getSplitIndexes(maxRecordsPerFile: Long, recordsInFile: Long, numRows: Long) = {
    (maxRecordsPerFile > 0, recordsInFile < maxRecordsPerFile) match {
      case (false, _) => IndexedSeq.empty
      case (true, false) =>
        (1 until ceilingDiv(numRows, maxRecordsPerFile))
            .map(i => (i * maxRecordsPerFile).toInt)
      case (true, true) => {
        val filledUp = maxRecordsPerFile - recordsInFile
        val remain = numRows - filledUp
        (0 until ceilingDiv(remain, maxRecordsPerFile))
            .map(i => (filledUp + i * maxRecordsPerFile).toInt)
      }
    }
  }

  /**
   * Split a table into parts if recordsInFile + batch row count would go above
   * maxRecordsPerFile and make the splits spillable.
   *
   * The logic to find out what the splits should be is delegated to getSplitIndexes.
   *
   * The input batch is closed in case of error or in case we have to split it.
   * It is not closed if it wasn't split.
   *
   * @param batch ColumnarBatch to split (and close)
   * @param maxRecordsPerFile max rowcount per file
   * @param recordsInFile row count in the file so far
   * @return array of SpillableColumnarBatch splits
   */
  def splitToFitMaxRecordsAndClose(
      batch: ColumnarBatch,
      maxRecordsPerFile: Long,
      recordsInFile: Long): Array[SpillableColumnarBatch] = {
    val (types, splitIndexes) = closeOnExcept(batch) { _ =>
      val types = GpuColumnVector.extractTypes(batch)
      val splitIndexes =
        getSplitIndexes(
          maxRecordsPerFile,
          recordsInFile,
          batch.numRows())
      (types, splitIndexes)
    }
    if (splitIndexes.isEmpty) {
      // this should never happen, as `splitToFitMaxRecordsAndClose` is called when
      // splits should already happen, but making it more efficient in that case
      Array(SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    } else {
      // actually split it
      val tbl = withResource(batch) { _ =>
        GpuColumnVector.from(batch)
      }
      val cts = withResource(tbl) { _ =>
        tbl.contiguousSplit(splitIndexes: _*)
      }
      withResource(cts) { _ =>
        cts.safeMap(ct =>
          SpillableColumnarBatch(ct, types, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      }
    }
  }
}

/**
 * Abstract class for writing out data in a single Spark task using the GPU.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.FileFormatDataWriter`.
 */
abstract class GpuFileFormatDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol) extends DataWriter[ColumnarBatch] {
  /**
   * Max number of files a single task writes out due to file size. In most cases the number of
   * files written should be very small. This is just a safe guard to protect some really bad
   * settings, e.g. maxRecordsPerFile = 1.
   */
  protected val MAX_FILE_COUNTER: Int = 1000 * 1000
  protected val updatedPartitions: mutable.Set[String] = mutable.Set[String]()
  protected var currentWriter: ColumnarOutputWriter = _

  /** Trackers for computing various statistics on the data as it's being written out. */
  protected val statsTrackers: Seq[ColumnarWriteTaskStatsTracker] =
    description.statsTrackers.map(_.newTaskInstance())

  /** Release resources of `currentWriter`. */
  protected def releaseCurrentWriter(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
        statsTrackers.foreach(_.closeFile(currentWriter.path()))
      } finally {
        currentWriter = null
      }
    }
  }

  /** Release all resources. Public for testing */
  def releaseResources(): Unit = {
    // Call `releaseCurrentWriter()` by default, as this is the only resource to be released.
    releaseCurrentWriter()
  }

  /** Write an iterator of column batch. */
  def writeWithIterator(iterator: Iterator[ColumnarBatch]): Unit = {
    while (iterator.hasNext) {
      write(iterator.next())
    }
  }

  /** Writes a columnar batch of records */
  def write(batch: ColumnarBatch): Unit

  /**
   * Returns the summary of relative information which
   * includes the list of partition strings written out. The list of partitions is sent back
   * to the driver and used to update the catalog. Other information will be sent back to the
   * driver too and used to e.g. update the metrics in UI.
   */
  override def commit(): WriteTaskResult = {
    releaseResources()
    val (taskCommitMessage, taskCommitTime) = TimingUtils.timeTakenMs {
      committer.commitTask(taskAttemptContext)
    }
    val summary = GpuFileFormatDataWriterShim.createWriteSummary(
      updatedPartitions = updatedPartitions.toSet,
      stats = statsTrackers.map(_.getFinalStats(taskCommitTime))
    )
    WriteTaskResult(taskCommitMessage, summary)
  }

  override def abort(): Unit = {
    try {
      releaseResources()
    } finally {
      committer.abortTask(taskAttemptContext)
    }
  }

  override def close(): Unit = {}
}

/** GPU data writer for empty partitions */
class GpuEmptyDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol
) extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  override def write(batch: ColumnarBatch): Unit = {
    batch.close()
  }
}

/** Writes data to a single directory (used for non-dynamic-partition writes). */
class GpuSingleDirectoryDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {
  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  // Initialize currentWriter and statsTrackers
  newOutputWriter()

  @scala.annotation.nowarn(
    "msg=method newTaskTempFile in class FileCommitProtocol is deprecated"
  )
  private def newOutputWriter(): Unit = {
    recordsInFile = 0
    releaseResources()

    val ext = description.outputWriterFactory.getFileExtension(taskAttemptContext)
    val currentPath = committer.newTaskTempFile(
      taskAttemptContext,
      None,
      f"-c$fileCounter%03d" + ext)

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  private def writeUpdateMetricsAndClose(scb: SpillableColumnarBatch): Unit = {
    recordsInFile += currentWriter.writeSpillableAndClose(scb, statsTrackers)
  }

  override def write(batch: ColumnarBatch): Unit = {
    val maxRecordsPerFile = description.maxRecordsPerFile
    if (!shouldSplitToFitMaxRecordsPerFile(
        maxRecordsPerFile, recordsInFile, batch.numRows())) {
      writeUpdateMetricsAndClose(
        SpillableColumnarBatch(batch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    } else {
      val partBatches = splitToFitMaxRecordsAndClose(
        batch, maxRecordsPerFile, recordsInFile)
      var needNewWriter = recordsInFile >= maxRecordsPerFile
      closeOnExcept(partBatches) { _ =>
        partBatches.zipWithIndex.foreach { case (partBatch, partIx) =>
          if (needNewWriter) {
            fileCounter += 1
            assert(fileCounter <= MAX_FILE_COUNTER,
              s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")
            newOutputWriter()
          }
          // null out the entry so that we don't double close
          partBatches(partIx) = null
          writeUpdateMetricsAndClose(partBatch)
          needNewWriter = true
        }
      }
    }
  }
}

/**
 * Dynamic partition writer with single writer, meaning only one writer is opened at any time
 * for writing, meaning this single function can write to multiple directories (partitions)
 * or files (bucketing). The data to be written are required to be sorted on partition and/or
 * bucket column(s) before writing.
 */
class GpuDynamicPartitionDataSingleWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {

  /** Wrapper class for status of a unique single output writer. */
  protected class WriterStatus(
      // output writer
      var outputWriter: ColumnarOutputWriter,

      /** Number of records in current file. */
      var recordsInFile: Long = 0,

      /**
       * File counter for writing current partition or bucket. For same partition or bucket,
       * we may have more than one file, due to number of records limit per file.
       */
      var fileCounter: Int = 0
  )

  /** Wrapper class for status and caches of a unique concurrent output writer.
   * Used by `GpuDynamicPartitionDataConcurrentWriter`
   */
  class WriterStatusWithCaches(
      // writer status
      var writerStatus: WriterStatus,

      // caches for this partition or writer
      val tableCaches: ListBuffer[SpillableColumnarBatch] = ListBuffer(),

      // current device bytes for the above caches
      var deviceBytes: Long = 0
  )

  /** Flag saying whether or not the data to be written out is partitioned. */
  protected val isPartitioned: Boolean = description.partitionColumns.nonEmpty

  /** Flag saying whether or not the data to be written out is bucketed. */
  protected val isBucketed: Boolean = description.bucketSpec.isDefined

  private var currentPartPath: String = ""

  private var currentWriterStatus: WriterStatus = _

  // All data is sorted ascending with default null ordering
  private val nullsSmallest = Ascending.defaultNullOrdering == NullsFirst

  if (isBucketed) {
    throw new UnsupportedOperationException("Bucketing is not supported on the GPU yet.")
  }

  assert(isPartitioned || isBucketed,
    s"""GpuDynamicPartitionWriteTask should be used for writing out data that's either
       |partitioned or bucketed. In this case neither is true.
       |GpuWriteJobDescription: $description
     """.stripMargin)

  /** Extracts the partition values out of an input batch. */
  protected lazy val getPartitionColumnsAsBatch: ColumnarBatch => ColumnarBatch = {
    val expressions = GpuBindReferences.bindGpuReferences(
      description.partitionColumns,
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  /** Extracts the output values of an input batch. */
  private lazy val getOutputColumnsAsBatch: ColumnarBatch => ColumnarBatch= {
    val expressions = GpuBindReferences.bindGpuReferences(
      description.dataColumns,
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  /** Extracts the output values of an input batch. */
  protected lazy val getOutputCb: ColumnarBatch => ColumnarBatch = {
    val expressions = GpuBindReferences.bindGpuReferences(
      description.dataColumns,
      description.allColumns)
    cb => {
      GpuProjectExec.project(cb, expressions)
    }
  }

  /**
   * Expression that given partition columns builds a path string like: col1=val/col2=val/...
   * This is used after we pull the unique partition values back to the host.
   */
  private lazy val partitionPathExpression: Expression = Concat(
    description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ScalaUDF(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  /** Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
   * the partition string.
   */
  protected lazy val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression), description.partitionColumns)
    row => proj(row).getString(0)
  }

  /** Release resources of writer. */
  private def releaseWriter(writer: ColumnarOutputWriter): Unit = {
    if (writer != null) {
      val path = writer.path()
      writer.close()
      statsTrackers.foreach(_.closeFile(path))
    }
  }

  /**
   * Opens a new OutputWriter given a partition key and/or a bucket id.
   * If bucket id is specified, we will append it to the end of the file name, but before the
   * file extension, e.g. part-r-00009-ea518ad4-455a-4431-b471-d24e03814677-00002.gz.parquet
   *
   * @param partDir the partition directory
   * @param bucketId the bucket which all tuples being written by this OutputWriter belong to,
   *                 currently does not support `bucketId`, it's always None
   * @param fileCounter integer indicating the number of files to be written to `partDir`
   */
  @scala.annotation.nowarn(
    "msg=method newTaskTempFile.* in class FileCommitProtocol is deprecated"
  )
  def newWriter(
      partDir: String,
      bucketId: Option[Int], // Currently it's always None
      fileCounter: Int
  ): ColumnarOutputWriter = {
    updatedPartitions.add(partDir)
    // Currently will be empty
    val bucketIdStr = bucketId.map(BucketingUtils.bucketIdToString).getOrElse("")

    // This must be in a form that matches our bucketing format. See BucketingUtils.
    val ext = f"$bucketIdStr.c$fileCounter%03d" +
        description.outputWriterFactory.getFileExtension(taskAttemptContext)

    val customPath = description.customPartitionLocations
        .get(PartitioningUtils.parsePathFragment(partDir))

    val currentPath = if (customPath.isDefined) {
      committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
    } else {
      committer.newTaskTempFile(taskAttemptContext, Option(partDir), ext)
    }

    val newWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
    newWriter
  }

  // distinct value sorted the same way the input data is sorted.
  private def distinctAndSort(t: Table): Table = {
    val columnIds = 0 until t.getNumberOfColumns
    withResource(t.groupBy(columnIds: _*).aggregate()) { distinct =>
      distinct.orderBy(columnIds.map(OrderByArg.asc(_, nullsSmallest)): _*)
    }
  }

  // Get the split indexes for t given the keys we want to split on
  private def splitIndexes(t: Table, keys: Table): Array[Int] = {
    val nullsSmallestArray = Array.fill[Boolean](t.getNumberOfColumns)(nullsSmallest)
    val desc = Array.fill[Boolean](t.getNumberOfColumns)(false)
    withResource(t.upperBound(nullsSmallestArray, keys, desc)) { cv =>
      GpuColumnVector.toIntArray(cv)
    }
  }

  // Convert a table to a ColumnarBatch on the host, so we can iterate through it.
  protected def copyToHostAsBatch(input: Table, colTypes: Array[DataType]): ColumnarBatch = {
    withResource(GpuColumnVector.from(input, colTypes)) { tmp =>
      new ColumnarBatch(GpuColumnVector.extractColumns(tmp).safeMap(_.copyToHost()), tmp.numRows())
    }
  }

  override def write(batch: ColumnarBatch): Unit = {
    // this single writer always passes `cachesMap` as None
    write(batch, cachesMap = None)
  }

  private case class SplitAndPath(var split: SpillableColumnarBatch, path: String)
      extends AutoCloseable {
    override def close(): Unit = {
      split.safeClose()
      split = null
    }
  }

  /**
   * Split a batch according to the sorted keys (partitions). Returns a tuple with an
   * array of the splits as `ContiguousTable`'s, and an array of paths to use to
   * write each partition.
   */
  private def splitBatchByKeyAndClose(
      batch: ColumnarBatch,
      partDataTypes: Array[DataType]): Array[SplitAndPath] = {
    val (outputColumnsBatch, partitionColumnsBatch) = withResource(batch) { _ =>
      closeOnExcept(getOutputColumnsAsBatch(batch)) { outputColumnsBatch =>
        closeOnExcept(getPartitionColumnsAsBatch(batch)) { partitionColumnsBatch =>
          (outputColumnsBatch, partitionColumnsBatch)
        }
      }
    }
    val (cbKeys, partitionIndexes) = closeOnExcept(outputColumnsBatch) { _ =>
      val partitionColumnsTbl = withResource(partitionColumnsBatch) { _ =>
        GpuColumnVector.from(partitionColumnsBatch)
      }
      withResource(partitionColumnsTbl) { _ =>
        withResource(distinctAndSort(partitionColumnsTbl)) { distinctKeysTbl =>
          val partitionIndexes = splitIndexes(partitionColumnsTbl, distinctKeysTbl)
          val cbKeys = copyToHostAsBatch(distinctKeysTbl, partDataTypes)
          (cbKeys, partitionIndexes)
        }
      }
    }

    val splits = closeOnExcept(cbKeys) { _ =>
      val spillableOutputColumnsBatch =
        SpillableColumnarBatch(outputColumnsBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      withRetryNoSplit(spillableOutputColumnsBatch) { spillable =>
        withResource(spillable.getColumnarBatch()) { outCb =>
          withResource(GpuColumnVector.from(outCb)) { outputColumnsTbl =>
            withResource(outputColumnsTbl) { _ =>
              outputColumnsTbl.contiguousSplit(partitionIndexes: _*)
            }
          }
        }
      }
    }

    val paths = closeOnExcept(splits) { _ =>
      withResource(cbKeys) { _ =>
        // Use the existing code to convert each row into a path. It would be nice to do this
        // on the GPU, but the data should be small and there are things we cannot easily
        // support on the GPU right now
        import scala.collection.JavaConverters._
        // paths
        cbKeys.rowIterator().asScala.map(getPartitionPath).toArray
      }
    }
    withResource(splits) { _ =>
      // NOTE: the `zip` here has the effect that will remove an extra `ContiguousTable`
      // added at the end of `splits` because we use `upperBound` to find the split points,
      // and the last split point is the number of rows.
      val outDataTypes = description.dataColumns.map(_.dataType).toArray
      splits.zip(paths).zipWithIndex.map { case ((split, path), ix) =>
        splits(ix) = null
        withResource(split) { _ =>
          SplitAndPath(
            SpillableColumnarBatch(
              split, outDataTypes, SpillPriorities.ACTIVE_BATCHING_PRIORITY),
            path)
        }
      }
    }
  }

  private def getBatchToWrite(
      partBatch: SpillableColumnarBatch,
      savedStatus: Option[WriterStatusWithCaches]): SpillableColumnarBatch = {
    val outDataTypes = description.dataColumns.map(_.dataType).toArray
    if (savedStatus.isDefined && savedStatus.get.tableCaches.nonEmpty) {
      // In the case where the concurrent partition writers fall back, we need to
      // incorporate into the current part any pieces that are already cached
      // in the `savedStatus`. Adding `partBatch` to what was saved could make a
      // concatenated batch with number of rows larger than `maxRecordsPerFile`,
      // so this concatenated result could be split later, which is not efficient. However,
      // the concurrent writers are default off in Spark, so it is not clear if this
      // code path is worth optimizing.
      val concat: Table =
        withResource(savedStatus.get.tableCaches) { subSpillableBatches =>
          val toConcat = subSpillableBatches :+ partBatch

          // clear the caches
          savedStatus.get.tableCaches.clear()

          withRetryNoSplit(toConcat.toSeq) { spillables =>
            withResource(spillables.safeMap(_.getColumnarBatch())) { batches =>
              withResource(batches.map(GpuColumnVector.from)) { subTables =>
                Table.concatenate(subTables: _*)
              }
            }
          }
        }
      withResource(concat) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(concat, outDataTypes),
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      }
    } else {
      partBatch
    }
  }

  /**
   * Write columnar batch.
   * If the `cachesMap` is not empty, this single writer should restore the writers and caches in
   * the `cachesMap`, this single writer should first combine the caches and current split data
   * for a specific partition before write.
   *
   * @param cb        the column batch
   * @param cachesMap used by `GpuDynamicPartitionDataConcurrentWriter` when fall back to single
   *                  writer, single writer should handle the stored writers and the pending caches
   */
  protected def write(
      batch: ColumnarBatch,
      cachesMap: Option[mutable.HashMap[String, WriterStatusWithCaches]]): Unit = {
    assert(isPartitioned)
    assert(!isBucketed)

    val maxRecordsPerFile = description.maxRecordsPerFile
    val partDataTypes = description.partitionColumns.map(_.dataType).toArray

    // We have an entire batch that is sorted, so we need to split it up by key
    // to get a batch per path
    withResource(splitBatchByKeyAndClose(batch, partDataTypes)) { splitsAndPaths =>
      splitsAndPaths.zipWithIndex.foreach { case (SplitAndPath(partBatch, partPath), ix) =>
        // If we fall back from `GpuDynamicPartitionDataConcurrentWriter`, we should get the
        // saved status
        val savedStatus = updateCurrentWriterIfNeeded(partPath, cachesMap)

        // combine `partBatch` with any remnants for this partition for the concurrent
        // writer fallback case in `savedStatus`
        splitsAndPaths(ix) = null
        val batchToWrite = getBatchToWrite(partBatch, savedStatus)

        // if the batch fits, write it as is, else split and write it.
        if (!shouldSplitToFitMaxRecordsPerFile(maxRecordsPerFile,
          currentWriterStatus.recordsInFile, batchToWrite.numRows())) {
          writeUpdateMetricsAndClose(currentWriterStatus, batchToWrite)
        } else {
          // materialize an actual batch since we are going to split it
          // on the GPU
          val batchToSplit = withRetryNoSplit(batchToWrite) { _ =>
            batchToWrite.getColumnarBatch()
          }
          val maxRecordsPerFileSplits = splitToFitMaxRecordsAndClose(
            batchToSplit,
            maxRecordsPerFile,
            currentWriterStatus.recordsInFile)
          writeSplitBatchesAndClose(maxRecordsPerFileSplits, maxRecordsPerFile, partPath)
        }
      }
    }
  }

  private def updateCurrentWriterIfNeeded(
    partPath: String,
    cachesMap: Option[mutable.HashMap[String, WriterStatusWithCaches]]):
  Option[WriterStatusWithCaches] = {
    var savedStatus: Option[WriterStatusWithCaches] = None
    if (currentPartPath != partPath) {
      val previousPartPath = currentPartPath
      currentPartPath = partPath

      // see a new partition, close the old writer
      val previousWriterStatus = currentWriterStatus
      if (previousWriterStatus != null) {
        releaseWriter(previousWriterStatus.outputWriter)
      }

      if (cachesMap.isDefined) {
        savedStatus = cachesMap.get.get(currentPartPath)
        if (savedStatus.isDefined) {
          // first try to restore the saved writer status,
          // `GpuDynamicPartitionDataConcurrentWriter` may already opened the writer, and may
          // have pending caches
          currentWriterStatus = savedStatus.get.writerStatus
          // entire batch that is sorted, see a new partition, the old write status is useless
          cachesMap.get.remove(previousPartPath)
        } else {
          // create a new one
          val writer = newWriter(partPath, None, 0)
          currentWriterStatus = new WriterStatus(writer)
          statsTrackers.foreach(_.newPartition())
        }
      } else {
        // create a new one
        val writer = newWriter(partPath, None, 0)
        currentWriterStatus = new WriterStatus(writer)
        statsTrackers.foreach(_.newPartition())
      }
    }
    savedStatus
  }

  /**
   * Write an array of spillable batches.
   *
   * Note: `spillableBatches` will be closed in this function.
   *
   * @param batches the SpillableColumnarBatch splits to be written
   * @param maxRecordsPerFile the max number of rows per file
   * @param partPath the partition directory
   */
  private def writeSplitBatchesAndClose(
      spillableBatches: Array[SpillableColumnarBatch],
      maxRecordsPerFile: Long,
      partPath: String): Unit = {
    var needNewWriter = currentWriterStatus.recordsInFile >= maxRecordsPerFile
    withResource(spillableBatches) { _ =>
      spillableBatches.zipWithIndex.foreach { case (part, partIx) =>
        if (needNewWriter) {
          currentWriterStatus.fileCounter += 1
          assert(currentWriterStatus.fileCounter <= MAX_FILE_COUNTER,
            s"File counter ${currentWriterStatus.fileCounter} " +
              s"is beyond max value $MAX_FILE_COUNTER")

          // will create a new file, close the old writer
          if (currentWriterStatus != null) {
            releaseWriter(currentWriterStatus.outputWriter)
          }

          // create a new writer and update the writer in the status
          currentWriterStatus.outputWriter =
            newWriter(partPath, None, currentWriterStatus.fileCounter)
          currentWriterStatus.recordsInFile = 0
        }
        spillableBatches(partIx) = null
        writeUpdateMetricsAndClose(currentWriterStatus, part)
        needNewWriter = true
      }
    }
  }

  protected def writeUpdateMetricsAndClose(
      writerStatus: WriterStatus,
      spillableBatch: SpillableColumnarBatch): Unit = {
    writerStatus.recordsInFile +=
        writerStatus.outputWriter.writeSpillableAndClose(spillableBatch, statsTrackers)
  }

  /** Release all resources. */
  override def releaseResources(): Unit = {
    // does not use `currentWriter`, single writer use `currentWriterStatus`
    assert(currentWriter == null)

    if (currentWriterStatus != null) {
      try {
        currentWriterStatus.outputWriter.close()
        statsTrackers.foreach(_.closeFile(currentWriterStatus.outputWriter.path()))
      } finally {
        currentWriterStatus = null
      }
    }
  }
}

/**
 * Dynamic partition writer with concurrent writers, meaning multiple concurrent writers are opened
 * for writing.
 *
 * The process has the following steps:
 *  - Step 1: Maintain a map of output writers per each partition columns. Keep all
 *    writers opened; Cache the inputted batches by splitting them into sub-groups and
 *    each partition holds a list of spillable sub-groups; Find and write the max pending
 *    partition data if the total caches exceed the limitation.
 *  - Step 2: If number of concurrent writers exceeds limit, fall back to sort-based write
 *    (`GpuDynamicPartitionDataSingleWriter`), sort rest of batches on partition.
 *    Write batch by batch, and eagerly close the writer when finishing
 *    Caller is expected to call `writeWithIterator()` instead of `write()` to write records.
 *    Note: when fall back to `GpuDynamicPartitionDataSingleWriter`, the single writer should
 *    restore un-closed writers and should handle un-flushed spillable caches.
 */
class GpuDynamicPartitionDataConcurrentWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol,
    spec: GpuConcurrentOutputWriterSpec,
    taskContext: TaskContext)
    extends GpuDynamicPartitionDataSingleWriter(description, taskAttemptContext, committer) {

  // Keep all the unclosed writers, key is partition directory string.
  // Note: if fall back to sort-based mode, also use the opened writers in the map.
  private val concurrentWriters = mutable.HashMap[String, WriterStatusWithCaches]()

  // guarantee to close the caches and writers when task is finished
  onTaskCompletion(taskContext)(closeCachesAndWriters())

  private val outDataTypes = description.dataColumns.map(_.dataType).toArray

  private val partitionFlushSize =
    if (description.concurrentWriterPartitionFlushSize <= 0) {
      // if the property is equal or less than 0, use default value given by the
      // writer factory
      description.outputWriterFactory.partitionFlushSize(taskAttemptContext)
    } else {
      // if the property is greater than 0, use the property value
      description.concurrentWriterPartitionFlushSize
    }

  // refer to current batch if should fall back to `single writer`
  private var currentFallbackColumnarBatch: ColumnarBatch = _

  override def abort(): Unit = {
    try {
      closeCachesAndWriters()
    } finally {
      committer.abortTask(taskAttemptContext)
    }
  }

  /**
   * State to indicate if we are falling back to sort-based writer.
   * Because we first try to use concurrent writers, its initial value is false.
   */
  private var fallBackToSortBased: Boolean = false

  private def writeWithSingleWriter(cb: ColumnarBatch): Unit = {
    // invoke `GpuDynamicPartitionDataSingleWriter`.write,
    // single writer will take care of the unclosed writers and the pending caches
    // in `concurrentWriters`
    super.write(cb, Some(concurrentWriters))
  }

  private def writeWithConcurrentWriter(cb: ColumnarBatch): Unit = {
    this.write(cb)
  }

  /**
   * Write an iterator of column batch.
   *
   * @param iterator the iterator of column batch
   */
  override def writeWithIterator(iterator: Iterator[ColumnarBatch]): Unit = {
    // 1: try concurrent writer
    while (iterator.hasNext && !fallBackToSortBased) {
      // concurrently write and update the `concurrentWriters` map
      // the `` will be updated
      writeWithConcurrentWriter(iterator.next())
    }

    // 2: fall back to single writer
    // Note single writer should restore writer status and handle the pending caches
    if (fallBackToSortBased) {
      // concat the put back batch and un-coming batches
      val newIterator = Iterator.single(currentFallbackColumnarBatch) ++ iterator
      // sort the all the batches in `iterator`

      val sortIterator: GpuOutOfCoreSortIterator = getSorted(newIterator)
      while (sortIterator.hasNext) {
        // write with sort-based single writer
        writeWithSingleWriter(sortIterator.next())
      }
    }
  }

  /**
   * Sort the input iterator by out of core sort
   *
   * @param iterator the input iterator
   * @return sorted iterator
   */
  private def getSorted(iterator: Iterator[ColumnarBatch]): GpuOutOfCoreSortIterator = {
    val gpuSortOrder: Seq[SortOrder] = spec.sortOrder
    val output: Seq[Attribute] = spec.output
    val sorter = new GpuSorter(gpuSortOrder, output)

    // use noop metrics below
    val sortTime = NoopMetric
    val opTime = NoopMetric
    val outputBatch = NoopMetric
    val outputRows = NoopMetric

    val targetSize = GpuSortExec.targetSize(spec.batchSize)
    // out of core sort the entire iterator
    GpuOutOfCoreSortIterator(iterator, sorter, targetSize,
      opTime, sortTime, outputBatch, outputRows)
  }

  /**
   * concurrent write the columnar batch
   * Note: if new partitions number in `cb` plus existing partitions number is greater than
   * `maxWriters` limit, will put back the whole `cb` to 'single writer`
   *
   * @param cb the columnar batch
   */
  override def write(cb: ColumnarBatch): Unit = {
    assert(isPartitioned)
    assert(!isBucketed)

    if (cb.numRows() == 0) {
      // TODO https://github.com/NVIDIA/spark-rapids/issues/6453
      // To solve above issue, I assume that an empty batch will be wrote for saving metadata.
      // If the assumption it's true, this concurrent writer should write the metadata here,
      // and should not run into below splitting and caching logic
      return
    }

    // 1. combine partition columns and `cb` columns into a column array
    val columnsWithPartition = ArrayBuffer[ColumnVector]()

    // this withResource is here to decrement the refcount of the partition columns
    // that are projected out of `cb`
    withResource(getPartitionColumnsAsBatch(cb)) { partitionColumnsBatch =>
      columnsWithPartition.appendAll(GpuColumnVector.extractBases(partitionColumnsBatch))
    }

    val cols = GpuColumnVector.extractBases(cb)
    columnsWithPartition ++= cols

    // 2. group by the partition columns
    // get sub-groups for each partition and get unique keys for each partition
    val groupsAndKeys = withResource(
        new Table(columnsWithPartition.toSeq: _*)) { colsWithPartitionTbl =>
      // [0, partition columns number - 1]
      val partitionIndices = description.partitionColumns.indices

      // group by partition columns
      val op = colsWithPartitionTbl.groupBy(partitionIndices: _*)
      // return groups and uniq keys table
      // Each row in uniq keys table is corresponding to a group
      op.contiguousSplitGroupsAndGenUniqKeys()
    }

    withResource(groupsAndKeys) { _ =>
      // groups number should equal to uniq keys number
      assert(groupsAndKeys.getGroups.length == groupsAndKeys.getUniqKeyTable.getRowCount)

      val (groups, keys) = (groupsAndKeys.getGroups, groupsAndKeys.getUniqKeyTable)

      // 3. generate partition strings for all sub-groups in advance
      val partDataTypes = description.partitionColumns.map(_.dataType).toArray
      val dataTypes = GpuColumnVector.extractTypes(cb)
      // generate partition string list for all groups
      val partitionStrList = getPartitionStrList(keys, partDataTypes)
      // key table is useless now
      groupsAndKeys.closeUniqKeyTable()

      // 4. cache each group according to each partitionStr
      withResource(groups) { _ =>

        // first update fallBackToSortBased
        withResource(cb) { _ =>
          var newPartitionNum = 0
          var groupIndex = 0
          while (!fallBackToSortBased && groupIndex < groups.length) {
            // get the partition string
            val partitionStr = partitionStrList(groupIndex)
            groupIndex += 1
            if (!concurrentWriters.contains(partitionStr)) {
              newPartitionNum += 1
              if (newPartitionNum + concurrentWriters.size >= spec.maxWriters) {
                fallBackToSortBased = true
                currentFallbackColumnarBatch = cb
                // `cb` should be put back to single writer
                GpuColumnVector.incRefCounts(cb)
              }
            }
          }
        }

        if (!fallBackToSortBased) {
          // not fall, collect all caches
          var groupIndex = 0
          while (groupIndex < groups.length) {
            // get the partition string and group pair
            val (partitionStr, group) = (partitionStrList(groupIndex), groups(groupIndex))
            val groupTable = group.getTable
            groupIndex += 1

            // create writer if encounter a new partition and put into `concurrentWriters` map
            if (!concurrentWriters.contains(partitionStr)) {
              val w = newWriter(partitionStr, None, 0)
              val ws = new WriterStatus(w)
              concurrentWriters.put(partitionStr, new WriterStatusWithCaches(ws))
              statsTrackers.foreach(_.newPartition())
            }

            // get data columns, tail part is data columns
            val dataColumns = ArrayBuffer[ColumnVector]()
            for (i <- description.partitionColumns.length until groupTable.getNumberOfColumns) {
              dataColumns += groupTable.getColumn(i)
            }
            withResource(new Table(dataColumns.toSeq: _*)) { dataTable =>
              withResource(GpuColumnVector.from(dataTable, dataTypes)) { cb =>
                val outputCb = getOutputCb(cb)
                // convert to spillable cache and add to the pending cache
                val currWriterStatus = concurrentWriters(partitionStr)
                // create SpillableColumnarBatch to take the owner of `outputCb`
                currWriterStatus.tableCaches += SpillableColumnarBatch(
                  outputCb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
                currWriterStatus.deviceBytes += GpuColumnVector.getTotalDeviceMemoryUsed(outputCb)
              }
            }
          }
        }
      }
    }

    // 5. find all big enough partitions and write
    if(!fallBackToSortBased) {
      for ((partitionDir, ws) <- findBigPartitions(partitionFlushSize)) {
        writeAndCloseCache(partitionDir, ws)
      }
    }
  }

  private def getPartitionStrList(
      uniqKeysTable: Table, partDataTypes: Array[DataType]): Array[String] = {
    withResource(copyToHostAsBatch(uniqKeysTable, partDataTypes)) { oneRowCb =>
      import scala.collection.JavaConverters._
      oneRowCb.rowIterator().asScala.map(getPartitionPath).toArray
    }
  }

  private def writeAndCloseCache(partitionDir: String, status: WriterStatusWithCaches): Unit = {
    assert(status.tableCaches.nonEmpty)

    // get concat table or the single table
    val spillableToWrite = if (status.tableCaches.length >= 2) {
      // concat the sub batches to write in once.
      val concatted = withRetryNoSplit(status.tableCaches.toSeq) { spillableSubBatches =>
        withResource(spillableSubBatches.safeMap(_.getColumnarBatch())) { subBatches =>
          withResource(subBatches.map(GpuColumnVector.from)) { subTables =>
            Table.concatenate(subTables: _*)
          }
        }
      }
      withResource(concatted) { _ =>
        SpillableColumnarBatch(
          GpuColumnVector.from(concatted, outDataTypes),
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
      }
    } else {
      // only one single table
      status.tableCaches.head
    }

    status.tableCaches.clear()

    val maxRecordsPerFile = description.maxRecordsPerFile
    if (!shouldSplitToFitMaxRecordsPerFile(
      maxRecordsPerFile, status.writerStatus.recordsInFile, spillableToWrite.numRows())) {
      writeUpdateMetricsAndClose(status.writerStatus, spillableToWrite)
    } else {
      val batchToSplit = withRetryNoSplit(spillableToWrite) { _ =>
        spillableToWrite.getColumnarBatch()
      }
      val splits = splitToFitMaxRecordsAndClose(
        batchToSplit,
        maxRecordsPerFile,
        status.writerStatus.recordsInFile)
      var needNewWriter = status.writerStatus.recordsInFile >= maxRecordsPerFile
      withResource(splits) { _ =>
        splits.zipWithIndex.foreach { case (split, partIndex) =>
          if (needNewWriter) {
            status.writerStatus.fileCounter += 1
            assert(status.writerStatus.fileCounter <= MAX_FILE_COUNTER,
              s"File counter ${status.writerStatus.fileCounter} " +
                s"is beyond max value $MAX_FILE_COUNTER")
            status.writerStatus.outputWriter.close()
            // start a new writer
            val w = newWriter(partitionDir, None, status.writerStatus.fileCounter)
            status.writerStatus.outputWriter = w
            status.writerStatus.recordsInFile = 0L
          }
          splits(partIndex) = null
          writeUpdateMetricsAndClose(status.writerStatus, split)
          needNewWriter = true
        }
      }
    }
    status.tableCaches.clear()
    status.deviceBytes = 0
  }

  def closeCachesAndWriters(): Unit = {
    // collect all caches and writers
    val allResources = ArrayBuffer[AutoCloseable]()
    allResources ++= concurrentWriters.values.flatMap(ws => ws.tableCaches)
    allResources ++= concurrentWriters.values.map { ws =>
      new AutoCloseable() {
        override def close(): Unit = {
          ws.writerStatus.outputWriter.close()
          statsTrackers.foreach(_.closeFile(ws.writerStatus.outputWriter.path()))
        }
      }
    }

    // safe close all the caches and writers
    allResources.safeClose()

    // clear `concurrentWriters` map
    concurrentWriters.values.foreach(ws => ws.tableCaches.clear())
    concurrentWriters.clear()
  }

  /** Release all resources. */
  override def releaseResources(): Unit = {
    // does not use `currentWriter`, only use the writers in the concurrent writer map
    assert(currentWriter == null)

    if (fallBackToSortBased) {
      // Note: we should close the last partition writer in the single writer.
      super.releaseResources()
    }

    // write all caches
    concurrentWriters.filter(pair => pair._2.tableCaches.nonEmpty)
        .foreach(pair => writeAndCloseCache(pair._1, pair._2))

    // close all resources
    closeCachesAndWriters()
  }

  private def findBigPartitions(
      sizeThreshold: Long): mutable.Map[String, WriterStatusWithCaches] = {
    concurrentWriters.filter(pair => pair._2.deviceBytes >= sizeThreshold)
  }
}

/**
 * Bucketing specification for all the write tasks.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.WriterBucketSpec`
 * @param bucketIdExpression Expression to calculate bucket id based on bucket column(s).
 * @param bucketFileNamePrefix Prefix of output file name based on bucket id.
 */
case class GpuWriterBucketSpec(
  bucketIdExpression: Expression,
  bucketFileNamePrefix: Int => String)

/**
 * A shared job description for all the GPU write tasks.
 * This is the GPU version of `org.apache.spark.sql.execution.datasources.WriteJobDescription`.
 */
class GpuWriteJobDescription(
    val uuid: String, // prevent collision between different (appending) write jobs
    val serializableHadoopConf: SerializableConfiguration,
    val outputWriterFactory: ColumnarOutputWriterFactory,
    val allColumns: Seq[Attribute],
    val dataColumns: Seq[Attribute],
    val partitionColumns: Seq[Attribute],
    val bucketSpec: Option[GpuWriterBucketSpec],
    val path: String,
    val customPartitionLocations: Map[TablePartitionSpec, String],
    val maxRecordsPerFile: Long,
    val timeZoneId: String,
    val statsTrackers: Seq[ColumnarWriteJobStatsTracker],
    val concurrentWriterPartitionFlushSize: Long)
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}