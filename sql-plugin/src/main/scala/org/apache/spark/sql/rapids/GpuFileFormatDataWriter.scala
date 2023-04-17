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

import ai.rapids.cudf.{ColumnVector, ContiguousTable, OrderByArg, Table}
import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Cast, Concat, Expression, Literal, NullsFirst, ScalaUDF, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.{BucketingUtils, ExecutedWriteSummary, PartitioningUtils, WriteTaskResult}
import org.apache.spark.sql.rapids.GpuFileFormatWriter.GpuConcurrentOutputWriterSpec
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

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

  /** Release all resources. */
  protected def releaseResources(): Unit = {
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
    val summary = ExecutedWriteSummary(
      updatedPartitions = updatedPartitions.toSet,
      stats = statsTrackers.map(_.getFinalStats(taskCommitTime)))
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
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) 
  with WriterUtil {
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

  override def write(batch: ColumnarBatch): Unit = {
    val maxRecordsPerFile = description.maxRecordsPerFile
    if (!needSplitBatch(maxRecordsPerFile, recordsInFile, batch.numRows())) {
      closeOnExcept(batch) { _ =>
        statsTrackers.foreach(_.newBatch(currentWriter.path(), batch))
        recordsInFile += batch.numRows()
      }
      currentWriter.writeAndClose(batch, statsTrackers)
    } else {
      withResource(batch) { batch =>
        withResource(GpuColumnVector.from(batch)) {table => 
          val splitIndexes = getSplitIndexes(
            maxRecordsPerFile,
            recordsInFile,
            table.getRowCount()
          )
  
          val dataTypes = GpuColumnVector.extractTypes(batch)
  
          var needNewWriter = recordsInFile >= maxRecordsPerFile
          withResource(table.contiguousSplit(splitIndexes: _*)) {tabs =>
            tabs.foreach(b => {
              if (needNewWriter) {
                fileCounter += 1
                assert(fileCounter <= MAX_FILE_COUNTER,
                  s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")
                newOutputWriter()
              }
              withResource(b.getTable()) {tab =>
                val bc = GpuColumnVector.from(tab, dataTypes)
                closeOnExcept(bc) { _ =>
                  statsTrackers.foreach(_.newBatch(currentWriter.path(), bc))
                  recordsInFile += b.getRowCount()
                }
                currentWriter.writeAndClose(bc, statsTrackers)
                needNewWriter = true
              }
            })
          }
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
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) 
  with WriterUtil{

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

  protected var currentPartPath: String = ""

  protected var currentWriterStatus: WriterStatus = _

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
  protected lazy val getPartitionColumns: ColumnarBatch => Table = {
    val expressions = GpuBindReferences.bindGpuReferences(
      description.partitionColumns,
      description.allColumns)
    cb => {
      val batch = GpuProjectExec.project(cb, expressions)
      try {
        GpuColumnVector.from(batch)
      } finally {
        batch.close()
      }
    }
  }

  /** Extracts the output values of an input batch. */
  protected lazy val getOutputColumns: ColumnarBatch => Table = {
    val expressions = GpuBindReferences.bindGpuReferences(
      description.dataColumns,
      description.allColumns)
    cb => {
      val batch = GpuProjectExec.project(cb, expressions)
      try {
        GpuColumnVector.from(batch)
      } finally {
        batch.close()
      }
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
  private [rapids] def releaseWriter(writer: ColumnarOutputWriter): Unit = {
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
   */
  @scala.annotation.nowarn(
    "msg=method newTaskTempFile.* in class FileCommitProtocol is deprecated"
  )
  protected def newWriter(
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
    val distinct = t.groupBy(columnIds: _*).aggregate()
    try {
      distinct.orderBy(columnIds.map(OrderByArg.asc(_, nullsSmallest)): _*)
    } finally {
      distinct.close()
    }
  }

  // Get the split indexes for t given the keys we want to split on
  private def splitIndexes(t: Table, keys: Table): Array[Int] = {
    val nullsSmallestArray = Array.fill[Boolean](t.getNumberOfColumns)(nullsSmallest)
    val desc = Array.fill[Boolean](t.getNumberOfColumns)(false)
    val cv = t.upperBound(nullsSmallestArray, keys, desc)
    try {
      GpuColumnVector.toIntArray(cv)
    } finally {
      cv.close()
    }
  }

  // Convert a table to a ColumnarBatch on the host, so we can iterate through it.
  protected def copyToHostAsBatch(input: Table, colTypes: Array[DataType]): ColumnarBatch = {
    val tmp = GpuColumnVector.from(input, colTypes)
    try {
      new ColumnarBatch(GpuColumnVector.extractColumns(tmp).map(_.copyToHost()), tmp.numRows())
    } finally {
      tmp.close()
    }
  }

  override def write(cb: ColumnarBatch): Unit = {
    // this single writer always passes `cachesMap` as None
    write(cb, cachesMap = None)
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
  def write(cb: ColumnarBatch,
      cachesMap: Option[mutable.HashMap[String, WriterStatusWithCaches]]): Unit = {
    assert(isPartitioned)
    assert(!isBucketed)

    // We have an entire batch that is sorted, so we need to split it up by key
    var needToCloseBatch = true
    var partitionColumns: Table = null
    var distinctKeys: Table = null
    var outputColumns: Table = null
    var splits: Array[ContiguousTable] = null
    var cbKeys: ColumnarBatch = null
    val maxRecordsPerFile = description.maxRecordsPerFile

    try {
      partitionColumns = getPartitionColumns(cb)
      val partDataTypes = description.partitionColumns.map(_.dataType).toArray
      distinctKeys = distinctAndSort(partitionColumns)
      val partitionIndexes = splitIndexes(partitionColumns, distinctKeys)
      partitionColumns.close()
      partitionColumns = null

      // split the original data on the indexes
      outputColumns = getOutputColumns(cb)
      val outDataTypes = description.dataColumns.map(_.dataType).toArray
      cb.close()
      needToCloseBatch = false
      splits = outputColumns.contiguousSplit(partitionIndexes: _*)
      outputColumns.close()
      outputColumns = null

      cbKeys = copyToHostAsBatch(distinctKeys, partDataTypes)
      distinctKeys.close()
      distinctKeys = null

      // Use the existing code to convert each row into a path. It would be nice to do this on
      // the GPU, but the data should be small and there are things we cannot easily support
      // on the GPU right now
      import scala.collection.JavaConverters._
      val paths = cbKeys.rowIterator().asScala.map(getPartitionPath)

      paths.toArray.zip(splits).foreach(combined => {
        val table = combined._2.getTable
        val partPath = combined._1

        // If fall back from for `GpuDynamicPartitionDataConcurrentWriter`, we should get the
        // saved status
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

        if (savedStatus.isDefined && savedStatus.get.tableCaches.nonEmpty) {
          // convert caches seq to tables and close caches seq
          val subTables = convertSpillBatchesToTables(savedStatus.get.tableCaches)
          // concat the caches and this `table`
          val concat = withResource(subTables) { _ =>
            // clear the caches
            savedStatus.get.tableCaches.clear()
            subTables += table
            Table.concatenate(subTables: _*)
          }
          // write concat table
          if (!needSplitBatch(
            maxRecordsPerFile,
            currentWriterStatus.recordsInFile,
            concat.getRowCount()))
          {
            val batch  = GpuColumnVector.from(concat, outDataTypes)
            closeOnExcept(batch) { _ =>
              statsTrackers.foreach(_.newBatch(currentWriterStatus.outputWriter.path(), batch))
              currentWriterStatus.recordsInFile += batch.numRows()
            }
            currentWriterStatus.outputWriter.writeAndClose(batch, statsTrackers)
          } else {
            writeTableAndClose(concat, outDataTypes,  maxRecordsPerFile, partPath)
          }
        } else {
          if (!needSplitBatch(
            maxRecordsPerFile,
            currentWriterStatus.recordsInFile,
            table.getRowCount()))
          {
            val batch = GpuColumnVector.from(table, outDataTypes)
            closeOnExcept(batch) { _ =>
              statsTrackers.foreach(_.newBatch(currentWriterStatus.outputWriter.path(), batch))
              currentWriterStatus.recordsInFile += batch.numRows()
            }
            currentWriterStatus.outputWriter.writeAndClose(batch, statsTrackers)
          } else {
            writeTableAndClose(table, outDataTypes, maxRecordsPerFile, partPath)
          }
        }
      })
    } finally {
      if (needToCloseBatch) {
        cb.close()
      }

      if (partitionColumns != null) {
        partitionColumns.close()
      }

      if (distinctKeys != null) {
        distinctKeys.close()
      }

      if (outputColumns != null) {
        outputColumns.close()
      }

      splits.safeClose()

      if (cbKeys != null) {
        cbKeys.close()
      }
    }
  }

  /**
   * Write a Table.
   *
   * Note: The `table` will be closed in this function.
   *
   * @param table the table to be written
   * @param outDataTypes the data types of the table
   * @param maxRecordsPerFile the max number of rows per file
   * @param partPath the partition directory
   */
  private def writeTableAndClose(
    table: Table, outDataTypes: Array[DataType], maxRecordsPerFile: Long, partPath: String) = {
    val splitIndexes = getSplitIndexes(
      maxRecordsPerFile,
      currentWriterStatus.recordsInFile,
      table.getRowCount()
    )
    var needNewWriter = currentWriterStatus.recordsInFile >= maxRecordsPerFile

    val tabs = withResource(table) { _ =>
      table.contiguousSplit(splitIndexes: _*)
    }
    withResource(tabs) { _ =>
      tabs.foreach(b => {
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
        val bc = GpuColumnVector.from(b.getTable(), outDataTypes)
        closeOnExcept(bc) { _ =>
          statsTrackers.foreach(_.newBatch(currentWriterStatus.outputWriter.path(), bc))
          currentWriterStatus.recordsInFile += b.getRowCount()
        }
        currentWriterStatus.outputWriter.writeAndClose(bc, statsTrackers)
        needNewWriter = true
      })
    }
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

  /**
   * convert spillable columnar batch seq to tables and close the input `spills`
   *
   * @param spills spillable columnar batch seq
   * @return table array
   */
  def convertSpillBatchesToTables(spills: Seq[SpillableColumnarBatch]): ArrayBuffer[Table] = {
    withResource(spills) { _ =>
      val subTablesBuffer = new ArrayBuffer[Table]
      spills.foreach { spillableCb =>
        withResource(spillableCb.getColumnarBatch()) { cb =>
          val currTable = GpuColumnVector.from(cb)
          subTablesBuffer += currTable
        }
      }
      subTablesBuffer
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
    spec: GpuConcurrentOutputWriterSpec)
    extends GpuDynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
        with Logging {

  // Keep all the unclosed writers, key is partition directory string.
  // Note: if fall back to sort-based mode, also use the opened writers in the map.
  private val concurrentWriters = mutable.HashMap[String, WriterStatusWithCaches]()

  // guarantee to close the caches and writers when task is finished
  TaskContext.get().addTaskCompletionListener[Unit](_ => closeCachesAndWriters())

  private val outDataTypes = description.dataColumns.map(_.dataType).toArray

  val partitionFlushSize = if (description.concurrentWriterPartitionFlushSize <= 0) {
    // if the property is equal or less than 0, use default value of parquet or orc
    val extension = description.outputWriterFactory
        .getFileExtension(taskAttemptContext).toLowerCase()
    if (extension.endsWith("parquet")) {
      taskAttemptContext.getConfiguration.getLong("write.parquet.row-group-size-bytes",
        128L * 1024L * 1024L) // 128M
    } else if (extension.endsWith("orc")) {
      taskAttemptContext.getConfiguration.getLong("orc.stripe.size",
        64L * 1024L * 1024L) // 64M
    } else {
      128L * 1024L * 1024L // 128M
    }
  } else {
    // if the property is greater than 0, use the property value
    description.concurrentWriterPartitionFlushSize
  }

  // refer to current batch if should fall back to `single writer`
  var currentFallbackColumnarBatch: ColumnarBatch = _

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

  def writeWithSingleWriter(cb: ColumnarBatch): Unit = {
    // invoke `GpuDynamicPartitionDataSingleWriter`.write,
    // single writer will take care of the unclosed writers and the pending caches
    // in `concurrentWriters`
    super.write(cb, Some(concurrentWriters))
  }

  def writeWithConcurrentWriter(cb: ColumnarBatch): Unit = {
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
    val cpuOrd = new LazilyGeneratedOrdering(sorter.cpuOrdering)

    // use noop metrics below
    val sortTime = NoopMetric
    val peakDevMemory = NoopMetric
    val opTime = NoopMetric
    val outputBatch = NoopMetric
    val outputRows = NoopMetric

    val targetSize = GpuSortExec.targetSize(spec.batchSize)
    // out of core sort the entire iterator
    GpuOutOfCoreSortIterator(iterator, sorter, cpuOrd, targetSize,
      opTime, sortTime, outputBatch, outputRows,
      peakDevMemory)
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
    withResource(getPartitionColumns(cb)) { partitionColumnsTable =>
      for (i <- 0 until partitionColumnsTable.getNumberOfColumns) {
        // append partition column
        columnsWithPartition += partitionColumnsTable.getColumn(i)
      }
    }
    val cols = GpuColumnVector.extractBases(cb)
    columnsWithPartition ++= cols

    // 2. group by the partition columns
    // get sub-groups for each partition and get unique keys for each partition
    val groupsAndKeys = withResource(new Table(columnsWithPartition: _*)) { colsWithPartitionTbl =>
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
            withResource(new Table(dataColumns: _*)) { dataTable =>
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

    // convert spillable caches to tables, and close `status.tableCaches`
    val subTables = convertSpillBatchesToTables(status.tableCaches)

    // get concat table or the single table
    val t: Table = if (status.tableCaches.length >= 2) {
      // concat the sub batches to write in once.
      val concat = Table.concatenate(subTables: _*)
      // close sub tables after concat
      subTables.safeClose()
      concat
    } else {
      // only one single table
      subTables.head
    }

    val maxRecordsPerFile = description.maxRecordsPerFile
    withResource(t) { _ =>
      val batch = GpuColumnVector.from(t, outDataTypes)
      if (!needSplitBatch(maxRecordsPerFile, status.writerStatus.recordsInFile, batch.numRows())) {
        statsTrackers.foreach(_.newBatch(status.writerStatus.outputWriter.path(), batch))
        status.writerStatus.recordsInFile += batch.numRows()
        status.writerStatus.outputWriter.writeAndClose(batch, statsTrackers)
      } else {
        withResource(batch) { batch =>
          withResource(GpuColumnVector.from(batch)) {table =>
            val splitIndexes = getSplitIndexes(
              maxRecordsPerFile,
              status.writerStatus.recordsInFile,
              table.getRowCount()
            )

            val dataTypes = GpuColumnVector.extractTypes(batch)

            var needNewWriter = status.writerStatus.recordsInFile >= maxRecordsPerFile
            withResource(table.contiguousSplit(splitIndexes: _*)) {tabs =>
              tabs.foreach(b => {
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
                val cb = withResource(b.getTable()) {tab =>
                  GpuColumnVector.from(tab, dataTypes)
                }
                statsTrackers.foreach(_.newBatch(status.writerStatus.outputWriter.path(), cb))
                status.writerStatus.recordsInFile += b.getRowCount()
                status.writerStatus.outputWriter.writeAndClose(cb, statsTrackers)
                needNewWriter = true
              })
            }
          }
        }
      }
      status.tableCaches.clear()
      status.deviceBytes = 0
    }
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

trait WriterUtil {
  def ceilingDiv(num: Long, divisor: Long) = {
    ((num + divisor - 1) / divisor).toInt
  }

  def needSplitBatch(maxRecordsPerFile: Long, recordsInFile: Long, numRowsInBatch: Long) = {
    maxRecordsPerFile > 0 && (recordsInFile + numRowsInBatch) > maxRecordsPerFile
  }

  def getSplitIndexes(maxRecordsPerFile: Long, recordsInFile: Long, numRows: Long) = {
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
}
