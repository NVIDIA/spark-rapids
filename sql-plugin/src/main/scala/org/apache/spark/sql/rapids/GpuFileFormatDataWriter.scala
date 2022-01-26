/*
 * Copyright (c) 2019-2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{ContiguousTable, OrderByArg, Table}
import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext

import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, Cast, Concat, Expression, Literal, NullsFirst, UnsafeProjection}
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.execution.datasources.{ExecutedWriteSummary, PartitioningUtils, WriteTaskResult}
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

  protected def releaseResources(): Unit = {
    if (currentWriter != null) {
      try {
        currentWriter.close()
      } finally {
        currentWriter = null
      }
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

  override def write(batch: ColumnarBatch): Unit = {
    var needToCloseBatch = true
    try {
      // TODO: This does not handle batch splitting to make sure maxRecordsPerFile is not exceeded.
      if (description.maxRecordsPerFile > 0 && recordsInFile >= description.maxRecordsPerFile) {
        fileCounter += 1
        assert(fileCounter < MAX_FILE_COUNTER,
          s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

        newOutputWriter()
      }

      statsTrackers.foreach(_.newBatch(batch))
      recordsInFile += batch.numRows
      needToCloseBatch = false
    } finally {
      if (needToCloseBatch) {
        batch.close()
      }
    }

    // It is the responsibility of the writer to close the batch.
    currentWriter.write(batch, statsTrackers)
  }
}

/**
 * Writes data to using dynamic partition writes, meaning this single function can write to
 * multiple directories (partitions) or files (bucketing).
 */
class GpuDynamicPartitionDataWriter(
    description: GpuWriteJobDescription,
    taskAttemptContext: TaskAttemptContext,
    committer: FileCommitProtocol)
  extends GpuFileFormatDataWriter(description, taskAttemptContext, committer) {

  /** Flag saying whether or not the data to be written out is partitioned. */
  private val isPartitioned = description.partitionColumns.nonEmpty

  /** Flag saying whether or not the data to be written out is bucketed. */
  private val isBucketed = description.bucketSpec.isDefined

  if (isBucketed) {
    throw new UnsupportedOperationException("Bucketing is not supported on the GPU yet.")
  }

  assert(isPartitioned || isBucketed,
    s"""GpuDynamicPartitionWriteTask should be used for writing out data that's either
       |partitioned or bucketed. In this case neither is true.
       |GpuWriteJobDescription: $description
       """.stripMargin)

  private var fileCounter: Int = _
  private var recordsInFile: Long = _
  private var currentPartPath: String = ""

  /** Extracts the partition values out of an input batch. */
  private lazy val getPartitionColumns: ColumnarBatch => Table = {
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
  private lazy val getOutputColumns: ColumnarBatch => Table = {
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

  /**
   * Expression that given partition columns builds a path string like: col1=val/col2=val/...
   * This is used after we pull the unique partition values back to the host.
   */
  private lazy val partitionPathExpression: Expression = Concat(
    description.partitionColumns.zipWithIndex.flatMap { case (c, i) =>
      val partitionName = ShimLoader.getSparkShims.getScalaUDFAsExpression(
        ExternalCatalogUtils.getPartitionPathString _,
        StringType,
        Seq(Literal(c.name), Cast(c, StringType, Option(description.timeZoneId))))
      if (i == 0) Seq(partitionName) else Seq(Literal(Path.SEPARATOR), partitionName)
    })

  /** Evaluates the `partitionPathExpression` above on a row of `partitionValues` and returns
   * the partition string.
   */
  private lazy val getPartitionPath: InternalRow => String = {
    val proj = UnsafeProjection.create(Seq(partitionPathExpression), description.partitionColumns)
    row => proj(row).getString(0)
  }

  @scala.annotation.nowarn(
    "msg=method newTaskTempFile.* in class FileCommitProtocol is deprecated"
  )
  private def newOutputWriter(partDir: String): Unit = {
    recordsInFile = 0
    releaseResources()

    updatedPartitions.add(partDir)

    // This must be in a form that matches our bucketing format. See BucketingUtils.
    val ext = f".c$fileCounter%03d" +
      description.outputWriterFactory.getFileExtension(taskAttemptContext)

    val customPath =
      description.customPartitionLocations.get(PartitioningUtils.parsePathFragment(partDir))

    val currentPath = if (customPath.isDefined) {
      committer.newTaskTempFileAbsPath(taskAttemptContext, customPath.get, ext)
    } else {
      committer.newTaskTempFile(taskAttemptContext, Some(partDir), ext)
    }

    currentWriter = description.outputWriterFactory.newInstance(
      path = currentPath,
      dataSchema = description.dataColumns.toStructType,
      context = taskAttemptContext)

    statsTrackers.foreach(_.newFile(currentPath))
  }

  // All data is sorted ascending with default null ordering
  private val nullsSmallest = Ascending.defaultNullOrdering == NullsFirst

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
  private def copyToHostAsBatch(input: Table, colTypes: Array[DataType]): ColumnarBatch = {
    val tmp = GpuColumnVector.from(input, colTypes)
    try {
      new ColumnarBatch(GpuColumnVector.extractColumns(tmp).map(_.copyToHost()), tmp.numRows())
    } finally {
      tmp.close()
    }
  }

  override def write(cb: ColumnarBatch): Unit = {
    // We have an entire batch that is sorted, so we need to split it up by key
    var needToCloseBatch = true
    var partitionColumns: Table = null
    var distinctKeys: Table = null
    var outputColumns: Table = null
    var splits: Array[ContiguousTable] = null
    var cbKeys: ColumnarBatch = null
    try {
      assert(isPartitioned)
      assert(!isBucketed)

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
        val batch = GpuColumnVector.from(table, outDataTypes)
        val partPath = combined._1
        if (currentPartPath != partPath) {
          currentPartPath = partPath
          statsTrackers.foreach(_.newPartition())
          fileCounter = 0
          newOutputWriter(currentPartPath)
        } else if (description.maxRecordsPerFile > 0 &&
          recordsInFile >= description.maxRecordsPerFile) {
          // Exceeded the threshold in terms of the number of records per file.
          // Create a new file by increasing the file counter.
          fileCounter += 1
          assert(fileCounter < MAX_FILE_COUNTER,
            s"File counter $fileCounter is beyond max value $MAX_FILE_COUNTER")

          newOutputWriter(currentPartPath)
        }
        statsTrackers.foreach(_.newBatch(batch))
        recordsInFile += batch.numRows
        currentWriter.write(batch, statsTrackers)
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
    val statsTrackers: Seq[ColumnarWriteJobStatsTracker])
  extends Serializable {

  assert(AttributeSet(allColumns) == AttributeSet(partitionColumns ++ dataColumns),
    s"""
         |All columns: ${allColumns.mkString(", ")}
         |Partition columns: ${partitionColumns.mkString(", ")}
         |Data columns: ${dataColumns.mkString(", ")}
       """.stripMargin)
}
