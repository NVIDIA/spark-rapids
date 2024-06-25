/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "311"}
{"spark": "312"}
{"spark": "313"}
{"spark": "320"}
{"spark": "321"}
{"spark": "321cdh"}
{"spark": "322"}
{"spark": "323"}
{"spark": "324"}
{"spark": "330"}
{"spark": "330cdh"}
{"spark": "330db"}
{"spark": "331"}
{"spark": "332"}
{"spark": "332cdh"}
{"spark": "333"}
{"spark": "334"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids

import java.util.{Date, UUID}

import com.nvidia.spark.TimingUtils
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.shims.{GpuBucketingUtils, RapidsFileSourceMetaUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{WriteTaskResult, WriteTaskStats}
import org.apache.spark.sql.execution.datasources.FileFormatWriter.OutputSpec
import org.apache.spark.sql.rapids.shims.RapidsHadoopWriterUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SerializableConfiguration, Utils}

/** A helper object for writing columnar data out to a location. */
object GpuFileFormatWriter extends Logging {

  private def verifySchema(format: ColumnarFileFormat, schema: StructType): Unit = {
    schema.foreach { field =>
      if (!format.supportDataType(field.dataType)) {
        throw new AnalysisException(
          s"$format data source does not support ${field.dataType.catalogString} data type.")
      }
    }
  }

  /** Describes how concurrent output writers should be executed. */
  case class GpuConcurrentOutputWriterSpec(maxWriters: Int, output: Seq[Attribute],
      batchSize: Long, sortOrder: Seq[SortOrder])

  /**
   * Basic work flow of this command is:
   * 1. Driver side setup, including output committer initialization and data source specific
   *    preparation work for the write job to be issued.
   * 2. Issues a write job consists of one or more executor side tasks, each of which writes all
   *    rows within an RDD partition.
   * 3. If no exception is thrown in a task, commits that task, otherwise aborts that task;  If any
   *    exception is thrown during task commitment, also aborts that task.
   * 4. If all tasks are committed, commit the job, otherwise aborts the job;  If any exception is
   *    thrown during job commitment, also aborts the job.
   * 5. If the job is successfully committed, perform post-commit operations such as
   *    processing statistics.
   * @return The set of all partition paths that were updated during this write job.
   */
  def write(
      sparkSession: SparkSession,
      plan: SparkPlan,
      fileFormat: ColumnarFileFormat,
      committer: FileCommitProtocol,
      outputSpec: OutputSpec,
      hadoopConf: Configuration,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      options: Map[String, String],
      useStableSort: Boolean,
      concurrentWriterPartitionFlushSize: Long,
      numStaticPartitionCols: Int = 0): Set[String] = {
    require(partitionColumns.size >= numStaticPartitionCols)

    val job = Job.getInstance(hadoopConf)
    job.setOutputKeyClass(classOf[Void])
    // The data is being written as columnar batches, but those are not serializable. Using the same
    // InternalRow type that Spark uses here, as it should not really matter. The columnar path
    // should not be executing the output format code that depends on this setting. Instead specific
    // output formats are detected and replaced with a different code path, otherwise the code
    // needs to fallback to the row-based write path.
    job.setOutputValueClass(classOf[InternalRow])
    FileOutputFormat.setOutputPath(job, new Path(outputSpec.outputPath))

    val partitionSet = AttributeSet(partitionColumns)
    // cleanup the internal metadata information of
    // the file source metadata attribute if any before write out when needed.
    val finalOutputSpec = outputSpec.copy(outputColumns = outputSpec.outputColumns
      .map(RapidsFileSourceMetaUtils.cleanupFileSourceMetadataInformation))
    val dataColumns = finalOutputSpec.outputColumns.filterNot(partitionSet.contains)

    val hasGpuEmpty2Null = plan.find(p => GpuV1WriteUtils.hasGpuEmptyToNull(p.expressions))
      .isDefined
    val empty2NullPlan = if (hasGpuEmpty2Null) {
      // Empty2Null has been inserted during logic optimization.
      plan
    } else {
      val projectList = GpuV1WriteUtils.convertGpuEmptyToNull(plan.output, partitionSet)
      if (projectList.nonEmpty) GpuProjectExec(projectList, plan)() else plan
    }

    val writerBucketSpec = GpuBucketingUtils.getWriterBucketSpec(bucketSpec, dataColumns,
      options, false)
    val sortColumns = bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }

    val caseInsensitiveOptions = CaseInsensitiveMap(options)

    val dataSchema = dataColumns.toStructType
    verifySchema(fileFormat, dataSchema)

    // NOTE: prepareWrite has side effects as it modifies the job configuration.
    val outputWriterFactory =
      fileFormat.prepareWrite(sparkSession, job, caseInsensitiveOptions, dataSchema)

    val description = new GpuWriteJobDescription(
      uuid = UUID.randomUUID.toString,
      serializableHadoopConf = new SerializableConfiguration(job.getConfiguration),
      outputWriterFactory = outputWriterFactory,
      allColumns = finalOutputSpec.outputColumns,
      dataColumns = dataColumns,
      partitionColumns = partitionColumns,
      bucketSpec = writerBucketSpec,
      path = finalOutputSpec.outputPath,
      customPartitionLocations = finalOutputSpec.customPartitionLocations,
      maxRecordsPerFile = caseInsensitiveOptions.get("maxRecordsPerFile").map(_.toLong)
          .getOrElse(sparkSession.sessionState.conf.maxRecordsPerFile),
      timeZoneId = caseInsensitiveOptions.get(DateTimeUtils.TIMEZONE_OPTION)
          .getOrElse(sparkSession.sessionState.conf.sessionLocalTimeZone),
      statsTrackers = statsTrackers,
      concurrentWriterPartitionFlushSize = concurrentWriterPartitionFlushSize
    )

    // We should first sort by dynamic partition columns, then bucket id, and finally
    // sorting columns.
    val requiredOrdering = partitionColumns.drop(numStaticPartitionCols) ++
      writerBucketSpec.map(_.bucketIdExpression) ++ sortColumns
    // the sort order doesn't matter
    val actualOrdering = empty2NullPlan.outputOrdering.map(_.child)
    val orderingMatched = if (requiredOrdering.length > actualOrdering.length) {
      false
    } else {
      requiredOrdering.zip(actualOrdering).forall {
        case (requiredOrder, childOutputOrder) =>
          requiredOrder.semanticEquals(childOutputOrder)
      }
    }

    SQLExecution.checkSQLExecutionId(sparkSession)

    // propagate the description UUID into the jobs, so that committers
    // get an ID guaranteed to be unique.
    job.getConfiguration.set("spark.sql.sources.writeJobUUID", description.uuid)

    // This call shouldn't be put into the `try` block below because it only initializes and
    // prepares the job, any exception thrown from here shouldn't cause abortJob() to be called.
    committer.setupJob(job)

    // check if we can use concurrent writer
    val maxWriterConfKey = "spark.sql.maxConcurrentOutputFileWriters"
    val maxWriters: Int = if (sparkSession.sessionState.conf.contains(maxWriterConfKey)) {
      sparkSession.sessionState.conf.getConfString(maxWriterConfKey).toInt
    } else {
      // Spark 31x does not support concurrent writer
      0
    }
    val concurrentWritersEnabled = maxWriters > 0 && sortColumns.isEmpty

    try {
      val (rdd, concurrentOutputWriterSpec) = if (orderingMatched) {
        (empty2NullPlan.executeColumnar(), None)
      } else {
        // SPARK-21165: the `requiredOrdering` is based on the attributes from analyzed plan, and
        // the physical plan may have different attribute ids due to optimizer removing some
        // aliases. Here we bind the expression ahead to avoid potential attribute ids mismatch.
        val orderingExpr = GpuBindReferences.bindReferences(
          requiredOrdering
              .map(attr => SortOrder(attr, Ascending)), finalOutputSpec.outputColumns)
        if (concurrentWritersEnabled) {
          val batchSize = RapidsConf.GPU_BATCH_SIZE_BYTES.get(sparkSession.sessionState.conf)
          (empty2NullPlan.executeColumnar(),
              Some(GpuConcurrentOutputWriterSpec(maxWriters, empty2NullPlan.output, batchSize,
                orderingExpr)))
        } else {
          val sortType = if (useStableSort) {
            FullSortSingleBatch
          } else {
            OutOfCoreSort
          }
          // TODO: Using a GPU ordering as a CPU ordering here. Should be OK for now since we do not
          //       support bucket expressions yet and the rest should be simple attributes.
          val sort = GpuSortExec(
            orderingExpr,
            global = false,
            child = empty2NullPlan,
            sortType = sortType
          )(orderingExpr).executeColumnar()
          (sort, None)
        }
      }

      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
        sparkSession.sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
      } else {
        rdd
      }

      // SPARK-41448 map reduce job IDs need to consistent across attempts for correctness
      val jobTrackerId = SparkHadoopWriterUtils.createJobTrackerID(new Date)
      val ret = new Array[WriteTaskResult](rddWithNonEmptyPartitions.partitions.length)
      sparkSession.sparkContext.runJob(
        rddWithNonEmptyPartitions,
        (taskContext: TaskContext, iter: Iterator[ColumnarBatch]) => {
          executeTask(
            description = description,
            jobTrackerId = jobTrackerId,
            sparkStageId = taskContext.stageId(),
            sparkPartitionId = taskContext.partitionId(),
            sparkAttemptNumber = taskContext.taskAttemptId().toInt & Integer.MAX_VALUE,
            committer,
            iterator = iter,
            concurrentOutputWriterSpec = concurrentOutputWriterSpec)
        },
        rddWithNonEmptyPartitions.partitions.indices,
        (index, res: WriteTaskResult) => {
          committer.onTaskCommit(res.commitMsg)
          ret(index) = res
        })

      val commitMsgs = ret.map(_.commitMsg)

      val (_, duration) = TimingUtils.timeTakenMs { committer.commitJob(job, commitMsgs) }
      logInfo(s"Write Job ${description.uuid} committed. Elapsed time: $duration ms.")

      processStats(description.statsTrackers, ret.map(_.summary.stats), duration)
      logInfo(s"Finished processing stats for write job ${description.uuid}.")

      // return a set of all the partition paths that were updated during this job
      ret.map(_.summary.updatedPartitions).reduceOption(_ ++ _).getOrElse(Set.empty)
    } catch { case cause: Throwable =>
      logError(s"Aborting job ${description.uuid}.", cause)
      committer.abortJob(job)
      throw new SparkException("Job aborted.", cause)
    }
  }

  /** Writes data out in a single Spark task. */
  private def executeTask(
      description: GpuWriteJobDescription,
      jobTrackerId: String,
      sparkStageId: Int,
      sparkPartitionId: Int,
      sparkAttemptNumber: Int,
      committer: FileCommitProtocol,
      iterator: Iterator[ColumnarBatch],
      concurrentOutputWriterSpec: Option[GpuConcurrentOutputWriterSpec]): WriteTaskResult = {

    val jobId = RapidsHadoopWriterUtils.createJobID(jobTrackerId, sparkStageId)
    val taskId = new TaskID(jobId, TaskType.MAP, sparkPartitionId)
    val taskAttemptId = new TaskAttemptID(taskId, sparkAttemptNumber)

    // Set up the attempt context required to use in the output committer.
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = description.serializableHadoopConf.value
      hadoopConf.set("mapreduce.job.id", jobId.toString)
      hadoopConf.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
      hadoopConf.set("mapreduce.task.attempt.id", taskAttemptId.toString)
      hadoopConf.setBoolean("mapreduce.task.ismap", true)
      hadoopConf.setInt("mapreduce.task.partition", 0)

      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    committer.setupTask(taskAttemptContext)

    val dataWriter =
      if (sparkPartitionId != 0 && !iterator.hasNext) {
        // In case of empty job, leave first partition to save meta for file format like parquet.
        new GpuEmptyDirectoryDataWriter(description, taskAttemptContext, committer)
      } else if (description.partitionColumns.isEmpty && description.bucketSpec.isEmpty) {
        new GpuSingleDirectoryDataWriter(description, taskAttemptContext, committer)
      } else {
        concurrentOutputWriterSpec match {
          case Some(spec) =>
            new GpuDynamicPartitionDataConcurrentWriter(description, taskAttemptContext,
              committer, spec)
          case _ =>
            new GpuDynamicPartitionDataSingleWriter(description, taskAttemptContext, committer)
        }
      }

    try {
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Execute the task to write rows out and commit the task.
        dataWriter.writeWithIterator(iterator)
        dataWriter.commit()
      })(catchBlock = {
        // If there is an error, abort the task
        dataWriter.abort()
        logError(s"Job $jobId aborted.")
      }, finallyBlock = {
        dataWriter.close()
      })
    } catch {
      case e: FetchFailedException =>
        throw e
      case t: Throwable =>
        throw new SparkException("Task failed while writing rows.", t)
    }
  }

  /**
   * For every registered [[WriteJobStatsTracker]], call `processStats()` on it, passing it
   * the corresponding [[WriteTaskStats]] from all executors.
   */
  private def processStats(
      statsTrackers: Seq[ColumnarWriteJobStatsTracker],
      statsPerTask: Seq[Seq[WriteTaskStats]],
      jobCommitDuration: Long)
  : Unit = {

    val numStatsTrackers = statsTrackers.length
    assert(statsPerTask.forall(_.length == numStatsTrackers),
      s"""Every WriteTask should have produced one `WriteTaskStats` object for every tracker.
         |There are $numStatsTrackers statsTrackers, but some task returned
         |${statsPerTask.find(_.length != numStatsTrackers).get.length} results instead.
       """.stripMargin)

    val statsPerTracker = if (statsPerTask.nonEmpty) {
      statsPerTask.transpose
    } else {
      statsTrackers.map(_ => Seq.empty)
    }

    statsTrackers.zip(statsPerTracker).foreach {
      case (statsTracker, stats) => statsTracker.processStats(stats, jobCommitDuration)
    }
  }
}
