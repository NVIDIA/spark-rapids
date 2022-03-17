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

package com.nvidia.spark.rapids

import java.io.{File, FilenameFilter}
import java.nio.charset.StandardCharsets

import com.nvidia.spark.rapids.shims.SparkShimImpl
import org.apache.commons.io.filefilter.WildcardFileFilter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.rapids.BasicColumnarWriteJobStatsTracker

/**
 * Tests for writing Parquet files with the GPU.
 */
class ParquetWriterSuite extends SparkQueryCompareTestSuite {
  test("file metadata") {
    val tempFile = File.createTempFile("stats", ".parquet")
    try {
      withGpuSparkSession(spark => {
        val df = mixedDfWithNulls(spark)
        df.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
        val filter: FilenameFilter = new WildcardFileFilter("*.parquet")
        val inputFile = HadoopInputFile.fromPath(
          new Path(tempFile.listFiles(filter)(0).getAbsolutePath),
          spark.sparkContext.hadoopConfiguration)
        val parquetMeta = ParquetFileReader.open(inputFile).getFooter

        val fileMeta = parquetMeta.getFileMetaData
        val extra = fileMeta.getKeyValueMetaData
        assert(extra.containsKey("org.apache.spark.version"))
        assert(extra.containsKey("org.apache.spark.sql.parquet.row.metadata"))

        val blocks = parquetMeta.getBlocks
        assertResult(1) { blocks.size }
        val block = blocks.get(0)
        assertResult(11) { block.getRowCount }
        val cols = block.getColumns
        assertResult(4) { cols.size }

        assertResult(3) { cols.get(0).getStatistics.getNumNulls }
        assertResult(-700L) { cols.get(0).getStatistics.genericGetMin }
        assertResult(1200L) { cols.get(0).getStatistics.genericGetMax }

        assertResult(4) { cols.get(1).getStatistics.getNumNulls }
        assertResult(1.0) { cols.get(1).getStatistics.genericGetMin }
        assertResult(9.0) { cols.get(1).getStatistics.genericGetMax }

        assertResult(4) { cols.get(2).getStatistics.getNumNulls }
        assertResult(90) { cols.get(2).getStatistics.genericGetMin }
        assertResult(99) { cols.get(2).getStatistics.genericGetMax }

        assertResult(1) { cols.get(3).getStatistics.getNumNulls }
        assertResult("A") {
          new String(cols.get(3).getStatistics.getMinBytes, StandardCharsets.UTF_8)
        }
        assertResult("\ud720\ud721") {
          new String(cols.get(3).getStatistics.getMaxBytes, StandardCharsets.UTF_8)
        }
      })
    } finally {
      tempFile.delete()
    }
  }

  test("sorted partitioned write") {
    val conf = new SparkConf().set(RapidsConf.SQL_ENABLED.key, "true")
    val tempFile = File.createTempFile("partitioned", ".parquet")
    try {
      SparkSessionHolder.withSparkSession(conf, spark => {
        import spark.implicits._
        val df = spark.sparkContext.parallelize((1L to 10000000L))
            .map{i => ("a", f"$i%010d", i)}.toDF("partkey", "val", "val2")
        df.repartition(1, $"partkey").sortWithinPartitions($"partkey", $"val", $"val2")
            .write.mode("overwrite").partitionBy("partkey").parquet(tempFile.getAbsolutePath)
        val firstRow = spark.read.parquet(tempFile.getAbsolutePath).head
        assertResult("0000000001")(firstRow.getString(0))
      })
    } finally {
      tempFile.delete()
    }
  }

  testExpectedGpuException(
    "Old dates in EXCEPTION mode",
    classOf[SparkException],
    oldDatesDf,
    new SparkConf().set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")) {
    val tempFile = File.createTempFile("oldDates", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  testExpectedGpuException(
    "Old timestamps millis in EXCEPTION mode",
    classOf[SparkException],
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  testExpectedGpuException(
    "Old timestamps in EXCEPTION mode",
    classOf[SparkException],
    oldTsDf,
    new SparkConf()
      .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "EXCEPTION")
      .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")) {
    val tempFile = File.createTempFile("oldTimeStamp", "parquet")
    tempFile.delete()
    frame => {
      frame.write.mode("overwrite").parquet(tempFile.getAbsolutePath)
      frame
    }
  }

  test("Job commit time metrics") {
    val slowCommitClass = "com.nvidia.spark.rapids.SlowFileCommitProtocolForTest"
    withGpuSparkSession(spark => {
      try {
        spark.sql("CREATE TABLE t(id STRING) USING PARQUET")
        val df = spark.sql("INSERT INTO TABLE t SELECT 'abc'")
        val insert = SparkShimImpl.findOperators(df.queryExecution.executedPlan,
          _.isInstanceOf[GpuDataWritingCommandExec]).head
          .asInstanceOf[GpuDataWritingCommandExec]
        assert(insert.metrics.contains(BasicColumnarWriteJobStatsTracker.JOB_COMMIT_TIME))
        assert(insert.metrics.contains(BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME))
        assert(insert.metrics(BasicColumnarWriteJobStatsTracker.JOB_COMMIT_TIME).value > 0)
        assert(insert.metrics(BasicColumnarWriteJobStatsTracker.TASK_COMMIT_TIME).value > 0)
      } finally {
        spark.sql("DROP TABLE IF EXISTS tempmetricstable")
      }
    }, new SparkConf().set("spark.sql.sources.commitProtocolClass", slowCommitClass))
  }
}

/** File committer that sleeps before committing each task and the job. */
case class SlowFileCommitProtocolForTest(
    jobId: String,
    path: String,
    dynamicPartitionOverwrite: Boolean = false)
    extends SQLHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite) {
  override def commitTask(
      taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    Thread.sleep(100)
    super.commitTask(taskContext)
  }

  override def commitJob(
      jobContext: JobContext,
      taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    Thread.sleep(100)
    super.commitJob(jobContext, taskCommits)
  }
}
