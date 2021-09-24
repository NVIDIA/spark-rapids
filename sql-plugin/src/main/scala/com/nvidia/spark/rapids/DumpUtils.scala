/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import java.io.File
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import ai.rapids.cudf.CompressionType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobID
import org.apache.hadoop.mapreduce.{TaskAttemptContext, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.ParquetOutputTimestampType
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

object DumpUtils extends Logging {
  /**
   * Debug utility to dump columnar batch to parquet file. <br>
   * It's running on GPU. Parquet column names are generated from columnar batch type info
   * @param columnarBatch the columnar batch to be dumped, should be GPU columnar batch
   * @param filePrefix parquet file prefix, e.g. /tmp/my-debug-prefix-
   * @return parquet file path if dump is successful, e.g. /tmp/my-debug-prefix-123.parquet
   */
  def dumpToParquetFile(columnarBatch: ColumnarBatch, filePrefix: String): Option[String] = {
    try {
      if(columnarBatch.numCols() == 0) {
        logWarning("dump to parquet failed, has no column, file prefix is " + filePrefix)
        None
      } else {
        Some(dumpToParquetFileImpl(columnarBatch, filePrefix))
      }
    } catch {
      case e: Exception =>
        logWarning("dump to parquet failed, file prefix is " + filePrefix, e)
        None
    }
  }

  private def dumpToParquetFileImpl(columnarBatch: ColumnarBatch, filePrefix: String): String = {
    // construct schema according field types
    val fields = new Array[StructField](columnarBatch.numCols())
    (0 until columnarBatch.numCols()).foreach(i => {
      // ,() are invalid in column name, replace these characters
      val name = ("c" + i + "_" + columnarBatch.column(i).dataType())
        .replace(',', '_')
        .replace('(', '[')
        .replace(')', ']')
      fields(i) = StructField(name, columnarBatch.column(i).dataType())
    })
    val schema = StructType(fields)

    // generate path, retry if file exists
    val random = new Random
    var path = ""
    var succeeded = false
    while (!succeeded) {
      path = filePrefix + random.nextInt(Int.MaxValue) + ".parquet"
      if(!new File(path).exists()) {
        succeeded = true
      }
    }

    // setup TaskAttemptContext that required by GpuParquetWriter.
    // some values in TaskAttemptContext are fake,
    // because of GpuParquetWriter not actually need
    val jobId = new JobID(createJobString, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)
    val taskAttemptContext: TaskAttemptContext = {
      // Set up the configuration object
      val hadoopConf = new Configuration()
      ParquetWriteSupport.setSchema(schema, hadoopConf)
      hadoopConf.setBoolean(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key, false)
      hadoopConf.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
        ParquetOutputTimestampType.INT96.toString)
      new TaskAttemptContextImpl(hadoopConf, taskAttemptId)
    }

    // write data and close
    val writer = new GpuParquetWriter(path, schema, CompressionType.SNAPPY,
      false, taskAttemptContext)

    try {
      // writer will invoke close on the columnar batch,
      // so incRef to leave this columnar batch available for the following process
      GpuColumnVector.incRefCounts(columnarBatch)
      writer.write(columnarBatch, Seq.empty)
    } finally {
      writer.close()
    }

    path
  }

  private def createJobString() : String = {
    val rand = new Random
    val base = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US).format(new Date())
    val l1 = rand.nextInt(Int.MaxValue)
    base + l1
  }
}
