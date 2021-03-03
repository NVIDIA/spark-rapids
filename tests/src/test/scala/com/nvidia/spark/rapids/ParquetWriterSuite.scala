/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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
import java.lang.reflect.Method
import java.nio.charset.StandardCharsets

import ai.rapids.cudf.{ColumnVector, DType, Table, TableWriter}
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.types.{ByteType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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

        val footer = ParquetFileReader.readFooters(spark.sparkContext.hadoopConfiguration,
          new Path(tempFile.getAbsolutePath)).get(0)

        val parquetMeta = footer.getParquetMetadata
        val fileMeta = footer.getParquetMetadata.getFileMetaData
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
}
