/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION.
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
import java.util.concurrent.Callable

import scala.util.Random

import ai.rapids.cudf.HostMemoryBuffer
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.shims.PartitionedFileUtilsShim
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMultiFileReaderSuite extends SparkQueryCompareTestSuite {

  test("avoid infinite loop when host buffers empty") {
    val conf = new Configuration(false)
    val membuffers =
      Array(SingleHMBAndMeta(
        HostMemoryBuffer.allocate(0), 0L, 0, Seq.empty))
    val multiFileReader = new MultiFileCloudPartitionReaderBase(
      conf,
      inputFiles = Array.empty,
      numThreads = 1,
      maxNumFileProcessed = 1,
      filters = Array.empty,
      execMetrics = Map.empty,
      maxReadBatchSizeRows = 1000,
      maxReadBatchSizeBytes = 64L * 1024L * 1024L) {

      // Setup some empty host buffers at the start
      currentFileHostBuffers = Some(new HostMemoryBuffersWithMetaDataBase {
        override def partitionedFile: PartitionedFile =
          PartitionedFileUtilsShim.newPartitionedFile(InternalRow.empty, "", 0, 0)
        override def memBuffersAndSizes: Array[SingleHMBAndMeta] = membuffers
        override def bytesRead: Long = 0
      })

      override def getBatchRunner(
          tc: TaskContext,
          file: PartitionedFile,
          origFile: Option[PartitionedFile],
          conf: Configuration,
          filters: Array[Filter]): Callable[HostMemoryBuffersWithMetaDataBase] = {
        () => null
      }

      override def readBatches(h: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] =
        EmptyGpuColumnarBatchIterator

      override def getFileFormatShortName: String = ""
    }

    withResource(multiFileReader) { _ =>
      assertResult(false)(multiFileReader.next())
    }
  }

  def generateRandomString(length: Int): String = {
    Iterator.fill(length)(Random.alphanumeric.head).mkString
  }

  def sqlExprToGenerateRandomString(length: Int, partitions: Int): String = {
    val randomString = generateRandomString(length)
    s"substring('$randomString', 1, floor(rand() * $partitions) + 1)"
  }

  def writeAndReadParquet(df: DataFrame, spark: SparkSession, outputFile: File,
      partCols:String*): DataFrame = {
    spark.conf.set("spark.rapids.sql.enabled", "false")
    df.write.partitionBy(partCols:_*).parquet(outputFile.getCanonicalPath)

    spark.conf.set("spark.rapids.sql.enabled", "true")
    spark.conf.set("spark.rapids.cudfColumnSizeLimit", 8000)
    spark.read.parquet(outputFile.getCanonicalPath)
  }

  test("WIP: test column size exceeding cuDF limit" +
    " - single partition column, single partition value") {
    val numRows = 10000
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val df = spark.range(numRows)
          .withColumn("partCol", lit(generateRandomString(10)))
        val resultAns = writeAndReadParquet(df, spark, file, "partCol").collect()
        assert(resultAns.length == numRows)
      })
    }
  }

  test("WIP: test column size exceeding cuDF limit" +
    " - single partition column, multiple partition values") {
    val numRows = 10000
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val df = spark.range(numRows)
          .withColumn("partCol", expr(sqlExprToGenerateRandomString(20, 10)))
        val resultAns = writeAndReadParquet(df, spark, file, "partCol").collect()
        assert(resultAns.length == numRows)
      })
    }
  }

  test("WIP: test column size exceeding cuDF limit" +
    " - multiple partition columns, multiple partition values, equal size columns") {
    val numRows = 10000
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val df = spark.range(numRows)
          .withColumn("partCol", expr(sqlExprToGenerateRandomString(15, 8)))
          .withColumn("partCol2", expr(sqlExprToGenerateRandomString(15, 8)))
        val resultAns = writeAndReadParquet(df, spark, file, "partCol", "partCol2").collect()
        assert(resultAns.length == numRows)
      })
    }
  }

  test("WIP: test column size exceeding cuDF limit" +
    " - multiple partition columns, multiple partition values, smaller first column") {
    val numRows = 10000
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val df = spark.range(numRows)
          .withColumn("partCol", expr(sqlExprToGenerateRandomString(10, 8)))
          .withColumn("partCol2", expr(sqlExprToGenerateRandomString(15, 8)))
        val resultAns = writeAndReadParquet(df, spark, file, "partCol", "partCol2").collect()
        assert(resultAns.length == numRows)
      })
    }
  }

  test("WIP: test column size exceeding cuDF limit" +
    " - multiple partition columns, multiple partition values, wider first column") {
    val numRows = 10000
    withTempPath { file =>
      withGpuSparkSession(spark => {
        val df = spark.range(numRows)
          .withColumn("partCol", expr(sqlExprToGenerateRandomString(15, 8)))
          .withColumn("partCol2", expr(sqlExprToGenerateRandomString(10, 8)))
        val resultAns = writeAndReadParquet(df, spark, file, "partCol", "partCol2").collect()
        assert(resultAns.length == numRows)
      })
    }
  }
}
