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

/*** spark-rapids-shim-json-lines
{"spark": "350"}
{"spark": "351"}
{"spark": "352"}
{"spark": "353"}
{"spark": "354"}
{"spark": "355"}
{"spark": "356"}
{"spark": "357"}
{"spark": "358"}
{"spark": "359"}
{"spark": "400"}
{"spark": "401"}
{"spark": "402"}
{"spark": "403"}
{"spark": "404"}
{"spark": "411"}
{"spark": "413"}
spark-rapids-shim-json-lines ***/
package com.nvidia.spark.rapids.iceberg

import java.nio.file.Files
import java.util.{HashMap => JHashMap}
import java.util.concurrent.CompletableFuture

import com.nvidia.spark.rapids.{CombineConf, PerfIO, PerfIOConf, ThreadPoolConfBuilder}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.fileio.iceberg.IcebergFileIO
import com.nvidia.spark.rapids.iceberg.parquet._
import com.nvidia.spark.rapids.parquet.HostMemoryEmptyMetaData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}
import org.apache.iceberg.Schema
import org.apache.iceberg.hadoop.HadoopFileIO
import org.apache.iceberg.types.Types
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.schema.{MessageType, Types => ParquetTypes}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.rapids.metrics.source.MockTaskContext

class GpuMultiThreadIcebergParquetReaderSuite extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    super.beforeAll()
    PerfIO.initS3Client(PerfIOConf(new SparkConf()))
  }

  override def afterAll(): Unit = {
    try {
      PerfIO.shutdownS3Client()
    } finally {
      super.afterAll()
    }
  }

  private val partitionFieldId = 1
  private val payloadFieldId = 2
  private val parquetSchema: MessageType = ParquetTypes.buildMessage()
    .optional(INT64).id(payloadFieldId).named("b").named("test")

  private def writeFile(path: Path, numRows: Int, conf: Configuration): Unit = {
    val groups = new SimpleGroupFactory(parquetSchema)
    withResource(ExampleParquetWriter.builder(path).withConf(conf).withType(parquetSchema)
        .build()) { writer =>
      (0 until numRows).foreach { value =>
        writer.write(groups.newGroup().append("b", value.toLong))
      }
    }
  }

  for (sameConstants <- Seq(false, true)) {
    test(s"actual Iceberg reader combines metadata-only files only when constants match: " +
        s"$sameConstants") {
      val dir = Files.createTempDirectory("iceberg-metadata-combine")
      val taskContext = new MockTaskContext(1, 0)
      TrampolineUtil.setTaskContext(taskContext)
      try {
        val hadoopConf = new Configuration()
        val firstRows = 2
        val secondRows = 3
        val paths = Seq(new Path(dir.resolve("first.parquet").toUri),
          new Path(dir.resolve("second.parquet").toUri))
        writeFile(paths.head, firstRows, hadoopConf)
        writeFile(paths(1), secondRows, hadoopConf)

        withResource(new HadoopFileIO(hadoopConf)) { fileIO =>
          val rapidsFileIO = new IcebergFileIO(fileIO)
          val files = paths.map(path =>
            IcebergPartitionedFile(rapidsFileIO.newInputFile(path.toString)))
          val firstConstant = 10L
          val secondConstant = if (sameConstants) firstConstant else 20L
          val constants = files.zip(Seq(firstConstant, secondConstant)).toMap
          val expectedSchema = new Schema(
            Types.NestedField.optional(partitionFieldId, "a", Types.LongType.get()))
          val conf = GpuIcebergParquetReaderConf(
            caseSensitive = true,
            hadoopConf,
            maxBatchSizeRows = 1000,
            maxBatchSizeBytes = 1L << 20,
            targetBatchSizeBytes = 1L << 20,
            maxGpuColumnSizeBytes = 1L << 20,
            useChunkedReader = false,
            maxChunkedReaderMemoryUsageSizeBytes = 0,
            skipReadEstimate = true,
            parquetDebugDumpPrefix = None,
            parquetDebugDumpAlways = false,
            metrics = Map.empty,
            threadConf = MultiThread(
              new ThreadPoolConfBuilder(1, true, 1L << 20, 10000L, false),
              maxNumFilesProcessed = files.size,
              CombineConf(1L << 20, 0),
              disableCombining = false,
              hasFilePathMetadata = false,
              hasRowPositionMetadata = false),
            expectedSchema,
            nameMapping = None,
            validateDeletionVectorCrc = false)

          withResource(new GpuMultiThreadIcebergParquetReader(
              rapidsFileIO, files, file => {
                val values = new JHashMap[Integer, java.lang.Long]()
                values.put(partitionFieldId, constants(file))
                values
              }, _ => None, _ => None, conf)) { icebergReader =>
            val reader = icebergReader.reader
            def readFile(index: Int) = {
              val runner = reader.getBatchRunner(taskContext,
                files(index).sparkPartitionedFile, hadoopConf, Array.empty)
              // Finish each real worker before combining, independent of scheduling or wait time.
              CompletableFuture.supplyAsync(() => {
                try {
                  runner.call()
                } finally {
                  TrampolineUtil.unsetTaskContext()
                }
              }).get()
            }

            withResource(readFile(0)) { first =>
              withResource(first.data) { firstMeta =>
                withResource(readFile(1)) { second =>
                  withResource(second.data) { secondMeta =>
                    assert(firstMeta.isInstanceOf[HostMemoryEmptyMetaData])
                    assert(secondMeta.isInstanceOf[HostMemoryEmptyMetaData])
                    assert(firstMeta.memBuffersAndSizes.map(_.numRows).sum == firstRows)
                    assert(secondMeta.memBuffersAndSizes.map(_.numRows).sum == secondRows)
                    withResource(reader.combineHMBs(Array(firstMeta, secondMeta))) { combined =>
                      val expectedRows = if (sameConstants) firstRows + secondRows else firstRows
                      assert(combined.memBuffersAndSizes.map(_.numRows).sum == expectedRows)
                      if (!sameConstants) {
                        assert(combined eq firstMeta)
                      }
                    }
                  }
                }
              }
            }
          }
        }
      } finally {
        try {
          taskContext.markTaskComplete()
        } finally {
          TrampolineUtil.unsetTaskContext()
          FileUtil.fullyDelete(dir.toFile)
        }
      }
    }
  }
}
