/*
 * Copyright (c) 2020-2025, NVIDIA CORPORATION.
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

import java.math.RoundingMode

import ai.rapids.cudf.{ColumnVector, Cuda, DType, Table}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.jni.kudo.DumpOption
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.rapids.GpuShuffleEnv
import org.apache.spark.sql.rapids.execution.TrampolineUtil
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuShuffleCoalesceSuite extends AnyFunSuite with BeforeAndAfterEach {
  var rapidsConf = new RapidsConf(Map[String, String]())

  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .decimal64Column(-3, RoundingMode.UNNECESSARY ,
          5.1, null, 3.3, 4.4e2, 0, -2.1e-1, 1.111, 2.345, null, 1.23e3)
        .build()) { table =>
      GpuColumnVector.from(table, Array(IntegerType, StringType, DoubleType,
        DecimalType(DType.DECIMAL64_MAX_PRECISION, 3)))
    }
  }

  /**
   * Retrieves the underlying column vectors for a batch. It increments the reference counts for
   * them if needed so the results need to be closed.
   */
  private def extractColumnVectors(batch: ColumnarBatch): Array[ColumnVector] = {
    if (GpuPackedTableColumn.isBatchPacked(batch)) {
      val packedColumn = batch.column(0).asInstanceOf[GpuPackedTableColumn]
      val table = packedColumn.getContiguousTable.getTable
      // The contiguous table is still responsible for closing these columns.
      (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
    } else if (GpuCompressedColumnVector.isBatchCompressed(batch)) {
      val compressedColumn = batch.column(0).asInstanceOf[GpuCompressedColumnVector]
      val descr = compressedColumn.getTableMeta.bufferMeta.codecBufferDescrs(0)
      val codec = TableCompressionCodec.getCodec(
        descr.codec, TableCompressionCodec.makeCodecConfig(rapidsConf))
      withResource(codec.createBatchDecompressor(100 * 1024 * 1024L,
        Cuda.DEFAULT_STREAM)) { decompressor =>
        compressedColumn.getTableBuffer.incRefCount()
        decompressor.addBufferToDecompress(compressedColumn.getTableBuffer,
          compressedColumn.getTableMeta.bufferMeta)
        withResource(decompressor.finishAsync()) { outputBuffers =>
          val outputBuffer = outputBuffers.head
          // There should be only one
          withResource(
            MetaUtils.getTableFromMeta(outputBuffer, compressedColumn.getTableMeta)) { table =>
            (0 until table.getNumberOfColumns).map(i => table.getColumn(i).incRefCount()).toArray
          }
        }
      }
    } else {
      GpuColumnVector.extractBases(batch).map(_.incRefCount())
    }
  }

  private def buildSubBatch(batch: ColumnarBatch, startRow: Int, endRow: Int): ColumnarBatch = {
    withResource(extractColumnVectors(batch)) { columns =>
      val types = GpuColumnVector.extractTypes(batch)
      val sliced = columns.zip(types).map { case (c, t) =>
        GpuColumnVector.from(c.subVector(startRow, endRow), t)
      }
      new ColumnarBatch(sliced.toArray, endRow - startRow)
    }
  }

  private def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    withResource(extractColumnVectors(expected)) { expectedColumns =>
      withResource(extractColumnVectors(actual)) { actualColumns =>
        assertResult(expectedColumns.length)(actualColumns.length)
        expectedColumns.zip(actualColumns).foreach { case (expected, actual) =>
          if (expected.getRowCount == 0) {
            assertResult(expected.getType)(actual.getType)
          } else {
            withResource(expected.equalToNullAware(actual)) { compareVector =>
              withResource(compareVector.all()) { compareResult =>
                assert(compareResult.getBoolean)
              }
            }
          }
        }
      }
    }
  }

  test("GPU kudo partitioning with deserialization") {
    TrampolineUtil.cleanupAnyExistingSession()
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
        .set(RapidsConf.SHUFFLE_KUDO_WRITE_MODE.key, "GPU")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }
      withResource(buildBatch()) { originalBatch =>
        GpuColumnVector.incRefCounts(originalBatch)
        val columns = GpuColumnVector.extractColumns(originalBatch)
        val numRows = originalBatch.numRows
        val dataTypes = GpuColumnVector.extractTypes(originalBatch)

        // Get serialized batches from the partitioning operation
        withResource(gp.sliceInternalGpuOrCpuAndClose(
          numRows, partitionIndices, columns).map(_._1)) { serializedPartitions =>

          // Create an iterator over the serialized partitions to simulate shuffle input
          val serializedIter = serializedPartitions.iterator

          // Set up metrics for the coalescing operation
          val metricsMap = Map(
            GpuMetric.CONCAT_TIME -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.NUM_INPUT_BATCHES -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.NUM_INPUT_ROWS -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.NUM_OUTPUT_BATCHES -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.NUM_OUTPUT_ROWS -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.OP_TIME_LEGACY -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.READ_THROTTLING_TIME -> com.nvidia.spark.rapids.NoopMetric,
            GpuMetric.SYNC_READ_TIME -> com.nvidia.spark.rapids.NoopMetric
          )

          // Create coalesce read option for kudo GPU mode
          val readOption = com.nvidia.spark.rapids.CoalesceReadOption(
            kudoEnabled = true,
            kudoMode = com.nvidia.spark.rapids.RapidsConf.ShuffleKudoMode.GPU,
            kudoDebugMode = DumpOption.Never,
            kudoDebugDumpPrefix = None,
            useAsync = false
          )

          // Get the coalescing iterator that will deserialize the batches
          val coalesceIter = GpuShuffleCoalesceUtils.getGpuShuffleCoalesceIterator(
            serializedIter, Long.MaxValue, dataTypes, readOption, metricsMap)

          // Collect all deserialized batches
          val deserializedBatches = coalesceIter.toArray

          // Verify we got the expected number of partitions
          assertResult(2)(deserializedBatches.length)

          // Verify row counts match expectations
          assertResult(2)(deserializedBatches(0).numRows)
          assertResult(8)(deserializedBatches(1).numRows)

          // Verify the deserialized data matches the original data
          deserializedBatches.zipWithIndex.foreach { case (deserializedBatch, partIndex) =>
            val startRow = partitionIndices(partIndex)
            val endRow = if (partIndex < partitionIndices.length - 1) {
              partitionIndices(partIndex + 1)
            } else {
              originalBatch.numRows
            }

            withResource(buildSubBatch(originalBatch, startRow, endRow)) { expectedBatch =>
              compareBatches(expectedBatch, deserializedBatch)
            }
          }
        }
      }
    }
  }
}
