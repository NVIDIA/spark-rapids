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
import java.math.RoundingMode

import ai.rapids.cudf.{ColumnVector, Cuda, DType, Table}
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsDiskBlockManager}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuPartitioningSuite extends FunSuite with Arm {
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
      val codec = TableCompressionCodec.getCodec(descr.codec)
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

  test("GPU partition") {
    SparkSession.getActiveSession.foreach(_.close())
    val conf = new SparkConf().set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "none")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val partitionIndices = Array(0, 2, 2)
      val gp = new GpuPartitioning {
        override val numPartitions: Int = partitionIndices.length
      }
      withResource(buildBatch()) { batch =>
        val columns = GpuColumnVector.extractColumns(batch)
        val numRows = batch.numRows
        withResource(gp.sliceInternalOnGpu(numRows, partitionIndices, columns)) { partitions =>
          partitions.zipWithIndex.foreach { case (partBatch, partIndex) =>
            val startRow = partitionIndices(partIndex)
            val endRow = if (partIndex < partitionIndices.length - 1) {
              partitionIndices(partIndex + 1)
            } else {
              batch.numRows
            }
            val expectedRows = endRow - startRow
            assertResult(expectedRows)(partBatch.numRows)
            assert(GpuPackedTableColumn.isBatchPacked(partBatch))
            withResource(buildSubBatch(batch, startRow, endRow)) { expectedBatch =>
              compareBatches(expectedBatch, partBatch)
            }
          }
        }
      }
    }
  }

  test("GPU partition with compression") {
    val conf = new SparkConf()
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "lz4")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf))
      val spillPriority = 7L
      val catalog = RapidsBufferCatalog.singleton
      withResource(new RapidsDeviceMemoryStore(catalog)) { deviceStore =>
        val partitionIndices = Array(0, 2, 2)
        val gp = new GpuPartitioning {
          override val numPartitions: Int = partitionIndices.length
        }
        withResource(buildBatch()) { batch =>
          val columns = GpuColumnVector.extractColumns(batch)
          val sparkTypes = GpuColumnVector.extractTypes(batch)
          val numRows = batch.numRows
          withResource(gp.sliceInternalOnGpu(numRows, partitionIndices, columns)) { partitions =>
            partitions.zipWithIndex.foreach { case (partBatch, partIndex) =>
              val startRow = partitionIndices(partIndex)
              val endRow = if (partIndex < partitionIndices.length - 1) {
                partitionIndices(partIndex + 1)
              } else {
                batch.numRows
              }
              val expectedRows = endRow - startRow
              assertResult(expectedRows)(partBatch.numRows)
              val columns = (0 until partBatch.numCols).map(i => partBatch.column(i))
              columns.foreach { column =>
                // batches with any rows should be compressed, and
                // batches with no rows should not be compressed.
                val actualRows = column match {
                  case c: GpuCompressedColumnVector =>
                    val rows = c.getTableMeta.rowCount
                    assert(rows != 0)
                    rows
                  case c: GpuPackedTableColumn =>
                    val rows = c.getContiguousTable.getRowCount
                    assert(rows == 0)
                    rows
                  case _ =>
                    throw new IllegalStateException("column should either be compressed or packed")
                }
                assertResult(expectedRows)(actualRows)
              }
              if (GpuCompressedColumnVector.isBatchCompressed(partBatch)) {
                val gccv = columns.head.asInstanceOf[GpuCompressedColumnVector]
                val bufferId = MockRapidsBufferId(partIndex)
                val devBuffer = gccv.getTableBuffer
                // device store takes ownership of the buffer
                devBuffer.incRefCount()
                deviceStore.addBuffer(bufferId, devBuffer, gccv.getTableMeta, spillPriority)
                withResource(buildSubBatch(batch, startRow, endRow)) { expectedBatch =>
                  withResource(catalog.acquireBuffer(bufferId)) { buffer =>
                    withResource(buffer.getColumnarBatch(sparkTypes)) { batch =>
                      compareBatches(expectedBatch, batch)
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

case class MockRapidsBufferId(tableId: Int) extends RapidsBufferId {
  override def getDiskPath(diskBlockManager: RapidsDiskBlockManager): File =
    throw new UnsupportedOperationException
}
