/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, Table}
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

  private def buildSubBatch(batch: ColumnarBatch, startRow: Int, endRow: Int): ColumnarBatch = {
    val columns = GpuColumnVector.extractBases(batch)
    val types = GpuColumnVector.extractTypes(batch)
    val sliced = columns.zip(types).map { case (c, t) =>
      GpuColumnVector.from(c.subVector(startRow, endRow), t)
    }
    new ColumnarBatch(sliced.toArray, endRow - startRow)
  }

  private def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    assertResult(expected.numCols)(actual.numCols)
    val expectedColumns = GpuColumnVector.extractBases(expected)
    val actualColumns = GpuColumnVector.extractBases(expected)
    expectedColumns.zip(actualColumns).foreach { case (expected, actual) =>
      // FIXME: For decimal types, NULL_EQUALS has not been supported in cuDF yet
      val cpVec = if (expected.getType.isDecimalType) {
        expected.equalTo(actual)
      } else {
        expected.equalToNullAware(actual)
      }
      withResource(cpVec) { compareVector =>
        withResource(compareVector.all()) { compareResult =>
          assert(compareResult.getBoolean)
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
            val columns = (0 until partBatch.numCols).map(i => partBatch.column(i))
            columns.foreach { column =>
              assert(column.isInstanceOf[GpuColumnVectorFromBuffer])
              assertResult(expectedRows)(column.asInstanceOf[GpuColumnVector].getRowCount)
            }
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
                  case c: GpuColumnVector =>
                    val rows = c.getRowCount
                    assert(rows == 0)
                    rows
                }
                assertResult(expectedRows)(actualRows)
              }
              if (GpuCompressedColumnVector.isBatchCompressed(partBatch)) {
                val gccv = columns.head.asInstanceOf[GpuCompressedColumnVector]
                val bufferId = MockRapidsBufferId(partIndex)
                val devBuffer = gccv.getBuffer
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
