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

import ai.rapids.cudf.{Cuda, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.rapids.{GpuShuffleEnv, RapidsDiskBlockManager}
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuPartitioningSuite extends FunSuite with Arm {
  private def buildBatch(): ColumnarBatch = {
    withResource(new Table.TestBuilder()
        .column(5, null.asInstanceOf[java.lang.Integer], 3, 1, 1, 1, 1, 1, 1, 1)
        .column("five", "two", null, null, "one", "one", "one", "one", "one", "one")
        .column(5.0, 2.0, 3.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0)
        .build()) { table =>
      GpuColumnVector.from(table)
    }
  }

  private def buildSubBatch(batch: ColumnarBatch, startRow: Int, endRow: Int): ColumnarBatch = {
    val columns = GpuColumnVector.extractBases(batch)
    val sliced = columns.safeMap(c => GpuColumnVector.from(c.subVector(startRow, endRow)))
    new ColumnarBatch(sliced.toArray, endRow - startRow)
  }

  private def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    assertResult(expected.numCols)(actual.numCols)
    val expectedColumns = GpuColumnVector.extractBases(expected)
    val actualColumns = GpuColumnVector.extractBases(expected)
    expectedColumns.zip(actualColumns).foreach { case (expected, actual) =>
      withResource(expected.equalToNullAware(actual)) { compareVector =>
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
      GpuShuffleEnv.init(new RapidsConf(conf), Cuda.memGetInfo())
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
        .set(RapidsConf.SHUFFLE_COMPRESSION_CODEC.key, "copy")
    TestUtils.withGpuSparkSession(conf) { _ =>
      GpuShuffleEnv.init(new RapidsConf(conf), Cuda.memGetInfo())
      val spillPriority = 7L
      val catalog = new RapidsBufferCatalog
      withResource(new RapidsDeviceMemoryStore(catalog)) { deviceStore =>
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
                assert(column.isInstanceOf[GpuCompressedColumnVector])
                assertResult(expectedRows) {
                  column.asInstanceOf[GpuCompressedColumnVector].getTableMeta.rowCount
                }
              }
              val gccv = columns.head.asInstanceOf[GpuCompressedColumnVector]
              val bufferId = MockRapidsBufferId(partIndex)
              val devBuffer = gccv.getBuffer.slice(0, gccv.getBuffer.getLength)
              deviceStore.addBuffer(bufferId, devBuffer, gccv.getTableMeta, spillPriority)
              withResource(buildSubBatch(batch, startRow, endRow)) { expectedBatch =>
                withResource(catalog.acquireBuffer(bufferId)) { buffer =>
                  compareBatches(expectedBatch, buffer.getColumnarBatch)
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
