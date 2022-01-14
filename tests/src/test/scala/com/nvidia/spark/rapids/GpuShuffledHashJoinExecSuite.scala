/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream}

import ai.rapids.cudf.{ColumnVector, HostMemoryBuffer, JCudfSerialization, Table}
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mockito.MockitoSugar

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuShuffledHashJoinExecSuite extends FunSuite with Arm with MockitoSugar {
  test("fallback with empty build iterator") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val mockBuildIter = mock[Iterator[ColumnarBatch]]
      when(mockBuildIter.hasNext).thenReturn(false)
      val mockStreamIter = mock[Iterator[ColumnarBatch]]
      val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
        0,
        Seq.empty,
        mockBuildIter,
        mockStreamIter,
        mock[GpuMetric],
        mock[GpuMetric])
      withResource(builtBatch) { _ =>
        // we ge an empty batch with no columns or rows
        assertResult(builtBatch.numCols())(0)
        assertResult(builtBatch.numRows())(0)
        // 2 invocations, once in the `getBuiltBatchAndStreamIter`
        // method, and a second one in `getSingleBatchWithVerification`
        verify(mockBuildIter, times(2)).hasNext
        verify(mockBuildIter, times(0)).next
        verify(mockStreamIter, times(0)).hasNext
      }
    }
  }

  test("fallback with 0 column build batches") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      withResource(GpuColumnVector.emptyBatchFromTypes(Array.empty)) {
        emptyBatch =>
          val buildIter = mock[Iterator[ColumnarBatch]]
          when(buildIter.hasNext).thenReturn(true, false)
          val buildBufferedIter = mock[BufferedIterator[ColumnarBatch]]
          when(buildBufferedIter.hasNext).thenReturn(true, false)
          when(buildBufferedIter.head).thenReturn(emptyBatch)
          when(buildBufferedIter.next).thenReturn(emptyBatch)
          when(buildIter.buffered).thenReturn(buildBufferedIter)
          val mockStreamIter = mock[Iterator[ColumnarBatch]]
          val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
            0,
            Seq.empty,
            buildIter,
            mockStreamIter,
            mock[GpuMetric],
            mock[GpuMetric])
          withResource(builtBatch) { _ =>
            assertResult(builtBatch.numCols())(0)
            assertResult(builtBatch.numRows())(0)
            // 1 invocation in the `getBuiltBatchAndStreamIter`
            // after which a buffered iterator is obtained and used for the fallback case
            verify(buildIter, times(1)).hasNext
            verify(buildIter, times(1)).buffered
            // we ask the buffered iterator for `head` to inspect the number of columns
            verify(buildBufferedIter, times(1)).head
            // the buffered iterator is passed to `getSingleBatchWithVerification`,
            // and that code calls hasNext twice
            verify(buildBufferedIter, times(2)).hasNext
            // and calls next to get that batch we buffered
            verify(buildBufferedIter, times(1)).next
            verify(mockStreamIter, times(0)).hasNext
          }
      }
    }
  }

  test("fallback with a non-SerializedTableColumn 1 col and 0 rows") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      val emptyBatch = GpuColumnVector.emptyBatchFromTypes(Seq(IntegerType).toArray)
      val buildIter = Seq(emptyBatch).iterator
      val mockStreamIter = mock[Iterator[ColumnarBatch]]
      val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
        0,
        Seq.empty,
        buildIter,
        mockStreamIter,
        mock[GpuMetric],
        mock[GpuMetric])
      withResource(builtBatch) { _ =>
        assertResult(builtBatch.numCols())(1)
        assertResult(builtBatch.numRows())(0)
        // 2 invocations, once in the `getBuiltBatchAndStreamIter
        // method, and one in `getSingleBatchWithVerification`
        verify(mockStreamIter, times(0)).hasNext
        // the buffered iterator drained the build iterator
        assertResult(buildIter.hasNext)(false)
      }
    }
  }

  test("fallback with a non-SerialiedTableColumn") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      closeOnExcept(ColumnVector.fromInts(1, 2, 3, 4, 5)) { cudfCol =>
        val cv = GpuColumnVector.from(cudfCol, IntegerType)
        val batch = new ColumnarBatch(Seq(cv).toArray, 5)
        val buildIter = Seq(batch).iterator
        val mockStreamIter = mock[Iterator[ColumnarBatch]]
        val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
          0,
          Seq.empty,
          buildIter,
          mockStreamIter,
          mock[GpuMetric],
          mock[GpuMetric])
        withResource(builtBatch) { _ =>
          assertResult(builtBatch.numCols())(1)
          assertResult(builtBatch.numRows())(5)
          // 2 invocations, once in the `getBuiltBatchAndStreamIter
          // method, and one in `getSingleBatchWithVerification`
          verify(mockStreamIter, times(0)).hasNext
          // the buffered iterator drained the build iterator
          assertResult(buildIter.hasNext)(false)
        }
      }
    }
  }

  def getSerializedBatch(tbl: Table): ColumnarBatch = {
    val outStream = new ByteArrayOutputStream()
    JCudfSerialization.writeToStream(tbl, outStream, 0, tbl.getRowCount)
    val dIn = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray))
    val header = new JCudfSerialization.SerializedTableHeader(dIn)
    closeOnExcept(HostMemoryBuffer.allocate(header.getDataLen, false)) { hostBuffer =>
      JCudfSerialization.readTableIntoBuffer(dIn, header, hostBuffer)
      SerializedTableColumn.from(header, hostBuffer)
    }
  }

  test("test a SerializedTableColumn") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      closeOnExcept(ColumnVector.fromInts(1, 2, 3, 4, 5)) { cudfCol =>
        val cv = GpuColumnVector.from(cudfCol, IntegerType)
        val batch = new ColumnarBatch(Seq(cv).toArray, 5)
        withResource(GpuColumnVector.from(batch)) { tbl =>
          val serializedBatch = getSerializedBatch(tbl)
          val mockStreamIter = mock[Iterator[ColumnarBatch]]
          val mockBufferedStreamIterator = mock[BufferedIterator[ColumnarBatch]]
          when(mockStreamIter.hasNext).thenReturn(true)
          when(mockStreamIter.buffered).thenReturn(mockBufferedStreamIterator)
          when(mockBufferedStreamIterator.hasNext).thenReturn(true)
          closeOnExcept(serializedBatch) { _ =>
            val buildIter = Seq(serializedBatch).iterator
            val attrs = AttributeReference("a", IntegerType, false)() :: Nil
            val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
              1024,
              attrs,
              buildIter,
              mockStreamIter,
              mock[GpuMetric],
              mock[GpuMetric])
            withResource(builtBatch) { _ =>
              verify(mockStreamIter, times(1)).hasNext
              assertResult(builtBatch.numCols())(1)
              assertResult(builtBatch.numRows())(5)
              // the buffered iterator drained the build iterator
              assertResult(buildIter.hasNext)(false)
            }
          }
        }
      }
    }
  }

  test("test two batches, going over the limit") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      closeOnExcept(ColumnVector.fromInts(1, 2, 3, 4, 5)) { cudfCol =>
        val cv = GpuColumnVector.from(cudfCol, IntegerType)
        val batch = new ColumnarBatch(Seq(cv).toArray, 5)
        withResource(GpuColumnVector.from(batch)) { tbl =>
          val serializedBatch = getSerializedBatch(tbl)
          val serializedBatch2 = getSerializedBatch(tbl)
          val mockStreamIter = mock[Iterator[ColumnarBatch]]
          val mockBufferedStreamIterator = mock[BufferedIterator[ColumnarBatch]]
          when(mockStreamIter.hasNext).thenReturn(true)
          when(mockStreamIter.buffered).thenReturn(mockBufferedStreamIterator)
          when(mockBufferedStreamIterator.hasNext).thenReturn(true)
          closeOnExcept(serializedBatch) { _ =>
            closeOnExcept(serializedBatch2) { _ =>
              val buildIter = Seq(serializedBatch, serializedBatch2).iterator
              val attrs = AttributeReference("a", IntegerType, false)() :: Nil
              val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
                0,
                attrs,
                buildIter,
                mockStreamIter,
                mock[GpuMetric],
                mock[GpuMetric])
              withResource(builtBatch) { _ =>
                verify(mockStreamIter, times(1)).hasNext
                assertResult(builtBatch.numCols())(1)
                assertResult(builtBatch.numRows())(10)
                // the buffered iterator drained the build iterator
                assertResult(buildIter.hasNext)(false)
              }
            }
          }
        }
      }
    }
  }

  test("test two batches, stating within the limit") {
    TestUtils.withGpuSparkSession(new SparkConf()) { _ =>
      closeOnExcept(ColumnVector.fromInts(1, 2, 3, 4, 5)) { cudfCol =>
        val cv = GpuColumnVector.from(cudfCol, IntegerType)
        val batch = new ColumnarBatch(Seq(cv).toArray, 5)
        withResource(GpuColumnVector.from(batch)) { tbl =>
          val serializedBatch = getSerializedBatch(tbl)
          val serializedBatch2 = getSerializedBatch(tbl)
          val mockStreamIter = mock[Iterator[ColumnarBatch]]
          val mockBufferedStreamIterator = mock[BufferedIterator[ColumnarBatch]]
          when(mockStreamIter.hasNext).thenReturn(true)
          when(mockStreamIter.buffered).thenReturn(mockBufferedStreamIterator)
          when(mockBufferedStreamIterator.hasNext).thenReturn(true)
          closeOnExcept(serializedBatch) { _ =>
            closeOnExcept(serializedBatch2) { _ =>
              val buildIter = Seq(serializedBatch, serializedBatch2).iterator
              val attrs = AttributeReference("a", IntegerType, false)() :: Nil
              val (builtBatch, bStreamIter) = GpuShuffledHashJoinExec.getBuiltBatchAndStreamIter(
                1024,
                attrs,
                buildIter,
                mockStreamIter,
                mock[GpuMetric],
                mock[GpuMetric])
              withResource(builtBatch) { _ =>
                verify(mockStreamIter, times(1)).hasNext
                assertResult(builtBatch.numCols())(1)
                assertResult(builtBatch.numRows())(10)
                // the buffered iterator drained the build iterator
                assertResult(buildIter.hasNext)(false)
              }
            }
          }
        }
      }
    }
  }
}
