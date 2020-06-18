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

import ai.rapids.cudf.{BufferType, ColumnVector, HostColumnVector, Table}
import org.scalatest.Assertions

import org.apache.spark.sql.vectorized.ColumnarBatch

/** A collection of utility methods useful in tests. */
object TestUtils extends Assertions with Arm {
  /** Compare the equality of two tables */
  def compareTables(expected: Table, actual: Table): Unit = {
    assertResult(expected.getRowCount)(actual.getRowCount)
    assertResult(expected.getNumberOfColumns)(actual.getNumberOfColumns)
    (0 until expected.getNumberOfColumns).foreach { i =>
      compareColumns(expected.getColumn(i), actual.getColumn(i))
    }
  }

  /** Compare the equality of two [[ColumnarBatch]] instances */
  def compareBatches(expected: ColumnarBatch, actual: ColumnarBatch): Unit = {
    assertResult(expected.numRows)(actual.numRows)
    assertResult(expected.numCols)(actual.numCols)
    (0 until expected.numCols).foreach { i =>
      compareColumns(expected.column(i).asInstanceOf[GpuColumnVector].getBase,
        actual.column(i).asInstanceOf[GpuColumnVector].getBase)
    }
  }

  /** Compre the equality of two `ColumnVector` instances */
  def compareColumns(expected: ColumnVector, actual: ColumnVector): Unit = {
    assertResult(expected.getType)(actual.getType)
    assertResult(expected.getRowCount)(actual.getRowCount)
    withResource(expected.copyToHost()) { expectedHost =>
      withResource(actual.copyToHost()) { actualHost =>
        compareColumnBuffers(expectedHost, actualHost, BufferType.DATA)
        compareColumnBuffers(expectedHost, actualHost, BufferType.VALIDITY)
        compareColumnBuffers(expectedHost, actualHost, BufferType.OFFSET)
      }
    }
  }

  private def compareColumnBuffers(
      expected: HostColumnVector,
      actual: HostColumnVector,
      bufferType: BufferType): Unit = {
    val expectedBuffer = expected.getHostBufferFor(bufferType)
    val actualBuffer = actual.getHostBufferFor(bufferType)
    if (expectedBuffer != null) {
      assertResult(expectedBuffer.asByteBuffer())(actualBuffer.asByteBuffer())
    } else {
      assertResult(null)(actualBuffer)
    }
  }
}
