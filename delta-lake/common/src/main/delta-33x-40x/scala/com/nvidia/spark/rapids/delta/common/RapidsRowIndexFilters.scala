/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.common

import ai.rapids.cudf.{ColumnVector, DType, Scalar}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.GpuColumnVector

import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.types.{ByteType, LongType}

trait RapidsRowIndexFilter {
  private val EMPTY_BITMAP = new RoaringBitmapArray()
  def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector
  def getBitmap: RoaringBitmapArray = EMPTY_BITMAP
}

/**
 * This creates a Rapids filter to create a skip_row column that will keep all the rows marked
 * in the bitmap. It's the inverse of RapidsDropMarkedRowsFilter
 */
final class RapidsKeepMarkedRowsFilter(bitmap: RoaringBitmapArray) extends RapidsRowIndexFilter {

  override def getBitmap: RoaringBitmapArray = bitmap

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val markedRowIndices = bitmap.toArray
    val containsMarkedRows =
      withResource(GpuColumnVector.from(ColumnVector.fromLongs(markedRowIndices: _*), LongType)) {
        markedRowIndicesCol =>
          rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
      }
    val indicesToDelete = withResource(containsMarkedRows) { containsMarkedRows =>
      containsMarkedRows.not()
    }
    withResource(indicesToDelete) { indicesToDelete =>
      GpuColumnVector.from(indicesToDelete.castTo(DType.INT8), ByteType)
    }
  }
}

/**
 * This creates a Rapids filter to create a skip_row column that will drop all the rows marked
 * in the bitmap.
 */
final class RapidsDropMarkedRowsFilter(bitmap: RoaringBitmapArray) extends RapidsRowIndexFilter {

  override def getBitmap: RoaringBitmapArray = bitmap

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val markedRowIndices = bitmap.toArray
    val containsMarkedRows =
      withResource(GpuColumnVector.from(ColumnVector.fromLongs(markedRowIndices: _*), LongType)) {
        markedRowIndicesCol =>
          rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
      }
    withResource(containsMarkedRows) { containsMarkedRows =>
      GpuColumnVector.from(containsMarkedRows.castTo(DType.INT8), ByteType)
    }
  }
}

final class CoalescedRapidsKeepMarkedRowsFilter(
  filters: Seq[RapidsRowIndexFilter],
  offsets: Seq[Long]) extends RapidsRowIndexFilter {

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val markedRowIndices = filters.zip(offsets).map { case (filter, offset) =>
      filter.getBitmap.toArray.map(i => i + offset)
    }.flatten
    val containsMarkedRows =
      withResource(GpuColumnVector.from(ColumnVector.fromLongs(markedRowIndices: _*), LongType)) {
        markedRowIndicesCol =>
          rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
      }
    val indicesToDelete = withResource(containsMarkedRows) { containsMarkedRows =>
      containsMarkedRows.not()
    }
    withResource(indicesToDelete) { indicesToDelete =>
      GpuColumnVector.from(indicesToDelete.castTo(DType.INT8), ByteType)
    }
  }
}

final class CoalescedRapidsDropMarkedRowsFilter(
  filters: Seq[RapidsRowIndexFilter],
  offsets: Seq[Long]) extends RapidsRowIndexFilter {

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val markedRowIndices = filters.zip(offsets).map { case (filter, offset) =>
      filter.getBitmap.toArray.map(i => i + offset)
    }.flatten
    val containsMarkedRows =
      withResource(GpuColumnVector.from(ColumnVector.fromLongs(markedRowIndices: _*), LongType)) {
        markedRowIndicesCol =>
          rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
      }
    withResource(containsMarkedRows) { containsMarkedRows =>
      GpuColumnVector.from(containsMarkedRows.castTo(DType.INT8), ByteType)
    }
  }
}

object RapidsDropAllRowsFilter extends RapidsRowIndexFilter {

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromByte(1.toByte)) { one =>
      GpuColumnVector.from(one, rowIndexCol.getRowCount.toInt, ByteType)
    }
  }
}

object RapidsKeepAllRowsFilter extends RapidsRowIndexFilter {

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      GpuColumnVector.from(zero, rowIndexCol.getRowCount.toInt, ByteType)
    }
  }
}
