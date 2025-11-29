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

import ai.rapids.cudf.{DType, HostColumnVector}
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsHostColumnBuilder}
import com.nvidia.spark.rapids.Arm.withResource
import scala.collection._

import org.apache.spark.sql.delta.deletionvectors.RoaringBitmapArray
import org.apache.spark.sql.types.{ByteType, LongType}

trait RapidsRowIndexFilter {
  protected val EMPTY_BITMAP = new RoaringBitmapArray()
  def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector
  def getBitmap: RoaringBitmapArray = EMPTY_BITMAP
  protected def getMarkedRowIndices: HostColumnVector =
    HostColumnVector.fromLongs(Array.empty[Long]: _*)
}

/**
 * This creates a Rapids filter to create a skip_row column that will keep all the rows marked
 * in the bitmap. It's the inverse of RapidsDropMarkedRowsFilter
 */
class RapidsKeepMarkedRowsFilter(bitmap: Option[RoaringBitmapArray]) extends RapidsRowIndexFilter {

  def this(bitmap: RoaringBitmapArray) = {
    this(Some(bitmap))
  }

  override def getBitmap: RoaringBitmapArray = bitmap.get

  override def getMarkedRowIndices: HostColumnVector = {
    val arr = getBitmap.toArray
    withResource(new RapidsHostColumnBuilder(
        new HostColumnVector.BasicType(false, DType.INT64), arr.length)) { builder =>
      builder.appendArray(arr: _*)
      builder.build()
    }
  }

  def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val containsMarkedRows =
      withResource(getMarkedRowIndices) { hostArray =>
        withResource(GpuColumnVector.from(hostArray.copyToDevice(), LongType)) {
          markedRowIndicesCol =>
            rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
        }
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
class RapidsDropMarkedRowsFilter(bitmap: Option[RoaringBitmapArray]) extends RapidsRowIndexFilter {

  def this(bitmap: RoaringBitmapArray) = {
    this(Some(bitmap))
  }

  override def getBitmap: RoaringBitmapArray = bitmap.get

  override def getMarkedRowIndices: HostColumnVector = {
    val arr = getBitmap.toArray
    withResource(new RapidsHostColumnBuilder(
      new HostColumnVector.BasicType(false, DType.INT64), arr.length)) { builder =>
      builder.appendArray(arr: _*)
      builder.build()
    }
  }

  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    val containsMarkedRows =
      withResource(getMarkedRowIndices) { hostArray =>
        withResource(GpuColumnVector.from(hostArray.copyToDevice(), LongType)) {
          markedRowIndicesCol =>
            rowIndexCol.getBase.contains(markedRowIndicesCol.getBase)
        }
      }
    withResource(containsMarkedRows) { containsMarkedRows =>
      GpuColumnVector.from(containsMarkedRows.castTo(DType.INT8), ByteType)
    }
  }
}

final class CoalescedRapidsDropMarkedRowsFilter(
  filters: Seq[RapidsRowIndexFilter],
  offsets: Seq[Long]) extends RapidsDropMarkedRowsFilter(None) {
  override def getMarkedRowIndices: HostColumnVector = {
    val size = filters.map(_.getBitmap.cardinality).sum.toInt
    if (size > 0) {
        withResource(
          new RapidsHostColumnBuilder(new HostColumnVector.BasicType(false, DType.INT64), size)) {
          builder =>
            // add the first filter without offsets
            builder.appendArray(filters.head.getBitmap.toArray: _*)
            // drop the first filter and the offset which is zero anyway
            filters.tail.zip(offsets.tail).map {
              case (filter, offset) =>
                if (!filter.getBitmap.isEmpty) {
                  val arr = filter.getBitmap.toArray
                  arr.foreach(i => builder.append(i + offset))
                }
            }
            builder.build()
        }
    } else {
      HostColumnVector.fromLongs(Array.empty[Long]: _*)
    }
  }
}

object RapidsKeepAllRowsFilter extends RapidsDropMarkedRowsFilter(None) {
  override def getBitmap: RoaringBitmapArray = EMPTY_BITMAP
}
