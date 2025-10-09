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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, StoredBitmap}
import org.apache.spark.sql.delta.storage.dv.HadoopFileSystemDVStore
import org.apache.spark.sql.types.{ByteType, LongType}

trait RapidsRowIndexFilterBase {
  def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector
}

class KeepMarkedRowsFilterBase(bitmap: RoaringBitmapArray) extends RapidsRowIndexFilterBase {
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

class DropMarkedRowsFilterBase(bitmap: RoaringBitmapArray) extends RapidsRowIndexFilterBase {
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

class DropAllRowsFilterBase extends RapidsRowIndexFilterBase {
  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromByte(1.toByte)) { one =>
      GpuColumnVector.from(one, rowIndexCol.getRowCount.toInt, ByteType)
    }
  }
}

class KeepAllRowsFilterBase extends RapidsRowIndexFilterBase {
  override def materializeIntoVector(rowIndexCol: GpuColumnVector): GpuColumnVector = {
    withResource(Scalar.fromByte(0.toByte)) { zero =>
      GpuColumnVector.from(zero, rowIndexCol.getRowCount.toInt, ByteType)
    }
  }
}

trait RowIndexMarkingFiltersBuilderBase {
  def getFilterForEmptyDeletionVector(): RapidsRowIndexFilterBase
  def getFilterForNonEmptyDeletionVector(bitmap: RoaringBitmapArray): RapidsRowIndexFilterBase

  def createInstance(
      deletionVector: DeletionVectorDescriptor,
      hadoopConf: Configuration,
      tablePath: Option[Path]): RapidsRowIndexFilterBase = {
    if (deletionVector.cardinality == 0) {
      getFilterForEmptyDeletionVector()
    } else {
      require(tablePath.nonEmpty, "Table path is required for non-empty deletion vectors")
      val dvStore = new HadoopFileSystemDVStore(hadoopConf)
      val storedBitmap = StoredBitmap.create(deletionVector, tablePath.get)
      val bitmap = storedBitmap.load(dvStore)
      getFilterForNonEmptyDeletionVector(bitmap)
    }
  }
}
