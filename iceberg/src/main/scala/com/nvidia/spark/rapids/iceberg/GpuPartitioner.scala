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

package com.nvidia.spark.rapids.iceberg

import java.lang.Math.toIntExact

import ai.rapids.cudf.{CloseableArray, DType, Scalar, Table, ColumnVector => CudfColumnVector}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.rapids.{GpuColumnVector, ShimReflectionUtils}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.jni.Hash
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.MethodUtils
import org.apache.iceberg.{PartitionSpec, Schema, StructLike}
import org.apache.iceberg.transforms.Transform
import scala.collection.JavaConverters._

import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq

import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuPartitioner(val spec: PartitionSpec, val schema: Schema) {

  private val fieldTransforms: Seq[FieldTransform] = spec.fields()
    .asScala
    .map(f => FieldTransform(fieldIndex(schema, f.fieldId()), f.transform()))
    .toSeq

  val keyCols: Array[Int] = (0 until spec.fields().size()).toArray

  def partition(input: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    val partitionTransformValues = new Table(
      fieldTransforms.safeMap(_.transform(input)).toArray:_*)

    val distinctPartitionTransformValues = partitionTransformValues
      .dropDuplicates(keyCols, DuplicateKeepOption.KEEP_FIRST, true)

    withResource(partitionTransformValues
      .leftDistinctJoinGatherMap(distinctPartitionTransformValues, true)) { partitionMap =>
      withResource(partitionMap.toColumnView(0, input.numRows())) { partitionMapCv =>
        val partitionedTable = GpuColumnVector.from(input).partition(partitionMapCv,
          toIntExact(partitionMapCv.getRowCount))
        withResource(partitionedTable) { _ =>
        }
      }
    }
  }
}

case class ColumnarBatchWithPartition(batch: ColumnarBatch, partition: StructLike)

private case class FieldTransform(fieldPos: Int, transform: Transform[_, _]) {
  require(GpuPartitioner.isBucketTransform(transform),
    s"Currently only bucket transform is supported, but got $transform")

  private val numBuckets = GpuPartitioner.getNumBuckets(transform)

  def transform(input: ColumnarBatch): CudfColumnVector = {
    val cv = input.column(fieldPos)
      .asInstanceOf[GpuColumnVector]
      .getBase

    withResource(Scalar.fromInt(numBuckets)) { b =>
      Hash.murmurHash32(0, Array(cv)).mod(b, DType.INT32)
    }
  }
}

object GpuPartitioner {
  private val bucketTransformClass = ShimReflectionUtils
    .loadClass("org.apache.iceberg.transforms.Bucket")

  def isBucketTransform(transform: Transform[_, _]): Boolean = {
    bucketTransformClass.isInstance(transform)
  }

  def getNumBuckets(transform: Transform[_, _]): Int = {
    MethodUtils.invokeMethod(transform, "numBuckets")
      .asInstanceOf[java.lang.Integer]
  }
}