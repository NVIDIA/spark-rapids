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

import scala.collection.JavaConverters._

import ai.rapids.cudf.{CloseableArray,  ColumnVector => CudfColumnVector, DType, Scalar, Table}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.rapids.{GpuColumnVector, RapidsHostColumnVector, ShimReflectionUtils}
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitioner.toRows
import com.nvidia.spark.rapids.jni.Hash
import org.apache.hadoop.shaded.org.apache.commons.lang3.reflect.MethodUtils
import org.apache.iceberg.{PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.{GpuTypeToSparkType, SparkStructLike}
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.types.Types

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuIcebergPartitioner(val spec: PartitionSpec, val schema: Schema,
    val dataSparkType: StructType) {
  private val sparkType: Array[DataType] = dataSparkType.fields.map(_.dataType)
  private val partitionSparkType: StructType = GpuTypeToSparkType.toSparkType(spec.partitionType())

  private val fieldTransforms: Seq[FieldTransform] = spec.fields()
    .asScala
    .map(f => FieldTransform(fieldIndex(schema, f.fieldId()), f.transform()))
    .toSeq

  val keyCols: Array[Int] = (0 until spec.fields().size()).toArray

  def partition(input: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    val numRows = input.numRows()
    val partitionValues = new Table(
      fieldTransforms.safeMap(_.transform(input)).toArray:_*)

    val distinctPartitionValues = partitionValues
      .dropDuplicates(keyCols, DuplicateKeepOption.KEEP_FIRST, true)

    val hostDistPartValues = toRows(spec.partitionType(), partitionSparkType,
      distinctPartitionValues)

    val numPartitions = toIntExact(distinctPartitionValues.getRowCount)

    withResource(partitionValues
      .leftDistinctJoinGatherMap(distinctPartitionValues, true)) { partitionMap =>
      withResource(partitionMap.toColumnView(0, input.numRows())) { partitionMapCv =>
        val partitionedTable = GpuColumnVector.from(input).partition(partitionMapCv,
          toIntExact(partitionMapCv.getRowCount))
        withResource(partitionedTable) { _ =>
          val partitionOffset = partitionedTable.getPartitions :+ numRows

          (0 until numPartitions).flatMap { partitionIdx =>
            val partitionRows = partitionOffset(partitionIdx + 1) - partitionOffset(partitionIdx)
            if (partitionRows > 0) {
              withResource(Scalar.fromInt(partitionOffset(partitionIdx))) { startRow =>
                withResource(CudfColumnVector.sequence(startRow, partitionRows)) { gatherMap =>
                  withResource(partitionedTable.getTable.gather(gatherMap)) { table =>
                    Some(ColumnarBatchWithPartition(GpuColumnVector.from(table, sparkType),
                      hostDistPartValues(partitionIdx)))
                  }
                }
              }
            } else {
              None
            }
          }
        }
      }
    }
  }
}

case class ColumnarBatchWithPartition(batch: ColumnarBatch, partition: StructLike)

private case class FieldTransform(fieldPos: Int, transform: Transform[_, _]) {
  require(GpuIcebergPartitioner.isBucketTransform(transform),
    s"Currently only bucket transform is supported, but got $transform")

  private val numBuckets = GpuIcebergPartitioner.getNumBuckets(transform)

  def transform(input: ColumnarBatch): CudfColumnVector = {
    val cv = input.column(fieldPos)
      .asInstanceOf[GpuColumnVector]
      .getBase

    withResource(Scalar.fromInt(numBuckets)) { b =>
      Hash.murmurHash32(0, Array(cv)).mod(b, DType.INT32)
    }
  }
}

object GpuIcebergPartitioner {
  private val bucketTransformClass = ShimReflectionUtils
    .loadClass("org.apache.iceberg.transforms.Bucket")

  def isBucketTransform(transform: Transform[_, _]): Boolean = {
    bucketTransformClass.isInstance(transform)
  }

  def getNumBuckets(transform: Transform[_, _]): Int = {
    MethodUtils.invokeMethod(transform, "numBuckets")
      .asInstanceOf[java.lang.Integer]
  }

  def toRows(icebergType: Types.StructType, sparkType: StructType,
      table: Table): Array[SparkStructLike] = {
    val numCols = table.getNumberOfColumns

    withResource(CloseableArray.wrap(new Array[ColumnVector](numCols))) { hostColArrays =>
      for (i <- 0 until numCols) {
        hostColArrays.set(i, new RapidsHostColumnVector(sparkType(i).dataType,
          table.getColumn(i).copyToHost()))
      }

      withResource(new ColumnarBatch(hostColArrays.release())) { hostBatch =>
        hostBatch.rowIterator()
          .asScala
          .map(internalRow => {
            val rowValues = new Array[Any](numCols)
            for (colIdx <- 0 until numCols) {
              rowValues(colIdx) = internalRow.get(colIdx, sparkType.fields(colIdx).dataType)
            }
            val row = new GenericRowWithSchema(rowValues, sparkType)
            new SparkStructLike(icebergType).wrap(row)
          }).toArray
      }
    }
  }
}