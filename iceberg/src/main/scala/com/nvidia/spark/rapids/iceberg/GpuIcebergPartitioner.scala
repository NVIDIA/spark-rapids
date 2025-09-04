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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar, Table}
import ai.rapids.cudf.Table.DuplicateKeepOption
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, GpuExpression, GpuLiteral, RapidsHostColumnVector}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitioner.toPartitionKeys
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.{GpuTypeToSparkType, SparkStructLike}
import org.apache.iceberg.spark.functions.GpuBucketExpression
import org.apache.iceberg.types.Types

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * A GPU based Iceberg partitioner that partitions the input columnar batch into multiple
 * columnar batches based on the given partition spec.
 *
 * @param spec the iceberg partition spec
 * @param dataSparkType the spark schema of the input data
 */
class GpuIcebergPartitioner(val spec: PartitionSpec,
  val dataSparkType: StructType) {
  require(spec.isPartitioned, "Should not create a partitioner for unpartitioned table")
  private val inputSchema: Schema = spec.schema()
  private val sparkType: Array[DataType] = dataSparkType.fields.map(_.dataType)
  private val partitionSparkType: StructType = GpuTypeToSparkType.toSparkType(spec.partitionType())

  private val partitionExprs: Seq[GpuExpression] = spec.fields().asScala.map(getPartitionExpr).toSeq

  private val keyCols: Array[Int] = (0 until spec.fields().size()).toArray

  /**
   * Partition the `input` columnar batch using iceberg's partition spec. It's the caller's
   * responsibility to close the input columnar batch.
   */
  def partition(input: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    if (input.numRows() == 0) {
      return Seq.empty
    }

    val numRows = input.numRows()

    val partitionKeysTable = withResource(partitionExprs.safeMap(_.columnarEval(input))) { parts =>
        val arr = new Array[CudfColumnVector](partitionExprs.size)
        for (i <- partitionExprs.indices) {
          arr(i) = parts(i).getBase
        }
        new Table(arr:_*)
    }

    val (partitionKeys, partitionMap) = withResource(partitionKeysTable) { _ =>
      val distinctPartitionKeysTable = partitionKeysTable.dropDuplicates(keyCols,
        DuplicateKeepOption.KEEP_FIRST, true)
      withResource(distinctPartitionKeysTable) { _ =>
        val partitionKeys =  toPartitionKeys(spec.partitionType(),
          partitionSparkType,
          distinctPartitionKeysTable)

        val partitionMap = partitionKeysTable
          .leftDistinctJoinGatherMap(distinctPartitionKeysTable, true)

        (partitionKeys, partitionMap)
      }
    }

    val numPartitions = partitionKeys.length

    val partitionedTable = withResource(partitionMap) { _ =>
      withResource(partitionMap.toColumnView(0, numRows)) { partitionMapCv =>
        withResource(GpuColumnVector.from(input)) { t =>
          t.partition(partitionMapCv, numPartitions)
        }
      }
    }

    withResource(partitionedTable) { _ =>
      val partitionOffset = partitionedTable.getPartitions :+ numRows

      (0 until numPartitions).flatMap { partitionIdx =>
        val partitionRows = partitionOffset(partitionIdx + 1) - partitionOffset(partitionIdx)
        if (partitionRows > 0) {
          withResource(Scalar.fromInt(partitionOffset(partitionIdx))) { startRow =>
            withResource(CudfColumnVector.sequence(startRow, partitionRows)) { gatherMap =>
              withResource(partitionedTable.getTable.gather(gatherMap)) { table =>
                Some(ColumnarBatchWithPartition(GpuColumnVector.from(table, sparkType),
                  partitionKeys(partitionIdx)))
              }
            }
          }
        } else {
          None
        }
      }
    }
  }

  private def getPartitionExpr(field: PartitionField)
  : GpuExpression = {
    val transform = field.transform()
    val inputIndex = fieldIndex(inputSchema, field.sourceId())
    val sparkField = dataSparkType.fields(inputIndex)
    val inputRefExpr = GpuBoundReference(inputIndex, sparkField.dataType,
      sparkField.nullable)(newExprId, s"input$inputIndex")

    transform.toString match {
      // bucket transform is like "bucket[16]"
      case s if s.startsWith("bucket") =>
        val bucket = s.substring("bucket[".length, s.length - 1).toInt
        GpuBucketExpression(GpuLiteral.create(bucket), inputRefExpr)
      case other =>
        throw new IllegalArgumentException(s"Unsupported transform: $other")
    }
  }
}

case class ColumnarBatchWithPartition(batch: ColumnarBatch, partition: StructLike) extends
  AutoCloseable {
  override def close(): Unit = {
    batch.close()
  }
}

object GpuIcebergPartitioner {

  private def toPartitionKeys(icebergType: Types.StructType,
    sparkType: StructType,
    table: Table): Array[SparkStructLike] = {
    val numCols = table.getNumberOfColumns
    val numRows = toIntExact(table.getRowCount)

    val hostColsArray = closeOnExcept(new Array[ColumnVector](numCols)) { hostCols =>
      for (colIdx <- 0 until numCols) {
        hostCols(colIdx) = new RapidsHostColumnVector(sparkType.fields(colIdx).dataType,
          table.getColumn(colIdx).copyToHost())
      }
      hostCols
    }

    withResource(new ColumnarBatch(hostColsArray, numRows)) { hostBatch =>
        hostBatch.rowIterator()
          .asScala
          .map(internalRow => {
            val row = new GenericRowWithSchema(internalRow.toSeq(sparkType).toArray, sparkType)
            new SparkStructLike(icebergType).wrap(row)
          }).toArray
    }
  }
}