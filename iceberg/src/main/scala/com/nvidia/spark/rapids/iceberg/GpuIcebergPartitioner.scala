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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, OrderByArg, Scalar, Table}
import com.nvidia.spark.rapids.{GpuBoundReference, GpuColumnVector, GpuExpression, GpuLiteral, RapidsHostColumnVector, SpillableColumnarBatch, SpillPriorities}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.SpillPriorities.ACTIVE_ON_DECK_PRIORITY
import com.nvidia.spark.rapids.iceberg.GpuIcebergPartitioner.toPartitionKeys
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.spark.{GpuTypeToSparkType, SparkStructLike}
import org.apache.iceberg.spark.functions.{GpuBucket, GpuBucketExpression, GpuTransform}
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

  private val keyColNum: Int = spec.fields().size()
  private val keyColIndices: Array[Int] = (0 until keyColNum).toArray
  private val keySortOrders: Array[OrderByArg] = (0 until keyColNum)
    .map(OrderByArg.asc(_, true))
    .toArray

  /**
   * Partition the `input` columnar batch using iceberg's partition spec.
   * <br/>
   * This method takes the ownership of the input columnar batch, and it should not be used after
   * this call.
   */
  def partition(input: ColumnarBatch): Seq[ColumnarBatchWithPartition] = {
    if (input.numRows() == 0) {
      return Seq.empty
    }

    val numRows = input.numRows()

    val spillableInput = closeOnExcept(input) { _ =>
      SpillableColumnarBatch(input, ACTIVE_ON_DECK_PRIORITY)
    }

    val (partitionKeys, partitions) = withRetryNoSplit(spillableInput) { scb =>
      val parts = withResource(scb.getColumnarBatch()) { inputBatch =>
        partitionExprs.safeMap(_.columnarEval(inputBatch))
      }
      val keysTable = withResource(parts) { _ =>
        val arr = new Array[CudfColumnVector](partitionExprs.size)
        for (i <- partitionExprs.indices) {
          arr(i) = parts(i).getBase
        }
        new Table(arr:_*)
      }

      val sortedKeyTableWithRowIdx = withResource(keysTable) { _ =>
        withResource(Scalar.fromInt(0)) { zero =>
          withResource(CudfColumnVector.sequence(zero, numRows)) { rowIdxCol =>
            val totalColCount = keysTable.getNumberOfColumns + 1
            val allCols = new Array[CudfColumnVector](totalColCount)

            for (i <- 0 until keysTable.getNumberOfColumns) {
              allCols(i) = keysTable.getColumn(i)
            }
            allCols(keysTable.getNumberOfColumns) = rowIdxCol

            withResource(new Table(allCols: _*)) { allColsTable =>
              allColsTable.orderBy(keySortOrders: _*)
            }
          }
        }
      }

      val (sortedPartitionKeys, splitIds, rowIdxCol) = withResource(sortedKeyTableWithRowIdx) { _ =>
        val uniqueKeysTable = sortedKeyTableWithRowIdx.groupBy(keyColIndices: _*)
          .aggregate()

        val sortedUniqueKeysTable = withResource(uniqueKeysTable) { _ =>
          uniqueKeysTable.orderBy(keySortOrders: _*)
        }

        val (sortedPartitionKeys, splitIds) = withResource(sortedUniqueKeysTable) { _ =>
          val partitionKeys = toPartitionKeys(spec.partitionType(),
            partitionSparkType,
            sortedUniqueKeysTable)

          val splitIdsCv = sortedKeyTableWithRowIdx.upperBound(
            sortedUniqueKeysTable,
            keySortOrders: _*)

          val splitIds = withResource(splitIdsCv) { _ =>
            GpuColumnVector.toIntArray(splitIdsCv)
          }

          (partitionKeys, splitIds)
        }

        val rowIdxCol = sortedKeyTableWithRowIdx.getColumn(keyColNum).incRefCount()
        (sortedPartitionKeys, splitIds, rowIdxCol)
      }

      withResource(rowIdxCol) { _ =>
        val inputTable = withResource(scb.getColumnarBatch()) { inputBatch =>
          GpuColumnVector.from(inputBatch)
        }

        val sortedDataTable = withResource(inputTable) { _ =>
          inputTable.gather(rowIdxCol)
        }

        val partitions = withResource(sortedDataTable) { _ =>
          sortedDataTable.contiguousSplit(splitIds: _*)
        }

        (sortedPartitionKeys, partitions)
      }
    }

    withResource(partitions) { _ =>
      partitionKeys.zip(partitions).map { case (partKey, partition) =>
        ColumnarBatchWithPartition(SpillableColumnarBatch(partition, sparkType, SpillPriorities
          .ACTIVE_BATCHING_PRIORITY), partKey)
      }.toSeq
    }

  }

  private def getPartitionExpr(field: PartitionField)
  : GpuExpression = {
    val transform = field.transform()
    val inputIndex = fieldIndex(inputSchema, field.sourceId())
    val sparkField = dataSparkType.fields(inputIndex)
    val inputRefExpr = GpuBoundReference(inputIndex, sparkField.dataType,
      sparkField.nullable)(newExprId, s"input$inputIndex")

    GpuTransform(transform.toString) match {
      // bucket transform is like "bucket[16]"
      case GpuBucket(bucket) =>
        GpuBucketExpression(GpuLiteral.create(bucket), inputRefExpr)
    }
  }
}

case class ColumnarBatchWithPartition(batch: SpillableColumnarBatch, partition: StructLike) extends
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

  private def addRowIdxToTable(table: Table): Table = {
    val cols = new Array[CudfColumnVector](table.getNumberOfColumns + 1)

    val rowIdxCol = withResource(Scalar.fromInt(0)) { zero =>
      CudfColumnVector.sequence(zero, table.getRowCount.toInt)
    }
    cols(table.getNumberOfColumns) = rowIdxCol

    closeOnExcept(cols) { _ =>
      for (idx <- 0 until table.getNumberOfColumns) {
        cols(idx) = table.getColumn(idx)
      }

      new Table(cols: _*)
    }
  }

  def partitionBy(keys: ColumnarBatch,
                  keyType: Types.StructType,
                  keySparkType: StructType,
                  values: ColumnarBatch,
                  valueSparkType: Array[DataType]): Seq[ColumnarBatchWithPartition] = {
    require(keys.numRows() == values.numRows(),
      s"Keys row count ${keys.numRows()} not matching with values row count ${values.numRows()}")

    val keySortOrders = (0 until keys.numCols()).map(OrderByArg.asc(_, true))
    val keyAggCols = (0 until keys.numCols()).toArray

    withResource(GpuColumnVector.from(keys)) { keysTable =>
      withResource(GpuColumnVector.from(values)) { valuesTable =>

        val sortedKeysWithRowIdx = withResource(addRowIdxToTable(keysTable)) { t =>
          t.orderBy(keySortOrders: _*)
        }

        val (partitionKeys, splits) = withResource(sortedKeysWithRowIdx) { _ =>
          val sortedUniqueKeyTable = {
            val uniqueKeyTable = keysTable.groupBy(keyAggCols: _*)
              .aggregate()
            withResource(uniqueKeyTable) { _ =>
              uniqueKeyTable.orderBy(keySortOrders: _*)
            }
          }

          withResource(sortedUniqueKeyTable) { _ =>
            val partKeys = toPartitionKeys(keyType, keySparkType, sortedUniqueKeyTable)

            val splitIdCv = sortedKeysWithRowIdx.upperBound(sortedUniqueKeyTable, keySortOrders: _*)
            val splitIds = withResource(splitIdCv) { cv =>
              GpuColumnVector.toIntArray(cv)
            }

            val sortedRowIdxCol = sortedKeysWithRowIdx
              .getColumn(keys.numCols())

            val splits= withResource(valuesTable.gather(sortedRowIdxCol)) { sortedValuesTable =>
              sortedValuesTable.contiguousSplit(splitIds: _*)
            }

            (partKeys, splits)
          }
        }


        partitionKeys.zip(splits).map {
          case (partKey, split) => ColumnarBatchWithPartition(
            SpillableColumnarBatch(split, valueSparkType, SpillPriorities
            .ACTIVE_BATCHING_PRIORITY), partKey)
        }
      }
    }
  }
}