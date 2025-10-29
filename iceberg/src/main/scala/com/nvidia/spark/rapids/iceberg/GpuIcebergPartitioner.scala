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

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Table}
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

  /**
   * Make a new table: [key columns, input columns]
   * [key columns, input columns]
   */
  private def makeKeysAndInputTable(spillableInput: SpillableColumnarBatch): Table = {
    withResource(spillableInput.getColumnarBatch()) { inputBatch =>
      // compute keys columns
      val keyCols = partitionExprs.safeMap(_.columnarEval(inputBatch))

      // combine keys columns and input columns into a new table
      withResource(keyCols) { _ =>
        withResource(GpuColumnVector.from(inputBatch)) { inputTable =>
          val numCols = keyCols.size + inputTable.getNumberOfColumns
          val cols = new Array[CudfColumnVector](numCols)
          for (i <- keyCols.indices) {
            cols(i) = keyCols(i).getBase
          }
          for (i <- 0 until inputTable.getNumberOfColumns) {
            cols(i + keyCols.size) = inputTable.getColumn(i)
          }
          new Table(cols:_*)
        }
      }
    }
  }

  /**
   * Make the input column indices in the table: [key columns, input columns]
   */
  private def makeInputIndices(inputColNum: Int): Array[Int] = {
    (keyColNum until (keyColNum + inputColNum)).toArray
  }

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
    val inputColumnIndices = makeInputIndices(input.numCols())

    val spillableInput = closeOnExcept(input) { _ =>
      SpillableColumnarBatch(input, ACTIVE_ON_DECK_PRIORITY)
    }

    withRetryNoSplit(spillableInput) { scb =>
      // make table: [key columns, input columns]
      val keysAndInputTable = makeKeysAndInputTable(scb)

      // split the input columns by the key columns,
      // note: the result does not contain the key columns
      val splitRet = withResource(keysAndInputTable) { _ =>
        keysAndInputTable.groupBy(keyColIndices: _*)
          .contiguousSplitGroupsAndGenUniqKeys(inputColumnIndices)
      }

      // generate results
      withResource(splitRet) { _ =>
        // generate the partition keys on the host side
        val partitionKeys = toPartitionKeys(spec.partitionType(),
          partitionSparkType,
          splitRet.getUniqKeyTable)

        // release unique table to save GPU memory
        splitRet.closeUniqKeyTable()

        // get the partitions
        val partitions = splitRet.getGroups

        // combine the partition keys and partitioned tables
        partitionKeys.zip(partitions).map { case (partKey, partition) =>
          ColumnarBatchWithPartition(SpillableColumnarBatch(partition, sparkType, SpillPriorities
            .ACTIVE_BATCHING_PRIORITY), partKey)
        }.toSeq
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
}
