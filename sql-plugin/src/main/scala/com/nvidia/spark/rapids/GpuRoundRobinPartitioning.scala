/*
 * Copyright (c) 2020-2022, NVIDIA CORPORATION.
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

import java.util.Random

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.TaskContext
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Represents a partitioning where incoming columnar batched rows are distributed evenly
 * across output partitions by starting from a zero-th partition number and distributing rows
 * in a round-robin fashion. This partitioning is used when implementing the
 * DataFrame.repartition() operator.
 */
case class GpuRoundRobinPartitioning(numPartitions: Int)
  extends GpuExpression with ShimExpression with GpuPartitioning {
  override def children: Seq[GpuExpression] = Nil

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  def partitionInternal(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    val sparkTypes = GpuColumnVector.extractTypes(batch)
    withResource(GpuColumnVector.from(batch)) { table =>
      if (numPartitions == 1) {
        val columns = (0 until table.getNumberOfColumns).zip(sparkTypes).map {
          case(idx, sparkType) =>
            GpuColumnVector.from(table.getColumn(idx).incRefCount(), sparkType)
        }.toArray
        return (Array(0), columns)
      }
      withResource(table.roundRobinPartition(numPartitions, getStartPartition)) { partedTable =>
        val parts = partedTable.getPartitions
        val columns = (0 until partedTable.getNumberOfColumns.toInt).zip(sparkTypes).map {
          case(idx, sparkType) =>
            GpuColumnVector.from(partedTable.getColumn(idx).incRefCount(), sparkType)
        }.toArray
        (parts, columns)
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    if (batch.numCols() <= 0) {
      return Array(batch).zipWithIndex
    }
    val totalRange = new NvtxRange("Round Robin partition", NvtxColor.PURPLE)
    try {
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        val partitionRange = new NvtxRange("partition", NvtxColor.BLUE)
        try {
          partitionInternal(batch)
        } finally {
          batch.close()
          partitionRange.close()
        }
      }
      val ret: Array[ColumnarBatch] =
        sliceInternalGpuOrCpu(numRows, partitionIndexes, partitionColumns)
      partitionColumns.safeClose()
      // Close the partition columns we copied them as a part of the slice
      ret.zipWithIndex.filter(_._1 != null)
    } finally {
      totalRange.close()
    }
  }

  private def getStartPartition : Int = {
    // Follow Spark's way of getting the randomized position to pass to the round robin,
    // which starts at randomNumber + 1 and we do the same here to maintain compatibility.
    // See ShuffleExchangeExec.getPartitionKeyExtractor for reference.
    val position = new Random(TaskContext.get().partitionId()).nextInt(numPartitions) + 1
    val partId = position.hashCode % numPartitions
    partId
  }
}
