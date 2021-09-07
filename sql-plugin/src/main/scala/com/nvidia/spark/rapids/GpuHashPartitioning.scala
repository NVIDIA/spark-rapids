/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{DType, NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.shims.v2.ShimExpression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution}
import org.apache.spark.sql.rapids.GpuMurmur3Hash
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends GpuExpression with ShimExpression with GpuPartitioning {

  override def children: Seq[Expression] = expressions
  override def nullable: Boolean = false
  override def dataType: DataType = IntegerType

  override def satisfies0(required: Distribution): Boolean = {
    super.satisfies0(required) || {
      required match {
        case h: HashClusteredDistribution =>
          expressions.length == h.expressions.length && expressions.zip(h.expressions).forall {
            case (l, r) => l.semanticEquals(r)
          }
        case ClusteredDistribution(requiredClustering, _) =>
          expressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  def partitionInternalAndClose(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    val types = GpuColumnVector.extractTypes(batch)
    val partedTable = withResource(batch) { batch =>
      val parts = withResource(new NvtxRange("Calculate part", NvtxColor.CYAN)) { _ =>
        withResource(GpuMurmur3Hash.compute(batch, expressions)) { hash =>
          withResource(GpuScalar.from(numPartitions, IntegerType)) { partsLit =>
            hash.pmod(partsLit, DType.INT32)
          }
        }
      }
      withResource(parts) { parts =>
        withResource(GpuColumnVector.from(batch)) { table =>
          table.partition(parts, numPartitions)
        }
      }
    }
    withResource(partedTable) { partedTable =>
      val parts = partedTable.getPartitions
      val tp = partedTable.getTable
      val columns = (0 until partedTable.getNumberOfColumns.toInt).zip(types).map {
        case (index, sparkType) =>
          GpuColumnVector.from(tp.getColumn(index).incRefCount(), sparkType)
      }
      (parts, columns.toArray)
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level
    withResource(new NvtxRange("Hash partition", NvtxColor.PURPLE)) { _ =>
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        withResource(new NvtxRange("partition", NvtxColor.BLUE)) { _ =>
          partitionInternalAndClose(batch)
        }
      }
      val ret = withResource(partitionColumns) { partitionColumns =>
        sliceInternalGpuOrCpu(numRows, partitionIndexes, partitionColumns)
      }
      // Close the partition columns we copied them as a part of the slice
      ret.zipWithIndex.filter(_._1 != null)
    }
  }
}
