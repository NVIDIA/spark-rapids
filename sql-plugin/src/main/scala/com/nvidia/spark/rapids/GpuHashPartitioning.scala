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

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, NvtxColor, NvtxRange, Table}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution}
import org.apache.spark.sql.rapids.GpuMurmur3Hash
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class GpuHashPartitioning(expressions: Seq[Expression], numPartitions: Int)
  extends GpuExpression with GpuPartitioning {

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

  override def columnarEval(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level
    val numRows = batch.numRows
    withResource(new NvtxRange("Hash partition", NvtxColor.PURPLE)) { _ =>
      val sortedTable = withResource(batch) { batch =>
        val parts = withResource(new NvtxRange("Calculate part", NvtxColor.CYAN)) { _ =>
          withResource(GpuMurmur3Hash.compute(batch, expressions)) { hash =>
            withResource(GpuScalar.from(numPartitions, IntegerType)) { partsLit =>
              hash.pmod(partsLit, DType.INT32)
            }
          }
        }
        withResource(new NvtxRange("sort by part", NvtxColor.DARK_GREEN)) { _ =>
          withResource(parts) { parts =>
            val allColumns = new ArrayBuffer[ColumnVector](batch.numCols() + 1)
            allColumns += parts
            allColumns ++= GpuColumnVector.extractBases(batch)
            withResource(new Table(allColumns: _*)) { fullTable =>
              fullTable.orderBy(Table.asc(0))
            }
          }
        }
      }
      val (partitionIndexes, partitionColumns) = withResource(sortedTable) { sortedTable =>
        val cutoffs = withResource(new Table(sortedTable.getColumn(0))) { justPartitions =>
          val partsTable = withResource(GpuScalar.from(0, IntegerType)) { zeroLit =>
            withResource(ColumnVector.sequence(zeroLit, numPartitions)) { partsColumn =>
              new Table(partsColumn)
            }
          }
          withResource(partsTable) { partsTable =>
            justPartitions.upperBound(Array(false), partsTable, Array(false))
          }
        }
        val partitionIndexes = withResource(cutoffs) { cutoffs =>
          val buffer = new ArrayBuffer[Int](numPartitions)
          // The first index is always 0
          buffer += 0
          withResource(cutoffs.copyToHost()) { hostCutoffs =>
            (0 until numPartitions).foreach { i =>
              buffer += hostCutoffs.getInt(i)
            }
          }
          buffer.toArray
        }
        val dataTypes = GpuColumnVector.extractTypes(batch)
        closeOnExcept(new ArrayBuffer[GpuColumnVector]()) { partitionColumns =>
          (1 until sortedTable.getNumberOfColumns).foreach { index =>
            partitionColumns +=
                GpuColumnVector.from(sortedTable.getColumn(index).incRefCount(),
                  dataTypes(index - 1))
          }

          (partitionIndexes, partitionColumns.toArray)
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
