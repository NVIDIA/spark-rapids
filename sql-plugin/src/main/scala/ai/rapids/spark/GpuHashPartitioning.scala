/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.cudf.{ColumnVector, NvtxColor, NvtxRange, Table}
import ai.rapids.spark.RapidsPluginImplicits._

import org.apache.spark.TaskContext
import org.apache.spark.sql.GpuShuffleEnv
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ArrayBuffer

case class GpuHashPartitioning(expressions: Seq[GpuExpression], numPartitions: Int)
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

  def getGpuKeyColumns(batch: ColumnarBatch) : Array[GpuColumnVector] =
    expressions.map(_.columnarEval(batch).asInstanceOf[GpuColumnVector]).toArray

  def getGpuDataColumns(batch: ColumnarBatch) : Array[GpuColumnVector] =
    GpuColumnVector.extractColumns(batch)

  def insertDedupe(indexesOut: Array[Int], colsIn: Array[GpuColumnVector], dedupedData: ArrayBuffer[ColumnVector]): Unit = {
    indexesOut.indices.foreach { i =>
      val b = colsIn(i).getBase
      val idx = dedupedData.indexOf(b)
      if (idx < 0) {
        indexesOut(i) = dedupedData.size
        dedupedData += b
      } else {
        indexesOut(i) = idx
      }
    }
  }

  def dedupe(keyCols: Array[GpuColumnVector], dataCols: Array[GpuColumnVector]):
  (Array[Int], Array[Int], Table) = {
    val base = new ArrayBuffer[ColumnVector](keyCols.length + dataCols.length)
    val keys = new Array[Int](keyCols.length)
    val data = new Array[Int](dataCols.length)

    insertDedupe(keys, keyCols, base)
    insertDedupe(data, dataCols, base)

    (keys, data, new Table(base: _*))
  }

  def partitionInternal(batch: ColumnarBatch): (Array[Int], Array[GpuColumnVector]) = {
    var gpuKeyColumns : Array[GpuColumnVector] = null
    var gpuDataColumns : Array[GpuColumnVector] = null
    try {
      gpuKeyColumns = getGpuKeyColumns(batch)
      gpuDataColumns = getGpuDataColumns(batch)

      val (keys, dataIndexes, table) = dedupe(gpuKeyColumns, gpuDataColumns)
      // Don't need the batch any more table has all we need in it.
      // gpuDataColumns did not increment the reference count when we got them, so don't close them.
      gpuDataColumns = null
      gpuKeyColumns.foreach(_.close())
      gpuKeyColumns = null
      batch.close()

      val partedTable = table.onColumns(keys: _*).partition(numPartitions)
      table.close()
      val parts = partedTable.getPartitions
      val columns = dataIndexes.map(idx => GpuColumnVector.from(partedTable.getColumn(idx).incRefCount()))
      partedTable.close()
      (parts, columns)
    } finally {
      if (gpuDataColumns != null) {
        gpuDataColumns.safeClose()
      }
      if (gpuKeyColumns != null) {
        gpuKeyColumns.safeClose()
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    //  We are doing this here because the cudf partition command is at this level
    val totalRange = new NvtxRange("Hash partition", NvtxColor.PURPLE)
    try {
      val (partitionIndexes, partitionColumns) = {
        val partitionRange = new NvtxRange("partition", NvtxColor.BLUE)
        try {
          partitionInternal(batch)
        } finally {
          partitionRange.close()
        }
      }
      val ret: Array[ColumnarBatch] = sliceInternalGpuOrCpu(batch, partitionIndexes, partitionColumns)
      partitionColumns.safeClose()
      // Close the partition columns we copied them as a part of the slice
      ret.zipWithIndex.filter(_._1 != null)
    } finally {
      totalRange.close()
    }
  }
}
