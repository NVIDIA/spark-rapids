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

package com.nvidia.spark.rapids

import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, NvtxColor, NvtxRange, Table}
import com.nvidia.spark.rapids.RapidsPluginImplicits._

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashClusteredDistribution}
import org.apache.spark.sql.types.{DataType, IntegerType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * This is a metadata operation that is used to indicate that the hash partitioning we are using
 * for this column does not match the CPU. This lets the CPU planner see that they are different
 * and insert any needed shuffles to make them match.
 */
case class IncompatGpuHash(child: Expression) extends GpuExpression {
  override def columnarEval(batch: ColumnarBatch): Any = child.columnarEval(batch)

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override def children: Seq[Expression] = Seq(child)
}

object IncompatGpuHashIfNeeded {
  def apply(keys: Seq[Expression]): Seq[Expression] = {
    // In the future when we start to support hash partitioning that matches what the
    // CPU does, then we can look at the data types and decide if we can match the CPU
    // or not
    keys.map { key =>
      if (key.isInstanceOf[IncompatGpuHash]) {
        key
      } else {
        IncompatGpuHash(key)
      }
    }
  }

  def removeFrom(exprs: Seq[Expression]): Seq[Expression] = {
    exprs.map {
      case h: IncompatGpuHash =>
        val exp = h.child
        exp.withNewChildren(removeFrom(exp.children))
      case other => other.withNewChildren(removeFrom(other.children))
    }
  }
}

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
          // For this distribution there is no join so we don't have to make sure that both sides
          // match, so we should remove the IncompatGpuHash instances
          val strippedExpressions = IncompatGpuHashIfNeeded.removeFrom(expressions)
          strippedExpressions.forall(x => requiredClustering.exists(_.semanticEquals(x)))
        case _ => false
      }
    }
  }

  def getGpuKeyColumns(batch: ColumnarBatch) : Array[GpuColumnVector] = {
    expressions.map(_.columnarEval(batch)
        .asInstanceOf[GpuColumnVector]).toArray
  }

  def getGpuDataColumns(batch: ColumnarBatch) : Array[GpuColumnVector] =
    GpuColumnVector.extractColumns(batch)

  def insertDedupe(
      indexesOut: Array[Int],
      colsIn: Array[GpuColumnVector],
      dedupedData: ArrayBuffer[ColumnVector]): Unit = {
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
      val sparkTypes = gpuDataColumns.map(_.dataType())

      val (keys, dataIndexes, table) = dedupe(gpuKeyColumns, gpuDataColumns)
      // Don't need the batch any more table has all we need in it.
      // gpuDataColumns did not increment the reference count when we got them,
      // so don't close them.
      gpuDataColumns = null
      gpuKeyColumns.foreach(_.close())
      gpuKeyColumns = null
      batch.close()

      val partedTable = table.onColumns(keys: _*).hashPartition(numPartitions)
      table.close()
      val parts = partedTable.getPartitions
      val columns = dataIndexes.zip(sparkTypes).map { case (idx, sparkType) =>
        GpuColumnVector.from(partedTable.getColumn(idx).incRefCount(), sparkType)
      }
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
      val numRows = batch.numRows
      val (partitionIndexes, partitionColumns) = {
        val partitionRange = new NvtxRange("partition", NvtxColor.BLUE)
        try {
          partitionInternal(batch)
        } finally {
          partitionRange.close()
        }
      }
      val ret = sliceInternalGpuOrCpu(numRows, partitionIndexes, partitionColumns)
      partitionColumns.safeClose()
      // Close the partition columns we copied them as a part of the slice
      ret.zipWithIndex.filter(_._1 != null)
    } finally {
      totalRange.close()
    }
  }
}
