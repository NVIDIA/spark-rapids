/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, Table}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, HashJoin}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GpuHashJoin {
  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      condition: Option[Expression]): Unit = joinType match {
    case Inner =>
    case LeftOuter | LeftSemi | LeftAnti =>
      if (condition.isDefined) {
        meta.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
      }
    case _ => meta.willNotWorkOnGpu(s"$joinType currently is not supported")
  }
}

trait GpuHashJoin extends GpuExec with HashJoin {

  protected lazy val (gpuBuildKeys, gpuStreamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = GpuBindReferences.bindReferences(leftKeys.asInstanceOf[Seq[GpuExpression]],
      left.output)
    val rkeys = GpuBindReferences.bindReferences(rightKeys.asInstanceOf[Seq[GpuExpression]],
      right.output)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  /**
   * Place the columns in left and the columns in right into a single ColumnarBatch
   */
  def combine(left: ColumnarBatch, right: ColumnarBatch): ColumnarBatch = {
    val l = GpuColumnVector.extractColumns(left)
    val r = GpuColumnVector.extractColumns(right)
    val c = l ++ r
    new ColumnarBatch(c.asInstanceOf[Array[ColumnVector]], left.numRows())
  }

  // TODO eventually dedupe the keys
  lazy val joinKeyIndices: Range = gpuBuildKeys.indices

  val localBuildOutput: Seq[Attribute] = buildPlan.output
  // The first columns are the ones we joined on and need to remove
  lazy val joinIndices: Seq[Int] = output.indices.map(v => v + joinKeyIndices.length)

  def doJoin(builtTable: Table,
      streamedBatch: ColumnarBatch,
      condition: Option[GpuExpression],
      numOutputRows: SQLMetric,
      numJoinOutputRows: SQLMetric,
      numOutputBatches: SQLMetric,
      joinTime: SQLMetric,
      filterTime: SQLMetric): ColumnarBatch = {

    val streamedTable = try {
      val streamedKeysBatch = GpuProjectExec.project(streamedBatch, gpuStreamedKeys)
      try {
        val combined = combine(streamedKeysBatch, streamedBatch)
        GpuColumnVector.from(combined)
      } finally {
        streamedKeysBatch.close()
      }
    } finally {
      streamedBatch.close()
    }

    val nvtxRange = new NvtxWithMetrics("hash join", NvtxColor.ORANGE, joinTime)
    val joined = try {
      buildSide match {
        case BuildLeft => doJoinLeftRight(builtTable, streamedTable)
        case BuildRight => doJoinLeftRight(streamedTable, builtTable)
      }
    } finally {
      streamedTable.close()
      nvtxRange.close()
    }

    numJoinOutputRows += joined.numRows()

    if (condition.isDefined) {
      GpuFilter(joined, condition.get, numOutputRows, numOutputBatches, filterTime)
    } else {
      numOutputRows += joined.numRows()
      numOutputBatches += 1
      joined
    }
  }

  def doJoinLeftRight(leftTable: Table, rightTable: Table): ColumnarBatch = {
    val joinedTable = joinType match {
      case LeftOuter => leftTable.onColumns(joinKeyIndices: _*)
        .leftJoin(rightTable.onColumns(joinKeyIndices: _*))
      case Inner =>
        leftTable.onColumns(joinKeyIndices: _*).innerJoin(rightTable.onColumns(joinKeyIndices: _*))
      case LeftSemi =>
        leftTable.onColumns(joinKeyIndices: _*)
          .leftSemiJoin(rightTable.onColumns(joinKeyIndices: _*))
      case LeftAnti =>
        leftTable.onColumns(joinKeyIndices: _*)
          .leftAntiJoin(rightTable.onColumns(joinKeyIndices: _*))
      case _ => throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
        s" supported")
    }
    try {
      val result = joinIndices.map(joinIndex =>
        GpuColumnVector.from(joinedTable.getColumn(joinIndex).incRefCount()))
        .toArray[ColumnVector]

      new ColumnarBatch(result, joinedTable.getRowCount.toInt)
    } finally {
      joinedTable.close()
    }
  }
}
