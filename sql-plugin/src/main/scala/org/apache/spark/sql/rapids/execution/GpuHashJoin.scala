/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION. All rights reserved.
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
package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{NvtxColor, Table}
import com.nvidia.spark.rapids._

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object JoinTypeChecks {
  def tagForGpu(joinType: JoinType, meta: RapidsMeta[_, _, _]): Unit = {
    val conf = meta.conf
    joinType match {
      case Inner if !conf.areInnerJoinsEnabled =>
        meta.willNotWorkOnGpu("inner joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_INNER_JOIN.key} to true")
      case Cross if !conf.areCrossJoinsEnabled =>
        meta.willNotWorkOnGpu("cross joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_CROSS_JOIN.key} to true")
      case LeftOuter if !conf.areLeftOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("left outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_OUTER_JOIN.key} to true")
      case RightOuter if !conf.areRightOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("right outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_RIGHT_OUTER_JOIN.key} to true")
      case FullOuter if !conf.areFullOuterJoinsEnabled =>
        meta.willNotWorkOnGpu("full outer joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_FULL_OUTER_JOIN.key} to true")
      case LeftSemi if !conf.areLeftSemiJoinsEnabled =>
        meta.willNotWorkOnGpu("left semi joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_SEMI_JOIN.key} to true")
      case LeftAnti if !conf.areLeftAntiJoinsEnabled =>
        meta.willNotWorkOnGpu("left anti joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_LEFT_ANTI_JOIN.key} to true")
      case _ => // not disabled
    }
  }
}

object GpuHashJoin {
  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)
    if (keyDataTypes.exists(dtype =>
      dtype.isInstanceOf[ArrayType] || dtype.isInstanceOf[StructType]
        || dtype.isInstanceOf[MapType])) {
      meta.willNotWorkOnGpu("Nested types in join keys are not supported")
    }
    JoinTypeChecks.tagForGpu(joinType, meta)
    joinType match {
      case _: InnerLike =>
      case FullOuter | RightOuter | LeftOuter | LeftSemi | LeftAnti =>
        if (condition.isDefined) {
          meta.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
        }
      case _ => meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
  }

  def incRefCount(cb: ColumnarBatch): ColumnarBatch = {
    GpuColumnVector.extractBases(cb).foreach(_.incRefCount())
    cb
  }
}

trait GpuHashJoin extends GpuExec {
  def left: SparkPlan
  def right: SparkPlan
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]
  def buildSide: GpuBuildSide

  protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case GpuBuildLeft => (left, right)
    case GpuBuildRight => (right, left)
  }

  protected lazy val (buildKeys, streamedKeys) = {
    require(leftKeys.length == rightKeys.length &&
        leftKeys.map(_.dataType)
            .zip(rightKeys.map(_.dataType))
            .forall(types => types._1.sameType(types._2)),
      "Join keys from two sides should have same length and types")
    buildSide match {
      case GpuBuildLeft => (leftKeys, rightKeys)
      case GpuBuildRight => (rightKeys, leftKeys)
    }
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
  }

  // If we have a single batch streamed in then we will produce a single batch of output
  // otherwise it can get smaller or bigger, we just don't know.  When we support out of
  // core joins this will change
  override def outputBatching: CoalesceGoal = {
    val batching = buildSide match {
      case GpuBuildLeft => GpuExec.outputBatching(right)
      case GpuBuildRight => GpuExec.outputBatching(left)
    }
    if (batching == RequireSingleBatch) {
      RequireSingleBatch
    } else {
      null
    }
  }

  protected lazy val (gpuBuildKeys, gpuStreamedKeys) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    buildSide match {
      case GpuBuildLeft => (lkeys, rkeys)
      case GpuBuildRight => (rkeys, lkeys)
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
  lazy val joinIndices: Seq[Int] = joinType match {
    case RightOuter =>
      // The left table and right table are switched in the output
      // because we don't support a right join, only left
      val numRight = right.output.length
      val numLeft = left.output.length
      val joinLength = joinKeyIndices.length
      def remap(index: Int): Int = {
        if (index < numLeft) {
          // part of the left table, but is on the right side of the tmp output
          index + joinLength + numRight
        } else {
          // part of the right table, but is on the left side of the tmp output
          index + joinLength - numLeft
        }
      }
      output.indices.map (remap)
    case _ =>
      val joinLength = joinKeyIndices.length
      output.indices.map (v => v + joinLength)
  }

  def doJoin(builtTable: Table,
      stream: Iterator[ColumnarBatch],
      boundCondition: Option[Expression],
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      streamTime: GpuMetric,
      joinTime: GpuMetric,
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      import scala.collection.JavaConverters._
      var nextCb: Option[ColumnarBatch] = None
      var first: Boolean = true

      TaskContext.get().addTaskCompletionListener[Unit](_ => closeCb())

      def closeCb(): Unit = {
        nextCb.foreach(_.close())
        nextCb = None
      }

      override def hasNext: Boolean = {
        var mayContinue = true
        while (nextCb.isEmpty && mayContinue) {
          val startTime = System.nanoTime()
          if (stream.hasNext) {
            val cb = stream.next()
            streamTime += (System.nanoTime() - startTime)
            nextCb = doJoin(builtTable, cb, boundCondition, joinOutputRows, numOutputRows,
              numOutputBatches, joinTime, filterTime)
            totalTime += (System.nanoTime() - startTime)
          } else if (first) {
            // We have to at least try one in some cases
            val cb = GpuColumnVector.emptyBatch(streamedPlan.output.asJava)
            streamTime += (System.nanoTime() - startTime)
            nextCb = doJoin(builtTable, cb, boundCondition, joinOutputRows, numOutputRows,
              numOutputBatches, joinTime, filterTime)
            totalTime += (System.nanoTime() - startTime)
          } else {
            mayContinue = false
          }
          first = false
        }
        nextCb.isDefined
      }

      override def next(): ColumnarBatch = {
        if (!hasNext) {
          throw new NoSuchElementException()
        }
        val ret = nextCb.get
        nextCb = None
        ret
      }
    }
  }

  private[this] def doJoin(builtTable: Table,
      streamedBatch: ColumnarBatch,
      boundCondition: Option[Expression],
      numOutputRows: GpuMetric,
      numJoinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      joinTime: GpuMetric,
      filterTime: GpuMetric): Option[ColumnarBatch] = {

    val combined = withResource(streamedBatch) { streamedBatch =>
      withResource(GpuProjectExec.project(streamedBatch, gpuStreamedKeys)) {
        streamedKeysBatch =>
          GpuHashJoin.incRefCount(combine(streamedKeysBatch, streamedBatch))
      }
    }
    val streamedTable = withResource(combined) { cb =>
      GpuColumnVector.from(cb)
    }

    val joined =
      withResource(new NvtxWithMetrics("hash join", NvtxColor.ORANGE, joinTime)) { _ =>
        // `doJoinLeftRight` closes the right table if the last argument (`closeRightTable`)
        // is true, but never closes the left table.
        buildSide match {
          case GpuBuildLeft =>
            // tell `doJoinLeftRight` it is ok to close the `streamedTable`, this can help
            // in order to close temporary/intermediary data after a filter in some scenarios.
            doJoinLeftRight(builtTable, streamedTable, true)
          case GpuBuildRight =>
            // tell `doJoinLeftRight` to not close `builtTable`, as it is owned by our caller,
            // here we close the left table as that one is never closed by `doJoinLeftRight`.
            withResource(streamedTable) { _ =>
              doJoinLeftRight(streamedTable, builtTable, false)
            }
        }
      }

    numJoinOutputRows += joined.numRows()

    val tmp = if (boundCondition.isDefined) {
      GpuFilter(joined, boundCondition.get, numOutputRows, numOutputBatches, filterTime)
    } else {
      numOutputRows += joined.numRows()
      numOutputBatches += 1
      joined
    }
    if (tmp.numRows() == 0) {
      // Not sure if there is a better way to work around this
      numOutputBatches.set(numOutputBatches.value - 1)
      tmp.close()
      None
    } else {
      Some(tmp)
    }
  }

  // This is a work around added in response to https://github.com/NVIDIA/spark-rapids/issues/1643.
  // to deal with slowness arising from many nulls in the build-side of the join. The work around
  // should be removed when https://github.com/rapidsai/cudf/issues/7300 is addressed.
  private[this] def filterNulls(table: Table, joinKeyIndices: Range, closeTable: Boolean): Table = {
    var mask: ai.rapids.cudf.ColumnVector = null
    try {
      joinKeyIndices.indices.foreach { c =>
        mask = withResource(table.getColumn(c).isNotNull) { nn =>
          if (mask == null) {
            nn.incRefCount()
          } else {
            withResource(mask) { _ =>
              mask.and(nn)
            }
          }
        }
      }
      table.filter(mask)
    } finally {
      if (mask != null) {
        mask.close()
      }

      // in some cases, we cannot close the table since it was the build table and is
      // reused.
      if (closeTable) {
        table.close()
      }
    }
  }

  private[this] def doJoinLeftRight(
      leftTable: Table, rightTable: Table, closeRightTable: Boolean): ColumnarBatch = {

    def withRightTable(body: Table => Table): Table = {
      val builtAnyNullable =
        (joinType == LeftSemi || joinType == LeftAnti) && gpuBuildKeys.exists(_.nullable)

      if (builtAnyNullable) {
        withResource(filterNulls(rightTable, joinKeyIndices, closeRightTable)) { filtered =>
          body(filtered)
        }
      } else {
        try {
          body(rightTable)
        } finally {
          if (closeRightTable) {
            rightTable.close()
          }
        }
      }
    }

    val joinedTable = withRightTable { rt =>
      joinType match {
        case LeftOuter => leftTable.onColumns(joinKeyIndices: _*)
            .leftJoin(rt.onColumns(joinKeyIndices: _*), false)
        case RightOuter => rt.onColumns(joinKeyIndices: _*)
            .leftJoin(leftTable.onColumns(joinKeyIndices: _*), false)
        case _: InnerLike => leftTable.onColumns(joinKeyIndices: _*)
            .innerJoin(rt.onColumns(joinKeyIndices: _*), false)
        case LeftSemi => leftTable.onColumns(joinKeyIndices: _*)
            .leftSemiJoin(rt.onColumns(joinKeyIndices: _*), false)
        case LeftAnti => leftTable.onColumns(joinKeyIndices: _*)
            .leftAntiJoin(rt.onColumns(joinKeyIndices: _*), false)
        case FullOuter => leftTable.onColumns(joinKeyIndices: _*)
            .fullJoin(rt.onColumns(joinKeyIndices: _*), false)
        case _ =>
          throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
              s" supported")
      }
    }

    try {
      val result = joinIndices.zip(output).map { case (joinIndex, outAttr) =>
        GpuColumnVector.from(joinedTable.getColumn(joinIndex).incRefCount(), outAttr.dataType)
      }.toArray[ColumnVector]

      new ColumnarBatch(result, joinedTable.getRowCount.toInt)
    } finally {
      joinedTable.close()
    }
  }
}
