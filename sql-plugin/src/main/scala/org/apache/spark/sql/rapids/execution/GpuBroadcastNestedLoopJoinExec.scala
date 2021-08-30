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

package org.apache.spark.sql.rapids.execution

import ai.rapids.cudf.{ast, GatherMap, NvtxColor, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback
import com.nvidia.spark.rapids.shims.sql.ShimBinaryExecNode

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, IdentityBroadcastMode, UnspecifiedDistribution}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.BroadcastQueryStageExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuBroadcastNestedLoopJoinMeta(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.nonEquiJoinMeta(conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = conditionMeta.toSeq

  override def tagPlanForGpu(): Unit = {
    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    JoinTypeChecks.tagForGpu(join.joinType, this)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | RightOuter | LeftSemi | LeftAnti =>
        conditionMeta.foreach(requireAstForGpuOn)
      case _ => willNotWorkOnGpu(s"${join.joinType} currently is not supported")
    }
    join.joinType match {
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        willNotWorkOnGpu(s"build left not supported for ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        willNotWorkOnGpu(s"build right not supported for ${join.joinType}")
      case _ =>
    }

    val Seq(leftPlan, rightPlan) = childPlans
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => leftPlan
      case GpuBuildRight => rightPlan
    }

    if (!canBuildSideBeReplaced(buildSide)) {
      willNotWorkOnGpu("the broadcast for this join must be on the GPU too")
    }

    if (!canThisBeReplaced) {
      buildSide.willNotWorkOnGpu(
        "the BroadcastNestedLoopJoin this feeds is not on the GPU")
    }
  }

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    // The broadcast part of this must be a BroadcastExchangeExec
    val gpuBuildSide = ShimLoader.getSparkShims.getBuildSide(join)
    val buildSide = gpuBuildSide match {
      case GpuBuildLeft => left
      case GpuBuildRight => right
    }
    verifyBuildSideWasReplaced(buildSide)

    val condition = conditionMeta.map(_.convertToGpu())
    val isAstCondition = conditionMeta.forall(_.canThisBeAst)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | LeftSemi | LeftAnti if gpuBuildSide == GpuBuildLeft =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case RightOuter if gpuBuildSide == GpuBuildRight =>
        throw new IllegalStateException(s"Unsupported build side for join type ${join.joinType}")
      case LeftOuter | RightOuter | LeftSemi | LeftAnti =>
        // Cannot post-filter these types of joins
        assert(isAstCondition, s"Non-AST condition in ${join.joinType}")
      case _ => throw new IllegalStateException(s"Unsupported join type ${join.joinType}")
    }

    val joinExec = ShimLoader.getSparkShims.getGpuBroadcastNestedLoopJoinShim(
      left, right, join,
      join.joinType,
      if (isAstCondition) condition else None,
      conf.gpuTargetBatchSizeBytes)
    if (isAstCondition) {
      joinExec
    } else {
      // condition cannot be implemented via AST so fallback to a post-filter if necessary
      condition.map(c => GpuFilterExec(c, joinExec)).getOrElse(joinExec)
    }
  }
}

/**
 * An iterator that does a cross join against a stream of batches.
 */
class CrossJoinIterator(
    builtBatch: LazySpillableColumnarBatch,
    stream: Iterator[LazySpillableColumnarBatch],
    targetSize: Long,
    buildSide: GpuBuildSide,
    joinTime: GpuMetric,
    totalTime: GpuMetric)
    extends AbstractGpuJoinIterator(
      "Cross join gather",
      targetSize,
      joinTime,
      totalTime) {
  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtBatch.close()
    }
  }

  override def hasNextStreamBatch: Boolean = stream.hasNext

  override def setupNextGatherer(startTime: Long): Option[JoinGatherer] = {
    val streamBatch = stream.next()

    // Don't close the built side because it will be used for each stream and closed
    // when the iterator is done.
    val (leftBatch, rightBatch) = buildSide match {
      case GpuBuildLeft => (LazySpillableColumnarBatch.spillOnly(builtBatch), streamBatch)
      case GpuBuildRight => (streamBatch, LazySpillableColumnarBatch.spillOnly(builtBatch))
    }

    val leftMap = LazySpillableGatherMap.leftCross(leftBatch.numRows, rightBatch.numRows)
    val rightMap = LazySpillableGatherMap.rightCross(leftBatch.numRows, rightBatch.numRows)

    val joinGatherer = (leftBatch.numCols, rightBatch.numCols) match {
      case (_, 0) =>
        rightBatch.close()
        rightMap.close()
        JoinGatherer(leftMap, leftBatch)
      case (0, _) =>
        leftBatch.close()
        leftMap.close()
        JoinGatherer(rightMap, rightBatch)
      case (_, _) => JoinGatherer(leftMap, leftBatch, rightMap, rightBatch)
    }
    if (joinGatherer.isDone) {
      joinGatherer.close()
      None
    } else {
      Some(joinGatherer)
    }
  }
}

class ConditionalNestedLoopJoinIterator(
    joinType: JoinType,
    buildSide: GpuBuildSide,
    builtBatch: LazySpillableColumnarBatch,
    stream: Iterator[LazySpillableColumnarBatch],
    streamAttributes: Seq[Attribute],
    targetSize: Long,
    condition: ast.CompiledExpression,
    spillCallback: SpillCallback,
    joinTime: GpuMetric,
    totalTime: GpuMetric)
    extends SplittableJoinIterator(
      s"$joinType join gather",
      stream,
      streamAttributes,
      builtBatch,
      targetSize,
      spillCallback,
      joinTime = joinTime,
      streamTime = NoopMetric,
      totalTime = totalTime) {
  override def close(): Unit = {
    if (!closed) {
      super.close()
      condition.close()
    }
  }

  override def computeNumJoinRows(cb: ColumnarBatch): Long = {
    withResource(GpuColumnVector.from(builtBatch.getBatch)) { builtTable =>
      withResource(GpuColumnVector.from(cb)) { streamTable =>
        val (left, right) = buildSide match {
          case GpuBuildLeft => (builtTable, streamTable)
          case GpuBuildRight => (streamTable, builtTable)
        }
        joinType match {
          case _: InnerLike =>left.conditionalInnerJoinRowCount(right, condition, false)
          case LeftOuter => left.conditionalLeftJoinRowCount(right, condition, false)
          case RightOuter => right.conditionalLeftJoinRowCount(left, condition, false)
          case LeftSemi => left.conditionalLeftSemiJoinRowCount(right, condition, false)
          case LeftAnti => left.conditionalLeftAntiJoinRowCount(right, condition, false)
          case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
        }
      }
    }
  }

  override def createGatherer(
      cb: ColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    if (numJoinRows.contains(0)) {
      // nothing matched
      return None
    }
    withResource(GpuColumnVector.from(builtBatch.getBatch)) { builtTable =>
      withResource(GpuColumnVector.from(cb)) { streamTable =>
        closeOnExcept(LazySpillableColumnarBatch(cb, spillCallback, "stream_data")) { streamBatch =>
          val builtSpillOnly = LazySpillableColumnarBatch.spillOnly(builtBatch)
          val (leftTable, leftBatch, rightTable, rightBatch) = buildSide match {
            case GpuBuildLeft => (builtTable, builtSpillOnly, streamTable, streamBatch)
            case GpuBuildRight => (streamTable, streamBatch, builtTable, builtSpillOnly)
          }
          val maps = computeGatherMaps(leftTable, rightTable, numJoinRows)
          makeGatherer(maps, leftBatch, rightBatch)
        }
      }
    }
  }

  private def computeGatherMaps(
      left: Table,
      right: Table,
      numJoinRows: Option[Long]): Array[GatherMap] = {
    joinType match {
      case _: InnerLike =>
        numJoinRows.map { rowCount =>
          left.conditionalInnerJoinGatherMaps(right, condition, false, rowCount)
        }.getOrElse {
          left.conditionalInnerJoinGatherMaps(right, condition, false)
        }
      case LeftOuter =>
        numJoinRows.map { rowCount =>
          left.conditionalLeftJoinGatherMaps(right, condition, false, rowCount)
        }.getOrElse {
          left.conditionalLeftJoinGatherMaps(right, condition, false)
        }
      case RightOuter =>
        val maps = numJoinRows.map { rowCount =>
          right.conditionalLeftJoinGatherMaps(left, condition, false, rowCount)
        }.getOrElse {
          right.conditionalLeftJoinGatherMaps(left, condition, false)
        }
        // Reverse the output of the join, because we expect the right gather map to
        // always be on the right
        maps.reverse
      case LeftSemi =>
        numJoinRows.map { rowCount =>
          Array(left.conditionalLeftSemiJoinGatherMap(right, condition, false, rowCount))
        }.getOrElse {
          Array(left.conditionalLeftSemiJoinGatherMap(right, condition, false))
        }
      case LeftAnti =>
        numJoinRows.map { rowCount =>
          Array(left.conditionalLeftAntiJoinGatherMap(right, condition, false, rowCount))
        }.getOrElse {
          Array(left.conditionalLeftAntiJoinGatherMap(right, condition, false))
        }
      case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
    }
  }
}

object GpuBroadcastNestedLoopJoinExecBase extends Arm {
  def nestedLoopJoin(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      numFirstTableColumns: Int,
      builtBatch: LazySpillableColumnarBatch,
      stream: Iterator[LazySpillableColumnarBatch],
      streamAttributes: Seq[Attribute],
      targetSize: Long,
      boundCondition: Option[GpuExpression],
      spillCallback: SpillCallback,
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      joinTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    val joinIterator = if (boundCondition.isEmpty) {
      // Semi and anti nested loop joins without a condition are degenerate joins and should have
      // been handled at a higher level rather than calling this method.
      assert(joinType.isInstanceOf[InnerLike], s"Unexpected unconditional join type: $joinType")
      new CrossJoinIterator(builtBatch, stream, targetSize, buildSide, joinTime, totalTime)
    } else {
      val compiledAst = boundCondition.get.convertToAst(numFirstTableColumns).compile()
      new ConditionalNestedLoopJoinIterator(joinType, buildSide, builtBatch,
        stream, streamAttributes, targetSize, compiledAst, spillCallback,
        joinTime = joinTime, totalTime = totalTime)
    }
    joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
    }
  }

  def divideIntoBatches(
      rowCounts: RDD[Long],
      targetSizeBytes: Long,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric): RDD[ColumnarBatch] = {
    // Hash aggregate explodes the rows out, so if we go too large
    // it can blow up. The size of a Long is 8 bytes so we just go with
    // that as our estimate, no nulls.
    val maxRowCount = targetSizeBytes / 8

    def divideIntoBatches(rows: Long): Iterable[ColumnarBatch] = {
      val numBatches = (rows + maxRowCount - 1) / maxRowCount
      (0L until numBatches).map(i => {
        val ret = new ColumnarBatch(new Array[ColumnVector](0))
        if ((i + 1) * maxRowCount > rows) {
          ret.setNumRows((rows - (i * maxRowCount)).toInt)
        } else {
          ret.setNumRows(maxRowCount.toInt)
        }
        numOutputRows += ret.numRows()
        numOutputBatches += 1
        // grab the semaphore for downstream processing
        GpuSemaphore.acquireIfNecessary(TaskContext.get())
        ret
      })
    }

    rowCounts.flatMap(divideIntoBatches)
  }
}

abstract class GpuBroadcastNestedLoopJoinExecBase(
    left: SparkPlan,
    right: SparkPlan,
    joinType: JoinType,
    condition: Option[Expression],
    targetSizeBytes: Long) extends ShimBinaryExecNode with GpuExec {

  import GpuMetric._

  // Spark BuildSide, BuildRight, BuildLeft changed packages between Spark versions
  // so return a GPU version that is agnostic to the Spark version.
  def getGpuBuildSide: GpuBuildSide

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    TOTAL_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_TOTAL_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS)) ++ spillMetrics

  /** BuildRight means the right relation <=> the broadcast relation. */
  private val (streamed, broadcast) = getGpuBuildSide match {
    case GpuBuildRight => (left, right)
    case GpuBuildLeft => (right, left)
  }

  def broadcastExchange: GpuBroadcastExchangeExecBase = broadcast match {
    case BroadcastQueryStageExec(_, gpu: GpuBroadcastExchangeExecBase) => gpu
    case BroadcastQueryStageExec(_, reused: ReusedExchangeExec) =>
      reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
    case gpu: GpuBroadcastExchangeExecBase => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
  }

  override def requiredChildDistribution: Seq[Distribution] = getGpuBuildSide match {
    case GpuBuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case GpuBuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  private[this] def makeBuiltBatch(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      val ret = broadcastRelation.value.batch
      buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(ret)
      GpuColumnVector.incRefCounts(ret)
    }
  }

  private[this] def computeBuildRowCount(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      buildDataSize += 0
      broadcastRelation.value.batch.numRows()
    }
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // Determine which table will be first in the join and bind the references accordingly
    // so the AST column references match the appropriate table.
    val (firstTable, secondTable) = joinType match {
      case RightOuter => (right, left)
      case _ => (left, right)
    }
    val numFirstTableColumns = firstTable.output.size
    val boundCondition = condition.map {
      GpuBindReferences.bindGpuReference(_, firstTable.output ++ secondTable.output)
    }

    val broadcastRelation =
      broadcastExchange.executeColumnarBroadcast[SerializeConcatHostBuffersDeserializeBatch]()

    if (boundCondition.isEmpty) {
      doUnconditionalJoin(broadcastRelation)
    } else {
      doConditionalJoin(broadcastRelation, boundCondition, numFirstTableColumns)
    }
  }

  private def doUnconditionalJoin(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch]
  ): RDD[ColumnarBatch] = {
    if (output.isEmpty) {
      doUnconditionalJoinRowCount(broadcastRelation)
    } else {
      val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val buildTime = gpuLongMetric(BUILD_TIME)
      val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
      lazy val builtBatch = makeBuiltBatch(broadcastRelation, buildTime, buildDataSize)
      joinType match {
        case LeftSemi =>
          // just return the left table
          left.executeColumnar().mapPartitions { leftIter =>
            leftIter.map { cb =>
              joinOutputRows += cb.numRows()
              numOutputRows += cb.numRows()
              numOutputBatches += 1
              cb
            }
          }
        case LeftAnti =>
          // degenerate case, no rows are returned.
          val childRDD = left.executeColumnar()
          new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, childRDD.getNumPartitions)
        case _ =>
          // Everything else is treated like an unconditional cross join
          val buildSide = getGpuBuildSide
          val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
          val joinTime = gpuLongMetric(JOIN_TIME)
          val totalTime = gpuLongMetric(TOTAL_TIME)
          streamed.executeColumnar().mapPartitions { streamedIter =>
            val lazyStream = streamedIter.map { cb =>
              withResource(cb) { cb =>
                LazySpillableColumnarBatch(cb, spillCallback, "stream_batch")
              }
            }
            new CrossJoinIterator(
              LazySpillableColumnarBatch(builtBatch, spillCallback, "built_batch"),
              lazyStream,
              targetSizeBytes,
              buildSide,
              joinTime = joinTime,
              totalTime = totalTime)
          }
      }
    }
  }

  /** Special-case handling of an unconditional join that just needs to output a row count. */
  private def doUnconditionalJoinRowCount(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch]
  ): RDD[ColumnarBatch] = {
    if (joinType == LeftAnti) {
      // degenerate case, no rows are returned.
      left.executeColumnar().mapPartitions { _ =>
        Iterator.single(new ColumnarBatch(Array(), 0))
      }
    } else {
      lazy val buildCount = if (joinType == LeftSemi) {
        // one-to-one mapping from input rows to output rows
        1
      } else {
        val buildTime = gpuLongMetric(BUILD_TIME)
        val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
        computeBuildRowCount(broadcastRelation, buildTime, buildDataSize)
      }

      def getRowCountAndClose(cb: ColumnarBatch): Long = {
        val ret = cb.numRows()
        cb.close()
        GpuSemaphore.releaseIfNecessary(TaskContext.get())
        ret
      }

      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val counts = streamed.executeColumnar().map(getRowCountAndClose)
      GpuBroadcastNestedLoopJoinExecBase.divideIntoBatches(
        counts.map(s => s * buildCount),
        targetSizeBytes,
        numOutputRows,
        numOutputBatches)
    }
  }

  private def doConditionalJoin(
      broadcastRelation: Broadcast[SerializeConcatHostBuffersDeserializeBatch],
      boundCondition: Option[GpuExpression],
      numFirstTableColumns: Int): RDD[ColumnarBatch] = {
    val buildTime = gpuLongMetric(BUILD_TIME)
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    lazy val builtBatch = makeBuiltBatch(broadcastRelation, buildTime, buildDataSize)

    val streamAttributes = streamed.output
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val totalTime = gpuLongMetric(TOTAL_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
    val nestedLoopJoinType = joinType
    val buildSide = getGpuBuildSide
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
    streamed.executeColumnar().mapPartitions { streamedIter =>
      val lazyStream = streamedIter.map { cb =>
        withResource(cb) { cb =>
          LazySpillableColumnarBatch(cb, spillCallback, "stream_batch")
        }
      }
      GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(
        nestedLoopJoinType, buildSide, numFirstTableColumns,
        LazySpillableColumnarBatch(builtBatch, spillCallback, "built_batch"),
        lazyStream, streamAttributes, targetSizeBytes, boundCondition, spillCallback,
        numOutputRows = numOutputRows,
        joinOutputRows = joinOutputRows,
        numOutputBatches = numOutputBatches,
        joinTime = joinTime,
        totalTime = totalTime)
    }
  }
}
