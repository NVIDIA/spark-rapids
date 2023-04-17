/*
 * Copyright (c) 2020-2023, NVIDIA CORPORATION.
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

import ai.rapids.cudf
import ai.rapids.cudf.{ast, GatherMap, NvtxColor, OutOfBoundsPolicy, Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.shims.{GpuBroadcastJoinMeta, ShimBinaryExecNode}

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
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

abstract class GpuBroadcastNestedLoopJoinMetaBase(
    join: BroadcastNestedLoopJoinExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends GpuBroadcastJoinMeta[BroadcastNestedLoopJoinExec](join, conf, parent, rule) {

  val conditionMeta: Option[BaseExprMeta[_]] =
    join.condition.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val gpuBuildSide: GpuBuildSide = GpuJoinUtils.getGpuBuildSide(join.buildSide)

  override def namedChildExprs: Map[String, Seq[BaseExprMeta[_]]] =
    JoinTypeChecks.nonEquiJoinMeta(conditionMeta)

  override val childExprs: Seq[BaseExprMeta[_]] = conditionMeta.toSeq

  override def tagPlanForGpu(): Unit = {
    JoinTypeChecks.tagForGpu(join.joinType, this)
    join.joinType match {
      case _: InnerLike =>
      case LeftOuter | RightOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
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
}

/**
 * An iterator that does a cross join against a stream of batches.
 */
class CrossJoinIterator(
    builtBatch: LazySpillableColumnarBatch,
    stream: Iterator[LazySpillableColumnarBatch],
    targetSize: Long,
    buildSide: GpuBuildSide,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends AbstractGpuJoinIterator(
      "Cross join gather",
      targetSize,
      opTime,
      joinTime) {
  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtBatch.close()
    }
  }

  override def hasNextStreamBatch: Boolean = stream.hasNext

  override def setupNextGatherer(): Option[JoinGatherer] = {
    val streamBatch = stream.next()

    // Don't include stream in op time.
    opTime.ns {
      // Don't close the built side because it will be used for each stream and closed
      // when the iterator is done.
      val (leftBatch, rightBatch) = buildSide match {
        case GpuBuildLeft => (LazySpillableColumnarBatch.spillOnly(builtBatch), streamBatch)
        case GpuBuildRight => (streamBatch, LazySpillableColumnarBatch.spillOnly(builtBatch))
      }

      val leftMap = LazySpillableGatherMap.leftCross(leftBatch.numRows, rightBatch.numRows)
      val rightMap = LazySpillableGatherMap.rightCross(leftBatch.numRows, rightBatch.numRows)

      // Cross joins do not need to worry about bounds checking because the gather maps
      // are generated using mod and div based on the number of rows on the left and
      // right, so we specify here `DONT_CHECK` for all.
      val joinGatherer = (leftBatch.numCols, rightBatch.numCols) match {
        case (_, 0) =>
          rightBatch.close()
          rightMap.close()
          JoinGatherer(leftMap, leftBatch, OutOfBoundsPolicy.DONT_CHECK)
        case (0, _) =>
          leftBatch.close()
          leftMap.close()
          JoinGatherer(rightMap, rightBatch, OutOfBoundsPolicy.DONT_CHECK)
        case (_, _) =>
          JoinGatherer(leftMap, leftBatch, rightMap, rightBatch,
            OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.DONT_CHECK)
      }
      if (joinGatherer.isDone) {
        joinGatherer.close()
        None
      } else {
        Some(joinGatherer)
      }
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
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends SplittableJoinIterator(
      s"$joinType join gather",
      stream,
      streamAttributes,
      builtBatch,
      targetSize,
      opTime = opTime,
      joinTime = joinTime) {
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
          case _: InnerLike =>left.conditionalInnerJoinRowCount(right, condition)
          case LeftOuter => left.conditionalLeftJoinRowCount(right, condition)
          case RightOuter => right.conditionalLeftJoinRowCount(left, condition)
          case LeftSemi => left.conditionalLeftSemiJoinRowCount(right, condition)
          case LeftAnti => left.conditionalLeftAntiJoinRowCount(right, condition)
          case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
        }
      }
    }
  }

  override def createGatherer(
      cb: LazySpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    if (numJoinRows.contains(0)) {
      // nothing matched
      return None
    }
    // cb will be closed by the caller, so use a spill-only version here
    val spillOnlyCb = LazySpillableColumnarBatch.spillOnly(cb)
    val batches = Seq(builtBatch, spillOnlyCb)
    batches.foreach(_.checkpoint())
    withRetryNoSplit {
      withRestoreOnRetry(batches) {
        withResource(GpuColumnVector.from(builtBatch.getBatch)) { builtTable =>
          withResource(GpuColumnVector.from(cb.getBatch)) { streamTable =>
          // We need a new LSCB that will be taken over by the gatherer, or closed
          closeOnExcept(LazySpillableColumnarBatch(spillOnlyCb.getBatch, "stream_data")) {
              streamBatch =>
                val builtSpillOnly = LazySpillableColumnarBatch.spillOnly(builtBatch)
                val (leftTable, leftBatch, rightTable, rightBatch) = buildSide match {
                  case GpuBuildLeft => (builtTable, builtSpillOnly, streamTable, streamBatch)
                  case GpuBuildRight => (streamTable, streamBatch, builtTable, builtSpillOnly)
                }
                val maps = computeGatherMaps(leftTable, rightTable, numJoinRows)
                makeGatherer(maps, leftBatch, rightBatch, joinType)
            }
          }
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
          left.conditionalInnerJoinGatherMaps(right, condition, rowCount)
        }.getOrElse {
          left.conditionalInnerJoinGatherMaps(right, condition)
        }
      case LeftOuter =>
        numJoinRows.map { rowCount =>
          left.conditionalLeftJoinGatherMaps(right, condition, rowCount)
        }.getOrElse {
          left.conditionalLeftJoinGatherMaps(right, condition)
        }
      case RightOuter =>
        val maps = numJoinRows.map { rowCount =>
          right.conditionalLeftJoinGatherMaps(left, condition, rowCount)
        }.getOrElse {
          right.conditionalLeftJoinGatherMaps(left, condition)
        }
        // Reverse the output of the join, because we expect the right gather map to
        // always be on the right
        maps.reverse
      case LeftSemi =>
        numJoinRows.map { rowCount =>
          Array(left.conditionalLeftSemiJoinGatherMap(right, condition, rowCount))
        }.getOrElse {
          Array(left.conditionalLeftSemiJoinGatherMap(right, condition))
        }
      case LeftAnti =>
        numJoinRows.map { rowCount =>
          Array(left.conditionalLeftAntiJoinGatherMap(right, condition, rowCount))
        }.getOrElse {
          Array(left.conditionalLeftAntiJoinGatherMap(right, condition))
        }
      case _ => throw new IllegalStateException(s"Unsupported join type $joinType")
    }
  }
}

object GpuBroadcastNestedLoopJoinExecBase {
  def nestedLoopJoin(
      joinType: JoinType,
      buildSide: GpuBuildSide,
      numFirstTableColumns: Int,
      builtBatch: LazySpillableColumnarBatch,
      stream: Iterator[LazySpillableColumnarBatch],
      streamAttributes: Seq[Attribute],
      targetSize: Long,
      boundCondition: Option[GpuExpression],
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
    val joinIterator = if (boundCondition.isEmpty) {
      // Semi and anti nested loop joins without a condition are degenerate joins and should have
      // been handled at a higher level rather than calling this method.
      assert(joinType.isInstanceOf[InnerLike], s"Unexpected unconditional join type: $joinType")
      new CrossJoinIterator(builtBatch, stream, targetSize, buildSide, opTime, joinTime)
    } else {
      if (joinType.isInstanceOf[ExistenceJoin]) {
        if (builtBatch.numCols == 0) {
          degenerateExistsJoinIterator(stream, builtBatch, boundCondition.get)
        } else {
          val compiledAst = boundCondition.get.convertToAst(numFirstTableColumns).compile()
          new ConditionalNestedLoopExistenceJoinIterator(
            builtBatch, stream, compiledAst, opTime, joinTime)
        }
      } else {
        val compiledAst = boundCondition.get.convertToAst(numFirstTableColumns).compile()
        new ConditionalNestedLoopJoinIterator(joinType, buildSide, builtBatch,
          stream, streamAttributes, targetSize, compiledAst,
          opTime = opTime, joinTime = joinTime)
      }
    }
    joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
    }
  }

  private def degenerateExistsJoinIterator(
      stream: Iterator[LazySpillableColumnarBatch],
      builtBatch: LazySpillableColumnarBatch,
      boundCondition: GpuExpression): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      override def hasNext: Boolean = stream.hasNext

      override def next(): ColumnarBatch = {
        withResource(stream.next()) { streamSpillable =>
          val streamBatch = streamSpillable.getBatch
          val existsCol: ColumnVector = if (builtBatch.numRows == 0) {
            withResource(Scalar.fromBool(false)) { falseScalar =>
              GpuColumnVector.from(cudf.ColumnVector.fromScalar(falseScalar, streamBatch.numRows),
                BooleanType)
            }
          } else {
            withResource(GpuExpressionsUtils.columnarEvalToColumn(
              boundCondition, streamBatch)) { condEval =>
              withResource(Scalar.fromBool(false)) { falseScalar =>
                GpuColumnVector.from(condEval.getBase.replaceNulls(falseScalar), BooleanType)
              }
            }
          }
          withResource(new ColumnarBatch(Array(existsCol), streamBatch.numRows)) { existsBatch =>
            GpuColumnVector.combineColumns(streamBatch, existsBatch)
          }
        }
      }
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
    gpuBuildSide: GpuBuildSide,
    condition: Option[Expression],
    targetSizeBytes: Long) extends ShimBinaryExecNode with GpuExec {

  import GpuMetric._

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException("This should only be called from columnar")

  override val outputRowsLevel: MetricsLevel = ESSENTIAL_LEVEL
  override val outputBatchesLevel: MetricsLevel = MODERATE_LEVEL
  override lazy val additionalMetrics: Map[String, GpuMetric] = Map(
    OP_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_OP_TIME),
    BUILD_DATA_SIZE -> createSizeMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_DATA_SIZE),
    BUILD_TIME -> createNanoTimingMetric(MODERATE_LEVEL, DESCRIPTION_BUILD_TIME),
    JOIN_TIME -> createNanoTimingMetric(DEBUG_LEVEL, DESCRIPTION_JOIN_TIME),
    JOIN_OUTPUT_ROWS -> createMetric(MODERATE_LEVEL, DESCRIPTION_JOIN_OUTPUT_ROWS))

  /** BuildRight means the right relation <=> the broadcast relation. */
  val (streamed, buildPlan) = gpuBuildSide match {
    case GpuBuildRight => (left, right)
    case GpuBuildLeft => (right, left)
  }

  def broadcastExchange: GpuBroadcastExchangeExecBase = buildPlan match {
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[GpuBroadcastExchangeExecBase] =>
      bqse.plan.asInstanceOf[GpuBroadcastExchangeExecBase]
    case bqse: BroadcastQueryStageExec if bqse.plan.isInstanceOf[ReusedExchangeExec] =>
      bqse.plan.asInstanceOf[ReusedExchangeExec].child.asInstanceOf[GpuBroadcastExchangeExecBase]
    case gpu: GpuBroadcastExchangeExecBase => gpu
    case reused: ReusedExchangeExec => reused.child.asInstanceOf[GpuBroadcastExchangeExecBase]
  }

  override def requiredChildDistribution: Seq[Distribution] = gpuBuildSide match {
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

  protected def makeBroadcastBuiltBatch(
      broadcastRelation: Broadcast[Any],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      val builtBatch = GpuBroadcastHelper.getBroadcastBatch(broadcastRelation, buildPlan.schema)
      buildDataSize += GpuColumnVector.getTotalDeviceMemoryUsed(builtBatch)
      builtBatch
    }
  }

  protected def computeBroadcastBuildRowCount(
      broadcastRelation: Broadcast[Any],
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    withResource(new NvtxWithMetrics("build join table", NvtxColor.GREEN, buildTime)) { _ =>
      buildDataSize += 0
      GpuBroadcastHelper.getBroadcastBatchNumRows(broadcastRelation)
    }
  }

  protected def makeBuiltBatch(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): ColumnarBatch = {
    // NOTE: pattern matching doesn't work here because of type-invariance
    val broadcastRelation = relation.asInstanceOf[Broadcast[Any]]
    makeBroadcastBuiltBatch(broadcastRelation, buildTime, buildDataSize)
  }

  protected def computeBuildRowCount(
      relation: Any,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): Int = {
    // NOTE: pattern matching doesn't work here because of type-invariance
    val broadcastRelation = relation.asInstanceOf[Broadcast[Any]]
    computeBroadcastBuildRowCount(broadcastRelation, buildTime, buildDataSize)
  }

  protected def getBroadcastRelation(): Any = {
    broadcastExchange.executeColumnarBroadcast[Any]()
  }

  override def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
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

    val broadcastRelation = getBroadcastRelation()

    val joinCondition = boundCondition.orElse {
      // For outer joins use a true condition if there are any columns in the build side
      // otherwise use a cross join.
      val useTrueCondition = joinType match {
        case LeftOuter if gpuBuildSide == GpuBuildRight => right.output.nonEmpty
        case RightOuter if gpuBuildSide == GpuBuildLeft => left.output.nonEmpty
        case _ => false
      }
      if (useTrueCondition) Some(GpuLiteral(true)) else None
    }

    if (joinCondition.isEmpty) {
      doUnconditionalJoin(broadcastRelation)
    } else {
      doConditionalJoin(broadcastRelation, joinCondition, numFirstTableColumns)
    }
  }

  private def leftExistenceJoin(
      relation: Any,
      exists: Boolean,
      buildTime: GpuMetric,
      buildDataSize: GpuMetric): RDD[ColumnarBatch] = {
    assert(gpuBuildSide == GpuBuildRight)
    streamed.executeColumnar().mapPartitionsInternal { streamedIter =>
      val buildRows = computeBuildRowCount(relation, buildTime, buildDataSize)
      if (buildRows > 0 == exists) {
        streamedIter
      } else {
        Iterator.empty
      }
    }
  }

  private def doUnconditionalJoin(relation: Any): RDD[ColumnarBatch] = {
    // Existence join should have a condition
    assert(!joinType.isInstanceOf[ExistenceJoin])

    if (output.isEmpty) {
      doUnconditionalJoinRowCount(relation)
    } else {
      val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
      val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
      val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
      val buildTime = gpuLongMetric(BUILD_TIME)
      val opTime = gpuLongMetric(OP_TIME)
      val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
      // NOTE: this is a def because we want a brand new `ColumnarBatch` to be returned
      // per partition (task), since each task is going to be taking ownership
      // of a columnar batch via `LazySpillableColumnarBatch`.
      // There are better ways to fix this: https://github.com/NVIDIA/spark-rapids/issues/7642
      def builtBatch = {
        makeBuiltBatch(relation, buildTime, buildDataSize)
      }
      val joinIterator: RDD[ColumnarBatch] = joinType match {
        case LeftSemi =>
          if (gpuBuildSide == GpuBuildRight) {
            leftExistenceJoin(relation, exists=true, buildTime, buildDataSize)
          } else {
            left.executeColumnar()
          }
        case LeftAnti =>
          if (gpuBuildSide == GpuBuildRight) {
            leftExistenceJoin(relation, exists=false, buildTime, buildDataSize)
          } else {
            // degenerate case, no rows are returned.
            val childRDD = left.executeColumnar()
            new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, childRDD.getNumPartitions)
          }
        case _ =>
          // Everything else is treated like an unconditional cross join
          val buildSide = gpuBuildSide
          val joinTime = gpuLongMetric(JOIN_TIME)
          streamed.executeColumnar().mapPartitions { streamedIter =>
            val lazyStream = streamedIter.map { cb =>
              withResource(cb) { cb =>
                LazySpillableColumnarBatch(cb, "stream_batch")
              }
            }
            val spillableBuiltBatch = withResource(builtBatch) {
              LazySpillableColumnarBatch(_, "built_batch")
            }
            new CrossJoinIterator(
              spillableBuiltBatch,
              lazyStream,
              targetSizeBytes,
              buildSide,
              opTime = opTime,
              joinTime = joinTime)
          }
      }
      joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }

  /** Special-case handling of an unconditional join that just needs to output a row count. */
  private def doUnconditionalJoinRowCount(relation: Any): RDD[ColumnarBatch] = {
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
        computeBuildRowCount(relation, buildTime, buildDataSize)
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
      relation: Any,
      boundCondition: Option[GpuExpression],
      numFirstTableColumns: Int): RDD[ColumnarBatch] = {
    val buildTime = gpuLongMetric(BUILD_TIME)
    val buildDataSize = gpuLongMetric(BUILD_DATA_SIZE)
    // NOTE: this is a def because we want a brand new `ColumnarBatch` to be returned
    // per partition (task), since each task is going to be taking ownership
    // of a columnar batch via `LazySpillableColumnarBatch`.
    // There are better ways to fix this: https://github.com/NVIDIA/spark-rapids/issues/7642
    def builtBatch: ColumnarBatch = {
      makeBuiltBatch(relation, buildTime, buildDataSize)
    }
    val streamAttributes = streamed.output
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val opTime = gpuLongMetric(OP_TIME)
    val joinTime = gpuLongMetric(JOIN_TIME)
    val joinOutputRows = gpuLongMetric(JOIN_OUTPUT_ROWS)
    val nestedLoopJoinType = joinType
    val buildSide = gpuBuildSide
    streamed.executeColumnar().mapPartitions { streamedIter =>
      val lazyStream = streamedIter.map { cb =>
        withResource(cb) { cb =>
          LazySpillableColumnarBatch(cb, "stream_batch")
        }
      }
      val spillableBuiltBatch = withResource(builtBatch) {
        LazySpillableColumnarBatch(_, "built_batch")
      }

      GpuBroadcastNestedLoopJoinExecBase.nestedLoopJoin(
        nestedLoopJoinType, buildSide, numFirstTableColumns,
        spillableBuiltBatch,
        lazyStream, streamAttributes, targetSizeBytes, boundCondition,
        numOutputRows = numOutputRows,
        joinOutputRows = joinOutputRows,
        numOutputBatches = numOutputBatches,
        opTime = opTime,
        joinTime = joinTime)
    }
  }
}

class ConditionalNestedLoopExistenceJoinIterator(
    spillableBuiltBatch: LazySpillableColumnarBatch,
    lazyStream: Iterator[LazySpillableColumnarBatch],
    condition: CompiledExpression,
    opTime: GpuMetric,
    joinTime: GpuMetric
) extends ExistenceJoinIterator(spillableBuiltBatch, lazyStream, opTime, joinTime) {

  use(condition)

  override def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap = {
    withResource(
      new NvtxWithMetrics("existence join scatter map", NvtxColor.ORANGE, joinTime)) { _ =>
      withResource(GpuColumnVector.from(leftColumnarBatch)) { leftTab =>
        withResource(GpuColumnVector.from(spillableBuiltBatch.getBatch)) { rightTab =>
          leftTab.conditionalLeftSemiJoinGatherMap(rightTab, condition)
        }
      }
    }
  }
}
