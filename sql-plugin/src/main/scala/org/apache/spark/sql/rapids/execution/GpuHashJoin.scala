/*
 * Copyright (c) 2020-2024, NVIDIA CORPORATION. All rights reserved.
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

import ai.rapids.cudf.{ColumnView, DType, GatherMap, GroupByAggregation, NullEquality, NullPolicy, NvtxColor, OutOfBoundsPolicy, ReductionAggregation, Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits.AutoCloseableProducingSeq
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.jni.GpuOOM
import com.nvidia.spark.rapids.shims.ShimBinaryExecNode

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

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
      case ExistenceJoin(_) if !conf.areExistenceJoinsEnabled =>
        meta.willNotWorkOnGpu("existence joins have been disabled. To enable set " +
            s"${RapidsConf.ENABLE_EXISTENCE_JOIN.key} to true")
      case _ => // not disabled
    }
  }

  val LEFT_KEYS = "leftKeys"
  val RIGHT_KEYS = "rightKeys"
  val CONDITION = "condition"

  private[this] val cudfSupportedKeyTypes =
    (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_128 + TypeSig.STRUCT).nested()
  private[this] val sparkSupportedJoinKeyTypes = TypeSig.all - TypeSig.MAP.nested()

  private[this] val joinRideAlongTypes =
    (cudfSupportedKeyTypes + TypeSig.DECIMAL_128 + TypeSig.BINARY +
        TypeSig.ARRAY + TypeSig.MAP).nested()

  val equiJoinExecChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(
      LEFT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      RIGHT_KEYS -> InputCheck(cudfSupportedKeyTypes, sparkSupportedJoinKeyTypes),
      CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN)))

  def equiJoinMeta(leftKeys: Seq[BaseExprMeta[_]],
      rightKeys: Seq[BaseExprMeta[_]],
      condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] = {
    Map(
      LEFT_KEYS -> leftKeys,
      RIGHT_KEYS -> rightKeys,
      CONDITION -> condition.toSeq)
  }

  val nonEquiJoinChecks: ExecChecks = ExecChecks(
    joinRideAlongTypes,
    TypeSig.all,
    Map(CONDITION -> InputCheck(TypeSig.BOOLEAN, TypeSig.BOOLEAN,
      notes = List("A non-inner join only is supported if the condition expression can be " +
          "converted to a GPU AST expression"))))

  def nonEquiJoinMeta(condition: Option[BaseExprMeta[_]]): Map[String, Seq[BaseExprMeta[_]]] =
    Map(CONDITION -> condition.toSeq)
}

object GpuHashJoin {

  def tagJoin(
      meta: SparkPlanMeta[_],
      joinType: JoinType,
      buildSide: GpuBuildSide,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      conditionMeta: Option[BaseExprMeta[_]]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)

    JoinTypeChecks.tagForGpu(joinType, meta)
    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti | ExistenceJoin(_) =>
        conditionMeta.foreach(meta.requireAstForGpuOn)
      case FullOuter =>
        conditionMeta.foreach(meta.requireAstForGpuOn)
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
          meta.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
        }
      case _ =>
        meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }

    buildSide match {
      case GpuBuildLeft if !canBuildLeft(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support left-side build")
      case GpuBuildRight if !canBuildRight(joinType) =>
        meta.willNotWorkOnGpu(s"$joinType does not support right-side build")
      case _ =>
    }
  }

  /** Determine if this type of join supports using the right side of the join as the build side. */
  def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | FullOuter | _: ExistenceJoin => true
    case _ => false
  }

  /** Determine if this type of join supports using the left side of the join as the build side. */
  def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter | FullOuter => true
    case _ => false
  }

  def extractTopLevelAttributes(
      exprs: Seq[Expression],
      includeAlias: Boolean): Seq[Option[Attribute]] =
    exprs.map {
      case a: AttributeReference => Some(a.toAttribute)
      case GpuAlias(a: AttributeReference, _) if includeAlias => Some(a.toAttribute)
      case _ => None
    }

  /**
   * Filter rows from the batch where any of the keys are null.
   */
  def filterNullsWithRetryAndClose(
      sb: SpillableColumnarBatch,
      boundKeys: Seq[Expression]): ColumnarBatch = {
    withRetryNoSplit(sb) { _ =>
      withResource(sb.getColumnarBatch()) { cb =>
        filterNulls(cb, boundKeys)
      }
    }
  }

  private def filterNulls(cb: ColumnarBatch, boundKeys: Seq[Expression]): ColumnarBatch = {
    var mask: ai.rapids.cudf.ColumnVector = null
    try {
      withResource(GpuProjectExec.project(cb, boundKeys)) { keys =>
        val keyColumns = GpuColumnVector.extractBases(keys)
        keyColumns.foreach { column =>
          if (column.hasNulls) {
            withResource(column.isNotNull) { nn =>
              if (mask == null) {
                mask = nn.incRefCount()
              } else {
                mask = withResource(mask) { _ =>
                  mask.and(nn)
                }
              }
            }
          }
        }
      }

      if (mask == null) {
        // There was nothing to filter.
        GpuColumnVector.incRefCounts(cb)
      } else {
        val colTypes = GpuColumnVector.extractTypes(cb)
        withResource(GpuColumnVector.from(cb)) { tbl =>
          withResource(tbl.filter(mask)) { filtered =>
            GpuColumnVector.from(filtered, colTypes)
          }
        }
      }
    } finally {
      if (mask != null) {
        mask.close()
      }
    }
  }

  /**
   * Given sequence of expressions, detect whether there exists any StructType expressions
   * who contains nullable child columns.
   * Since cuDF can not match nullable children as Spark during join, we detect them before join
   * to apply some walking around strategies. For some details, please refer the issue:
   * https://github.com/NVIDIA/spark-rapids/issues/2126.
   *
   * NOTE that this does not work for arrays of Structs or Maps that are not supported as join keys
   * yet.
   */
  def anyNullableStructChild(expressions: Seq[Expression]): Boolean = {
    def anyNullableChild(struct: StructType): Boolean = {
      struct.fields.exists { field =>
        if (field.nullable) {
          true
        } else field.dataType match {
          case structType: StructType =>
            anyNullableChild(structType)
          case _ => false
        }
      }
    }

    expressions.map(_.dataType).exists {
      case st: StructType =>
        anyNullableChild(st)
      case _ => false
    }
  }

  // scalastyle:off line.size.limit
  /**
   * The function is copied from Spark 3.2:
   *   https://github.com/apache/spark/blob/v3.2.2/sql/core/src/main/scala/org/apache/spark/sql/execution/joins/HashJoin.scala#L709-L713
   *
   * Returns whether the keys can be rewritten as a packed long. If
   * they can, we can assume that they are packed when we extract them out.
   */
  // scalastyle:on
  def canRewriteAsLongType(keys: Seq[Expression]): Boolean = {
    // TODO: support BooleanType, DateType and TimestampType
    keys.forall(_.dataType.isInstanceOf[IntegralType]) &&
      keys.map(_.dataType.defaultSize).sum <= 8
  }
}

abstract class BaseHashJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[Expression],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[Expression],
    streamAttributes: Seq[Attribute],
    targetSize: Long,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends SplittableJoinIterator(
      s"hash $joinType gather",
      stream,
      streamAttributes,
      built,
      targetSize,
      opTime = opTime,
      joinTime = joinTime) {
  // We can cache this because the build side is not changing
  protected lazy val (streamMagnificationFactor, isDistinctJoin) = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
      built.checkpoint()
      withRetryNoSplit {
        withRestoreOnRetry(built) {
          // This is okay because the build keys must be deterministic
          withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
            guessStreamMagnificationFactor(builtKeys)
          }
        }
      }
    case _ =>
      // existence joins don't change size
      (1.0, false)
  }

  override def computeNumJoinRows(cb: LazySpillableColumnarBatch): Long = {
    // TODO: Replace this estimate with exact join row counts using the corresponding cudf APIs
    //       being added in https://github.com/rapidsai/cudf/issues/9053.
    joinType match {
      // Full Outer join is implemented via LeftOuter/RightOuter, so use same estimate.
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        Math.ceil(cb.numRows * streamMagnificationFactor).toLong
      case _ => cb.numRows
    }
  }

  override def createGatherer(
      cb: LazySpillableColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    // cb will be closed by the caller, so use a spill-only version here
    val spillOnlyCb = LazySpillableColumnarBatch.spillOnly(cb)
    val batches = Seq(built, spillOnlyCb)
    batches.foreach(_.checkpoint())
    try {
      withRetryNoSplit {
        withRestoreOnRetry(batches) {
          // We need a new LSCB that will be taken over by the gatherer, or closed
          closeOnExcept(LazySpillableColumnarBatch(spillOnlyCb.getBatch, "stream_data")) {
            streamBatch =>
              // the original stream data batch is not spillable until
              // we ask it to be right here, because we called `getBatch` on it
              // to make up the new lazy spillable (`streamBatch`)
              spillOnlyCb.allowSpilling()

              withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
                // ensure that the build data can be spilled
                built.allowSpilling()
                joinGatherer(builtKeys, built, streamBatch)
              }
          }
        }
      }
    } catch {
      // This should work for all join types. There should be no need to do this for any
      // of the existence joins because the output rows will never be larger than the
      // input rows on the stream side.
      case oom @ (_ : OutOfMemoryError | _: GpuOOM) if joinType.isInstanceOf[InnerLike]
          || joinType == LeftOuter
          || joinType == RightOuter
          || joinType == FullOuter =>
        // Because this is just an estimate, it is possible for us to get this wrong, so
        // make sure we at least split the batch in half.
        val numBatches = Math.max(2, estimatedNumBatches(spillOnlyCb))

        // Split batch and return no gatherer so the outer loop will try again
        splitAndSave(spillOnlyCb.getBatch, numBatches, Some(oom))
        None
    }
  }

  /**
   * Perform a hash join, returning a gatherer if there is a join result.
   *
   * @param leftKeys table of join keys from the left table
   * @param leftData batch containing the full data from the left table
   * @param rightKeys table of join keys from the right table
   * @param rightData batch containing the full data from the right table
   * @return join gatherer if there are join results
   */
  protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer]

  private def joinGathererLeftRight(
      leftKeys: ColumnarBatch,
      leftData: LazySpillableColumnarBatch,
      rightKeys: ColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuColumnVector.from(leftKeys)) { leftKeysTab =>
      withResource(GpuColumnVector.from(rightKeys)) { rightKeysTab =>
        joinGathererLeftRight(leftKeysTab, leftData, rightKeysTab, rightData)
      }
    }
  }

  private def joinGatherer(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamKeys: ColumnarBatch,
      streamData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    buildSide match {
      case GpuBuildLeft =>
        joinGathererLeftRight(buildKeys, buildData, streamKeys, streamData)
      case GpuBuildRight =>
        joinGathererLeftRight(streamKeys, streamData, buildKeys, buildData)
    }
  }

  private def joinGatherer(
      buildKeys: ColumnarBatch,
      buildData: LazySpillableColumnarBatch,
      streamCb: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuProjectExec.project(streamCb.getBatch, boundStreamKeys)) { streamKeys =>
      // ensure we make the stream side spillable again
      streamCb.allowSpilling()
      joinGatherer(buildKeys, LazySpillableColumnarBatch.spillOnly(buildData), streamKeys, streamCb)
    }
  }

  private def countGroups(keys: ColumnarBatch): Table = {
    withResource(GpuColumnVector.from(keys)) { keysTable =>
      keysTable.groupBy(0 until keysTable.getNumberOfColumns: _*)
          .aggregate(GroupByAggregation.count(NullPolicy.INCLUDE).onColumn(0))
    }
  }

  /**
   * Guess the magnification factor for a stream side batch and detect if the build side contains
   * only unique join keys.
   * This is temporary until cudf gives us APIs to get the actual gather map size.
   */
  private def guessStreamMagnificationFactor(builtKeys: ColumnarBatch): (Double, Boolean) = {
    // Based off of the keys on the build side guess at how many output rows there
    // will be for each input row on the stream side. This does not take into account
    // the join type, data skew or even if the keys actually match.
    withResource(countGroups(builtKeys)) { builtCount =>
      val isDistinct = builtCount.getRowCount == builtKeys.numRows()
      val counts = builtCount.getColumn(builtCount.getNumberOfColumns - 1)
      withResource(counts.reduce(ReductionAggregation.mean(), DType.FLOAT64)) { scalarAverage =>
        (scalarAverage.getDouble, isDistinct)
      }
    }
  }

  private def estimatedNumBatches(cb: LazySpillableColumnarBatch): Int = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
      // We want the gather map size to be around the target size. There are two gather maps
      // that are made up of ints, so estimate how many rows per batch on the stream side
      // will produce the desired gather map size.
      val approximateStreamRowCount = ((targetSize.toDouble / 2) /
          DType.INT32.getSizeInBytes) / streamMagnificationFactor
      val estimatedRowsPerStreamBatch = Math.min(Int.MaxValue, approximateStreamRowCount)
      Math.ceil(cb.numRows / estimatedRowsPerStreamBatch).toInt
    case _ => 1
  }
}

/**
 * An iterator that does a hash join against a stream of batches.
 */
class HashJoinIterator(
    built: LazySpillableColumnarBatch,
    val boundBuiltKeys: Seq[Expression],
    private val stream: Iterator[LazySpillableColumnarBatch],
    val boundStreamKeys: Seq[Expression],
    val streamAttributes: Seq[Attribute],
    val targetSize: Long,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    opTime: GpuMetric,
    private val joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      stream,
      boundStreamKeys,
      streamAttributes,
      targetSize,
      joinType,
      buildSide,
      opTime = opTime,
      joinTime = joinTime) {
  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("hash join gather map", NvtxColor.ORANGE, joinTime)) { _ =>
      // hack to work around unique_join not handling empty tables
      if (joinType.isInstanceOf[InnerLike] &&
        (leftKeys.getRowCount == 0 || rightKeys.getRowCount == 0)) {
        None
      } else {
        val maps = joinType match {
          case LeftOuter if isDistinctJoin =>
            Array(leftKeys.leftDistinctJoinGatherMap(rightKeys, compareNullsEqual))
          case LeftOuter => leftKeys.leftJoinGatherMaps(rightKeys, compareNullsEqual)
          case RightOuter =>
            // Reverse the output of the join, because we expect the right gather map to
            // always be on the right
            rightKeys.leftJoinGatherMaps(leftKeys, compareNullsEqual).reverse
          case _: InnerLike if isDistinctJoin =>
            if (buildSide == GpuBuildRight) {
              leftKeys.innerDistinctJoinGatherMaps(rightKeys, compareNullsEqual)
            } else {
              rightKeys.innerDistinctJoinGatherMaps(leftKeys, compareNullsEqual).reverse
            }
          case _: InnerLike => leftKeys.innerJoinGatherMaps(rightKeys, compareNullsEqual)
          case LeftSemi => Array(leftKeys.leftSemiJoinGatherMap(rightKeys, compareNullsEqual))
          case LeftAnti => Array(leftKeys.leftAntiJoinGatherMap(rightKeys, compareNullsEqual))
          case _ =>
            throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
              s" supported")
        }
        makeGatherer(maps, leftData, rightData, joinType)
      }
    }
  }
}

/**
 * An iterator that does a hash join against a stream of batches with an inequality condition.
 * The compiled condition will be closed when this iterator is closed.
 */
class ConditionalHashJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[Expression],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[Expression],
    streamAttributes: Seq[Attribute],
    compiledCondition: CompiledExpression,
    targetSize: Long,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      stream,
      boundStreamKeys,
      streamAttributes,
      targetSize,
      joinType,
      buildSide,
      opTime = opTime,
      joinTime = joinTime) {
  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
    withResource(new NvtxWithMetrics("hash join gather map", NvtxColor.ORANGE, joinTime)) { _ =>
      withResource(GpuColumnVector.from(leftData.getBatch)) { leftTable =>
        withResource(GpuColumnVector.from(rightData.getBatch)) { rightTable =>
          val maps = joinType match {
            case _: InnerLike if buildSide == GpuBuildRight =>
              Table.mixedInnerJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
                compiledCondition, nullEquality)
            case _: InnerLike if buildSide == GpuBuildLeft =>
              // Even though it's an inner join, we need to switch the join order since the
              // condition has been compiled to expect the build side on the left and the stream
              // side on the right.
              // Reverse the output of the join, because we expect the right gather map to
              // always be on the right.
              Table.mixedInnerJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
                compiledCondition, nullEquality).reverse
            case LeftOuter =>
              Table.mixedLeftJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
                compiledCondition, nullEquality)
            case RightOuter =>
              // Reverse the output of the join, because we expect the right gather map to
              // always be on the right
              Table.mixedLeftJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
                compiledCondition, nullEquality).reverse
            case LeftSemi =>
              Array(Table.mixedLeftSemiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
                compiledCondition, nullEquality))
            case LeftAnti =>
              Array(Table.mixedLeftAntiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
                compiledCondition, nullEquality))
            case _ =>
              throw new NotImplementedError(s"Join $joinType $buildSide is not currently supported")
          }
          makeGatherer(maps, leftData, rightData, joinType)
        }
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      compiledCondition.close()
    }
  }
}


/**
 * An iterator that does the stream-side only of a hash full join  In other words, it performs the
 * left or right outer join for the stream side's view of a full outer join. As the join is
 * performed, the build-side rows that are referenced during the join are tracked and can be
 * retrieved after the iteration has completed to assist in performing the anti-join needed to
 * produce the final results needed for the full outer join.
 *
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildSideTrackerInit initial value of the build side row tracker, if any. This will be
 *                             closed by the iterator.
 * @param stream iterator to produce batches for the stream side table
 * @param boundStreamKeys bound expressions for the stream side equi-join keys
 * @param streamAttributes schema of the stream side table
 * @param compiledCondition compiled AST expression for the inequality condition of the join,
 *                          if any. NOTE: This will *not* be closed by the iterator.
 * @param targetSize target GPU batch size in bytes
 * @param buildSide which side of the join is being used for the build side
 * @param compareNullsEqual whether to compare nulls as equal during the join
 * @param opTime metric to update for total operation time
 * @param joinTime metric to update for join time
 */
class HashFullJoinStreamSideIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[Expression],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[Expression],
    streamAttributes: Seq[Attribute],
    compiledCondition: Option[CompiledExpression],
    targetSize: Long,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    opTime: GpuMetric,
    joinTime: GpuMetric)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      stream,
      boundStreamKeys,
      streamAttributes,
      targetSize,
      FullOuter,
      buildSide,
      opTime = opTime,
      joinTime = joinTime) {
  // Full Join is implemented via LeftOuter or RightOuter join, depending on the build side.
  private val useLeftOuterJoin = (buildSide == GpuBuildRight)

  private val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL

  private[this] var builtSideTracker: Option[SpillableColumnarBatch] = buildSideTrackerInit

  private def unconditionalLeftJoinGatherMaps(
      leftKeys: Table, rightKeys: Table): Array[GatherMap] = {
    if (useLeftOuterJoin) {
      leftKeys.leftJoinGatherMaps(rightKeys, compareNullsEqual)
    } else {
      // Reverse the output of the join, because we expect the right gather map to
      // always be on the right
      rightKeys.leftJoinGatherMaps(leftKeys, compareNullsEqual).reverse
    }
  }

  private def conditionalLeftJoinGatherMaps(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch,
      compiledCondition: CompiledExpression): Array[GatherMap] = {
    withResource(GpuColumnVector.from(leftData.getBatch)) { leftTable =>
      withResource(GpuColumnVector.from(rightData.getBatch)) { rightTable =>
        if (useLeftOuterJoin) {
          Table.mixedLeftJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
            compiledCondition, nullEquality)
        } else {
          // Reverse the output of the join, because we expect the right gather map to
          // always be on the right
          Table.mixedLeftJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
            compiledCondition, nullEquality).reverse
        }
      }
    }
  }

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("full hash join gather map",
      NvtxColor.ORANGE, joinTime)) { _ =>
      val maps = compiledCondition.map { condition =>
        conditionalLeftJoinGatherMaps(leftKeys, leftData, rightKeys, rightData, condition)
      }.getOrElse {
        unconditionalLeftJoinGatherMaps(leftKeys, rightKeys)
      }
      assert(maps.length == 2)
      try {
        val lazyLeftMap = LazySpillableGatherMap(maps(0), "left_map")
        val lazyRightMap = LazySpillableGatherMap(maps(1), "right_map")
        withResource(new NvtxWithMetrics("update tracking mask",
          NvtxColor.ORANGE, joinTime)) { _ =>
          closeOnExcept(Seq(lazyLeftMap, lazyRightMap)) { _ =>
            updateTrackingMask(if (buildSide == GpuBuildRight) lazyRightMap else lazyLeftMap)
          }
        }
        val (leftOutOfBoundsPolicy, rightOutOfBoundsPolicy) = {
          if (useLeftOuterJoin) {
            (OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.NULLIFY)
          } else {
            (OutOfBoundsPolicy.NULLIFY, OutOfBoundsPolicy.DONT_CHECK)
          }
        }
        val gatherer = JoinGatherer(lazyLeftMap, leftData, lazyRightMap, rightData,
          leftOutOfBoundsPolicy, rightOutOfBoundsPolicy)
        if (gatherer.isDone) {
          // Nothing matched...
          gatherer.close()
          None
        } else {
          Some(gatherer)
        }
      } finally {
        maps.foreach(_.close())
      }
    }
  }

  // Need to avoid close on exhaust so others can access the built side tracker after iteration.
  override protected val shouldAutoCloseOnExhaust: Boolean = false

  /**
   *  Retrieve the tracking data for the build side rows that have been referenced during the
   *  join. This is normally called after iteration has completed. The caller takes ownership
   *  of the resulting data and is responsible for closing it.
   */
  def releaseBuiltSideTracker(): Option[SpillableColumnarBatch] = {
    val result = builtSideTracker
    builtSideTracker = None
    result
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtSideTracker.foreach(_.close())
      builtSideTracker = None
    }
  }

  private def trueColumnTable(numRows: Int): Table = {
    withResource(Scalar.fromBool(true)) { trueScalar =>
      withResource(ai.rapids.cudf.ColumnVector.fromScalar(trueScalar, numRows)) {
        new Table(_)
      }
    }
  }

  // Create a boolean column to indicate which gather map rows are valid.
  private def validIndexMask(gatherView: ColumnView): ColumnView = {
    withResource(Scalar.fromInt(Int.MinValue)) { invalidIndex =>
      gatherView.notEqualTo(invalidIndex)
    }
  }

  /**
   * Update the tracking mask for the build side.
   */
  private def updateTrackingMask(buildSideGatherMap: LazySpillableGatherMap): Unit = {
    // Filter the build side gather map to remove invalid indices
    val numGatherMapRows = buildSideGatherMap.getRowCount.toInt
    val filteredGatherMap = {
      withResource(buildSideGatherMap.toColumnView(0, numGatherMapRows)) { gatherView =>
        withResource(gatherView.copyToColumnVector()) { gatherVec =>
          withResource(new Table(gatherVec)) { gatherTab =>
            withResource(validIndexMask(gatherView)) { mask =>
              gatherTab.filter(mask)
            }
          }
        }
      }
    }
    // Update all hits in the gather map with false (no longer needed) in the tracking table
    val updatedTrackingTable = withResource(filteredGatherMap) { filteredMap =>
      // Get the current tracking table, or all true table to start with
      val builtTrackingTable = builtSideTracker.map { spillableBatch =>
        withResource(spillableBatch) { scb =>
          withResource(scb.getColumnarBatch()) { trackingBatch =>
            GpuColumnVector.from(trackingBatch)
          }
        }
      }.getOrElse {
        trueColumnTable(built.numRows)
      }
      withResource(builtTrackingTable) { trackingTable =>
        withResource(Scalar.fromBool(false)) { falseScalar =>
          Table.scatter(Array(falseScalar), filteredMap.getColumn(0), trackingTable)
        }
      }
    }
    val previousTracker = builtSideTracker
    builtSideTracker = withResource(updatedTrackingTable) { _ =>
      Some(SpillableColumnarBatch(
        GpuColumnVector.from(updatedTrackingTable, Array[DataType](DataTypes.BooleanType)),
        SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
    }
    // If we throw above, we should not close the existing tracker
    previousTracker.foreach(_.close())
  }
}

/**
 * An iterator that does a hash full join against a stream of batches.  It does this by
 * doing a left or right outer join and keeping track of the hits on the build side.  It then
 * produces a final batch of all the build side rows that were not already included.
 *
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildSideTrackerInit initial value of the build side row tracker, if any. This will be
 *                             closed by the iterator.
 * @param stream iterator to produce batches for the stream side table
 * @param boundStreamKeys bound expressions for the stream side equi-join keys
 * @param streamAttributes schema of the stream side table
 * @param boundCondition expression for the inequality condition of the join, if any
 * @param targetSize target GPU batch size in bytes
 * @param buildSide which side of the join is being used for the build side
 * @param compareNullsEqual whether to compare nulls as equal during the join
 * @param opTime metric to update for total operation time
 * @param joinTime metric to update for join time
 */
class HashFullJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[Expression],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[Expression],
    streamAttributes: Seq[Attribute],
    boundCondition: Option[GpuExpression],
    numFirstConditionTableColumns: Int,
    targetSize: Long,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    opTime: GpuMetric,
    joinTime: GpuMetric) extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  private val compiledCondition: Option[CompiledExpression] = boundCondition.map { gpuExpr =>
    use(opTime.ns(gpuExpr.convertToAst(numFirstConditionTableColumns).compile()))
  }

  private val streamJoinIter = new HashFullJoinStreamSideIterator(built, boundBuiltKeys,
    buildSideTrackerInit, stream, boundStreamKeys, streamAttributes, compiledCondition, targetSize,
    buildSide, compareNullsEqual, opTime, joinTime)

  private var finalBatch: Option[ColumnarBatch] = None

  override def hasNext: Boolean = {
    if (streamJoinIter.hasNext || finalBatch.isDefined) {
      true
    } else {
      finalBatch = getFinalBatch()
      // Now that we've manifested the final batch, we can close the stream iterator early to free
      // GPU resources.
      streamJoinIter.close()
      finalBatch.isDefined
    }
  }

  override def next(): ColumnarBatch = {
    if (!hasNext) {
      throw new NoSuchElementException("batches exhausted")
    }
    if (streamJoinIter.hasNext) {
      streamJoinIter.next()
    } else {
      val batch = finalBatch.get
      finalBatch = None
      batch
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      streamJoinIter.close()
      finalBatch.foreach(_.close())
      finalBatch = None
    }
  }

  private def getFinalBatch(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("get final batch", NvtxColor.ORANGE, joinTime)) { _ =>
      streamJoinIter.releaseBuiltSideTracker() match {
        case None => None
        case Some(tracker) =>
          val filteredBatch = withResource(tracker) { scb =>
            withResource(scb.getColumnarBatch()) { trackerBatch =>
              withResource(GpuColumnVector.from(trackerBatch)) { trackerTab =>
                val batch = built.getBatch
                withResource(GpuColumnVector.from(batch)) { builtTable =>
                  withResource(builtTable.filter(trackerTab.getColumn(0))) { filterTab =>
                    GpuColumnVector.from(filterTab, GpuColumnVector.extractTypes(batch))
                  }
                }
              }
            }
          }
          // Combine build-side columns with null columns for stream side
          withResource(filteredBatch) { builtBatch =>
            val numFilterRows = builtBatch.numRows()
            if (numFilterRows > 0) {
              val streamColumns = streamAttributes.safeMap { attr =>
                GpuColumnVector.fromNull(numFilterRows, attr.dataType)
              }
              withResource(new ColumnarBatch(streamColumns.toArray, numFilterRows)) { streamBatch =>
                buildSide match {
                  case GpuBuildRight =>
                    Some(GpuColumnVector.combineColumns(streamBatch, builtBatch))
                  case GpuBuildLeft =>
                    Some(GpuColumnVector.combineColumns(builtBatch, streamBatch))
                }
              }
            } else {
              None
            }
          }
      }
    }
  }
}

class HashedExistenceJoinIterator(
  spillableBuiltBatch: LazySpillableColumnarBatch,
  boundBuildKeys: Seq[GpuExpression],
  lazyStream: Iterator[LazySpillableColumnarBatch],
  boundStreamKeys: Seq[GpuExpression],
  boundCondition: Option[GpuExpression],
  numFirstConditionTableColumns: Int,
  compareNullsEqual: Boolean,
  opTime: GpuMetric,
  joinTime: GpuMetric
) extends ExistenceJoinIterator(spillableBuiltBatch, lazyStream, opTime, joinTime) {

  val compiledConditionRes: Option[CompiledExpression] = boundCondition.map { gpuExpr =>
    use(opTime.ns(gpuExpr.convertToAst(numFirstConditionTableColumns).compile()))
  }

  private def leftKeysTable(leftColumnarBatch: ColumnarBatch): Table = {
    withResource(GpuProjectExec.project(leftColumnarBatch, boundStreamKeys)) {
      GpuColumnVector.from(_)
    }
  }

  private def rightKeysTable(): Table = {
    withResource(GpuProjectExec.project(spillableBuiltBatch.getBatch, boundBuildKeys)) {
      GpuColumnVector.from(_)
    }
  }

  private def conditionalBatchLeftSemiJoin(
    leftColumnarBatch: ColumnarBatch,
    leftKeysTab: Table,
    rightKeysTab: Table,
    compiledCondition: CompiledExpression): GatherMap = {
    withResource(GpuColumnVector.from(leftColumnarBatch)) { leftTab =>
      withResource(GpuColumnVector.from(spillableBuiltBatch.getBatch)) { rightTab =>
        Table.mixedLeftSemiJoinGatherMap(
          leftKeysTab,
          rightKeysTab,
          leftTab,
          rightTab,
          compiledCondition,
          if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL)
        }
    }
  }

  private def unconditionalBatchLeftSemiJoin(
    leftKeysTab: Table,
    rightKeysTab: Table
  ): GatherMap = {
    leftKeysTab.leftSemiJoinGatherMap(rightKeysTab, compareNullsEqual)
  }

  override def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap = {
    withResource(
      new NvtxWithMetrics("existence join scatter map", NvtxColor.ORANGE, joinTime)
    ) { _ =>
      withResource(leftKeysTable(leftColumnarBatch)) { leftKeysTab =>
        withResource(rightKeysTable()) { rightKeysTab =>
          compiledConditionRes.map { compiledCondition =>
            conditionalBatchLeftSemiJoin(leftColumnarBatch, leftKeysTab, rightKeysTab,
              compiledCondition)
          }.getOrElse {
            unconditionalBatchLeftSemiJoin(leftKeysTab, rightKeysTab)
          }
        }
      }
    }
  }
}

trait GpuJoinExec extends ShimBinaryExecNode with GpuExec {
  def joinType: JoinType
  def condition: Option[Expression]
  def leftKeys: Seq[Expression]
  def rightKeys: Seq[Expression]
  def isSkewJoin: Boolean = false
}

trait GpuHashJoin extends GpuJoinExec {
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
    if (batching.isInstanceOf[RequireSingleBatchLike]) {
      RequireSingleBatch
    } else {
      null
    }
  }

  def dedupDataFromKeys(
      rightOutput: Seq[Attribute],
      rightKeys: Seq[Expression],
      leftKeys: Seq[Expression]): (Seq[Attribute], Seq[NamedExpression]) = {
    // This means that we need a mapping from what we remove on the right to what in leftData can
    // provide it. These are still symbolic references, so we are going to convert everything into
    // attributes, and use it to make out mapping.
    val leftKeyAttributes = GpuHashJoin.extractTopLevelAttributes(leftKeys, includeAlias = true)
    val rightKeyAttributes = GpuHashJoin.extractTopLevelAttributes(rightKeys, includeAlias = false)
    val zippedKeysMapping = rightKeyAttributes.zip(leftKeyAttributes)
    val rightToLeftKeyMap = zippedKeysMapping.filter {
      case (Some(_), Some(_: AttributeReference)) => true
      case _ => false
    }.map {
      case (Some(right), Some(left)) => (right.exprId, left)
      case _ => throw new IllegalStateException("INTERNAL ERROR THIS SHOULD NOT BE REACHABLE")
    }.toMap

    val rightData = rightOutput.filterNot(att => rightToLeftKeyMap.contains(att.exprId))
    val remappedRightOutput = rightOutput.map { att =>
      rightToLeftKeyMap.get(att.exprId)
          .map(leftAtt => GpuAlias(leftAtt, att.name)(att.exprId))
          .getOrElse(att)
    }
    (rightData, remappedRightOutput)
  }

  // For join types other than FullOuter, we simply set compareNullsEqual as true to adapt
  // struct keys with nullable children. Non-nested keys can also be correctly processed with
  // compareNullsEqual = true, because we filter all null records from build table before join.
  // For some details, please refer the issue: https://github.com/NVIDIA/spark-rapids/issues/2126
  protected lazy val compareNullsEqual: Boolean = (joinType != FullOuter) &&
      GpuHashJoin.anyNullableStructChild(buildKeys)

  protected lazy val (boundBuildKeys, boundStreamKeys) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)

    buildSide match {
      case GpuBuildLeft => (lkeys, rkeys)
      case GpuBuildRight => (rkeys, lkeys)
    }
  }

  protected lazy val (numFirstConditionTableColumns, boundCondition) = {
    val (buildOutput, streamOutput) = buildSide match {
      case GpuBuildRight => (right.output, left.output)
      case GpuBuildLeft => (left.output, right.output)
    }
    val boundCondition = condition.map { c =>
      GpuBindReferences.bindGpuReference(c, streamOutput ++ buildOutput)
    }
    (streamOutput.size, boundCondition)
  }

  def doJoin(
      builtBatch: ColumnarBatch,
      stream: Iterator[ColumnarBatch],
      targetSize: Long,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
    // Filtering nulls on the build side is a workaround for Struct joins with nullable children
    // see https://github.com/NVIDIA/spark-rapids/issues/2126 for more info
    val builtAnyNullable = compareNullsEqual && buildKeys.exists(_.nullable)

    val nullFiltered = if (builtAnyNullable) {
      val sb = closeOnExcept(builtBatch)(
        SpillableColumnarBatch(_, SpillPriorities.ACTIVE_ON_DECK_PRIORITY))
      GpuHashJoin.filterNullsWithRetryAndClose(sb, boundBuildKeys)
    } else {
      builtBatch
    }

    val spillableBuiltBatch = withResource(nullFiltered) {
      LazySpillableColumnarBatch(_, "built")
    }

    val lazyStream = stream.map { cb =>
      withResource(cb) { cb =>
        LazySpillableColumnarBatch(cb, "stream_batch")
      }
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator = joinType match {
      case ExistenceJoin(_) =>
        new HashedExistenceJoinIterator(
          spillableBuiltBatch,
          boundBuildKeys,
          lazyStream,
          boundStreamKeys,
          boundCondition,
          numFirstConditionTableColumns,
          compareNullsEqual,
          opTime,
          joinTime)
      case FullOuter =>
        new HashFullJoinIterator(spillableBuiltBatch, boundBuildKeys, None, lazyStream,
          boundStreamKeys, streamedPlan.output, boundCondition, numFirstConditionTableColumns,
          targetSize, buildSide, compareNullsEqual, opTime, joinTime)
      case _ =>
        if (boundCondition.isDefined) {
          // ConditionalHashJoinIterator will close the compiled condition
          val compiledCondition =
            boundCondition.get.convertToAst(numFirstConditionTableColumns).compile()
          new ConditionalHashJoinIterator(spillableBuiltBatch, boundBuildKeys, lazyStream,
            boundStreamKeys, streamedPlan.output, compiledCondition,
            targetSize, joinType, buildSide, compareNullsEqual, opTime, joinTime)
        } else {
          new HashJoinIterator(spillableBuiltBatch, boundBuildKeys, lazyStream, boundStreamKeys,
            streamedPlan.output, targetSize, joinType, buildSide, compareNullsEqual,
            opTime, joinTime)
        }
    }

    joinIterator.map { cb =>
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }
}
