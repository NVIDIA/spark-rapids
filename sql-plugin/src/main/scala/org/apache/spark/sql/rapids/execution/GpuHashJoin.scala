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

import ai.rapids.cudf.{DType, GroupByAggregation, NullPolicy, NvtxColor, ReductionAggregation, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, FullOuter, Inner, InnerLike, JoinType, LeftAnti, LeftExistence, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.SparkPlan
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
      case _ => // not disabled
    }
  }

  val LEFT_KEYS = "leftKeys"
  val RIGHT_KEYS = "rightKeys"
  val CONDITION = "condition"

  private[this] val cudfSupportedKeyTypes =
    (TypeSig.commonCudfTypes + TypeSig.NULL + TypeSig.DECIMAL_64 + TypeSig.STRUCT).nested()
  private[this] val sparkSupportedJoinKeyTypes = TypeSig.all - TypeSig.MAP.nested()

  private[this] val joinRideAlongTypes =
    (cudfSupportedKeyTypes + TypeSig.ARRAY + TypeSig.MAP).nested()

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

object GpuHashJoin extends Arm {

  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)

    def unSupportNonEqualCondition(): Unit = if (condition.isDefined) {
      meta.willNotWorkOnGpu(s"$joinType joins currently do not support conditions")
    }
    def unSupportStructKeys(): Unit = if (keyDataTypes.exists(_.isInstanceOf[StructType])) {
      meta.willNotWorkOnGpu(s"$joinType joins currently do not support with struct keys")
    }
    JoinTypeChecks.tagForGpu(joinType, meta)
    joinType match {
      case _: InnerLike =>
      case RightOuter | LeftOuter | LeftSemi | LeftAnti =>
        unSupportNonEqualCondition()
      case FullOuter =>
        unSupportNonEqualCondition()
        // FullOuter join cannot support with struct keys as two issues below
        //  * https://github.com/NVIDIA/spark-rapids/issues/2126
        //  * https://github.com/rapidsai/cudf/issues/7947
        unSupportStructKeys()
      case _ =>
        meta.willNotWorkOnGpu(s"$joinType currently is not supported")
    }
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
  def filterNulls(cb: ColumnarBatch, boundKeys: Seq[Expression]): ColumnarBatch = {
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
    private val spillCallback: SpillCallback,
    private val streamTime: GpuMetric,
    private val joinTime: GpuMetric,
    private val totalTime: GpuMetric)
    extends SplittableJoinIterator(
      s"hash $joinType gather",
      stream,
      streamAttributes,
      built,
      targetSize,
      spillCallback,
      joinTime = joinTime,
      streamTime = streamTime,
      totalTime = totalTime) {
  // We can cache this because the build side is not changing
  private lazy val streamMagnificationFactor = joinType match {
    case _: InnerLike | LeftOuter | RightOuter =>
      withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
        guessStreamMagnificationFactor(builtKeys)
      }
    case _ =>
      // existence joins don't change size, and FullOuter cannot be split
      1.0
  }

  override def computeNumJoinRows(cb: ColumnarBatch): Long = {
    // TODO: Replace this estimate with exact join row counts using the corresponding cudf APIs
    //       being added in https://github.com/rapidsai/cudf/issues/9053.
    joinType match {
      case _: InnerLike | LeftOuter | RightOuter =>
        Math.ceil(cb.numRows() * streamMagnificationFactor).toLong
      case _ => cb.numRows()
    }
  }

  override def createGatherer(
      cb: ColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    try {
      withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
        joinGatherer(builtKeys, built, cb)
      }
    } catch {
      // This should work for all join types except for FullOuter. There should be no need
      // to do this for any of the existence joins because the output rows will never be
      // larger than the input rows on the stream side.
      case oom: OutOfMemoryError if joinType.isInstanceOf[InnerLike]
          || joinType == LeftOuter
          || joinType == RightOuter =>
        // Because this is just an estimate, it is possible for us to get this wrong, so
        // make sure we at least split the batch in half.
        val numBatches = Math.max(2, estimatedNumBatches(cb))

        // Split batch and return no gatherer so the outer loop will try again
        splitAndSave(cb, numBatches, Some(oom))
        None
    }
  }

  private def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("hash join gather map", NvtxColor.ORANGE, joinTime)) { _ =>
      val maps = joinType match {
        case LeftOuter => leftKeys.leftJoinGatherMaps(rightKeys, compareNullsEqual)
        case RightOuter =>
          // Reverse the output of the join, because we expect the right gather map to
          // always be on the right
          rightKeys.leftJoinGatherMaps(leftKeys, compareNullsEqual).reverse
        case _: InnerLike => leftKeys.innerJoinGatherMaps(rightKeys, compareNullsEqual)
        case LeftSemi => Array(leftKeys.leftSemiJoinGatherMap(rightKeys, compareNullsEqual))
        case LeftAnti => Array(leftKeys.leftAntiJoinGatherMap(rightKeys, compareNullsEqual))
        case FullOuter => leftKeys.fullJoinGatherMaps(rightKeys, compareNullsEqual)
        case _ =>
          throw new NotImplementedError(s"Joint Type ${joinType.getClass} is not currently" +
              s" supported")
      }
      makeGatherer(maps, leftData, rightData)
    }
  }

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
      streamCb: ColumnarBatch): Option[JoinGatherer] = {
    withResource(GpuProjectExec.project(streamCb, boundStreamKeys)) { streamKeys =>
      closeOnExcept(LazySpillableColumnarBatch(streamCb, spillCallback, "stream_data")) { sd =>
        joinGatherer(buildKeys, LazySpillableColumnarBatch.spillOnly(buildData), streamKeys, sd)
      }
    }
  }

  private def countGroups(keys: ColumnarBatch): Table = {
    withResource(GpuColumnVector.from(keys)) { keysTable =>
      keysTable.groupBy(0 until keysTable.getNumberOfColumns: _*)
          .aggregate(GroupByAggregation.count(NullPolicy.INCLUDE).onColumn(0))
    }
  }

  /**
   * Guess the magnification factor for a stream side batch.
   * This is temporary until cudf gives us APIs to get the actual gather map size.
   */
  private def guessStreamMagnificationFactor(builtKeys: ColumnarBatch): Double = {
    // Based off of the keys on the build side guess at how many output rows there
    // will be for each input row on the stream side. This does not take into account
    // the join type, data skew or even if the keys actually match.
    withResource(countGroups(builtKeys)) { builtCount =>
      val counts = builtCount.getColumn(builtCount.getNumberOfColumns - 1)
      withResource(counts.reduce(ReductionAggregation.mean(), DType.FLOAT64)) { scalarAverage =>
        scalarAverage.getDouble
      }
    }
  }

  private def estimatedNumBatches(cb: ColumnarBatch): Int = joinType match {
    case _: InnerLike | LeftOuter | RightOuter =>
      // We want the gather map size to be around the target size. There are two gather maps
      // that are made up of ints, so estimate how many rows per batch on the stream side
      // will produce the desired gather map size.
      val approximateStreamRowCount = ((targetSize.toDouble / 2) /
          DType.INT32.getSizeInBytes) / streamMagnificationFactor
      val estimatedRowsPerStreamBatch = Math.min(Int.MaxValue, approximateStreamRowCount)
      Math.ceil(cb.numRows() / estimatedRowsPerStreamBatch).toInt
    case _ => 1
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

  protected lazy val (boundBuildKeys, boundStreamKeys, boundCondition) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    val boundCondition =
      condition.map(c => GpuBindReferences.bindGpuReference(c, output))

    buildSide match {
      case GpuBuildLeft => (lkeys, rkeys, boundCondition)
      case GpuBuildRight => (rkeys, lkeys, boundCondition)
    }
  }

  def doJoin(
      builtBatch: ColumnarBatch,
      stream: Iterator[ColumnarBatch],
      targetSize: Long,
      spillCallback: SpillCallback,
      numOutputRows: GpuMetric,
      joinOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      streamTime: GpuMetric,
      joinTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    val realTarget = Math.max(targetSize, 10 * 1024)

    // Filtering nulls on the build side is a workaround for Struct joins with nullable children
    // see https://github.com/NVIDIA/spark-rapids/issues/2126 for more info
    val builtAnyNullable = compareNullsEqual && buildKeys.exists(_.nullable)

    val nullFiltered = if (builtAnyNullable) {
      GpuHashJoin.filterNulls(builtBatch, boundBuildKeys)
    } else {
      GpuColumnVector.incRefCounts(builtBatch)
    }

    val spillableBuiltBatch = withResource(nullFiltered) {
      LazySpillableColumnarBatch(_, spillCallback, "built")
    }

    val lazyStream = stream.map { cb =>
      withResource(cb) { cb =>
        LazySpillableColumnarBatch(cb, spillCallback, "stream_batch")
      }
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator =
      new HashJoinIterator(spillableBuiltBatch, boundBuildKeys, lazyStream, boundStreamKeys,
        streamedPlan.output, realTarget, joinType, buildSide, compareNullsEqual, spillCallback,
        streamTime, joinTime, totalTime)
    if (boundCondition.isDefined) {
      throw new IllegalStateException("Conditional joins are not supported on the GPU")
    } else {
      joinIterator.map { cb =>
        joinOutputRows += cb.numRows()
        numOutputRows += cb.numRows()
        numOutputBatches += 1
        cb
      }
    }
  }
}
