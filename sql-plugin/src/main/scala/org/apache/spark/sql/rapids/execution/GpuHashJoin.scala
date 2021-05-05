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

import ai.rapids.cudf.{GatherMap, NvtxColor, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.TaskContext
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
}

object GpuHashJoin extends Arm {

  def tagJoin(
      meta: RapidsMeta[_, _, _],
      joinType: JoinType,
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression]): Unit = {
    val keyDataTypes = (leftKeys ++ rightKeys).map(_.dataType)
    if (keyDataTypes.exists(dType =>
      dType.isInstanceOf[ArrayType] || dType.isInstanceOf[MapType])) {
      meta.willNotWorkOnGpu("ArrayType or MapType in join keys are not supported")
    }

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
    inputBuiltKeys: ColumnarBatch,
    inputBuiltData: ColumnarBatch,
    private val stream: Iterator[ColumnarBatch],
    val boundStreamKeys: Seq[Expression],
    val boundStreamData: Seq[Expression],
    val streamAttributes: Seq[Attribute],
    val targetSize: Long,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    private val spillCallback: SpillCallback,
    private val streamTime: GpuMetric,
    private val joinTime: GpuMetric,
    private val totalTime: GpuMetric) extends Iterator[ColumnarBatch] with Arm {
  import scala.collection.JavaConverters._

  // For some join types even if there is no stream data we might output something
  private var initialJoin = true
  private var nextCb: Option[ColumnarBatch] = None
  private var gathererStore: Option[JoinGatherer] = None
  // Close the input keys, the lazy spillable batch now owns it.
  private val builtKeys = withResource(inputBuiltKeys) { inputBuiltKeys =>
    LazySpillableColumnarBatch(inputBuiltKeys, spillCallback, "build_keys")
  }
  // Close the input data, the lazy spillable batch now owns it.
  private val builtData = withResource(inputBuiltData) { inputBuiltData =>
    LazySpillableColumnarBatch(inputBuiltData, spillCallback, "build_data")
  }
  private var closed = false

  def close(): Unit = {
    if (!closed) {
      builtKeys.close()
      builtData.close()
      nextCb.foreach(_.close())
      nextCb = None
      gathererStore.foreach(_.close())
      gathererStore = None
      closed = true
    }
  }

  TaskContext.get().addTaskCompletionListener[Unit](_ => close())

  private def nextCbFromGatherer(): Option[ColumnarBatch] = {
    withResource(new NvtxWithMetrics("hash join gather", NvtxColor.DARK_GREEN, joinTime)) { _ =>
      val ret = gathererStore.map { gather =>
        val nextRows = JoinGatherer.getRowsInNextBatch(gather, targetSize)
        gather.gatherNext(nextRows)
      }
      if (gathererStore.exists(_.isDone)) {
        gathererStore.foreach(_.close())
        gathererStore = None
      }

      if (ret.isDefined) {
        // We are about to return something. We got everything we need from it so now let it spill
        // if there is more to be gathered later on.
        gathererStore.foreach(_.allowSpilling())
      }
      ret
    }
  }

  private def makeGatherer(
      maps: Array[GatherMap],
      leftData: LazySpillableColumnarBatch,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    assert(maps.length > 0 && maps.length <= 2)
    try {
      val leftMap = maps.head
      val rightMap = if (maps.length > 1) {
        if (rightData.numCols == 0) {
          // No data so don't bother with it
          None
        } else {
          Some(maps(1))
        }
      } else {
        None
      }

      val lazyLeftMap = LazySpillableGatherMap(leftMap, spillCallback, "left_map")
      val gatherer = rightMap match {
        case None =>
          rightData.close()
          JoinGatherer(lazyLeftMap, leftData)
        case Some(right) =>
          val lazyRightMap = LazySpillableGatherMap(right, spillCallback, "right_map")
          JoinGatherer(lazyLeftMap, leftData, lazyRightMap, rightData)
      }
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
      withResource(GpuProjectExec.project(streamCb, boundStreamData)) { streamData =>
        joinGatherer(buildKeys, LazySpillableColumnarBatch.spillOnly(buildData),
          streamKeys, LazySpillableColumnarBatch(streamData, spillCallback, "stream_data"))
      }
    }
  }

  override def hasNext: Boolean = {
    if (closed) {
      return false
    }
    var mayContinue = true
    while (nextCb.isEmpty && mayContinue) {
      val startTime = System.nanoTime()
      if (gathererStore.exists(!_.isDone)) {
        nextCb = nextCbFromGatherer()
      } else if (stream.hasNext) {
        // Need to refill the gatherer
        gathererStore.foreach(_.close())
        gathererStore = None
        withResource(stream.next()) { cb =>
          streamTime += (System.nanoTime() - startTime)
          gathererStore = joinGatherer(builtKeys.getBatch, builtData, cb)
        }
        nextCb = nextCbFromGatherer()
      } else if (initialJoin) {
        withResource(GpuColumnVector.emptyBatch(streamAttributes.asJava)) { cb =>
          gathererStore = joinGatherer(builtKeys.getBatch, builtData, cb)
        }
        nextCb = nextCbFromGatherer()
      } else {
        mayContinue = false
      }
      totalTime += (System.nanoTime() - startTime)
      initialJoin = false
    }
    if (nextCb.isEmpty) {
      // Nothing is left to return so close ASAP.
      close()
    } else {
      builtKeys.allowSpilling()
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

  /**
   * Spark does joins rather simply. They do it row by row, and as such don't really worry
   * about how much space is being taken up. We are doing this in batches, and have the option to
   * deduplicate columns that we know are the same to save even more memory.
   *
   * As such we do the join in a few different stages.
   *
   * 1. We separate out the join keys from the data that will be gathered. The join keys are used
   * to produce a gather map, and then can be released. The data needs to stay until it has been
   * gathered. Depending on the type of join and what is being done the join output is likely to
   * contain the join keys twice. We don't want to do this because it takes up too much memory
   * so we remove the keys from the data for one side of the join.
   *
   * 2. After this we will do the join. We can produce multiple batches from a single
   * pair of input batches. The output of this stage is called the intermediate output and is the
   * data columns from each side of the join smashed together.
   *
   * 3. In some cases there is a condition that filters out data from the join that should not be
   * included. In the CPU code the condition will operate on the intermediate output. In some cases
   * the condition may need to be rewritten to point to the deduplicated key column.
   *
   * 4. Finally we need to fix up the data to produce the correct output. This should be a simple
   * projection that puts the deduplicated keys back to where they need to be.
   */
  protected lazy val (leftData, rightData, intermediateOutput, finalProject) = {
    require(leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val (leftData, remappedLeftOutput, rightData, remappedRightOutput) = joinType match {
      case FullOuter | RightOuter | LeftOuter =>
        // We cannot dedupe anything here because we can get nulls in the key columns on
        // at least one side, so they do not match
        (left.output, left.output, right.output, right.output)
      case LeftSemi | LeftAnti =>
        // These only need the keys from the right hand side, in fact there should only be keys on
        // the right hand side, except if there is a condition, but we don't support conditions for
        // these joins, so it is OK
        (left.output, left.output, Seq.empty, Seq.empty)
      case _: InnerLike =>
        val (rightData, remappedRightData) = dedupDataFromKeys(right.output, rightKeys, leftKeys)
        (left.output, left.output, rightData, remappedRightData)
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }

    val intermediateOutput = leftData ++ rightData

    val finalProject: Seq[Expression] = joinType match {
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        remappedLeftOutput ++ remappedRightOutput
      case LeftExistence(_) =>
        remappedLeftOutput
      case x =>
        throw new IllegalArgumentException(s"GpuHashJoin should not take $x as the JoinType")
    }
    (leftData, rightData, intermediateOutput, finalProject)
  }

  protected lazy val (boundBuildKeys, boundBuildData,
      boundStreamKeys, boundStreamData,
      boundCondition, boundFinalProject) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output)
    val ldata = GpuBindReferences.bindGpuReferences(leftData, left.output)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output)
    val rdata = GpuBindReferences.bindGpuReferences(rightData, right.output)
    val boundCondition =
      condition.map(c => GpuBindReferences.bindGpuReference(c, intermediateOutput))
    val boundFinalProject = GpuBindReferences.bindGpuReferences(finalProject, intermediateOutput)

    buildSide match {
      case GpuBuildLeft => (lkeys, ldata, rkeys, rdata, boundCondition, boundFinalProject)
      case GpuBuildRight => (rkeys, rdata, lkeys, ldata, boundCondition, boundFinalProject)
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
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    val realTarget = Math.max(targetSize, 10 * 1024)

    val (builtKeys, builtData) = {
      // Filtering nulls on the build side is a workaround.
      // 1) For a performance issue in LeftSemi and LeftAnti joins
      // https://github.com/rapidsai/cudf/issues/7300
      // 2) As a work around to Struct joins with nullable children
      // see https://github.com/NVIDIA/spark-rapids/issues/2126 for more info
      val builtAnyNullable = (compareNullsEqual || joinType == LeftSemi || joinType == LeftAnti) &&
          buildKeys.exists(_.nullable)

      val cb = if (builtAnyNullable) {
        GpuHashJoin.filterNulls(builtBatch, boundBuildKeys)
      } else {
        GpuColumnVector.incRefCounts(builtBatch)
      }

      withResource(cb) { cb =>
        closeOnExcept(GpuProjectExec.project(cb, boundBuildKeys)) { builtKeys =>
          (builtKeys, GpuProjectExec.project(cb, boundBuildData))
        }
      }
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator =
      new HashJoinIterator(builtKeys, builtData, stream, boundStreamKeys, boundStreamData,
        streamedPlan.output, realTarget, joinType, buildSide, compareNullsEqual, spillCallback,
        streamTime, joinTime, totalTime)
    val boundFinal = boundFinalProject
    if (boundCondition.isDefined) {
      val condition = boundCondition.get
      joinIterator.flatMap { cb =>
        joinOutputRows += cb.numRows()
        withResource(
          GpuFilter(cb, condition, numOutputRows, numOutputBatches, filterTime)) { filtered =>
          if (filtered.numRows == 0) {
            // Not sure if there is a better way to work around this
            numOutputBatches.set(numOutputBatches.value - 1)
            None
          } else {
            Some(GpuProjectExec.project(filtered, boundFinal))
          }
        }
      }
    } else {
      joinIterator.map { cb =>
        withResource(cb) { cb =>
          joinOutputRows += cb.numRows()
          numOutputRows += cb.numRows()
          numOutputBatches += 1
          GpuProjectExec.project(cb, boundFinal)
        }
      }
    }
  }
}
