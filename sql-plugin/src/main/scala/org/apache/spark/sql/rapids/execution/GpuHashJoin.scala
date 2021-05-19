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

import scala.collection.mutable

import ai.rapids.cudf.{Aggregation, DType, GatherMap, NullPolicy, NvtxColor, Table}
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
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
    builtInput: ColumnarBatch,
    val boundBuiltKeys: Seq[Expression],
    private val stream: Iterator[ColumnarBatch],
    val boundStreamKeys: Seq[Expression],
    val streamAttributes: Seq[Attribute],
    val targetSize: Long,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    private val spillCallback: SpillCallback,
    private val streamTime: GpuMetric,
    private val joinTime: GpuMetric,
    private val totalTime: GpuMetric) extends Iterator[ColumnarBatch] with Arm with Logging {
  import scala.collection.JavaConverters._

  // For some join types even if there is no stream data we might output something
  private var initialJoin = true
  // If the join explodes this holds batches from the stream side split into smaller
  // pieces.
  private val pendingSplits = mutable.Queue[SpillableColumnarBatch]()
  private var nextCb: Option[ColumnarBatch] = None
  private var gathererStore: Option[JoinGatherer] = None
  // Close the input data, the lazy spillable batch now owns it.
  private val built = withResource(builtInput) { builtInput =>
    LazySpillableColumnarBatch(builtInput, spillCallback, "built")
  }

  // We can cache this because the build side is not changing
  private lazy val estimatedRowsPerStreamBatch = joinType match {
    case _: InnerLike | LeftOuter | RightOuter =>
      withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
        guessJoinRowsForTargetSize(builtKeys)
      }
    case _ =>
      // existence joins don't change size, and FullOuter cannot be split
      1.0
  }
  private var closed = false

  def close(): Unit = {
    if (!closed) {
      built.close()
      nextCb.foreach(_.close())
      nextCb = None
      gathererStore.foreach(_.close())
      gathererStore = None
      pendingSplits.foreach(_.close())
      pendingSplits.clear()
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
      closeOnExcept(LazySpillableColumnarBatch(streamCb, spillCallback, "stream_data")) { sd =>
        joinGatherer(buildKeys, LazySpillableColumnarBatch.spillOnly(buildData), streamKeys, sd)
      }
    }
  }

  private def countGroups(keys: ColumnarBatch): Table = {
    withResource(GpuColumnVector.from(keys)) { keysTable =>
      keysTable.groupBy(0 until keysTable.getNumberOfColumns: _*)
          .aggregate(Aggregation.count(NullPolicy.INCLUDE).onColumn(0))
    }
  }

  /**
   * Guess how large a stream side batch should be to avoid really huge gather maps.
   * This is temporary until cudf gives us APIs to get the actual gather map size.
   */
  private def guessJoinRowsForTargetSize(builtKeys: ColumnarBatch): Double = {
    // Based off of the keys on the build side guess at how many output rows there
    // will be for each input row on the stream side. This does not take into account
    // the join type, data skew or even if the keys actually match.
    val averageStreamSizeExpansion = withResource(countGroups(builtKeys)) { builtCount =>
      val counts = builtCount.getColumn(builtCount.getNumberOfColumns - 1)
      withResource(counts.reduce(Aggregation.mean(), DType.FLOAT64)) { scalarAverage =>
        scalarAverage.getDouble
      }
    }

    // We want the gather map size to be around the target size. There are two gather maps
    // that are made up of ints, so estimate how many rows per batch on the stream side
    // will produce the desired gather map size.
    val approximateStreamRowCount = ((targetSize.toDouble / 2) /
        DType.INT32.getSizeInBytes) / averageStreamSizeExpansion
    Math.min(Int.MaxValue, approximateStreamRowCount)
  }

  private def estimatedNumBatches(cb: ColumnarBatch): Int = joinType match {
    case _: InnerLike | LeftOuter | RightOuter =>
      Math.ceil(cb.numRows() / estimatedRowsPerStreamBatch).toInt
    case _ => 1
  }

  private def splitAndSave(cb: ColumnarBatch,
      numBatches: Int,
      oom: Option[OutOfMemoryError] = None): Unit = {
    val batchSize = cb.numRows() / numBatches
    if (oom.isDefined && batchSize < 100) {
      // We just need some kind of cutoff to not get stuck in a loop if the batches get to be too
      // small but we want to at least give it a chance to work (mostly for tests where the
      // targetSize can be set really small)
      throw oom.get
    }
    val msg = s"Split stream batch into $numBatches batches of about $batchSize rows"
    if (oom.isDefined) {
      logWarning(s"OOM Encountered: $msg")
    } else {
      logInfo(msg)
    }
    val splits = withResource(GpuColumnVector.from(cb)) { tab =>
      val splitIndexes = (1 until numBatches).map(num => num * batchSize)
      tab.contiguousSplit(splitIndexes: _*)
    }
    withResource(splits) { splits =>
      val schema = GpuColumnVector.extractTypes(cb)
      pendingSplits ++= splits.map { ct =>
        SpillableColumnarBatch(ct, schema,
          SpillPriorities.ACTIVE_ON_DECK_PRIORITY, spillCallback)
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
      } else if (pendingSplits.nonEmpty || stream.hasNext) {
        // Need to refill the gatherer
        gathererStore.foreach(_.close())
        gathererStore = None
        val cb = if (pendingSplits.isEmpty) {
          val cb = stream.next()
          val estimatedBatches = estimatedNumBatches(cb)
          // The cutoff is arbitrary, just to avoid doing duplicate work
          if (estimatedBatches > 2) {
            withResource(cb) { cb =>
              splitAndSave(cb, estimatedBatches)
            }
            withResource(pendingSplits.dequeue()) { scb =>
              scb.getColumnarBatch()
            }
          } else {
            cb
          }
        } else {
          withResource(pendingSplits.dequeue()) { scb =>
            scb.getColumnarBatch()
          }
        }
        withResource(cb) { cb =>
          streamTime += (System.nanoTime() - startTime)
          try {
            withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
              gathererStore = joinGatherer(builtKeys, built, cb)
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
              // Now try again nextCbFromGatherer will return None, but there will be more to do
              // so the loop will not finish
              splitAndSave(cb, numBatches, Some(oom))
          }
        }
        nextCb = nextCbFromGatherer()
      } else if (initialJoin) {
        withResource(GpuColumnVector.emptyBatch(streamAttributes.asJava)) { cb =>
          withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
            gathererStore = joinGatherer(builtKeys, built, cb)
          }
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
      filterTime: GpuMetric,
      totalTime: GpuMetric): Iterator[ColumnarBatch] = {
    // The 10k is mostly for tests, hopefully no one is setting anything that low in production.
    val realTarget = Math.max(targetSize, 10 * 1024)

    // Filtering nulls on the build side is a workaround.
    // 1) For a performance issue in LeftSemi and LeftAnti joins
    // https://github.com/rapidsai/cudf/issues/7300
    // 2) As a work around to Struct joins with nullable children
    // see https://github.com/NVIDIA/spark-rapids/issues/2126 for more info
    val builtAnyNullable = (compareNullsEqual || joinType == LeftSemi || joinType == LeftAnti) &&
        buildKeys.exists(_.nullable)

    val nullFiltered = if (builtAnyNullable) {
      GpuHashJoin.filterNulls(builtBatch, boundBuildKeys)
    } else {
      GpuColumnVector.incRefCounts(builtBatch)
    }

    // The HashJoinIterator takes ownership of the built keys and built data. It will close
    // them when it is done
    val joinIterator =
      new HashJoinIterator(nullFiltered, boundBuildKeys, stream, boundStreamKeys,
        streamedPlan.output, realTarget, joinType, buildSide, compareNullsEqual, spillCallback,
        streamTime, joinTime, totalTime)
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
            Some(GpuColumnVector.incRefCounts(filtered))
          }
        }
      }
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
