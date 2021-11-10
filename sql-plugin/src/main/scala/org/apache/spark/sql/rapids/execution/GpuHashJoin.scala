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

import ai.rapids.cudf.{HashJoin, NvtxColor, Table}
import com.nvidia.spark.rapids._

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
    private val opTime: GpuMetric,
    private val joinTime: GpuMetric)
    extends SplittableJoinIterator(
      s"hash $joinType gather",
      stream,
      streamAttributes,
      built,
      targetSize,
      spillCallback,
      opTime = opTime,
      joinTime = joinTime) {
  private var isHashTableFromStreamSide = false
  private var hashTable: Option[HashJoin] = None
  private var streamKeysTable: Option[Table] = None
  private var builtKeysTable: Option[Table] = None

  private def freeHashTable(): Unit = {
    hashTable.foreach(_.close())
    hashTable = None
    isHashTableFromStreamSide = false
  }

  override def close(): Unit = {
    super.close()
    freeIntermediateResources()
  }

  override def freeIntermediateResources(): Unit = {
    freeHashTable()
    streamKeysTable.foreach(_.close())
    streamKeysTable = None
    builtKeysTable.foreach(_.close())
    builtKeysTable = None
  }

  override def setupNextGatherer(): Option[JoinGatherer] = {
    // fetching the next stream batch invalidates any previous stream keys
    streamKeysTable.foreach(_.close())
    streamKeysTable = None
    if (isHashTableFromStreamSide) {
      freeHashTable()
    }
    super.setupNextGatherer()
  }

  override def splitAndSave(cb: ColumnarBatch, numBatches: Int): Unit = {
    // splitting the stream batch invalidates the stream keys
    streamKeysTable.foreach(_.close())
    streamKeysTable = None
    if (isHashTableFromStreamSide) {
      freeHashTable()
    }
    super.splitAndSave(cb, numBatches)
  }

  override def computeNumJoinRows(cb: ColumnarBatch): Long = {
    withResource(new NvtxWithMetrics("hash join build", NvtxColor.ORANGE, joinTime)) { _ =>
      joinType match {
        case LeftSemi | LeftAnti =>
          // Semi or Anti joins do not explode, worst-case they return the same number of rows.
          buildSide match {
            case GpuBuildRight => cb.numRows
            case GpuBuildLeft => built.getBatch.numRows
          }
        case FullOuter =>
          // For full joins, currently we only support the entire stream table at once. There's no
          // reason to predict the output rows since the stream batch is not splittable.
          // The cudf API to return a full outer join row count performs excess computation, so
          // just return an answer here that will be ignored later.
          cb.numRows.max(built.getBatch.numRows)
        case _: InnerLike if built.getBatch.numRows == 0 || cb.numRows == 0 =>
          0
        case _: InnerLike if built.getBatch.numRows > cb.numRows =>
          // Inner joins can be reversed, so use the smaller stream-side batch as the hash table
          // as building the hash table is an expensive part of performing the join.
          freeHashTable()
          assert(streamKeysTable.isEmpty, "stream keys table already exists")
          val streamKeys = maybeProjectStreamKeys(cb)
          val hashJoin = new HashJoin(streamKeys, compareNullsEqual)
          hashTable = Some(hashJoin)
          isHashTableFromStreamSide = true
          val builtKeys = maybeProjectBuiltKeys()
          builtKeys.innerJoinRowCount(hashJoin)
        case _ =>
          val hashTable = maybeBuildHashTable()
          assert(streamKeysTable.isEmpty, "stream keys table already exists")
          val streamKeys = maybeProjectStreamKeys(cb)
          joinType match {
            case _: InnerLike =>
              streamKeys.innerJoinRowCount(hashTable)
            case LeftOuter =>
              assert(buildSide == GpuBuildRight, s"$joinType with $buildSide")
              streamKeys.leftJoinRowCount(hashTable)
            case RightOuter =>
              assert(buildSide == GpuBuildLeft, s"$joinType with $buildSide")
              streamKeys.leftJoinRowCount(hashTable)
            case _ =>
              throw new IllegalStateException(s"unexpected join type: $joinType")
          }
      }
    }
  }

  override def createGatherer(
      cb: ColumnarBatch,
      numJoinRows: Option[Long]): Option[JoinGatherer] = {
    withResource(new NvtxWithMetrics("hash join gather maps", NvtxColor.ORANGE, joinTime)) { _ =>
      val maps = joinType match {
        case FullOuter | LeftSemi | LeftAnti =>
          assert(hashTable.isEmpty, s"$joinType but somehow precomputed a hash table")
          assert(streamKeysTable.isEmpty, s"$joinType but somehow already projected stream keys")
          withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
            withResource(GpuColumnVector.from(builtKeys)) { builtTable =>
              withResource(GpuProjectExec.project(cb, boundStreamKeys)) { streamKeys =>
                withResource(GpuColumnVector.from(streamKeys)) { streamTable =>
                  joinType match {
                    case FullOuter =>
                      val maps = streamTable.fullJoinGatherMaps(builtTable, compareNullsEqual)
                      if (buildSide == GpuBuildLeft) maps.reverse else maps
                    case LeftSemi =>
                      assert(buildSide == GpuBuildRight, s"$joinType with $buildSide")
                      Array(streamTable.leftSemiJoinGatherMap(builtTable, compareNullsEqual))
                    case LeftAnti =>
                      assert(buildSide == GpuBuildRight, s"$joinType with $buildSide")
                      Array(streamTable.leftAntiJoinGatherMap(builtTable, compareNullsEqual))
                    case _ =>
                      throw new IllegalStateException(s"Unexpected join type $joinType")
                  }
                }
              }
            }
          }
        case _: InnerLike if isHashTableFromStreamSide =>
          assert(hashTable.isDefined, "missing hash table from stream side")
          val builtKeys = maybeProjectBuiltKeys()
          val maps = numJoinRows.map(builtKeys.innerJoinGatherMaps(hashTable.get, _))
              .getOrElse(builtKeys.innerJoinGatherMaps(hashTable.get))
          if (buildSide == GpuBuildRight) maps.reverse else maps
        case _ =>
          val hashTable = maybeBuildHashTable()
          val streamKeys = maybeProjectStreamKeys(cb)
          joinType match {
            case _: InnerLike =>
              val maps = numJoinRows.map(streamKeys.innerJoinGatherMaps(hashTable, _))
                  .getOrElse(streamKeys.innerJoinGatherMaps(hashTable))
              if (buildSide == GpuBuildLeft) maps.reverse else maps
            case LeftOuter =>
              assert(buildSide == GpuBuildRight, s"$joinType with $buildSide")
              numJoinRows.map(streamKeys.leftJoinGatherMaps(hashTable, _))
                  .getOrElse(streamKeys.leftJoinGatherMaps(hashTable))
            case RightOuter =>
              assert(buildSide == GpuBuildLeft, s"$joinType with $buildSide")
              // Reverse the output of the join, because we expect the right gather map to
              // always be on the right
              val maps = numJoinRows.map(streamKeys.leftJoinGatherMaps(hashTable, _))
                  .getOrElse(streamKeys.leftJoinGatherMaps(hashTable))
              maps.reverse
            case _ =>
              throw new IllegalStateException(s"Unexpected join type: $joinType")
          }
      }
      closeOnExcept(LazySpillableColumnarBatch(cb, spillCallback, "stream_data")) { streamData =>
        val buildData = LazySpillableColumnarBatch.spillOnly(built)
        val (left, right) = buildSide match {
          case GpuBuildLeft => (buildData, streamData)
          case GpuBuildRight => (streamData, buildData)
        }
        makeGatherer(maps, left, right, joinType)
      }
    }
  }

  private def maybeBuildHashTable(): HashJoin = {
    hashTable.getOrElse {
      withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { builtKeys =>
        withResource(GpuColumnVector.from(builtKeys)) { keysTable =>
          val hashJoin = new HashJoin(keysTable, compareNullsEqual)
          hashTable = Some(hashJoin)
          isHashTableFromStreamSide = false
          hashJoin
        }
      }
    }
  }

  private def maybeProjectStreamKeys(cb: ColumnarBatch): Table = {
    streamKeysTable.getOrElse {
      withResource(GpuProjectExec.project(cb, boundStreamKeys)) { streamKeysBatch =>
        val table = GpuColumnVector.from(streamKeysBatch)
        streamKeysTable = Some(table)
        table
      }
    }
  }

  private def maybeProjectBuiltKeys(): Table = {
    builtKeysTable.getOrElse {
      withResource(GpuProjectExec.project(built.getBatch, boundBuiltKeys)) { buildKeysBatch =>
        val table = GpuColumnVector.from(buildKeysBatch)
        builtKeysTable = Some(table)
        table
      }
    }
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
      opTime: GpuMetric,
      joinTime: GpuMetric): Iterator[ColumnarBatch] = {
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
        opTime, joinTime)
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
