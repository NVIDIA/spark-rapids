/*
 * Copyright (c) 2020-2026, NVIDIA CORPORATION. All rights reserved.
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

import ai.rapids.cudf.{ColumnView, DeviceMemoryBuffer, DType, GatherMap, KeyRemapping, NullEquality, OutOfBoundsPolicy, Scalar, Table}
import ai.rapids.cudf.ast.CompiledExpression
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.{withRestoreOnRetry, withRetryNoSplit}
import com.nvidia.spark.rapids.jni.{GpuOOM, JoinPrimitives}
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

object GatherMapsResult {
  def apply(left: GatherMap, right: GatherMap): GatherMapsResult =
    new GatherMapsResult(Some(left), Some(right))

  def makeFromLeft(left: GatherMap): GatherMapsResult =
    new GatherMapsResult(Some(left), None)

  def makeFromRight(right: GatherMap): GatherMapsResult =
    new GatherMapsResult(None, Some(right))

  /**
   * Create an identity gather map for the given number of rows.
   * An identity gather map maps each row to itself: [0, 1, 2, ..., numRows-1].
   * This is used for distinct outer joins where one side's rows stay in place.
   *
   * @param numRows the number of rows in the identity map
   * @return a GatherMap representing the identity mapping
   */
  def identityGatherMap(numRows: Long): GatherMap = {
    if (numRows == 0) {
      // Empty gather map - just allocate an empty buffer
      new GatherMap(DeviceMemoryBuffer.allocate(0))
    } else {
      // Create a sequence column [0, 1, 2, ..., numRows-1] as INT32
      withResource(GpuScalar.from(0, IntegerType)) { startScalar =>
        withResource(GpuScalar.from(1, IntegerType)) { stepScalar =>
          withResource(ai.rapids.cudf.ColumnVector.sequence(
            startScalar, stepScalar, numRows.toInt)) { seqCol =>
            // Get the data buffer from the column. getData() returns BaseDeviceMemoryBuffer
            // but GatherMap requires DeviceMemoryBuffer, so we use sliceWithCopy to get
            // a proper DeviceMemoryBuffer that owns its memory.
            // We don't have a way to release the buffers inside of the ColumnVector
            // to make this a zero copy operation.
            withResource(seqCol.getData) { baseBuffer =>
              val buffer = baseBuffer.sliceWithCopy(0, baseBuffer.getLength)
              new GatherMap(buffer)
            }
          }
        }
      }
    }
  }
}

/**
 * Holds the result of a left and right gather map
 */
class GatherMapsResult(private var leftOpt: Option[GatherMap],
                       private var rightOpt: Option[GatherMap]) extends AutoCloseable {

  def left: GatherMap = leftOpt.get
  def hasLeft: Boolean = leftOpt.isDefined
  def releaseLeft(): GatherMap = {
    val ret = leftOpt.get
    leftOpt = None
    ret
  }

  def right: GatherMap = rightOpt.get
  def hasRight: Boolean = rightOpt.isDefined
  def releaseRight(): GatherMap = {
    val ret = rightOpt.get
    rightOpt = None
    ret
  }

  override def close(): Unit = {
    leftOpt.foreach(_.close)
    rightOpt.foreach(_.close)
  }
}

/**
 * Context for executing a join in JoinImpl.
 * Bundles all the parameters needed for join execution to reduce method signature complexity.
 *
 * @param joinType the type of join to execute (Inner, LeftOuter, RightOuter, LeftSemi, LeftAnti)
 * @param leftKeys the left side join keys as a Table
 * @param rightKeys the right side join keys as a Table
 * @param leftRowCount row count of the left side (cached for efficiency)
 * @param rightRowCount row count of the right side (cached for efficiency)
 * @param isDistinct whether the build side keys are distinct (enables distinct join optimization).
 *                   This refers specifically to the side indicated by `effectiveBuildSide`.
 * @param compareNullsEqual whether nulls should be compared as equal
 * @param options join options including strategy and build side selection
 * @param effectiveBuildSide the resolved build side for the join. This is the side for which
 *                           `isDistinct` was computed and determines the physical build side.
 * @param condition optional lazy compiled condition for conditional joins
 * @param leftTable optional left data table (needed for conditional joins)
 * @param rightTable optional right data table (needed for conditional joins)
 * @param metrics join metrics for tracking execution statistics
 * @param effectiveStrategy the resolved join strategy to use (AUTO resolved to concrete strategy)
 * @param sortJoinSupported whether sort join is supported for the key types
 */
case class JoinContext(
    joinType: JoinType,
    leftKeys: Table,
    rightKeys: Table,
    leftRowCount: Long,
    rightRowCount: Long,
    isDistinct: Boolean,
    compareNullsEqual: Boolean,
    options: JoinOptions,
    effectiveBuildSide: GpuBuildSide,
    condition: Option[LazyCompiledCondition],
    leftTable: Option[Table],
    rightTable: Option[Table],
    metrics: JoinMetrics,
    effectiveStrategy: JoinStrategy.JoinStrategy = JoinStrategy.HASH_ONLY,
    sortJoinSupported: Boolean = true,
    useKeyRemapping: Boolean = false) {

  def hasCondition: Boolean = condition.isDefined
  def nullEquality: NullEquality =
    if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL

  /**
   * Returns the strategy to use for inner join execution.
   * Falls back from sort to hash if sort join is not supported.
   */
  def resolvedInnerStrategy: JoinStrategy.JoinStrategy = {
    effectiveStrategy match {
      case JoinStrategy.INNER_SORT_WITH_POST if !sortJoinSupported =>
        JoinStrategy.INNER_HASH_WITH_POST
      case s => s
    }
  }
}

object JoinContext {
  /**
   * Create a JoinContext for unconditional joins (no AST condition).
   */
  def forUnconditionalJoin(
      joinType: JoinType,
      leftKeys: Table,
      rightKeys: Table,
      isDistinct: Boolean,
      compareNullsEqual: Boolean,
      options: JoinOptions,
      effectiveBuildSide: GpuBuildSide,
      metrics: JoinMetrics,
      effectiveStrategy: JoinStrategy.JoinStrategy = JoinStrategy.HASH_ONLY,
      sortJoinSupported: Boolean = true,
      useKeyRemapping: Boolean = false): JoinContext = {
    JoinContext(
      joinType = joinType,
      leftKeys = leftKeys,
      rightKeys = rightKeys,
      leftRowCount = leftKeys.getRowCount,
      rightRowCount = rightKeys.getRowCount,
      isDistinct = isDistinct,
      compareNullsEqual = compareNullsEqual,
      options = options,
      effectiveBuildSide = effectiveBuildSide,
      condition = None,
      leftTable = None,
      rightTable = None,
      metrics = metrics,
      effectiveStrategy = effectiveStrategy,
      sortJoinSupported = sortJoinSupported,
      useKeyRemapping = useKeyRemapping)
  }

  /**
   * Create a JoinContext for conditional joins (with AST condition).
   */
  def forConditionalJoin(
      joinType: JoinType,
      leftKeys: Table,
      rightKeys: Table,
      leftTable: Table,
      rightTable: Table,
      isDistinct: Boolean,
      compareNullsEqual: Boolean,
      options: JoinOptions,
      effectiveBuildSide: GpuBuildSide,
      condition: LazyCompiledCondition,
      metrics: JoinMetrics,
      effectiveStrategy: JoinStrategy.JoinStrategy = JoinStrategy.HASH_ONLY,
      sortJoinSupported: Boolean = true,
      useKeyRemapping: Boolean = false): JoinContext = {
    JoinContext(
      joinType = joinType,
      leftKeys = leftKeys,
      rightKeys = rightKeys,
      leftRowCount = leftKeys.getRowCount,
      rightRowCount = rightKeys.getRowCount,
      isDistinct = isDistinct,
      compareNullsEqual = compareNullsEqual,
      options = options,
      effectiveBuildSide = effectiveBuildSide,
      condition = Some(condition),
      leftTable = Some(leftTable),
      rightTable = Some(rightTable),
      metrics = metrics,
      effectiveStrategy = effectiveStrategy,
      sortJoinSupported = sortJoinSupported,
      useKeyRemapping = useKeyRemapping)
  }
}

object JoinImpl {
  /**
   * Filter the results of an inner join using a lazy compiled AST condition.
   * Always uses left/right table ordering for the AST.
   *
   * @param inner the results of an inner join
   * @param leftTable the left table columns used by the AST
   * @param rightTable the right table columns used by the AST
   * @param lazyCondition lazy compiled condition for both build sides
   * @return the gather maps filtered by the AST expression
   */
  def filterInnerJoinWithLazyCondition(inner: GatherMapsResult,
                                       leftTable: Table,
                                       rightTable: Table,
                                       lazyCondition: LazyCompiledCondition): GatherMapsResult = {
    val compiledCondition = lazyCondition.getForBuildRight
    val arrayRet = JoinPrimitives.filterGatherMapsByAST(
      inner.left, inner.right, leftTable, rightTable, compiledCondition)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Convert the results of an inner join to a left outer join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @param rightRowCount the number of rows in the right hand table
   * @return a gather maps result for a left outer join
   */
  def makeLeftOuter(inner: GatherMapsResult,
                    leftRowCount: Int, rightRowCount: Int): GatherMapsResult = {
    val arrayRet = JoinPrimitives.makeLeftOuter(
      inner.left, inner.right, leftRowCount, rightRowCount)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Convert the results of an inner join to a right outer join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @param rightRowCount the number of rows in the right hand table
   * @return a gather maps result for a right outer join
   */
  def makeRightOuter(inner: GatherMapsResult,
                     leftRowCount: Int, rightRowCount: Int): GatherMapsResult = {
    // Switching left and right to allow us to use makeLeftOuter as a stand in for
    // makeRightOuter
    val arrayRet = JoinPrimitives.makeLeftOuter(
      inner.right, inner.left, rightRowCount, leftRowCount)
    // Then switch the results back so we are good
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Convert the results of an inner join to a left semi join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @return a gather maps result for a left semi join
   */
  def makeLeftSemi(inner: GatherMapsResult, leftRowCount: Int): GatherMapsResult = {
    val leftRet = JoinPrimitives.makeSemi(inner.left, leftRowCount)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Convert the results of an inner join to a left anti join
   * @param inner the inner join results
   * @param leftRowCount the number of rows in the left hand table
   * @return a gather maps result for a left anti join
   */
  def makeLeftAnti(inner: GatherMapsResult, leftRowCount: Int): GatherMapsResult = {
    val leftRet = JoinPrimitives.makeAnti(inner.left, leftRowCount)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do an inner hash join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                             compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table, so switch the tables passed in
    val arrayRet = JoinPrimitives.hashInnerJoin(rightKeys, leftKeys, compareNullsEqual)
    // Then switch the gather maps result
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do an inner hash join with the build table as the left table. Note that the
   * AST complied expression must also be compiled for right table then left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def innerHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              compiledCondition: CompiledExpression,
                              nullEquality: NullEquality): GatherMapsResult = {
    // Even though it's an inner join, we need to switch the join order since the
    // condition has been compiled to expect the build side on the left and the stream
    // side on the right.
    val arrayRet = Table.mixedInnerJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
      compiledCondition, nullEquality)
    // Reverse the output of the join, because we expect the right gather map to
    // always be on the right.
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  def innerHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              lazyCondition: LazyCompiledCondition,
                              nullEquality: NullEquality): GatherMapsResult = {
    innerHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildLeft, nullEquality)
  }

  /**
   * Do an inner hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table
    val arrayRet = JoinPrimitives.hashInnerJoin(leftKeys, rightKeys, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do an inner hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def innerHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              compiledCondition: CompiledExpression,
                              nullEquality: NullEquality): GatherMapsResult = {
    val arrayRet = Table.mixedInnerJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  def innerHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              leftTable: Table, rightTable: Table,
                              lazyCondition: LazyCompiledCondition,
                              nullEquality: NullEquality): GatherMapsResult = {
    innerHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildRight, nullEquality)
  }

  /**
   * Do an inner hash join with build side selection based on the config.
   *
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param suggestedBuildSide the suggested build side from the query plan. This is only used
   *                           directly when buildSideSelection is FIXED. For AUTO or SMALLEST
   *                           strategies, this serves as a hint but the actual physical build side
   *                           will be determined dynamically based on row counts. The returned
   *                           gather maps maintain consistent left/right semantics regardless of
   *                           which physical build side is chosen.
   * @return the gather maps to use
   */
  def innerHashJoin(leftKeys: Table, rightKeys: Table,
                    compareNullsEqual: Boolean,
                    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
                    suggestedBuildSide: GpuBuildSide): GatherMapsResult = {
    // Select the physical build side for the join algorithm based on the strategy.
    // This may differ from suggestedBuildSide when using AUTO or SMALLEST strategies.
    val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
      buildSideSelection, suggestedBuildSide, leftKeys.getRowCount, rightKeys.getRowCount)
    selectedBuildSide match {
      case GpuBuildLeft => innerHashJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case GpuBuildRight => innerHashJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
    }
  }

  /**
   * Do an inner sort join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerSortJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                             compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table, so switch the tables passed in
    val arrayRet = JoinPrimitives.sortMergeInnerJoin(rightKeys, leftKeys,
      false, false, compareNullsEqual)
    // Then switch the gather maps result
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do an inner sort join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def innerSortJoinBuildRight(leftKeys: Table, rightKeys: Table,
                              compareNullsEqual: Boolean): GatherMapsResult = {
    // The build table is always the right table
    val arrayRet = JoinPrimitives.sortMergeInnerJoin(leftKeys, rightKeys,
      false, false, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do an inner sort join with build side selection based on the config.
   *
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param suggestedBuildSide the suggested build side from the query plan. This is only used
   *                           directly when buildSideSelection is FIXED. For AUTO or SMALLEST
   *                           strategies, this serves as a hint but the actual physical build side
   *                           will be determined dynamically based on row counts. The returned
   *                           gather maps maintain consistent left/right semantics regardless of
   *                           which physical build side is chosen.
   * @return the gather maps to use
   */
  def innerSortJoin(leftKeys: Table, rightKeys: Table,
                    compareNullsEqual: Boolean,
                    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
                    suggestedBuildSide: GpuBuildSide): GatherMapsResult = {
    // Select the physical build side for the join algorithm based on the strategy.
    // This may differ from suggestedBuildSide when using AUTO or SMALLEST strategies.
    val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
      buildSideSelection, suggestedBuildSide, leftKeys.getRowCount, rightKeys.getRowCount)
    selectedBuildSide match {
      case GpuBuildLeft => innerSortJoinBuildLeft(leftKeys, rightKeys, compareNullsEqual)
      case GpuBuildRight => innerSortJoinBuildRight(leftKeys, rightKeys, compareNullsEqual)
    }
  }

  /**
   * Do an inner sort join with key remapping and build side selection.
   * Uses KeyRemapping to transform multi-column or complex keys into single INT32 remapped keys,
   * then performs sort-merge join on the remapped keys.
   *
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param suggestedBuildSide the suggested build side from the query plan
   * @param keyRemapTime metric to track time spent in key remapping
   * @return the gather maps to use
   */
  def innerSortJoinWithRemapping(
      leftKeys: Table,
      rightKeys: Table,
      compareNullsEqual: Boolean,
      buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
      suggestedBuildSide: GpuBuildSide,
      keyRemapTime: GpuMetric): GatherMapsResult = {
    // Select the physical build side for the join algorithm based on the strategy.
    val selectedBuildSide = JoinBuildSideSelection.selectPhysicalBuildSide(
      buildSideSelection, suggestedBuildSide, leftKeys.getRowCount, rightKeys.getRowCount)
    
    innerSortJoinWithRemapping(leftKeys, rightKeys, compareNullsEqual, 
      selectedBuildSide, keyRemapTime)
  }

  /**
   * Do an inner sort join with key remapping.
   * 
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @param buildSide which side (left or right) is the build side
   * @param keyRemapTime metric to track time spent in key remapping
   * @return the gather maps to use
   */
  private def innerSortJoinWithRemapping(
      leftKeys: Table,
      rightKeys: Table,
      compareNullsEqual: Boolean,
      buildSide: GpuBuildSide,
      keyRemapTime: GpuMetric): GatherMapsResult = {
    val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
    
    // Determine build and probe tables based on build side
    val (buildKeys, probeKeys, swapJoinOrder) = buildSide match {
      case GpuBuildLeft => (leftKeys, rightKeys, false)
      case GpuBuildRight => (rightKeys, leftKeys, true)
    }
    
    // Time only the key remapping operations (not the join itself)
    val (remappedBuildTable, remappedProbeTable) = keyRemapTime.ns {
      remapKeysForJoin(buildKeys, probeKeys, nullEquality)
    }
    
    // Perform sort-merge join on remapped INT32 keys (not timed under keyRemapTime)
    // Call sortMergeInnerJoin with argument order based on build side
    val arrayRet = if (swapJoinOrder) {
      // Build is right: pass (probe, build) order to sortMergeInnerJoin
      withResource(remappedProbeTable) { probe =>
        withResource(remappedBuildTable) { build =>
          JoinPrimitives.sortMergeInnerJoin(probe, build,
            false, false, compareNullsEqual)
        }
      }
    } else {
      // Build is left: pass (build, probe) order to sortMergeInnerJoin
      withResource(remappedBuildTable) { build =>
        withResource(remappedProbeTable) { probe =>
          JoinPrimitives.sortMergeInnerJoin(build, probe,
            false, false, compareNullsEqual)
        }
      }
    }
    
    // Return gather maps in the same order as sortMergeInnerJoin returned them
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Remap join keys for sort-merge join using KeyRemapping.
   * This transforms multi-column or complex keys into single INT32 remapped keys.
   * 
   * @param buildKeys the build side keys table
   * @param probeKeys the probe side keys table
   * @param nullEquality null equality setting
   * @return tuple of (remapped build table, remapped probe table)
   */
  private def remapKeysForJoin(
      buildKeys: Table,
      probeKeys: Table,
      nullEquality: NullEquality): (Table, Table) = {
    // KeyRemapping metrics (distinctCount, maxDuplicateKeyCount) are not needed for the
    // remapping operation itself, so we pass computeMetrics=false to avoid the overhead.
    // If these metrics were needed for join strategy selection, they would have been
    // computed earlier during planning. They don't need to be recomputed here.
    withResource(new KeyRemapping(buildKeys, nullEquality, false)) { keyRemap =>
      val remappedBuild = withResource(keyRemap.remapBuildKeys()) { remappedCol =>
        new Table(remappedCol)
      }
      closeOnExcept(remappedBuild) { _ =>
        val remappedProbe = withResource(keyRemap.remapProbeKeys(probeKeys)) { remappedCol =>
          new Table(remappedCol)
        }
        (remappedBuild, remappedProbe)
      }
    }
  }

  /**
   * Do a left outer hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def leftOuterHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    val arrayRet = leftKeys.leftJoinGatherMaps(rightKeys, compareNullsEqual)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  /**
   * Do a left outer hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def leftOuterHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  compiledCondition: CompiledExpression,
                                  nullEquality: NullEquality): GatherMapsResult = {
    val arrayRet = Table.mixedLeftJoinGatherMaps(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(0), arrayRet(1))
  }

  def leftOuterHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  lazyCondition: LazyCompiledCondition,
                                  nullEquality: NullEquality): GatherMapsResult = {
    leftOuterHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildRight, nullEquality)
  }

  /**
   * Do a right outer hash join with the build table as the left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather maps to use
   */
  def rightOuterHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    // Reverse the input to the join so we can use left outer to computer right outer
    val arrayRet = rightKeys.leftJoinGatherMaps(leftKeys, compareNullsEqual)
    // Then switch the output so the gather maps are correct
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  /**
   * Do a right outer hash join with the build table as the left table. Note that the
   * AST complied expression must also be compiled for right table then left table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather maps to use
   */
  def rightOuterHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  compiledCondition: CompiledExpression,
                                  nullEquality: NullEquality): GatherMapsResult = {
    // Reverse the output of the join, because we expect the right gather map to
    // always be on the right
    val arrayRet = Table.mixedLeftJoinGatherMaps(rightKeys, leftKeys, rightTable, leftTable,
      compiledCondition, nullEquality)
    GatherMapsResult(arrayRet(1), arrayRet(0))
  }

  def rightOuterHashJoinBuildLeft(leftKeys: Table, rightKeys: Table,
                                  leftTable: Table, rightTable: Table,
                                  lazyCondition: LazyCompiledCondition,
                                  nullEquality: NullEquality): GatherMapsResult = {
    rightOuterHashJoinBuildLeft(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildLeft, nullEquality)
  }

  /**
   * Do a left semi hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather map to use for the left table
   */
  def leftSemiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                  compareNullsEqual: Boolean): GatherMapsResult = {
    val leftRet = leftKeys.leftSemiJoinGatherMap(rightKeys, compareNullsEqual)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do a left semi hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather map to use for the left table
   */
  def leftSemiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 compiledCondition: CompiledExpression,
                                 nullEquality: NullEquality): GatherMapsResult = {
    val leftRet = Table.mixedLeftSemiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  def leftSemiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 lazyCondition: LazyCompiledCondition,
                                 nullEquality: NullEquality): GatherMapsResult = {
    leftSemiHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildRight, nullEquality)
  }

  /**
   * Do a left anti hash join with the build table as the right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param compareNullsEqual true if nulls should be compared as equal
   * @return the gather map to use for the left table
   */
  def leftAntiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 compareNullsEqual: Boolean): GatherMapsResult = {
    val leftRet = leftKeys.leftAntiJoinGatherMap(rightKeys, compareNullsEqual)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  /**
   * Do a left anti hash join with the build table as the right table. Note that the
   * AST complied expression must also be compiled for left table then right table.
   * @param leftKeys the left equality join keys
   * @param rightKeys the right equality join keys
   * @param leftTable the left AST related columns
   * @param rightTable the right AST related columns
   * @param compiledCondition the condition
   * @param nullEquality if nulls should eb equal or not
   * @return the gather map to use for the left table
   */
  def leftAntiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 compiledCondition: CompiledExpression,
                                 nullEquality: NullEquality): GatherMapsResult = {
    val leftRet = Table.mixedLeftAntiJoinGatherMap(leftKeys, rightKeys, leftTable, rightTable,
      compiledCondition, nullEquality)
    GatherMapsResult.makeFromLeft(leftRet)
  }

  def leftAntiHashJoinBuildRight(leftKeys: Table, rightKeys: Table,
                                 leftTable: Table, rightTable: Table,
                                 lazyCondition: LazyCompiledCondition,
                                 nullEquality: NullEquality): GatherMapsResult = {
    leftAntiHashJoinBuildRight(leftKeys, rightKeys, leftTable, rightTable,
      lazyCondition.getForBuildRight, nullEquality)
  }

  // ============================================================================
  // HIGH-LEVEL JOIN EXECUTION API
  // These methods provide a unified interface for join execution, dispatching
  // by join type first, then selecting the optimal strategy within each type.
  // ============================================================================

  /**
   * Execute a join and return gather maps.
   * This is the main entry point that dispatches by join type, then selects
   * the optimal strategy (distinct, hash, sort) within each type.
   *
   * @param ctx the join context containing all parameters
   * @return gather maps for the join result
   */
  def executeJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.joinType match {
      case _: InnerLike =>
        executeInnerJoin(ctx)
      case LeftOuter =>
        executeLeftOuterJoin(ctx)
      case RightOuter =>
        executeRightOuterJoin(ctx)
      case LeftSemi =>
        executeLeftSemiJoin(ctx)
      case LeftAnti =>
        executeLeftAntiJoin(ctx)
      case _ =>
        throw new NotImplementedError(s"Join type ${ctx.joinType} is not currently supported")
    }
  }

  // ============================================================================
  // JOIN-TYPE-SPECIFIC EXECUTION METHODS
  // Each method handles strategy selection and optimization for its join type.
  // ============================================================================

  private def executeInnerJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.resolvedInnerStrategy match {
      case JoinStrategy.INNER_SORT_WITH_POST =>
        ctx.metrics.numSortMergeJoins += 1
        ctx.metrics.joinSortTime.ns {
          val innerMaps = executeRawInnerJoinForSort(ctx)
          applyConditionFilter(innerMaps, ctx)
        }
      case JoinStrategy.INNER_HASH_WITH_POST =>
        ctx.metrics.numHashJoins += 1
        ctx.metrics.joinHashTime.ns {
          val innerMaps = executeRawInnerJoinForHash(ctx)
          applyConditionFilter(innerMaps, ctx)
        }
      case _ =>
        // HASH_ONLY: Use direct mixed join API for conditional, distinct or hash for unconditional
        ctx.metrics.numHashJoins += 1
        ctx.metrics.joinHashTime.ns {
          ctx.condition match {
            case Some(cond) =>
              // Use the already-selected effectiveBuildSide directly
              ctx.effectiveBuildSide match {
                case GpuBuildLeft =>
                  innerHashJoinBuildLeft(ctx.leftKeys, ctx.rightKeys,
                    ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
                case GpuBuildRight =>
                  innerHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys,
                    ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
              }
            case None =>
              executeRawInnerJoinForHash(ctx)
          }
        }
    }
  }

  /**
   * Execute a raw inner join using hash algorithm.
   * Uses distinct optimization when available.
   */
  private def executeRawInnerJoinForHash(ctx: JoinContext): GatherMapsResult = {
    if (ctx.isDistinct) {
      ctx.metrics.numDistinctJoins += 1
      ctx.metrics.joinDistinctTime.ns {
        val arrayRet = if (ctx.effectiveBuildSide == GpuBuildRight) {
          ctx.leftKeys.innerDistinctJoinGatherMaps(ctx.rightKeys, ctx.compareNullsEqual)
        } else {
          ctx.rightKeys.innerDistinctJoinGatherMaps(ctx.leftKeys, ctx.compareNullsEqual).reverse
        }
        GatherMapsResult(arrayRet(0), arrayRet(1))
      }
    } else {
      // Use the already-selected effectiveBuildSide directly
      ctx.effectiveBuildSide match {
        case GpuBuildLeft =>
          innerHashJoinBuildLeft(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
        case GpuBuildRight =>
          innerHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
      }
    }
  }

  /**
   * Execute a raw inner join using sort algorithm.
   * Note: Sort join does not have a distinct optimization path.
   */
  private def executeRawInnerJoinForSort(ctx: JoinContext): GatherMapsResult = {
    // Sort joins speed up cases when there are lots of duplicates so no need
    //  for us to try and do a distinct optimization.
    if (ctx.useKeyRemapping) {
      // Use the already-selected effectiveBuildSide directly
      innerSortJoinWithRemapping(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual,
        ctx.effectiveBuildSide, ctx.metrics.joinKeyRemapTime)
    } else {
      // Use the already-selected effectiveBuildSide directly
      ctx.effectiveBuildSide match {
        case GpuBuildLeft =>
          innerSortJoinBuildLeft(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
        case GpuBuildRight =>
          innerSortJoinBuildRight(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
      }
    }
  }

  /**
   * Apply condition filter to inner join results if a condition is present.
   */
  private def applyConditionFilter(
      innerMaps: GatherMapsResult,
      ctx: JoinContext): GatherMapsResult = {
    ctx.condition match {
      case Some(cond) =>
        withResource(innerMaps) { _ =>
          filterInnerJoinWithLazyCondition(innerMaps,
            ctx.leftTable.get, ctx.rightTable.get, cond)
        }
      case None => innerMaps
    }
  }

  /**
   * Execute a left outer join. Dispatches to the appropriate strategy implementation.
   */
  private def executeLeftOuterJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.resolvedInnerStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        executeLeftOuterWithHashPost(ctx)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        executeLeftOuterWithSortPost(ctx)
      case _ =>
        executeLeftOuterHashOnly(ctx)
    }
  }

  /**
   * Execute left outer join using HASH_ONLY strategy.
   * Uses distinct optimization when available for unconditional joins.
   */
  private def executeLeftOuterHashOnly(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      ctx.condition match {
        case Some(cond) =>
          leftOuterHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys,
            ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
        case None =>
          if (ctx.isDistinct) {
            ctx.metrics.numDistinctJoins += 1
            ctx.metrics.joinDistinctTime.ns {
              val rightRet = ctx.leftKeys.leftDistinctJoinGatherMap(
                ctx.rightKeys, ctx.compareNullsEqual)
              // For distinct left outer join, left side rows stay in place (identity map).
              // Create both maps so downstream code doesn't need special handling.
              val leftRet = GatherMapsResult.identityGatherMap(ctx.leftRowCount)
              closeOnExcept(leftRet) { _ =>
                GatherMapsResult(leftRet, rightRet)
              }
            }
          } else {
            leftOuterHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
          }
      }
    }
  }

  /**
   * Execute left outer join using INNER_HASH_WITH_POST strategy:
   * inner hash join -> filter -> convert to left outer.
   * Uses distinct optimization for the inner join when available.
   */
  private def executeLeftOuterWithHashPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      val innerMaps = executeRawInnerJoinForHash(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftOuter(filteredMaps, ctx.leftRowCount.toInt, ctx.rightRowCount.toInt)
      }
    }
  }

  /**
   * Execute left outer join using INNER_SORT_WITH_POST strategy:
   * inner sort join -> filter -> convert to left outer.
   */
  private def executeLeftOuterWithSortPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numSortMergeJoins += 1
    ctx.metrics.joinSortTime.ns {
      val innerMaps = executeRawInnerJoinForSort(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftOuter(filteredMaps, ctx.leftRowCount.toInt, ctx.rightRowCount.toInt)
      }
    }
  }

  /**
   * Execute a right outer join. Dispatches to the appropriate strategy implementation.
   */
  private def executeRightOuterJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.resolvedInnerStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        executeRightOuterWithHashPost(ctx)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        executeRightOuterWithSortPost(ctx)
      case _ =>
        executeRightOuterHashOnly(ctx)
    }
  }

  /**
   * Execute right outer join using HASH_ONLY strategy.
   * Uses distinct optimization when available for unconditional joins.
   */
  private def executeRightOuterHashOnly(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      ctx.condition match {
        case Some(cond) =>
          rightOuterHashJoinBuildLeft(ctx.leftKeys, ctx.rightKeys,
            ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
        case None =>
          if (ctx.isDistinct) {
            ctx.metrics.numDistinctJoins += 1
            ctx.metrics.joinDistinctTime.ns {
              val leftRet = ctx.rightKeys.leftDistinctJoinGatherMap(
                ctx.leftKeys, ctx.compareNullsEqual)
              // For distinct right outer join, right side rows stay in place (identity map).
              // Create both maps so downstream code doesn't need special handling.
              val rightRet = GatherMapsResult.identityGatherMap(ctx.rightRowCount)
              closeOnExcept(rightRet) { _ =>
                GatherMapsResult(leftRet, rightRet)
              }
            }
          } else {
            rightOuterHashJoinBuildLeft(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
          }
      }
    }
  }

  /**
   * Execute right outer join using INNER_HASH_WITH_POST strategy:
   * inner hash join -> filter -> convert to right outer.
   * Uses distinct optimization for the inner join when available.
   */
  private def executeRightOuterWithHashPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      val innerMaps = executeRawInnerJoinForHash(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeRightOuter(filteredMaps, ctx.leftRowCount.toInt, ctx.rightRowCount.toInt)
      }
    }
  }

  /**
   * Execute right outer join using INNER_SORT_WITH_POST strategy:
   * inner sort join -> filter -> convert to right outer.
   */
  private def executeRightOuterWithSortPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numSortMergeJoins += 1
    ctx.metrics.joinSortTime.ns {
      val innerMaps = executeRawInnerJoinForSort(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeRightOuter(filteredMaps, ctx.leftRowCount.toInt, ctx.rightRowCount.toInt)
      }
    }
  }

  /**
   * Execute a left semi join. Dispatches to the appropriate strategy implementation.
   */
  private def executeLeftSemiJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.resolvedInnerStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        executeLeftSemiWithHashPost(ctx)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        executeLeftSemiWithSortPost(ctx)
      case _ =>
        executeLeftSemiHashOnly(ctx)
    }
  }

  /**
   * Execute left semi join using HASH_ONLY strategy.
   */
  private def executeLeftSemiHashOnly(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      ctx.condition match {
        case Some(cond) =>
          leftSemiHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys,
            ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
        case None =>
          leftSemiHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
      }
    }
  }

  /**
   * Execute left semi join using INNER_HASH_WITH_POST strategy:
   * inner hash join -> filter -> convert to semi.
   * Uses distinct optimization for the inner join when available.
   */
  private def executeLeftSemiWithHashPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      val innerMaps = executeRawInnerJoinForHash(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftSemi(filteredMaps, ctx.leftRowCount.toInt)
      }
    }
  }

  /**
   * Execute left semi join using INNER_SORT_WITH_POST strategy:
   * inner sort join -> filter -> convert to semi.
   */
  private def executeLeftSemiWithSortPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numSortMergeJoins += 1
    ctx.metrics.joinSortTime.ns {
      val innerMaps = executeRawInnerJoinForSort(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftSemi(filteredMaps, ctx.leftRowCount.toInt)
      }
    }
  }

  /**
   * Execute a left anti join. Dispatches to the appropriate strategy implementation.
   */
  private def executeLeftAntiJoin(ctx: JoinContext): GatherMapsResult = {
    ctx.resolvedInnerStrategy match {
      case JoinStrategy.INNER_HASH_WITH_POST =>
        executeLeftAntiWithHashPost(ctx)
      case JoinStrategy.INNER_SORT_WITH_POST =>
        executeLeftAntiWithSortPost(ctx)
      case _ =>
        executeLeftAntiHashOnly(ctx)
    }
  }

  /**
   * Execute left anti join using HASH_ONLY strategy.
   */
  private def executeLeftAntiHashOnly(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      ctx.condition match {
        case Some(cond) =>
          leftAntiHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys,
            ctx.leftTable.get, ctx.rightTable.get, cond, ctx.nullEquality)
        case None =>
          leftAntiHashJoinBuildRight(ctx.leftKeys, ctx.rightKeys, ctx.compareNullsEqual)
      }
    }
  }

  /**
   * Execute left anti join using INNER_HASH_WITH_POST strategy:
   * inner hash join -> filter -> convert to anti.
   * Uses distinct optimization for the inner join when available.
   */
  private def executeLeftAntiWithHashPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numHashJoins += 1
    ctx.metrics.joinHashTime.ns {
      val innerMaps = executeRawInnerJoinForHash(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftAnti(filteredMaps, ctx.leftRowCount.toInt)
      }
    }
  }

  /**
   * Execute left anti join using INNER_SORT_WITH_POST strategy:
   * inner sort join -> filter -> convert to anti.
   */
  private def executeLeftAntiWithSortPost(ctx: JoinContext): GatherMapsResult = {
    ctx.metrics.numSortMergeJoins += 1
    ctx.metrics.joinSortTime.ns {
      val innerMaps = executeRawInnerJoinForSort(ctx)
      val filteredMaps = applyConditionFilter(innerMaps, ctx)
      withResource(filteredMaps) { _ =>
        makeLeftAnti(filteredMaps, ctx.leftRowCount.toInt)
      }
    }
  }
}

object GpuHashJoin {
  // Designed for the null-aware anti-join in GpuBroadcastHashJoin.
  def anyNullInKey(cb: ColumnarBatch, boundKeys: Seq[GpuExpression]): Boolean = {
    withResource(GpuProjectExec.project(cb, boundKeys)) { keysCb =>
      (0 until keysCb.numCols()).exists(keysCb.column(_).hasNull)
    }
  }

  // For the join on struct, it's equal for nulls in child columns of struct column, it's not
  // equal for the root struct when meets both nulls.
  // So for join types other than FullOuter and join on struct column with nullable child,
  // we simply set compareNullsEqual as true, and Spark plan already filter out the nulls for root
  // struct column.
  // For details, see https://github.com/NVIDIA/spark-rapids/issues/2126.
  // For Non-nested keys, it is also correctly processed, because compareNullsEqual will be set to
  // false which is the semantic of join: null != null when join
  def compareNullsEqual(
      joinType: JoinType,
      buildKeys: Seq[Expression]): Boolean = (joinType != FullOuter) &&
    GpuHashJoin.anyNullableStructChild(buildKeys)

  // For full outer and outer joins with build side matching outer side, we need to keep the
  // nulls in the build table and compare nulls as equal.
  // Note: for outer joins with build side matching outer side and join on struct column with child
  // is nullable, MUST not filter out null records from build table.
  def buildSideNeedsNullFilter(
      joinType: JoinType,
      compareNullsEqual: Boolean, // from function: compareNullsEqual
      buildSide: GpuBuildSide,
      buildKeys:Seq[Expression]): Boolean = {
    val needFilterOutNull = joinType match {
      case FullOuter => false
      case LeftOuter if buildSide == GpuBuildLeft => false
      case RightOuter if buildSide == GpuBuildRight => false
      case _ => true
    }
    needFilterOutNull && compareNullsEqual && buildKeys.exists(_.nullable)
  }

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
  }

  // This is used by sort join and broadcast join
  def tagBuildSide(meta: SparkPlanMeta[_], joinType: JoinType, buildSide: GpuBuildSide): Unit = {
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

/**
 * Result of unified join planning containing both the effective build side and strategy.
 * These are coupled decisions - changing one may require changing the other.
 *
 * @param effectiveBuildSide the physical build side to use for the join algorithm
 * @param effectiveStrategy the join strategy to use
 */
case class JoinPlanningResult(
    effectiveBuildSide: GpuBuildSide,
    effectiveStrategy: JoinStrategy.JoinStrategy)

object JoinStrategy extends Enumeration {
  type JoinStrategy = Value
  /**
   * AUTO: Use heuristics to automatically determine the best join strategy.
   * This is the default and will evolve over time as we add more heuristics.
   * 
   * '''When AUTO Switches to Sort-Merge Join:'''
   * 
   * For INNER, LEFT OUTER, and RIGHT OUTER joins, AUTO will automatically switch from hash join
   * to sort-merge join when the build side has highly skewed keys (detected via max duplicate
   * key count exceeding a threshold).
   * 
   * The switch occurs when:
   * 1. Join type is INNER, LEFT OUTER, or RIGHT OUTER
   * 2. Build side row count >= spark.rapids.sql.join.maxDuplicateKeyCount.minBuildRows
   *    (default: same as threshold value)
   * 3. Max duplicate key count >= spark.rapids.sql.join.maxDuplicateKeyCount.sortThreshold
   *    (default: 171609, based on experimental analysis)
   * 
   * '''How to Disable:'''
   * - Set spark.rapids.sql.join.maxDuplicateKeyCount.sortThreshold=0 (disables heuristic)
   * - Set spark.rapids.sql.join.strategy=HASH_ONLY (forces hash join)
   * - Set spark.rapids.sql.join.strategy=INNER_HASH_WITH_POST (forces hash with post-processing)
   */
  val AUTO = Value("AUTO")
  /**
   * INNER_HASH_WITH_POST: Force inner hash join with post-processing to convert to other
   * join types and apply join filtering. This performs an inner hash join first, then
   * applies post-processing transformations to produce the desired join type (e.g., left outer,
   * semi, anti) and applies any conditional filters.
   */
  val INNER_HASH_WITH_POST = Value("INNER_HASH_WITH_POST")
  /**
   * INNER_SORT_WITH_POST: Force inner sort-merge join with post-processing to convert to other
   * join types and apply join filtering. This performs an inner sort-merge join first, then
   * applies post-processing transformations to produce the desired join type. Falls back to
   * INNER_HASH_WITH_POST when ARRAY or STRUCT key types are present (unless key remapping
   * is enabled, which supports these types).
   */
  val INNER_SORT_WITH_POST = Value("INNER_SORT_WITH_POST")
  /**
   * HASH_ONLY: Force the use of traditional hash join only.
   */
  val HASH_ONLY = Value("HASH_ONLY")

  /**
   * Plan a join by selecting both the effective build side and strategy together.
   * These are coupled decisions - the strategy affects which build sides are valid,
   * and the build side affects which strategies are beneficial.
   *
   * Build side and strategy coupling:
   * - INNER_*_WITH_POST strategies allow freely selecting the build side
   * - HASH_ONLY can only change build side for InnerLike joins; outer/semi/anti have fixed sides
   *
   * When strategy is AUTO:
   * - With condition (Inner/LeftOuter/RightOuter): prefer INNER_*_WITH_POST, pick smaller side
   * - With condition (LeftSemi/LeftAnti): HASH_ONLY with fixed build side (right)
   * - Without condition:
   *   - InnerLike: HASH_ONLY, can pick smaller build side
   *   - LeftOuter: if left is smaller AND buildSideSelection allows, use INNER_*_WITH_POST
   *   - RightOuter: if right is smaller AND buildSideSelection allows, use INNER_*_WITH_POST
   *   - LeftSemi/LeftAnti: HASH_ONLY with fixed build side (right)
   *
   * When INNER_*_WITH_POST is selected: compute extended stats to decide between HASH and SORT
   *
   * @param joinOptions the join options containing strategy and config values
   * @param joinType the type of join being performed
   * @param hasCondition whether the join has an AST condition
   * @param planBuildSide the build side from the query plan
   * @param leftKeys the left side keys table
   * @param rightKeys the right side keys table
   * @param leftDataWithStats left batch with stats wrapper
   * @param rightDataWithStats right batch with stats wrapper
   * @param nullEquality null equality setting for stats computation
   * @return JoinPlanningResult with both effectiveBuildSide and effectiveStrategy
   */
  def planJoin(
      joinOptions: JoinOptions,
      joinType: JoinType,
      hasCondition: Boolean,
      planBuildSide: GpuBuildSide,
      leftKeys: Table,
      rightKeys: Table,
      leftDataWithStats: LazySpillableColumnarBatchWithStats,
      rightDataWithStats: LazySpillableColumnarBatchWithStats,
      nullEquality: NullEquality): JoinPlanningResult = {
    
    val leftRowCount = leftKeys.getRowCount
    val rightRowCount = rightKeys.getRowCount
    
    // Helper to get smaller build side (when allowed by buildSideSelection)
    def smallerBuildSide: GpuBuildSide =
      if (leftRowCount <= rightRowCount) GpuBuildLeft else GpuBuildRight
    
    // Helper to select build side respecting buildSideSelection config
    def selectBuildSide: GpuBuildSide = joinOptions.buildSideSelection match {
      case JoinBuildSideSelection.FIXED => planBuildSide
      case JoinBuildSideSelection.AUTO | JoinBuildSideSelection.SMALLEST => smallerBuildSide
    }
    
    // Can we change the build side from planBuildSide?
    val canChangeBuildSide = joinOptions.buildSideSelection match {
      case JoinBuildSideSelection.FIXED => false
      case JoinBuildSideSelection.AUTO | JoinBuildSideSelection.SMALLEST => true
    }
    
    // Helper to compute basic stats for a given build side
    def computeStats(buildSide: GpuBuildSide): JoinBuildSideStats = buildSide match {
      case GpuBuildLeft => leftDataWithStats.getOrComputeStats(leftKeys, nullEquality)
      case GpuBuildRight => rightDataWithStats.getOrComputeStats(rightKeys, nullEquality)
    }
    
    // Helper to compute extended stats (with maxDuplicateKeyCount) for hash vs sort decision
    def computeExtendedStats(buildSide: GpuBuildSide): JoinBuildSideStatsWithMaxDup = 
      buildSide match {
        case GpuBuildLeft => leftDataWithStats.getOrComputeMaxDupStats(leftKeys, nullEquality)
        case GpuBuildRight => rightDataWithStats.getOrComputeMaxDupStats(rightKeys, nullEquality)
      }
    
    // Helper to decide between hash and sort based on extended stats.
    // hashStrategy is the strategy to use when hash is preferred.
    def decideHashVsSort(
        buildSide: GpuBuildSide,
        hashStrategy: JoinStrategy): JoinStrategy = {
      val maxDupThreshold = joinOptions.maxDuplicateKeyCountSortThreshold
      val minBuildRows = joinOptions.maxDuplicateKeyCountMinBuildRows
      val buildRowCount = if (buildSide == GpuBuildLeft) leftRowCount else rightRowCount
      
      // Gate: only compute max-dup if thresholds are met
      if (maxDupThreshold <= 0 || buildRowCount < minBuildRows) {
        return hashStrategy
      }
      
      val maxDupStats = computeExtendedStats(buildSide)
      if (maxDupStats.maxDuplicateKeyCount >= maxDupThreshold) {
        INNER_SORT_WITH_POST
      } else {
        hashStrategy
      }
    }
    
    joinOptions.strategy match {
      case INNER_HASH_WITH_POST =>
        // Fixed strategy, can freely select build side
        val buildSide = selectBuildSide
        computeStats(buildSide)
        JoinPlanningResult(buildSide, INNER_HASH_WITH_POST)
        
      case INNER_SORT_WITH_POST =>
        // Fixed strategy, can freely select build side
        val buildSide = selectBuildSide
        computeStats(buildSide)
        JoinPlanningResult(buildSide, INNER_SORT_WITH_POST)
        
      case HASH_ONLY =>
        // Fixed HASH_ONLY, build side selection depends on join type
        val buildSide = joinType match {
          case _: InnerLike =>
            // Can freely select build side for inner joins
            selectBuildSide
          case LeftOuter => GpuBuildRight  // Fixed by cudf API
          case RightOuter => GpuBuildLeft  // Fixed by cudf API
          case LeftSemi | LeftAnti => GpuBuildRight  // Fixed by join semantics
          case FullOuter =>
            // FullOuter is decomposed into sub-joins before reaching here
            throw new IllegalStateException(
              s"FullOuter should be decomposed before join planning")
          case other =>
            throw new IllegalStateException(
              s"Unexpected join type in planJoin with HASH_ONLY: $other")
        }
        computeStats(buildSide)
        JoinPlanningResult(buildSide, HASH_ONLY)
        
      case AUTO =>
        planAutoStrategy(joinType, hasCondition, planBuildSide, leftRowCount, rightRowCount,
          canChangeBuildSide, selectBuildSide, smallerBuildSide,
          computeStats, (bs, hs) => decideHashVsSort(bs, hs))
    }
  }
  
  /**
   * Plan join when strategy is AUTO - determines both build side and strategy together.
   */
  private def planAutoStrategy(
      joinType: JoinType,
      hasCondition: Boolean,
      planBuildSide: GpuBuildSide,
      leftRowCount: Long,
      rightRowCount: Long,
      canChangeBuildSide: Boolean,
      selectBuildSide: => GpuBuildSide,
      smallerBuildSide: => GpuBuildSide,
      computeStats: GpuBuildSide => JoinBuildSideStats,
      decideHashVsSort: (GpuBuildSide, JoinStrategy) => JoinStrategy): JoinPlanningResult = {
    
    if (hasCondition) {
      // Conditional joins
      joinType match {
        case _: InnerLike | LeftOuter | RightOuter =>
          // Prefer INNER_*_WITH_POST for conditional joins, can pick smaller build side
          val buildSide = selectBuildSide
          val strategy = decideHashVsSort(buildSide, INNER_HASH_WITH_POST)
          JoinPlanningResult(buildSide, strategy)
          
        case LeftSemi | LeftAnti =>
          // HASH_ONLY with fixed build side (right)
          computeStats(GpuBuildRight)
          JoinPlanningResult(GpuBuildRight, HASH_ONLY)
          
        case FullOuter =>
          // FullOuter is decomposed into sub-joins before reaching here
          throw new IllegalStateException(
            s"FullOuter should be decomposed before join planning")
          
        case other =>
          throw new IllegalStateException(
            s"Unexpected join type in planAutoStrategy (conditional): $other")
      }
    } else {
      // Unconditional joins
      joinType match {
        case _: InnerLike =>
          // Can freely select build side, check hash vs sort heuristic
          // Note: For inner joins, INNER_HASH_WITH_POST is identical to HASH_ONLY
          val buildSide = selectBuildSide
          val strategy = decideHashVsSort(buildSide, INNER_HASH_WITH_POST)
          JoinPlanningResult(buildSide, strategy)
          
        case LeftOuter =>
          // cudf requires BuildRight for HASH_ONLY, but if left is smaller we need 
          // INNER_*_WITH_POST to get smaller build side
          if (canChangeBuildSide && leftRowCount < rightRowCount) {
            // Switch to smaller build side, requires INNER_*_WITH_POST
            val strategy = decideHashVsSort(GpuBuildLeft, INNER_HASH_WITH_POST)
            JoinPlanningResult(GpuBuildLeft, strategy)
          } else {
            // Use required build side (right), HASH_ONLY is faster when not switching
            val strategy = decideHashVsSort(GpuBuildRight, HASH_ONLY)
            JoinPlanningResult(GpuBuildRight, strategy)
          }
          
        case RightOuter =>
          // cudf requires BuildLeft for HASH_ONLY, but if right is smaller we need
          // INNER_*_WITH_POST to get smaller build side
          if (canChangeBuildSide && rightRowCount < leftRowCount) {
            // Switch to smaller build side, requires INNER_*_WITH_POST
            val strategy = decideHashVsSort(GpuBuildRight, INNER_HASH_WITH_POST)
            JoinPlanningResult(GpuBuildRight, strategy)
          } else {
            // Use required build side (left), HASH_ONLY is faster when not switching
            val strategy = decideHashVsSort(GpuBuildLeft, HASH_ONLY)
            JoinPlanningResult(GpuBuildLeft, strategy)
          }
          
        case LeftSemi | LeftAnti =>
          // Fixed build side (right), HASH_ONLY
          computeStats(GpuBuildRight)
          JoinPlanningResult(GpuBuildRight, HASH_ONLY)
          
        case FullOuter =>
          // FullOuter is decomposed into sub-joins before reaching here
          throw new IllegalStateException(
            s"FullOuter should be decomposed before join planning")
          
        case other =>
          throw new IllegalStateException(
            s"Unexpected join type in planAutoStrategy (unconditional): $other")
      }
    }
  }
}

/**
 * Enumeration of build side selection strategies for join operations.
 */
object JoinBuildSideSelection extends Enumeration {
  type JoinBuildSideSelection = Value
  /**
   * AUTO: Use heuristics to automatically determine the best build side.
   * Currently behaves the same as SMALLEST, but the heuristics may change in the future
   * to consider factors beyond just row count.
   */
  val AUTO = Value("AUTO")
  /**
   * FIXED: Use the build side as specified by the query plan.
   * This preserves the original Spark behavior without any dynamic selection.
   */
  val FIXED = Value("FIXED")
  /**
   * SMALLEST: Always select the side with the smallest row count as the build side.
   * This is determined on a batch-by-batch basis at join time.
   */
  val SMALLEST = Value("SMALLEST")

  /**
   * Determine the physical build side for a join operation based on the selection strategy.
   *
   * This selects which side the join algorithm will use as its build table, which may differ
   * from the data movement build side (which side was materialized/buffered/broadcast).
   *
   * @param selection the build side selection strategy (AUTO, FIXED, or SMALLEST)
   * @param planBuildSide the build side from the query plan (used for FIXED, or as a hint for
   *                      AUTO/SMALLEST strategies)
   * @param leftRowCount the row count of the left side
   * @param rightRowCount the row count of the right side
   * @return the physical build side for the join algorithm to use
   */
  def selectPhysicalBuildSide(
      selection: JoinBuildSideSelection,
      planBuildSide: GpuBuildSide,
      leftRowCount: Long,
      rightRowCount: Long): GpuBuildSide = {
    selection match {
      case FIXED => planBuildSide
      case AUTO | SMALLEST =>
        // Both AUTO and SMALLEST currently select the smaller side
        // AUTO may evolve to use additional heuristics in the future
        if (leftRowCount < rightRowCount) GpuBuildLeft else GpuBuildRight
    }
  }
}

/**
 * Enumeration of key remapping modes for sort-merge joins.
 */
object KeyRemappingMode extends Enumeration {
  type KeyRemappingMode = Value
  /**
   * AUTO: Enable remapping when beneficial (multi-key joins, complex key types
   * like STRING/ARRAY/STRUCT, or when remapping enables sort-merge join for
   * otherwise unsupported key types).
   * 
   * '''When AUTO Enables Key Remapping:'''
   * 
   * Key remapping is used for sort-merge joins when:
   * 1. Join has multiple key columns (e.g., ON a.k1=b.k1 AND a.k2=b.k2), OR
   * 2. Join has complex key types: STRING, ARRAY, or STRUCT
   * 
   * Key remapping converts multi-column or complex keys into single INT32 remapped keys,
   * which can be more efficient for sort-merge joins.
   * 
   * '''Join Types:'''
   * - Supported: INNER, LEFT OUTER, RIGHT OUTER, FULL OUTER
   * - Not supported: LEFT SEMI, LEFT ANTI, EXISTENCE (these never use sort-merge join)
   * 
   * '''How to Disable:'''
   * - Set spark.rapids.sql.join.keyRemapping.mode=NEVER (disables remapping)
   * - Set spark.rapids.sql.join.strategy=HASH_ONLY (avoids sort-merge entirely)
   */
  val AUTO = Value("AUTO")
  /**
   * ALWAYS: Always use remapping for sort-merge joins (primarily for testing/evaluation).
   */
  val ALWAYS = Value("ALWAYS")
  /**
   * NEVER: Never use remapping, fall back to hash join if sort-merge join cannot
   * handle the key types without remapping.
   */
  val NEVER = Value("NEVER")
  
  /**
   * Determine whether key remapping should be used for a sort-merge join.
   * 
   * @param mode the key remapping mode (AUTO, ALWAYS, or NEVER)
   * @param leftKeys the left side keys table
   * @param rightKeys the right side keys table
   * @param joinType the type of join being performed
   * @return true if key remapping should be used
   */
  def shouldRemapKeys(
      mode: KeyRemappingMode,
      leftKeys: Table,
      rightKeys: Table,
      joinType: JoinType): Boolean = {
    // Hard exclusion: never remap for LeftSemi/LeftAnti/ExistenceJoin
    joinType match {
      case LeftSemi | LeftAnti | _: ExistenceJoin => return false
      case _ => // continue
    }
    
    mode match {
      case NEVER => false
      case ALWAYS => true
      case AUTO =>
        // AUTO mode remaps keys when it improves sort-merge join performance.
        // Key remapping converts original join keys into compact INT32 remapped keys.
        //
        // Benefits of key remapping:
        // 1. Enables fast radix sort: GPU can use highly optimized radix sort for single
        //    fixed-width INT32 columns, but must use slower comparison-based sort for
        //    multi-key columns or complex types (STRING, ARRAY, STRUCT). Remapping enables
        //    the fast path by converting any key configuration to a single INT32 column.
        // 2. Reduces data movement: Sorting and comparing a single INT32 column requires
        //    less memory bandwidth than multi-column or variable-width data.
        // 3. Eliminates expensive null comparisons: The remapped INT32 values encode
        //    null handling, avoiding expensive null checks during sort and merge.
        // 4. Transforms join: This effectively converts the sort-merge join into a
        //    grouped-merge join where the faster sort algorithm pays for remapping cost.
        val numKeys = leftKeys.getNumberOfColumns
        
        // Remap if multiple keys (enables radix sort instead of comparison sort)
        if (numKeys > 1) {
          return true
        }
        
        // Remap if any key is complex type (STRING, ARRAY, STRUCT)
        // These require slow comparison-based sort; remapping enables fast radix sort
        val hasComplexKey = (0 until numKeys).exists { i =>
          val leftType = leftKeys.getColumn(i).getType
          val rightType = rightKeys.getColumn(i).getType
          isComplexType(leftType) || isComplexType(rightType)
        }
        
        hasComplexKey
    }
  }
  
  /**
   * Check if a DType is considered "complex" for remapping purposes.
   * Complex types benefit from remapping in AUTO mode.
   */
  private def isComplexType(dtype: DType): Boolean = {
    dtype match {
      case DType.STRING => true
      case DType.LIST => true
      case DType.STRUCT => true
      case _ => false
    }
  }
}

/**
 * Options to control join behavior.
 * @param strategy the join strategy to use (AUTO, INNER_HASH_WITH_POST, INNER_SORT_WITH_POST,
 *                 or HASH_ONLY)
 * @param buildSideSelection the build side selection strategy (AUTO, FIXED, or SMALLEST)
 * @param targetSize the target batch size in bytes for the join operation
 * @param sizeEstimateThreshold the threshold used to decide when to skip the expensive join
 *                              output size estimation (defaults to 0.75)
 * @param maxDuplicateKeyCountSortThreshold threshold for maxDuplicateKeyCount to trigger
 *                                          sort-merge join (0 or negative to disable)
 * @param maxDuplicateKeyCountMinBuildRows minimum build row count before computing max-dup
 * @param keyRemappingMode key remapping mode for sort-merge joins (AUTO, ALWAYS, or NEVER)
 */
case class JoinOptions(
    strategy: JoinStrategy.JoinStrategy,
    buildSideSelection: JoinBuildSideSelection.JoinBuildSideSelection,
    targetSize: Long,
    sizeEstimateThreshold: Double,
    maxDuplicateKeyCountSortThreshold: Int,
    maxDuplicateKeyCountMinBuildRows: Int,
    keyRemappingMode: KeyRemappingMode.KeyRemappingMode)

/**
 * Class to hold statistics on the build-side batch of a hash join.
 * @param streamMagnificationFactor estimated magnification of a stream batch during join
 * @param isDistinct true if all build-side join keys are distinct
 */
/**
 * Container for all join-related GPU metrics to reduce parameter passing overhead.
 */
case class JoinMetrics(
    opTime: GpuMetric,
    joinTime: GpuMetric,
    numDistinctJoins: GpuMetric,
    numHashJoins: GpuMetric,
    numSortMergeJoins: GpuMetric,
    joinDistinctTime: GpuMetric,
    joinHashTime: GpuMetric,
    joinSortTime: GpuMetric,
    joinKeyRemapTime: GpuMetric)

object JoinMetrics {
  import GpuMetric._
  
  /**
   * Create JoinMetrics from a metrics lookup function (typically `gpuLongMetric` or
   * a Map lookup).
   */
  def apply(metricsLookup: String => GpuMetric): JoinMetrics = {
    JoinMetrics(
      opTime = metricsLookup(OP_TIME_LEGACY),
      joinTime = metricsLookup(JOIN_TIME),
      numDistinctJoins = metricsLookup(NUM_DISTINCT_JOINS),
      numHashJoins = metricsLookup(NUM_HASH_JOINS),
      numSortMergeJoins = metricsLookup(NUM_SORT_MERGE_JOINS),
      joinDistinctTime = metricsLookup(JOIN_DISTINCT_TIME),
      joinHashTime = metricsLookup(JOIN_HASH_TIME),
      joinSortTime = metricsLookup(JOIN_SORT_TIME),
      joinKeyRemapTime = metricsLookup(JOIN_KEY_REMAP_TIME))
  }
}

/**
 * Statistics for the build side of a join.
 * Includes basic metrics computed using Table.distinctCount().
 *
 * @param streamMagnificationFactor estimated magnification of a stream batch during join
 * @param isDistinct true if all build-side join keys are distinct
 * @param distinctCount number of distinct keys on build side
 * @param buildRowCount total number of rows on build side
 */
class JoinBuildSideStats(
    val streamMagnificationFactor: Double,
    val isDistinct: Boolean,
    val distinctCount: Long,
    val buildRowCount: Long) {
  
  override def toString: String = {
    s"JoinBuildSideStats(magnification=$streamMagnificationFactor, " +
      s"isDistinct=$isDistinct, distinct=$distinctCount, rows=$buildRowCount)"
  }
}

/**
 * Extended statistics that include max duplicate key count.
 * Computed using Table.keyRemap() which is more expensive but provides
 * both distinctCount and maxDuplicateKeyCount in a single pass.
 *
 * @param maxDuplicateKeyCount maximum number of duplicates for any single key
 */
class JoinBuildSideStatsWithMaxDup(
    streamMagnificationFactor: Double,
    isDistinct: Boolean,
    distinctCount: Long,
    buildRowCount: Long,
    val maxDuplicateKeyCount: Long)
    extends JoinBuildSideStats(
      streamMagnificationFactor,
      isDistinct,
      distinctCount,
      buildRowCount) {
  
  override def toString: String = {
    s"JoinBuildSideStatsWithMaxDup(magnification=$streamMagnificationFactor, " +
      s"isDistinct=$isDistinct, distinct=$distinctCount, rows=$buildRowCount, " +
      s"maxDup=$maxDuplicateKeyCount)"
  }
}

object JoinBuildSideStats {
  /**
   * Compute basic build-side statistics from a Table of join keys.
   * Uses Table.distinctCount() which is relatively fast.
   *
   * @param keysTable the table containing join keys
   * @param nullEquality whether nulls should be considered equal for distinctCount
   * @return basic build-side statistics
   */
  def fromTable(keysTable: Table, nullEquality: NullEquality): JoinBuildSideStats = {
    val buildRowCount = keysTable.getRowCount
    val distinctCount = keysTable.distinctCount(nullEquality)
    val isDistinct = distinctCount == buildRowCount
    val magnificationFactor = if (distinctCount > 0) {
      buildRowCount.toDouble / distinctCount
    } else {
      1.0 // Empty table or all nulls
    }
    new JoinBuildSideStats(magnificationFactor, isDistinct, distinctCount, buildRowCount)
  }

  /**
   * Compute basic build-side statistics from a batch and bound key expressions.
   * This projects the keys from the batch and then computes stats.
   *
   * @param batch the columnar batch
   * @param boundBuildKeys the bound expressions for extracting join keys
   * @return basic build-side statistics
   */
  def fromBatch(batch: ColumnarBatch,
                boundBuildKeys: Seq[GpuExpression]): JoinBuildSideStats = {
    // This is okay because the build keys must be deterministic
    withResource(GpuProjectExec.project(batch, boundBuildKeys)) { buildKeys =>
      // Based off of the keys on the build side guess at how many output rows there
      // will be for each input row on the stream side. This does not take into account
      // the join type, data skew or even if the keys actually match.
      withResource(GpuColumnVector.from(buildKeys)) { keysTable =>
        // Use EQUAL for backwards compatibility with existing behavior
        fromTable(keysTable, NullEquality.EQUAL)
      }
    }
  }
}

object JoinBuildSideStatsWithMaxDup {
  /**
   * Compute extended build-side statistics including max duplicate key count.
   * Uses KeyRemapping which computes both distinctCount and maxDuplicateKeyCount
   * in a single pass. More expensive than basic stats but avoids duplicate work.
   *
   * @param keysTable the table containing join keys
   * @param nullEquality whether nulls should be considered equal
   * @return extended build-side statistics with max duplicate key count
   */
  def fromTable(keysTable: Table, nullEquality: NullEquality): JoinBuildSideStatsWithMaxDup = {
    val buildRowCount = keysTable.getRowCount
    
    // Use KeyRemapping to compute both distinct count and max duplicate count in one pass
    val (distinctCount, maxDupCount) = withResource(
      new KeyRemapping(keysTable, nullEquality, true)) { remap =>
      (remap.getDistinctCount, remap.getMaxDuplicateCount)
    }
    
    val isDistinct = distinctCount == buildRowCount
    val magnificationFactor = if (distinctCount > 0) {
      buildRowCount.toDouble / distinctCount
    } else {
      1.0 // Empty table or all nulls
    }
    
    new JoinBuildSideStatsWithMaxDup(
      magnificationFactor, isDistinct, distinctCount, buildRowCount, maxDupCount)
  }
}

/**
 * Base iterator for hash joins.
 *
 * @param buildSide the side that was materialized/buffered for this join (data movement decision).
 *                  This is distinct from the physical build side that the join algorithm selects
 *                  when using AUTO or SMALLEST build side selection strategies.
 */
abstract class BaseHashJoinIterator(
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    joinOptions: JoinOptions,
    joinType: JoinType,
    buildSide: GpuBuildSide,
    conditionForLogging: Option[Expression],
    metrics: JoinMetrics)
    extends SplittableJoinIterator(
      NvtxRegistry.JOIN_GATHER,
      stream,
      streamAttributes,
      built,
      joinOptions.targetSize,
      joinOptions.sizeEstimateThreshold,
      opTime = metrics.opTime,
      joinTime = metrics.joinTime) {
  
  // Unpack metrics for convenient access (opTime already defined as val in parent, joinTime not)
  protected val joinTime: GpuMetric = metrics.joinTime
  protected val numDistinctJoins: GpuMetric = metrics.numDistinctJoins
  protected val numHashJoins: GpuMetric = metrics.numHashJoins
  protected val numSortMergeJoins: GpuMetric = metrics.numSortMergeJoins
  protected val joinDistinctTime: GpuMetric = metrics.joinDistinctTime
  protected val joinHashTime: GpuMetric = metrics.joinHashTime
  protected val joinSortTime: GpuMetric = metrics.joinSortTime
  protected val joinKeyRemapTime: GpuMetric = metrics.joinKeyRemapTime
  // Wrap the build batch once and reuse it across all stream batches to cache stats
  protected lazy val builtDataWithStats = LazySpillableColumnarBatchWithStats(built)
  // We can cache this because the build side is not changing
  protected lazy val buildStats: JoinBuildSideStats = buildStatsOpt.getOrElse {
    joinType match {
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        // Compute stats from the build batch
        // Note: In the refactored code paths using wrappers, stats should already be cached
        built.checkpoint()
        withRetryNoSplit {
          withRestoreOnRetry(built) {
            JoinBuildSideStats.fromBatch(built.getBatch, boundBuiltKeys)
          }
        }
      case _ =>
        // existence joins don't change size
        // For existence joins, we don't have actual distinct/row counts, use placeholder values
        new JoinBuildSideStats(
          streamMagnificationFactor = 1.0,
          isDistinct = false,
          distinctCount = 0L,
          buildRowCount = 0L)
    }
  }

  /**
   * Check if sort join is supported for the given key expressions.
   * Sort join does not support ARRAY or STRUCT types in join keys unless key remapping is enabled.
   * 
   * @param keys the key expressions to check
   * @param useKeyRemapping whether key remapping will be used for this join
   * @return true if sort join is supported for these keys
   */
  protected def isSortJoinSupported(keys: Seq[GpuExpression], useKeyRemapping: Boolean): Boolean = {
    if (useKeyRemapping) {
      // When key remapping is enabled, ARRAY and STRUCT keys are supported
      // because they will be remapped to INT32 keys
      true
    } else {
      // Without key remapping, ARRAY and STRUCT keys are not supported
      !keys.exists { expr =>
        expr.dataType match {
          case _: ArrayType | _: StructType => true
          case _ => false
        }
      }
    }
  }

  /**
   * Check if sort join is supported for both build/stream keys and log a warning if not.
   * 
   * @param useKeyRemapping whether key remapping will be used for this join
   * @return true if sort join is supported
   */
  protected def isSortJoinSupportedOrWarn(useKeyRemapping: Boolean): Boolean = {
    val leftKeysSupported = isSortJoinSupported(boundBuiltKeys, useKeyRemapping)
    val rightKeysSupported = isSortJoinSupported(boundStreamKeys, useKeyRemapping)
    if (!leftKeysSupported || !rightKeysSupported) {
      logWarning(s"INNER_SORT_WITH_POST strategy requested but join keys contain " +
        s"ARRAY or STRUCT types which are not supported for sort joins without key remapping. " +
        s"Falling back to INNER_HASH_WITH_POST strategy. " +
        s"To enable ARRAY/STRUCT key support, set ${RapidsConf.JOIN_KEY_REMAPPING_MODE.key} " +
        s"to AUTO (default) or ALWAYS.")
    }
    leftKeysSupported && rightKeysSupported
  }

  override def computeNumJoinRows(cb: LazySpillableColumnarBatch): Long = {
    // TODO: Replace this estimate with exact join row counts using the corresponding cudf APIs
    //       being added in https://github.com/rapidsai/cudf/issues/9053.
    joinType match {
      // Full Outer join is implemented via LeftOuter/RightOuter, so use same estimate.
      case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
        Math.ceil(cb.numRows * buildStats.streamMagnificationFactor).toLong
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

  private def estimatedNumBatches(cb: LazySpillableColumnarBatch): Int = joinType match {
    case _: InnerLike | LeftOuter | RightOuter | FullOuter =>
      // We want the gather map size to be around the target size. There are two gather maps
      // that are made up of ints, so estimate how many rows per batch on the stream side
      // will produce the desired gather map size.
      val approximateStreamRowCount = ((joinOptions.targetSize.toDouble / 2) /
          DType.INT32.getSizeInBytes) / buildStats.streamMagnificationFactor
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
    val boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    private val stream: Iterator[LazySpillableColumnarBatch],
    val boundStreamKeys: Seq[GpuExpression],
    val streamAttributes: Seq[Attribute],
    lazyCompiledConditionOpt: Option[LazyCompiledCondition],
    joinOptions: JoinOptions,
    val joinType: JoinType,
    val buildSide: GpuBuildSide,
    val compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    metrics: JoinMetrics)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      buildStatsOpt,
      stream,
      boundStreamKeys,
      streamAttributes,
      joinOptions,
      joinType,
      buildSide,
      conditionForLogging,
      metrics) {

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    NvtxIdWithMetrics(NvtxRegistry.HASH_JOIN_GATHER_MAP, joinTime) {
      // Edge case: inner joins with empty tables produce no results
      if (joinType.isInstanceOf[InnerLike] &&
        (leftKeys.getRowCount == 0 || rightKeys.getRowCount == 0)) {
        None
      } else {
        val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL
        val (leftDataWithStats, rightDataWithStats) = buildSide match {
          case GpuBuildLeft =>
            (builtDataWithStats, LazySpillableColumnarBatchWithStats(rightData))
          case GpuBuildRight =>
            (LazySpillableColumnarBatchWithStats(leftData), builtDataWithStats)
        }

        // Unified planning: select both build side and strategy together
        val planResult = JoinStrategy.planJoin(
          joinOptions,
          joinType,
          lazyCompiledConditionOpt.isDefined,
          buildSide,
          leftKeys,
          rightKeys,
          leftDataWithStats,
          rightDataWithStats,
          nullEquality)

        // Get the stats for the selected build side from the wrapper
        val buildStats = planResult.effectiveBuildSide match {
          case GpuBuildLeft => leftDataWithStats.getOrComputeStats(leftKeys, nullEquality)
          case GpuBuildRight => rightDataWithStats.getOrComputeStats(rightKeys, nullEquality)
        }

        // Determine whether to use key remapping for sort joins
        val useKeyRemapping = 
          planResult.effectiveStrategy == JoinStrategy.INNER_SORT_WITH_POST &&
          KeyRemappingMode.shouldRemapKeys(
            joinOptions.keyRemappingMode,
            leftKeys,
            rightKeys,
            joinType)

        // Check if sort join is supported for the key types
        val sortJoinSupported = isSortJoinSupportedOrWarn(useKeyRemapping)

        // Build JoinContext and execute
        val maps = lazyCompiledConditionOpt match {
          case Some(condition) =>
            // Conditional join - need the data tables for AST filtering
            withResource(GpuColumnVector.from(leftDataWithStats.getBatch)) { leftTable =>
              withResource(GpuColumnVector.from(rightDataWithStats.getBatch)) { rightTable =>
                val ctx = JoinContext.forConditionalJoin(
                  joinType = joinType,
                  leftKeys = leftKeys,
                  rightKeys = rightKeys,
                  leftTable = leftTable,
                  rightTable = rightTable,
                  isDistinct = buildStats.isDistinct,
                  compareNullsEqual = compareNullsEqual,
                  options = joinOptions,
                  effectiveBuildSide = planResult.effectiveBuildSide,
                  condition = condition,
                  metrics = metrics,
                  effectiveStrategy = planResult.effectiveStrategy,
                  sortJoinSupported = sortJoinSupported,
                  useKeyRemapping = useKeyRemapping)
                JoinImpl.executeJoin(ctx)
              }
            }
          case None =>
            // Unconditional join
            val ctx = JoinContext.forUnconditionalJoin(
              joinType = joinType,
              leftKeys = leftKeys,
              rightKeys = rightKeys,
              isDistinct = buildStats.isDistinct,
              compareNullsEqual = compareNullsEqual,
              options = joinOptions,
              effectiveBuildSide = planResult.effectiveBuildSide,
              metrics = metrics,
              effectiveStrategy = planResult.effectiveStrategy,
              sortJoinSupported = sortJoinSupported,
              useKeyRemapping = useKeyRemapping)
            JoinImpl.executeJoin(ctx)
        }

        makeGatherer(maps, leftData, rightData, joinType)
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      lazyCompiledConditionOpt.foreach(_.close())
    }
  }
}

/**
 * An iterator that does the stream-side only of a hash join. Using full join as an example,
 * it performs the left or right outer join for the stream side's view of a full outer join.
 * As the join is performed, the build-side rows that are referenced during the join are tracked
 * and can be retrieved after the iteration has completed to assist in performing the anti-join
 * needed to produce the final results needed for the full outer join.
 *
 * @param joinType the type of join being performed
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildStatsOpt statistics computed for the build side batch, if any
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
class HashJoinStreamSideIterator(
    joinType: JoinType,
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStatsOpt: Option[JoinBuildSideStats],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    lazyCompiledCondition: Option[LazyCompiledCondition],
    joinOptions: JoinOptions,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    metrics: JoinMetrics)
    extends BaseHashJoinIterator(
      built,
      boundBuiltKeys,
      buildStatsOpt,
      stream,
      boundStreamKeys,
      streamAttributes,
      joinOptions,
      joinType,
      buildSide,
      conditionForLogging,
      metrics) {

  // Determine the type of join to use as we iterate through the stream-side batches.
  // Each of these outer joins can be implemented in terms of another join as we iterate
  // through the stream-side batches and then emit a final anti-join batch based on which
  // build-side rows were left untouched by the stream-side iteration.
  // FullOuter joins are implemented in terms of LeftOuter + RightAnti or RightOuter + LeftAnti
  // LeftOuter joins are implemented in terms of Inner + RightAnti
  // RightOuter joins are implemented in terms of Inner + LeftAnti
  private val subJoinType = joinType match {
    case FullOuter if buildSide == GpuBuildRight => LeftOuter
    case FullOuter if buildSide == GpuBuildLeft => RightOuter
    case LeftOuter if buildSide == GpuBuildLeft => Inner
    case RightOuter if buildSide == GpuBuildRight => Inner
    case t =>
      throw new IllegalStateException(s"unsupported join type $t with $buildSide")
  }

  private val nullEquality = if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL

  // The cudf API build side is determined by subJoinType, not the original buildSide:
  // - LeftOuter uses leftOuterHashJoinBuildRight (expects BuildRight AST)
  // - RightOuter uses rightOuterHashJoinBuildLeft (expects BuildLeft AST)
  // - Inner uses innerHashJoinBuildRight (expects BuildRight AST)
  private val cudfBuildSide: GpuBuildSide = subJoinType match {
    case LeftOuter => GpuBuildRight
    case RightOuter => GpuBuildLeft
    case Inner => GpuBuildRight
    case t => throw new IllegalStateException(s"unexpected subJoinType: $t")
  }

  private[this] var builtSideTracker: Option[SpillableColumnarBatch] = buildSideTrackerInit

  override protected def joinGathererLeftRight(
      leftKeys: Table,
      leftData: LazySpillableColumnarBatch,
      rightKeys: Table,
      rightData: LazySpillableColumnarBatch): Option[JoinGatherer] = {
    NvtxIdWithMetrics(NvtxRegistry.FULL_HASH_JOIN_GATHER_MAP, joinTime) {
      // Wrap batches with stats tracking capability
      // Reuse the cached wrapper for the build side, create a fresh wrapper for the stream side
      val (leftDataWithStats, rightDataWithStats) = buildSide match {
        case GpuBuildLeft =>
          (builtDataWithStats, LazySpillableColumnarBatchWithStats(rightData))
        case GpuBuildRight =>
          (LazySpillableColumnarBatchWithStats(leftData), builtDataWithStats)
      }

      // Unified planning: select both build side and strategy together
      // Note: Use subJoinType since that's what we're actually executing
      // The planJoin will respect the fixed build side requirements for outer joins
      val planResult = JoinStrategy.planJoin(
        joinOptions,
        subJoinType,
        lazyCompiledCondition.isDefined,
        cudfBuildSide,  // cudfBuildSide is already the required side for subJoinType
        leftKeys,
        rightKeys,
        leftDataWithStats,
        rightDataWithStats,
        nullEquality)

      // Get the stats for the selected build side from the wrapper
      val buildStats = planResult.effectiveBuildSide match {
        case GpuBuildLeft => leftDataWithStats.getOrComputeStats(leftKeys, nullEquality)
        case GpuBuildRight => rightDataWithStats.getOrComputeStats(rightKeys, nullEquality)
      }

      // Determine whether to use key remapping for sort joins
      val useKeyRemapping = 
        planResult.effectiveStrategy == JoinStrategy.INNER_SORT_WITH_POST &&
        KeyRemappingMode.shouldRemapKeys(
          joinOptions.keyRemappingMode,
          leftKeys,
          rightKeys,
          subJoinType)

      // Check if sort join is supported for the key types
      val sortJoinSupported = isSortJoinSupportedOrWarn(useKeyRemapping)

      // Build JoinContext and execute
      val maps = lazyCompiledCondition match {
        case Some(condition) =>
          // Conditional join - need the data tables for AST filtering
          withResource(GpuColumnVector.from(leftDataWithStats.getBatch)) { leftTable =>
            withResource(GpuColumnVector.from(rightDataWithStats.getBatch)) { rightTable =>
              val ctx = JoinContext.forConditionalJoin(
                joinType = subJoinType,
                leftKeys = leftKeys,
                rightKeys = rightKeys,
                leftTable = leftTable,
                rightTable = rightTable,
                isDistinct = buildStats.isDistinct,
                compareNullsEqual = compareNullsEqual,
                options = joinOptions,
                effectiveBuildSide = planResult.effectiveBuildSide,
                condition = condition,
                metrics = metrics,
                effectiveStrategy = planResult.effectiveStrategy,
                sortJoinSupported = sortJoinSupported,
                useKeyRemapping = useKeyRemapping)
              JoinImpl.executeJoin(ctx)
            }
          }
        case None =>
          // Unconditional join
          val ctx = JoinContext.forUnconditionalJoin(
            joinType = subJoinType,
            leftKeys = leftKeys,
            rightKeys = rightKeys,
            isDistinct = buildStats.isDistinct,
            compareNullsEqual = compareNullsEqual,
            options = joinOptions,
            effectiveBuildSide = planResult.effectiveBuildSide,
            metrics = metrics,
            effectiveStrategy = planResult.effectiveStrategy,
            sortJoinSupported = sortJoinSupported,
            useKeyRemapping = useKeyRemapping)
          JoinImpl.executeJoin(ctx)
      }

      withResource(maps) { _ =>
        val lazyLeftMap = LazySpillableGatherMap(maps.left, "left_map")
        val lazyRightMap = LazySpillableGatherMap(maps.right, "right_map")
        NvtxIdWithMetrics(NvtxRegistry.UPDATE_TRACKING_MASK, joinTime) {
          closeOnExcept(Seq(lazyLeftMap, lazyRightMap)) { _ =>
            updateTrackingMask(if (buildSide == GpuBuildRight) lazyRightMap else lazyLeftMap)
          }
        }
        val (leftOutOfBoundsPolicy, rightOutOfBoundsPolicy) = subJoinType match {
          case LeftOuter => (OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.NULLIFY)
          case RightOuter => (OutOfBoundsPolicy.NULLIFY, OutOfBoundsPolicy.DONT_CHECK)
          case Inner => (OutOfBoundsPolicy.DONT_CHECK, OutOfBoundsPolicy.DONT_CHECK)
          case t => throw new IllegalStateException(s"unsupported join type $t")
        }
        val gatherer = JoinGatherer(lazyLeftMap, leftData, lazyRightMap,
          rightData, leftOutOfBoundsPolicy, rightOutOfBoundsPolicy)
        if (gatherer.isDone) {
          // Nothing matched...
          gatherer.close()
          None
        } else {
          Some(gatherer)
        }
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
    val tracker = builtSideTracker
    builtSideTracker = None
    tracker
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      builtSideTracker.foreach(_.close())
      builtSideTracker = None
      lazyCompiledCondition.foreach(_.close())
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
 * An iterator that does a hash outer join against a stream of batches where either the join
 * type is a full outer join or the join type is a left or right outer join and the build side
 * matches the outer join side.  It does this by doing a subset of the original join (e.g.:
 * left outer for a full outer join) and keeping track of the hits on the build side.  It then
 * produces a final batch of all the build side rows that were not already included.
 *
 * @param joinType the type of join to perform
 * @param built spillable form of the build side table. This will be closed by the iterator.
 * @param boundBuiltKeys bound expressions for the build side equi-join keys
 * @param buildStats statistics computed for the build side batch, if any
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
class HashOuterJoinIterator(
    joinType: JoinType,
    built: LazySpillableColumnarBatch,
    boundBuiltKeys: Seq[GpuExpression],
    buildStats: Option[JoinBuildSideStats],
    buildSideTrackerInit: Option[SpillableColumnarBatch],
    stream: Iterator[LazySpillableColumnarBatch],
    boundStreamKeys: Seq[GpuExpression],
    streamAttributes: Seq[Attribute],
    lazyCompiledCondition: Option[LazyCompiledCondition],
    joinOptions: JoinOptions,
    buildSide: GpuBuildSide,
    compareNullsEqual: Boolean, // This is a workaround to how cudf support joins for structs
    conditionForLogging: Option[Expression],
    metrics: JoinMetrics) extends Iterator[ColumnarBatch] with TaskAutoCloseableResource {

  private val streamJoinIter = new HashJoinStreamSideIterator(joinType, built, boundBuiltKeys,
    buildStats, buildSideTrackerInit, stream, boundStreamKeys, streamAttributes, 
    lazyCompiledCondition,
    joinOptions, buildSide, compareNullsEqual, conditionForLogging, metrics)
  
  // Unpack joinTime for use in this iterator
  private val joinTime = metrics.joinTime

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
    NvtxIdWithMetrics(NvtxRegistry.GET_FINAL_BATCH, joinTime) {
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
  lazyCompiledCondition: Option[LazyCompiledCondition],
  buildSide: GpuBuildSide,
  compareNullsEqual: Boolean,
  metrics: JoinMetrics
) extends ExistenceJoinIterator(spillableBuiltBatch, lazyStream, metrics.opTime, metrics.joinTime) {

  // Unpack joinTime for use in this iterator
  private val joinTime = metrics.joinTime

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
    lazyCondition: LazyCompiledCondition): GatherMap = {
    withResource(
      withResource(GpuColumnVector.from(leftColumnarBatch)) { leftTab =>
        withResource(GpuColumnVector.from(spillableBuiltBatch.getBatch)) { rightTab =>
          JoinImpl.leftSemiHashJoinBuildRight(
            leftKeysTab,
            rightKeysTab,
            leftTab,
            rightTab,
            lazyCondition,
            if (compareNullsEqual) NullEquality.EQUAL else NullEquality.UNEQUAL)
        }
      }
    ) { gather =>
      gather.releaseLeft()
    }
  }

  private def unconditionalBatchLeftSemiJoin(
    leftKeysTab: Table,
    rightKeysTab: Table
  ): GatherMap = {
    withResource(JoinImpl.leftSemiHashJoinBuildRight(leftKeysTab, rightKeysTab,
      compareNullsEqual)) { result =>
      result.releaseLeft()
    }
  }

  override def existsScatterMap(leftColumnarBatch: ColumnarBatch): GatherMap = {
    NvtxIdWithMetrics(NvtxRegistry.EXISTENCE_JOIN_SCATTER_MAP, joinTime) {
      withResource(leftKeysTable(leftColumnarBatch)) { leftKeysTab =>
        withResource(rightKeysTable()) { rightKeysTab =>
          lazyCompiledCondition.map { lazyCondition =>
            conditionalBatchLeftSemiJoin(leftColumnarBatch, leftKeysTab, rightKeysTab,
              lazyCondition)
          }.getOrElse {
            unconditionalBatchLeftSemiJoin(leftKeysTab, rightKeysTab)
          }
        }
      }
    }
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      lazyCompiledCondition.foreach(_.close())
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
  /**
   * The build side determines which side of the join is materialized/buffered/broadcast.
   * This is a data movement decision made at query planning time.
   *
   * IMPORTANT: This is distinct from the physical build side that the join algorithm may select
   * when buildSideSelection is AUTO or SMALLEST. When those strategies are used:
   * - `buildSide` still determines which side to materialize (e.g., which to broadcast)
   * - The join algorithm may independently choose which materialized data to use as the build
   *   table based on runtime row counts
   * - The join abstractions (GatherMapsResult) ensure correct semantics regardless of the
   *   algorithm's internal choice
   */
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

  protected lazy val compareNullsEqual: Boolean =
    GpuHashJoin.compareNullsEqual(joinType, buildKeys)

  protected lazy val (boundBuildKeys, boundStreamKeys) = {
    val lkeys = GpuBindReferences.bindGpuReferences(leftKeys, left.output, allMetrics)
    val rkeys = GpuBindReferences.bindGpuReferences(rightKeys, right.output, allMetrics)

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
      GpuBindReferences.bindGpuReference(c, streamOutput ++ buildOutput,
        allMetrics)
    }
    (streamOutput.size, boundCondition)
  }

  /**
   * Condition bound to left.output ++ right.output.
   * LazyCompiledCondition will transform it for BuildLeft by rewriting ordinals.
   */
  protected lazy val boundConditionLeftRight: Option[GpuExpression] = {
    condition.map { c =>
      GpuBindReferences.bindGpuReference(c, left.output ++ right.output, allMetrics)
    }
  }

  def doJoin(
      builtBatch: ColumnarBatch,
      stream: Iterator[ColumnarBatch],
      joinOptions: JoinOptions,
      numOutputRows: GpuMetric,
      numOutputBatches: GpuMetric,
      metrics: JoinMetrics): Iterator[ColumnarBatch] = {

    val filterOutNull = GpuHashJoin.buildSideNeedsNullFilter(joinType, compareNullsEqual,
      buildSide, buildKeys)

    val nullFiltered = if (filterOutNull) {
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
        // Create a new LazyCompiledCondition for this iterator (it takes ownership)
        val lazyCond = boundConditionLeftRight.map { cond =>
          LazyCompiledCondition(cond, left.output.size, right.output.size)
        }
        new HashedExistenceJoinIterator(
          spillableBuiltBatch,
          boundBuildKeys,
          lazyStream,
          boundStreamKeys,
          lazyCond,
          buildSide,
          compareNullsEqual,
          metrics)
      case FullOuter =>
        // Create a new LazyCompiledCondition for this iterator (it takes ownership)
        val lazyCond = boundConditionLeftRight.map { cond =>
          LazyCompiledCondition(cond, left.output.size, right.output.size)
        }
        new HashOuterJoinIterator(joinType, spillableBuiltBatch, boundBuildKeys, None, None,
          lazyStream, boundStreamKeys, streamedPlan.output,
          lazyCond, joinOptions, buildSide,
          compareNullsEqual, condition, metrics)
      case _ =>
        if (boundConditionLeftRight.isDefined) {
          // HashJoinIterator will close the LazyCompiledCondition
          val lazyCond = LazyCompiledCondition(
            boundConditionLeftRight.get,
            left.output.size,
            right.output.size)
          new HashJoinIterator(spillableBuiltBatch, boundBuildKeys, None,
            lazyStream, boundStreamKeys, streamedPlan.output, Some(lazyCond),
            joinOptions, joinType, buildSide,
            compareNullsEqual, condition, metrics)
        } else {
          new HashJoinIterator(spillableBuiltBatch, boundBuildKeys, None,
            lazyStream, boundStreamKeys, streamedPlan.output, None, joinOptions,
            joinType, buildSide, compareNullsEqual, condition, metrics)
        }
    }

    joinIterator.map { cb =>
      numOutputRows += cb.numRows()
      numOutputBatches += 1
      cb
    }
  }
}
