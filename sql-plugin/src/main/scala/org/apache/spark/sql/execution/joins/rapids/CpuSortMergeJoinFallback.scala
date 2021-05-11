/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
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

package org.apache.spark.sql.execution.joins.rapids

import com.nvidia.spark.rapids.{ColumnarToRowIterator, GpuOutOfCoreSortIterator, RowToColumnarIterator}
import com.nvidia.spark.rapids.RapidsBuffer.SpillCallback

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, JoinedRow, Predicate, Projection, RowOrdering, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.{InnerLike, JoinType}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, RowIterator}
import org.apache.spark.sql.execution.joins.SortMergeJoinScanner
import org.apache.spark.sql.vectorized.ColumnarBatch

class CpuSortMergeJoinFallback(
    val joinType: JoinType,
    val left: Iterator[ColumnarBatch],
    val leftSchema: Seq[Attribute],
    val unboundLeftGpuKeys: Seq[Expression],
    val unboundLeftCpuKeys: Seq[Expression],
    val right: Iterator[ColumnarBatch],
    val rightSchema: Seq[Attribute],
    val unboundRightGpuKeys: Seq[Expression],
    val unboundRightCpuKeys: Seq[Expression],
    val cpuCondition: Option[Expression],
    val outputSchema: Seq[Attribute],
    private val targetSize: Long,
    private val spillThreshold: Int,
    private val inMemoryThreshold: Int,
    private val spillCallback: SpillCallback,
    private val cleanupResources: () => Unit) extends Iterator[ColumnarBatch] {

  private val leftGpuOrder = unboundLeftGpuKeys.map(SortOrder(_, Ascending))
  private val sortedLeft = GpuOutOfCoreSortIterator.simple(left, leftGpuOrder, leftSchema,
    targetSize, spillCallback)
  // This is an ugly hack to avoid data not being spillable when we fall back
  sortedLeft.firstPassReadBatches()
  private val sortedRowLeft = ColumnarToRowIterator.simple(sortedLeft, leftSchema)

  private val rightGpuOrder = unboundRightGpuKeys.map(SortOrder(_, Ascending))
  private val sortedRight = GpuOutOfCoreSortIterator.simple(right, rightGpuOrder, rightSchema,
    targetSize, spillCallback)
  // This is an ugly hack to avoid data not being spillable when we fall back
  sortedRight.firstPassReadBatches()
  private val sortedRowRight = ColumnarToRowIterator.simple(sortedRight, rightSchema)

  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(unboundLeftCpuKeys, leftSchema)

  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(unboundRightCpuKeys, rightSchema)

  private val cpuJoined = {
    // Most of this code is copied an pasted from SortMergeJoinExec.doExecute in Apache Spark
    // This code should go away once we can do a better join with exploding joins on the GPU
    val boundCondition: (InternalRow) => Boolean = {
      cpuCondition.map { cond =>
        Predicate.create(cond, leftSchema ++ rightSchema).eval _
      }.getOrElse {
        (r: InternalRow) => true
      }
    }

    // An ordering that can be used to compare keys from both sides.
    val keyOrdering = RowOrdering.createNaturalAscendingOrdering(unboundLeftCpuKeys.map(_.dataType))
    val resultProj: InternalRow => InternalRow = UnsafeProjection.create(outputSchema, outputSchema)

    joinType match {
      case _: InnerLike =>
        new RowIterator {
          private[this] var currentLeftRow: InternalRow = _
          private[this] var currentRightMatches: ExternalAppendOnlyUnsafeRowArray = _
          private[this] var rightMatchesIterator: Iterator[UnsafeRow] = null
          private[this] val smjScanner = new SortMergeJoinScanner(
            createLeftKeyGenerator(),
            createRightKeyGenerator(),
            keyOrdering,
            RowIterator.fromScala(sortedRowLeft),
            RowIterator.fromScala(sortedRowRight),
            inMemoryThreshold,
            spillThreshold,
            cleanupResources
          )
          private[this] val joinRow = new JoinedRow

          if (smjScanner.findNextInnerJoinRows()) {
            currentRightMatches = smjScanner.getBufferedMatches
            currentLeftRow = smjScanner.getStreamedRow
            rightMatchesIterator = currentRightMatches.generateIterator()
          }

          override def advanceNext(): Boolean = {
            while (rightMatchesIterator != null) {
              if (!rightMatchesIterator.hasNext) {
                if (smjScanner.findNextInnerJoinRows()) {
                  currentRightMatches = smjScanner.getBufferedMatches
                  currentLeftRow = smjScanner.getStreamedRow
                  rightMatchesIterator = currentRightMatches.generateIterator()
                } else {
                  currentRightMatches = null
                  currentLeftRow = null
                  rightMatchesIterator = null
                  return false
                }
              }
              joinRow(currentLeftRow, rightMatchesIterator.next())
              if (boundCondition(joinRow)) {
                return true
              }
            }
            false
          }

          override def getRow: InternalRow = resultProj(joinRow)
        }.toScala
      case x =>
        throw new IllegalArgumentException(
          s"SortMergeJoin should not take $x as the JoinType")
    }
  }

  private val finalResult = RowToColumnarIterator.simple(cpuJoined, outputSchema, targetSize)

  override def hasNext: Boolean = finalResult.hasNext

  override def next(): ColumnarBatch = finalResult.next()
}

object CpuSortMergeJoinFallback {
  def fallbackFunc(
      joinType: JoinType,
      leftSchema: Seq[Attribute],
      unboundLeftGpuKeys: Seq[Expression],
      unboundLeftCpuKeys: Seq[Expression],
      rightSchema: Seq[Attribute],
      unboundRightGpuKeys: Seq[Expression],
      unboundRightCpuKeys: Seq[Expression],
      cpuCondition: Option[Expression],
      outputSchema: Seq[Attribute],
      targetSize: Long,
      spillThreshold: Int,
      inMemoryThreshold: Int,
      spillCallback: SpillCallback,
      cleanupResources: () => Unit):
  (Iterator[ColumnarBatch], Iterator[ColumnarBatch]) => Iterator[ColumnarBatch] = {
    def doIt(left: Iterator[ColumnarBatch],
        right: Iterator[ColumnarBatch]): Iterator[ColumnarBatch] = {
      new CpuSortMergeJoinFallback(joinType,
        left, leftSchema, unboundLeftGpuKeys, unboundLeftCpuKeys,
        right, rightSchema, unboundRightGpuKeys, unboundRightCpuKeys,
        cpuCondition, outputSchema, targetSize, spillThreshold, inMemoryThreshold,
        spillCallback, cleanupResources)
    }
    doIt
  }
}