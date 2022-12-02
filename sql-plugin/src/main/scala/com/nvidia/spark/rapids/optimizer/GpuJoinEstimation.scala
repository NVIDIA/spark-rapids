/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.optimizer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Filter, Histogram, Join, LogicalPlan, Project, Statistics}
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils._
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.ValueInterval
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

/**
 * Copied from Spark's JoinEstimation.scala
 *
 * Modifications:
 *
 * - Call GpuStatsPlanVisitor to get stats for child, instead of calling child.stats
 * - Removed check that join inputs have row count estimates, because they always do now
 * - Made estimate cross joins much more expensive
 * - Made estimate for non-equi-join joins much more expensive
 */
case class GpuJoinEstimation(join: Join) extends Logging {

  private val leftStats = GpuStatsPlanVisitor.visit(join.left)
  private val rightStats = GpuStatsPlanVisitor.visit(join.right)

  /**
   * Estimate statistics after join. Return `None` if the join type is not supported, or we don't
   * have enough statistics for estimation.
   */
  def estimate: Option[Statistics] = {
    join.joinType match {
      case Inner | LeftOuter | RightOuter | FullOuter =>
        estimateInnerOuterJoin()
      case Cross =>
        logDebug("GpuJoinEstimation.CrossJoin")
        estimateInnerOuterJoin().map(s => s.copy(rowCount = s.rowCount.map(r => r*r)))
      case LeftSemi | LeftAnti =>
        estimateLeftSemiAntiJoin()
      case _ =>
        logDebug(s"[CBO] Unsupported join type: ${join.joinType}")
        None
    }
  }

  /**
   * Estimate output size and number of rows after a join operator, and update output column stats.
   */
  private def estimateInnerOuterJoin(): Option[Statistics] = {

    val x = join match {
      // TODO shim ExtractEquiJoinKeys

      // spark 3.2
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, _, _) =>

        // spark 3.3
      //case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, _, _, _) =>
        logDebug("GpuJoinEstimation.estimateInnerOuterJoin() ExtractEquiJoinKeys")

        // 1. Compute join selectivity
        val joinKeyPairs = extractJoinKeysWithColStats(leftKeys, rightKeys)
        val (numInnerJoinedRows, keyStatsAfterJoin) = computeCardinalityAndStats(joinKeyPairs)

        // 2. Estimate the number of output rows
        val leftRows = leftStats.rowCount.get
        val rightRows = rightStats.rowCount.get

        def getHadoopRel(p: LogicalPlan): Option[HadoopFsRelation] = {
          p match {
            case Project(_, l: LogicalRelation) => l.relation match {
              case l: HadoopFsRelation => Some(l)
              case _ => None
            }
            case Project(_, Filter(_, l: LogicalRelation)) => l.relation match {
              case l: HadoopFsRelation => Some(l)
              case _ => None
            }
            case _ => None
          }
        }

        (getHadoopRel(join.left), getHadoopRel(join.right)) match {
          case (Some(l: HadoopFsRelation), Some(r: HadoopFsRelation)) =>
            val ll = l.location.inputFiles.sorted
            val rr = r.location.inputFiles.sorted
            val sameLoc = ll sameElements rr
            logDebug(s"join locations same? $sameLoc:\n${ll.mkString}\n${rr.mkString}")
            if (sameLoc) {
              return Some(Statistics(rowCount = Some(leftRows.pow(2)),
                sizeInBytes = leftStats.sizeInBytes.pow(2)))
            }
          case _ =>
            logDebug("not hadoop relation")
        }

        // Make sure outputRows won't be too small based on join type.
        val outputRows = joinType match {
          case LeftOuter =>
            // All rows from left side should be in the result.
            leftRows.max(numInnerJoinedRows)
          case RightOuter =>
            // All rows from right side should be in the result.
            rightRows.max(numInnerJoinedRows)
          case FullOuter =>
            // T(A FOJ B) = T(A LOJ B) + T(A ROJ B) - T(A IJ B)
            leftRows.max(numInnerJoinedRows) +
              rightRows.max(numInnerJoinedRows) - numInnerJoinedRows
          case _ =>
            assert(joinType == Inner || joinType == Cross)
            // Don't change for inner or cross join
            numInnerJoinedRows
        }

        // 3. Update statistics based on the output of join
        val inputAttrStats = AttributeMap(
          leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
        val attributesWithStat = join.output.filter(a =>
          inputAttrStats.get(a).exists(_.hasCountStats))
        val (fromLeft, fromRight) = attributesWithStat.partition(join.left.outputSet.contains(_))

        val outputStats: Seq[(Attribute, ColumnStat)] = if (outputRows == 0) {
          // The output is empty, we don't need to keep column stats.
          Nil
        } else if (numInnerJoinedRows == 0) {
          joinType match {
            // For outer joins, if the join selectivity is 0, the number of output rows is the
            // same as that of the outer side. And column stats of join keys from the outer side
            // keep unchanged, while column stats of join keys from the other side should be updated
            // based on added null values.
            case LeftOuter =>
              fromLeft.map(a => (a, inputAttrStats(a))) ++
                fromRight.map(a => (a, nullColumnStat(a.dataType, leftRows)))
            case RightOuter =>
              fromRight.map(a => (a, inputAttrStats(a))) ++
                fromLeft.map(a => (a, nullColumnStat(a.dataType, rightRows)))
            case FullOuter =>
              fromLeft.map { a =>
                val oriColStat = inputAttrStats(a)
                (a, oriColStat.copy(nullCount = Some(oriColStat.nullCount.get + rightRows)))
              } ++ fromRight.map { a =>
                val oriColStat = inputAttrStats(a)
                (a, oriColStat.copy(nullCount = Some(oriColStat.nullCount.get + leftRows)))
              }
            case _ =>
              assert(joinType == Inner || joinType == Cross)
              Nil
          }
        } else if (numInnerJoinedRows == leftRows * rightRows) {
          // Cartesian product, just propagate the original column stats
          inputAttrStats.toSeq
        } else {
          join.joinType match {
            // For outer joins, don't update column stats from the outer side.
            case LeftOuter =>
              fromLeft.map(a => (a, inputAttrStats(a))) ++
                updateOutputStats(outputRows, fromRight, inputAttrStats, keyStatsAfterJoin)
            case RightOuter =>
              updateOutputStats(outputRows, fromLeft, inputAttrStats, keyStatsAfterJoin) ++
                fromRight.map(a => (a, inputAttrStats(a)))
            case FullOuter =>
              inputAttrStats.toSeq
            case _ =>
              assert(joinType == Inner || joinType == Cross)
              // Update column stats from both sides for inner or cross join.
              updateOutputStats(outputRows, attributesWithStat, inputAttrStats, keyStatsAfterJoin)
          }
        }

        val outputAttrStats = AttributeMap(outputStats)
        Some(Statistics(
          sizeInBytes = getOutputSize(join.output, outputRows, outputAttrStats),
          rowCount = Some(outputRows),
          attributeStats = outputAttrStats))

      case _ =>
        logDebug("GpuJoinEstimation.estimateInnerOuterJoin() cartesian product")

        // When there is no equi-join condition, we do estimation like cartesian product.
        val inputAttrStats = AttributeMap(
          leftStats.attributeStats.toSeq ++ rightStats.attributeStats.toSeq)
        // Propagate the original column stats

        // AQE POC change:
        // we really want to discourage cartesian joins / nested loop joins so lets
        // square the result here until we figure out a better solution
        val cartesianProduct = leftStats.rowCount.get * rightStats.rowCount.get
        val outputRows = cartesianProduct * cartesianProduct

        Some(Statistics(
          sizeInBytes = getOutputSize(join.output, outputRows, inputAttrStats),
          rowCount = Some(outputRows),
          attributeStats = inputAttrStats))
    }

    logDebug(s"GpuJoinEstimation.estimateInnerOuterJoin() -> $x")

    x
  }

  // scalastyle:off

  /**
   * The number of rows of A inner join B on A.k1 = B.k1 is estimated by this basic formula:
   * T(A IJ B) = T(A) * T(B) / max(V(A.k1), V(B.k1)),
   * where V is the number of distinct values (ndv) of that column. The underlying assumption for
   * this formula is: each value of the smaller domain is included in the larger domain.
   *
   * Generally, inner join with multiple join keys can be estimated based on the above formula:
   * T(A IJ B) = T(A) * T(B) / (max(V(A.k1), V(B.k1)) * max(V(A.k2), V(B.k2)) * ... * max(V(A.kn), V(B.kn)))
   * However, the denominator can become very large and excessively reduce the result, so we use a
   * conservative strategy to take only the largest max(V(A.ki), V(B.ki)) as the denominator.
   *
   * That is, join estimation is based on the most selective join keys. We follow this strategy
   * when different types of column statistics are available. E.g., if card1 is the cardinality
   * estimated by ndv of join key A.k1 and B.k1, card2 is the cardinality estimated by histograms
   * of join key A.k2 and B.k2, then the result cardinality would be min(card1, card2).
   *
   * @param keyPairs pairs of join keys
   * @return join cardinality, and column stats for join keys after the join
   */
  // scalastyle:on
  private def computeCardinalityAndStats(keyPairs: Seq[(AttributeReference, AttributeReference)])
  : (BigInt, AttributeMap[ColumnStat]) = {
    // If there's no column stats available for join keys, estimate as cartesian product.

    //TODO modified
    var joinCard: BigInt = (1000 + leftStats.rowCount.get + rightStats.rowCount.get) / 2

    val keyStatsAfterJoin = new mutable.HashMap[Attribute, ColumnStat]()
    var i = 0
    while (i < keyPairs.length && joinCard != 0) {
      val (leftKey, rightKey) = keyPairs(i)
      // Check if the two sides are disjoint
      val leftKeyStat = leftStats.attributeStats(leftKey)
      val rightKeyStat = rightStats.attributeStats(rightKey)
      val lInterval = ValueInterval(leftKeyStat.min, leftKeyStat.max, leftKey.dataType)
      val rInterval = ValueInterval(rightKeyStat.min, rightKeyStat.max, rightKey.dataType)
      if (ValueInterval.isIntersected(lInterval, rInterval)) {
        val (newMin, newMax) = ValueInterval.intersect(lInterval, rInterval, leftKey.dataType)
        val (card, joinStat) = (leftKeyStat.histogram, rightKeyStat.histogram) match {
          case (Some(l: Histogram), Some(r: Histogram)) =>
            computeByHistogram(leftKey, rightKey, l, r, newMin, newMax)
          case _ =>
            computeByNdv(leftKey, rightKey, newMin, newMax)
        }
        keyStatsAfterJoin += (
          // Histograms are propagated as unchanged. During future estimation, they should be
          // truncated by the updated max/min. In this way, only pointers of the histograms are
          // propagated and thus reduce memory consumption.
          leftKey -> joinStat.copy(histogram = leftKeyStat.histogram),
          rightKey -> joinStat.copy(histogram = rightKeyStat.histogram)
        )
        // Return cardinality estimated from the most selective join keys.
        if (card < joinCard) joinCard = card
      } else {
        // One of the join key pairs is disjoint, thus the two sides of join is disjoint.
        joinCard = 0
      }
      i += 1
    }
    (joinCard, AttributeMap(keyStatsAfterJoin.toSeq))
  }

  /** Returns join cardinality and the column stat for this pair of join keys. */
  private def computeByNdv(
      leftKey: AttributeReference,
      rightKey: AttributeReference,
      min: Option[Any],
      max: Option[Any]): (BigInt, ColumnStat) = {
    val leftKeyStat = leftStats.attributeStats(leftKey)
    val rightKeyStat = rightStats.attributeStats(rightKey)
    val maxNdv = leftKeyStat.distinctCount.get.max(rightKeyStat.distinctCount.get)
    // Compute cardinality by the basic formula.
    val card = BigDecimal(leftStats.rowCount.get * rightStats.rowCount.get) / BigDecimal(maxNdv)

    // Get the intersected column stat.
    val newNdv = Some(leftKeyStat.distinctCount.get.min(rightKeyStat.distinctCount.get))
    val newMaxLen = if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
      Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
    } else {
      None
    }
    val newAvgLen = if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
      Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
    } else {
      None
    }
    val newStats = ColumnStat(newNdv, min, max, Some(0), newAvgLen, newMaxLen)

    (ceil(card), newStats)
  }

  /** Compute join cardinality using equi-height histograms. */
  private def computeByHistogram(
      leftKey: AttributeReference,
      rightKey: AttributeReference,
      leftHistogram: Histogram,
      rightHistogram: Histogram,
      newMin: Option[Any],
      newMax: Option[Any]): (BigInt, ColumnStat) = {
    val overlappedRanges = getOverlappedRanges(
      leftHistogram = leftHistogram,
      rightHistogram = rightHistogram,
      // Only numeric values have equi-height histograms.
      lowerBound = newMin.get.toString.toDouble,
      upperBound = newMax.get.toString.toDouble)

    var card: BigDecimal = 0
    var totalNdv: Double = 0
    for (i <- overlappedRanges.indices) {
      val range = overlappedRanges(i)
      if (i == 0 || range.hi != overlappedRanges(i - 1).hi) {
        // If range.hi == overlappedRanges(i - 1).hi, that means the current range has only one
        // value, and this value is already counted in the previous range. So there is no need to
        // count it in this range.
        totalNdv += math.min(range.leftNdv, range.rightNdv)
      }
      // Apply the formula in this overlapped range.
      card += range.leftNumRows * range.rightNumRows / math.max(range.leftNdv, range.rightNdv)
    }

    val leftKeyStat = leftStats.attributeStats(leftKey)
    val rightKeyStat = rightStats.attributeStats(rightKey)
    val newMaxLen = if (leftKeyStat.maxLen.isDefined && rightKeyStat.maxLen.isDefined) {
      Some(math.min(leftKeyStat.maxLen.get, rightKeyStat.maxLen.get))
    } else {
      None
    }
    val newAvgLen = if (leftKeyStat.avgLen.isDefined && rightKeyStat.avgLen.isDefined) {
      Some((leftKeyStat.avgLen.get + rightKeyStat.avgLen.get) / 2)
    } else {
      None
    }
    val newStats = ColumnStat(Some(ceil(totalNdv)), newMin, newMax, Some(0), newAvgLen, newMaxLen)
    (ceil(card), newStats)
  }

  /**
   * Propagate or update column stats for output attributes.
   */
  private def updateOutputStats(
      outputRows: BigInt,
      output: Seq[Attribute],
      oldAttrStats: AttributeMap[ColumnStat],
      keyStatsAfterJoin: AttributeMap[ColumnStat]): Seq[(Attribute, ColumnStat)] = {
    val outputAttrStats = new ArrayBuffer[(Attribute, ColumnStat)]()
    val leftRows = leftStats.rowCount.get
    val rightRows = rightStats.rowCount.get

    output.foreach { a =>
      // check if this attribute is a join key
      if (keyStatsAfterJoin.contains(a)) {
        outputAttrStats += a -> keyStatsAfterJoin(a)
      } else {
        val oldColStat = oldAttrStats(a)
        val oldNumRows = if (join.left.outputSet.contains(a)) {
          leftRows
        } else {
          rightRows
        }
        val newColStat = oldColStat.updateCountStats(oldNumRows, outputRows)
        // TODO: support nullCount updates for specific outer joins
        outputAttrStats += a -> newColStat
      }
    }
    outputAttrStats.toSeq
  }

  private def extractJoinKeysWithColStats(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Seq[(AttributeReference, AttributeReference)] = {
    leftKeys.zip(rightKeys).collect {
      // Currently we don't deal with equal joins like key1 = key2 + 5.
      // Note: join keys from EqualNullSafe also fall into this case (Coalesce), consider to
      // support it in the future by using `nullCount` in column stats.
      case (lk: AttributeReference, rk: AttributeReference)
        if columnStatsWithCountsExist((leftStats, lk), (rightStats, rk)) => (lk, rk)
    }
  }

  private def estimateLeftSemiAntiJoin(): Option[Statistics] = {
    // TODO: It's error-prone to estimate cardinalities for LeftSemi and LeftAnti based on basic
    // column stats. Now we just propagate the statistics from left side. We should do more
    // accurate estimation when advanced stats (e.g. histograms) are available.
    if (rowCountsExist(join.left)) {
      val leftStats = GpuStatsPlanVisitor.visit(join.left)
      // Propagate the original column stats for cartesian product
      val outputRows = leftStats.rowCount.get
      Some(Statistics(
        sizeInBytes = getOutputSize(join.output, outputRows, leftStats.attributeStats),
        rowCount = Some(outputRows),
        attributeStats = leftStats.attributeStats))
    } else {
      None
    }
  }

  /**
   * Experimental / hacky POC code. Will re-implement if/when we prove this is a good approach.
   *
   * Try and estimate number of shuffles based on changes in partitioning (join keys) between joins
   */
  def countShuffles(plan: LogicalPlan): Int = {

    def countShufflesInner(
        indent: String,
        plan: LogicalPlan,
        joinKey: Option[(String, String)]): Int = {
      plan match {
        case j: Join if j.joinType == Inner || j.joinType == LeftSemi =>
          val ret = j.condition match {
            case Some(EqualTo(AttributeReference(l, _, _, _), AttributeReference(r, _, _, _))) =>
              val numShuffle = joinKey match {
                case Some((ll, rr)) =>
                  var count = 2
                  if ((l == ll) || (l == rr)) {
                    count -= 1
                  }
                  if ((r == ll) || (r == rr)) {
                    count -= 1
                  }
                  count
                case _ =>
                  0
              }
              numShuffle + plan.children
                .map(child => countShufflesInner(indent + "  ", child, Some((l, r))))
                .sum
            case _ =>
              // complex join expression that we do not support in this logic yet
              // assume it is a shuffle
              1 + plan.children.map(child => countShufflesInner(indent + "  ", child, joinKey)).sum
          }
          println(s"${indent}Join: ${j.left.simpleStringWithNodeId()} ${j.joinType} " +
            s"${j.right.simpleStringWithNodeId()} ON ${j.condition} [$ret shuffles]")
          ret
        case _ =>
          val ret = plan.children
            .map(child => countShufflesInner(indent + "  ", child, joinKey))
            .sum
          println(s"${indent}${plan.getClass.getSimpleName} [$ret shuffles]")
          ret
      }
    }

    countShufflesInner("", plan, None)
  }

}
