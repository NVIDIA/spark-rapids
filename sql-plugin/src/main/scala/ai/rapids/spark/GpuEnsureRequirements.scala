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
package ai.rapids.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec, ShuffleExchangeExecLike}
import org.apache.spark.sql.execution.joins.{HashedRelationBroadcastMode, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/**
 * Ensures that the [[org.apache.spark.sql.catalyst.plans.physical.Partitioning Partitioning]]
 * of input data meets the
 * [[org.apache.spark.sql.catalyst.plans.physical.Distribution Distribution]] requirements for
 * each operator by inserting [[ShuffleExchangeExec]] Operators where required.  Also ensure that
 * the input partition ordering requirements are met.
 */
case class GpuEnsureRequirements(conf: SQLConf) extends Rule[SparkPlan] {
  private def defaultNumPreShufflePartitions: Int =
    if (conf.adaptiveExecutionEnabled && conf.coalesceShufflePartitionsEnabled) {
      conf.initialShufflePartitionNum
    } else {
      conf.numShufflePartitions
    }

  private def semanticEqual(
      expr1: Expression,
      expr2: Expression,
      schema: StructType,
      outputSet: AttributeSet): Boolean = {
    (expr1, expr2) match {
      case (g: GpuAttributeReference, b: BoundReference) =>
        // TODO must be a better way to do this?
        val fieldName = schema.fields(b.ordinal).name
        val attr = outputSet.filter(_.name == fieldName).iterator.next()
        g.exprId == attr.exprId
      case (_: BoundReference, _: GpuAttributeReference) =>
        semanticEqual(expr2, expr1, schema, outputSet)
      case _ => expr1 == expr2
    }
  }

  private def ensureDistributionAndOrdering(operator: SparkPlan): SparkPlan = {
    val requiredChildDistributions: Seq[Distribution] = operator.requiredChildDistribution
    val requiredChildOrderings: Seq[Seq[SortOrder]] = operator.requiredChildOrdering
    var children: Seq[SparkPlan] = operator.children
    assert(requiredChildDistributions.length == children.length)
    assert(requiredChildOrderings.length == children.length)

    // Ensure that the operator's children satisfy their output distribution requirements.
    children = children.zip(requiredChildDistributions).map {
      case (child, distribution) if child.outputPartitioning.satisfies(distribution) =>
        child
      case (child, BroadcastDistribution(mode)) => (child.outputPartitioning, mode) match {
        //case (UnknownPartitioning(_), _) => BroadcastExchangeExec(mode, child)
        case (BroadcastPartitioning(HashedRelationBroadcastMode(key1)),
            HashedRelationBroadcastMode(key2)) =>
          if (key1.zip(key2).forall(x => semanticEqual(x._1, x._2, child.schema,
              child.outputSet))) {
            child
          } else {
            BroadcastExchangeExec(mode, child)
          }
        case _ =>
          BroadcastExchangeExec(mode, child)
      }
      case (child, distribution) =>
        val numPartitions = distribution.requiredNumPartitions
            .getOrElse(defaultNumPreShufflePartitions)
        ShuffleExchangeExec(distribution.createPartitioning(numPartitions), child)
    }

    // Get the indexes of children which have specified distribution requirements and need to have
    // same number of partitions.
    val childrenIndexes = requiredChildDistributions.zipWithIndex.filter {
      case (UnspecifiedDistribution, _) => false
      case (_: BroadcastDistribution, _) => false
      case _ => true
    }.map(_._2)

    val childrenNumPartitions =
      childrenIndexes.map(children(_).outputPartitioning.numPartitions).toSet

    if (childrenNumPartitions.size > 1) {
      // Get the number of partitions which is explicitly required by the distributions.
      val requiredNumPartitions = {
        val numPartitionsSet = childrenIndexes.flatMap {
          index => requiredChildDistributions(index).requiredNumPartitions
        }.toSet
        assert(numPartitionsSet.size <= 1,
          s"$operator have incompatible requirements of the number of partitions for its children")
        numPartitionsSet.headOption
      }

      // If there are non-shuffle children that satisfy the required distribution, we have
      // some tradeoffs when picking the expected number of shuffle partitions:
      // 1. We should avoid shuffling these children.
      // 2. We should have a reasonable parallelism.
      val nonShuffleChildrenNumPartitions =
      childrenIndexes.map(children).filterNot(_.isInstanceOf[ShuffleExchangeExec])
          .map(_.outputPartitioning.numPartitions)
      val expectedChildrenNumPartitions = if (nonShuffleChildrenNumPartitions.nonEmpty) {
        // Here we pick the max number of partitions among these non-shuffle children as the
        // expected number of shuffle partitions. However, if it's smaller than
        // `conf.numShufflePartitions`, we pick `conf.numShufflePartitions` as the
        // expected number of shuffle partitions.
        math.max(nonShuffleChildrenNumPartitions.max, conf.numShufflePartitions)
      } else {
        childrenNumPartitions.max
      }

      val targetNumPartitions = requiredNumPartitions.getOrElse(expectedChildrenNumPartitions)

      children = children.zip(requiredChildDistributions).zipWithIndex.map {
        case ((child, distribution), index) if childrenIndexes.contains(index) =>
          if (child.outputPartitioning.numPartitions == targetNumPartitions) {
            child
          } else {
            val defaultPartitioning = distribution.createPartitioning(targetNumPartitions)
            child match {
              // If child is an exchange, we replace it with a new one having defaultPartitioning.
              case s: ShuffleExchangeExecLike => s.withPartitioning(defaultPartitioning)
              case _ => ShuffleExchangeExec(defaultPartitioning, child)
            }
          }

        case ((child, _), _) => child
      }
    }

    // Now that we've performed any necessary shuffles, add sorts to guarantee output orderings:
    children = children.zip(requiredChildOrderings).map { case (child, requiredOrdering) =>
      // If child.outputOrdering already satisfies the requiredOrdering, we do not need to sort.
      if (SortOrder.orderingSatisfies(child.outputOrdering, requiredOrdering)) {
        child
      } else {
        SortExec(requiredOrdering, global = false, child = child)
      }
    }

    operator.withNewChildren(children)
  }

  private def reorder(
      leftKeys: IndexedSeq[Expression],
      rightKeys: IndexedSeq[Expression],
      expectedOrderOfKeys: Seq[Expression],
      currentOrderOfKeys: Seq[Expression]): (Seq[Expression], Seq[Expression]) = {
    if (expectedOrderOfKeys.size != currentOrderOfKeys.size) {
      return (leftKeys, rightKeys)
    }

    // Build a lookup between an expression and the positions its holds in the current key seq.
    val keyToIndexMap = mutable.Map.empty[Expression, mutable.BitSet]
    currentOrderOfKeys.zipWithIndex.foreach {
      case (key, index) =>
        keyToIndexMap.getOrElseUpdate(key.canonicalized, mutable.BitSet.empty).add(index)
    }

    // Reorder the keys.
    val leftKeysBuffer = new ArrayBuffer[Expression](leftKeys.size)
    val rightKeysBuffer = new ArrayBuffer[Expression](rightKeys.size)
    val iterator = expectedOrderOfKeys.iterator
    while (iterator.hasNext) {
      // Lookup the current index of this key.
      keyToIndexMap.get(iterator.next().canonicalized) match {
        case Some(indices) if indices.nonEmpty =>
          // Take the first available index from the map.
          val index = indices.firstKey
          indices.remove(index)

          // Add the keys for that index to the reordered keys.
          leftKeysBuffer += leftKeys(index)
          rightKeysBuffer += rightKeys(index)
        case _ =>
          // The expression cannot be found, or we have exhausted all indices for that expression.
          return (leftKeys, rightKeys)
      }
    }
    (leftKeysBuffer, rightKeysBuffer)
  }

  private def reorderJoinKeys(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      leftPartitioning: Partitioning,
      rightPartitioning: Partitioning): (Seq[Expression], Seq[Expression]) = {
    if (leftKeys.forall(_.deterministic) && rightKeys.forall(_.deterministic)) {
      (leftPartitioning, rightPartitioning) match {
        case (HashPartitioning(leftExpressions, _), _) =>
          reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, leftExpressions, leftKeys)
        case (_, HashPartitioning(rightExpressions, _)) =>
          reorder(leftKeys.toIndexedSeq, rightKeys.toIndexedSeq, rightExpressions, rightKeys)
        case _ =>
          (leftKeys, rightKeys)
      }
    } else {
      (leftKeys, rightKeys)
    }
  }

  /**
   * When the physical operators are created for JOIN, the ordering of join keys is based on order
   * in which the join keys appear in the user query. That might not match with the output
   * partitioning of the join node's children (thus leading to extra sort / shuffle being
   * introduced). This rule will change the ordering of the join keys to match with the
   * partitioning of the join nodes' children.
   */
  private def reorderJoinPredicates(plan: SparkPlan): SparkPlan = {
    plan match {
      case ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition, left, right) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        ShuffledHashJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, buildSide, condition,
          left, right)

      case SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, isPartial) =>
        val (reorderedLeftKeys, reorderedRightKeys) =
          reorderJoinKeys(leftKeys, rightKeys, left.outputPartitioning, right.outputPartitioning)
        SortMergeJoinExec(reorderedLeftKeys, reorderedRightKeys, joinType, condition,
          left, right, isPartial)

      case other => other
    }
  }

  def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    // TODO: remove this after we create a physical operator for `RepartitionByExpression`.
    case operator @ ShuffleExchangeExec(upper: HashPartitioning, child, _) =>
      child.outputPartitioning match {
        case lower: HashPartitioning if upper.semanticEquals(lower) => child
        case _ => operator
      }
    case operator: SparkPlan =>
      ensureDistributionAndOrdering(reorderJoinPredicates(operator))
  }
}

