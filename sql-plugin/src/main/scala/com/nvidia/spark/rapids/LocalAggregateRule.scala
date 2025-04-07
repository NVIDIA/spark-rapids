/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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
package com.nvidia.spark.rapids

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate._

/**
 * Replace two-stage AggregateExecs with one-shot Complete one if it is a local Aggregate.
 * Local Aggregate is the concept of physical plan which means no need to do shuffle exchange to
 * redistribute data before final aggregate. The LocalAggregate may emerge under certain
 * circumstance, such as the BucketScan Spec fully matches the groupBy keys.
 */
object LocalAggregateRule extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case LocalAggregatePattern(finalAgg: BaseAggregateExec, _) =>
        val aggExpressions = finalAgg.aggregateExpressions.map(_.copy(mode = Complete))
        val aggAttributes = aggExpressions.map(_.resultAttribute)
        finalAgg match {
          case hash: HashAggregateExec =>
            hash.copy(
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = hash.child.children.head)
          case sort: SortAggregateExec =>
            sort.copy(
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = sort.child.children.head)
          case obj: ObjectHashAggregateExec =>
            obj.copy(
              aggregateExpressions = aggExpressions,
              aggregateAttributes = aggAttributes,
              child = obj.child.children.head)
          case _ =>
            throw new IllegalStateException(s"Unexpected AggregateExec: $finalAgg")
        }
    }
  }
}

/**
 * Capture the pattern of LocalAggregate, which consists of two-stage AggregateExecs being
 * connected with each other directly (without the ShuffleExchange in between):
 * +- FinalAggregateExec
 *    +- PartialAggregateExec
 *
 * The LocalAggregate can be emerged regardless HashAggregateExec, SortAggregateExec or
 * ObjectHashAggregateExec.
 */
object LocalAggregatePattern {
  def unapply(plan: SparkPlan): Option[(BaseAggregateExec, BaseAggregateExec)] = {
    plan match {
      case hashAgg: HashAggregateExec
        if hashAgg.child.isInstanceOf[HashAggregateExec] &&
          isLocalExchange(hashAgg, hashAgg.child.asInstanceOf[HashAggregateExec]) =>
        Some(hashAgg -> hashAgg.child.asInstanceOf[HashAggregateExec])
      case sort: SortAggregateExec
        if sort.child.isInstanceOf[SortAggregateExec] &&
          isLocalExchange(sort, sort.child.asInstanceOf[SortAggregateExec]) =>
        Some(sort -> sort.child.asInstanceOf[SortAggregateExec])
      case obj: ObjectHashAggregateExec
        if obj.child.isInstanceOf[ObjectHashAggregateExec] &&
          isLocalExchange(obj, obj.child.asInstanceOf[ObjectHashAggregateExec])  =>
        Some(obj -> obj.child.asInstanceOf[ObjectHashAggregateExec])
      case _ =>
        None
    }
  }

  // Check if the two connected AggregateExecs are two stages of the same logical Aggregate. If
  // so, group expressions and aggregate expressions should be identical(except AggregateMode)
  private def isLocalExchange(merge: BaseAggregateExec, partial: BaseAggregateExec): Boolean = {
    val groupExpressions = merge.groupingExpressions
    val aggExpressions = merge.aggregateExpressions
    val childGroupExpressions = partial.groupingExpressions
    val childAggExpressions = partial.aggregateExpressions
    // Fast path
    if (groupExpressions.length != childGroupExpressions.length ||
      aggExpressions.length != childAggExpressions.length) {
      return false
    }
    // Check AggregateExpressions
    if (!aggExpressions.zip(childAggExpressions).forall {
      case (s2, s1) =>
        s2.mode == Final && s1.mode == Partial &&
          !s2.isDistinct && !s1.isDistinct &&
          // it is okay to use equals rather than semanticEquals
          s2.aggregateFunction.equals(s1.aggregateFunction)
    }) {
      return false
    }
    // Check GroupExpressions
    if (!groupExpressions.zip(childGroupExpressions).forall {
      case (s2, s1) => s2.semanticEquals(s1.toAttribute)
    }) {
      return false
    }
    true
  }
}
