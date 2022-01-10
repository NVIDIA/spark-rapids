/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import scala.annotation.tailrec
import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.TrampolineUtil
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, MapType}

// Spark 2.x - had to copy the GpuBaseAggregateMeta into each Hash and Sort Meta because no BaseAggregateExec class in Spark 2.x

class GpuHashAggregateMeta(
    val agg: HashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[HashAggregateExec](agg, conf, parent, rule) {

  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def tagPlanForGpu(): Unit = {
    // We don't support Arrays and Maps as GroupBy keys yet, even they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val arrayOrMapGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType]))
    if (arrayOrMapGroupings) {
      willNotWorkOnGpu("ArrayTypes or MapTypes in grouping expressions are not supported")
    }

    val dec128Grouping = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[DecimalType] &&
            dt.asInstanceOf[DecimalType].precision > GpuOverrides.DECIMAL64_MAX_PRECISION))
    if (dec128Grouping) {
      willNotWorkOnGpu("grouping by a 128-bit decimal value is not currently supported")
    }

    tagForReplaceMode()
  }

  /**
   * Tagging checks tied to configs that control the aggregation modes that are replaced.
   *
   * The rule of replacement is determined by `spark.rapids.sql.hashAgg.replaceMode`, which
   * is a string configuration consisting of AggregateMode names in lower cases connected by
   * &(AND) and |(OR). The default value of this config is `all`, which indicates replacing all
   * aggregates if possible.
   *
   * The `|` serves as the outer connector, which represents patterns of both sides are able to be
   * replaced. For instance, `final|partialMerge` indicates that aggregate plans purely in either
   * Final mode or PartialMerge mode can be replaced. But aggregate plans also contain
   * AggExpressions of other mode will NOT be replaced, such as: stage 3 of single distinct
   * aggregate who contains both Partial and PartialMerge.
   *
   * On the contrary, the `&` serves as the inner connector, which intersects modes of both sides
   * to form a mode pattern. The replacement only takes place for aggregate plans who have the
   * exact same mode pattern as what defined the rule. For instance, `partial&partialMerge` means
   * that aggregate plans can be only replaced if they contain AggExpressions of Partial and
   * contain AggExpressions of PartialMerge and don't contain AggExpressions of other modes.
   *
   * In practice, we need to combine `|` and `&` to form some sophisticated patterns. For instance,
   * `final&complete|final|partialMerge` represents aggregate plans in three different patterns are
   * GPU-replaceable: plans contain both Final and Complete modes; plans only contain Final mode;
   * plans only contain PartialMerge mode.
   */
  private def tagForReplaceMode(): Unit = {
    val aggPattern = agg.aggregateExpressions.map(_.mode).toSet
    val strPatternToReplace = conf.hashAggReplaceMode.toLowerCase

    if (aggPattern.nonEmpty && strPatternToReplace != "all") {
      val aggPatternsCanReplace = strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }
      if (!aggPatternsCanReplace.contains(aggPattern)) {
        val message = aggPattern.map(_.toString).mkString(",")
        willNotWorkOnGpu(s"Replacing mode pattern `$message` hash aggregates disabled")
      } else if (aggPattern == Set(Partial)) {
        // In partial mode, if there are non-distinct functions and multiple distinct functions,
        // non-distinct functions are computed using the First operator. The final result would be
        // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
        // the first batch computed and sent to CPU doesn't contain all the rows required to
        // compute non-distinct function(s), then Spark would consider that value as final result
        // (due to First). Fall back to CPU in this case.
        if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
          willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
              "functions are non-deterministic for non-distinct functions as it is " +
              "computed using First.")
        }
      }
    }

    if (!conf.partialMergeDistinctEnabled && aggPattern.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
          s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }
}

class GpuSortAggregateExecMeta(
    val agg: SortAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule) extends SparkPlanMeta[SortAggregateExec](agg, conf, parent, rule) {

  val groupingExpressions: Seq[BaseExprMeta[_]] =
    agg.groupingExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateExpressions: Seq[BaseExprMeta[_]] =
    agg.aggregateExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val aggregateAttributes: Seq[BaseExprMeta[_]] =
    agg.aggregateAttributes.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  val resultExpressions: Seq[BaseExprMeta[_]] =
    agg.resultExpressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] =
    groupingExpressions ++ aggregateExpressions ++ aggregateAttributes ++ resultExpressions

  override def tagPlanForGpu(): Unit = {
    // We don't support Arrays and Maps as GroupBy keys yet, even they are nested in Structs. So,
    // we need to run recursive type check on the structs.
    val arrayOrMapGroupings = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[ArrayType] || dt.isInstanceOf[MapType]))
    if (arrayOrMapGroupings) {
      willNotWorkOnGpu("ArrayTypes or MapTypes in grouping expressions are not supported")
    }

    val dec128Grouping = agg.groupingExpressions.exists(e =>
      TrampolineUtil.dataTypeExistsRecursively(e.dataType,
        dt => dt.isInstanceOf[DecimalType] &&
            dt.asInstanceOf[DecimalType].precision > GpuOverrides.DECIMAL64_MAX_PRECISION))
    if (dec128Grouping) {
      willNotWorkOnGpu("grouping by a 128-bit decimal value is not currently supported")
    }

    tagForReplaceMode()

    // Make sure this is the last check - if this is SortAggregate, the children can be sorts and we
    // want to validate they can run on GPU and remove them before replacing this with a
    // HashAggregate.  We don't want to do this if there is a first or last aggregate,
    // because dropping the sort will make them no longer deterministic.
    // In the future we might be able to pull the sort functionality into the aggregate so
    // we can sort a single batch at a time and sort the combined result as well which would help
    // with data skew.
    val hasFirstOrLast = agg.aggregateExpressions.exists { agg =>
      agg.aggregateFunction match {
        case _: First | _: Last => true
        case _ => false
      }
    }
    if (canThisBeReplaced && !hasFirstOrLast) {
      childPlans.foreach { plan =>
        if (plan.wrapped.isInstanceOf[SortExec]) {
          if (!plan.canThisBeReplaced) {
            willNotWorkOnGpu("one of the preceding SortExec's cannot be replaced")
          } else {
            plan.shouldBeRemoved("replacing sort aggregate with hash aggregate")
          }
        }
      }
    }
  }

  /**
   * Tagging checks tied to configs that control the aggregation modes that are replaced.
   *
   * The rule of replacement is determined by `spark.rapids.sql.hashAgg.replaceMode`, which
   * is a string configuration consisting of AggregateMode names in lower cases connected by
   * &(AND) and |(OR). The default value of this config is `all`, which indicates replacing all
   * aggregates if possible.
   *
   * The `|` serves as the outer connector, which represents patterns of both sides are able to be
   * replaced. For instance, `final|partialMerge` indicates that aggregate plans purely in either
   * Final mode or PartialMerge mode can be replaced. But aggregate plans also contain
   * AggExpressions of other mode will NOT be replaced, such as: stage 3 of single distinct
   * aggregate who contains both Partial and PartialMerge.
   *
   * On the contrary, the `&` serves as the inner connector, which intersects modes of both sides
   * to form a mode pattern. The replacement only takes place for aggregate plans who have the
   * exact same mode pattern as what defined the rule. For instance, `partial&partialMerge` means
   * that aggregate plans can be only replaced if they contain AggExpressions of Partial and
   * contain AggExpressions of PartialMerge and don't contain AggExpressions of other modes.
   *
   * In practice, we need to combine `|` and `&` to form some sophisticated patterns. For instance,
   * `final&complete|final|partialMerge` represents aggregate plans in three different patterns are
   * GPU-replaceable: plans contain both Final and Complete modes; plans only contain Final mode;
   * plans only contain PartialMerge mode.
   */
  private def tagForReplaceMode(): Unit = {
    val aggPattern = agg.aggregateExpressions.map(_.mode).toSet
    val strPatternToReplace = conf.hashAggReplaceMode.toLowerCase

    if (aggPattern.nonEmpty && strPatternToReplace != "all") {
      val aggPatternsCanReplace = strPatternToReplace.split("\\|").map { subPattern =>
        subPattern.split("&").map {
          case "partial" => Partial
          case "partialmerge" => PartialMerge
          case "final" => Final
          case "complete" => Complete
          case s => throw new IllegalArgumentException(s"Invalid Aggregate Mode $s")
        }.toSet
      }
      if (!aggPatternsCanReplace.contains(aggPattern)) {
        val message = aggPattern.map(_.toString).mkString(",")
        willNotWorkOnGpu(s"Replacing mode pattern `$message` hash aggregates disabled")
      } else if (aggPattern == Set(Partial)) {
        // In partial mode, if there are non-distinct functions and multiple distinct functions,
        // non-distinct functions are computed using the First operator. The final result would be
        // incorrect for non-distinct functions for partition size > 1. Reason for this is - if
        // the first batch computed and sent to CPU doesn't contain all the rows required to
        // compute non-distinct function(s), then Spark would consider that value as final result
        // (due to First). Fall back to CPU in this case.
        if (AggregateUtils.shouldFallbackMultiDistinct(agg.aggregateExpressions)) {
          willNotWorkOnGpu("Aggregates of non-distinct functions with multiple distinct " +
              "functions are non-deterministic for non-distinct functions as it is " +
              "computed using First.")
        }
      }
    }

    if (!conf.partialMergeDistinctEnabled && aggPattern.contains(PartialMerge)) {
      willNotWorkOnGpu("Replacing Partial Merge aggregates disabled. " +
          s"Set ${conf.partialMergeDistinctEnabled} to true if desired")
    }
  }
}

// SPARK 2.x we can't check for the TypedImperativeAggregate properly so don't say we do the ObjectHashAggregate
/*
class GpuObjectHashAggregateExecMeta(
    override val agg: ObjectHashAggregateExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _]],
    rule: DataFromReplacementRule)
    extends GpuTypedImperativeSupportedAggregateExecMeta(agg,
      agg.requiredChildDistributionExpressions, conf, parent, rule)

*/
