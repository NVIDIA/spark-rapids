/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.time.ZoneId

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, ComplexTypeMergingExpression, Expression, LambdaFunction, String2TrimExpression, TernaryExpression, UnaryExpression, WindowExpression, WindowFunction}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, ImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.window.WindowExecBase
import org.apache.spark.sql.types.DataType

trait DataFromReplacementRule {
  val operationName: String
  def incompatDoc: Option[String] = None
  def disabledMsg: Option[String] = None

  def confKey: String

  def getChecks: Option[TypeChecks[_]]
}

/**
 * A version of DataFromReplacementRule that is used when no replacement rule can be found.
 */
final class NoRuleDataFromReplacementRule extends DataFromReplacementRule {
  override val operationName: String = "NOT_FOUND"

  override def confKey = "NOT_FOUND"

  override def getChecks: Option[TypeChecks[_]] = None
}

/**
 * Holds metadata about a stage in the physical plan that is separate from the plan itself.
 * This is helpful in deciding when to replace part of the plan with a GPU enabled version.
 *
 * @param wrapped what we are wrapping
 * @param conf the config
 * @param parent the parent of this node, if there is one.
 * @param rule holds information related to the config for this object, typically this is the rule
 *          used to wrap the stage.
 * @tparam INPUT the exact type of the class we are wrapping.
 * @tparam BASE the generic base class for this type of stage, i.e. SparkPlan, Expression, etc.
 * @tparam OUTPUT when converting to a GPU enabled version of the plan, the generic base
 *                    type for all GPU enabled versions.
 */
abstract class RapidsMeta[INPUT <: BASE, BASE, OUTPUT <: BASE](
    val wrapped: INPUT,
    val conf: RapidsConf,
    val parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule) {

  /**
   * The wrapped plans that should be examined
   */
  val childPlans: Seq[SparkPlanMeta[_]]

  /**
   * The wrapped expressions that should be examined
   */
  val childExprs: Seq[BaseExprMeta[_]]

  /**
   * The wrapped scans that should be examined
   */
  val childScans: Seq[ScanMeta[_]]

  /**
   * The wrapped partitioning that should be examined
   */
  val childParts: Seq[PartMeta[_]]

  /**
   * The wrapped data writing commands that should be examined
   */
  val childDataWriteCmds: Seq[DataWritingCommandMeta[_]]

  /**
   * Convert what this wraps to a GPU enabled version.
   */
  def convertToGpu(): OUTPUT

  /**
   * Keep this on the CPU, but possibly convert its children under it to run on the GPU if enabled.
   * By default this just returns what is wrapped by this.  For some types of operators/stages,
   * like SparkPlan, each part of the query can be converted independent of other parts. As such in
   * a subclass this should be overridden to do the correct thing.
   */
  def convertToCpu(): BASE = wrapped

  private var cannotBeReplacedReasons: Option[mutable.Set[String]] = None
  private var mustBeReplacedReasons: Option[mutable.Set[String]] = None
  private var cannotReplaceAnyOfPlanReasons: Option[mutable.Set[String]] = None
  private var shouldBeRemovedReasons: Option[mutable.Set[String]] = None
  protected var cannotRunOnGpuBecauseOfSparkPlan: Boolean = false
  protected var cannotRunOnGpuBecauseOfCost: Boolean = false

  val gpuSupportedTag = TreeNodeTag[Set[String]]("rapids.gpu.supported")

  /**
   * Recursively force a section of the plan back onto CPU, stopping once a plan
   * is reached that is already on CPU.
   */
  final def recursiveCostPreventsRunningOnGpu(): Unit = {
    if (canThisBeReplaced && !mustThisBeReplaced) {
      costPreventsRunningOnGpu()
      childDataWriteCmds.foreach(_.recursiveCostPreventsRunningOnGpu())
    }
  }

  final def costPreventsRunningOnGpu(): Unit = {
    cannotRunOnGpuBecauseOfCost = true
    willNotWorkOnGpu("Removed by cost-based optimizer")
    childExprs.foreach(_.recursiveCostPreventsRunningOnGpu())
    childParts.foreach(_.recursiveCostPreventsRunningOnGpu())
    childScans.foreach(_.recursiveCostPreventsRunningOnGpu())
  }

  final def recursiveSparkPlanPreventsRunningOnGpu(): Unit = {
    cannotRunOnGpuBecauseOfSparkPlan = true
    childExprs.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childParts.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childScans.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    childDataWriteCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
  }

  final def recursiveSparkPlanRemoved(): Unit = {
    shouldBeRemoved("parent plan is removed")
    childExprs.foreach(_.recursiveSparkPlanRemoved())
    childParts.foreach(_.recursiveSparkPlanRemoved())
    childScans.foreach(_.recursiveSparkPlanRemoved())
    childDataWriteCmds.foreach(_.recursiveSparkPlanRemoved())
  }

  /**
   * Call this to indicate that this should not be replaced with a GPU enabled version
   * @param because why it should not be replaced.
   */
  final def willNotWorkOnGpu(because: String): Unit = {
    cannotBeReplacedReasons.get.add(because)
    // annotate the real spark plan with the reason as well so that the information is available
    // during query stage planning when AQE is on
    wrapped match {
      case p: SparkPlan =>
        p.setTagValue(gpuSupportedTag,
          p.getTagValue(gpuSupportedTag).getOrElse(Set.empty) + because)
      case _ =>
    }
  }

  final def mustBeReplaced(because: String): Unit = {
    mustBeReplacedReasons.get.add(because)
  }

  /**
   * Call this if there is a condition found that the entire plan is not allowed
   * to run on the GPU.
   */
  final def entirePlanWillNotWork(because: String): Unit = {
    cannotReplaceAnyOfPlanReasons.get.add(because)
  }

  final def shouldBeRemoved(because: String): Unit =
    shouldBeRemovedReasons.get.add(because)

  /**
   * Returns true if this node should be removed.
   */
  final def shouldThisBeRemoved: Boolean = shouldBeRemovedReasons.exists(_.nonEmpty)

  /**
   * Returns true iff this could be replaced.
   */
  final def canThisBeReplaced: Boolean = cannotBeReplacedReasons.exists(_.isEmpty)

  /**
   * Returns true iff this must be replaced because its children have already been
   * replaced and this needs to also be replaced for compatibility.
   */
  final def mustThisBeReplaced: Boolean = mustBeReplacedReasons.exists(_.nonEmpty)

  /**
   * Returns the list of reasons the entire plan can't be replaced. An empty
   * set means the entire plan is ok to be replaced, do the normal checking
   * per exec and children.
   */
  final def entirePlanExcludedReasons: Seq[String] = {
    cannotReplaceAnyOfPlanReasons.getOrElse(mutable.Set.empty).toSeq
  }

  /**
   * Returns true iff all of the expressions and their children could be replaced.
   */
  def canExprTreeBeReplaced: Boolean = childExprs.forall(_.canExprTreeBeReplaced)

  /**
   * Returns true iff all of the scans can be replaced.
   */
  def canScansBeReplaced: Boolean = childScans.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the partitioning can be replaced.
   */
  def canPartsBeReplaced: Boolean = childParts.forall(_.canThisBeReplaced)

  /**
   * Returns true iff all of the data writing commands can be replaced.
   */
  def canDataWriteCmdsBeReplaced: Boolean = childDataWriteCmds.forall(_.canThisBeReplaced)

  def confKey: String = rule.confKey
  final val operationName: String = rule.operationName
  final val incompatDoc: Option[String] = rule.incompatDoc
  def isIncompat: Boolean = incompatDoc.isDefined
  final val disabledMsg: Option[String] = rule.disabledMsg
  def isDisabledByDefault: Boolean = disabledMsg.isDefined

  def initReasons(): Unit = {
    cannotBeReplacedReasons = Some(mutable.Set[String]())
    mustBeReplacedReasons = Some(mutable.Set[String]())
    shouldBeRemovedReasons = Some(mutable.Set[String]())
    cannotReplaceAnyOfPlanReasons = Some(mutable.Set[String]())
  }

  /**
   * Tag all of the children to see if they are GPU compatible first.
   * Do basic common verification for the operators, and then call
   * [[tagSelfForGpu]]
   */
  final def tagForGpu(): Unit = {
    childScans.foreach(_.tagForGpu())
    childParts.foreach(_.tagForGpu())
    childExprs.foreach(_.tagForGpu())
    childDataWriteCmds.foreach(_.tagForGpu())
    childPlans.foreach(_.tagForGpu())

    initReasons()

    if (!conf.isOperatorEnabled(confKey, isIncompat, isDisabledByDefault)) {
      if (isIncompat && !conf.isIncompatEnabled) {
        willNotWorkOnGpu(s"the GPU version of ${wrapped.getClass.getSimpleName}" +
            s" is not 100% compatible with the Spark version. ${incompatDoc.get}. To enable this" +
            s" $operationName despite the incompatibilities please set the config" +
            s" $confKey to true. You could also set ${RapidsConf.INCOMPATIBLE_OPS} to true" +
            s" to enable all incompatible ops")
      } else if (isDisabledByDefault) {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled, and is disabled by default because ${disabledMsg.get}. Set $confKey" +
            s" to true if you wish to enable it")
      } else {
        willNotWorkOnGpu(s"the $operationName ${wrapped.getClass.getSimpleName} has" +
            s" been disabled. Set $confKey to true if you wish to enable it")
      }
    }

    tagSelfForGpu()
  }

  /**
   * Do any extra checks and tag yourself if you are compatible or not.  Be aware that this may
   * already have been marked as incompatible for a number of reasons.
   *
   * All of your children should have already been tagged so if there are situations where you
   * may need to disqualify your children for various reasons you may do it here too.
   */
  def tagSelfForGpu(): Unit

  private def indent(append: StringBuilder, depth: Int): Unit =
    append.append("  " * depth)

  def replaceMessage: String = "run on GPU"
  def noReplacementPossibleMessage(reasons: String): String = s"cannot run on GPU because $reasons"
  def suppressWillWorkOnGpuInfo: Boolean = false

  private def willWorkOnGpuInfo: String = cannotBeReplacedReasons match {
    case None => "NOT EVALUATED FOR GPU YET"
    case Some(v) if v.isEmpty &&
        (cannotRunOnGpuBecauseOfSparkPlan || shouldThisBeRemoved) => "could " + replaceMessage
    case Some(v) if v.isEmpty => "will " + replaceMessage
    case Some(v) =>
      noReplacementPossibleMessage(v mkString "; ")
  }

  private def willBeRemovedInfo: String = shouldBeRemovedReasons match {
    case None => ""
    case Some(v) if v.isEmpty => ""
    case Some(v) =>
      val reasons = v mkString "; "
      s" but is going to be removed because $reasons"
  }

  /**
   * When converting this to a string should we include the string representation of what this
   * wraps too?  This is off by default.
   */
  protected val printWrapped = false

  final private def getIndicatorChar: String = {
    if (shouldThisBeRemoved) {
      "#"
    } else if (cannotRunOnGpuBecauseOfCost) {
      "$"
    } else if (canThisBeReplaced) {
      if (cannotRunOnGpuBecauseOfSparkPlan) {
        "@"
      } else if (cannotRunOnGpuBecauseOfCost) {
        "$"
      } else {
        "*"
      }
    } else {
      "!"
    }
  }

  protected def checkTimeZoneId(timeZoneId: Option[String]): Unit = {
    timeZoneId.foreach { zoneId =>
      if (ZoneId.of(zoneId).normalized() != GpuOverrides.UTC_TIMEZONE_ID) {
        willNotWorkOnGpu(s"Only UTC zone id is supported. Actual zone id: $zoneId")
      }
    }
  }

  /**
   * Create a string representation of this in append.
   * @param strBuilder where to place the string representation.
   * @param depth how far down the tree this is.
   * @param all should all the data be printed or just what does not work on the GPU?
   */
  protected def print(strBuilder: StringBuilder, depth: Int, all: Boolean): Unit = {
    if ((all || !canThisBeReplaced || cannotRunOnGpuBecauseOfSparkPlan) &&
        !suppressWillWorkOnGpuInfo) {
      indent(strBuilder, depth)
      strBuilder.append(getIndicatorChar)

      strBuilder.append(operationName)
        .append(" <")
        .append(wrapped.getClass.getSimpleName)
        .append("> ")

      if (printWrapped) {
        strBuilder.append(wrapped)
          .append(" ")
      }

      strBuilder.append(willWorkOnGpuInfo).
        append(willBeRemovedInfo).
        append("\n")
    }
    printChildren(strBuilder, depth, all)
  }

  private final def printChildren(append: StringBuilder, depth: Int, all: Boolean): Unit = {
    childScans.foreach(_.print(append, depth + 1, all))
    childParts.foreach(_.print(append, depth + 1, all))
    childExprs.foreach(_.print(append, depth + 1, all))
    childDataWriteCmds.foreach(_.print(append, depth + 1, all))
    childPlans.foreach(_.print(append, depth + 1, all))
  }

  def explain(all: Boolean): String = {
    val appender = new StringBuilder()
    print(appender, 0, all)
    appender.toString()
  }

  override def toString: String = {
    explain(true)
  }
}

/**
 * Base class for metadata around `Partitioning`.
 */
abstract class PartMeta[INPUT <: Partitioning](part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Partitioning, GpuPartitioning](part, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override final def tagSelfForGpu(): Unit = {
    rule.getChecks.foreach(_.tag(this))
    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }
    tagPartForGpu()
  }

  def tagPartForGpu(): Unit = {}
}

/**
 * Metadata for Partitioning]] with no rule found
 */
final class RuleNotFoundPartMeta[INPUT <: Partitioning](
    part: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends PartMeta[INPUT](part, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagPartForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of partitioning ${part.getClass} could be found")
  }

  override def convertToGpu(): GpuPartitioning =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `Scan`.
 */
abstract class ScanMeta[INPUT <: Scan](scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Scan, Scan](scan, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}
}

/**
 * Metadata for `Scan` with no rule found
 */
final class RuleNotFoundScanMeta[INPUT <: Scan](
    scan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ScanMeta[INPUT](scan, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU enabled version of scan ${scan.getClass} could be found")
  }

  override def convertToGpu(): Scan =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `DataWritingCommand`.
 */
abstract class DataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends RapidsMeta[INPUT, DataWritingCommand, GpuDataWritingCommand](cmd, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override def tagSelfForGpu(): Unit = {}
}

/**
 * Metadata for `DataWritingCommand` with no rule found
 */
final class RuleNotFoundDataWritingCommandMeta[INPUT <: DataWritingCommand](
    cmd: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends DataWritingCommandMeta[INPUT](cmd, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagSelfForGpu(): Unit = {
    willNotWorkOnGpu(s"no GPU accelerated version of command ${cmd.getClass} could be found")
  }

  override def convertToGpu(): GpuDataWritingCommand =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Base class for metadata around `SparkPlan`.
 */
abstract class SparkPlanMeta[INPUT <: SparkPlan](plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, SparkPlan, GpuExec](plan, conf, parent, rule) {

  def tagForExplain(): Unit = {
    if (!canThisBeReplaced) {
      childExprs.foreach(_.recursiveSparkPlanPreventsRunningOnGpu)
      childParts.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childScans.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
      childDataWriteCmds.foreach(_.recursiveSparkPlanPreventsRunningOnGpu())
    }
    if (shouldThisBeRemoved) {
      childExprs.foreach(_.recursiveSparkPlanRemoved())
      childParts.foreach(_.recursiveSparkPlanRemoved())
      childScans.foreach(_.recursiveSparkPlanRemoved())
      childDataWriteCmds.foreach(_.recursiveSparkPlanRemoved())
    }
    childPlans.foreach(_.tagForExplain())
  }

  override val childPlans: Seq[SparkPlanMeta[SparkPlan]] =
    plan.children.map(GpuOverrides.wrapPlan(_, conf, Some(this)))
  override val childExprs: Seq[BaseExprMeta[_]] =
    plan.expressions.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  var cpuCost: Double = 0
  var gpuCost: Double = 0

  override def convertToCpu(): SparkPlan = {
    wrapped.withNewChildren(childPlans.map(_.convertIfNeeded()))
  }

  def getReasonsNotToReplaceEntirePlan: Seq[String] = {
    val childReasons = childPlans.flatMap(_.getReasonsNotToReplaceEntirePlan)
    entirePlanExcludedReasons ++ childReasons
  }

  // For adaptive execution we have to ensure we mark everything properly
  // the first time through and that has to match what happens when AQE
  // splits things up and does the subquery analysis at the shuffle boundaries.
  // If the AQE subquery analysis changes the plan from what is originally
  // marked we can end up with mismatches like happened in:
  // https://github.com/NVIDIA/spark-rapids/issues/1423
  // AQE splits subqueries at shuffle boundaries which means that it only
  // sees the children at that point. So in our fix up exchange we only
  // look at the children and mark is at will not work on GPU if the
  // child can't be replaced.
  private def fixUpExchangeOverhead(): Unit = {
    childPlans.foreach(_.fixUpExchangeOverhead())
    if (wrapped.isInstanceOf[ShuffleExchangeExec] &&
      !childPlans.exists(_.canThisBeReplaced) &&
        (plan.conf.adaptiveExecutionEnabled ||
        !parent.exists(_.canThisBeReplaced))) {
      willNotWorkOnGpu("Columnar exchange without columnar children is inefficient")
    }
  }

  /**
   * Run rules that happen for the entire tree after it has been tagged initially.
   */
  def runAfterTagRules(): Unit = {
    // In the first pass tagSelfForGpu will deal with each operator individually.
    // Children will be tagged first and then their parents will be tagged.  This gives
    // flexibility when tagging yourself to look at your children and disable yourself if your
    // children are not all on the GPU.  In some cases we need to be able to disable our
    // children too, or in this case run a rule that will disable operations when looking at
    // more of the tree.  These exceptions should be documented here.  We need to take special care
    // that we take into account all side-effects of these changes, because we are **not**
    // re-triggering the rules associated with parents, grandparents, etc.  If things get too
    // complicated we may need to update this to have something with triggers, but then we would
    // have to be very careful to avoid loops in the rules.

    // RULES:
    // 1) For ShuffledHashJoin and SortMergeJoin we need to verify that all of the exchanges
    // feeding them are either all on the GPU or all on the CPU, because the hashing is not
    // consistent between the two implementations. This is okay because it is only impacting
    // shuffled exchanges. So broadcast exchanges are not impacted which could have an impact on
    // BroadcastHashJoin, and shuffled exchanges are not used to disable anything downstream.
    fixUpExchangeOverhead()
  }

  override final def tagSelfForGpu(): Unit = {
    rule.getChecks.foreach(_.tag(this))

    if (!canExprTreeBeReplaced) {
      willNotWorkOnGpu("not all expressions can be replaced")
    }

    if (!canScansBeReplaced) {
      willNotWorkOnGpu("not all scans can be replaced")
    }

    if (!canPartsBeReplaced) {
      willNotWorkOnGpu("not all partitioning can be replaced")
    }

    if (!canDataWriteCmdsBeReplaced) {
      willNotWorkOnGpu("not all data writing commands can be replaced")
    }

    tagPlanForGpu()
  }

  /**
   * Called to verify that this plan will work on the GPU. Generic checks will have already been
   * done. In general this method should only tag this operator as bad.  If it needs to tag
   * one of its children please take special care to update the comment inside
   * `tagSelfForGpu` so we don't end up with something that could be cyclical.
   */
  def tagPlanForGpu(): Unit = {}

  /**
   * If this is enabled to be converted to a GPU version convert it and return the result, else
   * do what is needed to possibly convert the rest of the plan.
   */
  final def convertIfNeeded(): SparkPlan = {
    if (shouldThisBeRemoved) {
      if (childPlans.isEmpty) {
        throw new IllegalStateException("can't remove when plan has no children")
      } else if (childPlans.size > 1) {
        throw new IllegalStateException("can't remove when plan has more than 1 child")
      }
      childPlans.head.convertIfNeeded()
    } else {
      if (canThisBeReplaced) {
        convertToGpu()
      } else {
        convertToCpu()
      }
    }
  }
}

/**
 * Metadata for `SparkPlan` with no rule found
 */
final class RuleNotFoundSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of operator ${plan.getClass} could be found")

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

/**
 * Metadata for `SparkPlan` that should not be replaced or have any kind of warning for
 */
final class DoNotReplaceOrWarnSparkPlanMeta[INPUT <: SparkPlan](
    plan: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
    extends SparkPlanMeta[INPUT](plan, conf, parent, new NoRuleDataFromReplacementRule) {

  /** We don't want to spam the user with messages about these operators */
  override def suppressWillWorkOnGpuInfo: Boolean = true

  override def tagPlanForGpu(): Unit =
    willNotWorkOnGpu(s"there is no need to replace ${plan.getClass}")

  override def convertToGpu(): GpuExec =
    throw new IllegalStateException("Cannot be converted to GPU")
}

sealed abstract class ExpressionContext
object ProjectExprContext extends ExpressionContext {
  override def toString: String = "project"
}
object LambdaExprContext extends ExpressionContext  {
  override def toString: String = "lambda"
}
object GroupByAggExprContext extends ExpressionContext  {
  override def toString: String = "aggregation"
}
object ReductionAggExprContext extends ExpressionContext  {
  override def toString: String = "reduction"
}
object WindowAggExprContext extends ExpressionContext  {
  override def toString: String = "window"
}

object ExpressionContext {
  private[this] def findParentPlanMeta(meta: BaseExprMeta[_]): Option[SparkPlanMeta[_]] =
    meta.parent match {
      case Some(p: BaseExprMeta[_]) => findParentPlanMeta(p)
      case Some(p: SparkPlanMeta[_]) => Some(p)
      case _ => None
    }

  def getAggregateFunctionContext(meta: BaseExprMeta[_]): ExpressionContext = {
    val parent = findParentPlanMeta(meta)
    assert(parent.isDefined, "It is expected that an aggregate function is a child of a SparkPlan")
    parent.get.wrapped match {
      case _: WindowExecBase => WindowAggExprContext
      case agg: BaseAggregateExec =>
        if (agg.groupingExpressions.isEmpty) {
          ReductionAggExprContext
        } else {
          GroupByAggExprContext
        }
      case _ => throw new IllegalStateException(
        s"Found an aggregation function in an unexpected context $parent")
    }
  }

  def getRegularOperatorContext(meta: RapidsMeta[_, _, _]): ExpressionContext = meta.wrapped match {
    case _: LambdaFunction => LambdaExprContext
    case _: Expression if meta.parent.isDefined => getRegularOperatorContext(meta.parent.get)
    case _ => ProjectExprContext
  }
}

/**
 * Base class for metadata around `Expression`.
 */
abstract class BaseExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends RapidsMeta[INPUT, Expression, Expression](expr, conf, parent, rule) {

  override val childPlans: Seq[SparkPlanMeta[_]] = Seq.empty
  override val childExprs: Seq[BaseExprMeta[_]] =
    expr.children.map(GpuOverrides.wrapExpr(_, conf, Some(this)))
  override val childScans: Seq[ScanMeta[_]] = Seq.empty
  override val childParts: Seq[PartMeta[_]] = Seq.empty
  override val childDataWriteCmds: Seq[DataWritingCommandMeta[_]] = Seq.empty

  override val printWrapped: Boolean = true

  val ignoreUnsetDataTypes = false

  override def canExprTreeBeReplaced: Boolean =
    canThisBeReplaced && super.canExprTreeBeReplaced

  def dataType: DataType = expr.dataType

  lazy val context: ExpressionContext = expr match {
    case _: LambdaFunction => LambdaExprContext
    case _: WindowExpression => WindowAggExprContext
    case _: WindowFunction => WindowAggExprContext
    case _: AggregateFunction => ExpressionContext.getAggregateFunctionContext(this)
    case _: AggregateExpression => ExpressionContext.getAggregateFunctionContext(this)
    case _ => ExpressionContext.getRegularOperatorContext(this)
  }

  final override def tagSelfForGpu(): Unit = {
    if (wrapped.foldable && !GpuOverrides.isLit(wrapped)) {
      willNotWorkOnGpu(s"Cannot run on GPU. Is ConstantFolding excluded? Expression " +
        s"$wrapped is foldable and operates on non literals")
    }
    rule.getChecks.foreach(_.tag(this))
    tagExprForGpu()
  }

  /**
   * Called to verify that this expression will work on the GPU. For most expressions without
   * extra checks all of the checks should have already been done.
   */
  def tagExprForGpu(): Unit = {}
}

abstract class ExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends BaseExprMeta[INPUT](expr, conf, parent, rule) {

  override def convertToGpu(): GpuExpression
}

/**
 * Base class for metadata around `UnaryExpression`.
 */
abstract class UnaryExprMeta[INPUT <: UnaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs.head.convertToGpu())

  def convertToGpu(child: Expression): GpuExpression
}

/**
 * Base class for metadata around `AggregateFunction`.
 */
abstract class AggExprMeta[INPUT <: AggregateFunction](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs.head.convertToGpu())

  def convertToGpu(child: Expression): GpuExpression
}

/**
 * Base class for metadata around `ImperativeAggregate`.
 */
abstract class ImperativeAggExprMeta[INPUT <: ImperativeAggregate](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Base class for metadata around `BinaryExpression`.
 */
abstract class BinaryExprMeta[INPUT <: BinaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression = {
    val Seq(lhs, rhs) = childExprs.map(_.convertToGpu())
    convertToGpu(lhs, rhs)
  }

  def convertToGpu(lhs: Expression, rhs: Expression): GpuExpression
}

/**
 * Base class for metadata around `TernaryExpression`.
 */
abstract class TernaryExprMeta[INPUT <: TernaryExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression = {
    val Seq(child0, child1, child2) = childExprs.map(_.convertToGpu())
    convertToGpu(child0, child1, child2)
  }

  def convertToGpu(val0: Expression, val1: Expression,
                   val2: Expression): GpuExpression
}

abstract class String2TrimExpressionMeta[INPUT <: String2TrimExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
    extends ExprMeta[INPUT](expr, conf, parent, rule) {

  override final def convertToGpu(): GpuExpression = {
    val gpuCol :: gpuTrimParam = childExprs.map(_.convertToGpu())
    convertToGpu(gpuCol, gpuTrimParam.headOption)
  }

  def convertToGpu(column: Expression, target: Option[Expression] = None): GpuExpression
}

/**
 * Base class for metadata around `ComplexTypeMergingExpression`.
 */
abstract class ComplexTypeMergingExprMeta[INPUT <: ComplexTypeMergingExpression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends ExprMeta[INPUT](expr, conf, parent, rule) {
  override final def convertToGpu(): GpuExpression =
    convertToGpu(childExprs.map(_.convertToGpu()))

  def convertToGpu(childExprs: Seq[Expression]): GpuExpression
}

/**
 * Metadata for `Expression` with no rule found
 */
final class RuleNotFoundExprMeta[INPUT <: Expression](
    expr: INPUT,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]])
  extends ExprMeta[INPUT](expr, conf, parent, new NoRuleDataFromReplacementRule) {

  override def tagExprForGpu(): Unit =
    willNotWorkOnGpu(s"no GPU enabled version of expression ${expr.getClass} could be found")

  override def convertToGpu(): GpuExpression =
    throw new IllegalStateException("Cannot be converted to GPU")
}