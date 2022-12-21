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

package com.nvidia.spark.rapids.optimizer

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, Expression, ExpressionSet, IsNotNull, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Join reordering rule based on the paper "Improving Join Reordering for Large Scale Distributed
 * Computing", available at https://ieeexplore.ieee.org/document/9378281.
 *
 * This optimizer rule is not specific to GPU and could potentially be contributed back to Apache
 * Spark.
 */
object FactDimensionJoinReorder 
    extends Rule[LogicalPlan] 
    with PredicateHelper 
    with Logging {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = new RapidsConf(plan.conf)
    if (!(conf.isSqlEnabled && conf.joinReorderingEnabled)) {
      return plan
    }
    val t0 = System.nanoTime()
    val reorderedJoin = plan.transformUp {
      case j@Join(_, _, Inner, Some(_), JoinHint.NONE) if isSupportedJoin(j) =>
        reorder(j, conf)
      case p @ Project(projectList, Join(_, _, Inner, Some(_), JoinHint.NONE))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        reorder(p, conf)
    }
    val t1 = System.nanoTime()
    val elapsedTimeMillis = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS)
    logDebug(s"FactDimensionJoinReorder took $elapsedTimeMillis millis")
    reorderedJoin
  }

  /**
   * Determine if the join is eligible for join reordering.
   */
  private def isSupportedJoin(plan: LogicalPlan): Boolean = plan match {
    case Join(l, r, Inner, Some(cond), JoinHint.NONE) =>
      splitConjunctivePredicates(cond).forall(isSupportedJoinCond) &&
        isSupportedJoin(l) &&
        isSupportedJoin(r)

    case Project(projectList, _) =>
      projectList.forall(_.isInstanceOf[AttributeReference])

    case _: LogicalRelation | _: Filter =>
      true

    case _ =>
      false
  }

  private def isSupportedJoinCond(cond: Expression): Boolean = {
    if (!cond.deterministic) {
      println(s"isSupportedJoinCond non-deterministic: $cond")
      return false
    }
    cond match {
      case b: BinaryExpression =>
        // comparison between two columns e.g. `l == r` or `l < r`
        b.left.isInstanceOf[AttributeReference] && b.right.isInstanceOf[AttributeReference]
      case _ => false
    }
  }

  private def reorder(
      plan: LogicalPlan,
      conf: RapidsConf): LogicalPlan = {
    logDebug(s"Attempt to reorder join:\n$plan")

    // unnest the join into a list of input relations and list of join conditions
    val (treeShape, joinInputs, joinConditions) = extractInnerJoins(plan)

    // this check is redundant since we would fail to extract statistics in the next step
    // if the relation contains joins, but this makes the check more explicit
    if (joinInputs.exists(containsJoin)) {
      logDebug("Failed to extract inner joins")
      return plan
    }

    // convert plans into relations with statistics
    val maybeRelations = joinInputs.map(Relation.apply)
    if (maybeRelations.exists(_.isEmpty)) {
      logDebug("Failed to extract statistics for all relations")
      return plan
    }
    val relations = maybeRelations.flatten

    // split into facts and dimensions
    val (facts, dims) = findFactDimRels(relations, conf.joinReorderingRatio)

    logDebug(s"Found ${facts.length} facts and ${dims.length} dims")
    if (dims.length < 2) {
      logDebug("Too few dim tables")
      return plan
    }
    if (facts.length > conf.joinReorderingMaxFact) {
      logDebug("Too many fact tables")
      return plan
    }

    // order the dimensions by size
    val dimsBySize = relationsOrdered(dims, conf.joinReorderingPreserveOrder,
      conf.joinReorderingFilterSelectivity)

    // copy the join conditions into a HashSet
    val conds = new mutable.HashSet[Expression]()
    joinConditions.foreach(e => conds += e)

    val dimLogicalPlans = dimsBySize.map(_.plan)

    val newPlan = if (facts.length == 1) {

      val shape = if (conf.joinReorderingPreserveShape) {
        treeShape
      } else {
        LeftDeep
      }

      // the single fact table case closely follows the design in the paper
      val (numJoins, join) = buildJoinTree(facts.head.plan, dimLogicalPlans, conds, shape)

      // check for dominant fact table (at least half of joins must be against fact table)
      if (numJoins < (relations.length-1)/2) {
        logDebug("Failed dominant fact table check")
        return plan
      }

      join

    } else {

      // the multiple fact table case is not covered by the design in the paper, so this is
      // an extension to that design

      // for now, we build the final tree as a bushy tree when we have multiple fact tables
      // but we may want to experiment more with this in a future version of the rule.
      // tracking issue: https://github.com/NVIDIA/spark-rapids/issues/7399

      // first we build one left-deep join tree for each fact table
      val factDimJoins = facts.map(f => buildJoinTree(f.plan,
        dimLogicalPlans, conds, LeftDeep))

      // sort so that fact tables with more joins appear earlier
      val sortedFactDimJoins = factDimJoins.sortBy(_._1).reverse.map(_._2)

      // now we join the fact-dim join trees together as a left-deep tree, but
      // this can result in the final plan being a bushy tree
      val (numJoins, newPlan) = buildJoinTree(sortedFactDimJoins.head,
        sortedFactDimJoins.drop(1), conds, LeftDeep)

      if (numJoins == factDimJoins.length - 1) {
        newPlan
      } else {
        logDebug("Could not join all fact-dim joins")
        plan
      }
    }

    if (conds.nonEmpty) {
      logDebug(s"could not apply all join conditions: $conds")
      return plan
    }

    // verify output is correct
    if (!plan.output.forall(attr => newPlan.output.contains(attr))) {
      logDebug(s"new plan is missing some expected output attributes:" +
        s"\nexpected: ${plan.output}" +
        s"\nactual: ${newPlan.output}")
      return plan
    }

    logDebug(s"NEW PLAN\n$newPlan")

    newPlan
  }

  /** Categorize relations as fact or dimension relations base on size */
  def findFactDimRels(relations: Seq[Relation], joinReorderingRatio: Double)
      : (Seq[Relation], Seq[Relation]) = {
    val largest = relations.map(_.size).max.toDouble
    val facts = new ListBuffer[Relation]()
    val dims = new ListBuffer[Relation]()
    for (rel <- relations) {
      if (rel.size.toDouble / largest <= joinReorderingRatio) {
        dims += rel
      } else {
        facts += rel
      }
    }
    (facts, dims)
  }

  /** Build a query that joins the fact table to the dimension tables */
  def buildJoinTree(
      fact: LogicalPlan,
      dims: Seq[LogicalPlan],
      conds: mutable.HashSet[Expression],
      shape: TreeShape): (Int, LogicalPlan) = {
    var plan = fact
    var numJoins = 0
    for (dim <- dims) {
      val left = if (shape == Bushy) { plan } else { fact }
      val joinConds = new ListBuffer[Expression]()
      for (cond <- conds) {
        cond match {
          case b: BinaryExpression => (b.left, b.right) match {
            case (l: AttributeReference, r: AttributeReference) =>
              if (left.output.exists(_.exprId == l.exprId) &&
                dim.output.exists(_.exprId == r.exprId)) {
                joinConds += cond
              } else if (left.output.exists(_.exprId == r.exprId) &&
                dim.output.exists(_.exprId == l.exprId)) {
                joinConds += cond
              }
            case _ =>
          }
          case _ =>
        }
      }
      if (joinConds.nonEmpty) {
        joinConds.foreach(conds.remove)
        val factDimJoinCond = joinConds.reduce(And)
        logDebug(s"join fact to dim on $factDimJoinCond")
        shape match {
          case LeftDeep | Bushy =>
            plan = Join(plan, dim, Inner, Some(factDimJoinCond), JoinHint.NONE)
          case RightDeep =>
            plan = Join(dim, plan, Inner, Some(factDimJoinCond), JoinHint.NONE)
        }
        numJoins += 1
      }
    }
    (numJoins, plan)
  }

  /**
   * Order a set of relations such that filtered relations are
   * ordered by size (smallest first) and unfiltered relations are kept
   * in the original user-defined order.
   *
   * If `joinReorderingPreserveOrder` is disabled then the unfiltered
   * relations are also sorted by size.
   *
   * The two lists are then combined by repeatedly inspecting the first relation
   * from each list and picking the smallest one.
   */
  def relationsOrdered(
      rels: Seq[Relation],
      joinReorderingPreserveOrder: Boolean,
      joinReorderingFilterSelectivity: Double): Seq[Relation]  = {
    val unfiltered = if (joinReorderingPreserveOrder) {
      // leave unfiltered dimensions in the user-specified order
      rels.filterNot(_.hasFilter)
    } else {
      // order by size (smallest first)
      rels.filterNot(_.hasFilter).sortBy(_.size)
    }

    // order filtered dimensions by size (smallest first)
    val filtered = rels.filter(_.hasFilter)
      .map(f => f.copy(size = (f.size * joinReorderingFilterSelectivity).toLong))
      .sortBy(_.size)

    // combine the two lists
    val dims = new ListBuffer[Relation]()
    var i = 0
    var j = 0
    while (i < filtered.length || j < unfiltered.length) {
      if (i < filtered.length && j < unfiltered.length) {
        if (filtered(i).size < unfiltered(j).size) {
          dims += filtered(i)
          i += 1
        } else {
          dims += unfiltered(j)
          j += 1
        }
      } else if (i < filtered.length) {
        dims += filtered(i)
        i += 1
      } else {
        dims += unfiltered(j)
        j += 1
      }
    }
    dims
  }

  /**
   * Extracts items of consecutive inner joins and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  def extractInnerJoins(plan: LogicalPlan): (TreeShape, Seq[LogicalPlan], ExpressionSet) = {

    var containsLeftJoin = false
    var containsRightJoin = false

    def _extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], ExpressionSet) = {
      plan match {
        case Join(left, right, Inner, Some(cond), JoinHint.NONE) =>
          if (containsJoin(left)) {
            containsLeftJoin = true
          }
          if (containsJoin(right)) {
            containsRightJoin = true
          }
          val (leftPlans, leftConditions) = _extractInnerJoins(left)
          val (rightPlans, rightConditions) = _extractInnerJoins(right)
          (leftPlans ++ rightPlans, leftConditions ++ rightConditions ++
            splitConjunctivePredicates(cond))
        case Project(projectList, j@Join(_, _, Inner, Some(_), JoinHint.NONE))
          if projectList.forall(_.isInstanceOf[Attribute]) =>
          _extractInnerJoins(j)
        case _ =>
          (Seq(plan), ExpressionSet())
      }
    }

    val (plans, conds) = _extractInnerJoins(plan)

    val treeShape = if (containsLeftJoin && containsRightJoin) {
      Bushy
    } else if (containsLeftJoin) {
      LeftDeep
    } else if (containsRightJoin) {
      RightDeep
    } else {
      // did not find any nested joins
      LeftDeep
    }

    (treeShape, plans, conds)
  }

  /** Determine if a plan contains a join operator */
  private def containsJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Join => true
      case _ => plan.children.exists(containsJoin)
    }
  }

}

sealed trait TreeShape
object LeftDeep extends TreeShape
object RightDeep extends TreeShape
object Bushy extends TreeShape

/**
 * Wrapper for logical plan with size.
 */
case class Relation(plan: LogicalPlan, size: Long) extends PredicateHelper {
  def hasFilter: Boolean = {
    def hasFilter(plan: LogicalPlan): Boolean = {
      plan match {
        case Filter(cond, _) =>
          // we ignore IsNotNull filters since they exist for all join keys in all relations
          !splitConjunctivePredicates(cond).forall(_.isInstanceOf[IsNotNull])
        case _ => plan.children.exists(hasFilter)
      }
    }
    hasFilter(plan)
  }

  override def toString: String = s"Relation [size=$size]:\n$plan"
}

object Relation extends Logging {

  def apply(plan: LogicalPlan): Option[Relation] = {
    getStats(plan).map(stats => Relation(plan, stats.rowCount.get.toLong))
  }

  /** Get statistics for underlying relation */
  def getStats(plan: LogicalPlan): Option[Statistics] = {
    plan match {
      case p: LogicalRelation =>
        Some(inferRowCount(p.schema, p.computeStats()))
      case other if other.children.length == 1 =>
        getStats(other.children.head)
      case _ =>
        None
    }
  }

  private def inferRowCount(schema: StructType, stats: Statistics): Statistics = {
    if (stats.rowCount.isDefined) {
      stats
    } else {
      var size = 0
      for (field <- schema.fields) {
        // estimate the size of one row based on schema
        val fieldSize = field.dataType match {
          case DataTypes.ByteType | DataTypes.BooleanType => 1
          case DataTypes.ShortType => 2
          case DataTypes.IntegerType | DataTypes.FloatType => 4
          case DataTypes.LongType | DataTypes.DoubleType => 8
          case DataTypes.StringType => 50
          case DataTypes.DateType | DataTypes.TimestampType => 8
          case _ => 20
        }
        size += fieldSize
      }
      val estimatedRowCount = Some(stats.sizeInBytes / size)
      new Statistics(stats.sizeInBytes, estimatedRowCount)
    }
  }
}