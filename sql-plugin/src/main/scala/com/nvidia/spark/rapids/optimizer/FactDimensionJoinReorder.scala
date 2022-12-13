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
    val originalShuffleCount = countShuffles(plan)
    val newShuffleCount = countShuffles(reorderedJoin)
    val newPlan = if (originalShuffleCount < newShuffleCount) {
      logDebug("FactDimensionJoinReorder join reordering introduced " +
        "extra shuffles, so reverting to original plan")
      plan
    } else {
      reorderedJoin
    }
    val t1 = System.nanoTime()
    val elapsedTimeMillis = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS)
    logDebug(s"FactDimensionJoinReorder took $elapsedTimeMillis millis")
    newPlan
  }

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
    // we may want to make this more restrictive in the future but mainly we are looking to
    // support comparisons between columns, such as '=', '<', and similar
    cond match {
      case b: BinaryExpression =>
        b.left.isInstanceOf[AttributeReference] && b.right.isInstanceOf[AttributeReference]
      case _ => false
    }
  }

  private def reorder(
      plan: LogicalPlan,
      conf: RapidsConf): LogicalPlan = {
    logDebug(s"FactDimensionJoinReorder: Attempt to reorder join:\n$plan")

    // unnest the join into a list of input relations and list of join conditions
    val (joinInputs, joinConditions) = extractInnerJoins(plan)

    // this check is redundant since we would fail to extract statistics in the next step
    // if the relation contains joins, but this makes the check more explicit
    if (joinInputs.exists(containsJoin)) {
      logDebug("FactDimensionJoinReorder: Failed to extract inner joins")
      return plan
    }

    // convert plans into relations with statistics
    val maybeRelations = joinInputs.map(Relation.apply)
    if (maybeRelations.exists(_.isEmpty)) {
      logDebug("FactDimensionJoinReorder: Failed to extract statistics for all relations")
      return plan
    }
    val relations = maybeRelations.flatten

    // split into facts and dimensions
    val largest = relations.map(_.size).max.toDouble
    val facts = new ListBuffer[Relation]()
    val dims = new ListBuffer[Relation]()
    for (rel <- relations) {
      if (rel.size.toDouble/largest <= conf.joinReorderingRatio) {
        dims += rel
      } else {
        facts += rel
      }
    }

    logDebug(s"FactDimensionJoinReorder: Found ${facts.length} facts and ${dims.length} dims")
    if (facts.length > conf.joinReorderingMaxFact) {
      logDebug("FactDimensionJoinReorder: Too many fact tables")
      return plan
    }

    // order the dimensions by size
    val dimsBySize = relationsOrdered(dims, conf)
    dimsBySize.foreach(dim => logDebug(s"[DIM] [SIZE=${dim.size}]:\n$dim"))

    // copy the join conditions into a HashSet
    val conds = new mutable.HashSet[Expression]()
    joinConditions.foreach(e => conds += e)

    val dimLogicalPlans = dimsBySize.map(_.plan)

    val newPlan = if (facts.length == 1) {
      // this is the use case described in the paper
      val (numJoins, join) = buildJoinTree(facts.head.plan, dimLogicalPlans, conds)

      // check for dominant fact table (at least half of joins must be against fact table)
      if (numJoins < (relations.length-1)/2) {
        logDebug("Failed dominant fact table check")
        return plan
      }

      join

    } else {
      // this code is an experimental extension to the paper where we
      // attempt to rewrite joins involving multiple fact tables

      // first we build one join tree for each fact table
      val factDimJoins = new ListBuffer[(Int, LogicalPlan)]()
      for (fact <- facts) {
        val x = buildJoinTree(fact.plan, dimLogicalPlans, conds)
        logDebug(s"[FACT-DIM JOIN] ${x._2}")
        factDimJoins += x
      }

      // sort so that fact tables with more joins appear earlier
      val sortedFactDimJoins = factDimJoins.sortBy(-_._1).map(_._2)

      // now we join the fact-dim join trees together
      buildJoinTree(sortedFactDimJoins.head, sortedFactDimJoins.drop(1), conds)._2
    }

    if (conds.nonEmpty) {
      logDebug(s"FactDimensionJoinReorder: could not apply all join conditions: $conds")
      return plan
    }

    // verify output is correct
    if (!plan.output.forall(attr => newPlan.output.contains(attr))) {
      logDebug(s"FactDimensionJoinReorder: new plan is missing some expected output attributes:" +
        s"\nexpected: ${plan.output}" +
        s"\nactual: ${newPlan.output}")
      return plan
    }

    logDebug(s"FactDimensionJoinReorder: NEW PLAN\n$newPlan")

    newPlan
  }

  private def buildJoinTree(
      fact: LogicalPlan,
      dims: Seq[LogicalPlan],
      conds: mutable.HashSet[Expression]): (Int, LogicalPlan) = {
    var plan = fact
    var numJoins = 0
    for (dim <- dims) {
      val joinConds = new ListBuffer[Expression]()
      for (cond <- conds) {
        cond match {
          case b: BinaryExpression => (b.left, b.right) match {
            case (l: AttributeReference, r: AttributeReference) =>
              if (fact.output.exists(_.exprId == l.exprId) &&
                dim.output.exists(_.exprId == r.exprId)) {
                joinConds += cond
              } else if (fact.output.exists(_.exprId == r.exprId) &&
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
        logDebug(s"FactDimensionJoinReorder: join fact to dim on $factDimJoinCond")
        plan = Join(plan, dim, Inner, Some(factDimJoinCond), JoinHint.NONE)
        numJoins += 1
      }
    }
    (numJoins, plan)
  }

  /**
   * Order a set of dimension relations such that filtered relations are
   * ordered by size (smallest first) and unfiltered relations are kept
   * in the original user-defined order. The two lists are then combined
   * by repeatedly inspecting the first relation from each list and picking
   * the smallest one.
   */
  def relationsOrdered(
      rels: Seq[Relation],
      conf: RapidsConf): Seq[Relation]  = {
    val unfiltered = if (conf.joinReorderingPreserveOrder) {
      // leave unfiltered dimensions in the user-specified order
      rels.filterNot(_.hasFilter)
    } else {
      // order by size (smallest first)
      rels.filterNot(_.hasFilter).sortBy(_.size)
    }
    unfiltered.foreach(rel => logDebug(s"[UNFILTERED] $rel"))

    // order filtered dimensions by size (smallest first)
    val filtered = rels.filter(_.hasFilter)
      .map(f => f.copy(size = (f.size * conf.joinReorderingFilterSelectivity).toLong))
      .sortBy(_.size)
    filtered.foreach(rel => logDebug(s"[FILTERED] $rel"))

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
    dims.foreach(rel => logDebug(s"[ORDERED DIM] $rel"))
    dims
  }

  /**
   * Extracts items of consecutive inner joins and join conditions.
   * This method works for bushy trees and left/right deep trees.
   */
  private def extractInnerJoins(plan: LogicalPlan): (Seq[LogicalPlan], ExpressionSet) = {
    //TODO add restriction that we only do this for left/right deep trees?
    plan match {
      case Join(left, right, Inner, Some(cond), JoinHint.NONE) =>
        val (leftPlans, leftConditions) = extractInnerJoins(left)
        val (rightPlans, rightConditions) = extractInnerJoins(right)
        (leftPlans ++ rightPlans, leftConditions ++ rightConditions ++
          splitConjunctivePredicates(cond))
      case Project(projectList, j@Join(_, _, Inner, Some(_), JoinHint.NONE))
        if projectList.forall(_.isInstanceOf[Attribute]) =>
        extractInnerJoins(j)
      case _ =>
        (Seq(plan), ExpressionSet())
    }
  }

  /** Determine if a plan contains a join operator */
  private def containsJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case _: Join => true
      case _ => plan.children.exists(containsJoin)
    }
  }

  /**
   * Estimate the number of shuffles for a given logical plan based on changes in join keys.
   */
  def countShuffles(plan: LogicalPlan): Int = {
    //TODO implement
    0
  }
}

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