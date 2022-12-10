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
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, BinaryExpression, Expression, ExpressionSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * Join reordering rule based on the paper "Improving Join Reordering for Large Scale Distributed
 * Computing", available at https://ieeexplore.ieee.org/document/9378281.
 *
 * This optimizer rule is not specific to GPU and could potentially be contributed back to Apache
 * Spark.
 */
object FactDimensionJoinReorder extends Rule[LogicalPlan] with PredicateHelper {

  private val alreadyOptimized = new TreeNodeTag[Boolean]("rapids.join.reordering")

  private var totalTimeMillis = 0L

  def apply(plan: LogicalPlan): LogicalPlan = {
    val conf = new RapidsConf(plan.conf)
    if (!conf.joinReorderingEnabled) {
      return plan
    }
    val ratio = conf.joinReorderingRatio
    val maxFact = conf.joinReorderingMaxFact
    val t0 = System.nanoTime()
    val reorderedJoin = plan.transformDown {
      case j@Join(_, _, Inner, Some(_), JoinHint.NONE) if isSupportedJoin(j) =>
        reorder(j, j.output, ratio, maxFact)
      //TODO add projection case back in here?
    }
    val originalShuffleCount = countShuffles(plan)
    val newShuffleCount = countShuffles(reorderedJoin)
    val newPlan = if (originalShuffleCount < newShuffleCount) {
      println("FactDimensionJoinReorder join reordering introduced " +
        "extra shuffles, so reverting to original plan")
      plan
    } else {
      reorderedJoin
    }
    val t1 = System.nanoTime()
    val elapsedTimeMillis = TimeUnit.MILLISECONDS.convert(t1 - t0, TimeUnit.NANOSECONDS)
    totalTimeMillis += elapsedTimeMillis
    println(s"FactDimensionJoinReorder took $elapsedTimeMillis millis ($totalTimeMillis total)")
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
      println(s"isSupportedJoin: unsupported: ${plan.getClass}")
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
      output: Seq[Attribute],
      ratio: Double,
      maxFact: Int): LogicalPlan = {
    if (plan.getTagValue(alreadyOptimized).isDefined) {
      return plan
    }

    println(s"FactDimensionJoinReorder: Attempt to reorder join:\n$plan")

    // unnest the join into a list of input relations and list of join conditions
    val (joinInputs, joinConditions) = extractInnerJoins(plan)

    // this check is redundant since we would fail to extract statistics in the next step
    // if the relation contains joins, but this makes the check more explicit
    if (joinInputs.exists(containsJoin)) {
      println("FactDimensionJoinReorder: Failed to extract inner joins")
      return plan
    }

    // convert plans into relations with statistics
    val maybeRelations = joinInputs.map(Relation.apply)
    if (maybeRelations.exists(_.isEmpty)) {
      println("FactDimensionJoinReorder: Failed to extract statistics for all relations")
      return plan
    }
    val relations = maybeRelations.flatten

    // split into facts and dimensions
    val largest = relations.map(_.size).max.toDouble
    val facts = new ListBuffer[Relation]()
    val dims = new ListBuffer[Relation]()
    for (rel <- relations) {
      if (rel.size.toDouble/largest <= ratio) {
        dims += rel
      } else {
        facts += rel
      }
    }

    println(s"FactDimensionJoinReorder: Found ${facts.length} facts and ${dims.length} dims")
    if (facts.length > maxFact) {
      println("FactDimensionJoinReorder: Too many fact tables")
      return plan
    }

    // TODO add dominant fact table check

    // order the dimensions by size
    val dimsBySize = relationsOrdered(dims)
    dimsBySize.foreach(dim => println(s"[DIM] [SIZE=${dim.size}]:\n$dim"))

    // copy the join conditions into a HashSet
    val conds = new mutable.HashSet[Expression]()
    joinConditions.foreach(e => {
      println(s"JOIN condition: $e")
      conds += e
    })

    val dimLogicalPlans = dimsBySize.map(_.plan)

    val newPlan = if (facts.length == 1) {
      // this is the use case described in the paper
      buildJoinTree(facts.head.plan, dimLogicalPlans, conds)
    } else {
      // this code is an experimental extension to the paper where we
      // attempt to rewrite joins involving multiple fact tables

      // first we build one join tree for each fact table
      val factDimJoins = new ListBuffer[LogicalPlan]()
      for (fact <- facts) {
        val plan = buildJoinTree(fact.plan, dimLogicalPlans, conds)
        println(s"[FACT-DIM JOIN] $plan")
        factDimJoins += plan
      }

      // now we join the fact-dim join trees together
      // TODO sort with most dominant facts first
      buildJoinTree(factDimJoins.head, factDimJoins.drop(1), conds)
    }

    val finalPlan = if (conds.isEmpty) {
      newPlan
    } else {
      // if there are any conditions left over, add them as a filter
      // TODO maybe this should be an error instead?
      val predicate = conds.reduce(And)
      println(s"FactDimensionJoinReorder: Adding filter: $predicate")
      Filter(predicate, newPlan)
    }

    // if we successfully reordered the join then tag the child plans so
    // that we don't attempt any more optimizations as we continue to transformDown
//    finalFinalPlan.transformDown {
//      case p =>
//        p.setTagValue(alreadyOptimized, true)
//        p
//    }

    println(s"FactDimensionJoinReorder: NEW PLAN\n$finalPlan")

    finalPlan
  }

  private def buildJoinTree(
      fact: LogicalPlan,
      dims: Seq[LogicalPlan],
      conds: mutable.HashSet[Expression]): LogicalPlan = {
    var plan = fact
    for (dim <- dims) {
      val joinConds = new ListBuffer[Expression]()
      for (cond <- conds) {
        cond match {
          case b: BinaryExpression =>
            val l = b.left.asInstanceOf[AttributeReference]
            val r = b.right.asInstanceOf[AttributeReference]
            if (fact.output.exists(_.exprId == l.exprId) &&
              dim.output.exists(_.exprId == r.exprId)) {
              joinConds += cond
            } else if (fact.output.exists(_.exprId == r.exprId) &&
              dim.output.exists(_.exprId == l.exprId)) {
              joinConds += cond
            }
          case _ =>
            //TODO ignore? error?
          println(s"FactDimensionJoinReorder: ignoring join cond: $cond")
        }
      }
      if (joinConds.nonEmpty) {
        joinConds.foreach(conds.remove)
        val factDimJoinCond = joinConds.reduce(And)
        println(s"FactDimensionJoinReorder: join fact to dim on $factDimJoinCond")
        plan = Join(plan, dim, Inner, Some(factDimJoinCond), JoinHint.NONE)
      }
    }
    plan
  }

  /**
   * Order a set of dimension relations such that filtered relations are
   * ordered by size (smallest first) and unfiltered relations are kept
   * in the original user-defined order. The two lists are then combined
   * by repeatedly inspecting the first relation from each list and picking
   * the smallest one.
   */
  def relationsOrdered(rels: Seq[Relation]): Seq[Relation]  = {
    // leave unfiltered dimensions in the user-specified order
    val unfiltered = rels.filterNot(_.hasFilter)
    // order filtered dimensions by size (smallest first)
    val filtered = rels.filter(_.hasFilter).sortBy(_.size)
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
case class Relation(plan: LogicalPlan, size: Long) {
  def hasFilter: Boolean = {
    def hasFilter(plan: LogicalPlan): Boolean = {
      plan match {
        case _: Filter => true
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