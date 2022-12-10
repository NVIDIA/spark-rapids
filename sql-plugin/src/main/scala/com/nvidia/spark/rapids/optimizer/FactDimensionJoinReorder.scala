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

import scala.collection.mutable.ListBuffer

import com.nvidia.spark.rapids.RapidsConf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BinaryExpression, Expression, ExpressionSet, PredicateHelper}
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
    val ratio = conf.joinReorderingRatio
    val t0 = System.nanoTime()
    val reorderedJoin = plan.transformDown {
      case j@Join(_, _, Inner, Some(_), JoinHint.NONE) if isSupportedJoin(j) =>
        reorder(j, j.output, ratio)
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

    case _: LogicalRelation =>
      //TODO add restriction on relations that have size available?
      true

    case Project(projectList, _) =>
      projectList.forall(_.isInstanceOf[AttributeReference])

    case _: Filter =>
      //TODO add restrictions on supported filters
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

  private def reorder(plan: LogicalPlan, output: Seq[Attribute], ratio: Double): LogicalPlan = {
    if (plan.getTagValue(alreadyOptimized).isDefined) {
      return plan
    }

    println(s"FactDimensionJoinReorder: Attempt to reorder join:\n$plan")

    // unnest the join into a list of input relations and list of join conditions
    val (joinInputs, joinConditions) = extractInnerJoins(plan)

    // temp debug logging
    joinInputs.foreach { rel =>
      println(s"JOIN input: ${rel.simpleStringWithNodeId()}:\n$rel")
    }
    joinConditions.foreach { cond =>
      println(s"JOIN condition: $cond")
    }

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

    // if we successfully reordered the join then tag the child plans so
    // that we don't attempt any more optimizations as we continue to transformDown
    plan.transformDown {
      case p =>
        p.setTagValue(alreadyOptimized, true)
        p
    }

    plan
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

// OLD POC CODE BELOW HERE

//println(s"countShuffles: $plan")
//    def _countShuffles(
//                        indent: String,
//                        plan: LogicalPlan,
//                        joinKey: Seq[Expression]): Int = {
//      val (x, newJoinKey) = plan match {
//        case j: Join if j.joinType == Inner || j.joinType == LeftSemi =>
//          j.condition match {
//            case Some(x) =>
//              val joinConds = extractJoinKeys(x)
//              //println(s"${indent}prev joinKey = $joinKey; " +
//              //                s"new joinConds = ${joinConds.mkString(",")}")
//
//              // TODO calculate num shuffles
//
//              (0, joinConds)
//            case _ =>
//              // complex join expression that we do not support in this logic yet
//              //println(s"${indent}prev joinKey = $joinKey; " +
//              //                s"new joinConds = ???")
//
//              (0, joinKey)
//          }
//        case _ =>
//          //println(s"${indent}prev joinKey = $joinKey; " +
//          //            s"op = ${plan.simpleStringWithNodeId()}")
//
//          (0, joinKey)
//      }
//      x + plan.children.map(child => _countShuffles(indent + "  ", child, newJoinKey)).sum
//    }
//
//    _countShuffles("", plan, Seq.empty)
//  }


//    //TODO avoid optimizing plans multiple times as we transformDown
////    if (plan.getTagValue(alreadyOptimized).isDefined) {
////      return plan
////    }
////    plan.setTagValue(alreadyOptimized, true)
//
//    //println(s"reorder: $plan")
//    val (items, conditions) = extractInnerJoins(plan)
//    val joinItems = items.map(Relation.apply)
//    val (fact, dims) = extractFactDimensionTables(joinItems, conditions, ratio)
//    val reorderedJoin = fact match {
//      case Some(fact) =>
//        // preserve order of unfiltered dimensions
//        val unfilteredDims = dims.filterNot(_.hasFilter)
//        // sort filtered dimensions by size (smallest first)
//        val filteredDims = dims.filter(_.hasFilter).sortBy(_.size)
//
//        // Merge both the lists of dimensions by giving user order
//        // the preference for tables without a selective predicate,
//        // whereas for tables with selective predicates giving preference
//        // to smaller tables. When comparing the top of both
//        // the lists, if size of the top table in the selective predicate
//        // list is smaller than top of the other list, choose it otherwise
//        // vice-versa.
//        // This algorithm is a greedy approach where smaller
//        // joins with filtered dimension table are preferred for execution
//        // earlier than other Joins to improve Join performance. We try to keep
//        // the user order intact when unsure about reordering to make sure
//        // regressions are minimized.
//        val result = new ListBuffer[Relation]()
//        var i = 0
//        var j = 0
//        while (i < filteredDims.size || j < unfilteredDims.size) {
//          if (i < filteredDims.size && j < unfilteredDims.size) {
//            if (filteredDims.head.size < unfilteredDims.head.size) {
//              result += filteredDims(i)
//              i += 1
//            } else {
//              result += unfilteredDims(j)
//              j += 1
//            }
//          } else if (i < filteredDims.size) {
//            result += filteredDims(i)
//            i += 1
//          } else if (j < unfilteredDims.size) {
//            result += unfilteredDims(j)
//            j += 1
//          }
//        }
//
//        val newPlan = buildJoinTree(fact, result, conditions)
//        println(s"FactDimensionJoinReorder NEW PLAN: $newPlan")
//        newPlan
//
//      case _ =>
//        println("FactDimensionJoinReorder No dominant fact table; aborting join reordering")
//        plan
//    }
//
//    replaceWithOrderedJoin(reorderedJoin)
//  }
//
//
//  def extractInnerJoins(plan: LogicalPlan): Unit = {
//
//  }
//
//  /**
//   * Build a list of all individual joins between tables.
//   */
//  def unnestJoins(plan: LogicalPlan): Seq[SimpleJoin] = {
//
//    def _unnestJoins(plan: LogicalPlan, joins: ListBuffer[SimpleJoin]) {
//
//      plan match {
//        case Join(l, r, Inner, Some(cond), JoinHint.NONE)
//            if isSupportedJoinCond(cond) =>
//
//          var leftPlan: Option[LogicalPlan] = None
//          var rightPlan: Option[LogicalPlan] = None
//
//          for (c <- splitConjunctivePredicates(cond)) {
//            val e = c.asInstanceOf[BinaryExpression]
//            val ll = e.left.asInstanceOf[AttributeReference]
//            val rr = e.right.asInstanceOf[AttributeReference]
//
//            resolveTablePlan(l, ll) match {
//              case Some(leftSubPlan) =>
//                resolveTablePlan(r, rr) match {
//                  case Some(rightSubPlan) =>
//                    leftPlan = Some(leftSubPlan)
//                    rightPlan = Some(rightSubPlan)
//                  case _ =>
//                }
//              case _ =>
//                resolveTablePlan(r, ll) match {
//                  case Some(leftSubPlan) =>
//                    resolveTablePlan(l, rr) match {
//                      case Some(rightSubPlan) =>
//                        leftPlan = Some(leftSubPlan)
//                        rightPlan = Some(rightSubPlan)
//                      case _ =>
//                    }
//                  case _ =>
//                }
//            }
//          }
//
//          // recurse into join inputs
//          _unnestJoins(l, joins)
//          _unnestJoins(r, joins)
//
//          // add the join to the list after recursing into the children so that we
//          // preserve the user-defined order of joins
//          if (leftPlan.isDefined && rightPlan.isDefined) {
//            joins += SimpleJoin(
//              Relation.apply(leftPlan.get),
//              Relation.apply(rightPlan.get),
//              cond
//            )
//          }
//
//        case _ =>
//          plan.children.foreach(child => _unnestJoins(child, joins))
//      }
//    }
//
//    val joins = new ListBuffer[SimpleJoin]()
//    _unnestJoins(plan, joins)
//    joins
//  }
//
//  /**
//   * Find the leaf sub-plan in a join that contains the relation referenced by the specific
//   * column (which is used in a join expression), along with any Filter or SubqueryAlias nodes.
//   *
//   * The returned plan should not contain any joins.
//   */
//  private def resolveTablePlan(plan: LogicalPlan, col: AttributeReference)
//      : Option[LogicalPlan] = {
//
//    def findRel(plan: LogicalPlan, col: AttributeReference): Option[LogicalPlan] = {
//      // TODO handle SubqueryAlias
//      plan match {
//        case LogicalRelation(_, output, _, _) =>
//          if (output.map(_.name).contains(col.name)) {
//            Some(plan)
//          } else {
//            None
//          }
//        case _ =>
//          for (child <- plan.children) {
//            findRel(child, col) match {
//              case Some(x) => return Some(x)
//              case _ =>
//            }
//          }
//          None
//      }
//    }
//
//    var candidatePlan = plan
//    while (true) {
//      findRel(candidatePlan, col) match {
//        case Some(_) =>
//          // keep recursing until we have a plan without a join
//          val join = findJoin(candidatePlan)
//          if (join.isDefined) {
//            val l = findRel(join.get.left, col)
//            if (l.isDefined) {
//              candidatePlan = l.get
//            } else {
//              val r = findRel(join.get.right, col)
//              candidatePlan = r.get
//            }
//          } else {
//            return Some(candidatePlan)
//          }
//        case _ =>
//          return None
//      }
//    }
//
//    // unreachable
//    throw new IllegalStateException()
//  }
//
//  /** find first (top-level) join in plan */
//  def findJoin(plan: LogicalPlan): Option[Join] = {
//    plan match {
//      case j: Join => Some(j)
//      case _ =>
//        for (child <- plan.children) {
//          val j = findJoin(child)
//          if (j.isDefined) {
//            return j
//          }
//        }
//        None
//    }
//  }
//
//
//  private def replaceWithOrderedJoin(plan: LogicalPlan): LogicalPlan = plan match {
//    case j@Join(left, right, jt: InnerLike, Some(cond), JoinHint.NONE) =>
//      val replacedLeft = replaceWithOrderedJoin(left)
//      val replacedRight = replaceWithOrderedJoin(right)
//      OrderedJoin(replacedLeft, replacedRight, jt, Some(cond))
//    case p@Project(_, j@Join(_, _, _: InnerLike, Some(_), JoinHint.NONE)) =>
//      p.copy(child = replaceWithOrderedJoin(j))
//    case _ =>
//      plan
//  }
//
//
//  private def extractJoinKeys(x: Expression): Seq[Expression] = {
//    splitConjunctivePredicates(x).filter {
//      case EqualTo(_: AttributeReference, _: AttributeReference) => true
//      case _ => false
//    }
//  }
//
//  /**
//   * Determine if this join is between a fact table and dimension tables.
//   *
//   * At least half of joins should be inner joins involving one common
//   * table (the fact table). Other tables being joined with fact table are considered
//   * dimension tables.
//   */
//  private def extractFactDimensionTables(
//      plans: Seq[Relation],
//      conditions: ExpressionSet,
//      ratio: Double): (Option[Relation], Seq[Relation]) = {
//
//    //println(s"extractFactDimensionTables called with ${plans.length} plans")
//
//    val facts = new ListBuffer[Relation]()
//    val dims = new ListBuffer[Relation]()
//    val largest = plans.map(_.size).max.toDouble
//    for (plan <- plans) {
//      if (plan.size.toDouble/largest < ratio) {
//        dims += plan
//      } else {
//        facts += plan
//      }
//    }
//
//    if (facts.length == 1) {
//      if (dims.length > 1) {
//        // count number of joins to the fact table
//        var count = 0
//        val outputNames = facts.head.plan.output.map(_.name)
//        for (condition <- conditions) {
//          condition match {
//            case b: BinaryExpression => (b.left, b.right) match {
//              case (l: AttributeReference, r: AttributeReference) =>
//                if (outputNames.contains(l.name) || outputNames.contains(r.name)) {
//                  count += 1
//                }
//              case _ =>
//                throw new IllegalStateException(
//                  s"extractFactDimensionTables unsupported expression: $condition")
//            }
//            case _ =>
//              throw new IllegalStateException(
//                s"extractFactDimensionTables unsupported expression: $condition")
//          }
//        }
//
//        val numJoins = plans.length - 1
//        if (count >= numJoins / 2) {
//          (facts.headOption, dims)
//        } else {
//          println("FactDimensionJoinReorder fact table was not " +
//            "involved in at least half the joins; aborting")
//          (None, Seq.empty)
//        }
//      } else {
//        println(s"FactDimensionJoinReorder wrong number of dim " +
//          s"tables (${facts.length}); aborting")
//        (None, Seq.empty)
//
//      }
//
//    } else {
//      println(s"FactDimensionJoinReorder wrong number of fact tables (${facts.length}); aborting")
//      (None, Seq.empty)
//    }
//  }
//
//  private def buildJoinTree(fact: Relation, dims: Seq[Relation], conditions: ExpressionSet)
//      : LogicalPlan = {
//    //println(s"buildJoinTree() fact=$fact, dims=\n${dims.mkString("\n")}")
//
//    var plan = fact.plan
//    val factAttributes = fact.plan.output.map(_.name)
//
//    val dimsIndirectJoin = new ListBuffer[Relation]()
//
//    for (dim <- dims) {
//      val dimAttributes = dim.plan.output.map(_.name)
//
//      val joinConditions = new ListBuffer[Expression]()
//      for (condition <- conditions) {
//        condition match {
//          case b: BinaryExpression => (b.left, b.right) match {
//            case (l: AttributeReference, r: AttributeReference) =>
//              //println(s"$l == $r")
//              val isFactJoin = factAttributes.contains(l.name) ||
//                factAttributes.contains(r.name)
//              val isDimJoin = dimAttributes.contains(l.name) ||
//                dimAttributes.contains(r.name)
//              if (isFactJoin && isDimJoin) {
//                joinConditions += condition
//              }
//            case _ =>
//              throw new IllegalStateException(
//                s"buildJoinTree unsupported expression: $condition")
//          }
//          case _ =>
//            throw new IllegalStateException(
//              s"buildJoinTree unsupported expression: $condition")
//        }
//      }
//
//      if (joinConditions.nonEmpty) {
//        val expression = joinConditions.reduce(And)
//        //println(s"join fact ${fact.name} to dim ${dim.name} on $expression")
//        plan = Join(plan, dim.plan, Inner, Some(expression), JoinHint.NONE)
//      } else {
//        // probably an indirect join via another table
//        //println(s"could not join fact ${fact.name} directly to dim ${dim.name}")
//        dimsIndirectJoin += dim
//      }
//    }
//
//    for (dim <- dimsIndirectJoin) {
//      val dimAttributes = dim.plan.output.map(_.name)
//
//      val joinConditions = new ListBuffer[Expression]()
//      for (condition <- conditions) {
//        condition match {
//          case b: BinaryExpression => (b.left, b.right) match {
//            case (l: AttributeReference, r: AttributeReference) =>
//              //println(s"$l == $r")
//              if (dimAttributes.contains(l.name) ||
//                dimAttributes.contains(r.name)) {
//                joinConditions += condition
//              }
//            case _ =>
//              throw new IllegalStateException(
//                s"buildJoinTree unsupported expression: $condition")
//          }
//          case _ =>
//            throw new IllegalStateException(
//              s"buildJoinTree unsupported expression: $condition")
//        }
//      }
//
//      if (joinConditions.nonEmpty) {
//        val expression = joinConditions.reduce(And)
//        //println(s"join to dim {dim.name} on $expression")
//        plan = Join(plan, dim.plan, Inner, Some(expression), JoinHint.NONE)
//      } else {
//        //println(s"joining to dim ${dim.name} without any join condition")
//        //plan = Join(plan, dim.plan, Inner, None, JoinHint.NONE)
//        throw new IllegalStateException("possible cartesian join")
//      }
//    }
//
//    // TODO check that all conditions were applied
//
//    plan
//  }
//
//}




