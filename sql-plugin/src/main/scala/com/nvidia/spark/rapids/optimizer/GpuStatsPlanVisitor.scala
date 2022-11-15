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

import org.apache.spark.internal.Logging
import org.apache.spark.rapids.shims.GpuShuffleExchangeExec
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.adaptive.{LogicalQueryStage, QueryStageExec}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * A [[LogicalPlanVisitor]] that computes the statistics for the cost-based optimizer.
 *
 * Copied from Spark's BasicStatsPlanVisitor and modified to call GPU versions of estimation logic.
 */
object GpuStatsPlanVisitor extends LogicalPlanVisitor[Statistics] with Logging {

  private val statsTag = new TreeNodeTag[Statistics]("rapids.stats")

  override def visit(p: LogicalPlan): Statistics = {
    val stats = p.getTagValue(statsTag) match {
      case Some(stats) =>
        stats
      case _ =>
        val stats = super.visit(p)
        p.setTagValue(statsTag, stats)
        stats
    }
    logDebug(s"${p.getClass.getSimpleName}: stats=$stats")
    stats
  }

  private def fallback(p: LogicalPlan): Statistics = default(p)

  override def default(p: LogicalPlan): Statistics = p match {
    case LogicalQueryStage(logicalPlan, physicalPlan) => physicalPlan match {
        case qs: QueryStageExec => qs.plan match {
          case e: GpuShuffleExchangeExec =>
            // access GPU stats directly
            logDebug(s"Using GPU stats for completed query stage: $physicalPlan")
            e.runtimeStatistics
          case _ =>
            logDebug(s"Falling back to Spark stats for completed query stage: $physicalPlan")
            inferRowCount(logicalPlan.schema, logicalPlan.stats) // fallback to Spark stats
        }
        case _ =>
          logDebug(s"Falling back to Spark stats for completed query stage: $physicalPlan")
          inferRowCount(logicalPlan.schema, logicalPlan.stats) // fallback to Spark stats
    }
    case _: LocalRelation =>
      // LocalRelation is inserted by PruneFilters when there is a predicate that is always false
      //TODO this maybe is no longer needed here
      Statistics(sizeInBytes = 0, rowCount = Some(0))
    case p: LogicalRelation =>
      val stats = inferRowCount(p.schema, p.computeStats())
      val relation = p.relation.asInstanceOf[HadoopFsRelation]
      logDebug(s"GpuStatsPlanVisitor " +
        s"rel=${relation.location.inputFiles.head} " +
        s"rowCount=${stats.rowCount}")
      stats
    case p: LeafNode =>
      inferRowCount(p.schema, p.computeStats())
    case _: LogicalPlan =>
      val stats = p.children.map(child => visit(child))
      val rowCount = if (stats.exists(_.rowCount.isEmpty)) {
        None
      } else {
        Some(stats.map(_.rowCount.get).filter(_ > 0L).product)
      }
      inferRowCount(p.schema, Statistics(sizeInBytes =
        stats.map(_.sizeInBytes).filter(_ > 0L).product, rowCount = rowCount))
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

  override def visitAggregate(p: Aggregate): Statistics = {
    GpuAggregateEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitDistinct(p: Distinct): Statistics = {
    val child = p.child
    visitAggregate(Aggregate(child.output, child.output, child))
  }

  override def visitExcept(p: Except): Statistics = fallback(p)

  override def visitExpand(p: Expand): Statistics = fallback(p)

  override def visitFilter(p: Filter): Statistics = {
    GpuFilterEstimation(p).estimate.getOrElse(fallback(p))
  }

  override def visitGenerate(p: Generate): Statistics = default(p)

  override def visitGlobalLimit(p: GlobalLimit): Statistics = fallback(p)

  override def visitIntersect(p: Intersect): Statistics = {
    val leftStats = default(p.left)
    val rightStats = default(p.right)
    val leftSize = leftStats.sizeInBytes
    val rightSize = rightStats.sizeInBytes
    if (leftSize < rightSize) {
      Statistics(sizeInBytes = leftSize, rowCount = leftStats.rowCount)
    } else {
      Statistics(sizeInBytes = rightSize, rowCount = rightStats.rowCount)
    }
  }

  override def visitJoin(p: Join): Statistics = {
    GpuJoinEstimation(p).estimate.getOrElse(fallback(p))
  }

  override def visitLocalLimit(p: LocalLimit): Statistics = fallback(p)

  override def visitPivot(p: Pivot): Statistics = default(p)

  override def visitProject(p: Project): Statistics = {
    GpuProjectEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitRepartition(p: Repartition): Statistics = fallback(p)

  override def visitRepartitionByExpr(p: RepartitionByExpression): Statistics = fallback(p)

  // TODO this does not exist until spark 3.3.x
  //override def visitRebalancePartitions(p: RebalancePartitions): Statistics = fallback(p)

  override def visitSample(p: Sample): Statistics = fallback(p)

  override def visitScriptTransform(p: ScriptTransformation): Statistics = default(p)

  override def visitUnion(p: Union): Statistics = {
    GpuUnionEstimation.estimate(p).getOrElse(fallback(p))
  }

  override def visitWindow(p: Window): Statistics = fallback(p)

  override def visitSort(p: Sort): Statistics = {
    GpuStatsPlanVisitor.visit(p.child)
  }

  override def visitTail(p: Tail): Statistics = {
    fallback(p)
  }

  override def visitWithCTE(p: WithCTE): Statistics = fallback(p)
}
