/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.delta.RapidsRepartitionByFilePath.FILE_PATH_FIELD

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy, UnaryExecNode}
import org.apache.spark.sql.types.{LongType, StringType, StructField}
import org.apache.spark.sql.vectorized.ColumnarBatch

object RapidsRepartitionByFilePath {
  val ROW_ID_COL = "_metadata_row_id"
  val ROW_ID_FIELD = StructField(ROW_ID_COL, LongType, nullable = false)
  val FILE_PATH_COL = "_metadata_file_path"
  val FILE_PATH_FIELD = StructField(FILE_PATH_COL, StringType, nullable = false)
}

case class RapidsRepartitionByFilePath(optNumPartition: Option[Int],
    child: LogicalPlan) extends UnaryNode {
  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)
}

/**
 * This class is used to repartition the input data based on the file path when doing low shuffle
 * merge.
 *
 * In low shuffle merge, when saving unmodified target table data, we need to do an anti-join
 * from the touched target files to modified row ids to get the untouched target data like
 * following condition
 * t.__metadata_file_path=m.__metadata_file_path AND (t.__metadata_row_id-m.__metadata_row_id=0)
 *
 * The query optimizer will generate a
 * [[org.apache.spark.sql.execution.joins.ShuffledHashJoinExec]] with forcing children from both
 * sides to meet satisfying [[HashPartitioning]](numPartition, `__metadata_file_path`). This usually
 * requires a [[org.apache.spark.sql.execution.exchange.ShuffleExchangeExec]] to shuffle
 * the data based on the file path. The scan of touched target table files already meets one file
 * per partition, so we can avoid the shuffle by a special kind of coalescing that repartition the
 * data based on the file path.
 *
 * @param optNumPartition Number of partitions to repartition the data, if not set, use the
 *                        numShufflePartitions in spark conf.
 * @param child           Input table scan with `__metadata_file_path` column.
 */
case class RapidsRepartitionByFilePathExec(optNumPartition: Option[Int],
    child: SparkPlan) extends UnaryExecNode {

  private lazy val numPartitions = optNumPartition.getOrElse(conf.numShufflePartitions)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("RapidsParquetFileScanExec should not be executed")
  }

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  override def outputPartitioning: Partitioning = {
    child.output.find(_.name == FILE_PATH_FIELD.name) match {
      case Some(attr) => HashPartitioning(Seq(attr), numPartitions)
      case None => throw new IllegalArgumentException(
        s"Cannot find ${FILE_PATH_FIELD.name} in the output of the child plan")
    }
  }
}

case class GpuRapidsRepartitionByFilePathExec(optNumPartition: Option[Int],
    gpuPartitionIdExpr: GpuExpression,
    child: SparkPlan) extends UnaryExecNode with GpuExec {
  private lazy val numPartitions = optNumPartition.getOrElse(conf.numShufflePartitions)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = {
    child.output.find(_.name == FILE_PATH_FIELD.name) match {
      case Some(attr) => HashPartitioning(Seq(attr), numPartitions)
      case None => throw new IllegalArgumentException(
        s"Cannot find ${FILE_PATH_FIELD.name} in the output of the child plan")
    }
  }

  // The same as what feeds us
  override def outputBatching: CoalesceGoal = GpuExec.outputBatching(child)

  protected override def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException(
    s"${getClass.getCanonicalName} does not support row-based execution")

  override protected def internalDoExecuteColumnar(): RDD[ColumnarBatch] = {
    val rdd = child.executeColumnar().cache()
    logInfo(
      s"""Repartitioning by file path with numPartitions=$numPartitions,
         |input partitions=${rdd.getNumPartitions}""".stripMargin)
    if (numPartitions == 1 && rdd.getNumPartitions < 1) {
      // Make sure we don't output an RDD with 0 partitions, when claiming that we have a
      // `SinglePartition`.
      new GpuCoalesceExec.EmptyRDDWithPartitions(sparkContext, numPartitions)
    } else {
      rdd.coalesce(numPartitions,
        shuffle = false,
        Some(HashPartitioningByFilePathCoalescer(gpuPartitionIdExpr)))
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

case class HashPartitioningByFilePathCoalescer(partitionIdExpr: GpuExpression)
  extends PartitionCoalescer with Logging {
  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    logInfo(s"""maxPartitions in HashPartitioningByFilePathCoalescer: $maxPartitions""")

    if (maxPartitions <= 0) {
      throw new IllegalArgumentException(s"Number of partitions ($maxPartitions) must be positive.")
    }
    val partitionGroups = new Array[PartitionGroup](maxPartitions)
    for (i <- 0 until maxPartitions) {
      partitionGroups(i) = new PartitionGroup()
    }

    parent.mapPartitionsWithIndex({
      case (parentRddPartitionId, iter) =>
        if (iter.hasNext) {
          val firstRow = iter.next().asInstanceOf[ColumnarBatch]
          val partitionId = partitionIdExpr.columnarEval(firstRow).copyToHost().getInt(0)
          Seq((parentRddPartitionId, partitionId)).iterator
        } else {
          Seq((parentRddPartitionId, 0)).iterator
        }
    }, true).collect().foreach({
      case (parentRddPartitionId, partitionId) =>
        partitionGroups(partitionId).partitions += parent.partitions(parentRddPartitionId)
    })

    logInfo(
      s"""partitionGroups in HashPartitioningByFilePathCoalescer:
         |${partitionGroups.map(pg => pg.partitions.mkString(",")).mkString("|")}""".stripMargin)

    partitionGroups
  }
}


object RapidsRepartitionByFilePathStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case RapidsRepartitionByFilePath(optNumPartition, child) =>
      RapidsRepartitionByFilePathExec(optNumPartition, planLater(child)) :: Nil
    case _ => Nil
  }
}

class RapidsRepartitionByFilePathExecMeta(
    plan: RapidsRepartitionByFilePathExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[RapidsRepartitionByFilePathExec](plan, conf, parent, rule) with Logging {

  override def convertToGpu(): GpuExec = {
    val partitionIdExpr = plan
      .outputPartitioning
      .asInstanceOf[HashPartitioning]
      .partitionIdExpression
    val gpuPartitionIdExpr = GpuOverrides.wrapExpr(partitionIdExpr, conf, Some(this)).convertToGpu()
    val boundExpr = GpuBindReferences.bindGpuReference(gpuPartitionIdExpr, plan.child.output)

    GpuRapidsRepartitionByFilePathExec(
      plan.optNumPartition,
      boundExpr,
      childPlans.head.convertIfNeeded())
  }
}