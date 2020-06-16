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

import ai.rapids.spark.GpuMetricNames.{DESCRIPTION_TOTAL_TIME, TOTAL_TIME}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{PartialMapperPartitionSpec, ShuffledRowRDD, ShufflePartitionSpec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.adaptive.{CustomShuffleReaderExecLike, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.rapids.execution.ShuffledBatchRDD
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper of shuffle query stage, which follows the given partition arrangement.
 *
 * @param child           It is usually `ShuffleQueryStageExec`, but can be the shuffle exchange
 *                        node during canonicalization.
 * @param partitionSpecs  The partition specs that defines the arrangement.
 * @param description     The string description of this shuffle reader.
 */
case class GpuCustomShuffleReaderExec(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec],
    description: String) extends UnaryExecNode with GpuExec with CustomShuffleReaderExecLike {

  override def supportsColumnar: Boolean = true

  override lazy val additionalMetrics: Map[String, SQLMetric] = Map(
    TOTAL_TIME -> SQLMetrics.createNanoTimingMetric(sparkContext, DESCRIPTION_TOTAL_TIME),
  )

  override def output: Seq[Attribute] = child.output
  override lazy val outputPartitioning: Partitioning = {
    // If it is a local shuffle reader with one mapper per task, then the output partitioning is
    // the same as the plan before shuffle.
    // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
    if (partitionSpecs.forall(_.isInstanceOf[PartialMapperPartitionSpec]) &&
        partitionSpecs.map(_.asInstanceOf[PartialMapperPartitionSpec].mapIndex).toSet.size ==
            partitionSpecs.length) {
      child match {
        case ShuffleQueryStageExec(_, s: ShuffleExchangeExec) =>
          s.child.outputPartitioning
        case ShuffleQueryStageExec(_, r @ ReusedExchangeExec(_, s: ShuffleExchangeExec)) =>
          s.child.outputPartitioning match {
            case e: Expression => r.updateAttr(e).asInstanceOf[Partitioning]
            case other => other
          }
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    } else {
      UnknownPartitioning(partitionSpecs.length)
    }
  }

  override def stringArgs: Iterator[Any] = Iterator(description)

  private var cachedShuffleRDD: RDD[ColumnarBatch] = null

  override protected def doExecute(): RDD[InternalRow] = {
    throw new IllegalStateException()
  }

  /**
   * Produces the result of the query as an `RDD[ColumnarBatch]` if [[supportsColumnar]] returns
   * true. By convention the executor that creates a ColumnarBatch is responsible for closing it
   * when it is no longer needed. This allows input formats to be able to reuse batches if needed.
   */
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    if (cachedShuffleRDD == null) {
      cachedShuffleRDD = child match {
        case stage: ShuffleQueryStageExec =>
          new ShuffledBatchRDD(
            stage.shuffle.shuffleDependencyColumnar, stage.shuffle.readMetrics ++ metrics,
              partitionSpecs.toArray) //TODO ???
        case _ =>
          throw new IllegalStateException("operating on canonicalization plan")
      }
    }
    cachedShuffleRDD

  }
}

