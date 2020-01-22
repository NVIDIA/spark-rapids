/*
 * Copyright (c) 2020, NVIDIA CORPORATION.
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

package ai.rapids.spark

import ai.rapids.spark.RapidsPluginImplicits._
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, Partitioning}
import org.apache.spark.sql.execution.{LimitExec, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.mutable.ListBuffer

/**
 * Helper trait which defines methods that are shared by both
 * [[GpuLocalLimitExec]] and [[GpuGlobalLimitExec]].
 */
trait GpuBaseLimitExec extends LimitExec with GpuExec {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  protected override def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val crdd = child.executeColumnar()
    crdd.mapPartitions { cbIter =>
      new Iterator[ColumnarBatch] {
        var iterator = cbIter
        var remainingLimit = limit

        TaskContext.get().addTaskCompletionListener[Unit] { _ =>
          iterator = null
        }

        override def hasNext: Boolean = remainingLimit <= limit && iterator.hasNext

        override def next(): ColumnarBatch = {
          val batch = iterator.next()
          val resultCVs = new ListBuffer[GpuColumnVector]
          // if batch > remainingLimit, then slice and add to the resultBatches, break
          if (batch.numRows() > remainingLimit) {
            val table = GpuColumnVector.from(batch)
            val exception: Throwable = null
            try {
              for (i <- 0 until table.getNumberOfColumns) {
                val slices = table.getColumn(i).slice(0, remainingLimit)
                assert(slices.length > 0)
                resultCVs.append(GpuColumnVector.from(slices(0)))
              }
              remainingLimit = 0
              new ColumnarBatch(resultCVs.toArray, resultCVs(0).getRowCount.toInt)
            } finally {
              // close the table
              table.safeClose(exception)
              // close the batch
              batch.safeClose(exception)
            }
          } else {
            remainingLimit -= batch.numRows()
            batch
          }
        }
      }
    }
  }
}

/**
 * Take the first `limit` elements of each child partition, but do not collect or shuffle them.
 */
case class GpuLocalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec

/**
 * Take the first `limit` elements of the child's single output partition.
 */
case class GpuGlobalLimitExec(limit: Int, child: SparkPlan) extends GpuBaseLimitExec {
  override def requiredChildDistribution: List[Distribution] = AllTuples :: Nil
}