/*
 * Copyright (c) 2020-2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python.spark310db

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf
import ai.rapids.cudf.{Aggregation, OrderByArg}
import ai.rapids.cudf.Aggregation.NullPolicy
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.GpuAggregateExpression
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class GpuWindowInPandasExecMetaBase(
    winPandas: WindowInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[WindowInPandasExec](winPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  // todo - DATABRICKS completely removed windowExpress (https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/python/WindowInPandasExec.scala#L84), is projectList ok here?
  val windowExpressions: Seq[BaseExprMeta[NamedExpression]] =
    winPandas.projectList.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val partitionSpec: Seq[BaseExprMeta[Expression]] =
    winPandas.partitionSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  val orderSpec: Seq[BaseExprMeta[SortOrder]] =
    winPandas.orderSpec.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  // Same check with that in GpuWindowExecMeta
  override def tagPlanForGpu(): Unit = {
    // Implementation depends on receiving a `NamedExpression` wrapped WindowExpression.
    windowExpressions.map(meta => meta.wrapped)
      .filter(expr => !expr.isInstanceOf[NamedExpression])
      .foreach(_ => willNotWorkOnGpu(because = "Unexpected query plan with Windowing" +
        " Pandas UDF; cannot convert for GPU execution. " +
        "(Detail: WindowExpression not wrapped in `NamedExpression`.)"))

    // Early check for the frame type, only supporting RowFrame for now, which is different from
    // the node GpuWindowExec.
    windowExpressions
      .flatMap(meta => meta.wrapped.collect { case e: SpecifiedWindowFrame => e })
      .filter(swf => swf.frameType.equals(RangeFrame))
      .foreach(rf => willNotWorkOnGpu(because = s"Only support RowFrame for now," +
        s" but found ${rf.frameType}"))
  }
}

/**
 * This iterator will group the rows in the incoming batches per the window
 * "partitionBy" specification to make sure each group goes into only one batch, and
 * each batch contains only one group data.
 * @param wrapped the incoming ColumnarBatch iterator.
 * @param partitionSpec the partition specification of the window expression for this node.
 */
class GroupingIterator(
    wrapped: Iterator[ColumnarBatch],
    partitionSpec: Seq[Expression],
    inputRows: GpuMetric,
    inputBatches: GpuMetric,
    spillCallback: RapidsBuffer.SpillCallback) extends Iterator[ColumnarBatch] with Arm {

  // Currently do it in a somewhat ugly way. In the future cuDF will provide a dedicated API.
  // Current solution assumes one group data exists in only one batch, so just split the
  // batch into multiple batches to make sure one batch contains only one group.
  private val groupBatches: mutable.Queue[SpillableColumnarBatch] = mutable.Queue.empty

  TaskContext.get().addTaskCompletionListener[Unit]{ _ =>
    groupBatches.foreach(_.close())
    groupBatches.clear()
  }

  override def hasNext(): Boolean = groupBatches.nonEmpty || wrapped.hasNext

  override def next(): ColumnarBatch = {
    if (groupBatches.nonEmpty) {
      groupBatches.dequeue().getColumnarBatch()
    } else {
      val batch = wrapped.next()
      inputBatches += 1
      inputRows += batch.numRows()
      if (batch.numRows() > 0 && batch.numCols() > 0 && partitionSpec.nonEmpty) {
        val partitionIndices = partitionSpec.indices
        // 1) Calculate the split indices via cudf.Table.groupBy and aggregation Count.
        //   a) Compute the count number for each group in a batch, including null values.
        //   b) Restore the original order (Ascending with NullsFirst) defined in the plan,
        //      since cudf.Table.groupBy will break the original order.
        //   c) The 'count' column can be used to calculate the indices for split.
        val cntTable = withResource(GpuProjectExec.project(batch, partitionSpec)) { projected =>
          withResource(GpuColumnVector.from(projected)) { table =>
            table
              .groupBy(partitionIndices:_*)
              .aggregate(Aggregation.count(NullPolicy.INCLUDE).onColumn(0))
          }
        }
        val orderedTable = withResource(cntTable) { table =>
          table.orderBy(partitionIndices.map(id => OrderByArg.asc(id, true)): _*)
        }
        val (countHostCol, numRows) = withResource(orderedTable) { table =>
          // Yes copying the data to host, it would be OK since just copying the aggregated
          // count column.
          (table.getColumn(table.getNumberOfColumns - 1).copyToHost(), table.getRowCount)
        }
        val splitIndices = withResource(countHostCol) { cntCol =>
          // Verified the type of Count column is integer, so use getInt
          // Also drop the last count value due to the split API definition.
          (0L until (numRows - 1)).map(id => cntCol.getInt(id))
        }.scan(0)(_+_).tail

        // 2) Split the table when needed
        val resultBatch = if (splitIndices.nonEmpty) {
          // More than one group, split it and close this incoming batch
          withResource(batch) { cb =>
            withResource(GpuColumnVector.from(cb)) { table =>
              withResource(table.contiguousSplit(splitIndices:_*)) { tables =>
                // Return the first one and enqueue others
                val splitBatches = tables.safeMap(table =>
                  GpuColumnVectorFromBuffer.from(table, GpuColumnVector.extractTypes(batch)))
                groupBatches.enqueue(splitBatches.tail.map(sb =>
                  SpillableColumnarBatch(sb, SpillPriorities.ACTIVE_ON_DECK_PRIORITY,
                    spillCallback)): _*)
                splitBatches.head
              }
            }
          }
        } else {
          // Only one group, return it directly
          batch
        }

        resultBatch
      } else {
        // Empty batch, or No partition defined for Window operation, return it directly.
        // When no partition is defined in window, there will be only one partition with one batch,
        // meaning one group.
        batch
      }
    }
  }
}
