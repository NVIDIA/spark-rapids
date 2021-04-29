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

package org.apache.spark.sql.execution.python.rapids

import java.io.File

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.{AggregateInPandasExec, ArrowPythonRunner, HybridRowQueue}
import org.apache.spark.sql.rapids.execution.python.GpuPythonHelper
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils


class GpuAggregateInPandasExecMeta(
    aggPandas: AggregateInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[AggregateInPandasExec](aggPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  // Ignore the expressions since columnar way is not supported yet
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec =
    GpuAggregateInPandasExec(
      aggPandas.groupingExpressions,
      aggPandas.udfExpressions,
      aggPandas.resultExpressions,
      childPlans.head.convertIfNeeded()
    )
}

/*
 * This GpuAggregateInPandasExec aims at supporting running Pandas UDF code
 * on GPU at Python side.
 *
 * (Currently it will not run on GPU itself, since the columnar way is not implemented yet.)
 *
 */
case class GpuAggregateInPandasExec(
    groupingExpressions: Seq[NamedExpression],
    udfExpressions: Seq[PythonUDF],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryExecNode with GpuExec {

  override def supportsColumnar = false
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Columnar execution is not supported by $this yet")
  }

  // Most code is copied from AggregateInPandasExec, except two GPU related calls
  override val output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingExpressions.map(ShimLoader.getSparkShims.sortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val inputRDD = child.execute()

    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (allInputs.exists(_.semanticEquals(e))) {
          allInputs.indexWhere(_.semanticEquals(e))
        } else {
          allInputs += e
          dataTypes += e.dataType
          allInputs.length - 1
        }
      }.toArray
    }.toArray

    // Schema of input rows to the python runner
    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    })

    // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) iter else {
      val prunedProj = UnsafeProjection.create(allInputs, child.output)

      val grouped = if (groupingExpressions.isEmpty) {
        // Use an empty unsafe row as a place holder for the grouping key
        Iterator((new UnsafeRow(), iter))
      } else {
        GroupedIterator(iter, groupingExpressions, child.output)
      }.map { case (key, rows) =>
        (key, rows.map(prunedProj))
      }

      val context = TaskContext.get()

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), groupingExpressions.length)
      context.addTaskCompletionListener[Unit] { _ =>
        queue.close()
      }

      // Add rows to queue to join later with the result.
      val projectedRowIter = grouped.map { case (groupingKey, rows) =>
        queue.add(groupingKey.asInstanceOf[UnsafeRow])
        rows
      }

      // Start of GPU things
      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }
      // End of GPU things

      val columnarBatchIter = new ArrowPythonRunner(
        pyFuncs,
        PythonEvalType.SQL_GROUPED_AGG_PANDAS_UDF,
        argOffsets,
        aggInputSchema,
        sessionLocalTimeZone,
        pythonRunnerConf).compute(projectedRowIter, context.partitionId(), context)

      val joinedAttributes =
        groupingExpressions.map(_.toAttribute) ++ udfExpressions.map(_.resultAttribute)
      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(resultExpressions, joinedAttributes)

      columnarBatchIter.map(_.rowIterator.next()).map { aggOutputRow =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, aggOutputRow)
        resultProj(joinedRow)
      }
    }}
  }
}
