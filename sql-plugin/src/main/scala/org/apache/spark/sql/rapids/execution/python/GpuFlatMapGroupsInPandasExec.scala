/*
 * Copyright (c) 2019-2020, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute,
  AttributeSet, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution,
  Distribution, Partitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.{ArrowPythonRunner, FlatMapGroupsInPandasExec}
import org.apache.spark.sql.execution.python.rapids.GpuPandasUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuFlatMapGroupsInPandasExecMeta(
    flatPandas: FlatMapGroupsInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[FlatMapGroupsInPandasExec](flatPandas, conf, parent, rule) {

  // Handle the child expressions(Python UDF) ourselves.
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec =
    GpuFlatMapGroupsInPandasExec(
      wrapped.groupingAttributes, wrapped.func, wrapped.output, wrapped.child
    )
}

/**
 * This is the GPU version of FlatMapGroupsInPandasExec
 *
 */
case class GpuFlatMapGroupsInPandasExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends SparkPlan with UnaryExecNode with GpuExec {

  override def supportsColumnar = false
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // TBD
    super.doExecuteColumnar()
  }

  // Most code is copied from MapInPandasExec, except two GPU related calls
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
  private val pandasFunction = func.asInstanceOf[PythonUDF].func
  private val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val (dedupAttributes, argOffsets) = resolveArgOffsets(child, groupingAttributes)

    // Map grouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) iter else {

      val data = groupAndProject(iter, groupingAttributes, child.output, dedupAttributes)
        .map { case (_, x) => x }

      // Start of GPU things
      GpuPythonHelper.injectGpuInfo(chainedFunc)
      PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      // End of GPU things

      val runner = new ArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        Array(argOffsets),
        StructType.fromAttributes(dedupAttributes),
        sessionLocalTimeZone,
        pythonRunnerConf)

      executePython(data, output, runner)
    }}
  }
}
