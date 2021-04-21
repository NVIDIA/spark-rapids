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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution,
  Distribution, Partitioning}
import org.apache.spark.sql.execution.{BinaryExecNode, CoGroupedIterator, SparkPlan}
import org.apache.spark.sql.execution.python.{CoGroupedArrowPythonRunner,
  FlatMapCoGroupsInPandasExec}
import org.apache.spark.sql.execution.python.rapids.GpuPandasUtils._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch


class GpuFlatMapCoGroupsInPandasExecMeta(
    flatPandas: FlatMapCoGroupsInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[FlatMapCoGroupsInPandasExec](flatPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  // Ignore the expressions since columnar way is not supported yet
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec = {
    val Seq(left, right) = childPlans.map(_.convertIfNeeded())
    GpuFlatMapCoGroupsInPandasExec(
      flatPandas.leftGroup, flatPandas.rightGroup,
      flatPandas.func,
      flatPandas.output,
      left,
      right
    )
  }
}

/*
 *
 * This GpuFlatMapCoGroupsInPandasExec aims at supporting running Pandas functional code
 * on GPU at Python side.
 *
 * (Currently it will not run on GPU itself, since the columnar way is not implemented yet.)
 *
 */
case class GpuFlatMapCoGroupsInPandasExec(
    leftGroup: Seq[Attribute],
    rightGroup: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    left: SparkPlan,
    right: SparkPlan)
  extends SparkPlan with BinaryExecNode with GpuExec {

  override def supportsColumnar = false
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Columnar execution is not supported by $this yet")
  }

  // Most code is copied from FlatMapCoGroupsInPandasExec, except two GPU related calls
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
  private val pandasFunction = func.asInstanceOf[PythonUDF].func
  private val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] = {
    val leftDist = if (leftGroup.isEmpty) AllTuples else ClusteredDistribution(leftGroup)
    val rightDist = if (rightGroup.isEmpty) AllTuples else ClusteredDistribution(rightGroup)
    leftDist :: rightDist :: Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    leftGroup.map(ShimLoader.getSparkShims.sortOrder(_, Ascending)) ::
      rightGroup.map(ShimLoader.getSparkShims.sortOrder(_, Ascending)) :: Nil
  }

  override protected def doExecute(): RDD[InternalRow] = {
    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)

    val (leftDedup, leftArgOffsets) = resolveArgOffsets(left, leftGroup)
    val (rightDedup, rightArgOffsets) = resolveArgOffsets(right, rightGroup)

    // Map cogrouped rows to ArrowPythonRunner results, Only execute if partition is not empty
    left.execute().zipPartitions(right.execute())  { (leftData, rightData) =>
      if (leftData.isEmpty && rightData.isEmpty) Iterator.empty else {

        val leftGrouped = groupAndProject(leftData, leftGroup, left.output, leftDedup)
        val rightGrouped = groupAndProject(rightData, rightGroup, right.output, rightDedup)
        val data = new CoGroupedIterator(leftGrouped, rightGrouped, leftGroup)
          .map { case (_, l, r) => (l, r) }

        // Start of GPU things
        if (isPythonOnGpuEnabled) {
          GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
          PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
        }
        // End of GPU things

        val runner = new CoGroupedArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_COGROUPED_MAP_PANDAS_UDF,
          Array(leftArgOffsets ++ rightArgOffsets),
          StructType.fromAttributes(leftDedup),
          StructType.fromAttributes(rightDedup),
          sessionLocalTimeZone,
          pythonRunnerConf)

        executePython(data, output, runner)
      }
    }
  }
}
