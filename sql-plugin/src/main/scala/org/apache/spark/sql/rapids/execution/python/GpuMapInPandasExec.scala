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

import ai.rapids.cudf
import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.rapids.execution.python.BatchGroupUtils.executePython
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuMapInPandasExecMeta(
    mapPandas: MapInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: DataFromReplacementRule)
  extends SparkPlanMeta[MapInPandasExec](mapPandas, conf, parent, rule) {

  override def replaceMessage: String = "partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  private val udf: BaseExprMeta[PythonUDF] = GpuOverrides.wrapExpr(
    mapPandas.func.asInstanceOf[PythonUDF], conf, Some(this))
  private val resultAttrs: Seq[BaseExprMeta[Attribute]] =
    mapPandas.output.map(GpuOverrides.wrapExpr(_, conf, Some(this)))

  override val childExprs: Seq[BaseExprMeta[_]] = resultAttrs :+ udf

  override def convertToGpu(): GpuExec =
    GpuMapInPandasExec(
      udf.convertToGpu(),
      resultAttrs.map(_.convertToGpu()).asInstanceOf[Seq[Attribute]],
      childPlans.head.convertIfNeeded()
    )
}

/*
 * A relation produced by applying a function that takes an iterator of pandas DataFrames
 * and outputs an iterator of pandas DataFrames.
 *
 * This GpuMapInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for its Python processes.
 *
 */
case class GpuMapInPandasExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with GpuPythonExecBase {

  private val pandasFunction = func.asInstanceOf[GpuPythonUDF].func

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val (mNumInputRows, mNumInputBatches, mNumOutputRows, mNumOutputBatches,
         spillCallback) = commonGpuMetrics()

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val pyInputTypes = child.schema
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    // Start process
    child.executeColumnar().mapPartitionsInternal { inputIter =>
      val context = TaskContext.get()

      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val pyInputSchema = StructType(StructField("in_struct", pyInputTypes) :: Nil)
      val pythonOutputSchema = StructType(StructField("out_struct",
        StructType.fromAttributes(output)) :: Nil)

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }

      val contextAwareIter = new Iterator[ColumnarBatch] {
        // This is to implement the same logic of `ContextAwareIterator` involved from 3.0.2 .
        // Doing this can avoid shim layers for Spark versions before 3.0.2.
        override def hasNext: Boolean =
          !context.isCompleted() && !context.isInterrupted() && inputIter.hasNext

        override def next(): ColumnarBatch = inputIter.next()
      }

      val pyInputIterator = new RebatchingRoundoffIterator(contextAwareIter, pyInputTypes,
          batchSize, mNumInputRows, mNumInputBatches, spillCallback)
        .map { batch =>
          // Here we wrap it via another column so that Python sides understand it
          // as a DataFrame.
          withResource(batch) { b =>
            val structColumn = cudf.ColumnVector.makeStruct(GpuColumnVector.extractBases(b): _*)
            withResource(structColumn) { stColumn =>
              val gpuColumn = GpuColumnVector.from(stColumn.incRefCount(), pyInputTypes)
              new ColumnarBatch(Array(gpuColumn), b.numRows())
            }
          }
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
          argOffsets,
          pyInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf,
          batchSize,
          onDataWriteFinished = null,
          pythonOutputSchema,
          // We can not assert the result batch from Python has the same row number with the
          // input batch. Because Map Pandas UDF allows the output of arbitrary length
          // and columns.
          // Then try to read as many as possible by specifying `minReadTargetBatchSize` as
          // `Int.MaxValue` here.
          Int.MaxValue)

        executePython(pyInputIterator, output, pyRunner, mNumOutputRows, mNumOutputBatches)
      } else {
        // Empty partition, return it directly
        inputIter
      }
    } // end of mapPartitionsInternal
  } // end of doExecuteColumnar

}
