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

package org.apache.spark.sql.rapids.execution.python

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore
import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression,
  PythonUDF, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

class GpuMapInPandasExecMeta(
    mapPandas: MapInPandasExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[MapInPandasExec](mapPandas, conf, parent, rule) {

  override def couldReplaceMessage: String = "could partially run on GPU"
  override def noReplacementPossibleMessage(reasons: String): String =
    s"cannot run even partially on the GPU because $reasons"

  // Ignore the udf since columnar way is not supported yet
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec =
    GpuMapInPandasExec(
      mapPandas.func,
      mapPandas.output,
      childPlans.head.convertIfNeeded()
    )
}

/*
 * A relation produced by applying a function that takes an iterator of pandas DataFrames
 * and outputs an iterator of pandas DataFrames.
 *
 * This GpuMapInPandasExec aims at supporting running Pandas functional code
 * on GPU at Python side.
 *
 * (Currently it will not run on GPU itself, since the columnar way is not implemented yet.)
 *
 */
case class GpuMapInPandasExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with GpuExec {

  override def supportsColumnar = false
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new IllegalStateException(s"Columnar execution is not supported by $this yet")
  }

  // Most code is copied from MapInPandasExec, except two GPU related calls
  private val pandasFunction = func.asInstanceOf[PythonUDF].func

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    child.execute().mapPartitionsInternal { inputIter =>
      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
      val sessionLocalTimeZone = conf.sessionLocalTimeZone
      val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
      val outputTypes = child.schema

      // Here we wrap it via another row so that Python sides understand it
      // as a DataFrame.
      val wrappedIter = inputIter.map(InternalRow(_))

      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter =
        if (batchSize > 0) new BatchIterator(wrappedIter, batchSize) else Iterator(wrappedIter)

      val context = TaskContext.get()

      // Start of GPU things
      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(chainedFunc, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(context)
      }
      // End of GPU things

      val columnarBatchIter = new ArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        argOffsets,
        StructType(StructField("struct", outputTypes) :: Nil),
        sessionLocalTimeZone,
        pythonRunnerConf).compute(batchIter, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        // Scalar Iterator UDF returns a StructType column in ColumnarBatch, select
        // the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = output.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch.rowIterator.asScala
      }.map(unsafeProj)
    }
  }
}
