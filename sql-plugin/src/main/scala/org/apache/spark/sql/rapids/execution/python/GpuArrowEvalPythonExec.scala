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

import scala.collection.JavaConverters._

import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, PythonUDF}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

class GpuArrowEvalPythonExecMeta(
    arrowPy: ArrowEvalPythonExec,
    conf: RapidsConf,
    parent: Option[RapidsMeta[_, _, _]],
    rule: ConfKeysAndIncompat)
  extends SparkPlanMeta[ArrowEvalPythonExec](arrowPy, conf, parent, rule) {

  // Handle the child expressions(Python UDF) ourselves.
  override val childExprs: Seq[BaseExprMeta[_]] = Seq.empty

  override def convertToGpu(): GpuExec =
    GpuArrowEvalPythonExec(
      wrapped.udfs, wrapped.resultAttrs, wrapped.child, wrapped.evalType
    )
}

/**
 * A physical plan of GPU version that evaluates a PythonUDF where data saved
 * in arrow format.
 *
 */
case class GpuArrowEvalPythonExec(
    udfs: Seq[PythonUDF],
    resultAttrs: Seq[Attribute],
    override val child: SparkPlan,
    evalType: Int)
  extends EvalPythonExec(udfs, resultAttrs, child) with GpuExec {

  override def supportsColumnar = false
  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    // TBD
    super.doExecuteColumnar()
  }

  // 'evaluate' is from EvalPythonExec.
  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    GpuPythonHelper.injectGpuInfo(funcs)
    PythonWorkerSemaphore.acquireIfNecessary(context)
    doEvaluate(funcs, argOffsets, iter, schema, context)
  }

  /**
    * Code below is copied from `ArrowEvalPythonExec`
    */
  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  private def doEvaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // DO NOT use iter.grouped(). See BatchIterator.
    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val columnarBatchIter = new ArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.flatMap { batch =>
      val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
      assert(outputTypes == actualDataTypes, "Invalid schema from pandas_udf: " +
        s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
      batch.rowIterator.asScala
    }
  }
}
