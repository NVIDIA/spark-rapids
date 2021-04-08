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
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PythonUDF}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python._
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
  extends UnaryExecNode with GpuExec {

  private val pandasFunction = func.asInstanceOf[GpuPythonUDF].func

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override lazy val allMetrics: Map[String, GpuMetric] = Map(
    NUM_OUTPUT_ROWS -> createMetric(outputRowsLevel, DESCRIPTION_NUM_OUTPUT_ROWS),
    NUM_OUTPUT_BATCHES -> createMetric(outputBatchesLevel, DESCRIPTION_NUM_OUTPUT_BATCHES),
    NUM_INPUT_ROWS -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_ROWS),
    NUM_INPUT_BATCHES -> createMetric(DEBUG_LEVEL, DESCRIPTION_NUM_INPUT_BATCHES)
  ) ++ spillMetrics

  override protected def doExecute(): RDD[InternalRow] =
    throw new IllegalStateException(s"Row-based execution should not occur for $this")

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf)
    val pyInputTypes = child.schema
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    // Start process
    child.executeColumnar().mapPartitionsInternal { inputIter =>
      val queue: BatchQueue = new BatchQueue()
      val context = TaskContext.get()
      context.addTaskCompletionListener[Unit](_ => queue.close())

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
        batchSize, numInputRows, numInputBatches, spillCallback)
        .map { batch =>
          // Here we wrap it via another column so that Python sides understand it
          // as a DataFrame.
          val structColumn = cudf.ColumnVector.makeStruct(GpuColumnVector.extractBases(batch): _*)
          val pyInputBatch = withResource(structColumn) { stColumn =>
            val gpuColumn = GpuColumnVector.from(stColumn.incRefCount(), pyInputTypes)
            new ColumnarBatch(Array(gpuColumn), batch.numRows())
          }
          // cache the original batches for release later.
          queue.add(batch, spillCallback)
          pyInputBatch
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
          () => queue.close(),
          pythonOutputSchema,
          Int.MaxValue)
        val pythonOutputIter = pyRunner.compute(pyInputIterator, context.partitionId(), context)

        new Iterator[ColumnarBatch] {

          override def hasNext: Boolean = pythonOutputIter.hasNext

          override def next(): ColumnarBatch = {
            // We can not assert the result batch from Python has the same row number with the
            // input batch. Because Map Pandas UDF allows the output of arbitrary length
            // and columns.
            // Then try to read as many as possible by specifying `minReadTargetBatchSize` as
            // `Int.MaxValue` when creating the `GpuArrowPythonRunner` above.
            withResource(pythonOutputIter.next()) { cbFromPython =>
              numOutputBatches += 1
              numOutputRows += cbFromPython.numRows
              extractChildren(cbFromPython)
            }
          }

          private[this] def extractChildren(batch: ColumnarBatch): ColumnarBatch = {
            assert(batch.numCols() == 1, "Expect only one struct column")
            assert(batch.column(0).dataType().isInstanceOf[StructType],
              "Expect a struct column")
            // Scalar Iterator UDF returns a StructType column in ColumnarBatch, select
            // the children here
            val structColumn = batch.column(0).asInstanceOf[GpuColumnVector].getBase
            val outputColumns = output.zipWithIndex.safeMap {
              case (attr, i) =>
                withResource(structColumn.getChildColumnView(i)) { childView =>
                  GpuColumnVector.from(childView.copyToColumnVector(), attr.dataType)
                }
            }
            new ColumnarBatch(outputColumns.toArray, batch.numRows())
          }
        }
      } else {
        // Empty partition, return it directly
        inputIter
      }
    } // end of mapPartitionsInternal
  } // end of doExecuteColumnar

}
