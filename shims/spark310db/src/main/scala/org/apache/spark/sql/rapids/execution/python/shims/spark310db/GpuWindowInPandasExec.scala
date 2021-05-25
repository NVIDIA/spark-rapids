/*
 * Copyright (c) 2021, NVIDIA CORPORATION.
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

package org.apache.spark.sql.rapids.execution.python.shims.spark310db

import scala.collection.mutable.ArrayBuffer

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.python.PythonWorkerSemaphore

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.rapids.execution.python.{GpuWindowInPandasExecBase, GpuPythonHelper, GpuPythonUDF, GroupingIterator}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This GpuWindowInPandasExec aims at accelerating the data transfer between
 * JVM and Python, and scheduling GPU resources for Python processes
 */
case class GpuWindowInPandasExec(
    projectList: Seq[Expression],  // parameter name is different
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan) extends GpuWindowInPandasExecBase {

  override final def pythonModuleKey: String = "databricks"

  // On Databricks, the projectList contains not only the window expression, but may also contains
  // the input attributes. So we need to extract the window expressions from it.
  override def windowExpression: Seq[Expression] = projectList.filter { expr =>
    expr.find(node => node.isInstanceOf[GpuWindowExpression]).isDefined
  }

  // On Databricks, the projectList is expected to be the final output, and it is nondeterministic.
  // It may contain the input attributes or not, or even part of the input attributes. So
  // we need to project the joined batch per this projectList.
  // But for the schema, just return it directly.
  override def output: Seq[Attribute] = projectList
    .map(_.asInstanceOf[NamedExpression].toAttribute)

  override def projectResult(joinedBatch: ColumnarBatch): ColumnarBatch = {
    // Project the data
    withResource(joinedBatch) { joinBatch =>
      GpuProjectExec.project(joinBatch, outReferences)
    }
  }

  // On Databricks, binding the references on driver side will get some invalid expressions
  // (e.g. none#0L, none@1L) in the `projectList`, causing failures in `test_window` test.
  // So need to do the binding for `projectList` lazily, and the binding will actually run
  // on executors now.
  private lazy val outReferences = {
    val allExpressions = windowFramesWithExpressions.map(_._2).flatten
    val references = allExpressions.zipWithIndex.map { case (e, i) =>
      // Results of window expressions will be on the right side of child's output
      GpuBoundReference(child.output.size + i, e.dataType, e.nullable)
    }
    val unboundToRefMap = allExpressions.zip(references).toMap
    // Bound the project list for GPU
    GpuBindReferences.bindGpuReferences(
      projectList.map(_.transform(unboundToRefMap)), child.output)
  }

  // Override doExecuteColumnar so we use the correct GpuArrowPythonRunner
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numInputRows = gpuLongMetric(NUM_INPUT_ROWS)
    val numInputBatches = gpuLongMetric(NUM_INPUT_BATCHES)
    val numOutputRows = gpuLongMetric(NUM_OUTPUT_ROWS)
    val numOutputBatches = gpuLongMetric(NUM_OUTPUT_BATCHES)
    val spillCallback = GpuMetric.makeSpillCallback(allMetrics)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    // 1) Unwrap the expressions and build some info data:
    //    - Map from expression index to frame index
    //    - Helper functions
    //    - isBounded by given a frame index
    val expressionsWithFrameIndex = windowFramesWithExpressions.map(_._2).zipWithIndex.flatMap {
      case (bufferEs, frameIndex) => bufferEs.map(expr => (expr, frameIndex))
    }
    val exprIndex2FrameIndex = expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap
    val (lowerBoundIndex, upperBoundIndex, frameWindowBoundTypes) =
      computeWindowBoundHelpers
    val isBounded = { frameIndex: Int => lowerBoundIndex(frameIndex) >= 0 }

    // 2) Extract window functions, here should be Python (Pandas) UDFs
    val allWindowExpressions = expressionsWithFrameIndex.map(_._1)
    val udfExpressions = allWindowExpressions.map {
      case e: GpuWindowExpression => e.windowFunction.asInstanceOf[GpuPythonUDF]
    }
    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == allWindowExpressions.length)

    // 3) Build the python runner configs
    val udfWindowBoundTypesStr = pyFuncs.indices
      .map(expId => frameWindowBoundTypes(exprIndex2FrameIndex(expId)).value)
      .mkString(",")
    val pythonRunnerConf: Map[String, String] = ArrowUtils.getPythonRunnerConfMap(conf) +
      (windowBoundTypeConf -> udfWindowBoundTypesStr)

    // 4) Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs. This is similar to how other Python UDF node
    // handles UDF inputs.
    val dataInputs = new ArrayBuffer[Expression]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (dataInputs.exists(_.semanticEquals(e))) {
          dataInputs.indexWhere(_.semanticEquals(e))
        } else {
          dataInputs += e
          dataInputs.length - 1
        }
      }.toArray
    }.toArray

    // In addition to UDF inputs, we will prepend window bounds for each UDFs.
    // For bounded windows, we prepend lower bound and upper bound. For unbounded windows,
    // we no not add window bounds. (strictly speaking, we only need to lower or upper bound
    // if the window is bounded only on one side, this can be improved in the future)

    // 5) Setting window bounds for each window frames. Each window frame has different bounds so
    // each has its own window bound columns.
    val windowBoundsInput = windowFramesWithExpressions.indices.flatMap { frameIndex =>
      if (isBounded(frameIndex)) {
        Seq(
          BoundReference(lowerBoundIndex(frameIndex), IntegerType, nullable = false),
          BoundReference(upperBoundIndex(frameIndex), IntegerType, nullable = false)
        )
      } else {
        Seq.empty
      }
    }

    // 6) Setting the window bounds argOffset for each UDF. For UDFs with bounded window, argOffset
    // for the UDF is (lowerBoundOffset, upperBoundOffset, inputOffset1, inputOffset2, ...)
    // For UDFs with unbounded window, argOffset is (inputOffset1, inputOffset2, ...)
    pyFuncs.indices.foreach { exprIndex =>
      val frameIndex = exprIndex2FrameIndex(exprIndex)
      if (isBounded(frameIndex)) {
        argOffsets(exprIndex) = Array(lowerBoundIndex(frameIndex), upperBoundIndex(frameIndex)) ++
            argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      } else {
        argOffsets(exprIndex) = argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      }
    }

    // 7) Building the final input and schema
    val allInputs = windowBoundsInput ++ dataInputs
    val allInputTypes = allInputs.map(_.dataType)
    val pythonInputSchema = StructType(
      allInputTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }
    )

    lazy val isPythonOnGpuEnabled = GpuPythonHelper.isPythonOnGpuEnabled(conf, pythonModuleKey)
    // cache in a local to avoid serializing the plan

    // Build the Python output schema from UDF expressions instead of the 'windowExpression',
    // because the 'windowExpression' does NOT always represent the Python output schema.
    // For example, on Databricks when projecting only one column from a Python UDF output
    // where containing multiple result columns, there will be only one item in the
    // 'windowExpression' for the projecting output, but the output schema for this Python
    // UDF contains multiple columns.
    val pythonOutputSchema = StructType.fromAttributes(udfExpressions.map(_.resultAttribute))
    val childOutput = child.output

    // 8) Start processing.
    child.executeColumnar().mapPartitions { inputIter =>
      val context = TaskContext.get()
      val queue: BatchQueue = new BatchQueue()
      context.addTaskCompletionListener[Unit](_ => queue.close())

      val boundDataRefs = GpuBindReferences.bindGpuReferences(dataInputs, childOutput)
      // Re-batching the input data by GroupingIterator
      val boundPartitionRefs = GpuBindReferences.bindGpuReferences(partitionSpec, childOutput)
      val groupedIterator = new GroupingIterator(inputIter, boundPartitionRefs,
        numInputRows, numInputBatches, spillCallback)
      val pyInputIterator = groupedIterator.map { batch =>
        // We have to do the project before we add the batch because the batch might be closed
        // when it is added
        val projectedBatch = GpuProjectExec.project(batch, boundDataRefs)
        // Compute the window bounds and insert to the head of each row for one batch
        val inputBatch = withResource(projectedBatch) { projectedCb =>
          insertWindowBounds(projectedCb)
        }
        queue.add(batch, spillCallback)
        inputBatch
      }

      if (isPythonOnGpuEnabled) {
        GpuPythonHelper.injectGpuInfo(pyFuncs, isPythonOnGpuEnabled)
        PythonWorkerSemaphore.acquireIfNecessary(TaskContext.get())
      }

      if (pyInputIterator.hasNext) {
        val pyRunner = new GpuArrowPythonRunner(
          pyFuncs,
          PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
          argOffsets,
          pythonInputSchema,
          sessionLocalTimeZone,
          pythonRunnerConf,
          /* The whole group data should be written in a single call, so here is unlimited */
          Int.MaxValue,
          () => queue.finish(),
          pythonOutputSchema)

        val outputBatchIterator = pyRunner.compute(pyInputIterator, context.partitionId(), context)
        new Iterator[ColumnarBatch] {
          // for hasNext we are waiting on the queue to have something inserted into it
          // instead of waiting for a result to be ready from python. The reason for this
          // is to let us know the target number of rows in the batch that we want when reading.
          // It is a bit hacked up but it works. In the future when we support spilling we should
          // store the number of rows separate from the batch. That way we can get the target batch
          // size out without needing to grab the GpuSemaphore which we cannot do if we might block
          // on a read operation.
          // Besides, when the queue is empty, need to call the `hasNext` of the out iterator to
          // trigger reading and handling the control data followed with the stream data.
          override def hasNext: Boolean = queue.hasNext || outputBatchIterator.hasNext

          private [this] def combine(
                                      origBatch: ColumnarBatch,
                                      retBatch: ColumnarBatch): ColumnarBatch = {
            val lColumns = GpuColumnVector.extractColumns(origBatch)
            val rColumns = GpuColumnVector.extractColumns(retBatch)
            new ColumnarBatch(lColumns.map(_.incRefCount()) ++ rColumns.map(_.incRefCount()),
              origBatch.numRows())
          }

          override def next(): ColumnarBatch = {
            val numRows = queue.peekBatchSize
            // Update the expected batch size for next read
            pyRunner.minReadTargetBatchSize = numRows
            withResource(outputBatchIterator.next()) { cbFromPython =>
              assert(cbFromPython.numRows() == numRows)
              withResource(queue.remove()) { origBatch =>
                numOutputBatches += 1
                numOutputRows += numRows
                projectResult(combine(origBatch, cbFromPython))
              }
            }
          }
        } // End of new Iterator
      } else {
        // Empty partition, return the input iterator directly
        inputIter
      }

    } // End of mapPartitions
  } // End of doExecuteColumnar

}
