/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.ScalableTaskCompletion.onTaskCompletion
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, Expression}
import org.apache.spark.sql.rapids.BridgeUnsafeProjection
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A GPU expression that wraps a CPU expression subtree. This allows individual CPU expressions
 * to run while keeping the overall plan on the GPU.
 *
 * This expression:
 * 1. Takes GPU column data from its GPU expression children  
 * 2. Copies the data to host memory
 * 3. Evaluates the CPU expression tree on the host data
 * 4. Copies the results back to GPU memory
 *
 * @param gpuInputs GPU expressions that provide inputs to the CPU expression tree
 * @param cpuExpression The CPU expression tree to evaluate (not included as children)
 * @param outputDataType The output data type of the expression
 * @param outputNullable Whether the output can be null
 */
case class GpuCpuBridgeExpression(
    gpuInputs: Seq[Expression],
    cpuExpression: Expression,
    outputDataType: DataType,
    outputNullable: Boolean) extends GpuExpression with ShimExpression 
    with Logging with GpuBind with GpuMetricsInjectable {

  // Only GPU inputs are children for GPU expression tree traversal
  override def children: Seq[Expression] = gpuInputs ++ Seq(cpuExpression)

  override def dataType: DataType = outputDataType
  override def nullable: Boolean = outputNullable
  // Bridge expressions may contain CPU expressions with unknown side effects
  override def hasSideEffects: Boolean = true

  // Mutable metrics fields - injected after binding
  private var cpuBridgeProcessingTime: Option[GpuMetric] = None
  private var cpuBridgeWaitTime: Option[GpuMetric] = None

  /**
   * Inject metrics into this expression. Called after binding but before execution.
   */
  override def injectMetrics(metrics: Map[String, GpuMetric]): Unit = {
    import GpuMetric._
    cpuBridgeProcessingTime = metrics.get(CPU_BRIDGE_PROCESSING_TIME)
    cpuBridgeWaitTime = metrics.get(CPU_BRIDGE_WAIT_TIME)
  }

  override def prettyName: String = "gpu_cpu_bridge"

  override def toString: String = {
    val gpuInputsStr = gpuInputs.map(_.toString).mkString(", ")
    s"GpuCpuBridge(gpuInputs=[${gpuInputsStr}], cpuExpression=${cpuExpression.toString})"
  }

  override def sql: String = {
    val gpuInputsSql = gpuInputs.map(_.sql).mkString(", ")
    s"gpu_cpu_bridge(gpuInputs=[${gpuInputsSql}], cpuExpression=${cpuExpression.sql})"
  }

  override def bind(input: AttributeSeq): GpuExpression = {
    // The CPU expressions are already bound, so we don't need/want to
    // rebind them.

    // Regular GPU binding does not properly handle
    // named lambda variables. It would probably be good to fix this
    // at some point, but this works for now...
    val boundGpuInputs = gpuInputs.map { arg =>
      GpuBindReferences.bindRefInternal[Expression, GpuExpression](arg, input, {
        case lr: GpuNamedLambdaVariable if input.indexOf(lr.exprId) >= 0 =>
          val ordinal = input.indexOf(lr.exprId)
          GpuBoundReference(ordinal, lr.dataType, input(ordinal).nullable)(lr.exprId, lr.name)
      })
    }
    val boundExpression = GpuCpuBridgeExpression(boundGpuInputs, cpuExpression,
      outputDataType, outputNullable)
    // Preserve metrics when binding
    boundExpression.cpuBridgeProcessingTime = this.cpuBridgeProcessingTime
    boundExpression.cpuBridgeWaitTime = this.cpuBridgeWaitTime
    boundExpression
  }

  @transient private lazy val resultType = GpuColumnVector.convertFrom(dataType, nullable)
  
  // Thread-local projection for code generation path
  // Each thread gets its own projection to avoid internal memory reuse conflicts
  // while allowing reuse across batches within a thread.
  // Cleanup is registered on first access per task to prevent memory leaks in thread pools.
  // Cleanup is probably unnecessary, as the expression should be collected, but
  // it's good practice.
  @transient private lazy val threadLocalProjection: ThreadLocal[BridgeUnsafeProjection] =
    ThreadLocal.withInitial(() => {
      // Register cleanup when this thread first accesses the ThreadLocal in this task
      // Only register if we're actually in a Spark task (not in tests)
      Option(org.apache.spark.TaskContext.get()).foreach { _ =>
        onTaskCompletion {
          threadLocalProjection.remove()
        }
      }
      createCodeGeneratedProjection()
    })
  
  @transient private lazy val evaluationFunction: (Iterator[InternalRow], Int) => GpuColumnVector =
    createEvaluationFunction()

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      // Handle empty batch case
      return GpuColumnVector.fromNull(numRows, dataType)
    }
    
    // Time the entire CPU bridge operation from the SparkPlan perspective
    GpuMetric.nsOption(cpuBridgeWaitTime) {
      // Currently sequential processing only
      logDebug(s"Using sequential processing for ${numRows} rows " +
        s"(expression: ${cpuExpression.getClass.getSimpleName})")
      evaluateSequentially(batch)
    }
  }
  
  /**
   * Evaluate the expression sequentially (single-threaded).
   */
  private def evaluateSequentially(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    // Evaluate GPU input expressions and get GPU columns
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))

    // Time the CPU processing (columnar->row->CPU expr->columnar)
    GpuMetric.nsOption(cpuBridgeProcessingTime) {
      // Convert columnar data to rows for CPU expression evaluation
      // Note: ColumnarBatch takes ownership of the columns, so we don't close them separately
      val inputBatch = new ColumnarBatch(gpuInputColumns.toArray, numRows)
      val rowIterator = new ColumnarToRowIterator(
        Iterator.single(inputBatch),
        NoopMetric,
        NoopMetric,
        NoopMetric,
        NoopMetric,
        nullSafe = false,
        releaseSemaphore = false
      )

      // Delegate to evaluation function - ColumnarToRowIterator manages the batch lifecycle
      evaluationFunction(rowIterator, numRows)
    }
  }

  /**
   * Creates an evaluation function for the CPU expression.
   * Takes an iterator of rows and the expected row count, produces a complete 
   * GpuColumnVector result.
   * 
   * Uses code generation by default. If codegen fails, Spark's CodeGeneratorWithInterpretedFallback
   * will automatically create an InterpretedBridgeUnsafeProjection instead.
   */
  private def createEvaluationFunction(): (Iterator[InternalRow], Int) => GpuColumnVector = {
    (rowIterator: Iterator[InternalRow], numRows: Int) => {
      withResource(new RapidsHostColumnBuilder(resultType, numRows)) { builder =>
        // Get thread-local projection - each thread has its own to avoid memory conflicts
        // but the projection is reused across batches within the same thread for performance
        val projection = threadLocalProjection.get()
        
        // Stream through the iterator without caching - preserves memory efficiency
        projection.apply(rowIterator, Array(builder))

        // Build the result column and put it on the GPU
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    }
  }
  
  /**
   * Creates a code-generated projection for the CPU expression using a 
   * modified version of Spark's code generation.
   * This provides much better performance than interpreted evaluation.
   */
  private def createCodeGeneratedProjection(): BridgeUnsafeProjection =
    BridgeUnsafeProjection.create(Seq(cpuExpression))
}

