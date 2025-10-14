/*
 * Copyright (c) 2025, NVIDIA CORPORATION.
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

import ai.rapids.cudf.{NvtxColor, NvtxRange}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.ShimExpression
import java.util.concurrent.Callable
import scala.util.{Failure, Success, Try}

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
    val gpuInputsStr = if (gpuInputs.nonEmpty) {
      gpuInputs.map(_.toString).mkString(", ")
    } else {
      ""
    }
    s"GpuCpuBridge(gpuInputs=[${gpuInputsStr}], cpuExpression=${cpuExpression.toString})"
  }

  override def sql: String = {
    val gpuInputsSql = if (gpuInputs.nonEmpty) {
      gpuInputs.map(_.sql).mkString(", ")
    } else {
      ""
    }
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
  // while allowing reuse across batches within a thread
  @transient private lazy val threadLocalProjection: ThreadLocal[BridgeUnsafeProjection] =
    ThreadLocal.withInitial(() => createCodeGeneratedProjection(cpuExpression))
  
  @transient private lazy val evaluationFunction: (Iterator[InternalRow], Int) => GpuColumnVector =
    createEvaluationFunction(cpuExpression)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      // Handle empty batch case
      return GpuColumnVector.fromNull(numRows, dataType)
    }
    
    // Time the entire CPU bridge operation from the SparkPlan perspective
    val waitStartTime = System.nanoTime()
    try {
      // Check if we should use parallel processing
      // Note: Nondeterministic expressions are excluded from CPU bridge entirely,
      // so all expressions reaching this point are safe for parallel processing
      val result = if (GpuCpuBridgeThreadPool.shouldParallelize(numRows)) {
        logDebug(s"Using parallel processing for ${numRows} rows " +
          s"(expression: ${cpuExpression.getClass.getSimpleName})")
        evaluateInParallel(batch)
      } else {
        logDebug(s"Using sequential processing for ${numRows} rows " +
          s"(expression: ${cpuExpression.getClass.getSimpleName})")
        evaluateSequentially(batch)
      }
      result
    } finally {
      // Record the total wait time from the SparkPlan's perspective
      val waitTime = System.nanoTime() - waitStartTime
      cpuBridgeWaitTime.foreach(_.add(waitTime))
    }
  }
  
  /**
   * Evaluate the expression sequentially (single-threaded) for small batches.
   */
  private def evaluateSequentially(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    // Evaluate GPU input expressions and get GPU columns
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))

    // Time the CPU processing (columnar->row->CPU expr->columnar)
    val processingStartTime = System.nanoTime()
    try {
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
    } finally {
      // Record the actual CPU processing time
      val processingTime = System.nanoTime() - processingStartTime
      cpuBridgeProcessingTime.foreach(_.add(processingTime))
    }
  }
  
  /**
   * Evaluate the expression in parallel using sub-batches for large batches.
   */
  private def evaluateInParallel(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    val subBatchCount = GpuCpuBridgeThreadPool.getSubBatchCount(numRows)
    val ranges = GpuCpuBridgeThreadPool.getSubBatchRanges(numRows, subBatchCount)
    
    logDebug(s"Processing $numRows rows in $subBatchCount parallel sub-batches " +
      s"AVG ${numRows.toDouble/subBatchCount}")

    // We are going to violate some constraints on the retry framework a little bit here
    // We want to be able to have a retry block in the thread pool that will execute the
    // CPU expressions, but to truly be correct all data must be spillable for the
    // task when a retry block is rolled back. The issue here is that our input batch
    // will not be spillable because we don't have control over that life cycle so
    // we will just declare it good enough and realize that even if we roll back
    // all the threads some data will still not be spillable.

    // Evaluate GPU input expressions once
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))
    
    // Create spillable wrapper for GPU input columns to ensure they can be spilled
    val spillableInputBatch = closeOnExcept(
      new ColumnarBatch(gpuInputColumns.toArray, numRows)) { inputBatch =>
      SpillableColumnarBatch(inputBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
    }

    val subBatchResults = withResource(spillableInputBatch) { spillableInputBatch =>
      // Submit sub-batch processing tasks to the thread pool with priority
      val futures = ranges.map { case (startRow, endRow) =>
        val task = new SpillableSubBatchEvaluationTask(spillableInputBatch, startRow, endRow, 
          cpuBridgeProcessingTime)
        GpuCpuBridgeThreadPool.submitPrioritizedTask(task)
      }
      
      // Collect results from all sub-batches - these will be spillable results
      // Note: Wait time is measured at the columnarEval level, not here
      futures.safeMap { future =>
        Try(future.get()) match {
          case Success(spillableResult) => 
            // Convert spillable result back to regular GpuColumnVector for concatenation  
            // The spillable result transfers ownership, so we don't need incRefCount
            withResource(spillableResult) { _ =>
              withResource(spillableResult.getColumnarBatch()) { cb =>
                cb.column(0).asInstanceOf[GpuColumnVector].incRefCount()
              }
            }
          case Failure(exception) =>
            // Cancel remaining futures on error with proper resource cleanup
            cleanupFuturesOnFailure(futures)
            throw new RuntimeException("Sub-batch evaluation failed", exception)
        }
      }
    }
      
    // Concatenate all sub-batch results
    concatenateResults(subBatchResults)
  }

  /**
   * Task for evaluating a sub-batch of rows on the CPU with spillable input and output.
   * Note: All expressions using CPU bridge are deterministic (nondeterministic 
   * expressions are excluded entirely), so parallel processing is safe.
   */
  private class SpillableSubBatchEvaluationTask(
      spillableInputBatch: SpillableColumnarBatch,
      startRow: Int,
      endRow: Int,
      cpuBridgeProcessingTime: Option[GpuMetric]) extends Callable[SpillableColumnarBatch] {
    
    override def call(): SpillableColumnarBatch = {
      val subBatchSize = endRow - startRow

      // spillableInputBatch is what we are going to retry around, but its life cycle is
      // controlled by the main task thread and shared between multiple threads in this pool.
      // So we will not try and close it, nor try and split it as a part of the retry.
      withRetryNoSplit {
        // Create a sub-batch wrapper that represents just the slice we need
        val subBatchInput = withResource(spillableInputBatch.getColumnarBatch()) { inputBatch =>
          val subBatchColumns = (0 until inputBatch.numCols()).safeMap { i =>
            val col = inputBatch.column(i).asInstanceOf[GpuColumnVector]
            closeOnExcept(col.getBase.subVector(startRow, endRow)) { sliced =>
              GpuColumnVector.from(sliced, col.dataType())
            }
          }
          new ColumnarBatch(subBatchColumns.toArray, subBatchSize)
        }
        // The iterator takes ownership of the subBatchInput
        val rowIterator = new ColumnarToRowIterator(
          Iterator.single(subBatchInput),
          NoopMetric,
          NoopMetric,
          NoopMetric,
          NoopMetric,
          nullSafe = false,
          releaseSemaphore = false
        )
        withResource(new NvtxRange("evaluateOnCPU", NvtxColor.BLUE)) { _ =>
          // Time the actual CPU processing on this thread
          val processingStartTime = System.nanoTime()
          val result = try {
            evaluationFunction(rowIterator, subBatchInput.numRows())
          } finally {
            val processingTime = System.nanoTime() - processingStartTime
            cpuBridgeProcessingTime.foreach(_.add(processingTime))
          }

          // Convert result to spillable format
          val resultBatch = new ColumnarBatch(Array(result), subBatchInput.numRows())
          SpillableColumnarBatch(resultBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
        }
      }
    }
  }
  
  /**
   * Properly cleanup futures on failure, handling both cancellable and non-cancellable futures.
   * This method addresses the race condition where futures might be in the middle of allocating
   * resources when cancellation is attempted.
   */
  private def cleanupFuturesOnFailure(
      futures: Seq[java.util.concurrent.Future[SpillableColumnarBatch]]): Unit = {
    import java.util.concurrent.TimeUnit
    
    // Track which futures were successfully cancelled vs. which are still running
    val futuresAndCancellations = futures.map { f =>
      val didCancel = f.cancel(true)
      (f, didCancel)
    }
    
    // For futures that couldn't be cancelled, we need to wait for them to complete
    // and clean up their resources to prevent leaks
    val uncancelledFutures = futuresAndCancellations.filter { case (_, didCancel) => !didCancel }
    
    if (uncancelledFutures.nonEmpty) {
      logDebug(s"${uncancelledFutures.length} futures could not be cancelled, " +
        "waiting for completion to clean up resources")
    }
    
    var cleanupFailure: Option[Throwable] = None
    uncancelledFutures.foreach { case (future, _) =>
      try {
        // Wait with a short timeout for the future to complete
        // If it times out, we'll have to accept the potential leak
        val spillableResult = future.get(100, TimeUnit.MILLISECONDS)
        spillableResult.close() // Clean up the spillable result
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          logWarning("Timed out waiting for future completion during cleanup. " +
            "Potential resource leak.")
        case _: java.util.concurrent.CancellationException =>
          // Future was cancelled after all, no cleanup needed
        case t: Throwable =>
          // Capture the first cleanup exception but don't let it mask the original error
          if (cleanupFailure.isEmpty) {
            cleanupFailure = Some(t)
          } else {
            logWarning(s"Exception during future cleanup: ${t.getMessage}")
          }
      }
    }
    
    // Log any cleanup failures but don't throw - we want the original exception to propagate
    cleanupFailure.foreach { t =>
      logError("Failed to clean up some future resources during error handling", t)
    }
  }
  
  /**
   * Concatenate results from multiple sub-batches into a single result column.
   */
  private def concatenateResults(subBatchResults: Seq[GpuColumnVector]): GpuColumnVector = {
    if (subBatchResults.length == 1) {
      subBatchResults.head
    } else {
      try {
        // Convert to cuDF columns for concatenation
        val cudfColumns = subBatchResults.map(_.getBase)
        val concatenated = ai.rapids.cudf.ColumnVector.concatenate(cudfColumns: _*)
        closeOnExcept(concatenated) { concat =>
          GpuColumnVector.from(concat, dataType)
        }
      } finally {
        // Clean up sub-batch results
        subBatchResults.safeClose()
      }
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
  private def createEvaluationFunction(
    cpuExpression: Expression): (Iterator[InternalRow], Int) => GpuColumnVector = {
    createCodegenEvaluationFunction(cpuExpression)
  }
  
  /**
   * Creates a code-generated evaluation function that writes to the RapidsHostColumnBuilder.
   * Uses BridgeUnsafeProjection for code generation that writes directly to the builder.
   * Uses thread-local projections to avoid expensive creation on each batch
   * while preventing data corruption from internal memory reuse across threads.
   * Streams through the iterator without caching rows in memory.
   */
  private def createCodegenEvaluationFunction(
    cpuExpression: Expression): (Iterator[InternalRow], Int) => GpuColumnVector = {
    
    logDebug(s"Using code generation for ${cpuExpression.getClass.getSimpleName}")
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
   * Creates a code-generated projection for the CPU expression using Spark's code generation.
   * This provides much better performance than interpreted evaluation.
   */
  private def createCodeGeneratedProjection(expr: Expression): BridgeUnsafeProjection = {
    // Create a projection that evaluates the expression and returns the result
    // The projection takes the input row and produces a single-column result
    val expressions = Seq(expr)
    
    // Use Spark's code generation framework to create an optimized projection
    BridgeUnsafeProjection.create(expressions)
  }
}