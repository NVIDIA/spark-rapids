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

import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.RmmRapidsRetryIterator.withRetryNoSplit
import com.nvidia.spark.rapids.shims.ShimExpression
import java.util.concurrent.Callable
import scala.util.{Failure, Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

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
 * @param codegenEnabled Whether to use code generation for CPU evaluation
 */
case class GpuCpuBridgeExpression(
    gpuInputs: Seq[Expression],
    cpuExpression: Expression,
    outputDataType: DataType,
    outputNullable: Boolean,
    codegenEnabled: Boolean) extends GpuExpression with ShimExpression with Logging {

  // Only GPU inputs are children for GPU expression tree traversal
  override def children: Seq[Expression] = gpuInputs

  override def dataType: DataType = outputDataType
  override def nullable: Boolean = outputNullable
  // Bridge expressions may contain CPU expressions with unknown side effects
  override def hasSideEffects: Boolean = true

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

  @transient private lazy val evaluationFunction: (Iterator[InternalRow], Int) => GpuColumnVector =
    createEvaluationFunction(cpuExpression)

  @transient private lazy val resultType = GpuColumnVector.convertFrom(dataType, nullable)

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    if (numRows == 0) {
      // Handle empty batch case
      return GpuColumnVector.fromNull(numRows, dataType)
    }
    
    // Check if we should use parallel processing
    // Note: Nondeterministic expressions are excluded from CPU bridge entirely,
    // so all expressions reaching this point are safe for parallel processing
    if (GpuCpuBridgeThreadPool.shouldParallelize(numRows)) {
      logDebug(s"Using parallel processing for ${numRows} rows " +
        s"(expression: ${cpuExpression.getClass.getSimpleName})")
      evaluateInParallel(batch)
    } else {
      evaluateSequentially(batch)
    }
  }
  
  /**
   * Evaluate the expression sequentially (single-threaded) for small batches.
   */
  private def evaluateSequentially(batch: ColumnarBatch): GpuColumnVector = {
    val numRows = batch.numRows()
    // Evaluate GPU input expressions and get GPU columns
    val gpuInputColumns = gpuInputs.safeMap(_.columnarEval(batch))

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
        val task = new SpillableSubBatchEvaluationTask(spillableInputBatch, startRow, endRow)
        val subBatchSize = endRow - startRow
        GpuCpuBridgeThreadPool.submitPrioritizedTask(task, subBatchSize)
      }
      
      // Collect results from all sub-batches - these will be spillable results
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
      endRow: Int) extends Callable[SpillableColumnarBatch] {
    
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

        val result = evaluationFunction(rowIterator, subBatchInput.numRows())

        // Convert result to spillable format
        val resultBatch = new ColumnarBatch(Array(result), subBatchInput.numRows())
        SpillableColumnarBatch(resultBatch, SpillPriorities.ACTIVE_ON_DECK_PRIORITY)
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
   */
  private def createEvaluationFunction(
    cpuExpression: Expression): (Iterator[InternalRow], Int) => GpuColumnVector = {
    if (codegenEnabled) {
      // Try code generation first for better performance
      try {
        logDebug(s"Using code generation for ${cpuExpression.getClass.getSimpleName}")
        createCodegenEvaluationFunction(cpuExpression)
      } catch {
        case e: Exception =>
          // Fall back to interpreted evaluation if code generation fails
          logWarning(s"Code generation failed for ${cpuExpression.getClass.getSimpleName}, " +
            s"falling back to interpreted evaluation: ${e.getMessage}")
          createInterpretedEvaluationFunction(cpuExpression)
      }
    } else {
      // Code generation disabled, use interpreted evaluation
      logDebug(s"Code generation disabled, using interpreted evaluation for " +
        s"${cpuExpression.getClass.getSimpleName}")
      createInterpretedEvaluationFunction(cpuExpression)
    }
  }
  
  /**
   * Creates a codegen evaluation function that uses GpuRowToColumnConverter for UnsafeRow handling.
   * Each batch creates a fresh UnsafeProjection to avoid data corruption from internal memory
   * reuse.
   * Streams through the iterator without caching rows in memory.
   */
  private def createCodegenEvaluationFunction(
    cpuExpression: Expression): (Iterator[InternalRow], Int) => GpuColumnVector = {
    (rowIterator: Iterator[InternalRow], numRows: Int) => {
      withResource(new RapidsHostColumnBuilder(resultType, numRows)) { builder =>
        // Create a fresh UnsafeProjection for this batch to avoid data corruption
        // UnsafeProjection reuses internal memory, so we need a separate instance per batch
        val projection = createCodeGeneratedProjection(cpuExpression)
        
        // Get the optimized converter for this data type and nullability
        val converter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
        
        // Stream through the iterator without caching - preserves memory efficiency
        rowIterator.foreach { row =>
          val resultRow = projection.apply(row)
          converter.append(resultRow, 0, builder)
        }
        
        // Build the result column and put it on the GPU
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    }
  }
  
  /**
   * Creates an interpreted evaluation function that goes directly from Any value to 
   * RapidsHostColumnBuilder.
   * Preserves the original interpreted path efficiency - no GpuRowToColumnConverter overhead.
   * Streams through the iterator without caching rows in memory.
   */
  private def createInterpretedEvaluationFunction(
    cpuExpression: Expression): (Iterator[InternalRow], Int) => GpuColumnVector = {
    // Generate a specialized append function once based on the data 
    // type (similar to TypeConverter pattern)
    val appendFunction = createOptimizedAppendFunction(dataType, nullable)
    
    (rowIterator: Iterator[InternalRow], numRows: Int) => {
      withResource(new RapidsHostColumnBuilder(resultType, numRows)) { builder =>
        // Stream through the iterator without caching - preserves memory efficiency
        rowIterator.foreach { row =>
          val result = cpuExpression.eval(row)
          appendFunction(result, builder)
        }
        
        // Build the result column and put it on the GPU
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    }
  }
  
  /**
   * Creates an optimized append function for the specific data type and nullability.
   * Similar to how TypeConverter generates specialized functions, this avoids the overhead
   * of evaluating the data type on every append operation.
   */
  private def createOptimizedAppendFunction(dataType: DataType, 
    nullable: Boolean): (Any, RapidsHostColumnBuilder) => Unit = {
    dataType match {
      // Primitive types - generate specialized functions
      case BooleanType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Boolean])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Boolean])
      
      case ByteType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Byte])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Byte])
      
      case ShortType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Short])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Short])
      
      case IntegerType | DateType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Int])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Int])
      
      case LongType | TimestampType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Long])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Long])
      
      case FloatType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Float])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Float])
      
      case DoubleType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else builder.append(value.asInstanceOf[Double])
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Double])
      
      case StringType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) builder.appendNull() else {
            val utf8String = value.asInstanceOf[UTF8String]
            builder.appendUTF8String(utf8String.getBytes)
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => {
          val utf8String = value.asInstanceOf[UTF8String]
          builder.appendUTF8String(utf8String.getBytes)
        }
      
      case BinaryType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) {
            builder.appendNull() 
          } else {
            builder.appendByteList(value.asInstanceOf[Array[Byte]])
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.appendByteList(value.asInstanceOf[Array[Byte]])
      
      case _: DecimalType =>
        if (nullable) (value: Any, builder: RapidsHostColumnBuilder) => 
          if (value == null) {
            builder.appendNull() 
          } else {
            builder.append(value.asInstanceOf[Decimal].toJavaBigDecimal)
          }
        else (value: Any, builder: RapidsHostColumnBuilder) => 
          builder.append(value.asInstanceOf[Decimal].toJavaBigDecimal)
      
      case _ =>
        // For complex types or unsupported types, fall back to TypeConverter
        // Only pay the SpecificInternalRow overhead when we actually need it
        logDebug(s"Using TypeConverter fallback for complex/unsupported type: $dataType")
        val converter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
        val resultRow = new SpecificInternalRow(Array(dataType))
        
        (value: Any, builder: RapidsHostColumnBuilder) => {
          resultRow.update(0, value)
          converter.append(resultRow, 0, builder)
        }
    }
  }
  
  /**
   * Creates a code-generated projection for the CPU expression using Spark's code generation.
   * This provides much better performance than interpreted evaluation.
   */
  private def createCodeGeneratedProjection(expr: Expression): UnsafeProjection = {
    // Create a projection that evaluates the expression and returns the result
    // The projection takes the input row and produces a single-column result
    val expressions = Seq(expr)
    
    // Use Spark's code generation framework to create an optimized projection
    UnsafeProjection.create(expressions)
  }
}