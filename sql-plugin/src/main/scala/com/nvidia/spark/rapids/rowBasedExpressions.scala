/*
 * Copyright (c) 2022, NVIDIA CORPORATION.
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

import ai.rapids.cudf.HostColumnVector
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import com.nvidia.spark.rapids.shims.{ShimBinaryExpression, ShimExpression, ShimTernaryExpression, ShimUnaryExpression}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
  * This trait enables expressions that might conditionally be evaluated on a row-basis (ie CPU)
  * within the context of GPU operation 
  */
trait GpuWrappedRowBasedExpression[SparkExpr <: Expression]
    extends GpuExpression
    with ShimExpression
    with Logging { 

  def rowExpression: SparkExpr

  def checkNull: Boolean

  override def prettyName: String = s"gpuwrapped(${rowExpression.prettyName})"

  override def columnarEval(batch: ColumnarBatch): Any = {
    val cpuExprStart = System.nanoTime
    val prepareArgsEnd = System.nanoTime

    // These child columns will be closed by `ColumnarToRowIterator`.
    val argCols = children.safeMap(GpuExpressionsUtils.columnarEvalToColumn(_, batch))
    try {
      // 1 Convert the argument columns to row.
      // 2 Evaluate the CPU UDF row by row and cache the result.
      // 3 Build a result column from the cache.
      val retConverter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
      val retType = GpuColumnVector.convertFrom(dataType, nullable)
      val retRow = new GenericInternalRow(size = 1)
      closeOnExcept(new HostColumnVector.ColumnBuilder(retType, batch.numRows)) { builder =>
        /**
         * This `nullSafe` is for https://github.com/NVIDIA/spark-rapids/issues/3942.
         * And more details can be found from
         *     https://github.com/NVIDIA/spark-rapids/pull/3997#issuecomment-957650846
         */
        val nullSafe = checkNull && GpuUserDefinedFunction.hostColumnAssertionEnabled
        new ColumnarToRowIterator(
            Iterator.single(new ColumnarBatch(argCols.toArray, batch.numRows())),
            NoopMetric,
            NoopMetric,
            NoopMetric,
            NoopMetric,
            nullSafe).foreach { row =>
          retRow.update(0, rowExpression.eval(row))
          retConverter.append(retRow, 0, builder)
        }
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          val cpuRunningTime = System.nanoTime - prepareArgsEnd
          // Use log to record the eclipsed time for the Expression running before
          // figuring out how to support Spark metrics in this expression.
          logDebug(s"It took ${cpuRunningTime} ns to run Expression ${rowExpression.prettyName}, " +
            s"and ${prepareArgsEnd - cpuExprStart} ns to get the input from children.")
          GpuColumnVector.from(resultCol, dataType)
        }
      }
    } catch {
      case e: Exception =>
        throw e
    }

  }
}

trait GpuWrappedRowBasedUnaryExpression[SparkUnaryExpr <: UnaryExpression]
    extends GpuUnaryExpression 
    with ShimUnaryExpression
    with GpuWrappedRowBasedExpression[SparkUnaryExpr] {
}

trait GpuWrappedRowBasedBinaryExpression[SparkBinaryExpr <: BinaryExpression]
    extends GpuBinaryExpression 
    with ShimBinaryExpression
    with GpuWrappedRowBasedExpression[SparkBinaryExpr] {
}

trait GpuWrappedRowBasedTernaryExpression[SparkTernaryExpr <: TernaryExpression]
    extends GpuTernaryExpression 
    with ShimTernaryExpression
    with GpuWrappedRowBasedExpression[SparkTernaryExpr] {
}