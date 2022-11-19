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
import com.nvidia.spark.rapids.shims.{ShimBinaryExpression, ShimQuaternaryExpression, ShimTernaryExpression, ShimUnaryExpression}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.vectorized.ColumnarBatch

/*
 * This trait enables expressions that might conditionally be evaluated on a row-basis (ie CPU)
 * within the context of GPU operation 
 */
trait GpuWrappedRowBasedExpression[SparkExpr <: Expression]
    extends GpuExpression
    with Logging { 

  def rowExpression(children: Seq[Expression]): SparkExpr

  def nullSafe: Boolean

  private def prepareEvaluation(expression: Expression): Expression = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(expression)
    serializer.deserialize(serializer.serialize(expr))
  }

  private def evaluateWithoutCodegen(
    expression: Expression, inputRow: InternalRow = EmptyRow): Any = {
    expression.foreach {
      case n: Nondeterministic => n.initialize(0)
      case _ =>
    }
    expression.eval(inputRow)
  }

  private def evalRow(children: Seq[Expression], inputRow: InternalRow): Any = {
    def expr = prepareEvaluation(rowExpression(children))
    evaluateWithoutCodegen(expr, inputRow)
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    val cpuExprStart = System.nanoTime
    val prepareArgsEnd = System.nanoTime

    val childTypes = children.map(_.dataType)
    // These child columns will be closed by `ColumnarToRowIterator`.
    // 1 Evaluate all the children GpuExpressions to columns
    val argCols = children.safeMap(GpuExpressionsUtils.columnarEvalToColumn(_, batch))
    try {
      // 2 Convert the argument columns to row.
      val retConverter = GpuRowToColumnConverter.getConverterForType(dataType, nullable)
      val retType = GpuColumnVector.convertFrom(dataType, nullable)
      val retRow = new GenericInternalRow(size = 1)
      closeOnExcept(new HostColumnVector.ColumnBuilder(retType, batch.numRows)) { builder =>
        new ColumnarToRowIterator(
            Iterator.single(new ColumnarBatch(argCols.toArray, batch.numRows())),
            NoopMetric,
            NoopMetric,
            NoopMetric,
            NoopMetric,
            nullSafe).foreach { row =>
          // 3 Row by row, convert the argument row to literal expressions to pass to the 
          //   CPU Spark expression, then evaluate the CPU Spark expression and cache the 
          //   result row.
          val newChildren = childTypes.zip(row.toSeq(childTypes)).map { case (dt, value) =>
            Literal.create(value, dt)
          }
          logWarning(s"new children: $newChildren")
          retRow.update(0, evalRow(newChildren, row))
          retConverter.append(retRow, 0, builder)
        }
        // 4 Build a result column from the cache of result rows
        closeOnExcept(builder.buildAndPutOnDevice()) { resultCol =>
          val cpuRunningTime = System.nanoTime - prepareArgsEnd
          // Use log to record the eclipsed time for the Expression running before
          // figuring out how to support Spark metrics in this expression.
          logDebug(s"It took ${cpuRunningTime} ns to run Expression ${prettyName}, " +
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
    extends ShimUnaryExpression
    with GpuWrappedRowBasedExpression[SparkUnaryExpr] {

    def rowExpression(children: Seq[Expression]): SparkUnaryExpr = 
      unaryExpression(children(0))

    def unaryExpression(child: Expression): SparkUnaryExpr
}

trait GpuWrappedRowBasedBinaryExpression[SparkBinaryExpr <: BinaryExpression]
    extends ShimBinaryExpression
    with GpuWrappedRowBasedExpression[SparkBinaryExpr] {

    def rowExpression(children: Seq[Expression]): SparkBinaryExpr = 
      binaryExpression(children(0), children(1))

    def binaryExpression(left: Expression, right: Expression): SparkBinaryExpr
}

trait GpuWrappedRowBasedTernaryExpression[SparkTernaryExpr <: TernaryExpression]
    extends ShimTernaryExpression
    with GpuWrappedRowBasedExpression[SparkTernaryExpr] {

    def rowExpression(children: Seq[Expression]): SparkTernaryExpr = 
      ternaryExpression(children(0), children(1), children(2))

    def ternaryExpression(first: Expression,
        second: Expression, third: Expression): SparkTernaryExpr
}

trait GpuWrappedRowBasedQuaternaryExpression[SparkQuaternaryExpr <: QuaternaryExpression]
    extends ShimQuaternaryExpression
    with GpuWrappedRowBasedExpression[SparkQuaternaryExpr]