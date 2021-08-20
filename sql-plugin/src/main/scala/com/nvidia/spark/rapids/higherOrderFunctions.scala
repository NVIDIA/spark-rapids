/*
 * Copyright (c) 2019-2021, NVIDIA CORPORATION.
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

import java.util.Optional

import ai.rapids.cudf
import ai.rapids.cudf.{ColumnView, DType}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, NamedExpression}
import org.apache.spark.sql.types.{ArrayType, DataType, Metadata}
import org.apache.spark.sql.vectorized
import org.apache.spark.sql.vectorized.ColumnarBatch


/**
 * A named lambda variable. In Spark on the CPU this includes an AtomicReference to the value that
 * is updated for each time this needs to be called. On the GPU we have to bind this and turn into
 * a GpuBoundReference for a modified batch. In the future this might also be bound to an AST
 * expression.
 */
case class GpuNamedLambdaVariable(
    name: String,
    dataType: DataType,
    nullable: Boolean,
    exprId: ExprId = NamedExpression.newExprId)
    extends GpuLeafExpression
        with NamedExpression
        with GpuUnevaluable {

  override def qualifier: Seq[String] = Seq.empty

  override def newInstance(): NamedExpression =
    copy(exprId = NamedExpression.newExprId)

  override def toAttribute: Attribute = {
    AttributeReference(name, dataType, nullable, Metadata.empty)(exprId, Seq.empty)
  }

  override def toString: String = s"lambda $name#${exprId.id}$typeSuffix"

  override def simpleString(maxFields: Int): String = {
    s"lambda $name#${exprId.id}: ${dataType.simpleString(maxFields)}"
  }
}

/**
 * A lambda function and its arguments on the GPU. This is mostly just a wrapper around the
 * function expression.
 */
case class GpuLambdaFunction(
    function: Expression,
    arguments: Seq[NamedExpression],
    hidden: Boolean = false)
    extends GpuExpression {

  override def children: Seq[Expression] = function +: arguments
  override def dataType: DataType = function.dataType
  override def nullable: Boolean = function.nullable

//  lazy val bound: Boolean = arguments.forall(_.resolved)

  // TODO is there something better that we can do here???
  override def columnarEval(batch: ColumnarBatch): Any =
    function.asInstanceOf[GpuExpression].columnarEval(batch)
}

/**
 * A higher order function takes one or more (lambda) functions and applies these to some objects.
 * The function produces a number of variables which can be consumed by some lambda function.
 */
trait GpuHigherOrderFunction extends GpuExpression {

  override def nullable: Boolean = arguments.exists(_.nullable)

  override def children: Seq[Expression] = arguments ++ functions

  /**
   * Arguments of the higher ordered function.
   */
  def arguments: Seq[Expression]

  /**
   * Functions applied by the higher order function.
   */
  def functions: Seq[Expression]
}

/**
 * Trait for functions having as input one argument and one function.
 */
trait SimpleGpuHigherOrderFunction extends GpuHigherOrderFunction  {

  def argument: Expression

  override def arguments: Seq[Expression] = argument :: Nil

  def function: Expression

  override def functions: Seq[Expression] = function :: Nil
}

case class GpuArrayTransform(
    argument: Expression,
    function: Expression)
    extends SimpleGpuHigherOrderFunction
    with GpuBindPost {

  override def dataType: ArrayType = ArrayType(function.dataType, function.nullable)

  private lazy val inputElementType = argument.dataType.asInstanceOf[ArrayType].elementType

  override def prettyName: String = "transform"

  override def postBind: GpuExpression = {
    // TODO this should probably be made common at some point.
    // TODO need to look at bound references under the lambda function too and figure out
    //  how to remap everything so it goes together properly. For now I am just going to
    //  assume that all we have are the regular arguments and remap those
    val lamb = function.asInstanceOf[GpuLambdaFunction]
    val lambdaVarMap = lamb.arguments.zipWithIndex.map { case (expr, index) =>
      (expr.exprId, (expr, index))
    }.toMap
    this.transformUp {
      case lambdaVar: GpuNamedLambdaVariable if lambdaVarMap.contains(lambdaVar.exprId) =>
        val (expr, ordinal) = lambdaVarMap(lambdaVar.exprId)
        GpuBoundReference(ordinal, expr.dataType, expr.nullable)
    }.asInstanceOf[GpuExpression]
  }

  private[this] def makeElementProjectBatch(
      inputBatch: ColumnarBatch,
      listColumn: cudf.ColumnVector,
      elementType: DataType): ColumnarBatch = {
    assert(listColumn.getType.equals(DType.LIST))

    // TODO we should actually do a good job of this with explode/etc, but for now...
    val (cv, rows) = withResource(listColumn.getChildColumnView(0)) { dataColumn =>
      withResource(dataColumn.copyToColumnVector()) { copiedDataCol =>
        (Array[vectorized.ColumnVector](
          GpuColumnVector.from(copiedDataCol.incRefCount(), elementType)),
            copiedDataCol.getRowCount.toInt)
      }
    }
    new ColumnarBatch(cv, rows)
  }

  private[this] def makeListFrom(
      dataCol: cudf.ColumnVector,
      listCol: cudf.ColumnVector,
      resultType: DataType): GpuColumnVector = {
    withResource(listCol.getOffsets) { offsets =>
      withResource(listCol.getValid) { validity =>
        withResource(new ColumnView(DType.LIST, listCol.getRowCount,
          Optional.of(listCol.getNullCount), validity, offsets,
          Array[ColumnView](dataCol))) { view =>
          GpuColumnVector.from(view.copyToColumnVector(), resultType)
        }
      }
    }
  }

  override def columnarEval(batch: ColumnarBatch): Any = {
    withResource(GpuExpressionsUtils.columnarEvalToColumn(argument, batch)) { arg =>
      val dataCol = withResource(
        makeElementProjectBatch(batch, arg.getBase, inputElementType)) { cb =>
        GpuExpressionsUtils.columnarEvalToColumn(function, cb)
      }
      withResource(dataCol) { dataCol =>
        makeListFrom(dataCol.getBase, arg.getBase, dataType)
      }
    }
  }

}