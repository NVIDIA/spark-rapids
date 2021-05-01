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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, Expression, SortOrder}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuBindReferences extends Logging {

  private[this] def postBindCheck[A <: Expression](base: A): Unit = {
    base.foreach { expr =>
      // The condition is needed to have it match what transform
      // looks at, otherwise we can check things that would not be modified.
      if (expr.containsChild.nonEmpty) {
        expr match {
          case _: GpuExpression =>
          case _: SortOrder =>
          case other =>
            throw new IllegalArgumentException(
              s"Bound an expression that shouldn't be here ${other.getClass}")
        }
      }
    }
  }

  // Mostly copied from BoundAttribute.scala so we can do columnar processing
  private[this] def bindRefInternal[A <: Expression, R <: Expression](
      expression: A,
      input: AttributeSeq): R = {
    val ret = expression.transform {
      case a: AttributeReference =>
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        } else {
          GpuBoundReference(ordinal, a.dataType, input(ordinal).nullable)
        }
    }.asInstanceOf[R]
    postBindCheck(ret)
    ret
  }

  def bindGpuReference[A <: Expression](
      expression: A,
      input: AttributeSeq): GpuExpression =
    bindRefInternal(expression, input)

  /**
   * A helper function to bind given expressions to an input schema where the expressions are
   * to be processed on the GPU, and the result type indicates this.
   */
  def bindGpuReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[GpuExpression] = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    expressions.map(GpuBindReferences.bindGpuReference(_, input)).toList
  }

  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq): A =
    bindRefInternal(expression, input)

  /**
   * A helper function to bind given expressions to an input schema where the expressions are
   * to be processed on the GPU.  Most of the time `bindGpuReferences` should be used, unless
   * you know that the return type is `SortOrder` or is a comment trait like `Attribute`.
   */
  def bindReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    expressions.map(GpuBindReferences.bindReference(_, input)).toList
  }
}

case class GpuBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends GpuLeafExpression {

  override def toString: String = s"input[$ordinal, ${dataType.simpleString}, $nullable]"

  override def columnarEval(batch: ColumnarBatch): Any = {
    batch.column(ordinal) match {
      case fb: GpuColumnVectorFromBuffer =>
        // When doing a project we might re-order columns or do other things that make it
        // so this no longer looks like the original contiguous buffer it came from
        // so to avoid it appearing to down stream processing as the same buffer we change
        // the type here.
        new GpuColumnVector(fb.dataType(), fb.getBase.incRefCount())
      case cv: GpuColumnVector => cv.incRefCount()
    }
  }
}
