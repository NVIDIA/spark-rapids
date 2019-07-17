/*
 * Copyright (c) 2019, NVIDIA CORPORATION.
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

package ai.rapids.spark

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, BoundReference}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

object GpuBindReferences extends Logging {

  // Mostly copied from BoundAttribute.scala so we can do columnar processing
  def bindReference[A <: GpuExpression](
      expression: A,
      input: AttributeSeq,
      allowFailures: Boolean = false): A = {
    expression.transform { case a: AttributeReference =>
      val ordinal = input.indexOf(a.exprId)
      if (ordinal == -1) {
        if (allowFailures) {
          a
        } else {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        }
      } else {
        new GpuBoundReference(ordinal, a.dataType, input(ordinal).nullable)
      }
    }.asInstanceOf[A]
  }

  /**
   * A helper function to bind given expressions to an input schema.
   */
  def bindReferences[A <: GpuExpression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    expressions.map(GpuBindReferences.bindReference(_, input))
  }
}

class GpuBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
  extends BoundReference(ordinal, dataType, nullable) with GpuExpression {

  override def columnarEval(batch: ColumnarBatch): Any = {
    batch.column(ordinal).asInstanceOf[GpuColumnVector].inRefCount()
  }
}