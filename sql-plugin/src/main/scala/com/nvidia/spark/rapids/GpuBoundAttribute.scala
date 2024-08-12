/*
 * Copyright (c) 2019-2024, NVIDIA CORPORATION.
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

import ai.rapids.cudf.ast
import com.nvidia.spark.rapids.shims.ShimExpression

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression, SortOrder}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.catalyst.expressions.GpuEquivalentExpressions
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A trait that allows an Expression to control how it and its child expressions are bound. This
 * should be used with a lot of caution as binding can be really hard to debug if you get it wrong.
 * The output of bind should have all instances of AttributeReference replaced with
 * GpuBoundReference.
 */
trait GpuBind {
  /**
   * Returns a modified version of `this` with at a minimum all AttributeReferences in the child
   * expressions replaced with GpuBoundReference instances.
   */
  def bind(input: AttributeSeq): GpuExpression
}

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
              s"Found an expression that shouldn't be here ${other.getClass}")
        }
      }
    }
  }

  /**
   * An alternative to `Expression.transformDown`, but when a result is returned by `rule` it is
   * assumed that it handled processing exp and all of its children, so rule will not be called on
   * the children of that result recursively.
   */
  def transformNoRecursionOnReplacement(exp: Expression)
      (rule: PartialFunction[Expression, Expression]): Expression = {
    rule.lift(exp) match {
      case None =>
        exp.mapChildren(c => transformNoRecursionOnReplacement(c)(rule))
      case Some(e) =>
        e
    }
  }

  // Mostly copied from BoundAttribute.scala but with a few big changes so we can do columnar
  // processing. Use with Caution
  def bindRefInternal[A <: Expression, R <: Expression](
      expression: A,
      input: AttributeSeq,
      partial: PartialFunction[Expression, Expression] = PartialFunction.empty): R = {
    val regularMatch: PartialFunction[Expression, Expression] = {
      case bind: GpuBind =>
        bind.bind(input)
      case a: AttributeReference =>
        val ordinal = input.indexOf(a.exprId)
        if (ordinal == -1) {
          sys.error(s"Couldn't find $a in ${input.attrs.mkString("[", ",", "]")}")
        } else {
          GpuBoundReference(ordinal, a.dataType, input(ordinal).nullable)(a.exprId, a.name)
        }
    }
    val matchFunc = regularMatch.orElse(partial)
    val ret = transformNoRecursionOnReplacement(expression)(matchFunc).asInstanceOf[R]
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

  /**
   * A helper function to bind given expressions to an input schema where the expressions are
   * to be processed on the GPU, and the result type indicates this.
   * Some expressions that can be combined into a single expression call as well as
   * common sub-expressions may be factored out where possible to reduce the runtime and memory.
   * All of these can be controlled by the configuration passed in.
   */
  def bindGpuReferencesTiered[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq,
      conf: SQLConf): GpuTieredProject = {

    if (RapidsConf.ENABLE_TIERED_PROJECT.get(conf)) {
      val replaced = if (RapidsConf.ENABLE_COMBINED_EXPRESSIONS.get(conf)) {
        GpuEquivalentExpressions.replaceMultiExpressions(expressions, conf)
      } else {
        expressions
      }
      val exprTiers = GpuEquivalentExpressions.getExprTiers(replaced)
      val inputTiers = GpuEquivalentExpressions.getInputTiers(exprTiers, input)
      // Update ExprTiers to include the columns that are pass through and drop unneeded columns
      val newExprTiers = exprTiers.zipWithIndex.map {
        case (exprTier, index) =>
          // get what the output should look like.
          val atInput = index + 1
          if (atInput < inputTiers.length) {
            inputTiers(atInput).attrs.map { attr =>
              exprTier.find { expr =>
                expr.asInstanceOf[NamedExpression].toAttribute == attr
              }.getOrElse(attr)
            }
          } else {
            exprTier
          }
      }
      val tiered = newExprTiers.zip(inputTiers).map {
        case (es: Seq[Expression], is: AttributeSeq) =>
          es.map(GpuBindReferences.bindGpuReference(_, is)).toList
      }
      logTrace {
        "INPUT:\n" +
          expressions.zipWithIndex.map {
            case (expr, idx) =>
              s"\t$idx:\t$expr"
          }.mkString("\n") +
          "\nOUTPUT:\n" +
          tiered.zipWithIndex.map {
            case (exprs, tier) =>
              s"\tTIER $tier\n" +
                exprs.zipWithIndex.map {
                  case (expr, idx) =>
                    s"\t\t$idx:\t$expr"
                }.mkString("\n")
          }.mkString("\n")
      }
      GpuTieredProject(tiered)
    } else {
      GpuTieredProject(Seq(GpuBindReferences.bindGpuReferences(expressions, input)))
    }
  }
}

case class GpuBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
    (val exprId: ExprId, val name: String)
  extends GpuLeafExpression with ShimExpression {

  override def toString: String =
    s"input[$ordinal, ${dataType.simpleString}, $nullable]($name#${exprId.id})"

  override def columnarEval(batch: ColumnarBatch): GpuColumnVector = {
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

  override def convertToAst(numFirstTableColumns: Int): ast.AstExpression = {
    // Spark treats all inputs as a single sequence of columns. For example, a join will put all
    // the columns of the left table followed by all the columns of the right table. cudf AST
    // instead uses explicit table references to distinguish which table is being indexed by a
    // column index. To translate from Spark to AST, we check the Spark column index against the
    // number of columns the leftmost input table to know which table is being referenced and
    // adjust the column index accordingly.
    if (ordinal >= numFirstTableColumns) {
      new ast.ColumnReference(ordinal - numFirstTableColumns, ast.TableReference.RIGHT)
    } else {
      new ast.ColumnReference(ordinal, ast.TableReference.LEFT)
    }
  }
}
