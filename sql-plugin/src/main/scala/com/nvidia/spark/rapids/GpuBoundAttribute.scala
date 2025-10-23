/*
 * Copyright (c) 2019-2025, NVIDIA CORPORATION.
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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression}
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
    transformNoRecursionOnReplacement(expression)(matchFunc).asInstanceOf[R]
  }

  // ========== Internal APIs (for use by GpuBind implementations and recursive calls) ==========
  // These methods do NOT inject metrics and should be used when implementing GpuBind.bind()

  /**
   * Internal binding method for a single GPU expression without metric injection.
   * This is for use by GpuBind implementations and should not be called directly
   * from SparkPlan nodes. Use the public API that requires metrics instead.
   */
  def bindGpuReferenceInternal[A <: Expression](
      expression: A,
      input: AttributeSeq): GpuExpression =
    bindRefInternal(expression, input)

  /**
   * Internal binding method for multiple GPU expressions without metric injection.
   * This is for use by GpuBind implementations and should not be called directly
   * from SparkPlan nodes. Use the public API that requires metrics instead.
   */
  def bindGpuReferencesInternal[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[GpuExpression] = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    expressions.map(GpuBindReferences.bindGpuReferenceInternal(_, input)).toList
  }

  /**
   * Internal binding method for expressions without metric injection.
   * This is for use by GpuBind implementations and should not be called directly
   * from SparkPlan nodes.
   */
  def bindReferenceInternal[A <: Expression](
      expression: A,
      input: AttributeSeq): A =
    bindRefInternal(expression, input)

  /**
   * Internal binding method for multiple expressions without metric injection.
   * This is for use by GpuBind implementations and should not be called directly
   * from SparkPlan nodes.
   */
  def bindReferencesInternal[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq): Seq[A] = {
    // Force list to avoid recursive Java serialization of lazy list Seq implementation
    expressions.map(GpuBindReferences.bindReferenceInternal(_, input)).toList
  }

  /**
   * Internal binding method for tiered expressions without metric injection.
   * This is for use by GpuBind implementations and should not be called directly
   * from SparkPlan nodes. Use the public API that requires metrics instead.
   */
  def bindGpuReferencesTieredInternal[A <: Expression](
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
          es.map(GpuBindReferences.bindGpuReferenceInternal(_, is)).toList
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
      GpuTieredProject(Seq(GpuBindReferences.bindGpuReferencesInternal(expressions, input)))
    }
  }

  // ========== Public "Front Door" APIs (for use by SparkPlan nodes) ==========
  // These methods require metrics and inject them after binding

  /**
   * Bind a single GPU expression and inject metrics.
   * This is the public API for use by SparkPlan nodes.
   * @param expression The expression to bind
   * @param input The input schema
   * @param metrics Metrics to inject into the bound expression
   */
  def bindGpuReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      metrics: Map[String, GpuMetric]): GpuExpression = {
    val bound = bindGpuReferenceInternal(expression, input)
    GpuMetric.injectMetrics(Seq(bound), metrics)
    bound
  }

  /**
   * Bind multiple GPU expressions and inject metrics.
   * This is the public API for use by SparkPlan nodes.
   * @param expressions The expressions to bind
   * @param input The input schema
   * @param metrics Metrics to inject into the bound expressions
   */
  def bindGpuReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq,
      metrics: Map[String, GpuMetric]): Seq[GpuExpression] = {
    val bound = bindGpuReferencesInternal(expressions, input)
    GpuMetric.injectMetrics(bound, metrics)
    bound
  }

  /**
   * Bind a single expression and inject metrics.
   * This is the public API for use by SparkPlan nodes.
   * @param expression The expression to bind
   * @param input The input schema
   * @param metrics Metrics to inject into the bound expression
   */
  def bindReference[A <: Expression](
      expression: A,
      input: AttributeSeq,
      metrics: Map[String, GpuMetric]): A = {
    val bound = bindReferenceInternal(expression, input)
    GpuMetric.injectMetrics(Seq(bound), metrics)
    bound
  }

  /**
   * Bind multiple expressions and inject metrics.
   * This is the public API for use by SparkPlan nodes.
   * @param expressions The expressions to bind
   * @param input The input schema
   * @param metrics Metrics to inject into the bound expressions
   */
  def bindReferences[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq,
      metrics: Map[String, GpuMetric]): Seq[A] = {
    val bound = bindReferencesInternal(expressions, input)
    GpuMetric.injectMetrics(bound, metrics)
    bound
  }

  /**
   * Bind expressions in a tiered manner for optimized projection and inject metrics.
   * This is the public API for use by SparkPlan nodes.
   * @param expressions The expressions to bind
   * @param input The input schema
   * @param conf SQL configuration
   * @param metrics Metrics to inject into the bound expressions
   */
  def bindGpuReferencesTiered[A <: Expression](
      expressions: Seq[A],
      input: AttributeSeq,
      conf: SQLConf,
      metrics: Map[String, GpuMetric]): GpuTieredProject = {
    val bound = bindGpuReferencesTieredInternal(expressions, input, conf)
    bound.injectMetrics(metrics)
    bound
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
