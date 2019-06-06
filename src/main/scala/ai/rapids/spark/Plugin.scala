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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, BindReferences, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, ColumnarToRowExec, ProjectExec, RowToColumnarExec, SparkPlan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuProjectExec(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExec(projectList, child) {

  override def supportsColumnar = true
  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] = BindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    AutoCloseColumnBatchIterator.map(rdd,
      (cb: ColumnarBatch) => {
        val newColumns = boundProjectList.map(
          expr => {
            val result = expr.asInstanceOf[Expression].columnarEval(cb)
            // TODO it is possible for a constant to be returned that we need to
            // create a columnVector for, might be a special sub-class
            // that only stores a single value.
            result.asInstanceOf[ColumnVector]
          }).toArray
        new ColumnarBatch(newColumns, cb.numRows())
      }
    )
  }

  override def equals(other: Any): Boolean = {
    if (!super.equals(other)) {
      return false
    }
    return other.isInstanceOf[GpuProjectExec]
  }
}

case class GpuOverrides() extends Rule[SparkPlan] {
  def areAllSupportedTypes(types: DataType*): Boolean = {
    types.forall(_ match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case StringType => true
      case _ => false
    })
  }

  def replaceWithGpuExpression(exp: Expression): Expression = exp match {
    case a: Alias =>
      Alias(replaceWithGpuExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      att // No sub expressions and already supports columnar so just return it
    case lit: Literal =>
      lit // No sub expressions and already supports columnar so just return it
    case add: Add if (areAllSupportedTypes(add.dataType, add.left.dataType, add.right.dataType))=>
      new GpuAdd(replaceWithGpuExpression(add.left), replaceWithGpuExpression(add.right))
    case exp =>
      logWarning(s"GPU Processing for expression ${exp.getClass} ${exp} is not currently supported.")
      exp
  }

  def replaceWithGpuPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: ProjectExec =>
      new GpuProjectExec(plan.projectList.map((exp) =>
        replaceWithGpuExpression(exp).asInstanceOf[NamedExpression]),
        replaceWithGpuPlan(plan.child))
    case p =>
      logWarning(s"GPU Processing for ${p.getClass} is not currently supported.")
      p.withNewChildren(p.children.map(replaceWithGpuPlan))
  }

  def apply(plan: SparkPlan) :SparkPlan = {
    replaceWithGpuPlan(plan)
  }
}

case class GpuTransitionOverrides() extends Rule[SparkPlan] {

  def replaceWithGpuPlan(plan: SparkPlan): SparkPlan = plan match {
      // TODO need to verify that all columnar processing is GPU accelerated, or insert transitions
      // to/from host columnar data (this is likely to happen for python, R, and .net processing
      // This may come in the future, but does nto currently happen
    case r2c: RowToColumnarExec =>
      new GpuRowToColumnarExec(replaceWithGpuPlan(r2c.child))
    case c2r: ColumnarToRowExec =>
      new GpuColumnarToRowExec(replaceWithGpuPlan(c2r.child))
    case p =>
      p.withNewChildren(p.children.map(replaceWithGpuPlan))
  }

  def apply(plan: SparkPlan) :SparkPlan = {
    replaceWithGpuPlan(plan)
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {
  def gpuEnabled = session.sqlContext.
    getConf("ai.rapids.gpu.enabled", "true").trim.toBoolean
  val overrides = GpuOverrides()
  val overrideTransitions = GpuTransitionOverrides()

  override def pre: Rule[SparkPlan] = plan => {
    if (gpuEnabled) {
      // TODO as a part of this scan we need to check that all of the types are supported
      // We don't support Structs, Maps, Arrays, or Calendar Intervals.  If they are a part of
      // any operation we need to stop right there and not insert columnar versions.
      overrides(plan)
    } else {
      plan
    }
  }

  override def post: Rule[SparkPlan] = plan => {
    if (gpuEnabled) {
      overrideTransitions(plan)
    } else {
      plan
    }
  }
}

/**
  * Extension point to enable GPU processing.
  *
  * To run on a GPU set spark.sql.extensions to ai.rapids.spark.Plugin
  */
class Plugin extends Function1[SparkSessionExtensions, Unit] with Logging {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    logWarning("Installing extensions to enable rapids.ai GPU support." +
      " To disable GPU support set `ai.rapids.gpu.enabled` to false")
    extensions.injectColumnar((session) => ColumnarOverrideRules(session))
  }
}
