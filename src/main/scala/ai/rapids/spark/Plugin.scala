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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class GpuProjectExec(projectList: Seq[GpuExpression], child: SparkPlan)
  extends ProjectExec(projectList.asInstanceOf[Seq[NamedExpression]], child) {

  override def supportsColumnar = true
  // Disable code generation for now...
  override def supportCodegen: Boolean = false

  override def doExecuteColumnar() : RDD[ColumnarBatch] = {
    val boundProjectList: Seq[Any] = GpuBindReferences.bindReferences(projectList, child.output)
    val rdd = child.executeColumnar()
    AutoCloseColumnBatchIterator.map(rdd,
      (cb: ColumnarBatch) => {
        val newColumns = boundProjectList.map(
          expr => {
            val result = expr.asInstanceOf[GpuExpression].columnarEval(cb)
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

class CannotReplaceException(str: String) extends RuntimeException(str) {

}

case class GpuOverrides(session: SparkSession) extends Rule[SparkPlan] with Logging {
  lazy val incompatEnabled = Plugin.isIncompatEnabled(session)
  lazy val gpuEnabled = Plugin.isGpuEnabled(session)

  def areAllSupportedTypes(types: DataType*): Boolean = {
    types.forall(_ match {
      case BooleanType => true
      case ByteType => true
      case ShortType => true
      case IntegerType => true
      case LongType => true
      case FloatType => true
      case DoubleType => true
      case DateType => true
      case TimestampType => false // true we really need to understand how the timezone works with this
      case StringType => false // true we cannot convert rows to strings so we cannot support this right now...
      case _ => false
    })
  }

  def replaceIncompatUnaryExpressions(exp: UnaryExpression, child: GpuExpression): GpuExpression = exp match {
      // for all of the following the floating point results are not exact
    case atan: Atan => new GpuAtan(child)
    case cos: Cos => new GpuCos(child)
    case exp: Exp => new GpuExp(child)
    case log: Log => new GpuLog(child)
    case sin: Sin => new GpuSin(child)
    case tan: Tan => new GpuTan(child)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceUnaryExpressions(exp: UnaryExpression, child: GpuExpression): GpuExpression = exp match {
    case cast: Cast if GpuCast.canCast(cast.child.dataType, cast.dataType) =>
      new GpuCast(child, cast.dataType, cast.timeZoneId)
    case min: UnaryMinus => new GpuUnaryMinus(child)
    case plus: UnaryPositive => new GpuUnaryPositive(child)
    case abs: Abs => new GpuAbs(child)
    case acos: Acos => new GpuAcos(child)
    case asin: Asin => new GpuAsin(child)
    case sqrt: Sqrt => new GpuSqrt(child)
    case floor: Floor => new GpuFloor(child)
    case ceil: Ceil => new GpuCeil(child)
    case exp if incompatEnabled => replaceIncompatUnaryExpressions(exp, child)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceIncompatBinaryExpressions(exp: BinaryExpression,
      left: GpuExpression, right:GpuExpression): GpuExpression = exp match {
    // floating point results are not always bit for bit exact
    case pow: Pow => new GpuPow(left, right)
    // divide by 0 results in null for spark but -Infinity for cudf
    case div: Divide => new GpuDivide(left, right)
    // divide by 0 results in null for spark but -1 for cudf
    case div: IntegralDivide => new GpuIntegralDivide(left, right)
    // divide by 0 results in null for spark, but not for cudf
    case rem: Remainder => new GpuRemainder(left, right)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceBinaryExpressions(exp: BinaryExpression,
      left: GpuExpression, right:GpuExpression): GpuExpression = exp match {
    case add: Add => new GpuAdd(left, right)
    case sub: Subtract => new GpuSubtract(left, right)
    case mul: Multiply => new GpuMultiply(left, right)
    case exp if incompatEnabled => replaceIncompatBinaryExpressions(exp, left, right)
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithGpuExpression(exp: Expression): GpuExpression = exp match {
    case a: Alias =>
      new GpuAlias(replaceWithGpuExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      new GpuAttributeReference(att.name, att.dataType, att.nullable,
        att.metadata)(att.exprId, att.qualifier)
    case lit: Literal =>
      new GpuLiteral(lit.value, lit.dataType)
    case exp: UnaryExpression if areAllSupportedTypes(exp.dataType, exp.child.dataType) =>
      replaceUnaryExpressions(exp, replaceWithGpuExpression(exp.child))
    case exp: BinaryExpression if (areAllSupportedTypes(exp.dataType, exp.left.dataType, exp.right.dataType)) =>
      replaceBinaryExpressions(exp,
        replaceWithGpuExpression(exp.left), replaceWithGpuExpression(exp.right))
    case exp =>
      throw new CannotReplaceException(s"expression ${exp.getClass} ${exp} is not currently supported.")
  }

  def replaceWithGpuPlan(plan: SparkPlan): SparkPlan =
    try {
      plan match {
        case plan: ProjectExec =>
          new GpuProjectExec(plan.projectList.map((exp) => replaceWithGpuExpression(exp)),
            replaceWithGpuPlan(plan.child))
        case p =>
          logWarning(s"GPU Processing for ${p.getClass} is not currently supported.")
          p.withNewChildren(p.children.map(replaceWithGpuPlan))
      }
    } catch {
      case exp: CannotReplaceException =>
        logWarning(s"Columnar processing for ${plan.getClass} is not currently supported" +
          s"because ${exp.getMessage}")
        plan.withNewChildren(plan.children.map(replaceWithGpuPlan))
    }

  override def apply(plan: SparkPlan) :SparkPlan = {
    if (gpuEnabled) {
      replaceWithGpuPlan(plan)
    } else {
      plan
    }
  }
}

case class GpuTransitionOverrides(session: SparkSession) extends Rule[SparkPlan] {
  lazy val underTest = Plugin.isTestEnabled(session)
  lazy val gpuEnabled = Plugin.isGpuEnabled(session)

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

  def assertIsOnTheGpu(exp: Expression): Unit = {
    if (!exp.isInstanceOf[GpuExpression]) {
      throw new IllegalArgumentException(s"The expression ${exp} is not columnar ${exp.getClass}")
    }
  }

  def assertIsOnTheGpu(plan: SparkPlan): Unit = {
    plan match {
      case lts: LocalTableScanExec =>
        if (!lts.expressions.forall(_.isInstanceOf[AttributeReference])) {
          throw new IllegalArgumentException("It looks like some operations were " +
            s"pushed down to LocalTableScanExec ${lts.expressions.mkString(",")}")
        }
      case _: GpuColumnarToRowExec => () // Ignored
      case _: ShuffleExchangeExec => () // Ignored for now
      case _ =>
        if (!plan.supportsColumnar) {
          throw new IllegalArgumentException(s"Part of the plan is not columnar ${plan.getClass}\n${plan}")
        }
        plan.expressions.foreach(assertIsOnTheGpu)
    }
    plan.children.foreach(assertIsOnTheGpu)
  }

  override def apply(plan: SparkPlan) :SparkPlan = {
    if (gpuEnabled) {
      val ret = replaceWithGpuPlan(plan)
      if (underTest) {
        assertIsOnTheGpu(ret)
      }
      ret
    } else {
      plan
    }
  }
}

case class ColumnarOverrideRules(session: SparkSession) extends ColumnarRule with Logging {

  val overrides = GpuOverrides(session)
  val overrideTransitions = GpuTransitionOverrides(session)

  override def preColumnarTransitions : Rule[SparkPlan] = overrides

  override def postColumnarTransitions: Rule[SparkPlan] = overrideTransitions
}

object Plugin {
  val GPU_ENABLED_CONF: String = "ai.rapids.gpu.enabled"
  // Some operations are currently not 100% compatible with spark.  This will enable
  // those operations if a customer is willing to work around compatibility issues for
  // more processing on the GPU
  val INCOMPATIBLE_OPS_CONF: String = "ai.rapids.gpu.incompatible_ops"
  val TEST_CONF: String = "ai.rapids.gpu.testing"

  def isGpuEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(GPU_ENABLED_CONF, "true").trim.toBoolean

  def isIncompatEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(INCOMPATIBLE_OPS_CONF, "false").trim.toBoolean

  def isTestEnabled(session: SparkSession): Boolean = session.sqlContext.
    getConf(TEST_CONF, "false").trim.toBoolean
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
