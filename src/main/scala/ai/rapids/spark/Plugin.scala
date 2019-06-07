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

case class GpuOverrides(session: SparkSession) extends Rule[SparkPlan] with Logging {
  lazy val underTest = session.sqlContext.
    getConf("ai.rapids.gpu.testing", "false").trim.toBoolean
  // Some operations are currently not 100% compatible with spark.  This will enable
  // those operations if a customer is willing to work around compatibility issues for
  // more processing on the GPU
  lazy val enableIncompat = underTest || session.sqlContext.
    getConf("ai.rapids.gpu.incompatible_ops", "false").trim.toBoolean

  lazy val gpuEnabled = session.sqlContext.
    getConf("ai.rapids.gpu.enabled", "true").trim.toBoolean

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

  def replaceWithGpuExpression(exp: Expression): Expression = exp match {
    case a: Alias =>
      Alias(replaceWithGpuExpression(a.child), a.name)(a.exprId, a.qualifier, a.explicitMetadata)
    case att: AttributeReference =>
      att // No sub expressions and already supports columnar so just return it
    case lit: Literal =>
      lit // No sub expressions and already supports columnar so just return it
    case add: Add if (areAllSupportedTypes(add.dataType, add.left.dataType, add.right.dataType)) =>
      new GpuAdd(replaceWithGpuExpression(add.left),
        replaceWithGpuExpression(add.right))
    case sub: Subtract if (areAllSupportedTypes(sub.dataType, sub.left.dataType, sub.right.dataType)) =>
      new GpuSubtract(replaceWithGpuExpression(sub.left),
        replaceWithGpuExpression(sub.right))
    case mul: Multiply if (areAllSupportedTypes(mul.dataType, mul.left.dataType, mul.right.dataType)) =>
      new GpuMultiply(replaceWithGpuExpression(mul.left),
        replaceWithGpuExpression(mul.right))
    case min: UnaryMinus if (areAllSupportedTypes(min.dataType, min.child.dataType)) =>
      new GpuUnaryMinus(replaceWithGpuExpression(min.child))
    case plus: UnaryPositive if (areAllSupportedTypes(plus.dataType, plus.child.dataType)) =>
      new GpuUnaryPositive(replaceWithGpuExpression(plus.child))
    case abs: Abs if (areAllSupportedTypes(abs.dataType, abs.child.dataType)) =>
      new GpuAbs(replaceWithGpuExpression(abs.child))
    case acos: Acos if (areAllSupportedTypes(acos.child.dataType)) =>
      new GpuAcos(replaceWithGpuExpression(acos.child))
    case asin: Asin if (areAllSupportedTypes(asin.child.dataType)) =>
      new GpuAsin(replaceWithGpuExpression(asin.child))
    case atan: Atan if (areAllSupportedTypes(atan.child.dataType)) =>
      new GpuAtan(replaceWithGpuExpression(atan.child))
    case ceil: Ceil if (areAllSupportedTypes(ceil.child.dataType)) =>
      new GpuCeil(replaceWithGpuExpression(ceil.child))
    case cos: Cos if (areAllSupportedTypes(cos.child.dataType)) =>
      new GpuCos(replaceWithGpuExpression(cos.child))
    case exp: Exp if (areAllSupportedTypes(exp.child.dataType)) =>
      new GpuExp(replaceWithGpuExpression(exp.child))
    case floor: Floor if (areAllSupportedTypes(floor.child.dataType)) =>
      new GpuFloor(replaceWithGpuExpression(floor.child))
    case log: Log if (areAllSupportedTypes(log.child.dataType)) =>
      new GpuLog(replaceWithGpuExpression(log.child))
    case sin: Sin if (areAllSupportedTypes(sin.child.dataType)) =>
      new GpuSin(replaceWithGpuExpression(sin.child))
    case sqrt: Sqrt if (areAllSupportedTypes(sqrt.child.dataType)) =>
      new GpuSqrt(replaceWithGpuExpression(sqrt.child))
    case tan: Tan if (areAllSupportedTypes(tan.child.dataType)) =>
      new GpuTan(replaceWithGpuExpression(tan.child))
    case cast: Cast if (areAllSupportedTypes(cast.dataType, cast.child.dataType) &&
      GpuCast.canCast(cast.child.dataType, cast.dataType)) =>
      new GpuCast(replaceWithGpuExpression(cast.child), cast.dataType, cast.timeZoneId)
    case pow: Pow if enableIncompat && // floating point results are not always bit for bit exact
      (areAllSupportedTypes(pow.dataType, pow.left.dataType, pow.right.dataType)) =>
      new GpuPow(replaceWithGpuExpression(pow.left),
        replaceWithGpuExpression(pow.right))
    case div: Divide if enableIncompat && // divide by 0 results in null for spark but -Infinity for cudf
      (areAllSupportedTypes(div.dataType, div.left.dataType, div.right.dataType)) =>
      new GpuDivide(replaceWithGpuExpression(div.left),
        replaceWithGpuExpression(div.right))
    case div: IntegralDivide if enableIncompat && // divide by 0 results in null for spark but -1 for cudf
      (areAllSupportedTypes(div.dataType, div.left.dataType, div.right.dataType)) =>
      new GpuIntegralDivide(replaceWithGpuExpression(div.left),
        replaceWithGpuExpression(div.right))
    case rem: Remainder if enableIncompat &&  // divide by 0 results in null for spark, but not for cudf
      (areAllSupportedTypes(rem.dataType, rem.left.dataType, rem.right.dataType)) =>
      new GpuRemainder(replaceWithGpuExpression(rem.left),
        replaceWithGpuExpression(rem.right))
    case exp =>
      if (underTest) {
        throw new IllegalStateException(s"GPU Processing for expression ${exp.getClass} ${exp} is not currently supported.")
      } else {
        logWarning(s"GPU Processing for expression ${exp.getClass} ${exp} is not currently supported.")
      }
      exp
  }

  def replaceWithGpuPlan(plan: SparkPlan): SparkPlan = plan match {
    case plan: ProjectExec =>
      new GpuProjectExec(plan.projectList.map((exp) =>
        replaceWithGpuExpression(exp).asInstanceOf[NamedExpression]),
        replaceWithGpuPlan(plan.child))
    case p =>
      if (underTest) {
        p match {
          case _ :LocalTableScanExec => ()
          case _ :ShuffleExchangeExec => () //Ignored for now
          case _ =>
            throw new IllegalStateException(s"GPU Processing for ${p.getClass} is not currently supported.")
        }
      } else {
        logWarning(s"GPU Processing for ${p.getClass} is not currently supported.")
      }
      p.withNewChildren(p.children.map(replaceWithGpuPlan))
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

  lazy val gpuEnabled = session.sqlContext.
    getConf("ai.rapids.gpu.enabled", "true").trim.toBoolean

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

 override def apply(plan: SparkPlan) :SparkPlan = {
    if (gpuEnabled) {
      replaceWithGpuPlan(plan)
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
